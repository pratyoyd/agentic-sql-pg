#!/usr/bin/env python3
"""
Stage 3.5 revised: join-only cardinality feedback pilot on flag-18.

Runs each rep twice on agentic_poc_scaled:
  1. Baseline: vanilla execution
  2. Hinted: with Rows() hints on join cardinalities from accumulated history

Only join nodes with ≥2 relation aliases are hintable.
"""

import json
import statistics
import time
from pathlib import Path
from typing import Any

import psycopg

from pg_plan_parser import extract_plan_tree, PLAN_CRITICAL_OPS

CONNINFO = "host=localhost port=5434 dbname=agentic_poc_scaled"
TRACE_DIR = Path("stage2/traces")
OUTPUT_DIR = Path("stage3_5_join_pilot")

NUM_RUNS = 6
WARMUP_RUNS = 2

JOIN_OPS = {"Hash Join", "Merge Join", "Nested Loop"}


def load_trace(path: Path) -> list[dict]:
    return [json.loads(l) for l in open(path)]


def run_explain_analyze(conn, sql: str) -> tuple[list[dict], list]:
    """Returns (flat_tree, raw_json)."""
    row = conn.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}").fetchone()
    raw = row[0]
    tree = extract_plan_tree(raw)
    return tree, raw


def measure_latency(conn, sql: str) -> list[float]:
    times = []
    for _ in range(NUM_RUNS):
        t0 = time.time()
        conn.execute(sql).fetchall()
        times.append((time.time() - t0) * 1000)
    return times


def stable_median(times: list[float]) -> float:
    return statistics.median(times[WARMUP_RUNS:])


def q_error(est: float, act: float) -> float:
    if est <= 0 or act <= 0:
        return 1.0
    return max(est / act, act / est)


def max_q_error_joins(tree: list[dict]) -> float:
    mx = 1.0
    for n in tree:
        if n["operator_type"] not in JOIN_OPS:
            continue
        est, act = n.get("estimated_card"), n.get("actual_card")
        if est and act and act > 0:
            mx = max(mx, q_error(est, act))
    return mx


def build_join_sig_history(plan_trees: list[list[dict]]) -> dict:
    """sig -> {actual_card, relation_aliases, query_idx}. Most recent wins."""
    history = {}
    for qi, tree in enumerate(plan_trees):
        for node in tree:
            if node["operator_type"] not in JOIN_OPS:
                continue
            aliases = node.get("relation_aliases", [])
            if len(aliases) < 2:
                continue
            act = node.get("actual_card")
            if act is None:
                continue
            history[node["operator_signature"]] = {
                "actual_card": act,
                "relation_aliases": aliases,
                "query_idx": qi,
            }
    return history


def construct_join_hints(conn, sql: str, plan_history: list[list[dict]]):
    """
    Construct Rows() hints for join nodes.
    Returns (hinted_sql, hint_block, applied_hints, overhead_ms).
    """
    t0 = time.time()
    sig_hist = build_join_sig_history(plan_history)

    if not sig_hist:
        return sql, "", [], (time.time() - t0) * 1000

    # EXPLAIN (no ANALYZE) to get vanilla plan structure
    row = conn.execute(f"EXPLAIN (FORMAT JSON) {sql}").fetchone()
    vanilla_tree = extract_plan_tree(row[0])

    hints = []
    applied = []
    for node in vanilla_tree:
        if node["operator_type"] not in JOIN_OPS:
            continue
        aliases = node.get("relation_aliases", [])
        if len(aliases) < 2:
            continue
        sig = node["operator_signature"]
        if sig not in sig_hist:
            continue
        match = sig_hist[sig]
        alias_str = " ".join(sorted(aliases))
        actual = match["actual_card"]
        hint = f"Rows({alias_str} #{actual})"
        hints.append(hint)
        applied.append({
            "signature": sig,
            "relation_aliases": aliases,
            "actual_card": actual,
            "source_query_idx": match["query_idx"],
            "hint_string": hint,
        })

    overhead_ms = (time.time() - t0) * 1000
    if hints:
        block = "/*+ " + " ".join(hints) + " */"
        return f"{block}\n{sql}", block, applied, overhead_ms
    return sql, "", applied, overhead_ms


def format_plan_text(plan_json, depth=0):
    if isinstance(plan_json, list):
        node = plan_json[0].get("Plan", {})
    else:
        node = plan_json
    return _fmt(node, depth)


def _fmt(n, d=0):
    indent = "  " * d
    nt = n.get("Node Type", "?")
    est = n.get("Plan Rows", "?")
    act = n.get("Actual Rows", "?")
    tm = n.get("Actual Total Time", "?")
    rel = n.get("Relation Name", "")
    alias = n.get("Alias", "")
    jt = n.get("Join Type", "")
    strat = n.get("Strategy", "")

    line = f"{indent}{nt}"
    if jt: line += f" [{jt}]"
    if strat: line += f" [{strat}]"
    if rel: line += f" on {rel}"
    if alias and alias != rel: line += f" ({alias})"
    for k in ("Filter", "Hash Cond", "Join Filter", "Merge Cond", "Index Cond"):
        v = n.get(k)
        if v: line += f"  {k}: {v}"
    line += f"  (est={est} actual={act} time={tm}ms)"
    lines = [line]
    for c in n.get("Plans", []):
        lines.append(_fmt(c, d + 1))
    return "\n".join(lines)


def compare_plans(b_tree, h_tree):
    b_types = [(n["operator_type"], n.get("relation_aliases", [])) for n in b_tree]
    h_types = [(n["operator_type"], n.get("relation_aliases", [])) for n in h_tree]
    topo = b_types != h_types

    par_ops = {"Gather", "Gather Merge"}
    b_par = [n["operator_type"] for n in b_tree if n["operator_type"] in par_ops]
    h_par = [n["operator_type"] for n in h_tree if n["operator_type"] in par_ops]
    par_changed = b_par != h_par

    diffs = []
    if topo:
        for i, (b, h) in enumerate(zip(
            [n["operator_type"] for n in b_tree],
            [n["operator_type"] for n in h_tree]
        )):
            if b != h:
                diffs.append(f"node {i}: {b} → {h}")
        if len(b_tree) != len(h_tree):
            diffs.append(f"node count: {len(b_tree)} → {len(h_tree)}")
    if par_changed:
        diffs.append(f"parallel: {b_par} → {h_par}")

    return {
        "plan_changed_topology": topo,
        "plan_changed_parallelism": par_changed,
        "plan_changed_any": topo or par_changed,
        "diffs": diffs,
    }


def run_baseline(trace: list[dict]) -> list[dict]:
    conn = psycopg.connect(CONNINFO, autocommit=True)
    conn.execute("DISCARD PLANS")
    results = []
    for e in trace:
        if not e["success"]:
            continue
        sql = e["raw_sql"]
        seq = e["query_seq"]
        try:
            tree, raw = run_explain_analyze(conn, sql)
            lats = measure_latency(conn, sql)
            med = stable_median(lats)
            results.append({
                "query_seq": seq, "sql": sql,
                "plan_tree": tree, "plan_json": raw,
                "latencies_ms": lats, "median_latency_ms": med,
                "q_error_join_max": max_q_error_joins(tree),
            })
        except Exception as ex:
            print(f"  Baseline Q{seq} error: {ex}")
    conn.close()
    return results


def run_hinted(trace: list[dict]) -> list[dict]:
    conn = psycopg.connect(CONNINFO, autocommit=True)
    conn.execute("DISCARD PLANS")
    plan_history = []
    results = []
    for e in trace:
        if not e["success"]:
            continue
        sql = e["raw_sql"]
        seq = e["query_seq"]
        try:
            hinted_sql, block, applied, overhead = construct_join_hints(conn, sql, plan_history)
            tree, raw = run_explain_analyze(conn, hinted_sql)
            lats = measure_latency(conn, hinted_sql)
            med = stable_median(lats)
            plan_history.append(tree)
            results.append({
                "query_seq": seq, "sql": sql,
                "hinted_sql": hinted_sql, "hint_block": block,
                "applied_hints": applied, "injection_overhead_ms": overhead,
                "plan_tree": tree, "plan_json": raw,
                "latencies_ms": lats, "median_latency_ms": med,
                "q_error_join_max": max_q_error_joins(tree),
            })
        except Exception as ex:
            print(f"  Hinted Q{seq} error: {ex}")
    conn.close()
    return results


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    trace_path = TRACE_DIR / "flag-18_rep_a.jsonl"
    trace = load_trace(trace_path)
    successful = [e for e in trace if e.get("success", True)]
    print(f"flag-18_rep_a: {len(successful)} successful queries")

    print("\nRunning baseline...")
    baseline = run_baseline(successful)
    print(f"  Done: {len(baseline)} queries")

    print("Running hinted...")
    hinted = run_hinted(successful)
    hints_applied = sum(1 for r in hinted if r.get("hint_block"))
    print(f"  Done: {len(hinted)} queries, {hints_applied} had hints")

    # Per-query diffs
    b_by_seq = {r["query_seq"]: r for r in baseline}
    h_by_seq = {r["query_seq"]: r for r in hinted}
    diffs = []
    for seq in sorted(b_by_seq.keys()):
        if seq not in h_by_seq:
            continue
        b, h = b_by_seq[seq], h_by_seq[seq]
        cmp = compare_plans(b["plan_tree"], h["plan_tree"])
        lat_b, lat_h = b["median_latency_ms"], h["median_latency_ms"]
        delta = lat_h - lat_b
        delta_pct = (delta / lat_b * 100) if lat_b > 0 else 0
        diffs.append({
            "query_seq": seq,
            **cmp,
            "latency_baseline_ms": round(lat_b, 3),
            "latency_hinted_ms": round(lat_h, 3),
            "latency_delta_ms": round(delta, 3),
            "latency_delta_pct": round(delta_pct, 2),
            "q_error_baseline_max": round(b["q_error_join_max"], 2),
            "q_error_hinted_max": round(h["q_error_join_max"], 2),
            "hints_injected": h.get("applied_hints", []),
            "hint_block": h.get("hint_block", ""),
            "injection_overhead_ms": round(h.get("injection_overhead_ms", 0), 3),
        })

    # Aggregates
    n = len(diffs)
    hinted_count = sum(1 for d in diffs if d["hint_block"])
    changed_count = sum(1 for d in diffs if d["plan_changed_any"])
    deltas = [d["latency_delta_ms"] for d in diffs]
    deltas_pct = [d["latency_delta_pct"] for d in diffs]
    changed_deltas = [d["latency_delta_ms"] for d in diffs if d["plan_changed_any"]]
    overheads = [d["injection_overhead_ms"] for d in diffs]
    b_total = sum(d["latency_baseline_ms"] for d in diffs)
    h_total = sum(d["latency_hinted_ms"] for d in diffs)

    print(f"\n{'='*60}")
    print(f"PILOT RESULTS: flag-18 (1 rep, {n} queries)")
    print(f"{'='*60}")
    print(f"Queries with hints injected: {hinted_count}/{n}")
    print(f"Queries with plan changes:   {changed_count}/{n}")
    print()

    print("Per-query detail:")
    print(f"{'Q':>3} {'Hints':>6} {'Changed':>8} {'Base ms':>10} {'Hint ms':>10} {'Delta ms':>10} {'Delta%':>8} {'QE base':>8} {'QE hint':>8}")
    for d in diffs:
        hints_n = len(d["hints_injected"]) if d["hints_injected"] else 0
        print(f"Q{d['query_seq']:>2} {hints_n:>6} {'YES' if d['plan_changed_any'] else 'no':>8} "
              f"{d['latency_baseline_ms']:>10.1f} {d['latency_hinted_ms']:>10.1f} "
              f"{d['latency_delta_ms']:>10.1f} {d['latency_delta_pct']:>7.1f}% "
              f"{d['q_error_baseline_max']:>8.2f} {d['q_error_hinted_max']:>8.2f}")

    print()
    if deltas:
        print(f"All queries:  median delta = {statistics.median(deltas):.1f}ms ({statistics.median(deltas_pct):.1f}%)")
    if changed_deltas:
        print(f"Changed only: median delta = {statistics.median(changed_deltas):.1f}ms")
    print(f"Session total: baseline={b_total:.1f}ms hinted={h_total:.1f}ms delta={h_total-b_total:.1f}ms ({(h_total-b_total)/b_total*100:.1f}%)")
    if overheads:
        print(f"Injection overhead: median={statistics.median(overheads):.2f}ms P95={sorted(overheads)[min(int(0.95*len(overheads)), len(overheads)-1)]:.2f}ms")

    # Save everything
    output = {
        "task": "flag-18",
        "rep": "flag-18_rep_a",
        "total_queries": n,
        "hinted_queries": hinted_count,
        "changed_queries": changed_count,
        "diffs": diffs,
        "baseline_total_ms": round(b_total, 1),
        "hinted_total_ms": round(h_total, 1),
    }
    with open(OUTPUT_DIR / "pilot_results.json", "w") as f:
        json.dump(output, f, indent=2, default=str)

    # Case studies: queries where hints were injected
    cases = []
    for d in diffs:
        if d["hint_block"] or d["plan_changed_any"]:
            b, h = b_by_seq[d["query_seq"]], h_by_seq[d["query_seq"]]
            cases.append({
                "query_seq": d["query_seq"],
                "sql": b["sql"],
                "hint_block": d["hint_block"],
                "hints": d["hints_injected"],
                "baseline_plan": format_plan_text(b["plan_json"]),
                "hinted_plan": format_plan_text(h["plan_json"]),
                "baseline_latency": d["latency_baseline_ms"],
                "hinted_latency": d["latency_hinted_ms"],
                "delta_pct": d["latency_delta_pct"],
                "plan_changed": d["plan_changed_any"],
                "diffs": d["diffs"],
            })
    with open(OUTPUT_DIR / "case_studies.json", "w") as f:
        json.dump(cases, f, indent=2, default=str)

    # Save raw per-query data
    with open(OUTPUT_DIR / "baseline_raw.json", "w") as f:
        json.dump([{k: v for k, v in r.items() if k != "plan_json"} for r in baseline],
                  f, indent=2, default=str)
    with open(OUTPUT_DIR / "hinted_raw.json", "w") as f:
        json.dump([{k: v for k, v in r.items() if k != "plan_json"} for r in hinted],
                  f, indent=2, default=str)

    print(f"\nResults saved to {OUTPUT_DIR}/")
    return output, cases


if __name__ == "__main__":
    main()
