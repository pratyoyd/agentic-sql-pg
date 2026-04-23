#!/usr/bin/env python3
"""
Stage 3.5 pilot: end-to-end cardinality feedback on flag-19.

Runs each rep trace twice on agentic_poc_scaled:
  1. Baseline: vanilla execution
  2. Hinted: with Rows() hints from accumulated cardinality history

Measures per-query plan diffs, latency deltas, and injection overhead.
"""

import json
import statistics
import time
from pathlib import Path
from typing import Any

import psycopg

from pg_plan_parser import extract_plan_tree, is_plan_critical, PLAN_CRITICAL_OPS
from hint_constructor import construct_hints_for_query, build_signature_history

CONNINFO = "host=localhost port=5434 dbname=agentic_poc_scaled"
TRACE_DIR = Path("stage2/traces")
STAGE35_DIR = Path("stage3.5")

# Latency measurement: 6 runs, discard first 2, median of runs 3-6
NUM_RUNS = 6
WARMUP_RUNS = 2


def load_trace(trace_path: Path) -> list[dict]:
    """Load a JSONL trace file."""
    entries = []
    for line in open(trace_path):
        entries.append(json.loads(line))
    return entries


def run_explain_analyze(conn, sql: str) -> tuple[list[dict], dict]:
    """Run EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) and return (plan_tree, raw_json)."""
    row = conn.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}").fetchone()
    raw = row[0]
    tree = extract_plan_tree(raw)
    return tree, raw


def measure_latency(conn, sql: str) -> list[float]:
    """Run SQL NUM_RUNS times, return wall-clock times in ms."""
    times = []
    for _ in range(NUM_RUNS):
        t0 = time.time()
        conn.execute(sql).fetchall()
        times.append((time.time() - t0) * 1000)
    return times


def stable_median(times: list[float]) -> float:
    """Discard first WARMUP_RUNS, take median of rest."""
    return statistics.median(times[WARMUP_RUNS:])


def q_error(estimated: float, actual: float) -> float:
    """Compute q-error: max(est/act, act/est). Both must be > 0."""
    if estimated <= 0 or actual <= 0:
        return 1.0
    return max(estimated / actual, actual / estimated)


def max_q_error_plan_critical(tree: list[dict]) -> float:
    """Max q-error across plan-critical nodes."""
    max_qe = 1.0
    for node in tree:
        if node["operator_type"] not in PLAN_CRITICAL_OPS:
            continue
        est = node.get("estimated_card")
        act = node.get("actual_card")
        if est is not None and act is not None and act > 0:
            max_qe = max(max_qe, q_error(est, act))
    return max_qe


def flatten_plan_topology(tree: list[dict]) -> list[tuple[str, int]]:
    """Extract (node_type, depth-ish) list for topology comparison."""
    return [(n["operator_type"], len(n.get("relation_aliases", []))) for n in tree]


def compare_plans(baseline_tree: list[dict], hinted_tree: list[dict]) -> dict:
    """Compare two plan trees for topology, parallelism, and any differences."""
    b_types = [n["operator_type"] for n in baseline_tree]
    h_types = [n["operator_type"] for n in hinted_tree]

    # Check topology
    topology_changed = b_types != h_types

    # Check parallelism (Gather, Gather Merge nodes)
    parallel_ops = {"Gather", "Gather Merge"}
    b_parallel = [n["operator_type"] for n in baseline_tree if n["operator_type"] in parallel_ops]
    h_parallel = [n["operator_type"] for n in hinted_tree if n["operator_type"] in parallel_ops]
    parallelism_changed = b_parallel != h_parallel

    # Any change
    any_changed = topology_changed or parallelism_changed

    # Describe diffs
    diffs = []
    if topology_changed:
        for i, (b, h) in enumerate(zip(b_types, h_types)):
            if b != h:
                diffs.append(f"node {i}: {b} → {h}")
        if len(b_types) != len(h_types):
            diffs.append(f"node count: {len(b_types)} → {len(h_types)}")
    if parallelism_changed:
        diffs.append(f"parallel ops: {b_parallel} → {h_parallel}")

    return {
        "plan_changed_topology": topology_changed,
        "plan_changed_parallelism": parallelism_changed,
        "plan_changed_any": any_changed,
        "diffs": diffs,
    }


def run_baseline(trace: list[dict]) -> list[dict]:
    """Run baseline condition for one rep."""
    conn = psycopg.connect(CONNINFO, autocommit=True)
    conn.execute("DISCARD PLANS")

    results = []
    for entry in trace:
        if not entry["success"]:
            continue
        sql = entry["raw_sql"]
        query_seq = entry["query_seq"]

        # EXPLAIN ANALYZE for plan tree
        plan_tree, plan_json = run_explain_analyze(conn, sql)

        # Latency measurement
        latencies = measure_latency(conn, sql)
        median_lat = stable_median(latencies)

        results.append({
            "query_seq": query_seq,
            "sql": sql,
            "plan_tree": plan_tree,
            "plan_json": plan_json,
            "latencies_ms": latencies,
            "median_latency_ms": median_lat,
            "q_error_max": max_q_error_plan_critical(plan_tree),
        })

    conn.close()
    return results


def run_hinted(trace: list[dict]) -> list[dict]:
    """Run hinted condition for one rep."""
    conn = psycopg.connect(CONNINFO, autocommit=True)
    conn.execute("DISCARD PLANS")

    plan_history: list[list[dict]] = []  # plan trees from prior queries
    results = []

    for entry in trace:
        if not entry["success"]:
            continue
        sql = entry["raw_sql"]
        query_seq = entry["query_seq"]

        # Construct hints from history
        hinted_sql, hint_block, applied_hints, overhead_ms = \
            construct_hints_for_query(sql, conn, plan_history)

        # EXPLAIN ANALYZE with (possibly) hinted SQL
        plan_tree, plan_json = run_explain_analyze(conn, hinted_sql)

        # Latency measurement (with hints)
        latencies = measure_latency(conn, hinted_sql)
        median_lat = stable_median(latencies)

        # Add to history for future queries
        plan_history.append(plan_tree)

        results.append({
            "query_seq": query_seq,
            "sql": sql,
            "hinted_sql": hinted_sql,
            "hint_block": hint_block,
            "applied_hints": applied_hints,
            "injection_overhead_ms": overhead_ms,
            "plan_tree": plan_tree,
            "plan_json": plan_json,
            "latencies_ms": latencies,
            "median_latency_ms": median_lat,
            "q_error_max": max_q_error_plan_critical(plan_tree),
        })

    conn.close()
    return results


def compute_per_query_diffs(baseline: list[dict], hinted: list[dict]) -> list[dict]:
    """Compute per-query comparison between baseline and hinted conditions."""
    diffs = []
    b_by_seq = {r["query_seq"]: r for r in baseline}
    h_by_seq = {r["query_seq"]: r for r in hinted}

    for seq in sorted(b_by_seq.keys()):
        if seq not in h_by_seq:
            continue
        b = b_by_seq[seq]
        h = h_by_seq[seq]

        plan_cmp = compare_plans(b["plan_tree"], h["plan_tree"])
        lat_b = b["median_latency_ms"]
        lat_h = h["median_latency_ms"]
        delta = lat_h - lat_b
        delta_pct = (delta / lat_b * 100) if lat_b > 0 else 0

        diffs.append({
            "query_seq": seq,
            **plan_cmp,
            "latency_baseline_ms": round(lat_b, 3),
            "latency_hinted_ms": round(lat_h, 3),
            "latency_delta_ms": round(delta, 3),
            "latency_delta_pct": round(delta_pct, 2),
            "q_error_baseline_max": round(b["q_error_max"], 2),
            "q_error_hinted_max": round(h["q_error_max"], 2),
            "hints_injected": h.get("applied_hints", []),
            "injection_overhead_ms": round(h.get("injection_overhead_ms", 0), 3),
        })

    return diffs


def compute_rep_aggregates(diffs: list[dict]) -> dict:
    """Compute aggregate metrics for one rep."""
    n = len(diffs)
    topology_changed = sum(1 for d in diffs if d["plan_changed_topology"])
    parallelism_changed = sum(1 for d in diffs if d["plan_changed_parallelism"])
    any_changed = sum(1 for d in diffs if d["plan_changed_any"])

    deltas = [d["latency_delta_ms"] for d in diffs]
    deltas_pct = [d["latency_delta_pct"] for d in diffs]
    overheads = [d["injection_overhead_ms"] for d in diffs]

    # Only queries with plan changes
    changed_deltas = [d["latency_delta_ms"] for d in diffs if d["plan_changed_any"]]
    changed_deltas_pct = [d["latency_delta_pct"] for d in diffs if d["plan_changed_any"]]

    baseline_total = sum(d["latency_baseline_ms"] for d in diffs)
    hinted_total = sum(d["latency_hinted_ms"] for d in diffs)

    def safe_median(lst):
        return statistics.median(lst) if lst else 0
    def safe_p95(lst):
        if not lst:
            return 0
        lst_sorted = sorted(lst)
        idx = min(int(0.95 * len(lst_sorted)), len(lst_sorted) - 1)
        return lst_sorted[idx]

    return {
        "total_queries": n,
        "topology_changed": topology_changed,
        "parallelism_changed": parallelism_changed,
        "any_changed": any_changed,
        "median_delta_ms": round(safe_median(deltas), 3),
        "p95_delta_ms": round(safe_p95(deltas), 3),
        "median_delta_pct": round(safe_median(deltas_pct), 2),
        "changed_median_delta_ms": round(safe_median(changed_deltas), 3) if changed_deltas else None,
        "changed_median_delta_pct": round(safe_median(changed_deltas_pct), 2) if changed_deltas else None,
        "baseline_total_ms": round(baseline_total, 1),
        "hinted_total_ms": round(hinted_total, 1),
        "session_delta_ms": round(hinted_total - baseline_total, 1),
        "mean_overhead_ms": round(statistics.mean(overheads), 3) if overheads else 0,
        "p95_overhead_ms": round(safe_p95(overheads), 3) if overheads else 0,
    }


def compute_binned_analysis(diffs: list[dict]) -> list[dict]:
    """Bin queries by session position, compute median delta per bin."""
    bins = [(1, 5), (6, 10), (11, 15), (16, 20), (21, 999)]
    bin_labels = ["1-5", "6-10", "11-15", "16-20", "21+"]
    results = []
    for (lo, hi), label in zip(bins, bin_labels):
        bin_diffs = [d for d in diffs if lo <= d["query_seq"] + 1 <= hi]
        if bin_diffs:
            deltas = [d["latency_delta_ms"] for d in bin_diffs]
            deltas_pct = [d["latency_delta_pct"] for d in bin_diffs]
            changed = sum(1 for d in bin_diffs if d["plan_changed_any"])
            results.append({
                "bin": label,
                "n_queries": len(bin_diffs),
                "n_changed": changed,
                "median_delta_ms": round(statistics.median(deltas), 3),
                "median_delta_pct": round(statistics.median(deltas_pct), 2),
            })
    return results


def format_plan_tree_text(plan_json: Any, depth: int = 0) -> str:
    """Format a raw EXPLAIN JSON plan tree as indented text."""
    if isinstance(plan_json, list):
        plan = plan_json[0].get("Plan", {})
    else:
        plan = plan_json
    return _format_node(plan, depth)


def _format_node(node: dict, depth: int = 0) -> str:
    indent = "  " * depth
    nt = node.get("Node Type", "?")
    est = node.get("Plan Rows", "?")
    actual = node.get("Actual Rows", "?")
    time_ms = node.get("Actual Total Time", "?")
    alias = node.get("Alias", "")
    rel = node.get("Relation Name", "")
    jt = node.get("Join Type", "")
    strat = node.get("Strategy", "")

    parts = [f"{indent}{nt}"]
    if jt:
        parts[0] += f" [{jt}]"
    if strat:
        parts[0] += f" [{strat}]"
    if rel:
        parts[0] += f" on {rel}"
    if alias and alias != rel:
        parts[0] += f" ({alias})"

    # Predicates
    for key in ("Filter", "Hash Cond", "Join Filter", "Merge Cond", "Index Cond"):
        v = node.get(key)
        if v:
            parts[0] += f"  {key}: {v}"

    parts[0] += f"  (est={est} actual={actual} time={time_ms}ms)"

    lines = [parts[0]]
    for child in node.get("Plans", []):
        lines.append(_format_node(child, depth + 1))
    return "\n".join(lines)


def run_pilot():
    """Run the full pilot on flag-19."""
    STAGE35_DIR.mkdir(parents=True, exist_ok=True)

    # Load traces
    trace_files = sorted(TRACE_DIR.glob("flag-19_rep_*.jsonl"))
    trace_files = [f for f in trace_files if "_summary" not in f.name]
    print(f"Found {len(trace_files)} rep trace(s) for flag-19")

    all_diffs = []
    rep_results = []

    for trace_path in trace_files:
        rep_name = trace_path.stem  # e.g. flag-19_rep_a
        trace = load_trace(trace_path)
        successful = [e for e in trace if e["success"]]
        print(f"\n{'='*60}")
        print(f"Rep: {rep_name} ({len(successful)} successful queries)")
        print(f"{'='*60}")

        # Baseline
        print("\nRunning baseline condition...")
        baseline = run_baseline(successful)
        print(f"  Baseline done: {len(baseline)} queries")

        # Hinted
        print("Running hinted condition...")
        hinted = run_hinted(successful)
        hints_applied = sum(1 for r in hinted if r.get("hint_block"))
        print(f"  Hinted done: {len(hinted)} queries, {hints_applied} had hints injected")

        # Per-query diffs
        diffs = compute_per_query_diffs(baseline, hinted)
        agg = compute_rep_aggregates(diffs)

        rep_results.append({
            "rep": rep_name,
            "baseline": baseline,
            "hinted": hinted,
            "diffs": diffs,
            "aggregates": agg,
        })
        all_diffs.extend(diffs)

        print(f"\n  Aggregate: {json.dumps(agg, indent=2)}")

    # Overall aggregates
    overall_agg = compute_rep_aggregates(all_diffs) if all_diffs else {}
    binned = compute_binned_analysis(all_diffs) if all_diffs else []

    # Save full results
    output = {
        "task": "flag-19",
        "reps": [{
            "rep": r["rep"],
            "diffs": r["diffs"],
            "aggregates": r["aggregates"],
        } for r in rep_results],
        "overall": overall_agg,
        "binned_analysis": binned,
    }

    out_path = STAGE35_DIR / "pilot_results.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\nResults saved: {out_path}")

    # Save case studies (queries with plan changes)
    case_studies = []
    for rep in rep_results:
        b_by_seq = {r["query_seq"]: r for r in rep["baseline"]}
        h_by_seq = {r["query_seq"]: r for r in rep["hinted"]}
        for d in rep["diffs"]:
            seq = d["query_seq"]
            if d["plan_changed_any"] or d["hints_injected"]:
                b = b_by_seq[seq]
                h = h_by_seq[seq]
                case_studies.append({
                    "rep": rep["rep"],
                    "query_seq": seq,
                    "sql": b["sql"],
                    "baseline_plan_text": format_plan_tree_text(b["plan_json"]),
                    "hinted_plan_text": format_plan_tree_text(h["plan_json"]),
                    "diff": d,
                    "hints": h.get("applied_hints", []),
                    "hint_block": h.get("hint_block", ""),
                })

    cases_path = STAGE35_DIR / "case_studies.json"
    with open(cases_path, "w") as f:
        json.dump(case_studies, f, indent=2, default=str)
    print(f"Case studies saved: {cases_path}")

    return output, case_studies


if __name__ == "__main__":
    output, case_studies = run_pilot()
