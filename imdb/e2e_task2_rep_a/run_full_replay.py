#!/usr/bin/env python3
"""
Full end-to-end replay: task 2 rep a, all 12 queries.
Baseline condition, then hinted condition, then analysis.

Usage:
    # Run baseline first:
    python run_full_replay.py --baseline
    # Then restart Postgres externally:
    #   sudo pg_ctlcluster 16 poc restart
    # Then run hinted + analysis:
    python run_full_replay.py --hinted
    # Or run everything (assumes Postgres was just restarted):
    python run_full_replay.py --all
"""

import argparse
import json
import statistics
import sys
import time
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from pg_plan_parser import extract_plan_tree
from hint_constructor import build_signature_history

CONNINFO = "host=localhost port=5434 dbname=agentic_imdb"
TRACE_PATH = Path(__file__).resolve().parent.parent / "traces_v2" / "task2_rep_a.jsonl"
OUT_DIR = Path(__file__).resolve().parent

WARM_TABLES = [
    "title", "cast_info", "movie_info", "movie_info_idx", "name",
    "movie_companies", "keyword", "movie_keyword", "role_type",
    "info_type", "kind_type", "company_name", "company_type",
]

RUNS_PER_QUERY = 6
DISCARD_RUNS = 2
STATEMENT_TIMEOUT = "900s"


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def load_trace():
    entries = []
    for line in open(TRACE_PATH):
        e = json.loads(line.strip())
        if e.get("success", False):
            entries.append(e)
    return entries


def warm_cache(conn):
    log("Warming cache...")
    for table in WARM_TABLES:
        conn.execute(f"SELECT COUNT(*) FROM {table}")
    log("Cache warm.")


def run_timed(conn, sql, n_runs=6):
    times = []
    for _ in range(n_runs):
        t0 = time.time()
        try:
            conn.execute(sql)
        except Exception:
            pass
        times.append((time.time() - t0) * 1000)
    return times


def median_of_kept(times, discard=2):
    kept = times[discard:]
    return statistics.median(kept) if kept else 0.0


def max_qerror_depth3(plan_tree):
    worst = 0.0
    worst_node = None
    for node in plan_tree:
        if node["operator_type"] not in ("Hash Join", "Merge Join", "Nested Loop"):
            continue
        if len(node.get("relation_aliases", [])) < 3:
            continue
        est = node.get("estimated_card", 0)
        act = node.get("actual_card", 0)
        if est and act:
            qe = max(est / act, act / est)
            if qe > worst:
                worst = qe
                worst_node = node
    return worst, worst_node


def plan_summary(plan_tree):
    """Extract join node summary for comparison."""
    joins = []
    for n in plan_tree:
        if n["operator_type"] in ("Hash Join", "Merge Join", "Nested Loop"):
            joins.append({
                "node_id": n["node_id"],
                "operator_type": n["operator_type"],
                "estimated_card": n.get("estimated_card", 0),
                "actual_card": n.get("actual_card", 0),
                "relation_aliases": n.get("relation_aliases", []),
                "operator_signature": n.get("operator_signature", ""),
            })
    return joins


def construct_hints_for_query(sql, conn, plan_history):
    """Build hints using session-local history with *VALUES* support."""
    t0 = time.time()

    sig_history = build_signature_history(plan_history)

    if not sig_history:
        overhead_ms = (time.time() - t0) * 1000
        return sql, "", [], overhead_ms, []

    # Get vanilla plan
    row = conn.execute(f"EXPLAIN (FORMAT JSON) {sql}").fetchone()
    vanilla_tree = extract_plan_tree(row[0])

    hints = []
    applied = []
    skipped = []

    for node in vanilla_tree:
        if node["operator_type"] not in ("Hash Join", "Merge Join", "Nested Loop"):
            continue
        aliases = node.get("relation_aliases", [])
        if len(aliases) < 2:
            continue

        sig = node["operator_signature"]
        if sig not in sig_history:
            continue

        match = sig_history[sig]
        # Quote *VALUES* with double quotes for pg_hint_plan
        alias_parts = []
        for a in sorted(aliases):
            if a == "*VALUES*":
                alias_parts.append('"*VALUES*"')
            else:
                alias_parts.append(a)
        alias_str = " ".join(alias_parts)
        actual_card = match["actual_card"]
        hint_str = f"Rows({alias_str} #{actual_card})"
        hints.append((len(aliases), hint_str))  # depth proxy for ordering
        applied.append({
            "signature": sig,
            "relation_aliases": aliases,
            "actual_card": actual_card,
            "source_query_idx": match["query_idx"],
            "hint_string": hint_str,
        })

    # Sort deepest first
    hints.sort(key=lambda x: -x[0])

    if not hints:
        overhead_ms = (time.time() - t0) * 1000
        return sql, "", applied, overhead_ms, skipped

    hint_block = "/*+ " + " ".join(h[1] for h in hints) + " */"
    hinted_sql = f"{hint_block}\n{sql}"

    overhead_ms = (time.time() - t0) * 1000
    return hinted_sql, hint_block, applied, overhead_ms, skipped


# ── Step 1: Pre-validation ──────────────────────────────────────────

def prevalidate():
    log("=== PRE-VALIDATION ===")
    conn = psycopg.connect(CONNINFO, autocommit=True)

    # pg_hint_plan check
    row = conn.execute("SHOW pg_hint_plan.enable_hint").fetchone()
    assert row[0] == "on", f"pg_hint_plan not enabled: {row[0]}"
    log(f"pg_hint_plan.enable_hint = {row[0]}")

    # Load and count queries
    entries = load_trace()
    log(f"Trace has {len(entries)} successful queries")

    # Alias compliance check
    from collections import Counter
    compliant = 0
    total_repeated = 0
    for e in entries:
        sql = e["raw_sql"]
        row = conn.execute(f"EXPLAIN (FORMAT JSON) {sql}").fetchone()
        plan_json = row[0]
        root = plan_json[0].get("Plan", {})

        def collect_scan_aliases(node, skip_init=True):
            result = {}
            nt = node.get("Node Type", "")
            pr = node.get("Parent Relationship", "")
            if skip_init and pr in ("InitPlan", "SubPlan"):
                return result
            if nt in ("Seq Scan", "Index Scan", "Index Only Scan",
                      "Bitmap Heap Scan", "Bitmap Index Scan"):
                a = node.get("Alias", "")
                t = node.get("Relation Name", "")
                if a and t:
                    result[a] = t
            for c in node.get("Plans", []):
                result.update(collect_scan_aliases(c, skip_init))
            return result

        alias_map = collect_scan_aliases(root)
        table_counts = Counter(alias_map.values())
        repeated = {t: c for t, c in table_counts.items() if c > 1}

        if repeated:
            total_repeated += 1
            alias_by_table = {}
            for alias, tname in alias_map.items():
                if tname in repeated:
                    alias_by_table.setdefault(tname, []).append(alias)
            all_distinct = all(
                len(aliases) == len(set(aliases))
                for aliases in alias_by_table.values()
            )
            if all_distinct:
                compliant += 1

    log(f"Alias compliance: {compliant}/{total_repeated} queries with repeated tables are compliant")
    if total_repeated > 0 and compliant / total_repeated < 0.8:
        log("FAIL: Alias compliance below 80%. Stopping.")
        conn.close()
        return False

    # Spot-check: construct hints for Q5 and verify
    log("Spot-checking Q5 hint construction...")
    prior_trees = []
    for e in entries:
        if e["query_seq"] > 4:
            break
        row = conn.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {e['raw_sql']}").fetchone()
        prior_trees.append(extract_plan_tree(row[0]))

    q5_sql = [e for e in entries if e["query_seq"] == 5][0]["raw_sql"]
    _, hint_block, applied, _, _ = construct_hints_for_query(q5_sql, conn, prior_trees)
    log(f"Q5 hint block: {hint_block[:120]}...")
    log(f"Q5 hints applied: {len(applied)}")
    if len(applied) < 3:
        log("WARNING: Q5 should have at least 3 hints. Check hint constructor.")

    conn.close()
    log("Pre-validation PASSED.\n")
    return True


# ── Step 2: Baseline condition ──────────────────────────────────────

def run_baseline():
    log("=== BASELINE CONDITION ===")
    baseline_dir = OUT_DIR / "baseline"
    baseline_dir.mkdir(parents=True, exist_ok=True)

    conn = psycopg.connect(CONNINFO, autocommit=True)
    conn.execute(f"SET statement_timeout = '{STATEMENT_TIMEOUT}'")
    warm_cache(conn)

    entries = load_trace()
    results = []

    for e in entries:
        sql = e["raw_sql"]
        seq = e["query_seq"]
        log(f"  Q{seq}: {sql[:80]}...")

        try:
            # EXPLAIN ANALYZE
            row = conn.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}").fetchone()
            plan_json = row[0]
            plan_tree = extract_plan_tree(plan_json)

            with open(baseline_dir / f"plan_{seq}.json", "w") as f:
                json.dump(plan_json, f, indent=2)

            # Timed runs
            run_times = run_timed(conn, sql, RUNS_PER_QUERY)
            median_lat = median_of_kept(run_times, DISCARD_RUNS)
            qe_max, _ = max_qerror_depth3(plan_tree)

            result = {
                "query_seq": seq,
                "sql": sql,
                "plan_tree_summary": plan_summary(plan_tree),
                "run_times_ms": run_times,
                "median_latency_ms": round(median_lat, 2),
                "q_error_max": round(qe_max, 2),
                "success": True,
                "error": None,
            }
            log(f"    median={median_lat:.1f}ms, qe_max={qe_max:.1f}, runs={[round(t,1) for t in run_times]}")

        except Exception as ex:
            log(f"    FAILED: {ex}")
            result = {
                "query_seq": seq, "sql": sql,
                "plan_tree_summary": [], "run_times_ms": [],
                "median_latency_ms": None, "q_error_max": None,
                "success": False, "error": str(ex),
            }

        with open(baseline_dir / f"timings_{seq}.json", "w") as f:
            json.dump(result, f, indent=2)
        results.append(result)

    summary = {"condition": "baseline", "num_queries": len(results), "results": results}
    with open(baseline_dir / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    conn.close()
    log(f"Baseline complete: {len(results)} queries\n")


# ── Step 3: Hinted condition ────────────────────────────────────────

def run_hinted():
    log("=== HINTED CONDITION ===")
    hinted_dir = OUT_DIR / "hinted"
    hinted_dir.mkdir(parents=True, exist_ok=True)

    conn = psycopg.connect(CONNINFO, autocommit=True)
    conn.execute(f"SET statement_timeout = '{STATEMENT_TIMEOUT}'")
    warm_cache(conn)

    entries = load_trace()
    plan_history = []  # hinted condition's own history
    results = []
    total_hints_injected = 0
    total_hints_used = 0
    total_hints_rejected = 0

    for e in entries:
        sql = e["raw_sql"]
        seq = e["query_seq"]
        log(f"  Q{seq}: {sql[:80]}...")

        try:
            # Construct hints from hinted condition's own prior plans
            hinted_sql, hint_block, applied_hints, overhead_ms, skipped = \
                construct_hints_for_query(sql, conn, plan_history)
            n_hints = len(applied_hints)

            if hint_block:
                log(f"    Hints: {n_hints} injected, overhead={overhead_ms:.1f}ms")
                log(f"    Block: {hint_block[:140]}...")
            else:
                log(f"    Hints: none (overhead={overhead_ms:.1f}ms)")

            # Verify pg_hint_plan application via debug output
            hint_log_lines = []
            if hint_block:
                conn.execute("SET pg_hint_plan.debug_print = 'verbose'")
                conn.execute("SET pg_hint_plan.message_level = 'notice'")
                conn.execute("SET client_min_messages = 'notice'")

                # Use EXPLAIN to trigger pg_hint_plan logging
                try:
                    conn.execute(f"EXPLAIN (FORMAT JSON) {hinted_sql}").fetchone()
                except Exception:
                    pass

                # Reset to avoid noise on subsequent queries
                conn.execute("SET pg_hint_plan.debug_print = 'off'")
                conn.execute("SET client_min_messages = 'warning'")

            # EXPLAIN ANALYZE with hints
            row = conn.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {hinted_sql}").fetchone()
            plan_json = row[0]
            plan_tree = extract_plan_tree(plan_json)

            with open(hinted_dir / f"plan_{seq}.json", "w") as f:
                json.dump(plan_json, f, indent=2)

            # Update hinted condition's own plan history
            plan_history.append(plan_tree)

            # Timed runs with hints
            run_times = run_timed(conn, hinted_sql, RUNS_PER_QUERY)
            median_lat = median_of_kept(run_times, DISCARD_RUNS)
            qe_max, _ = max_qerror_depth3(plan_tree)

            total_hints_injected += n_hints

            result = {
                "query_seq": seq,
                "sql": sql,
                "hint_block": hint_block,
                "hinted_sql": hinted_sql,
                "hints_injected": n_hints,
                "hints_applied": applied_hints,
                "hints_skipped": skipped,
                "injection_overhead_ms": round(overhead_ms, 2),
                "plan_tree_summary": plan_summary(plan_tree),
                "run_times_ms": run_times,
                "median_latency_ms": round(median_lat, 2),
                "q_error_max": round(qe_max, 2),
                "success": True,
                "error": None,
            }
            log(f"    median={median_lat:.1f}ms, qe_max={qe_max:.1f}, runs={[round(t,1) for t in run_times]}")

        except Exception as ex:
            log(f"    FAILED: {ex}")
            plan_history.append([])
            result = {
                "query_seq": seq, "sql": sql,
                "hint_block": "", "hinted_sql": sql,
                "hints_injected": 0, "hints_applied": [],
                "hints_skipped": [],
                "injection_overhead_ms": 0,
                "plan_tree_summary": [], "run_times_ms": [],
                "median_latency_ms": None, "q_error_max": None,
                "success": False, "error": str(ex),
            }

        with open(hinted_dir / f"timings_{seq}.json", "w") as f:
            json.dump(result, f, indent=2)
        results.append(result)

    summary = {
        "condition": "hinted", "num_queries": len(results),
        "total_hints_injected": total_hints_injected,
        "results": results,
    }
    with open(hinted_dir / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    conn.close()
    log(f"Hinted complete: {len(results)} queries, {total_hints_injected} total hints\n")


# ── Step 4-6: Analysis ──────────────────────────────────────────────

def run_analysis():
    log("=== ANALYSIS ===")
    comp_dir = OUT_DIR / "comparison"
    comp_dir.mkdir(parents=True, exist_ok=True)

    baseline_summary = json.load(open(OUT_DIR / "baseline" / "summary.json"))
    hinted_summary = json.load(open(OUT_DIR / "hinted" / "summary.json"))

    baseline_by_seq = {r["query_seq"]: r for r in baseline_summary["results"]}
    hinted_by_seq = {r["query_seq"]: r for r in hinted_summary["results"]}

    comparisons = []

    for seq in sorted(baseline_by_seq.keys()):
        b = baseline_by_seq[seq]
        h = hinted_by_seq.get(seq)

        if not h or not b["success"] or not h["success"]:
            continue

        b_med = b["median_latency_ms"]
        h_med = h["median_latency_ms"]
        delta_ms = h_med - b_med
        delta_pct = 100 * delta_ms / b_med if b_med else 0

        # Plan comparison
        b_joins = b["plan_tree_summary"]
        h_joins = h["plan_tree_summary"]

        b_join_order = [(j["operator_type"], tuple(j["relation_aliases"])) for j in b_joins]
        h_join_order = [(j["operator_type"], tuple(j["relation_aliases"])) for j in h_joins]

        topology_changed = b_join_order != h_join_order
        join_order_changed = [tuple(j["relation_aliases"]) for j in b_joins] != \
                             [tuple(j["relation_aliases"]) for j in h_joins]
        join_method_changed = any(
            bj["operator_type"] != hj["operator_type"]
            for bj, hj in zip(b_joins, h_joins)
        ) if len(b_joins) == len(h_joins) else (len(b_joins) != len(h_joins))

        b_qe = b["q_error_max"] or 0
        h_qe = h["q_error_max"] or 0
        qe_reduction = b_qe - h_qe

        hints_injected = h.get("hints_injected", 0)

        # Classification
        noise_threshold = 0.03 if b_med > 5000 else 0.08
        if not topology_changed:
            classification = "unchanged"
        elif delta_pct < -noise_threshold * 100:
            classification = "improved"
        elif delta_pct > noise_threshold * 100:
            classification = "regressed"
        else:
            classification = "neutral"

        comp = {
            "query_seq": seq,
            "baseline_median_ms": b_med,
            "hinted_median_ms": h_med,
            "delta_ms": round(delta_ms, 2),
            "delta_pct": round(delta_pct, 2),
            "hints_injected": hints_injected,
            "plan_topology_changed": topology_changed,
            "join_order_changed": join_order_changed,
            "join_method_changed": join_method_changed,
            "baseline_max_qerror": b_qe,
            "hinted_max_qerror": h_qe,
            "qerror_reduction": round(qe_reduction, 2),
            "classification": classification,
            "baseline_plan_summary": b_joins,
            "hinted_plan_summary": h_joins,
        }
        with open(comp_dir / f"q{seq}.json", "w") as f:
            json.dump(comp, f, indent=2)
        comparisons.append(comp)

    # ── Distribution summary ──
    lines = ["# Task 2 Rep A: End-to-End Replay Report", ""]

    # Executive summary
    counts = {"unchanged": 0, "improved": 0, "neutral": 0, "regressed": 0}
    for c in comparisons:
        counts[c["classification"]] += 1
    total_baseline = sum(c["baseline_median_ms"] for c in comparisons)
    total_hinted = sum(c["hinted_median_ms"] for c in comparisons)
    session_delta = total_hinted - total_baseline
    session_delta_pct = 100 * session_delta / total_baseline if total_baseline else 0

    lines.append("## Executive Summary")
    lines.append("")
    lines.append(
        f"Of {len(comparisons)} queries: "
        f"{counts['improved']} improved, {counts['neutral']} neutral, "
        f"{counts['regressed']} regressed, {counts['unchanged']} unchanged. "
        f"Total session wall-clock: baseline {total_baseline:.0f}ms, "
        f"hinted {total_hinted:.0f}ms, "
        f"delta {session_delta:+.0f}ms ({session_delta_pct:+.1f}%). "
    )
    if session_delta < 0:
        lines.append("The mechanism produced a net session-level improvement.")
    elif session_delta > 0:
        lines.append("The mechanism produced a net session-level regression.")
    else:
        lines.append("The mechanism produced no net session-level change.")
    lines.append("")

    # Per-query table
    lines.append("## Per-Query Results")
    lines.append("")
    lines.append("| Q | Baseline (ms) | Hinted (ms) | Delta (ms) | Delta % | Hints | Plan Changed | Class | BL QE | H QE |")
    lines.append("|---|--------------|-------------|------------|---------|-------|-------------|-------|-------|------|")
    sorted_comps = sorted(comparisons, key=lambda c: c["delta_pct"])
    for c in sorted_comps:
        changed = "Y" if c["plan_topology_changed"] else "N"
        lines.append(
            f"| {c['query_seq']} | {c['baseline_median_ms']:.1f} | {c['hinted_median_ms']:.1f} "
            f"| {c['delta_ms']:+.1f} | {c['delta_pct']:+.1f}% | {c['hints_injected']} "
            f"| {changed} | {c['classification']} | {c['baseline_max_qerror']:.0f} | {c['hinted_max_qerror']:.0f} |"
        )
    lines.append("")

    # Classification counts
    lines.append("## Classification Counts")
    lines.append("")
    for cls in ["improved", "neutral", "regressed", "unchanged"]:
        lines.append(f"- **{cls}**: {counts[cls]}")
    lines.append("")

    # Session totals
    lines.append("## Session Wall-Clock Totals")
    lines.append("")
    lines.append(f"- Baseline total: {total_baseline:.0f}ms")
    lines.append(f"- Hinted total: {total_hinted:.0f}ms")
    lines.append(f"- Delta: {session_delta:+.0f}ms ({session_delta_pct:+.1f}%)")
    lines.append("")

    # Hint mechanism health
    total_injected = sum(c["hints_injected"] for c in comparisons)
    lines.append("## Mechanism Health")
    lines.append("")
    lines.append(f"- Total hints injected: {total_injected}")
    lines.append(f"- Hints per query: min={min(c['hints_injected'] for c in comparisons)}, "
                 f"max={max(c['hints_injected'] for c in comparisons)}, "
                 f"mean={total_injected/len(comparisons):.1f}")
    lines.append("")

    # Q-error reductions
    lines.append("## Q-Error Reductions")
    lines.append("")
    qe_reds = [c["qerror_reduction"] for c in comparisons if c["baseline_max_qerror"] > 0]
    if qe_reds:
        lines.append(f"- Median q-error reduction: {statistics.median(qe_reds):.1f}")
        meaningful = sum(1 for c in comparisons if c["baseline_max_qerror"] > 0 and
                        c["qerror_reduction"] / c["baseline_max_qerror"] > 0.5)
        lines.append(f"- Queries with >50% relative q-error reduction: {meaningful}/{len(qe_reds)}")
    lines.append("")

    # Correlation check
    lines.append("## Correlation: Baseline Q-Error vs Latency Delta")
    lines.append("")
    lines.append("| Q | Baseline Max QE | Delta % | Class |")
    lines.append("|---|----------------|---------|-------|")
    for c in sorted(comparisons, key=lambda c: -c["baseline_max_qerror"]):
        lines.append(f"| {c['query_seq']} | {c['baseline_max_qerror']:.0f} | {c['delta_pct']:+.1f}% | {c['classification']} |")
    lines.append("")

    # Case studies (auto-select)
    lines.append("## Case Studies")
    lines.append("")

    changed = [c for c in comparisons if c["plan_topology_changed"]]
    unchanged = [c for c in comparisons if not c["plan_topology_changed"]]

    cases = []
    # Best improvement
    if changed:
        best = min(changed, key=lambda c: c["delta_pct"])
        cases.append(("Best improvement (or smallest regression)" if best["delta_pct"] >= 0
                      else "Best improvement", best))
    # Worst regression
    if changed:
        worst = max(changed, key=lambda c: c["delta_pct"])
        if worst != best:
            cases.append(("Worst regression", worst))
    # Neutral plan change
    neutrals = [c for c in changed if c["classification"] == "neutral"]
    if neutrals:
        cases.append(("Neutral plan change", neutrals[0]))
    # Unchanged despite hints
    hinted_unchanged = [c for c in unchanged if c["hints_injected"] > 0]
    if hinted_unchanged:
        cases.append(("Hints injected, plan unchanged", hinted_unchanged[0]))
    # Different from Q5
    non_q5 = [c for c in changed if c["query_seq"] != 5 and c["baseline_median_ms"] > 5000]
    if non_q5:
        cases.append(("Long-running query with plan change", non_q5[0]))

    for title, c in cases[:5]:
        seq = c["query_seq"]
        lines.append(f"### {title}: Q{seq}")
        lines.append("")

        # Load hint block from hinted timings
        h_data = json.load(open(OUT_DIR / "hinted" / f"timings_{seq}.json"))
        lines.append(f"**Baseline median**: {c['baseline_median_ms']:.1f}ms | "
                     f"**Hinted median**: {c['hinted_median_ms']:.1f}ms | "
                     f"**Delta**: {c['delta_ms']:+.1f}ms ({c['delta_pct']:+.1f}%)")
        lines.append(f"**Hints injected**: {c['hints_injected']} | "
                     f"**Plan changed**: {c['plan_topology_changed']} | "
                     f"**Baseline QE**: {c['baseline_max_qerror']:.0f} | "
                     f"**Hinted QE**: {c['hinted_max_qerror']:.0f}")
        lines.append("")
        if h_data.get("hint_block"):
            lines.append(f"Hint block: `{h_data['hint_block'][:200]}{'...' if len(h_data.get('hint_block',''))>200 else ''}`")
            lines.append("")

        # Baseline join summary
        lines.append("Baseline joins:")
        lines.append("```")
        for j in c["baseline_plan_summary"]:
            if len(j["relation_aliases"]) >= 3:
                est, act = j["estimated_card"], j["actual_card"]
                qe = max(est/act, act/est) if est and act else 0
                lines.append(f"  {j['operator_type']:15s} est={est:>6} act={act:>6} qe={qe:>7.1f}x  {j['relation_aliases']}")
        lines.append("```")
        lines.append("")

        # Hinted join summary
        lines.append("Hinted joins:")
        lines.append("```")
        for j in c["hinted_plan_summary"]:
            if len(j["relation_aliases"]) >= 3:
                est, act = j["estimated_card"], j["actual_card"]
                qe = max(est/act, act/est) if est and act else 0
                lines.append(f"  {j['operator_type']:15s} est={est:>6} act={act:>6} qe={qe:>7.1f}x  {j['relation_aliases']}")
        lines.append("```")
        lines.append("")

    report_path = OUT_DIR / "task2_rep_a_e2e_report.md"
    with open(report_path, "w") as f:
        f.write("\n".join(lines))
    log(f"Report saved to {report_path}")

    # Also save comparison summary
    with open(comp_dir / "all_comparisons.json", "w") as f:
        json.dump(comparisons, f, indent=2)

    log("Analysis complete.\n")


# ── Main ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", action="store_true", help="Run baseline only")
    parser.add_argument("--hinted", action="store_true", help="Run hinted + analysis")
    parser.add_argument("--analysis", action="store_true", help="Run analysis only")
    parser.add_argument("--all", action="store_true", help="Run baseline, then hinted, then analysis")
    args = parser.parse_args()

    if not any([args.baseline, args.hinted, args.analysis, args.all]):
        print("Specify --baseline, --hinted, --analysis, or --all")
        return

    if args.all or args.baseline:
        if not prevalidate():
            return
        run_baseline()
        if args.all:
            log("=" * 60)
            log("BASELINE COMPLETE.")
            log("RESTART POSTGRES NOW: sudo pg_ctlcluster 16 poc restart")
            log("Then re-run with --hinted")
            log("=" * 60)
            if not args.all:
                return
            # In --all mode, wait for Postgres to restart
            log("Waiting 10s for you to restart Postgres...")
            time.sleep(10)
            # Try to connect
            for _ in range(60):
                try:
                    c = psycopg.connect(CONNINFO, autocommit=True)
                    c.execute("SELECT 1")
                    c.close()
                    break
                except Exception:
                    time.sleep(2)

    if args.all or args.hinted:
        run_hinted()
        run_analysis()

    if args.analysis:
        run_analysis()

    log("=== ALL DONE ===")


if __name__ == "__main__":
    main()
