#!/usr/bin/env python3
"""
Hinted condition replay for the task 2 pilot.

Replays the same task 2 traces with session-local cardinality feedback:
for each query, constructs Rows() hints from the hinted condition's own
prior plan trees, prepends them to the SQL, and measures latencies with
the 6-run discard-2 protocol.

Requires pg_hint_plan loaded via shared_preload_libraries.
Does NOT require sudo — Postgres restarts between reps must be done
externally. The script waits for Postgres to become available after
printing a restart prompt.
"""

import json
import statistics
import sys
import time
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from pg_plan_parser import extract_plan_tree
from hint_constructor import build_signature_history, construct_hints

CONNINFO = "host=localhost port=5434 dbname=agentic_imdb"
TRACE_DIR = Path(__file__).resolve().parent.parent / "traces"
PILOT_DIR = Path(__file__).resolve().parent

WARM_TABLES = [
    "title", "cast_info", "movie_info", "movie_info_idx", "name",
    "movie_companies", "keyword", "movie_keyword", "role_type",
    "info_type", "kind_type", "company_name", "company_type",
]

REPS = ["a", "b", "c"]
TASK = "task2"
RUNS_PER_QUERY = 6
DISCARD_RUNS = 2
STATEMENT_TIMEOUT = "600s"


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def wait_for_postgres(max_wait=120):
    """Wait for Postgres to accept connections after a restart."""
    log("Waiting for Postgres to accept connections...")
    start = time.time()
    while time.time() - start < max_wait:
        try:
            conn = psycopg.connect(CONNINFO, autocommit=True)
            conn.execute("SELECT 1")
            conn.close()
            log("Postgres is ready.")
            return True
        except Exception:
            time.sleep(1)
    log("ERROR: Postgres did not become available within timeout.")
    return False


def warm_cache(conn):
    log("Warming cache...")
    for table in WARM_TABLES:
        conn.execute(f"SELECT COUNT(*) FROM {table}")
    log("Cache warm.")


def load_trace(rep):
    path = TRACE_DIR / f"{TASK}_rep_{rep}.jsonl"
    entries = []
    for line in open(path):
        e = json.loads(line.strip())
        if e.get("success", False):
            entries.append(e)
    return entries


def run_explain_analyze(conn, sql):
    t0 = time.time()
    explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"
    row = conn.execute(explain_sql).fetchone()
    exec_ms = (time.time() - t0) * 1000
    plan_json = row[0]
    plan_tree = extract_plan_tree(plan_json)
    return plan_json, plan_tree, exec_ms


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
        if not any(kw in node.get("operator_type", "") for kw in ("Join", "Nested Loop")):
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


def construct_hints_for_query_deepest_first(sql, conn, plan_history):
    """
    Build hints using session-local history. Returns hints ordered deepest-first.
    """
    t0 = time.time()

    sig_history = build_signature_history(plan_history)

    if not sig_history:
        overhead_ms = (time.time() - t0) * 1000
        return sql, "", [], overhead_ms, []

    # Run EXPLAIN (no ANALYZE) to get vanilla plan
    explain_row = conn.execute(f"EXPLAIN (FORMAT JSON) {sql}").fetchone()
    vanilla_plan_json = explain_row[0]
    vanilla_tree = extract_plan_tree(vanilla_plan_json)

    # Build hints with depth info for ordering
    hints_with_depth = []
    applied = []
    skipped = []

    for node in vanilla_tree:
        node_type = node["operator_type"]
        if node_type not in ("Hash Join", "Merge Join", "Nested Loop"):
            continue
        aliases = node.get("relation_aliases", [])
        if len(aliases) < 2:
            continue

        sig = node["operator_signature"]
        if sig not in sig_history:
            continue

        match = sig_history[sig]
        # Depth proxy: number of relation aliases (more aliases = deeper join)
        depth = len(aliases)
        alias_str = " ".join(sorted(aliases))
        actual_card = match["actual_card"]
        hint_str = f"Rows({alias_str} #{actual_card})"

        hints_with_depth.append((depth, hint_str))
        applied.append({
            "signature": sig,
            "relation_aliases": aliases,
            "actual_card": actual_card,
            "source_query_idx": match["query_idx"],
            "hint_string": hint_str,
            "depth": depth,
        })

    # Sort deepest first (most aliases first)
    hints_with_depth.sort(key=lambda x: -x[0])

    if not hints_with_depth:
        overhead_ms = (time.time() - t0) * 1000
        return sql, "", applied, overhead_ms, skipped

    hint_block = "/*+ " + " ".join(h[1] for h in hints_with_depth) + " */"
    hinted_sql = f"{hint_block}\n{sql}"

    overhead_ms = (time.time() - t0) * 1000
    return hinted_sql, hint_block, applied, overhead_ms, skipped


def run_hinted():
    log("\n=== Hinted Condition ===")
    hinted_dir = PILOT_DIR / "hinted"
    hinted_dir.mkdir(parents=True, exist_ok=True)

    all_results = {}

    for rep_idx, rep in enumerate(REPS):
        log(f"\n--- Hinted rep {rep} ---")

        conn = psycopg.connect(CONNINFO, autocommit=True)
        conn.execute(f"SET statement_timeout = '{STATEMENT_TIMEOUT}'")
        warm_cache(conn)

        entries = load_trace(rep)
        rep_dir = hinted_dir / rep
        rep_dir.mkdir(parents=True, exist_ok=True)

        # Hinted condition's own plan history (starts empty each rep)
        plan_history = []
        rep_results = []

        for i, entry in enumerate(entries):
            sql = entry["raw_sql"]
            seq = entry.get("query_seq", i)
            log(f"  Q{seq}: {sql[:80]}...")

            try:
                # Construct hints from this rep's own prior plans
                hinted_sql, hint_block, applied_hints, overhead_ms, skipped = \
                    construct_hints_for_query_deepest_first(sql, conn, plan_history)

                n_hints = len(applied_hints)
                if hint_block:
                    log(f"    Hints: {n_hints} injected, overhead={overhead_ms:.1f}ms")
                    log(f"    Block: {hint_block[:120]}...")
                else:
                    log(f"    Hints: none (overhead={overhead_ms:.1f}ms)")

                # EXPLAIN ANALYZE with hints
                plan_json, plan_tree, explain_ms = run_explain_analyze(conn, hinted_sql)
                with open(rep_dir / f"plan_{seq}.json", "w") as f:
                    json.dump(plan_json, f, indent=2)

                # Add this query's plan to the hinted condition's history
                plan_history.append(plan_tree)

                # Timed runs with hints
                run_times = run_timed(conn, hinted_sql, RUNS_PER_QUERY)
                median_lat = median_of_kept(run_times, DISCARD_RUNS)

                qe_max, qe_node = max_qerror_depth3(plan_tree)

                result = {
                    "query_seq": seq,
                    "sql": sql,
                    "hint_block": hint_block,
                    "hinted_sql": hinted_sql,
                    "hints_injected": n_hints,
                    "hints_applied": applied_hints,
                    "hints_skipped": skipped,
                    "injection_overhead_ms": round(overhead_ms, 2),
                    "plan_tree_summary": [
                        {"node_id": n["node_id"], "operator_type": n["operator_type"],
                         "estimated_card": n["estimated_card"], "actual_card": n["actual_card"],
                         "relation_aliases": n["relation_aliases"],
                         "operator_signature": n["operator_signature"]}
                        for n in plan_tree
                    ],
                    "run_times_ms": run_times,
                    "median_latency_ms": round(median_lat, 2),
                    "q_error_max": round(qe_max, 2),
                    "success": True,
                    "error": None,
                }
                log(f"    median={median_lat:.1f}ms, qe_max={qe_max:.1f}, runs={[round(t,1) for t in run_times]}")

            except Exception as e:
                log(f"    FAILED: {e}")
                # Still add empty plan to history to maintain sequence alignment
                plan_history.append([])
                result = {
                    "query_seq": seq, "sql": sql,
                    "hint_block": "", "hinted_sql": sql,
                    "hints_injected": 0, "hints_applied": [],
                    "hints_skipped": [],
                    "injection_overhead_ms": 0,
                    "plan_tree_summary": [], "run_times_ms": [],
                    "median_latency_ms": None, "q_error_max": None,
                    "success": False, "error": str(e),
                }

            with open(rep_dir / f"timings_{seq}.json", "w") as f:
                json.dump(result, f, indent=2)
            rep_results.append(result)

        conn.close()

        summary = {
            "condition": "hinted", "rep": rep,
            "num_queries": len(rep_results),
            "results": rep_results,
        }
        with open(hinted_dir / f"summary_{rep}.json", "w") as f:
            json.dump(summary, f, indent=2)
        all_results[rep] = rep_results
        log(f"  Rep {rep} complete: {len(rep_results)} queries")

    return all_results


def hinted_report(all_results):
    log("\n=== Hinted Report ===")
    lines = ["# Hinted Replay Report: Task 2 (IMDB)", ""]
    lines.append("## Per-Query Results")
    lines.append("")
    lines.append("| Rep | Seq | Hints | Median (ms) | QE Max | Overhead (ms) | Run Times (ms) |")
    lines.append("|-----|-----|-------|-------------|--------|---------------|----------------|")

    for rep in REPS:
        for r in all_results[rep]:
            if r["success"]:
                times_str = ", ".join(f"{t:.0f}" for t in r["run_times_ms"])
                lines.append(
                    f"| {rep} | {r['query_seq']} | {r['hints_injected']} "
                    f"| {r['median_latency_ms']:.1f} | {r['q_error_max']:.0f} "
                    f"| {r['injection_overhead_ms']:.1f} | {times_str} |"
                )
            else:
                lines.append(f"| {rep} | {r['query_seq']} | - | FAIL | - | - | {r.get('error','')} |")

    lines.append("")
    lines.append("## Per-Rep Totals")
    lines.append("")
    lines.append("| Rep | Queries | Total Hinted (ms) | Mean (ms) | Median (ms) | Total Overhead (ms) |")
    lines.append("|-----|---------|-------------------|-----------| ------------|---------------------|")
    for rep in REPS:
        results = [r for r in all_results[rep] if r["success"]]
        lats = [r["median_latency_ms"] for r in results]
        overheads = [r["injection_overhead_ms"] for r in results]
        total = sum(lats)
        mean = statistics.mean(lats) if lats else 0
        med = statistics.median(lats) if lats else 0
        total_oh = sum(overheads)
        lines.append(f"| {rep} | {len(results)} | {total:.0f} | {mean:.0f} | {med:.0f} | {total_oh:.1f} |")

    lines.append("")

    report_path = PILOT_DIR / "hinted_report.md"
    with open(report_path, "w") as f:
        f.write("\n".join(lines))
    log(f"Report saved to {report_path}")


def main():
    PILOT_DIR.mkdir(parents=True, exist_ok=True)

    log("=" * 60)
    log("HINTED REPLAY: TASK 2")
    log("=" * 60)

    # Verify pg_hint_plan is available (loaded via shared_preload_libraries)
    try:
        conn = psycopg.connect(CONNINFO, autocommit=True)
        row = conn.execute("SHOW pg_hint_plan.enable_hint").fetchone()
        assert row[0] == "on", f"pg_hint_plan.enable_hint is {row[0]}, expected 'on'"
        log(f"pg_hint_plan verified: enable_hint={row[0]}")
        conn.close()
    except Exception as e:
        log(f"FATAL: pg_hint_plan not available: {e}")
        sys.exit(1)

    all_results = run_hinted()
    hinted_report(all_results)

    log("\n=== HINTED REPLAY COMPLETE ===")


if __name__ == "__main__":
    main()
