#!/usr/bin/env python3
"""
Baseline-only replay for task 2.
No pg_hint_plan, no Postgres restart (no sudo needed).
Just measures baseline latencies with 6-run discard-2 protocol.
"""

import json
import statistics
import sys
import time
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from pg_plan_parser import extract_plan_tree

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


def validate():
    log("=== Validation ===")
    conn = psycopg.connect(CONNINFO, autocommit=True)

    log("Row counts:")
    expected = {
        "cast_info": 36_244_344, "title": 2_528_312,
        "movie_info": 14_835_720, "movie_info_idx": 1_380_035,
        "name": 4_167_491, "movie_companies": 2_609_129,
        "keyword": 134_170, "movie_keyword": 4_523_930,
    }
    validation = {"row_counts": {}, "indexes": [], "traces": {}}
    for table, exp in expected.items():
        cnt = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        validation["row_counts"][table] = cnt
        status = "OK" if abs(cnt - exp) / exp < 0.01 else f"MISMATCH (expected ~{exp})"
        log(f"  {table}: {cnt:,} {status}")

    log("FK indexes:")
    idx_rows = conn.execute("""
        SELECT tablename, indexname FROM pg_indexes
        WHERE schemaname = 'public'
        ORDER BY tablename, indexname
    """).fetchall()
    idx_by_table = {}
    for table, idx in idx_rows:
        idx_by_table.setdefault(table, []).append(idx)
    for t in ["cast_info", "movie_info", "movie_info_idx", "movie_keyword", "movie_companies"]:
        idxs = idx_by_table.get(t, [])
        log(f"  {t}: {len(idxs)} indexes: {', '.join(idxs[:5])}")

    log("Traces:")
    for rep in REPS:
        entries = load_trace(rep)
        validation["traces"][rep] = len(entries)
        log(f"  rep {rep}: {len(entries)} successful queries")

    conn.close()
    with open(PILOT_DIR / "validation.json", "w") as f:
        json.dump(validation, f, indent=2)
    return validation


def run_baseline():
    log("\n=== Baseline Condition ===")
    baseline_dir = PILOT_DIR / "baseline"
    baseline_dir.mkdir(parents=True, exist_ok=True)

    all_results = {}

    for rep in REPS:
        log(f"\n--- Baseline rep {rep} ---")

        conn = psycopg.connect(CONNINFO, autocommit=True)
        conn.execute(f"SET statement_timeout = '{STATEMENT_TIMEOUT}'")
        warm_cache(conn)

        entries = load_trace(rep)
        rep_dir = baseline_dir / rep
        rep_dir.mkdir(parents=True, exist_ok=True)

        rep_results = []

        for i, entry in enumerate(entries):
            sql = entry["raw_sql"]
            seq = entry.get("query_seq", i)
            log(f"  Q{seq}: {sql[:80]}...")

            try:
                plan_json, plan_tree, explain_ms = run_explain_analyze(conn, sql)
                with open(rep_dir / f"plan_{seq}.json", "w") as f:
                    json.dump(plan_json, f, indent=2)

                run_times = run_timed(conn, sql, RUNS_PER_QUERY)
                median_lat = median_of_kept(run_times, DISCARD_RUNS)

                qe_max, qe_node = max_qerror_depth3(plan_tree)

                result = {
                    "query_seq": seq,
                    "sql": sql,
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
                result = {
                    "query_seq": seq, "sql": sql,
                    "plan_tree_summary": [], "run_times_ms": [],
                    "median_latency_ms": None, "q_error_max": None,
                    "success": False, "error": str(e),
                }

            with open(rep_dir / f"timings_{seq}.json", "w") as f:
                json.dump(result, f, indent=2)
            rep_results.append(result)

        conn.close()

        summary = {
            "condition": "baseline", "rep": rep,
            "num_queries": len(rep_results),
            "results": rep_results,
        }
        with open(baseline_dir / f"summary_{rep}.json", "w") as f:
            json.dump(summary, f, indent=2)
        all_results[rep] = rep_results
        log(f"  Rep {rep} complete: {len(rep_results)} queries")

    return all_results


def baseline_report(all_results):
    log("\n=== Baseline Report ===")
    lines = ["# Baseline Replay Report: Task 2 (IMDB)", ""]
    lines.append("## Per-Query Results")
    lines.append("")
    lines.append("| Rep | Seq | Median (ms) | QE Max | Run Times (ms) |")
    lines.append("|-----|-----|-------------|--------|----------------|")

    for rep in REPS:
        for r in all_results[rep]:
            if r["success"]:
                times_str = ", ".join(f"{t:.0f}" for t in r["run_times_ms"])
                lines.append(f"| {rep} | {r['query_seq']} | {r['median_latency_ms']:.1f} | {r['q_error_max']:.0f} | {times_str} |")
            else:
                lines.append(f"| {rep} | {r['query_seq']} | FAIL | - | {r.get('error','')} |")

    lines.append("")
    lines.append("## Per-Rep Totals")
    lines.append("")
    lines.append("| Rep | Queries | Total Baseline (ms) | Mean (ms) | Median (ms) |")
    lines.append("|-----|---------|--------------------|-----------| ------------|")
    for rep in REPS:
        results = [r for r in all_results[rep] if r["success"]]
        lats = [r["median_latency_ms"] for r in results]
        total = sum(lats)
        mean = statistics.mean(lats) if lats else 0
        med = statistics.median(lats) if lats else 0
        lines.append(f"| {rep} | {len(results)} | {total:.0f} | {mean:.0f} | {med:.0f} |")

    lines.append("")

    report_path = PILOT_DIR / "baseline_report.md"
    with open(report_path, "w") as f:
        f.write("\n".join(lines))
    log(f"Report saved to {report_path}")


def main():
    PILOT_DIR.mkdir(parents=True, exist_ok=True)

    log("=" * 60)
    log("BASELINE REPLAY: TASK 2")
    log("=" * 60)

    validate()
    all_results = run_baseline()
    baseline_report(all_results)

    log("\n=== BASELINE COMPLETE ===")


if __name__ == "__main__":
    main()
