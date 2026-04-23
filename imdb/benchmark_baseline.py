#!/usr/bin/env python3
"""
Benchmark: measure actual baseline cost of reuse queries WITHOUT materialization.

Approach:
  - "Reuse" measurement: CREATE TEMP TABLE from base SQL, run reuse query against it
  - "Baseline" measurement: Prepend base SQL as a CTE with the temp table's name,
    then run the reuse query unchanged. This forces the base computation to run
    from raw tables on every query — no materialization at all.

Example:
  Base SQL: SELECT ci.person_id, t.title ... FROM cast_info ci JOIN title t ...
  Temp table name: director_films_base_629b5a6d
  Reuse query: SELECT genre, AVG(rating) FROM director_films_base_629b5a6d GROUP BY genre

  Baseline becomes:
    WITH director_films_base_629b5a6d AS (
      SELECT ci.person_id, t.title ... FROM cast_info ci JOIN title t ...
    )
    SELECT genre, AVG(rating) FROM director_films_base_629b5a6d GROUP BY genre

No views, no temp tables in the baseline path — only raw base tables.
"""

import json
import re
import time
from pathlib import Path

import psycopg

CONNINFO = "host=localhost port=5434 dbname=agentic_imdb"
SESSION_DIR = Path("sessions")
RESULTS_PATH = Path("reports/baseline_benchmark.json")


def extract_saved_sql(trace: list[dict]) -> dict[str, str]:
    """Extract {logical_name: original_sql} from save() calls in a trace."""
    saved = {}
    for t in trace:
        if not t.get("workspace_save"):
            continue
        sql = t["raw_sql"]
        parts = sql.split("$$")
        if len(parts) < 3:
            continue
        inner_sql = parts[1].strip()

        resp = t.get("agent_response", "")
        m = re.search(r"SAVE DECISION:\s*SAVE(?:_CTE)?\s+(\w+)", resp)
        save_name = m.group(1) if m else None

        if save_name:
            saved[save_name] = inner_sql

    return saved


def find_temp_table_names(ws: dict) -> dict[str, str]:
    """From workspace dump, get {logical_name: temp_table_name} mapping."""
    mapping = {}
    if not ws or "activity" not in ws:
        return mapping
    for evt in ws["activity"]:
        if evt.get("call_type") == "save" and evt.get("payload"):
            payload = evt["payload"]
            name = payload.get("name", "")
            hint = payload.get("usage_hint", "")
            m = re.search(r'FROM\s+(\S+)', hint)
            if m:
                mapping[name] = m.group(1)
    return mapping


def build_baseline_sql(reuse_sql: str, temp_table_name: str, base_sql: str) -> str:
    """
    Prepend the base SQL as a CTE named after the temp table.
    If reuse_sql already starts with WITH, merge into it.
    """
    reuse_stripped = reuse_sql.strip()

    cte_def = f"{temp_table_name} AS (\n{base_sql}\n)"

    # Check if reuse query starts with WITH (case-insensitive)
    if re.match(r'\s*WITH\s', reuse_stripped, re.IGNORECASE):
        # Insert our CTE after the WITH keyword
        baseline = re.sub(
            r'(?i)^(\s*WITH\s+)',
            f'WITH {cte_def},\n',
            reuse_stripped,
            count=1
        )
    else:
        baseline = f"WITH {cte_def}\n{reuse_stripped}"

    return baseline


def run_explain_analyze(conn, sql: str) -> dict:
    """Run EXPLAIN ANALYZE and return execution time."""
    try:
        explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"
        t0 = time.time()
        row = conn.execute(explain_sql).fetchone()
        wall = (time.time() - t0) * 1000
        plan = row[0]
        entry = plan[0] if isinstance(plan, list) else plan
        return {
            "success": True,
            "execution_ms": entry.get("Execution Time", 0),
            "planning_ms": entry.get("Planning Time", 0),
            "wall_ms": round(wall, 1),
        }
    except Exception as e:
        return {
            "success": False,
            "execution_ms": 0,
            "planning_ms": 0,
            "wall_ms": 0,
            "error": str(e)[:300],
        }


def benchmark_scenario(conn, scenario_id: int, reps: list[str]) -> list[dict]:
    """Benchmark all reuse queries in a scenario."""
    results = []

    for rep in reps:
        if scenario_id == 1:
            trace_path = SESSION_DIR / f"scenario1_rep_{rep}.jsonl"
            ws_path = SESSION_DIR / f"scenario1_rep_{rep}_workspace.json"
        else:
            trace_path = SESSION_DIR / f"scenario{scenario_id}_rep_{rep}.jsonl"
            ws_path = SESSION_DIR / f"scenario{scenario_id}_rep_{rep}_workspace.json"

        if not trace_path.exists():
            continue

        trace = [json.loads(l) for l in open(trace_path)]
        ws = json.load(open(ws_path)) if ws_path.exists() else None

        # Get mappings
        saved_sqls = extract_saved_sql(trace)
        temp_names = find_temp_table_names(ws)

        # Map temp_table_name -> original_sql
        temp_to_sql = {}
        for logical_name, temp_name in temp_names.items():
            if logical_name in saved_sqls:
                temp_to_sql[temp_name] = saved_sqls[logical_name]

        if not temp_to_sql:
            continue

        # Find and benchmark reuse queries
        for i, t in enumerate(trace):
            if t.get("workspace_save"):
                continue
            reuse_sql = t.get("raw_sql", "")
            reuse_sql_lower = reuse_sql.lower()

            # Check which temp table this query references
            matched_temp = None
            for temp_name in temp_to_sql:
                if temp_name.lower() in reuse_sql_lower:
                    matched_temp = temp_name
                    break

            if not matched_temp:
                continue

            # Skip queries that already failed in the original session
            if not t.get("success", True):
                continue

            base_sql = temp_to_sql[matched_temp]

            print(f"  Sc{scenario_id} rep {rep} q{i}: ", end="", flush=True)

            # --- Measurement 1: WITH temp table (the "reuse" case) ---
            conn.execute(f"DROP TABLE IF EXISTS {matched_temp}")
            conn.execute(f"CREATE TEMP TABLE {matched_temp} AS {base_sql}")
            conn.execute(f"ANALYZE {matched_temp}")

            reuse_result = run_explain_analyze(conn, reuse_sql)

            # Drop temp table before baseline run
            conn.execute(f"DROP TABLE IF EXISTS {matched_temp}")

            # --- Measurement 2: CTE substitution (the "baseline" — raw tables only) ---
            baseline_sql = build_baseline_sql(reuse_sql, matched_temp, base_sql)

            conn.execute("SET statement_timeout = '1200s'")
            baseline_result = run_explain_analyze(conn, baseline_sql)
            conn.execute("SET statement_timeout = '300s'")

            # Cleanup
            conn.execute(f"DROP TABLE IF EXISTS {matched_temp}")

            entry = {
                "scenario_id": scenario_id,
                "rep": rep,
                "query_seq": i,
                "temp_table": matched_temp,
                "reuse_sql_preview": reuse_sql[:150],
                "baseline_sql_preview": baseline_sql[:200],
                "reuse_ms": reuse_result["execution_ms"],
                "reuse_success": reuse_result["success"],
                "reuse_error": reuse_result.get("error"),
                "baseline_ms": baseline_result["execution_ms"],
                "baseline_success": baseline_result["success"],
                "baseline_error": baseline_result.get("error"),
            }

            if reuse_result["success"] and baseline_result["success"]:
                entry["speedup"] = round(baseline_result["execution_ms"] / max(0.1, reuse_result["execution_ms"]), 2)
                entry["savings_ms"] = round(baseline_result["execution_ms"] - reuse_result["execution_ms"], 1)
                print(f"reuse={reuse_result['execution_ms']:.0f}ms  baseline={baseline_result['execution_ms']:.0f}ms  speedup={entry['speedup']:.1f}x")
            else:
                entry["speedup"] = None
                entry["savings_ms"] = None
                err = baseline_result.get("error", "") or reuse_result.get("error", "")
                print(f"FAILED: {err[:100]}")

            results.append(entry)

    return results


def main():
    conn = psycopg.connect(CONNINFO, autocommit=True)
    conn.execute("SET statement_timeout = '300s'")

    all_results = []

    # Scenario 1
    print("=== Scenario 1: Genre Evolution ===")
    all_results.extend(benchmark_scenario(conn, 1, ["a", "b", "c"]))

    # Scenarios 2-10
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from scenario_common import SCENARIOS

    for sid in range(2, 11):
        sc = SCENARIOS[sid]
        print(f"\n=== Scenario {sid}: {sc['title']} ===")
        all_results.extend(benchmark_scenario(conn, sid, sc["reps"]))

    conn.close()

    # Save raw results
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(RESULTS_PATH, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nResults saved to {RESULTS_PATH}")

    # Summary
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}\n")

    successful = [r for r in all_results if r["speedup"] is not None]
    failed = [r for r in all_results if r["speedup"] is None]
    print(f"Benchmarked: {len(all_results)} reuse queries ({len(successful)} OK, {len(failed)} failed)\n")

    if successful:
        total_reuse = sum(r["reuse_ms"] for r in successful)
        total_baseline = sum(r["baseline_ms"] for r in successful)
        total_savings = sum(r["savings_ms"] for r in successful)

        print(f"{'Metric':<25} {'Value':>15}")
        print(f"{'-'*25} {'-'*15}")
        print(f"{'Total reuse time':<25} {total_reuse/1000:>12.1f}s")
        print(f"{'Total baseline time':<25} {total_baseline/1000:>12.1f}s")
        print(f"{'Total savings':<25} {total_savings/1000:>12.1f}s")
        print(f"{'Overall speedup':<25} {total_baseline/max(1,total_reuse):>12.2f}x")
        print(f"{'Reduction':<25} {total_savings/max(1,total_baseline)*100:>11.1f}%")

        # Per-scenario breakdown
        print(f"\n{'Scenario':<35} | {'Reuse':>8} | {'Baseline':>10} | {'Savings':>9} | {'Speedup':>7} | {'N':>3}")
        print(f"{'-'*35}-|{'-'*10}|{'-'*12}|{'-'*11}|{'-'*9}|{'-'*5}")

        for sid in sorted(set(r["scenario_id"] for r in successful)):
            sc_results = [r for r in successful if r["scenario_id"] == sid]
            sc_reuse = sum(r["reuse_ms"] for r in sc_results)
            sc_base = sum(r["baseline_ms"] for r in sc_results)
            sc_save = sum(r["savings_ms"] for r in sc_results)
            sc_speedup = sc_base / max(1, sc_reuse)
            n = len(sc_results)
            print(f"{'Sc' + str(sid):<35} | {sc_reuse/1000:>6.1f}s | {sc_base/1000:>8.1f}s | {sc_save/1000:>7.1f}s | {sc_speedup:>5.1f}x | {n:>3}")

    if failed:
        print(f"\nFailed queries ({len(failed)}):")
        for r in failed:
            err = r.get('baseline_error') or r.get('reuse_error') or '?'
            print(f"  Sc{r['scenario_id']} rep {r['rep']} q{r['query_seq']}: {err[:100]}")


if __name__ == "__main__":
    main()
