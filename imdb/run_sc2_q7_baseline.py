#!/usr/bin/env python3
"""Run the Sc2 rep a q7 baseline query (no temp table, raw tables only)."""

import json
import time

import psycopg

CONNINFO = "host=localhost port=5434 dbname=agentic_imdb"

conn = psycopg.connect(CONNINFO, autocommit=True)
conn.execute("SET statement_timeout = '2700s'")  # 45 min

sql = open("/tmp/sc2_q7_baseline.sql").read()

print(f"Starting EXPLAIN ANALYZE at {time.strftime('%H:%M:%S')}")
print(f"Timeout: 45 minutes")
print(f"Query length: {len(sql)} chars")

t0 = time.time()
try:
    row = conn.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}").fetchone()
    elapsed = time.time() - t0
    plan = row[0]
    entry = plan[0] if isinstance(plan, list) else plan
    exec_time = entry.get("Execution Time", 0)
    plan_time = entry.get("Planning Time", 0)

    print(f"DONE at {time.strftime('%H:%M:%S')}")
    print(f"Wall clock: {elapsed:.1f}s")
    print(f"Execution Time: {exec_time:.1f}ms ({exec_time/1000:.1f}s)")
    print(f"Planning Time: {plan_time:.1f}ms")
    print(f"Reuse (temp table) was: 327,915ms (328s)")
    print(f"Speedup: {exec_time/327915:.1f}x")

    result = {"execution_ms": exec_time, "planning_ms": plan_time, "wall_s": round(elapsed, 1)}
    with open("reports/sc2_q7_baseline_result.json", "w") as f:
        json.dump(result, f, indent=2)
    print("Result saved to reports/sc2_q7_baseline_result.json")
except Exception as e:
    elapsed = time.time() - t0
    print(f"FAILED after {elapsed:.1f}s: {e}")
    result = {"error": str(e), "wall_s": round(elapsed, 1)}
    with open("reports/sc2_q7_baseline_result.json", "w") as f:
        json.dump(result, f, indent=2)

conn.close()
