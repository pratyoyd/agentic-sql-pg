#!/usr/bin/env python3
"""
Environment check for Postgres PoC.
Verifies: Postgres reachable, pg_hint_plan loaded, Rows() hint works.
"""

import platform
import subprocess
import sys

import psycopg

CONNINFO = "host=localhost port=5434 dbname=agentic_poc"


def main():
    print("[env_check] === Environment Verification ===\n")

    # Python / psycopg / OS
    print(f"Python:   {sys.version}")
    print(f"psycopg:  {psycopg.__version__}")
    print(f"OS:       {platform.system()} {platform.release()}")
    print()

    conn = psycopg.connect(CONNINFO, autocommit=True)

    # Postgres version
    row = conn.execute("SELECT version()").fetchone()
    print(f"Postgres: {row[0]}")

    # pg_hint_plan extension version
    row = conn.execute(
        "SELECT extversion FROM pg_extension WHERE extname = 'pg_hint_plan'"
    ).fetchone()
    if row is None:
        print("FAIL: pg_hint_plan extension not installed")
        sys.exit(1)
    print(f"pg_hint_plan: {row[0]}")
    print()

    # Setup test tables
    conn.execute("DROP TABLE IF EXISTS _env_test1, _env_test2")
    conn.execute("CREATE TABLE _env_test1 (id int, val text)")
    conn.execute("INSERT INTO _env_test1 VALUES (1,'a'),(2,'b')")
    conn.execute("CREATE TABLE _env_test2 (id int, val text)")
    conn.execute("INSERT INTO _env_test2 VALUES (1,'x'),(2,'y')")
    conn.execute("ANALYZE _env_test1")
    conn.execute("ANALYZE _env_test2")

    # Baseline plan (no hint)
    row_base = conn.execute(
        "EXPLAIN (FORMAT JSON) SELECT * FROM _env_test1 t1 JOIN _env_test2 t2 ON t1.id = t2.id"
    ).fetchone()
    base_plan = row_base[0][0]["Plan"]
    base_rows = base_plan["Plan Rows"]

    # Hinted plan
    row_hint = conn.execute(
        "EXPLAIN (FORMAT JSON) /*+ Rows(t1 t2 #1000) */ "
        "SELECT * FROM _env_test1 t1 JOIN _env_test2 t2 ON t1.id = t2.id"
    ).fetchone()
    hint_plan = row_hint[0][0]["Plan"]
    hint_rows = hint_plan["Plan Rows"]

    print(f"Baseline join estimated rows: {base_rows}")
    print(f"Hinted  join estimated rows:  {hint_rows}")

    if hint_rows == 1000 and base_rows != 1000:
        print("\nPASS: pg_hint_plan Rows() hint changes cardinality estimate.")
    else:
        print(f"\nFAIL: hint did not change estimate (base={base_rows}, hinted={hint_rows})")
        sys.exit(1)

    # Cleanup
    conn.execute("DROP TABLE _env_test1, _env_test2")
    conn.close()

    print("\n[env_check] All checks passed.")


if __name__ == "__main__":
    main()
