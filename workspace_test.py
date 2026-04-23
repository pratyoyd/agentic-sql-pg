#!/usr/bin/env python3
"""Smoke test for workspace.sql primitives."""

import os
import sys
from pathlib import Path

import psycopg

CONNINFO = (
    f"host={os.environ.get('DB_HOST', 'localhost')} "
    f"port={os.environ.get('DB_PORT', '5434')} "
    f"dbname={os.environ.get('DB_NAME', 'agentic_imdb')} "
    f"user={os.environ.get('DB_USER', os.environ.get('USER', ''))} "
    f"password={os.environ.get('DB_PASSWORD', '')}"
)

SQL_FILE = Path(__file__).parent / "workspace.sql"

failures = 0


def check(step: int, label: str, condition: bool, detail: str = ""):
    global failures
    tag = "PASS" if condition else "FAIL"
    if not condition:
        failures += 1
    msg = f"  [{tag}] Step {step}: {label}"
    if detail:
        msg += f" — {detail}"
    print(msg)


def source_sql(conn):
    """Source workspace.sql into the connection."""
    sql = SQL_FILE.read_text()
    conn.execute(sql)


def main():
    global failures

    print("=== Workspace smoke test ===\n")

    # --- Session 1 ---
    conn1 = psycopg.connect(CONNINFO, autocommit=True)
    source_sql(conn1)

    # Step 1: catalog() should return 0 rows
    rows = conn1.execute("SELECT * FROM workspace.catalog()").fetchall()
    check(1, "catalog() empty on fresh session", len(rows) == 0,
          f"got {len(rows)} rows")

    # Step 2: save() creates a handle
    result = conn1.execute(
        "SELECT workspace.save('kt_small', 'kind_type distinct kinds', "
        "'SELECT DISTINCT kind FROM kind_type')"
    ).fetchone()[0]
    check(2, "save() creates handle",
          result.get("status") == "created"
          and result.get("row_count") == 7
          and result.get("creation_ms", 0) > 0,
          f"status={result.get('status')}, row_count={result.get('row_count')}, "
          f"creation_ms={result.get('creation_ms')}")

    # Step 3: save() with same query under different name → dedup
    result2 = conn1.execute(
        "SELECT workspace.save('kt_dup', 'same query', "
        "'SELECT DISTINCT kind FROM kind_type')"
    ).fetchone()[0]
    check(3, "save() dedup on identical query",
          result2.get("status") == "existing"
          and result2.get("handle_name") == "kt_small",
          f"status={result2.get('status')}, handle_name={result2.get('handle_name')}")

    # Step 4: catalog() returns 1 row, access_count=0
    rows = conn1.execute("SELECT * FROM workspace.catalog()").fetchall()
    check(4, "catalog() returns 1 row with access_count=0",
          len(rows) == 1 and rows[0][7] == 0,  # access_count is column index 7
          f"rows={len(rows)}, access_count={rows[0][7] if rows else '?'}")

    # Step 5: touch() bumps access_count
    conn1.execute("SELECT workspace.touch('kt_small')")
    rows = conn1.execute("SELECT * FROM workspace.catalog()").fetchall()
    check(5, "touch() bumps access_count to 1",
          len(rows) == 1 and rows[0][7] == 1,
          f"access_count={rows[0][7] if rows else '?'}")

    # Step 6: save() with invalid handle name raises exception
    try:
        conn1.execute(
            "SELECT workspace.save('1bad-name', 'bad', 'SELECT 1')"
        )
        check(6, "save() rejects invalid handle name", False,
              "no exception raised")
    except psycopg.errors.RaiseException as e:
        check(6, "save() rejects invalid handle name",
              "invalid handle name" in str(e).lower(),
              f"exception: {str(e)[:120]}")

    # Step 7: drop() returns true
    result = conn1.execute("SELECT workspace.drop('kt_small')").fetchone()[0]
    check(7, "drop() returns true for existing handle",
          result is True, f"got {result}")

    # Step 8: drop() again returns false (idempotent)
    result = conn1.execute("SELECT workspace.drop('kt_small')").fetchone()[0]
    check(8, "drop() returns false on second call",
          result is False, f"got {result}")

    # Step 9: catalog() returns 0 rows
    rows = conn1.execute("SELECT * FROM workspace.catalog()").fetchall()
    check(9, "catalog() empty after drop", len(rows) == 0,
          f"got {len(rows)} rows")

    conn1.close()

    # --- Session 2: isolation ---
    conn2 = psycopg.connect(CONNINFO, autocommit=True)
    source_sql(conn2)
    rows = conn2.execute("SELECT * FROM workspace.catalog()").fetchall()
    check(10, "new session catalog() is empty (session isolation)",
          len(rows) == 0, f"got {len(rows)} rows")
    conn2.close()

    # --- Summary ---
    print(f"\n{'='*40}")
    if failures == 0:
        print("All 10 steps passed.")
    else:
        print(f"{failures} step(s) FAILED.")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
