#!/usr/bin/env python3
"""
Stage 3: Scale InsightBench data to 1M+ rows per fact table.

Method: row replication via INSERT ... SELECT ... FROM table, generate_series(1, k-1).
Dimension tables (_sysuser, small lookup tables ≤200 rows) stay at original size.
No PK offset needed — these tables have no integer primary keys; joins are on text columns.
"""

import json
import math
import sys
from pathlib import Path

import psycopg

from pg_loader import load_task_csvs, ALL_TASKS

CONNINFO_ORIG = "host=localhost port=5434 dbname=agentic_poc"
CONNINFO_SCALED = "host=localhost port=5434 dbname=agentic_poc_scaled"
CONNINFO_POSTGRES = "host=localhost port=5434 dbname=postgres"

DIMENSION_THRESHOLD = 200
TARGET_MIN_ROWS = 1_000_000
MAX_SCALE_FACTOR = 20000

STAGE3_DIR = Path("stage3")


def create_scaled_database():
    """Create agentic_poc_scaled database (drop if exists)."""
    conn = psycopg.connect(CONNINFO_POSTGRES, autocommit=True)
    conn.execute("DROP DATABASE IF EXISTS agentic_poc_scaled")
    conn.execute("CREATE DATABASE agentic_poc_scaled")
    conn.close()
    print("Created database: agentic_poc_scaled")


def get_table_row_count(conn, table_name: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def get_table_columns(conn, table_name: str) -> list[str]:
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = %s ORDER BY ordinal_position",
        [table_name]
    ).fetchall()
    return [r[0] for r in rows]


def is_dimension_table(table_name: str, row_count: int) -> bool:
    """Dimension tables: _sysuser suffix or ≤200 rows in multi-table tasks."""
    if "_sysuser" in table_name:
        return True
    return row_count <= DIMENSION_THRESHOLD


def scale_task(task_key: str, conn_scaled, task_tables: list[tuple[str, int]]) -> dict:
    """Scale one task's tables. Returns scaling info."""
    # Find the largest fact table to determine scale factor
    fact_tables = [(t, n) for t, n in task_tables if not is_dimension_table(t, n)]
    dim_tables = [(t, n) for t, n in task_tables if is_dimension_table(t, n)]

    if not fact_tables:
        # All tables are dimensions — treat the largest as fact
        fact_tables = task_tables
        dim_tables = []

    max_fact_rows = max(n for _, n in fact_tables)
    k = math.ceil(TARGET_MIN_ROWS / max_fact_rows)
    k = min(k, MAX_SCALE_FACTOR)

    info = {"task": task_key, "scale_factor": k, "tables": {}}

    # Scale fact tables
    for table_name, orig_rows in fact_tables:
        if k > 1:
            # Replicate: INSERT k-1 copies of every row
            conn_scaled.execute(
                f"INSERT INTO {table_name} "
                f"SELECT {table_name}.* FROM {table_name}, "
                f"generate_series(1, {k - 1})"
            )
        final_count = get_table_row_count(conn_scaled, table_name)
        info["tables"][table_name] = {
            "original": orig_rows,
            "scaled": final_count,
            "type": "fact",
        }
        print(f"  {table_name}: {orig_rows} → {final_count} (×{k})")

    # Dimension tables stay at original size (already loaded by load_task_csvs)
    for table_name, orig_rows in dim_tables:
        final_count = get_table_row_count(conn_scaled, table_name)
        info["tables"][table_name] = {
            "original": orig_rows,
            "scaled": final_count,
            "type": "dimension",
        }
        print(f"  {table_name}: {orig_rows} → {final_count} (dimension, unchanged)")

    return info


def scale_tasks(task_keys: list[str]) -> dict:
    """Scale specified tasks. Returns full scaling manifest."""
    conn_scaled = psycopg.connect(CONNINFO_SCALED, autocommit=True)
    manifest = {}

    for task_key in task_keys:
        print(f"\n{'='*50}")
        print(f"Scaling {task_key}...")

        # Load original CSVs into the scaled database (creates fresh tables)
        task_tables = load_task_csvs(conn_scaled, task_key)
        print(f"  Loaded: {task_tables}")

        # Scale fact tables
        info = scale_task(task_key, conn_scaled, task_tables)
        manifest[task_key] = info

    # Run ANALYZE once on all tables
    print("\nRunning ANALYZE on all tables...")
    for task_key, info in manifest.items():
        for table_name in info["tables"]:
            conn_scaled.execute(f"ANALYZE {table_name}")
    print("ANALYZE complete.")

    conn_scaled.close()
    return manifest


def write_row_counts(manifest: dict, path: Path):
    """Write row_counts.txt from manifest."""
    lines = []
    lines.append(f"{'Table':<20} {'Original':>10} {'Scaled':>12} {'Type':<10} {'Factor':>8}")
    lines.append("-" * 65)
    for task_key in sorted(manifest.keys(), key=lambda k: int(k.split("-")[1])):
        info = manifest[task_key]
        for table_name in sorted(info["tables"].keys()):
            t = info["tables"][table_name]
            factor = t["scaled"] // t["original"] if t["original"] > 0 else 0
            lines.append(f"{table_name:<20} {t['original']:>10} {t['scaled']:>12} {t['type']:<10} {factor:>8}")

    path.write_text("\n".join(lines) + "\n")
    print(f"Row counts written to {path}")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", nargs="+", help="Task keys to scale (default: flag-20 flag-28)")
    parser.add_argument("--all", action="store_true", help="Scale all 31 tasks")
    parser.add_argument("--create-db", action="store_true", help="Create the scaled database first")
    parser.add_argument("--no-create-db", action="store_true", help="Skip database creation")
    args = parser.parse_args()

    STAGE3_DIR.mkdir(parents=True, exist_ok=True)

    if args.all:
        task_keys = ALL_TASKS
    elif args.tasks:
        task_keys = args.tasks
    else:
        task_keys = ["flag-20", "flag-28"]

    if not args.no_create_db:
        create_scaled_database()

    manifest = scale_tasks(task_keys)

    manifest_path = STAGE3_DIR / "scale_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"\nManifest: {manifest_path}")

    write_row_counts(manifest, STAGE3_DIR / "row_counts.txt")


if __name__ == "__main__":
    main()
