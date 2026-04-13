#!/usr/bin/env python3
"""
Load InsightBench CSVs into Postgres for any task.
Infers column types (int, float, date, text) from data.
"""

import csv
import json
import re
from pathlib import Path

INSIGHT_BENCH_ROOT = Path("/scratch/insight-bench")
CSV_DIR = INSIGHT_BENCH_ROOT / "data" / "notebooks" / "csvs"
TASK_JSON_DIR = INSIGHT_BENCH_ROOT / "data" / "notebooks"

ALL_TASKS = [f"flag-{i}" for i in range(1, 32)]


def _is_int(s):
    try:
        int(s)
        return True
    except (ValueError, TypeError):
        return False


def _is_float(s):
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False


def _is_date(s):
    return bool(re.match(r'^\d{4}-\d{2}-\d{2}$', str(s).strip()))


def _is_datetime(s):
    return bool(re.match(r'^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}', str(s).strip()))


def _table_name_from_csv(csv_filename: str) -> str:
    return csv_filename.replace(".csv", "").replace("-", "_")


def _has_time_component(samples: list[str]) -> bool:
    """Check if datetime samples have meaningful time (not all 00:00:00)."""
    for s in samples:
        m = re.match(r'^\d{4}-\d{2}-\d{2}[ T](\d{2}:\d{2}:\d{2})', str(s).strip())
        if m and m.group(1) != '00:00:00':
            return True
    return False


def _infer_type(samples: list[str]) -> str:
    """Infer Postgres column type from sample values.
    Prefers DATE over TIMESTAMP so that date - date returns integer days
    (matching DuckDB behavior) rather than interval."""
    non_empty = [s for s in samples if s]
    if not non_empty:
        return "TEXT"
    if all(_is_int(s) for s in non_empty):
        return "INTEGER"
    if all(_is_float(s) for s in non_empty):
        return "DOUBLE PRECISION"
    if all(_is_datetime(s) for s in non_empty):
        return "TIMESTAMP"
    if all(_is_date(s) for s in non_empty):
        return "DATE"
    return "TEXT"


def load_task_csvs(conn, task_key: str) -> list[str]:
    """Load all CSVs for a task into Postgres. Returns list of table names."""
    meta = json.load(open(TASK_JSON_DIR / f"{task_key}.json"))
    tables_created = []

    csv_paths = [meta["dataset_csv_path"]]
    if meta.get("user_dataset_csv_path"):
        csv_paths.append(meta["user_dataset_csv_path"])

    for csv_rel in csv_paths:
        csv_filename = Path(csv_rel).name
        csv_path = CSV_DIR / csv_filename
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV not found: {csv_path}")

        table_name = _table_name_from_csv(csv_filename)

        # Read CSV to infer schema
        with open(csv_path) as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            rows = list(reader)

        if not rows:
            print(f"  WARNING: {csv_filename} is empty, skipping")
            continue

        # Infer column types
        col_defs = []
        for col in headers:
            samples = [r[col] for r in rows[:100] if r[col]]
            pg_type = _infer_type(samples)
            col_defs.append(f'"{col}" {pg_type}')

        conn.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
        conn.execute(f"CREATE TABLE {table_name} ({', '.join(col_defs)})")

        # COPY from CSV
        copy_sql = f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
        with conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                with open(csv_path, "rb") as f:
                    while data := f.read(65536):
                        copy.write(data)

        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        conn.execute(f"ANALYZE {table_name}")
        tables_created.append((table_name, count))

    return tables_created


def load_all_tasks(conn) -> dict[str, list]:
    """Load all 31 tasks. Returns {task: [(table, row_count), ...]}."""
    results = {}
    for task in ALL_TASKS:
        tables = load_task_csvs(conn, task)
        results[task] = tables
        for tname, count in tables:
            print(f"  [loader] {task}/{tname}: {count} rows")
    return results


if __name__ == "__main__":
    import psycopg
    conn = psycopg.connect("host=localhost port=5434 dbname=agentic_poc", autocommit=True)
    load_all_tasks(conn)
    conn.close()
