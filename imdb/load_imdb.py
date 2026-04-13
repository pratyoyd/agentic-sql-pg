#!/usr/bin/env python3
"""
Load the IMDB dataset (JOB benchmark) into PostgreSQL.
Creates schema, loads CSVs via COPY, creates FK indexes, runs ANALYZE.
"""

import time
from pathlib import Path

import psycopg

CONNINFO_POSTGRES = "host=localhost port=5434 dbname=postgres"
CONNINFO_IMDB = "host=localhost port=5434 dbname=agentic_imdb"

SCHEMA_SQL = Path("job_repo/schema.sql")
FKINDEXES_SQL = Path("job_repo/fkindexes.sql")
CSV_DIR = Path(".")

# Table load order (respects nothing — IMDB CSVs have no FK constraints enforced)
TABLES = [
    "comp_cast_type", "company_type", "info_type", "kind_type",
    "link_type", "role_type",  # small lookup tables first
    "keyword", "char_name", "name", "company_name",  # medium tables
    "aka_name", "aka_title", "title",  # larger tables
    "cast_info", "movie_companies", "movie_info", "movie_info_idx",
    "movie_keyword", "movie_link", "complete_cast", "person_info",
]


def create_database():
    conn = psycopg.connect(CONNINFO_POSTGRES, autocommit=True)
    conn.execute("DROP DATABASE IF EXISTS agentic_imdb")
    conn.execute("CREATE DATABASE agentic_imdb")
    conn.close()
    print("Created database: agentic_imdb")


def create_schema(conn):
    schema_sql = SCHEMA_SQL.read_text()
    # Split on semicolons and execute each statement
    for stmt in schema_sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)
    print("Schema created (21 tables)")


def load_csv(conn, table_name: str) -> int:
    csv_path = CSV_DIR / f"{table_name}.csv"
    if not csv_path.exists():
        print(f"  WARNING: {csv_path} not found, skipping")
        return 0

    copy_sql = f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, DELIMITER ',', QUOTE '\"', ESCAPE '\\')"
    t0 = time.time()
    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            with open(csv_path, "rb") as f:
                while data := f.read(1 << 20):  # 1MB chunks
                    copy.write(data)
    elapsed = time.time() - t0

    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"  {table_name}: {count:>12,} rows ({elapsed:.1f}s)")
    return count


def create_indexes(conn):
    idx_sql = FKINDEXES_SQL.read_text()
    for stmt in idx_sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)
    print("FK indexes created (23 indexes)")


def run_analyze(conn):
    for table in TABLES:
        conn.execute(f"ANALYZE {table}")
    print("ANALYZE complete on all 21 tables")


def enable_hint_plan(conn_postgres):
    """Create pg_hint_plan extension in the new database."""
    conn = psycopg.connect(CONNINFO_IMDB, autocommit=True)
    try:
        conn.execute("CREATE EXTENSION IF NOT EXISTS pg_hint_plan")
        print("pg_hint_plan extension created")
    except Exception as e:
        print(f"pg_hint_plan: {e}")
    conn.close()


def main():
    create_database()

    conn = psycopg.connect(CONNINFO_IMDB, autocommit=True)

    print("\nCreating schema...")
    create_schema(conn)

    print("\nLoading CSV files...")
    total_rows = 0
    row_counts = {}
    for table in TABLES:
        count = load_csv(conn, table)
        total_rows += count
        row_counts[table] = count

    print(f"\nTotal rows loaded: {total_rows:,}")

    print("\nCreating FK indexes...")
    t0 = time.time()
    create_indexes(conn)
    print(f"  Index creation took {time.time()-t0:.1f}s")

    print("\nRunning ANALYZE...")
    run_analyze(conn)

    conn.close()

    # Enable pg_hint_plan
    enable_hint_plan(None)

    # Write load report
    report = ["# IMDB Dataset Load Report\n"]
    report.append(f"**Total rows:** {total_rows:,}\n")
    report.append("| Table | Rows |")
    report.append("|-------|------|")
    for table in sorted(row_counts.keys()):
        report.append(f"| {table} | {row_counts[table]:,} |")
    report.append("")

    Path("load_report.md").write_text("\n".join(report))
    print(f"\nLoad report: load_report.md")


if __name__ == "__main__":
    main()
