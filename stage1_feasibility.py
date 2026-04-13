#!/usr/bin/env python3
"""
Stage 1: Feasibility on flag-28 (3 reps).

1. Load flag-28 CSVs into Postgres.
2. Replay DuckDB traces against Postgres with EXPLAIN ANALYZE.
3. Compute DuckDB-style signature reuse rate.
4. Compute pg_hint_plan applicability rate.
5. Pick 3 example queries.
"""

import csv
import json
import os
import sys
from pathlib import Path

import psycopg
from pg_plan_parser import (
    extract_plan_tree, is_plan_critical, is_hintable,
    PLAN_CRITICAL_OPS, BOOKKEEPING_OPS,
)

CONNINFO = "host=localhost port=5434 dbname=agentic_poc"
DUCKDB_LOG_DIR = Path("/scratch/agentic-sql/logs")
CSV_DIR = Path("/scratch/insight-bench/data/notebooks/csvs")
OUTPUT_DIR = Path("stage1/raw_plans/flag-28")


def load_flag28_tables(conn):
    """Load flag-28 CSVs into Postgres tables."""
    print("[stage1] Loading flag-28 tables into Postgres...")

    for csv_file, table_name in [
        ("flag-28.csv", "flag_28"),
        ("flag-28-sysuser.csv", "flag_28_sysuser"),
    ]:
        csv_path = CSV_DIR / csv_file
        conn.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")

        # Read CSV to infer schema and load
        with open(csv_path) as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            rows = list(reader)

        if not rows:
            print(f"  WARNING: {csv_file} is empty")
            continue

        # Infer column types from data
        col_defs = []
        for col in headers:
            # Sample values to guess type
            samples = [r[col] for r in rows[:50] if r[col]]
            if all(_is_int(s) for s in samples) and samples:
                col_defs.append(f'"{col}" INTEGER')
            elif all(_is_float(s) for s in samples) and samples:
                col_defs.append(f'"{col}" DOUBLE PRECISION')
            elif all(_is_date(s) for s in samples) and samples:
                col_defs.append(f'"{col}" DATE')
            else:
                col_defs.append(f'"{col}" TEXT')

        create_sql = f"CREATE TABLE {table_name} ({', '.join(col_defs)})"
        conn.execute(create_sql)

        # Bulk insert using COPY
        copy_sql = f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
        with conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                with open(csv_path, "rb") as f:
                    while data := f.read(65536):
                        copy.write(data)

        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        conn.execute(f"ANALYZE {table_name}")
        print(f"  {table_name}: {count} rows loaded, ANALYZE done")


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
    """Check if a string looks like a date (YYYY-MM-DD)."""
    import re
    return bool(re.match(r'^\d{4}-\d{2}-\d{2}$', str(s).strip()))


def load_duckdb_trace(task: str, rep: str) -> list[dict]:
    """Load a DuckDB JSONL trace."""
    path = DUCKDB_LOG_DIR / f"{task}_sweep_{rep}.jsonl"
    entries = []
    for line in open(path):
        entries.append(json.loads(line))
    return entries


def adapt_sql_for_postgres(raw_sql: str) -> str:
    """
    Adapt DuckDB SQL to Postgres dialect.
    Handles common incompatibilities.
    """
    sql = raw_sql.strip()

    # DuckDB MEDIAN -> Postgres percentile_cont(0.5) within group (order by ...)
    # This is complex to do generically; for simple cases:
    import re

    # Replace MEDIAN(expr) with PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY expr)
    def _replace_median(m):
        expr = m.group(1)
        return f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {expr})"
    sql = re.sub(r'\bMEDIAN\s*\(([^)]+)\)', _replace_median, sql, flags=re.IGNORECASE)

    # DuckDB EPOCH(interval) -> Postgres EXTRACT(EPOCH FROM interval)
    def _replace_epoch(m):
        expr = m.group(1)
        return f"EXTRACT(EPOCH FROM {expr})"
    sql = re.sub(r'\bEPOCH\s*\(([^)]+)\)', _replace_epoch, sql, flags=re.IGNORECASE)

    # DuckDB YEAR(date) -> EXTRACT(YEAR FROM date)
    def _replace_year(m):
        expr = m.group(1)
        return f"EXTRACT(YEAR FROM {expr})::INTEGER"
    sql = re.sub(r'\bYEAR\s*\(([^)]+)\)', _replace_year, sql, flags=re.IGNORECASE)

    # DuckDB MONTH(date) -> EXTRACT(MONTH FROM date)
    def _replace_month(m):
        expr = m.group(1)
        return f"EXTRACT(MONTH FROM {expr})::INTEGER"
    sql = re.sub(r'\bMONTH\s*\(([^)]+)\)', _replace_month, sql, flags=re.IGNORECASE)

    # DuckDB QUARTER(date) -> EXTRACT(QUARTER FROM date)
    def _replace_quarter(m):
        expr = m.group(1)
        return f"EXTRACT(QUARTER FROM {expr})::INTEGER"
    sql = re.sub(r'\bQUARTER\s*\(([^)]+)\)', _replace_quarter, sql, flags=re.IGNORECASE)

    # DuckDB PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY expr) is same in PG — but
    # DuckDB also has QUANTILE_CONT which is the same
    sql = re.sub(r'\bQUANTILE_CONT\b', 'PERCENTILE_CONT', sql, flags=re.IGNORECASE)

    # DuckDB string || concat works in PG too

    # DuckDB date arithmetic: end_date - start_date returns integer days in DuckDB
    # In Postgres, date - date also returns integer, so this usually works.
    # But if they're TEXT columns, we need casts.

    # DuckDB LIST_AGG -> STRING_AGG in Postgres
    sql = re.sub(r'\bLIST_AGG\b', 'STRING_AGG', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bGROUP_CONCAT\b', 'STRING_AGG', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bLIST\b\s*\(', 'ARRAY_AGG(', sql, flags=re.IGNORECASE)

    # STRFTIME -> TO_CHAR
    def _replace_strftime(m):
        fmt = m.group(1)
        expr = m.group(2)
        return f"TO_CHAR({expr}, {fmt})"
    sql = re.sub(r"\bSTRFTIME\s*\(\s*('[^']*')\s*,\s*([^)]+)\)", _replace_strftime, sql, flags=re.IGNORECASE)

    # DuckDB ROUND(double, int) -> Postgres needs ROUND(numeric, int)
    # Wrap ROUND arguments: ROUND(expr, n) -> ROUND((expr)::numeric, n)
    def _fix_round(m):
        expr = m.group(1)
        precision = m.group(2)
        # Don't double-cast if already cast
        if '::numeric' in expr:
            return f"ROUND({expr}, {precision})"
        return f"ROUND(({expr})::numeric, {precision})"
    sql = re.sub(r'\bROUND\s*\(\s*((?:[^()]*|\([^()]*(?:\([^()]*\))?[^()]*\))*?)\s*,\s*(\d+)\s*\)',
                 _fix_round, sql, flags=re.IGNORECASE)

    # DuckDB DATEDIFF('day', a, b) -> (b::date - a::date) in Postgres
    def _replace_datediff(m):
        unit = m.group(1).strip("'\"").lower()
        a = m.group(2).strip()
        b = m.group(3).strip()
        if unit == 'day':
            return f"({b} - {a})"
        elif unit == 'month':
            return f"(EXTRACT(YEAR FROM {b}) * 12 + EXTRACT(MONTH FROM {b}) - EXTRACT(YEAR FROM {a}) * 12 - EXTRACT(MONTH FROM {a}))"
        return f"({b} - {a})"
    sql = re.sub(r"\bDATEDIFF\s*\(\s*('[^']*')\s*,\s*([^,]+)\s*,\s*([^)]+)\)",
                 _replace_datediff, sql, flags=re.IGNORECASE)
    # Also handle DATE_DIFF
    sql = re.sub(r"\bDATE_DIFF\s*\(\s*('[^']*')\s*,\s*([^,]+)\s*,\s*([^)]+)\)",
                 _replace_datediff, sql, flags=re.IGNORECASE)

    # DuckDB ::DOUBLE -> ::DOUBLE PRECISION
    sql = re.sub(r'::DOUBLE\b(?!\s+PRECISION)', '::DOUBLE PRECISION', sql, flags=re.IGNORECASE)

    return sql


def replay_trace(conn, trace: list[dict], rep: str):
    """
    Replay a DuckDB trace against Postgres.
    Returns list of (query_idx, raw_sql, plan_json, node_list).
    """
    output_dir = OUTPUT_DIR / rep
    output_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for i, entry in enumerate(trace):
        raw_sql = entry.get("raw_sql", "")
        if not raw_sql.strip():
            continue

        pg_sql = adapt_sql_for_postgres(raw_sql)

        try:
            explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {pg_sql}"
            row = conn.execute(explain_sql).fetchone()
            plan_json = row[0]

            # Save raw plan
            with open(output_dir / f"{i}.json", "w") as f:
                json.dump(plan_json, f, indent=2)

            nodes = extract_plan_tree(plan_json)
            results.append({
                "query_idx": i,
                "raw_sql": raw_sql,
                "pg_sql": pg_sql,
                "plan_json": plan_json,
                "nodes": nodes,
            })
        except Exception as e:
            print(f"  [stage1] Q{i} FAILED: {e}")
            # Save error
            with open(output_dir / f"{i}_error.txt", "w") as f:
                f.write(f"SQL: {pg_sql}\n\nError: {e}")
            results.append({
                "query_idx": i,
                "raw_sql": raw_sql,
                "pg_sql": pg_sql,
                "plan_json": None,
                "nodes": [],
                "error": str(e),
            })

    return results


def compute_reuse_and_applicability(all_results: dict[str, list]):
    """
    Compute DuckDB-style signature reuse rate and pg_hint_plan applicability rate.
    """
    total_nodes = 0
    reuse_hits = 0
    plan_critical_total = 0
    plan_critical_hits = 0
    hintable_hits = 0
    hintable_plan_critical_hits = 0

    q_errors_on_hits = []
    examples = []  # For picking 3 example queries

    for rep, results in all_results.items():
        seen_sigs = {}  # sig -> (actual_card, node_info)

        for r in results:
            for node in r["nodes"]:
                sig = node["operator_signature"]
                op_type = node["operator_type"]
                ec = node["estimated_card"]
                actual = node["actual_card"]

                if not sig or not op_type:
                    continue

                total_nodes += 1
                is_critical = is_plan_critical(op_type)
                if is_critical:
                    plan_critical_total += 1

                if sig in seen_sigs:
                    reuse_hits += 1
                    if is_critical:
                        plan_critical_hits += 1

                    hintable, hint_str = is_hintable(node)
                    if hintable:
                        hintable_hits += 1
                        if is_critical:
                            hintable_plan_critical_hits += 1

                    # Q-error
                    if ec and actual and ec > 0 and actual > 0:
                        q_error = max(ec, actual) / min(ec, actual)
                        q_errors_on_hits.append(q_error)

                    # Collect example info
                    examples.append({
                        "rep": rep,
                        "query_idx": r["query_idx"],
                        "node": node,
                        "hintable": hintable,
                        "hint_str": hint_str,
                        "q_error": max(ec, actual) / min(ec, actual) if ec and actual and ec > 0 and actual > 0 else None,
                        "raw_sql": r["raw_sql"][:200],
                        "plan_json": r["plan_json"],
                    })
                else:
                    seen_sigs[sig] = (actual, node)

    print("\n[stage1] === Feasibility Results ===")
    print(f"  Total plan nodes across flag-28 (3 reps): {total_nodes}")
    print(f"  DuckDB-style signature reuse hits: {reuse_hits}")
    reuse_rate = reuse_hits / total_nodes if total_nodes else 0
    print(f"  Signature reuse rate (all): {reuse_rate:.3f} ({reuse_hits}/{total_nodes})")

    pc_reuse_rate = plan_critical_hits / plan_critical_total if plan_critical_total else 0
    print(f"  Plan-critical reuse rate:   {pc_reuse_rate:.3f} ({plan_critical_hits}/{plan_critical_total})")

    applicability_rate = hintable_hits / reuse_hits if reuse_hits else 0
    print(f"\n  pg_hint_plan applicability (of reuse hits): {applicability_rate:.3f} ({hintable_hits}/{reuse_hits})")

    hintable_pc_rate = hintable_plan_critical_hits / plan_critical_hits if plan_critical_hits else 0
    print(f"  pg_hint_plan applicability (plan-critical hits): {hintable_pc_rate:.3f} ({hintable_plan_critical_hits}/{plan_critical_hits})")

    if q_errors_on_hits:
        import numpy as np
        arr = np.array(q_errors_on_hits)
        print(f"\n  Q-error on reuse hits: mean={np.mean(arr):.2f}, p50={np.median(arr):.2f}, p95={np.percentile(arr, 95):.2f}, n={len(arr)}")

    # --- Pick 3 examples ---
    print("\n[stage1] === Example Queries ===")
    _pick_examples(examples)

    return {
        "total_nodes": total_nodes,
        "reuse_hits": reuse_hits,
        "reuse_rate": reuse_rate,
        "plan_critical_total": plan_critical_total,
        "plan_critical_hits": plan_critical_hits,
        "plan_critical_reuse_rate": pc_reuse_rate,
        "hintable_hits": hintable_hits,
        "applicability_rate": applicability_rate,
        "hintable_plan_critical_hits": hintable_plan_critical_hits,
        "hintable_pc_rate": hintable_pc_rate,
    }


def _pick_examples(examples):
    """Pick and display 3 example queries."""
    # 1. Trivially applicable (hintable join, low q-error)
    trivial = [e for e in examples if e["hintable"] and e["q_error"]
               and e["q_error"] < 2 and "Join" in e["node"]["operator_type"]]
    # 2. Applicable but uninteresting (hintable scan, low q-error)
    uninteresting = [e for e in examples if e["hintable"] and e["q_error"]
                     and e["q_error"] < 1.5 and "Scan" in e["node"]["operator_type"]]
    # 3. Valuable but NOT hintable (high q-error, aggregate or non-hintable)
    valuable_nothint = [e for e in examples if not e["hintable"] and e["q_error"]
                        and e["q_error"] > 3]

    for label, candidates in [
        ("Example 1 (trivially applicable join hint)", trivial),
        ("Example 2 (applicable but uninteresting scan)", uninteresting),
        ("Example 3 (valuable but NOT hintable)", valuable_nothint),
    ]:
        print(f"\n--- {label} ---")
        if not candidates:
            print("  No matching example found.")
            continue
        # Pick the one with most interesting q-error
        if "NOT hintable" in label:
            ex = max(candidates, key=lambda e: e["q_error"] or 0)
        else:
            ex = candidates[0]

        node = ex["node"]
        print(f"  Rep: {ex['rep']}, Query: Q{ex['query_idx']}")
        print(f"  Operator: {node['operator_type']}")
        print(f"  Tables: {node['tables']}, Aliases: {node.get('relation_aliases', [])}")
        print(f"  Predicates: {node['predicates'][:2]}")
        print(f"  Estimated: {node['estimated_card']}, Actual: {node['actual_card']}")
        print(f"  Q-error: {ex['q_error']:.2f}" if ex['q_error'] else "  Q-error: N/A")
        print(f"  Hintable: {ex['hintable']}")
        if ex["hint_str"]:
            print(f"  Hint: {ex['hint_str']}")
        else:
            print(f"  Why not hintable: {node['operator_type']} cannot receive Rows() hint "
                  f"(aggregation/bookkeeping level)")
        print(f"  SQL fragment: {ex['raw_sql']}")


def main():
    # Step 1: Load tables
    conn = psycopg.connect(CONNINFO, autocommit=True)
    load_flag28_tables(conn)
    conn.close()

    # Step 2-3: Replay traces
    all_results = {}
    for rep in ["a", "b", "c"]:
        print(f"\n[stage1] Replaying flag-28 rep {rep}...")
        trace = load_duckdb_trace("flag-28", rep)
        # Fresh connection per session
        conn = psycopg.connect(CONNINFO, autocommit=True)
        results = replay_trace(conn, trace, rep)
        conn.close()

        ok = sum(1 for r in results if r["nodes"])
        fail = sum(1 for r in results if not r["nodes"])
        print(f"  Rep {rep}: {ok} queries parsed, {fail} failed")
        all_results[rep] = results

    # Step 4-5: Compute reuse and applicability
    stats = compute_reuse_and_applicability(all_results)

    # Save stats
    os.makedirs("stage1", exist_ok=True)
    with open("stage1/feasibility_stats.json", "w") as f:
        json.dump(stats, f, indent=2)

    # Go/no-go
    print("\n[stage1] === GO / NO-GO ===")
    rate = stats["applicability_rate"]
    if rate >= 0.5:
        print(f"  Applicability rate {rate:.1%} >= 50% threshold. GO — proceed to Stage 2.")
    else:
        print(f"  Applicability rate {rate:.1%} < 50% threshold. NO-GO — reconsider injection strategy.")


if __name__ == "__main__":
    main()
