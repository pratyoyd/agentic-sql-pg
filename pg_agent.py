#!/usr/bin/env python3
"""
Minimal ReAct agent for Postgres agentic data analysis trace generation.

Uses Claude Code CLI (claude -p) as the LLM backend.
Generates SQL queries against PostgreSQL, logged via PgSessionLogger.
"""

import argparse
import json
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path

import psycopg

from pg_loader import load_task_csvs, ALL_TASKS
from pg_logger import PgSessionLogger, format_result

CONNINFO = "host=localhost port=5434 dbname=agentic_poc"
INSIGHT_BENCH_ROOT = Path("/scratch/insight-bench")
TASK_JSON_DIR = INSIGHT_BENCH_ROOT / "data" / "notebooks"

MAX_QUERIES = 40
MAX_PARSE_FAILURES = 3
CLAUDE_CMD = "claude"


def load_task_metadata(task_key: str) -> dict:
    with open(TASK_JSON_DIR / f"{task_key}.json") as f:
        return json.load(f)


def get_schema_description(conn: psycopg.Connection, table_name: str) -> str:
    """Return a human-readable schema string for an agent's system prompt."""
    rows = conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = %s ORDER BY ordinal_position",
        [table_name]
    ).fetchall()
    lines = [f"Table: {table_name}"]
    for col_name, data_type in rows:
        lines.append(f"  {col_name}: {data_type}")

    sample = conn.execute(f'SELECT * FROM "{table_name}" LIMIT 3').fetchall()
    if sample:
        col_names = [d.name for d in conn.execute(f'SELECT * FROM "{table_name}" LIMIT 0').description]
        lines.append(f"\nSample rows:")
        lines.append(" | ".join(col_names))
        for row in sample:
            lines.append(" | ".join(str(v) for v in row))
    return "\n".join(lines)


def build_system_prompt(goal: str, schema_descriptions: list[str]) -> str:
    schemas = "\n\n".join(schema_descriptions)
    return f"""You are a data analyst exploring a PostgreSQL 16 database. Your goal is:

{goal}

The database has the following schema:

{schemas}

Instructions:
- To run a SQL query, write it inside a ```sql ... ``` code block.
- Write exactly ONE query per response.
- After seeing the result, reason briefly about what you learned and what to investigate next.
- You are querying a PostgreSQL 16 database via psycopg. Use PostgreSQL-compatible SQL: quote identifiers with double quotes when needed, use `::` for type casts, use `DATE_TRUNC('month', col)` or `EXTRACT(MONTH FROM col)` for date parts, use `ILIKE` for case-insensitive matching, and prefer explicit column names over `GROUP BY 1, 2` ordinals when possible.
- When you have gathered enough evidence to address the goal, write DONE on its own line and provide a summary of your findings.
- Do not fabricate data. Only draw conclusions from query results you have actually seen.

Important: Explore the data using a diverse strategy. Try different analytical angles, GROUP BY dimensions, and investigation paths. Do not follow a fixed template."""


def extract_sql(response: str) -> str | None:
    match = re.search(r"```sql\s*\n(.*?)```", response, re.DOTALL)
    if match:
        return match.group(1).strip()
    return None


def has_done(response: str) -> bool:
    return bool(re.search(r"^\s*DONE\s*$", response, re.MULTILINE))


def call_claude(prompt: str, model: str = "sonnet") -> str:
    result = subprocess.run(
        [CLAUDE_CMD, "-p", prompt, "--model", model],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        raise RuntimeError(f"claude -p failed (rc={result.returncode}): {result.stderr[:500]}")
    return result.stdout.strip()


def run_session(task_key: str, log_dir: str = "stage2/traces",
                session_id_override: str | None = None) -> dict:
    """Run one agent session for a task. Returns session summary."""
    metadata = load_task_metadata(task_key)
    goal = metadata["metadata"]["goal"]

    # Create Postgres connection and load data
    conn = psycopg.connect(CONNINFO, autocommit=True)

    # Load tables (idempotent — drops and recreates)
    tables_loaded = load_task_csvs(conn, task_key)
    table_names = [t[0] for t in tables_loaded]
    print(f"Tables loaded: {table_names}")

    # Build schema descriptions
    schema_descs = [get_schema_description(conn, t) for t in table_names]

    system_prompt = build_system_prompt(goal, schema_descs)

    # Session ID
    if session_id_override:
        session_id = session_id_override
    else:
        session_id = f"{task_key}_rep_a"

    raw_plan_dir = Path("stage2/raw_plans") / task_key
    logger = PgSessionLogger(session_id, log_dir=log_dir, raw_plan_dir=raw_plan_dir)

    print(f"\n{'='*60}")
    print(f"Session: {session_id}")
    print(f"Goal: {goal}")
    print(f"{'='*60}\n")

    history: list[tuple[str, str]] = []
    query_count = 0
    parse_failures = 0
    t_start = time.time()

    while query_count < MAX_QUERIES and parse_failures < MAX_PARSE_FAILURES:
        prompt_parts = [system_prompt]
        for role, text in history:
            if role == "agent":
                prompt_parts.append(f"\n[Your previous response]\n{text}")
            elif role == "result":
                prompt_parts.append(f"\n[Query result]\n{text}")
        prompt_parts.append("\n[Now respond with your next analysis step.]")
        full_prompt = "\n".join(prompt_parts)

        print(f"--- Turn {query_count + 1} ---")
        try:
            response = call_claude(full_prompt)
        except Exception as e:
            print(f"  LLM error: {e}")
            break

        print(f"  Agent: {response[:200]}{'...' if len(response) > 200 else ''}")

        if has_done(response):
            history.append(("agent", response))
            print(f"\n  Agent signaled DONE after {query_count} queries.")
            break

        sql = extract_sql(response)
        if sql is None:
            parse_failures += 1
            print(f"  No SQL found (parse failure {parse_failures}/{MAX_PARSE_FAILURES})")
            history.append(("agent", response))
            history.append(("result", "No SQL query found in your response. Please write a SQL query in a ```sql ... ``` block, or write DONE if you are finished."))
            continue

        parse_failures = 0
        query_count += 1

        print(f"  SQL: {sql[:120]}{'...' if len(sql) > 120 else ''}")
        result = logger.execute_and_log(conn, sql)
        result_text = format_result(result)
        print(f"  Result: {result['result_rows']} rows, success={result['success']}")

        history.append(("agent", response))
        history.append(("result", result_text))

    elapsed = time.time() - t_start

    summary = {
        "session_id": session_id,
        "task_key": task_key,
        "goal": goal,
        "model": "claude-code-cli-sonnet",
        "num_queries": query_count,
        "parse_failures": parse_failures,
        "wall_clock_seconds": round(elapsed, 1),
        "log_file": str(logger.log_path),
    }

    summary_path = Path(log_dir) / f"{session_id}_summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"\nSession complete: {query_count} queries in {elapsed:.1f}s")
    print(f"Log: {logger.log_path}")

    conn.close()
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", type=str, help="Single task key")
    parser.add_argument("--all", action="store_true", help="Run all 31 tasks")
    args = parser.parse_args()

    if args.all:
        for task_key in ALL_TASKS:
            try:
                run_session(task_key)
            except Exception as e:
                print(f"ERROR on {task_key}: {e}")
    elif args.task:
        run_session(args.task)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
