#!/usr/bin/env python3
"""
ReAct agent for IMDB agentic data analysis — v2 with alias instruction.
Identical to imdb_agent.py except:
  1. Adds alias-convention instruction to the system prompt.
  2. Writes traces to traces_v2/ and plans to raw_plans_v2/.
"""

import argparse
import json
import re
import subprocess
import time
from pathlib import Path

import psycopg

CONNINFO = "host=localhost port=5434 dbname=agentic_imdb"
TRACE_DIR = Path("traces_v2")
RAW_PLAN_DIR = Path("raw_plans_v2")

MAX_QUERIES = 50
MAX_PARSE_FAILURES = 3
CLAUDE_CMD = "claude"

# Same tasks as imdb_agent.py
TASKS = {
    "task2": {
        "name": "Director-actor collaborations",
        "goal": """You are analyzing long-term collaboration patterns in the film industry. Identify the most productive director-actor pairs across films released from 1990 to 2010. A "productive pair" is one where the same director and actor worked together on at least three films. For the top pairs you find, characterize what makes their collaborations consistent — shared genres, shared production companies, shared keywords, or shared themes. Reach a concluding observation about what distinguishes repeat-collaboration pairs from one-off collaborations. When you have a defensible answer, state your final conclusion and stop.""",
    },
}

ALIAS_INSTRUCTION = """
**Alias convention for repeated table references.** When a query references the same base table more than once — either across multiple CTEs, across multiple subqueries, or through a self-join — use a distinct descriptive alias for each reference that reflects its semantic role. For example, if you are joining `cast_info` to itself to find directors and actors, use `cast_info ci_dir` in one location and `cast_info ci_act` in the other, never two copies of `cast_info ci`. This applies only when the same base table appears more than once in the same query; single references can keep short aliases as usual."""


def get_schema_description(conn) -> str:
    rows = conn.execute("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position
    """).fetchall()

    tables = {}
    for tname, cname, dtype in rows:
        tables.setdefault(tname, []).append(f"  {cname}: {dtype}")

    counts = {}
    for tname in tables:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {tname}").fetchone()[0]
        counts[tname] = cnt

    lines = []
    for tname in sorted(tables.keys()):
        lines.append(f"Table: {tname} ({counts[tname]:,} rows)")
        lines.extend(tables[tname])
        lines.append("")
    return "\n".join(lines)


def build_system_prompt(goal: str, schema_desc: str) -> str:
    return f"""You are a data analyst exploring a PostgreSQL 16 database containing the IMDB dataset (Internet Movie Database). Your goal is:

{goal}

The database has the following schema:

{schema_desc}

Key relationships:
- title.id is the central movie identifier. Most tables join to it via movie_id.
- cast_info links people (person_id → name.id) to movies (movie_id → title.id) with roles (role_id → role_type.id).
- movie_info and movie_info_idx store key-value pairs about movies. info_type.id identifies the type of info (e.g., genres, countries, ratings, runtimes).
- movie_companies links movies to production companies via company_id → company_name.id.
- movie_keyword links movies to keywords via keyword_id → keyword.id.
- movie_link tracks relationships between movies (sequels, remakes, etc.) via link_type_id → link_type.id.
- To find directors: cast_info WHERE role_id = (SELECT id FROM role_type WHERE role = 'director').
- To find actors: cast_info WHERE role_id = (SELECT id FROM role_type WHERE role = 'actor') OR role_id = (SELECT id FROM role_type WHERE role = 'actress').
- Ratings are in movie_info_idx where info_type_id = (SELECT id FROM info_type WHERE info = 'rating').
- Genres are in movie_info where info_type_id = (SELECT id FROM info_type WHERE info = 'genres').
- Countries are in movie_info where info_type_id = (SELECT id FROM info_type WHERE info = 'countries').
- Runtimes are in movie_info where info_type_id = (SELECT id FROM info_type WHERE info = 'runtimes').

Instructions:
- To run a SQL query, write it inside a ```sql ... ``` code block.
- Write exactly ONE query per response.
- After seeing the result, reason briefly about what you learned and what to investigate next.
- You are querying a PostgreSQL 16 database. Use PostgreSQL-compatible SQL: quote identifiers with double quotes when needed, use `::` for type casts, use ILIKE for case-insensitive matching.
- The database is large (74M rows total, cast_info alone is 36M rows). Use JOINs efficiently and add WHERE filters early. Avoid unbounded cross-joins.
- When you have gathered enough evidence to address the goal, write DONE on its own line and provide a summary of your findings.
- Do not fabricate data. Only draw conclusions from query results you have actually seen.

Important: Explore the data using a diverse strategy. Try different analytical angles, GROUP BY dimensions, and investigation paths. Do not follow a fixed template.
{ALIAS_INSTRUCTION}"""


def extract_sql(response: str) -> str | None:
    match = re.search(r"```sql\s*\n(.*?)```", response, re.DOTALL)
    if match:
        return match.group(1).strip()
    return None


def has_done(response: str) -> bool:
    return bool(re.search(r"^\s*DONE\s*$", response, re.MULTILINE))


def call_claude(prompt: str, model: str = "sonnet") -> str:
    result = subprocess.run(
        [CLAUDE_CMD, "-p", "--model", model],
        input=prompt,
        capture_output=True, text=True, timeout=1200,
    )
    if result.returncode != 0:
        raise RuntimeError(f"claude -p failed (rc={result.returncode}): {result.stderr[:500]}")
    return result.stdout.strip()


def format_result(result: dict, max_rows: int = 30) -> str:
    if not result["success"]:
        return f"ERROR: {result['error']}"
    cols = result["columns"]
    data = result["data"][:max_rows]
    if not cols:
        return "(query returned no columns)"
    lines = [" | ".join(str(c) for c in cols)]
    lines.append("-" * len(lines[0]))
    for row in data:
        lines.append(" | ".join(str(v) for v in row))
    if result["result_rows"] > max_rows:
        lines.append(f"... ({result['result_rows']} total rows, showing first {max_rows})")
    else:
        lines.append(f"({result['result_rows']} rows)")
    return "\n".join(lines)


def execute_and_log(conn, sql: str, plan_path: Path | None = None) -> dict:
    from pg_plan_parser import extract_plan_tree
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from pg_plan_parser import extract_plan_tree

    t0 = time.time()
    try:
        explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"
        plan_row = conn.execute(explain_sql).fetchone()
        plan_json = plan_row[0]
        plan_tree = extract_plan_tree(plan_json)

        if plan_path:
            plan_path.parent.mkdir(parents=True, exist_ok=True)
            with open(plan_path, "w") as f:
                json.dump(plan_json, f, indent=2)

        result = conn.execute(sql)
        description = result.description or []
        col_names = [d.name for d in description]
        rows = result.fetchall()
        elapsed_ms = (time.time() - t0) * 1000
        display_data = [list(r) for r in rows[:50]]
        for row in display_data:
            for i, v in enumerate(row):
                if not isinstance(v, (str, int, float, bool, type(None))):
                    row[i] = str(v)

        return {
            "success": True, "result_rows": len(rows),
            "columns": col_names, "data": display_data,
            "error": None, "execution_ms": round(elapsed_ms, 2),
            "plan_tree": plan_tree,
        }
    except Exception as e:
        elapsed_ms = (time.time() - t0) * 1000
        return {
            "success": False, "result_rows": 0,
            "columns": [], "data": [],
            "error": str(e), "execution_ms": round(elapsed_ms, 2),
            "plan_tree": [],
        }


def run_session(task_key: str, rep: str, schema_desc: str) -> dict:
    task = TASKS[task_key]
    goal = task["goal"]
    session_id = f"{task_key}_rep_{rep}"

    conn = psycopg.connect(CONNINFO, autocommit=True)
    conn.execute("SET statement_timeout = '600s'")
    system_prompt = build_system_prompt(goal, schema_desc)

    trace_path = TRACE_DIR / f"{session_id}.jsonl"
    TRACE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"Session: {session_id}")
    print(f"Task: {task['name']}")
    print(f"{'='*60}\n")

    history = []
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
            print(f"\n--- Final Response ---\n{response}\n--- End ---")
            break

        sql = extract_sql(response)
        if sql is None:
            parse_failures += 1
            print(f"  No SQL found ({parse_failures}/{MAX_PARSE_FAILURES})")
            history.append(("agent", response))
            history.append(("result", "No SQL query found. Please write a SQL query in a ```sql ... ``` block, or write DONE if finished."))
            continue

        parse_failures = 0
        query_count += 1

        print(f"  SQL: {sql[:120]}{'...' if len(sql) > 120 else ''}")

        plan_path = RAW_PLAN_DIR / task_key / f"rep_{rep}" / f"{query_count - 1}.json"
        result = execute_and_log(conn, sql, plan_path)
        result_text = format_result(result)
        print(f"  Result: {result['result_rows']} rows, success={result['success']}, {result['execution_ms']:.0f}ms")

        try:
            import sqlglot
            from sqlglot import exp
            parsed = sqlglot.parse_one(sql, dialect="postgres")
            meta_tables = sorted(set(t.name for t in parsed.find_all(exp.Table) if t.name))
            meta_columns = sorted(set(c.name for c in parsed.find_all(exp.Column) if c.name))
        except Exception:
            meta_tables = []
            meta_columns = []

        entry = {
            "session_id": session_id,
            "query_seq": query_count - 1,
            "timestamp": time.time(),
            "raw_sql": sql,
            "tables": meta_tables,
            "columns": meta_columns,
            "predicates": [],
            "group_by_cols": [],
            "result_rows": result["result_rows"],
            "success": result["success"],
            "error_msg": result.get("error"),
            "execution_ms": result["execution_ms"],
            "plan_tree": [
                {k: v for k, v in node.items()}
                for node in result.get("plan_tree", [])
            ],
        }
        with open(trace_path, "a") as f:
            f.write(json.dumps(entry, default=str) + "\n")

        history.append(("agent", response))
        history.append(("result", result_text))

    elapsed = time.time() - t_start
    conn.close()

    final_answer = None
    for role, text in reversed(history):
        if role == "agent" and has_done(text):
            final_answer = text
            break

    summary = {
        "session_id": session_id,
        "task_key": task_key,
        "task_name": task["name"],
        "question": task["goal"],
        "rep": rep,
        "num_queries": query_count,
        "parse_failures": parse_failures,
        "wall_clock_seconds": round(elapsed, 1),
        "trace_path": str(trace_path),
        "final_answer": final_answer,
    }

    summary_path = TRACE_DIR / f"{session_id}_summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nSession complete: {query_count} queries in {elapsed:.1f}s")

    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", type=str, required=True, choices=list(TASKS.keys()))
    parser.add_argument("--rep", type=str, default="a", choices=["a", "b", "c"])
    args = parser.parse_args()

    print("Loading schema description...")
    conn = psycopg.connect(CONNINFO, autocommit=True)
    schema_desc = get_schema_description(conn)
    conn.close()
    print(f"Schema description: {len(schema_desc)} chars")

    run_session(args.task, args.rep, schema_desc)


if __name__ == "__main__":
    main()
