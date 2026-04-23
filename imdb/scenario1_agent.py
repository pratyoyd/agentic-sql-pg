#!/usr/bin/env python3
"""
Scenario 1 agent: Genre evolution analysis with M1 (workspace) + M2 (intent declaration).
Model: Opus, via claude -p --model opus.
Reps: a, b, c.
"""

import argparse
import json
import re
import subprocess
import time
from pathlib import Path

import psycopg

CONNINFO = "host=localhost port=5434 dbname=agentic_imdb"
SESSION_DIR = Path("sessions")
RAW_PLAN_DIR = Path("raw_plans_scenario1")

MAX_QUERIES = 50
MAX_PARSE_FAILURES = 3
CLAUDE_CMD = "claude"
MODEL = "opus"

# ── Task ──

TASK_GOAL = """\
TASK: Genre evolution analysis, 1990 to 2020.

You are a data analyst for a film industry trends team. The team wants to
understand how genres have evolved over three decades in terms of both
volume (how many films produced) and critical reception (average rating).

Your goal: identify the THREE genres whose critical reception has shifted
MOST dramatically between the 1990s and the 2010s, and for each of those
three genres, characterize the shift in one sentence.

A shift is defined as change in average rating (weighted by vote count)
from 1990-1999 to 2010-2019. Use the movie_info_idx table's rating info
type for ratings. Use movie_info's genre info type for genre classification.

You have full access to the IMDB schema. Explore as you see fit: check
multiple genres, control for film count to avoid low-sample artifacts,
consider whether the shift is driven by a particular sub-era within the
decade, or by specific production companies dominating the genre.

When you have enough evidence, state your three genres with their shifts,
stop, and say "DONE".

First priority is completing the analysis correctly. Use the workspace and
intent-declaration mechanisms opportunistically — they are there to help
you avoid recomputation, not to distract from the analysis."""

# ── M1: Workspace prompt ──

M1_WORKSPACE = """\
WORKSPACE: You have a session-local workspace for sharing intermediate
results across your queries. Using it well will let you answer multi-step
questions much faster.

=== PROTOCOL (FOLLOW EVERY TURN) ===

Every response you write must begin with these three parts, in order:

PART 1 — Catalog check (one line, always):
    -- WORKSPACE STATE: [list current saved names, or "empty"]

PART 2 — Reuse decision (one sentence, always):
    -- REUSE: [name of intermediate you're building on, or "starting fresh because <reason>"]

PART 3 — Your SQL query.

After you receive the query result, your NEXT response begins with a
SAVE DECISION before anything else:
    -- SAVE DECISION: [one of the three options below]
      SAVE <snake_case_name> "<description>" — if the result meets save criteria (below)
      SKIP — if it does not, with one-line reason
      N/A — if the prior query was cheap (< 1 sec) or already a reference to saved data

If you choose SAVE, issue the save call immediately as your next SQL:
    SELECT workspace.save('<name>', '<description>', $$<the original query>$$);

=== CTE CASE (POST-HOC) ===

If the query you just ran had a WITH clause that computed a base
you're likely to reuse (not just a helper for this one query), your
SAVE DECISION has a fourth option:

  SAVE_CTE <snake_case_name> "<desc>" — save the CTE body, not the
    final aggregated output

When you use SAVE_CTE, your next SQL should be:

    SELECT workspace.save('<name>', '<desc>', $$
      <the exact SQL from inside your WITH clause>
    $$);

Then subsequent queries reference the saved intermediate and drop the
WITH clause entirely.

Use SAVE_CTE instead of SAVE when: your query produced a small
aggregated answer but the CTE that fed it is the valuable base
you'll want to re-aggregate later.

=== WHEN TO SAVE ===

Save a computation when all three hold:
  [A] It took 3+ seconds OR joined 4+ tables
  [B] Its result has fewer than 200,000 rows
  [C] The result is a USEFUL BASE for further analysis — not an endpoint

Criterion [C] is the one that matters most. Save the INPUTS to your
analysis, not the OUTPUTS.

GOOD SAVE CANDIDATES (save these):
  - A 5-table join producing 50K rows of (movie, year, genre, rating, votes)
    that you will group, filter, and rank in several different ways
  - A filtered base table (e.g., all director cast_info rows for 1990-2020)
    you will join against multiple other tables
  - An intermediate pre-aggregation at fine granularity (e.g., per-movie,
    per-director) that you will re-aggregate coarser later

BAD SAVE CANDIDATES (do NOT save these):
  - A final aggregated answer (genre → avg_rating by decade). If you need
    it in a different shape later, you'll have to recompute anyway.
  - A narrowly-bucketed result (e.g., ratings by "1990s" and "2010s"
    decade labels). If your analysis pivots to a different bucketing,
    the saved result becomes useless. Save at fine granularity, aggregate
    in the final query.
  - A result under 1 second or under 4 tables — cheap to recompute.
  - A single-use lookup (e.g., "what is the info_type_id for rating").

The single most common mistake is saving aggregated results. Don't. Save
the rich, pre-aggregated join output, then write small fast queries that
group and filter against it.

=== GRANULARITY PRINCIPLE ===

The key principle: save at the FINEST granularity your session will use.
A per-movie, per-person, or per-company rollup is an excellent save —
it's aggregated enough to be fast to reuse, but fine-grained enough that
any coarser aggregation (by year, decade, genre, country) is a cheap
GROUP BY against it.

What makes an aggregation UNSAFE to save is baking in a dimension you
might pivot on. If you bucket ratings into "1990s" and "2010s" and later
need "1990s vs 2000s", the saved result is useless. Keep the
pivot-vulnerable dimensions as raw values, not buckets.

Save: (movie_id, year, genre, rating, votes) — year is raw, bucket in queries
Don't save: (genre, decade_bucket, avg_rating) — decade baked in
Save: (director_id, year, film_count, avg_rating) — year raw
Don't save: (director_id, era, avg_rating) where era ∈ {early, mid, late}
Save: (company_id, year, film_count, total_revenue) — fine grain
Don't save: (company_tier, total_revenue) — tier baked in

=== HOW TO REFERENCE A SAVED INTERMEDIATE ===

When PART 1's catalog shows a saved intermediate, the catalog row contains
its temp_table name. Reference it directly:
    SELECT genre, AVG(rating)
    FROM genre_decade_base     -- the temp_table from the catalog row
    GROUP BY genre;

Prefer writing against a saved intermediate over recomputing the join.

=== FAIL-SAFE ===

If any of this is unclear on a given turn, default to:
  -- WORKSPACE STATE: empty (or list names)
  -- REUSE: starting fresh
  <your query>
  -- SAVE DECISION: SKIP — <reason>
and proceed. Following the protocol matters more than getting it perfect."""

# ── M2: Intent declaration prompt ──

M2_INTENT = """\
INTENT DECLARATION: Separately from the workspace mechanism above, you
have a way to signal when you're about to do REPEATED analysis against
a shared computation. The workspace handles results that survive the
session; intent declaration handles tight clusters of 2-5 queries that
share a subexpression.

=== THE TRIGGER ===

Before you type any of these phrases in your reasoning, STOP and declare
intent first:
  "Let me also check this by <X>"
  "Now let me break this down by <X>"
  "Let me verify across <X>"
  "Let me look at this grouped by <X>, <Y>, <Z>"
  "I want to see the top N and also check <X>"

When you notice yourself about to write a phrase like that — especially
if the previous query was expensive — you're about to issue a cluster
of variants. Declare intent, then issue them.

=== THE DECLARATION ===

Emit this comment block BEFORE your next SQL query:

/*+ MATERIALIZE_INTENT
     name          = 'snake_case_name'
     variants      = <number, 2 to 5>
     shared_base   = <short description of what these queries share>
     diff_clauses  = ['<what varies in variant 1>', '<variant 2>', ...]
     rationale     = '<one sentence: why these specific variants>'
*/

Then issue your first variant.

=== CONCRETE EXAMPLE (follow this pattern) ===

After computing a genre×decade ratings table, you think:
"Let me now check this by country of origin too, and also top films."

That's a cluster of 3 variants on the same base. Declare first:

/*+ MATERIALIZE_INTENT
     name          = 'genre_decade_base'
     variants      = 3
     shared_base   = 'films 1990-2019 with genre, decade, rating, votes,
                      and production_country'
     diff_clauses  = ['GROUP BY genre, decade (already issued)',
                      'GROUP BY country, decade',
                      'ORDER BY rating DESC LIMIT 20']
     rationale     = 'I am checking the shift across three dimensions —
                      genre (done), country (next), and headline titles
                      (after) — all against the same 1990-2019 film pool.'
*/

SELECT ... (the country-by-decade variant)

=== RULES ===

1. Declare intent ONLY when you have 2+ variants clearly in mind. If you
   only have the next query and might or might not follow up, don't
   declare — just issue the query and use SAVE DECISION after.
2. After declaring, issue the variants one per turn. Each variant should
   reference the shared base by name so it's clear which declaration it
   belongs to.
3. If after declaring you decide NOT to issue a planned variant (because
   results changed your plan), write one line before moving on:
       -- ABANDONING variant <i>: <reason>
4. Over-declaring is worse than under-declaring. A declared intent that
   only issues 1 variant wastes materialization cost. Err toward under.

=== WORKSPACE VS INTENT — THE DIFFERENCE ===

Workspace: you computed something. Might need it later. Don't know when
or how. Save it, go on.
  → Decision made AFTER computation.
  → Survives as long as you keep it.

Intent: you're ABOUT to compute something and you already know you'll
need 2-5 variants of it right now.
  → Decision made BEFORE computation.
  → Ephemeral cluster, done in a few turns.

Both can be active at once. Use both. They are not redundant."""

# ── Alias instruction (from v2) ──

ALIAS_INSTRUCTION = """\
**Alias convention for repeated table references.** When a query references
the same base table more than once — either across multiple CTEs, across
multiple subqueries, or through a self-join — use a distinct descriptive
alias for each reference that reflects its semantic role."""


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


def build_system_prompt(schema_desc: str) -> str:
    return f"""You are a data analyst exploring a PostgreSQL 16 database containing the IMDB dataset (Internet Movie Database).

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
- Vote counts are in movie_info_idx where info_type_id = (SELECT id FROM info_type WHERE info = 'votes').
- Genres are in movie_info where info_type_id = (SELECT id FROM info_type WHERE info = 'genres').

Instructions:
- IMPORTANT: Every response MUST follow the protocol defined in the
  WORKSPACE section below. Do NOT emit a bare SQL block. Every response
  begins with the structured markers (WORKSPACE STATE, REUSE, and
  optionally SAVE DECISION), then the SQL query in a ```sql block.
- Write exactly ONE query per response.
- You are querying a PostgreSQL 16 database. Use PostgreSQL-compatible SQL.
- The database is large (74M rows total, cast_info alone is 36M rows). Use JOINs efficiently and add WHERE filters early.
- When you have gathered enough evidence to address the goal, write DONE on its own line and provide a summary of your findings.
- Do not fabricate data. Only draw conclusions from query results you have actually seen.
- Explore the data using a diverse strategy. Try different analytical angles.

{ALIAS_INSTRUCTION}

{M1_WORKSPACE}

{M2_INTENT}

{TASK_GOAL}"""


def extract_sql(response: str) -> str | None:
    match = re.search(r"```sql\s*\n(.*?)```", response, re.DOTALL)
    if match:
        return match.group(1).strip()
    return None


def has_done(response: str) -> bool:
    return bool(re.search(r"^\s*DONE\s*$", response, re.MULTILINE))


def call_claude(prompt: str) -> str:
    result = subprocess.run(
        [CLAUDE_CMD, "-p", "--model", MODEL],
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
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from pg_plan_parser import extract_plan_tree

    t0 = time.time()
    try:
        # Skip EXPLAIN for workspace function calls (they aren't SELECTs on tables)
        is_workspace_call = any(k in sql.lower() for k in
            ['workspace.save', 'workspace.catalog', 'workspace.drop',
             'workspace.touch', 'workspace.dump'])

        plan_tree = []
        if not is_workspace_call:
            try:
                explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"
                plan_row = conn.execute(explain_sql).fetchone()
                plan_json = plan_row[0]
                plan_tree = extract_plan_tree(plan_json)
                if plan_path:
                    plan_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(plan_path, "w") as f:
                        json.dump(plan_json, f, indent=2)
            except Exception:
                pass  # Some queries can't be EXPLAIN'd (e.g., CREATE TEMP TABLE)

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


def run_session(rep: str, schema_desc: str) -> dict:
    session_id = f"scenario1_rep_{rep}"

    conn = psycopg.connect(CONNINFO, autocommit=True)
    conn.execute("SET statement_timeout = '600s'")
    system_prompt = build_system_prompt(schema_desc)

    trace_path = SESSION_DIR / f"{session_id}.jsonl"
    SESSION_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"Session: {session_id}")
    print(f"Model: {MODEL}")
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
            history.append(("result",
                "No SQL query found. Please write a SQL query in a ```sql ... ``` block, or write DONE if finished."))
            continue

        parse_failures = 0
        query_count += 1

        print(f"  SQL: {sql[:120]}{'...' if len(sql) > 120 else ''}")

        plan_path = RAW_PLAN_DIR / f"rep_{rep}" / f"{query_count - 1}.json"
        result = execute_and_log(conn, sql, plan_path)
        result_text = format_result(result)
        print(f"  Result: {result['result_rows']} rows, success={result['success']}, {result['execution_ms']:.0f}ms")

        # Metadata extraction via sqlglot
        try:
            import sqlglot
            from sqlglot import exp
            parsed = sqlglot.parse_one(sql, dialect="postgres")
            meta_tables = sorted(set(t.name for t in parsed.find_all(exp.Table) if t.name))
            meta_columns = sorted(set(c.name for c in parsed.find_all(exp.Column) if c.name))
        except Exception:
            meta_tables = []
            meta_columns = []

        # Detect workspace / M2 patterns
        sql_lower = sql.lower()
        is_ws_save = 'workspace.save' in sql_lower
        is_ws_catalog = 'workspace.catalog' in sql_lower
        is_ws_drop = 'workspace.drop' in sql_lower
        has_intent = 'materialize_intent' in response.upper()
        has_abandon = '-- abandoning variant' in response.lower()

        entry = {
            "session_id": session_id,
            "query_seq": query_count - 1,
            "timestamp": time.time(),
            "raw_sql": sql,
            "agent_response": response,
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
            "workspace_save": is_ws_save,
            "workspace_catalog": is_ws_catalog,
            "workspace_drop": is_ws_drop,
            "m2_intent_declared": has_intent,
            "m2_abandon": has_abandon,
        }
        with open(trace_path, "a") as f:
            f.write(json.dumps(entry, default=str) + "\n")

        history.append(("agent", response))
        history.append(("result", result_text))

    elapsed = time.time() - t_start

    # Dump workspace activity
    try:
        ws_row = conn.execute("SELECT workspace.dump_activity()").fetchone()
        ws_json = ws_row[0] if ws_row else {}
        ws_path = SESSION_DIR / f"{session_id}_workspace.json"
        with open(ws_path, "w") as f:
            json.dump(ws_json, f, indent=2)
        print(f"  Workspace activity saved to {ws_path}")
    except Exception as e:
        print(f"  Warning: could not dump workspace activity: {e}")
        ws_json = {}

    conn.close()

    final_answer = None
    for role, text in reversed(history):
        if role == "agent" and has_done(text):
            final_answer = text
            break

    summary = {
        "session_id": session_id,
        "scenario": "scenario1_genre_evolution",
        "rep": rep,
        "model": MODEL,
        "num_queries": query_count,
        "parse_failures": parse_failures,
        "wall_clock_seconds": round(elapsed, 1),
        "trace_path": str(trace_path),
        "final_answer": final_answer,
        "workspace_activity": ws_json,
    }

    summary_path = SESSION_DIR / f"{session_id}_summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nSession complete: {query_count} queries in {elapsed:.1f}s")

    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rep", type=str, default="a", choices=["a", "b", "c"])
    args = parser.parse_args()

    print("Loading schema description...")
    conn = psycopg.connect(CONNINFO, autocommit=True)
    schema_desc = get_schema_description(conn)
    conn.close()
    print(f"Schema description: {len(schema_desc)} chars")

    run_session(args.rep, schema_desc)


if __name__ == "__main__":
    main()
