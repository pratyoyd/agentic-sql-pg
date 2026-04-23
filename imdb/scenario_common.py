#!/usr/bin/env python3
"""
Shared infrastructure for scenarios 2-10.
Agent harness, report generation, M1/M2 prompt blocks.
"""

import json
import re
import subprocess
import time
from pathlib import Path

import psycopg

CONNINFO = "host=localhost port=5434 dbname=agentic_imdb"
SESSION_DIR = Path("sessions")
RAW_PLAN_DIR = Path("raw_plans")
CLAUDE_CMD = "claude"
MODEL = "opus"

# ── Scenario registry ──

SCENARIOS = {
    2: {
        "name": "director_career",
        "title": "Director Career Trajectory",
        "reps": ["a", "b"],
        "max_queries": 25,
        "task": """\
TASK: Director career trajectory analysis.

You are a data analyst studying how directors' careers evolve. Among
directors who directed at least 10 films between 1990 and 2020, identify
THREE CAREER PATTERNS that distinguish directors whose average critical
rating ROSE over the course of their career from those whose average
FELL.

For each pattern, give the pattern a short name (e.g., "genre
consolidation", "budget scaling") and characterize it in one sentence
with supporting evidence from the data.

Use movie_info_idx for ratings. Cast_info with the appropriate role_type
identifies directors. You have full access to the IMDB schema.

When you have enough evidence, state your three patterns with their
evidence, stop, and say "DONE".

First priority is completing the analysis correctly. Use the workspace
and intent-declaration mechanisms opportunistically — they are there to
help you avoid recomputation, not to distract from the analysis.""",
    },
    3: {
        "name": "company_genre_shifts",
        "title": "Production Company Genre Shifts",
        "reps": ["a", "b"],
        "max_queries": 25,
        "task": """\
TASK: Production company trajectory analysis.

You are a data analyst studying how production companies evolve their
creative output. Among production companies with 50 or more films in the
database, identify the THREE COMPANIES that most dramatically shifted
their genre mix between the 1990s and the 2010s.

For each company, characterize in one sentence: what genres they moved
away from, what genres they moved toward, and whether the shift
correlated with a change in average critical rating.

Use movie_companies to connect films to companies, company_name for
names, movie_info for genre classification, and movie_info_idx for
ratings.

When you have enough evidence, state the three companies with their
shifts, stop, and say "DONE".

First priority is completing the analysis correctly. Use the workspace
and intent-declaration mechanisms opportunistically.""",
    },
    4: {
        "name": "cast_size_rating",
        "title": "Cast Size x Rating x Era",
        "reps": ["a", "b"],
        "max_queries": 25,
        "task": """\
TASK: Cast size, rating, and era analysis.

You are a data analyst investigating whether the relationship between
cast size and critical rating has changed over time. Cast size is the
number of distinct actors/actresses in a film per cast_info.

Goal: quantify how the cast-size-to-rating relationship shifted from
the 1990s to the 2010s. Then check whether this shift holds across all
major genres (the top 5 by film count), or whether it is concentrated
in specific genres.

State your findings as: (a) the overall shift in one sentence with
specific numbers, and (b) which genres show the shift and which do not,
with evidence.

Use cast_info for cast, title for films, movie_info_idx for ratings,
movie_info for genre.

When you have enough evidence, stop and say "DONE".

First priority is completing the analysis correctly.""",
    },
    5: {
        "name": "international_coprod",
        "title": "International Co-Production Trends",
        "reps": ["a", "b"],
        "max_queries": 25,
        "task": """\
TASK: International co-production trend analysis.

You are a data analyst studying film industry globalization. Identify
how co-productions between US and non-US companies evolved from 1990 to
2020, specifically in terms of:
  - Volume over time (how many co-productions per year)
  - Genre mix (which genres are most common in co-productions vs
    single-country productions)
  - Critical reception (do co-productions rate higher or lower than
    single-country films, and does this change over time)

Co-productions are films where movie_companies contains both a US-based
and a non-US-based company. Use company_name for country codes, title
for kind filtering (kind_id = 1 for movies), movie_info_idx for ratings.

State your findings as three numbered observations, each backed by a
specific number from the data.

When you have enough evidence, stop and say "DONE".""",
    },
    6: {
        "name": "franchise_durability",
        "title": "Franchise Durability",
        "reps": ["a", "b"],
        "max_queries": 25,
        "task": """\
TASK: Franchise decay or durability analysis.

You are a data analyst studying film franchises. Find franchises that
have at least 5 films released between 2000 and 2020.

Goal: identify THE THREE FRANCHISES that maintained their rating best
across installments, and THE THREE that declined most. Characterize
what distinguishes the two groups: what do the rating-maintainers have
in common, and what do the decliners have in common.

IMPORTANT DATA NOTE: movie_link's "follows"/"followed by" links are
used almost exclusively for TV episodes, not theatrical films. Do NOT
attempt to build franchise chains from movie_link — it will return
zero results for kind_id=1 movies. Instead, identify franchises by
title patterns (e.g., LIKE 'Harry Potter%', regex for numbered
sequels like 'Rocky II', 'Rocky III'). You can define a set of
well-known franchise patterns and match against title.

Use title for films, movie_info_idx for ratings. You may also use
movie_companies and cast_info to characterize what distinguishes
the two groups (e.g., director continuity, studio consistency).

When you have enough evidence, state both groups with their
distinguishing characteristics, stop, and say "DONE".

First priority is completing the analysis correctly. Use the workspace
and intent-declaration mechanisms opportunistically.""",
    },
    7: {
        "name": "writer_director",
        "title": "Writer-Director Separation Impact",
        "reps": ["a", "b"],
        "max_queries": 25,
        "task": """\
TASK: Writer-director separation impact analysis.

You are a data analyst studying whether films where the writer and
director are the same person rate differently from films where they are
separate people.

Goal: (a) quantify the overall rating difference between
writer-is-director films and writer-and-director-separate films across
1990-2020, and (b) check whether this pattern varies by decade (1990s,
2000s, 2010s) and by genre (focus on the top 5 genres by film count).

Writers and directors are identified in cast_info via role_type. You
need to self-join cast_info to find films where the same person appears
in both roles for the same movie.

State your findings as: (a) overall effect size with a number, and
(b) a breakdown showing where the pattern is strongest and weakest.

When you have enough evidence, stop and say "DONE".""",
    },
    8: {
        "name": "actor_archetypes",
        "title": "Actor Career Archetypes",
        "reps": ["a", "b"],
        "max_queries": 25,
        "task": """\
TASK: Actor career diversification analysis.

You are a data analyst studying actor careers. Among actors with 20 or
more credits in the database, identify THREE DISTINCT CAREER ARCHETYPES
based on the role types they played (lead/supporting/cameo or similar)
over time.

For each archetype: give it a name (e.g., "lead-to-supporting
transition", "consistent character actor"), describe the typical
role-type trajectory in one sentence, and give 2-3 example actors from
the data whose career fits the archetype.

Role types are in cast_info.role_id joining to role_type. Billing
position (cast_info.nr_order) helps distinguish lead from supporting.
Use name for actor names, title for films.

When you have enough evidence, state your three archetypes with
examples, stop, and say "DONE".""",
    },
    9: {
        "name": "series_film_spillover",
        "title": "Series-to-Film Spillover",
        "reps": ["a", "b"],
        "max_queries": 25,
        "task": """\
TASK: Series-to-film spillover analysis.

You are a data analyst studying career mobility between television
series and feature films. Identify actors who moved substantially
between TV series (kind_type series or tv series) and feature films
(kind_type movie) between 2000 and 2020.

Substantial movement means: at least 10 credits in EACH medium during
the window.

Goal: (a) how many actors fit this criterion, (b) for those actors,
characterize whether their critical rating on films is higher, lower,
or similar compared to actors who stayed predominantly in one medium,
and (c) identify THREE EXAMPLE ACTORS whose cross-medium movement
correlates with notable rating changes.

Use cast_info for credits, title.kind_id for medium type, kind_type for
names, movie_info_idx for ratings, name for actor names.

When you have enough evidence, state your answers to (a), (b), and
(c), stop, and say "DONE".""",
    },
    10: {
        "name": "budget_correlation",
        "title": "Budget Era Correlation",
        "reps": ["a", "b"],
        "max_queries": 25,
        "task": """\
TASK: Budget-rating correlation analysis across eras.

You are a data analyst studying the relationship between production
budget and critical rating over time. Budget information is in
movie_info with info_type for budget.

Goal: (a) compute the correlation between budget and rating across
three decades (1990s, 2000s, 2010s) to identify how the relationship
has evolved, and (b) find THREE BUDGET TIERS where the correlation is
strongest and THREE where it is weakest, then characterize why.

Use movie_info for budget (filter to USD-denominated entries, parse
numeric values), movie_info_idx for ratings, title for films.

Note: many budget entries will be in non-USD currencies or non-numeric
formats. You'll need to filter those out.

State your findings as: (a) correlation by decade with numbers, and
(b) the strongest and weakest budget tiers with a one-sentence
explanation each.

When you have enough evidence, stop and say "DONE".""",
    },
}


# ── Updated Block A (with SAVE_CTE) ──

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

    -- SAVE DECISION: [one of the four options below]
      SAVE <snake_case_name> "<description>" — save the query result as a base
      SAVE_CTE <snake_case_name> "<description>" — save the CTE body instead of the query output (see CTE CASE below)
      SKIP — if the query result does not meet save criteria, with one-line reason
      N/A — if the prior query was cheap (< 1 sec) or already a reference to saved data

If you choose SAVE, issue the save call immediately as your next SQL:
    SELECT workspace.save('<name>', '<description>', $$<the original query>$$);

If you choose SAVE_CTE, extract the CTE body from your previous query
and save it:
    SELECT workspace.save('<name>', '<description>', $$<the exact SQL from inside your WITH clause>$$);

=== WHEN TO SAVE ===

Save a computation when all three hold:
  [A] It took 3+ seconds OR joined 4+ tables
  [B] Its result has fewer than 200,000 rows
  [C] The result is a USEFUL BASE for further analysis — not an endpoint

Criterion [C] is the one that matters most. Save the INPUTS to your
analysis, not the OUTPUTS.

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

GOOD SAVE CANDIDATES (save these):
  - A 5-table join producing 50K rows of (movie, year, genre, rating, votes)
    that you will group, filter, and rank in several different ways
  - A filtered base table (e.g., all director cast_info rows for 1990-2020)
    you will join against multiple other tables
  - An intermediate pre-aggregation at fine granularity (e.g., per-movie,
    per-director) that you will re-aggregate coarser later

BAD SAVE CANDIDATES (do NOT save these):
  - A result under 1 second or under 4 tables — cheap to recompute.
  - A single-use lookup (e.g., "what is the info_type_id for rating").
  - A result with a dimension baked in that you might pivot away from.

=== CTE CASE (POST-HOC) ===

If the query you just ran had a WITH clause that computed a base you're
likely to reuse (not just a helper for this one query), your SAVE
DECISION has a fourth option: SAVE_CTE.

Use SAVE_CTE instead of SAVE when your query produced a small aggregated
answer but the CTE that fed it is the valuable base you'll want to
re-aggregate later.

Example situation: you write
    WITH base AS (SELECT <5-table join on films>)
    SELECT genre, AVG(rating) FROM base GROUP BY genre;

The output is a tiny aggregated table (bad save candidate). But the
base CTE is a 5-table join you'll want to re-aggregate by decade,
country, etc. Save the CTE body, not the final output:

    -- SAVE DECISION: SAVE_CTE films_base "5-table film base 1990-2019"
    SELECT workspace.save('films_base', '5-table film base 1990-2019', $$
      SELECT <the exact SELECT that was inside your WITH clause>
    $$);

Then all subsequent queries reference films_base directly instead of
re-writing the WITH clause.

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


# ── Block B (unchanged) ──

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


ALIAS_INSTRUCTION = """\
**Alias convention for repeated table references.** When a query references
the same base table more than once — either across multiple CTEs, across
multiple subqueries, or through a self-join — use a distinct descriptive
alias for each reference that reflects its semantic role."""


# ── Schema description ──

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


def build_system_prompt(schema_desc: str, task_goal: str) -> str:
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

{task_goal}"""


# ── SQL extraction and LLM ──

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
                pass

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


# ── Session runner ──

MAX_PARSE_FAILURES = 3


def run_session(scenario_id: int, rep: str, schema_desc: str) -> dict:
    sc = SCENARIOS[scenario_id]
    scenario_name = sc["name"]
    max_queries = sc["max_queries"]
    task_goal = sc["task"]

    session_id = f"scenario{scenario_id}_rep_{rep}"
    plan_dir = RAW_PLAN_DIR / f"scenario{scenario_id}" / f"rep_{rep}"

    conn = psycopg.connect(CONNINFO, autocommit=True)
    conn.execute("SET statement_timeout = '600s'")
    system_prompt = build_system_prompt(schema_desc, task_goal)

    trace_path = SESSION_DIR / f"{session_id}.jsonl"
    SESSION_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"Session: {session_id} ({sc['title']})")
    print(f"Model: {MODEL}, Max queries: {max_queries}")
    print(f"{'='*60}\n")

    history = []
    query_count = 0
    parse_failures = 0
    t_start = time.time()

    while query_count < max_queries and parse_failures < MAX_PARSE_FAILURES:
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

        plan_path = plan_dir / f"{query_count - 1}.json"
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
        "scenario": f"scenario{scenario_id}_{scenario_name}",
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


# ── Report generation ──

def load_trace(scenario_id: int, rep: str) -> list[dict]:
    path = SESSION_DIR / f"scenario{scenario_id}_rep_{rep}.jsonl"
    if not path.exists():
        return []
    return [json.loads(l) for l in open(path)]


def load_summary(scenario_id: int, rep: str) -> dict | None:
    path = SESSION_DIR / f"scenario{scenario_id}_rep_{rep}_summary.json"
    if not path.exists():
        return None
    return json.load(open(path))


def load_workspace(scenario_id: int, rep: str) -> dict | None:
    path = SESSION_DIR / f"scenario{scenario_id}_rep_{rep}_workspace.json"
    if not path.exists():
        return None
    return json.load(open(path))


def compute_per_rep(scenario_id: int, rep: str) -> dict | None:
    trace = load_trace(scenario_id, rep)
    summary = load_summary(scenario_id, rep)
    if not trace or not summary:
        return None

    query_count = len(trace)
    successful = [r for r in trace if r.get("success")]
    tables_per_q = [len(r.get("tables", [])) for r in successful]
    plan_depths = []
    for r in successful:
        pt = r.get("plan_tree", [])
        if pt:
            plan_depths.append(max(n.get("node_id", 0) for n in pt) + 1)

    db_compute_ms = sum(r.get("execution_ms", 0) for r in trace)

    return {
        "rep": rep,
        "query_count": query_count,
        "tables_mean": round(sum(tables_per_q) / len(tables_per_q), 1) if tables_per_q else 0,
        "tables_p50": sorted(tables_per_q)[len(tables_per_q) // 2] if tables_per_q else 0,
        "tables_p95": sorted(tables_per_q)[int(len(tables_per_q) * 0.95)] if tables_per_q else 0,
        "plan_depth_mean": round(sum(plan_depths) / len(plan_depths), 1) if plan_depths else 0,
        "plan_depth_max": max(plan_depths) if plan_depths else 0,
        "wall_clock_s": summary.get("wall_clock_seconds", 0),
        "db_compute_s": round(db_compute_ms / 1000, 1),
        "final_answer": summary.get("final_answer") is not None,
        "done_clean": summary.get("final_answer") is not None,
    }


def compute_m1_metrics(scenario_id: int, reps_data: list[str]) -> dict:
    totals = {
        "save_calls": 0, "save_cte_calls": 0, "state_acknowledged": 0,
        "reuse_count": 0,
        "save_but_never_reused": 0, "qualified_but_not_saved": 0,
        "total_queries": 0,
        "save_decisions_emitted": 0, "skip_decisions_emitted": 0,
        "protocol_turns": 0,
    }

    for rep in reps_data:
        trace = load_trace(scenario_id, rep)
        ws = load_workspace(scenario_id, rep)
        if not trace:
            continue

        save_calls = sum(1 for r in trace if r.get("workspace_save"))
        totals["save_calls"] += save_calls

        for r in trace:
            resp = r.get("agent_response", "")
            has_ws = bool(re.search(r'-- WORKSPACE STATE:', resp))
            has_reuse = bool(re.search(r'-- REUSE:', resp))
            has_save_dec = bool(re.search(r'-- SAVE DECISION:', resp))
            if has_ws or has_reuse or has_save_dec:
                totals["protocol_turns"] += 1
            if re.search(r'-- SAVE DECISION.*?:\s*SAVE_CTE\b', resp):
                totals["save_cte_calls"] += 1
            if re.search(r'-- SAVE DECISION.*?:\s*SAVE(?:_CTE)?\b', resp):
                totals["save_decisions_emitted"] += 1
            if re.search(r'-- SAVE DECISION.*?:\s*SKIP\b', resp):
                totals["skip_decisions_emitted"] += 1
            if has_ws:
                m = re.search(r'-- WORKSPACE STATE:\s*(.+)', resp)
                if m:
                    val = m.group(1).strip().lower()
                    if val and val not in ("empty", "none", "n/a", "{}"):
                        totals["state_acknowledged"] += 1

        saved_tables = set()
        if ws and "activity" in ws:
            for evt in ws["activity"]:
                if evt.get("call_type") == "save" and evt.get("payload"):
                    hint = evt["payload"].get("usage_hint", "")
                    m = re.search(r'FROM\s+(\S+)', hint)
                    if m:
                        saved_tables.add(m.group(1))

        reuse_count = 0
        reused_names = set()
        for r in trace:
            sql = r.get("raw_sql", "").lower()
            for t in saved_tables:
                if t.lower() in sql and not r.get("workspace_save"):
                    reuse_count += 1
                    reused_names.add(t)
                    break

        totals["reuse_count"] += reuse_count
        totals["save_but_never_reused"] += len(saved_tables - reused_names)

        for r in trace:
            if not r.get("success"):
                continue
            ms = r.get("execution_ms", 0)
            ntables = len(r.get("tables", []))
            rows = r.get("result_rows", 0)
            if (ms > 3000 or ntables >= 4) and rows < 200000:
                if not r.get("workspace_save"):
                    totals["qualified_but_not_saved"] += 1

        totals["total_queries"] += len(trace)

    totals["reuse_rate"] = (
        round(totals["reuse_count"] / totals["save_calls"], 2)
        if totals["save_calls"] > 0 else 0
    )
    totals["state_ack_rate"] = (
        round(totals["state_acknowledged"] /
              max(1, totals["total_queries"] - totals["save_calls"]), 2)
    )
    return totals


def compute_m2_metrics(scenario_id: int, reps_data: list[str]) -> dict:
    totals = {
        "declare_calls": 0, "declared_variants_total": 0,
        "actual_variants_issued": 0, "abandonment_with_justification": 0,
    }

    for rep in reps_data:
        trace = load_trace(scenario_id, rep)
        if not trace:
            continue

        for r in trace:
            resp = r.get("agent_response", "")
            intents = re.findall(r'/\*\+\s*MATERIALIZE_INTENT.*?\*/', resp, re.DOTALL)
            totals["declare_calls"] += len(intents)
            for intent in intents:
                m = re.search(r"variants\s*=\s*(\d+)", intent)
                if m:
                    totals["declared_variants_total"] += int(m.group(1))
            abandons = re.findall(r'--\s*ABANDONING\s+variant', resp, re.IGNORECASE)
            totals["abandonment_with_justification"] += len(abandons)

    totals["declaration_precision"] = (
        round(totals["actual_variants_issued"] / totals["declared_variants_total"], 2)
        if totals["declared_variants_total"] > 0 else 0
    )
    return totals


def generate_scenario_report(scenario_id: int):
    sc = SCENARIOS[scenario_id]
    reps = sc["reps"]
    report_path = Path("reports") / f"scenario{scenario_id}_{sc['name']}.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    completed_reps = [r for r in reps if load_summary(scenario_id, r) is not None]
    if not completed_reps:
        print(f"Scenario {scenario_id}: no completed reps found.")
        return

    lines = []
    lines.append(f"# Scenario {scenario_id}: {sc['title']} — Report\n")
    lines.append("## Header\n")
    lines.append(f"- **Scenario**: {sc['title']}")
    lines.append(f"- **Model**: Opus (via claude -p --model opus)")
    lines.append(f"- **Max queries**: {sc['max_queries']}")
    lines.append(f"- **Reps completed**: {len(completed_reps)} / {len(reps)}")

    total_wall = sum(load_summary(scenario_id, r).get("wall_clock_seconds", 0) for r in completed_reps)
    total_db = sum(sum(t.get("execution_ms", 0) for t in load_trace(scenario_id, r)) / 1000
                   for r in completed_reps)
    lines.append(f"- **Total wall-clock**: {total_wall:.0f}s")
    lines.append(f"- **Total DB compute**: {total_db:.1f}s")
    lines.append("")

    # Per-rep summary
    lines.append("## Per-Rep Summary\n")
    lines.append("| Rep | Queries | Tables/q (mean/p50/p95) | Plan depth (mean/max) | Wall-clock | DB compute | Final answer | DONE |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for rep in completed_reps:
        d = compute_per_rep(scenario_id, rep)
        if d:
            lines.append(
                f"| {rep} | {d['query_count']} | "
                f"{d['tables_mean']}/{d['tables_p50']}/{d['tables_p95']} | "
                f"{d['plan_depth_mean']}/{d['plan_depth_max']} | "
                f"{d['wall_clock_s']:.0f}s | {d['db_compute_s']:.1f}s | "
                f"{'yes' if d['final_answer'] else 'no'} | "
                f"{'yes' if d['done_clean'] else 'no'} |"
            )
    lines.append("")

    # M1 metrics
    m1 = compute_m1_metrics(scenario_id, completed_reps)
    lines.append("## M1 Metrics (Workspace)\n")
    lines.append(f"- **m1_save_calls**: {m1['save_calls']}")
    lines.append(f"- **m1_save_cte_calls**: {m1['save_cte_calls']}")
    lines.append(f"- **m1_state_acknowledged**: {m1['state_acknowledged']} (turns where agent acknowledged saved content)")
    lines.append(f"- **m1_state_ack_rate**: {m1['state_ack_rate']:.0%}")
    lines.append(f"- **m1_reuse_count**: {m1['reuse_count']} (queries referencing saved temp tables)")
    lines.append(f"- **m1_reuse_rate**: {m1['reuse_rate']} (reuse_count / save_calls)")
    lines.append(f"- **m1_save_but_never_reused**: {m1['save_but_never_reused']}")
    lines.append(f"- **m1_qualified_but_not_saved**: {m1['qualified_but_not_saved']}")
    lines.append(f"- **protocol_turns**: {m1['protocol_turns']} / {m1['total_queries']} (turns with any protocol marker)")
    lines.append(f"- **save_decisions_emitted**: {m1['save_decisions_emitted']} SAVE/SAVE_CTE, {m1['skip_decisions_emitted']} SKIP")
    lines.append("")

    # M2 metrics
    m2 = compute_m2_metrics(scenario_id, completed_reps)
    lines.append("## M2 Metrics (Intent Declaration)\n")
    lines.append(f"- **m2_declare_calls**: {m2['declare_calls']}")
    lines.append(f"- **m2_declared_variants_total**: {m2['declared_variants_total']}")
    lines.append(f"- **m2_abandonment_with_justification**: {m2['abandonment_with_justification']}")
    lines.append("")

    # Signal assessment when all reps done
    if len(completed_reps) == len(reps):
        lines.append("## Signal Assessment\n")
        save_per_session = m1["save_calls"] / len(completed_reps)
        state_ack_rate = m1["state_ack_rate"]

        signals = []
        if 1 <= save_per_session <= 3:
            signals.append("GREEN: save_calls 1-3/session")
        elif save_per_session > 0:
            signals.append(f"YELLOW: save_calls {save_per_session:.1f}/session (outside 1-3 range)")
        else:
            signals.append("RED: zero workspace saves despite opportunities")

        if state_ack_rate >= 0.6:
            signals.append(f"GREEN: state acknowledgment rate {state_ack_rate:.0%} ≥ 60%")
        elif state_ack_rate > 0:
            signals.append(f"YELLOW: state acknowledgment rate {state_ack_rate:.0%} < 60%")
        else:
            signals.append("RED: no workspace state acknowledgment")

        if m1["save_cte_calls"] > 0:
            signals.append(f"GREEN: {m1['save_cte_calls']} SAVE_CTE decisions emitted")
        else:
            signals.append("YELLOW: no SAVE_CTE decisions")

        if m2["declare_calls"] > 0:
            signals.append(f"GREEN: {m2['declare_calls']} intent declarations emitted")
        else:
            signals.append("YELLOW: no MATERIALIZE_INTENT declarations")

        for s in signals:
            lines.append(f"- {s}")
        lines.append("")

    with open(report_path, "w") as f:
        f.write("\n".join(lines))
    print(f"Report written to {report_path} ({len(completed_reps)} reps)")


# ── Aggregate report ──

def generate_aggregate_report():
    report_path = Path("reports") / "aggregate_scenarios_2_to_10.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    lines = []
    lines.append("# Aggregate Report: Scenarios 2–10\n")
    lines.append(f"- **Total scenarios**: 9")
    lines.append(f"- **Reps per scenario**: 2")
    lines.append(f"- **Model**: Opus")
    lines.append("")

    # Summary table
    lines.append("## Per-Scenario Summary\n")
    lines.append("| # | Scenario | Reps done | Queries (total) | Wall-clock | DB compute | Save calls | SAVE_CTE | Reuse count | Protocol % | M2 intents | DONE rate |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|")

    all_m1 = {}
    total_reps_with_save = 0
    total_reps_with_save_cte = 0
    total_reps_with_m2 = 0
    total_completed = 0

    for sid in range(2, 11):
        sc = SCENARIOS[sid]
        reps = sc["reps"]
        completed = [r for r in reps if load_summary(sid, r) is not None]
        if not completed:
            lines.append(f"| {sid} | {sc['title']} | 0/{len(reps)} | — | — | — | — | — | — | — | — | — |")
            continue

        total_completed += len(completed)
        m1 = compute_m1_metrics(sid, completed)
        m2 = compute_m2_metrics(sid, completed)
        all_m1[sid] = m1

        total_queries = m1["total_queries"]
        total_wall = sum(load_summary(sid, r).get("wall_clock_seconds", 0) for r in completed)
        total_db = sum(sum(t.get("execution_ms", 0) for t in load_trace(sid, r)) / 1000 for r in completed)
        done_count = sum(1 for r in completed if load_summary(sid, r).get("final_answer") is not None)
        protocol_pct = round(100 * m1["protocol_turns"] / max(1, total_queries))

        # Per-rep breakdown for adoption rates
        for r in completed:
            rep_m1 = compute_m1_metrics(sid, [r])
            if rep_m1["save_calls"] > 0:
                total_reps_with_save += 1
            if rep_m1["save_cte_calls"] > 0:
                total_reps_with_save_cte += 1
            rep_m2 = compute_m2_metrics(sid, [r])
            if rep_m2["declare_calls"] > 0:
                total_reps_with_m2 += 1

        lines.append(
            f"| {sid} | {sc['title']} | {len(completed)}/{len(reps)} | "
            f"{total_queries} | {total_wall:.0f}s | {total_db:.1f}s | "
            f"{m1['save_calls']} | {m1['save_cte_calls']} | {m1['reuse_count']} | "
            f"{protocol_pct}% | {m2['declare_calls']} | {done_count}/{len(completed)} |"
        )

    lines.append("")

    # Cross-scenario metrics
    lines.append("## Cross-Scenario Adoption Rates\n")
    lines.append(f"- **M1 save adoption**: {total_reps_with_save}/{total_completed} reps had at least one save ({round(100*total_reps_with_save/max(1,total_completed))}%)")
    lines.append(f"- **SAVE_CTE trigger rate**: {total_reps_with_save_cte}/{total_completed} reps used SAVE_CTE ({round(100*total_reps_with_save_cte/max(1,total_completed))}%)")
    lines.append(f"- **M2 trigger rate**: {total_reps_with_m2}/{total_completed} reps had MATERIALIZE_INTENT ({round(100*total_reps_with_m2/max(1,total_completed))}%)")
    lines.append("")

    # Compute reduction estimate
    lines.append("## Compute Reduction Estimate\n")
    lines.append("| Scenario | Total DB (s) | Reuse queries | Avg reuse query (ms) | Estimated base cost (ms) | Est. savings |")
    lines.append("|---|---|---|---|---|---|")

    for sid in range(2, 11):
        sc = SCENARIOS[sid]
        completed = [r for r in sc["reps"] if load_summary(sid, r) is not None]
        if not completed:
            continue
        m1 = compute_m1_metrics(sid, completed)
        total_db_ms = sum(sum(t.get("execution_ms", 0) for t in load_trace(sid, r)) for r in completed)
        reuse_count = m1["reuse_count"]

        # Estimate: each reuse query avoided re-running the base join
        # Approximate base cost = total_db / total_queries * average_table_count_factor
        if reuse_count > 0 and m1["total_queries"] > 0:
            # Avg cost of non-reuse queries as proxy for what reuse saved
            all_traces = []
            for r in completed:
                all_traces.extend(load_trace(sid, r))
            save_queries = [t for t in all_traces if t.get("workspace_save")]
            avg_save_ms = sum(t.get("execution_ms", 0) for t in save_queries) / max(1, len(save_queries))
            reuse_queries = []
            ws = None
            for r in completed:
                ws = load_workspace(sid, r)
                if ws:
                    break
            saved_tables = set()
            if ws and "activity" in ws:
                for evt in ws["activity"]:
                    if evt.get("call_type") == "save" and evt.get("payload"):
                        hint = evt["payload"].get("usage_hint", "")
                        mm = re.search(r'FROM\s+(\S+)', hint)
                        if mm:
                            saved_tables.add(mm.group(1).lower())
            for t in all_traces:
                sql_lower = t.get("raw_sql", "").lower()
                if any(st in sql_lower for st in saved_tables) and not t.get("workspace_save"):
                    reuse_queries.append(t)
            avg_reuse_ms = sum(t.get("execution_ms", 0) for t in reuse_queries) / max(1, len(reuse_queries))
            est_savings_ms = reuse_count * max(0, avg_save_ms - avg_reuse_ms)
            lines.append(
                f"| {sid} | {total_db_ms/1000:.1f} | {reuse_count} | "
                f"{avg_reuse_ms:.0f} | {avg_save_ms:.0f} | ~{est_savings_ms/1000:.1f}s |"
            )
        else:
            lines.append(f"| {sid} | {total_db_ms/1000:.1f} | 0 | — | — | — |")

    lines.append("")

    # M1 pattern quality ranking
    lines.append("## M1 Pattern Quality Ranking\n")
    ranked = []
    for sid in range(2, 11):
        sc = SCENARIOS[sid]
        completed = [r for r in sc["reps"] if load_summary(sid, r) is not None]
        if not completed:
            continue
        m1 = compute_m1_metrics(sid, completed)
        # Score: protocol compliance + save adoption + reuse
        score = (
            m1["protocol_turns"] / max(1, m1["total_queries"]) * 40 +
            min(m1["save_calls"] / max(1, len(completed)), 3) / 3 * 30 +
            min(m1["reuse_rate"], 10) / 10 * 30
        )
        ranked.append((sid, sc["title"], score, m1))

    ranked.sort(key=lambda x: -x[2])
    for sid, title, score, m1 in ranked:
        quality = "STRONG" if score >= 60 else "MODERATE" if score >= 30 else "WEAK"
        lines.append(f"- **{quality}**: Scenario {sid} ({title}) — score {score:.0f}/100 "
                      f"(saves={m1['save_calls']}, reuse={m1['reuse_count']}, "
                      f"protocol={m1['protocol_turns']}/{m1['total_queries']})")
    lines.append("")

    with open(report_path, "w") as f:
        f.write("\n".join(lines))
    print(f"Aggregate report written to {report_path}")
