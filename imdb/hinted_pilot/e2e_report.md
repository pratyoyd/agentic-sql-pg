D# End-to-End Single-Query Validation Report

## Overview

This test validates the full hint mechanism pipeline on a single query: agent generates SQL under a new alias instruction, a hint is constructed from earlier queries in the same session, pg_hint_plan applies it, and the plan and latency are measured.

## Part 1: Recording under alias instruction

**Agent**: Claude Sonnet, task 2 (director-actor collaborations), 1 rep
**Trace**: `traces_v2/task2_rep_a.jsonl`
**Queries**: 12 successful (13 total including DONE)
**Wall clock**: 430s

### Alias instruction

Added to system prompt:
> **Alias convention for repeated table references.** When a query references the same base table more than once — either across multiple CTEs, across multiple subqueries, or through a self-join — use a distinct descriptive alias for each reference that reflects its semantic role. For example, if you are joining `cast_info` to itself to find directors and actors, use `cast_info ci_dir` in one location and `cast_info ci_act` in the other, never two copies of `cast_info ci`. This applies only when the same base table appears more than once in the same query; single references can keep short aliases as usual.

### Alias compliance

Of 8 queries with repeated base tables in the join tree: **8/8 (100%) compliant**.

Examples of distinct aliases adopted:
- `cast_info`: `ci_dir` / `ci_act`
- `name`: `nd` / `na`
- `movie_info_idx`: `mii` / `mii_votes`

The `role_type` and `info_type` duplicates in scalar subqueries (`SELECT id FROM role_type WHERE role = 'director'`) become InitPlans and do not participate in the join tree, so they cause no pg_hint_plan ambiguity.

### Hintable join surface

With the new aliases: **61/65 join nodes (93.8%) hintable** — up from 14.9% with original traces.

## Part 2: Bug fixes

### Bug 1: Signature function (`pg_plan_parser.py`)

**Problem**: `_operator_signature` hashed only the current node's `Relation Name` (always `[]` for join nodes). All Nested Loops with the same predicate got the same hash.

**Fix**: For join nodes, collect all base table names from the subtree via `_collect_subtree_base_tables()`. Keep duplicates (so `cast_info x cast_info x title` differs from `cast_info x title`). Include Values Scan as `__values__` and CTE Scan as `__cte__<name>`.

### Bug 2: Alias collector (`pg_plan_parser.py`)

**Problem**: `_collect_relation_aliases` only collected from Seq Scan / Index Scan nodes. Missed CTE Scan and Values Scan aliases, causing hint alias sets to be incomplete.

**Fix**: Added `CTE Scan` and `Values Scan` to the collected node types. The `*VALUES*` alias (Postgres's internal alias for VALUES clauses) is now included in Rows() hints.

### Bug 3: VALUES scan in subtree (discovered during testing)

**Problem**: pg_hint_plan counts the VALUES scan as a relation in each joinrel. A hint like `Rows(ci_dir nd t #72)` targets a 3-relation join, but the actual joinrel is 4-relation `{*VALUES*, ci_dir, nd, t}`. The hint is reported as "used" but has no effect.

**Fix**: Include `"*VALUES*"` in all Rows() hints where the VALUES scan participates in the join subtree. This is handled automatically by the updated `_collect_relation_aliases`.

## Part 3: Single-query validation

### Query selection

**Q5** (query_seq=5) selected because:
- 3 matching join signatures from Q4 in the same rep
- Baseline q-errors up to 3990x at depth 3+ join nodes
- Fast query (~400ms), enabling quick validation

### SQL text

```sql
WITH pair_movies AS (
    SELECT kp.director, kp.actor, t.id AS movie_id
    FROM (VALUES
        ('Jordan, Neil', 'Rea, Stephen'),
        ('Rodriguez, Robert', 'Trejo, Danny'),
        ('Marshall, Garry', 'Elizondo, Hector'),
        ('Lee, Spike', 'Turturro, John')
    ) AS kp(director, actor)
    JOIN name nd ON nd.name = kp.director
    JOIN name na ON na.name = kp.actor
    JOIN cast_info ci_dir ON ci_dir.person_id = nd.id
        AND ci_dir.role_id = (SELECT id FROM role_type WHERE role = 'director')
    JOIN cast_info ci_act ON ci_act.movie_id = ci_dir.movie_id
        AND ci_act.person_id = na.id
        AND ci_act.role_id IN (SELECT id FROM role_type WHERE role IN ('actor','actress'))
    JOIN title t ON t.id = ci_dir.movie_id
    WHERE t.production_year BETWEEN 1990 AND 2010
      AND t.kind_id = (SELECT id FROM kind_type WHERE kind = 'movie')
),
keyword_counts AS (
    SELECT pm.director, pm.actor, k.keyword, COUNT(*) AS film_count
    FROM pair_movies pm
    JOIN movie_keyword mk ON mk.movie_id = pm.movie_id
    JOIN keyword k ON k.id = mk.keyword_id
    GROUP BY pm.director, pm.actor, k.keyword
    HAVING COUNT(*) >= 3
)
SELECT director, actor, keyword, film_count
FROM keyword_counts
ORDER BY director, actor, film_count DESC
LIMIT 50;
```

### Hint block

```sql
/*+ Rows("*VALUES*" ci_act ci_dir na nd role_type t #44)
    Rows("*VALUES*" ci_act ci_dir nd role_type t #4452)
    Rows("*VALUES*" ci_act ci_dir nd t #9383)
    Rows("*VALUES*" ci_dir nd t #73)
    Rows("*VALUES*" ci_dir nd #178) */
```

All 5 hints sourced from Q4 (same rep). Mapping:
- `#44`: Q4's actual cardinality at the 6-table join {*VALUES*, ci_act, ci_dir, na, nd, role_type, t}
- `#4452`: Q4's actual at {*VALUES*, ci_act, ci_dir, nd, role_type, t}
- `#9383`: Q4's actual at {*VALUES*, ci_act, ci_dir, nd, t}
- `#73`: Q4's actual at {*VALUES*, ci_dir, nd, t}
- `#178`: Q4's actual at {*VALUES*, ci_dir, nd}

### pg_hint_plan application

```
HintStateDump: {used hints:
  Rows(*VALUES* ci_dir nd #178)
  Rows(*VALUES* ci_dir nd t #73)
  Rows(*VALUES* ci_act ci_dir nd t #9383)
  Rows(*VALUES* ci_act ci_dir nd role_type t #4452)
  Rows(*VALUES* ci_act ci_dir na nd role_type t #44)},
{not used hints:(none)}, {duplicate hints:(none)}, {error hints:(none)}
```

**5/5 hints applied. Zero errors.**

### Plan comparison

| Attribute | Baseline | Hinted |
|---|---------|--------|
| Initial Hash Join on | `nd.name` (directors) | `na.name` (actors) |
| Join order | VALUES->nd->ci_dir->t->ci_act->role_type->na->mk->k | VALUES->na->ci_act->role_type->t->ci_dir->mk->nd->k |
| Top join est. rows | 2 | 84 |
| Top join actual rows | 4176 | 4176 |
| Top join q-error | 2088x | 49.7x |
| Plan topology changed | - | Yes: completely reversed join direction |
| Join method changed | - | Yes: Hash Join target moved from nd to na |
| New operators | - | Memoize appeared (non-parallel plan) |

**Baseline plan** (director-first):
```
VALUES -> Hash Join (nd.name) -> ci_dir -> t -> ci_act -> role_type -> na -> mk -> k
```

**Hinted plan** (actor-first):
```
VALUES -> Hash Join (na.name) -> ci_act -> role_type -> t -> ci_dir -> mk -> nd -> k
```

### Latency

| Metric | Baseline | Hinted |
|---|---------|--------|
| Run 1 | 492.6ms | 473.5ms |
| Run 2 | 436.7ms | 462.3ms |
| Run 3 | 368.5ms | 463.8ms |
| Run 4 | 408.1ms | 492.4ms |
| Run 5 | 373.1ms | 617.7ms |
| Run 6 | 393.5ms | 477.6ms |
| **Median (runs 3-6)** | **383.3ms** | **485.0ms** |
| **Delta** | | **+101.7ms (+26.5%)** |

### Q-error comparison (depth 3+ join nodes)

| Node aliases | Baseline est | Baseline act | Baseline qe | Hinted est | Hinted act | Hinted qe |
|---|---|---|---|---|---|---|
| top 8-way join | 2 | 4176 | 2088x | 84 | 4176 | 49.7x |
| 7-way (no k) | 2 | 4176 | 2088x | 84 | 4176 | 49.7x |
| 6-way (no mk,k) | 1 | 36 | 36x | 8 | 18969 | 2371x |
| 5-way (no na,mk,k) | 1 | 3990 | 3990x | 2 | 360 | 180x |

The top-level q-error improved 42x (2088x -> 49.7x). Some intermediate nodes worsened because the different join order produced different intermediate cardinalities.

## Conclusions

1. **Alias instruction**: 100% compliance. The agent reliably produces distinct aliases for repeated table references when instructed.

2. **Hint surface**: 93.8% of join nodes are hintable with distinct aliases, up from 14.9% with original traces.

3. **Structural signatures**: The fixed signature function correctly matches joins across queries. No false collisions observed on the tested queries.

4. **pg_hint_plan application**: All hints applied successfully. The `"*VALUES*"` alias discovery was critical — without it, hints on VALUES-containing queries were silently ignored despite being reported as "used".

5. **Plan change**: The mechanism produced a dramatic structural plan change — completely reversed join order, different Hash Join placement, different intermediate row estimates.

6. **Latency**: The hinted plan is a **+26.5% regression**. Despite much better top-level cardinality estimates (q-error dropped from 2088x to 49.7x), the actor-first join path produces more intermediate rows than the director-first path, making the overall execution slower. The optimizer's original plan was locally optimal even with extreme q-errors because Nested Loop with small actual cardinalities is efficient regardless of the estimate.

7. **Implication**: Better cardinality estimates do not automatically produce faster plans. The mechanism works end-to-end at the infrastructure level, but producing latency improvements requires either (a) finding queries where the baseline plan is genuinely suboptimal due to cardinality errors, or (b) a more selective hint strategy that only injects hints likely to improve rather than merely correct.
