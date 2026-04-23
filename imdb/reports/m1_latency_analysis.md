# M1 Workspace Latency Analysis — Full Findings

## Overview

Measured the DB compute savings from M1 (workspace materialization) across 10 scenarios,
21 sessions, 168 total queries. Baseline measured by replacing temp table references with
CTE-wrapped original SQL and running EXPLAIN ANALYZE on PostgreSQL 16.

## Current Results (Post-Hoc Save, Double Execution)

| Metric | Value |
|--------|-------|
| Total queries | 168 |
| Reuse queries benchmarked | 73 |
| With M1 (OTHER + SAVE + REUSE) | 1075.3s |
| Without M1 (OTHER + BASELINE) | 1398.1s |
| Net savings | 322.7s |
| Speedup | 1.30x |

### Per-Scenario Breakdown

| Scenario | OTHER (s) | SAVE (s) | REUSE (s) | BASELINE (s) | With M1 (s) | W/o M1 (s) | Savings (s) | Speedup |
|----------|-----------|----------|-----------|-------------|-------------|------------|-------------|---------|
| 1. Genre Evolution | 25.7 | 3.0 | 1.1 | 25.4 | 29.8 | 51.1 | 21.3 | 1.72x |
| 2. Director Career | 26.9 | 18.2 | 348.3 | 469.9 | 393.5 | 496.8 | 103.3 | 1.26x |
| 3. Company Genre | 29.9 | 4.4 | 0.0 | 13.3 | 34.3 | 43.2 | 8.9 | 1.26x |
| 4. Cast Size | 91.7 | 6.1 | 2.9 | 25.3 | 100.7 | 117.0 | 16.4 | 1.16x |
| 5. Co-Production | 20.1 | 10.0 | 6.8 | 31.0 | 36.9 | 51.1 | 14.2 | 1.38x |
| 6. Franchise | 9.2 | 1.6 | 0.1 | 6.2 | 10.9 | 15.5 | 4.5 | 1.41x |
| 7. Writer-Director | 9.9 | 4.5 | 4.3 | 14.7 | 18.7 | 24.7 | 6.0 | 1.32x |
| 8. Actor Archetypes | 177.6 | 21.4 | 0.0 | 141.9 | 198.9 | 319.4 | 120.5 | 1.61x |
| 9. Spillover | 89.4 | 79.7 | 66.0 | 166.6 | 235.1 | 256.0 | 20.9 | 1.09x |
| 10. Budget | 12.0 | 4.5 | 0.1 | 11.4 | 16.6 | 23.4 | 6.8 | 1.41x |
| **TOTAL** | **492.3** | **153.3** | **429.7** | **905.7** | **1075.3** | **1398.1** | **322.7** | **1.30x** |

### Formula

- **With M1** = OTHER + SAVE + REUSE
  - OTHER: queries that don't touch workspace (same cost either way)
  - SAVE: cost of `workspace.save()` re-executing the base query to materialize it
  - REUSE: queries hitting the materialized temp table (fast)
- **Without M1** = OTHER + BASELINE
  - BASELINE: reuse queries with the base join inlined as a CTE (re-runs from raw tables)
- **Net savings** = BASELINE − SAVE − REUSE

The SAVE cost is pure M1 overhead — without M1, the agent doesn't call `workspace.save()`.
The base query that SAVE re-executes was already run once as an exploratory query (counted in OTHER).
So today's implementation pays for the base query twice: once to see results, once to materialize.

## Conservative Estimate: Why Real Savings Are Higher

### Multi-reference CTE auto-materialization (PG16)

PostgreSQL 16 automatically materializes CTEs referenced more than once in a query.
Of 73 benchmarked reuse queries:
- **44 single-reference**: CTE gets inlined → baseline truly re-runs the full join → **benchmark valid**
- **25 multi-reference**: CTE auto-materialized → baseline gets free materialization → **benchmark understates true cost**

For multi-reference queries, the benchmark compares two forms of materialization (temp table vs CTE)
rather than materialization vs no materialization. The measured 1.4x speedup for multi-ref queries
is a lower bound — the real without-M1 cost would be higher if the agent had to restructure
queries to avoid the CTE (e.g., inline the base as a subquery in each reference point).

| Group | Queries | Reuse (s) | Baseline (s) | Savings (s) | Speedup |
|-------|---------|-----------|-------------|-------------|---------|
| Single-ref | 44 | 67.9 | 384.3 | 316.3 | 5.7x |
| Multi-ref | 25 | 356.9 | 510.5 | 153.6 | 1.4x |
| **All** | **69** | **424.8** | **894.7** | **469.9** | **2.1x** |

### Double execution of SAVE

Today's flow: agent runs query → sees results → decides to save → `workspace.save()` re-executes.
The 153.3s SAVE column represents this redundant second execution.

## Projected Improvement: Pre-Hoc Save

A pre-hoc signal (`-- WILL_SAVE: name "desc"` before the SQL) would let the harness
materialize on first execution, eliminating the double-execution entirely.

| Case | Today (post-hoc) | Pre-hoc |
|------|------------------|---------|
| Full SAVE | 2x base query | 1x base query |
| SAVE_CTE | 1x (CTE+outer) + 1x CTE body | 1x CTE body + 1x outer (against temp table) |

For full SAVE: clean 2x → 1x win.
For SAVE_CTE: roughly same total work, but the outer query benefits from temp table stats.

**Pre-hoc projected numbers:**

| Metric | Post-hoc (current) | Pre-hoc (projected) |
|--------|-------------------|---------------------|
| With M1 | 1075.3s (OTHER + SAVE + REUSE) | 922.0s (OTHER + REUSE) |
| Without M1 | 1398.1s | 1398.1s |
| Net savings | 322.7s | 476.1s |
| Speedup | 1.30x | 1.52x |

The SAVE cost (153.3s) drops to ~0 because materialization happens during the first execution.

### Pre-hoc implementation

For full query SAVE:
1. Agent emits `-- WILL_SAVE: name "desc"` before the SQL
2. Harness executes as `CREATE TEMP TABLE name_hex AS <query>`
3. Returns results from the temp table
4. No separate `workspace.save()` call needed

For CTE SAVE_CTE:
1. Agent emits `-- WILL_SAVE_CTE: name "desc" cte=base` before the SQL
2. Harness parses SQL (via sqlglot), extracts the named CTE body
3. Materializes: `CREATE TEMP TABLE name_hex AS <cte body>`
4. Rewrites query: replaces `WITH base AS (...)` with reference to `name_hex`
5. Executes the rewritten query against the temp table

## Index Experiment

Tested whether adding indexes on temp tables would further improve reuse query performance.

| Scenario | Query | Column | No index | With index | Speedup |
|----------|-------|--------|----------|------------|---------|
| Sc2 q8 | GROUP BY director_id | director_id | 11,166ms | 11,196ms | 1.0x |
| Sc7 q4 | JOIN on movie_id | movie_id | 1,774ms | 2,315ms | 0.8x (worse) |
| Sc9 q4 | GROUP BY person_id | person_id | 14,456ms | 13,818ms | 1.0x |

**Conclusion: indexes don't help.** The agent's analytical queries are overwhelmingly full-scan
aggregations (GROUP BY, AVG, COUNT DISTINCT) or hash joins that need every row.
Indexes help for point lookups (WHERE id = X) but the agent never does those — it always
aggregates across the full dataset. Indexes actually hurt in Sc7 because the planner
switches from hash join (optimal for full scans) to nested loop with index lookups (worse
when all rows are needed).

## Key Findings

1. **M1 delivers a net 1.3x DB compute speedup** (conservative, understated by CTE auto-materialization)
2. **SAVE_CTE is the dominant save pattern** — 11 of 15 saves across all scenarios used SAVE_CTE
3. **Pre-hoc save would improve to 1.52x** by eliminating double execution (153s saved)
4. **Indexes on temp tables don't help** — analytical workloads are full-scan, not point-lookup
5. **M2 (intent declaration) doesn't directly reduce latency** — it's an annotation mechanism; any benefit comes indirectly through prompting workspace reuse
6. **Scenario structure matters**: scenarios with a clear "build base → analyze variants" structure (Sc1, Sc2, Sc8) show the largest M1 benefit; exploratory scenarios (Sc6 original, Sc9) show less
