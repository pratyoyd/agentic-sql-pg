# Stage 2 Hintable Rate Correction

## Addendum to stage 2 metrics

The stage 2 report included two hintable metrics:

- **Plan-critical reuse rate: 47.5%** (488 hits / 1028 plan-critical nodes). This number
  is correct and measures signature-level reuse across all plan-critical operator types.
- **Hintable reuse rate: 65.8%** (364 hits / 553 scan+join nodes). This number counted
  both scan-level and join-level hits as "hintable via pg_hint_plan Rows()."

## The correction

pg_hint_plan 1.6.1's `Rows()` hint requires **at least two relations**. It overrides
the join output cardinality for multi-relation joins but cannot override single-table
scan cardinality estimates. Verified directly:

```
EXPLAIN /*+ Rows(flag_19 #42) */ SELECT COUNT(*) FROM flag_19 WHERE department = 'IT';
INFO:  pg_hint_plan: hint syntax error at or near " "
DETAIL:  Rows hint requires at least two relations.
```

A dummy-table workaround was tested: rewriting single-table queries as cross-joins
with a 1-row table, then hinting the pseudo-join. While the hint was accepted when
using a real base table, the rewrite itself destroyed parallel execution on 2 of 3
tested queries (100-160% wall-clock overhead), making it impractical.

## Decomposition of the 488 plan-critical reuse hits

| Operator category | Hits | Hintable via Rows()? |
|-------------------|------|---------------------|
| Scans (Seq Scan, Index Scan, etc.) | 343 (70.3%) | No — single relation |
| Joins (Hash Join, Merge Join, Nested Loop) | 21 (4.3%) | Yes |
| Aggregates (HashAggregate, GroupAggregate, WindowAgg) | 124 (25.4%) | No — not a join |

## Corrected join-only hintable rate

- **Join-only hintable reuse rate: 3.8%** (21 hits / 553 scan+join nodes)
- This replaces the 65.8% figure for purposes of what pg_hint_plan can actually target.

## The plan-critical reuse rate stands

The 47.5% plan-critical reuse rate remains correct as a characterization metric. It
measures how often the agent revisits operator signatures across queries. This rate
is independent of hint mechanism — it describes the workload, not the tool. A future
mechanism that could target scans and aggregates (e.g., pg_dbms_stats for statistics
injection) would use the full 47.5% surface.

## Pilot validation

A join-only pilot on flag-18 (the most join-heavy task: 13/13 queries are joins,
10 join-level reuse hits) showed that hints were accepted but produced **zero plan
changes**. Postgres's join cardinality estimates at 1M-row scale are already accurate
(q-error 1.0-2.4× on the join output), so the Rows() hint injects a value within
~8 rows of what the optimizer already estimated. The hint confirms rather than corrects.

This suggests that at InsightBench's join patterns (simple fact-dimension equi-joins),
the estimation gap that hints could fill is at the scan level (filtered scan selectivity)
and aggregate level (GROUP BY group count), not at the join level.
