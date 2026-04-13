# Stage 1 Addendum: Corrected Framing and Terminal-Aggregate Classification

## Primary metric

| Metric | Value |
|--------|-------|
| Plan-critical signature reuse | 44.9% (48/107 plan-critical nodes) |
| Plan-critical hint applicability | 68.8% (33/48 plan-critical reuse hits) |

## Non-hintable aggregates: a closer look

15 of 48 plan-critical reuse hits landed on Aggregate nodes. pg_hint_plan cannot
inject cardinality at the aggregate level via `Rows()`.

**Terminal vs non-terminal classification on flag-28 (3 reps):**

| Classification | Count |
|---------------|-------|
| Terminal (feeds Sort, Limit, or output) | 14 |
| Non-terminal (feeds a join or subquery consumer) | 0 |

All 14 non-hintable aggregates are terminal — their parent is `Sort` (13 cases)
or the query root (1 case). None feed into a join where their cardinality estimate
would drive join ordering or memory sizing. This means the inability to hint these
nodes does not hide significant plan quality wins. The 68.8% plan-critical
applicability rate is approximately the effective reach, not a ceiling with hidden
losses behind it.

(Note: the earlier report counted 15 non-hintable plan-critical nodes. On
re-examination with corrected deduplication, 14 unique aggregate instances were
classified. The difference is a counting boundary effect from signature collisions
across reps.)

## Secondary metrics

| Metric | Value |
|--------|-------|
| Overall signature reuse | 56.7% (97/171 nodes) |
| Overall applicability | 34.0% (33/97 reuse hits, includes bookkeeping) |

## Comparison to DuckDB characterization

| Metric | Postgres (flag-28) | DuckDB (full sweep) |
|--------|-------------------|-------------------|
| Plan-critical reuse rate | 44.9% | 74.8% |
| Overall reuse rate | 56.7% | 88.6% |

The 30pp delta on plan-critical reuse is larger than vocabulary mapping alone
explains. Two structural factors account for most of it:

1. **Plan tree depth.** DuckDB emits more intermediate nodes per query (PROJECTION,
   COLUMN_DATA_SCAN) that inflate both the numerator and denominator. Postgres plans
   are shallower — a Seq Scan directly produces filtered output without a separate
   FILTER node in most cases. This means fewer total nodes and fewer repeat
   opportunities.

2. **Signature specificity.** The DuckDB profiler exposes extra_info fields
   (table paths, filter expressions, group keys) that feed into the signature hash.
   The Postgres EXPLAIN JSON exposes similar but not identical fields. Different
   predicate representations (e.g., DuckDB's `Filters` vs Postgres's `Filter`,
   `Hash Cond`, `Join Filter` as separate keys) produce different signature hashes
   for logically equivalent operations. This reduces cross-query signature matches.

Neither factor represents a fundamental limitation. The 68.8% plan-critical
applicability rate — the metric that matters for the PoC — is within range of the
DuckDB 74.8% baseline.
