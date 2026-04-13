# Stage 2: Postgres Baseline Replay — Full 31-Task Characterization

## Environment
- PostgreSQL 16.13, pg_hint_plan 1.6.1
- Python 3.12.3, psycopg 3.3.3
- Linux 6.8.0-57-generic
- Data: InsightBench 31 tasks, rep a, ~500 rows per table

## Replay Summary
- **Tasks replayed:** 31
- **Queries executed:** 381
- **Query errors:** 66

**Tasks with errors:**
- `flag-13`: 1 errors
  - Q11: column "opened_at" does not exist
- `flag-15`: 3 errors
  - Q3: aggregate function calls cannot be nested
  - Q6: column "period" does not exist
  - Q9: operator does not exist: integer - interval
- `flag-16`: 9 errors
  - Q0: cannot cast type interval to numeric
  - Q1: cannot cast type interval to numeric
  - Q2: cannot cast type interval to numeric
- `flag-18`: 10 errors
  - Q1: operator does not exist: interval >= integer
  - Q2: operator does not exist: interval < integer
  - Q3: operator does not exist: interval < integer
- `flag-20`: 1 errors
  - Q13: syntax error at or near "LIMIT"
- `flag-21`: 1 errors
  - Q0: syntax error at or near "CROSS"
- `flag-22`: 1 errors
  - Q13: column "cost_bracket" does not exist
- `flag-23`: 4 errors
  - Q0: aggregate function calls cannot be nested
  - Q12: operator does not exist: interval = integer
  - Q13: operator does not exist: interval <= integer
- `flag-25`: 12 errors
  - Q1: cannot cast type interval to numeric
  - Q2: cannot cast type interval to numeric
  - Q3: cannot cast type interval to numeric
- `flag-26`: 14 errors
  - Q0: cannot cast type interval to numeric
  - Q1: cannot cast type interval to numeric
  - Q2: cannot cast type interval to numeric
- `flag-3`: 1 errors
  - Q8: DISTINCT is not implemented for window functions
- `flag-30`: 1 errors
  - Q9: column "flag_30.state" must appear in the GROUP BY clause or be used in an aggregate function
- `flag-31`: 4 errors
  - Q4: unit "dayofyear" not recognized for type date
  - Q10: unit "dayofyear" not recognized for type date
  - Q11: unit "dayofyear" not recognized for type date
- `flag-4`: 1 errors
  - Q6: column "h1_count" does not exist
- `flag-6`: 2 errors
  - Q10: syntax error at or near "END"
  - Q14: syntax error at or near "PIVOT"
- `flag-9`: 1 errors
  - Q2: function string_agg(text) does not exist

## Structural Characterization (Group A)

| Metric | Postgres | DuckDB | Delta |
|--------|----------|--------|-------|
| Table Jaccard (mean) | 0.950 | 0.931 | +0.019 |
| Col Jaccard (select_cols) | 0.503 | 0.520 | -0.017 |
| Col Jaccard (where_cols) | 0.621 | 0.640 | -0.019 |
| Col Jaccard (groupby_cols) | 0.319 | 0.318 | +0.001 |
| Template repetition | 0.000 | 0.001 | -0.001 |
| Inter-query gap median (s) | 11.319 | 11.568 | -0.249 |
| Session length (mean) | 12.700 | 14.106 | -1.406 |

## Opportunity Quantification (Group B)

### Result Cache Hit Rate
- Postgres: **0.145** (51/351)
- DuckDB: 0.147

### Cardinality Reuse Rate

| Split | Postgres Hit Rate | DuckDB Hit Rate | Postgres Hintable |
|-------|-------------------|-----------------|-------------------|
| **Plan-critical** | **0.443** (385/870) | 0.748 | **0.740** (285/385) |
| Bookkeeping | 0.851 (478/562) | 0.958 | 0.000 (0/478) |
| Overall | 0.603 (863/1432) | 0.886 | — |

**By operator type (plan-critical, ≥5 total):**
| Operator | Hits | Total | Hit Rate | Hintable | Hint Rate |
|----------|------|-------|----------|----------|-----------|
| Aggregate | 72 | 369 | 0.195 | 0 | 0.000 |
| Hash Join | 10 | 34 | 0.294 | 9 | 0.900 |
| Seq Scan | 276 | 419 | 0.659 | 276 | 1.000 |
| WindowAgg | 27 | 44 | 0.614 | 0 | 0.000 |

### Q-error on Plan-Critical Reuse Hits
- Postgres: mean=4.64, P95=17.88, N=381
- DuckDB: mean=5.91, P95=19.64

## Prediction Inputs (Group C)

### GROUP BY Prediction (N=310)
| Predictor | Postgres Top-1 | DuckDB Top-1 | Postgres Top-3 |
|-----------|---------------|--------------|----------------|
| Most Frequent | 0.142 | 0.133 | 0.277 |
| Last Seen | 0.142 | 0.133 | 0.265 |
| Markov-1 | 0.055 | 0.055 | 0.084 |

### Move Sequence Frequencies
| Move | Count | Fraction |
|------|-------|----------|
| pivot | 180 | 0.472 |
| widen | 57 | 0.150 |
| drill_down | 50 | 0.131 |
| cross_table | 28 | 0.073 |
| reframe | 25 | 0.066 |
| overview | 20 | 0.052 |
| other | 13 | 0.034 |
| deepen | 8 | 0.021 |

**Top-5 bigrams:**
| Sequence | Count |
|----------|-------|
| pivot → pivot | 86 |
| pivot → widen | 38 |
| drill_down → pivot | 32 |
| widen → pivot | 29 |
| pivot → drill_down | 22 |

### Anchor Dimensions (≥50% of GROUP BYs)
| Task/Session | Anchors |
|-------------|---------|
| flag_1 | category |
| flag_10 | 1 |
| flag_12 | assigned_to |
| flag_13 | assigned_to, month |
| flag_15 | 1, 2 |
| flag_17 | department |
| flag_19 | department |
| flag_2 | 1 |
| flag_23 | user |
| flag_24 | department |
| flag_25 | 1 |
| flag_26 | warranty_days |
| flag_27 | manager |
| flag_28 | department |
| flag_29 | priority |
| flag_30 | category, department |
| flag_31 | QUARTER(start_date) |
| flag_4 | assigned_to |
| flag_6 | assigned_to |
| flag_7 | assigned_to, month |
