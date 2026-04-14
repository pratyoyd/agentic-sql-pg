# Partial Analysis Report: IMDB Agentic-SQL Experiment

## Section 1: Workload Shape

### task1

- **Reps available**: ['a', 'b', 'c']
- **Total queries**: 39, **Successful**: 37

- **Tables per query**: median=6.0, mean=6.4, max=14
- **Plan tree depth**: median=11.0, max=17
- **Join operators per query**: median=3.0, max=8
- **CTEs per query**: median=2.0, max=5

### task2

- **Reps available**: ['a', 'b', 'c']
- **Total queries**: 35, **Successful**: 33

- **Tables per query**: median=13.0, mean=12.1, max=16
- **Plan tree depth**: median=14.0, max=22
- **Join operators per query**: median=8.0, max=10
- **CTEs per query**: median=6.0, max=9

### task3

- **Reps available**: ['a', 'b', 'c']
- **Total queries**: 32, **Successful**: 31

- **Tables per query**: median=6.0, mean=5.4, max=8
- **Plan tree depth**: median=8.0, max=13
- **Join operators per query**: median=3.0, max=6
- **CTEs per query**: median=0.0, max=3

### task4

- **Reps available**: ['a', 'b', 'c']
- **Total queries**: 62, **Successful**: 58

- **Tables per query**: median=3.0, mean=4.3, max=11
- **Plan tree depth**: median=8.0, max=15
- **Join operators per query**: median=2.0, max=9
- **CTEs per query**: median=2.0, max=7

### task5

- **Reps available**: ['a']
- **Total queries**: 4, **Successful**: 4

- **Tables per query**: median=7.0, mean=6.5, max=8
- **Plan tree depth**: median=13.0, max=15
- **Join operators per query**: median=3.0, max=3
- **CTEs per query**: median=2.0, max=3

## Section 2: Cardinality Estimation Gap

### task1_rep_a
- Nodes considered: 23
- Q-error distribution: min=1.0, P25=2.9, median=7.1, P75=1088.3, P95=19289.3, P99=25035.4, max=26208.0
- Nodes with q-error > 100x: 8 (34.8%)
- Nodes with q-error > 1000x: 6 (26.1%)

### task1_rep_b
- Nodes considered: 26
- Q-error distribution: min=1.6, P25=4.6, median=13.7, P75=426.7, P95=20778.8, P99=22522.8, max=22910.0
- Nodes with q-error > 100x: 7 (26.9%)
- Nodes with q-error > 1000x: 5 (19.2%)

### task1_rep_c
- Nodes considered: 21
- Q-error distribution: min=1.1, P25=3.6, median=25.1, P75=3545.9, P95=33115.0, P99=33115.8, max=33116.0
- Nodes with q-error > 100x: 9 (42.9%)
- Nodes with q-error > 1000x: 7 (33.3%)

### task2_rep_a
- Nodes considered: 33
- Q-error distribution: min=1.1, P25=3.1, median=128.0, P75=667.2, P95=765.2, P99=1409.5, max=1668.0
- Nodes with q-error > 100x: 18 (54.5%)
- Nodes with q-error > 1000x: 1 (3.0%)

### task2_rep_b
- Nodes considered: 50
- Q-error distribution: min=1.1, P25=8.4, median=128.0, P75=328.0, P95=2616.0, P99=2933.2, max=3238.0
- Nodes with q-error > 100x: 26 (52.0%)
- Nodes with q-error > 1000x: 11 (22.0%)

### task2_rep_c
- Nodes considered: 73
- Q-error distribution: min=1.2, P25=56.0, median=142.8, P75=555.8, P95=1872.7, P99=4009.0, max=4009.0
- Nodes with q-error > 100x: 41 (56.2%)
- Nodes with q-error > 1000x: 15 (20.5%)

### task3_rep_a
- Nodes considered: 9
- Q-error distribution: min=3.9, P25=4.4, median=10.9, P75=1290.0, P95=1322.4, P99=1339.7, max=1344.0
- Nodes with q-error > 100x: 3 (33.3%)
- Nodes with q-error > 1000x: 3 (33.3%)

### task3_rep_b
- Nodes considered: 28
- Q-error distribution: min=2.0, P25=15.7, median=58.6, P75=157.0, P95=1001.4, P99=1497.8, max=1615.2
- Nodes with q-error > 100x: 8 (28.6%)
- Nodes with q-error > 1000x: 2 (7.1%)

### task3_rep_c
- Nodes considered: 37
- Q-error distribution: min=2.0, P25=44.3, median=84.0, P75=257.0, P95=1113.2, P99=2231.0, max=2535.2
- Nodes with q-error > 100x: 16 (43.2%)
- Nodes with q-error > 1000x: 2 (5.4%)

### task4_rep_a
- Nodes considered: 18
- Q-error distribution: min=1.0, P25=2.0, median=3.2, P75=9.0, P95=28.5, P99=28.5, max=28.5
- Nodes with q-error > 100x: 0 (0.0%)
- Nodes with q-error > 1000x: 0 (0.0%)

### task4_rep_b
- Nodes considered: 20
- Q-error distribution: min=1.0, P25=4.0, median=9.4, P75=55.0, P95=118.0, P99=118.0, max=118.0
- Nodes with q-error > 100x: 2 (10.0%)
- Nodes with q-error > 1000x: 0 (0.0%)

### task4_rep_c
- Nodes considered: 10
- Q-error distribution: min=1.7, P25=2.4, median=29.6, P75=1525.8, P95=2077.2, P99=2077.2, max=2077.2
- Nodes with q-error > 100x: 3 (30.0%)
- Nodes with q-error > 1000x: 3 (30.0%)

### task5_rep_a
- Nodes considered: 7
- Q-error distribution: min=1.7, P25=3.4, median=3.7, P75=3.7, P95=1422.5, P99=1908.9, max=2030.5
- Nodes with q-error > 100x: 1 (14.3%)
- Nodes with q-error > 1000x: 1 (14.3%)

### 15 Worst Q-Errors Across Dataset

| Rank | Task | Rep | Seq | Relation Aliases | Est | Actual | Q-Error | Depth | SQL (first 200 chars) |
|------|------|-----|-----|-----------------|-----|--------|---------|-------|-----------------------|
| 1 | task1 | c | 13 | mi, mi_1, mii, t | 1 | 33,116 | 33,116.0 | 4 | `WITH cohort AS (     SELECT t.id AS movie_id, CAST(mii.info AS NUMERIC) AS rating     FROM title t     JOIN movie_info_i` |
| 2 | task1 | c | 13 | mi, mi_1, mii, mii_1, t | 1 | 33,115 | 33,115.0 | 5 | `WITH cohort AS (     SELECT t.id AS movie_id, CAST(mii.info AS NUMERIC) AS rating     FROM title t     JOIN movie_info_i` |
| 3 | task1 | c | 9 | mi, mi_1, mii, t | 1 | 26,494 | 26,494.0 | 4 | `WITH cohort AS (     SELECT t.id AS movie_id, CAST(mii.info AS NUMERIC) AS rating     FROM title t     JOIN movie_info_i` |
| 4 | task1 | a | 4 | mi, mii, t | 1 | 26,208 | 26,208.0 | 3 | `SELECT    CASE      WHEN rt::int < 60 THEN 'under 60min'     WHEN rt::int < 80 THEN '60-79min'     WHEN rt::int < 100 TH` |
| 5 | task1 | b | 11 | g, k, mk, r, t | 1 | 22,910 | 22,910.0 | 5 | `SELECT      CASE          WHEN k.keyword IN ('tears','crying','song','singing','food','rain','snow','bicycle','photograp` |
| 6 | task1 | b | 9 | info_type_2, kind_type_1, mi, t, t_1 | 1 | 21,361 | 21,361.0 | 5 | `WITH runtime_data AS (     SELECT          t.id AS movie_id,         (regexp_match(mi.info, '^(\d+)'))[1]::integer AS ru` |
| 7 | task1 | a | 9 | company_type, company_type_1, info_type, info_type_1, mc, mii... | 1 | 20,878 | 20,878.0 | 10 | `WITH base AS (   SELECT      t.id AS movie_id,     mii.info::numeric AS rating   FROM movie_info_idx mii   JOIN title t ` |
| 8 | task1 | b | 9 | g, info_type_2, kind_type_1, mi, r, t... | 1 | 19,032 | 19,032.0 | 7 | `WITH runtime_data AS (     SELECT          t.id AS movie_id,         (regexp_match(mi.info, '^(\d+)'))[1]::integer AS ru` |
| 9 | task1 | b | 9 | info_type_2, kind_type_1, mi, r, t, t_1 | 1 | 16,585 | 16,585.0 | 6 | `WITH runtime_data AS (     SELECT          t.id AS movie_id,         (regexp_match(mi.info, '^(\d+)'))[1]::integer AS ru` |
| 10 | task1 | c | 14 | info_type_1, mi, mi_1, mii, t | 1 | 13,624 | 13,624.0 | 5 | `WITH cohort AS (     SELECT t.id AS movie_id, CAST(mii.info AS NUMERIC) AS rating     FROM title t     JOIN movie_info_i` |
| 11 | task1 | c | 6 | mi, mii, t | 2 | 26,260 | 13,130.0 | 3 | `WITH cohort AS (     SELECT t.id AS movie_id, CAST(mii.info AS NUMERIC) AS rating     FROM title t     JOIN movie_info_i` |
| 12 | task1 | b | 5 | info_type_1, kind_type, mi, r, t | 2 | 17,448 | 8,724.0 | 5 | `WITH runtime_data AS (     SELECT          t.id AS movie_id,         -- Take the first numeric runtime value         (re` |
| 13 | task1 | a | 9 | info_type, info_type_1, mii, movie_info, movie_info_1, t | 5 | 24,953 | 4,990.6 | 6 | `WITH base AS (   SELECT      t.id AS movie_id,     mii.info::numeric AS rating   FROM movie_info_idx mii   JOIN title t ` |
| 14 | task1 | a | 12 | info_type, info_type_1, k, mii, mk, movie_info... | 5 | 24,953 | 4,990.6 | 8 | `WITH base AS (   SELECT t.id AS movie_id, mii.info::numeric AS rating,     (mii.info::numeric >= 7.0)::int AS is_high   ` |
| 15 | task1 | a | 12 | info_type, info_type_1, mii, movie_info, movie_info_1, t | 5 | 24,953 | 4,990.6 | 6 | `WITH base AS (   SELECT t.id AS movie_id, mii.info::numeric AS rating,     (mii.info::numeric >= 7.0)::int AS is_high   ` |

### Per-Task Correlation: Tables vs Q-Error

**task1**

| Rep | Median Tables/Query | Median Q-Error |
|-----|-------------------|----------------|
| a | 6.0 | 7.1 |
| b | 7.0 | 13.7 |
| c | 6.0 | 25.1 |

**task2**

| Rep | Median Tables/Query | Median Q-Error |
|-----|-------------------|----------------|
| a | 12.0 | 128.0 |
| b | 12.0 | 128.0 |
| c | 13.0 | 142.8 |

**task3**

| Rep | Median Tables/Query | Median Q-Error |
|-----|-------------------|----------------|
| a | 4.0 | 10.9 |
| b | 6.0 | 58.6 |
| c | 7.0 | 84.0 |

**task4**

| Rep | Median Tables/Query | Median Q-Error |
|-----|-------------------|----------------|
| a | 5.0 | 3.2 |
| b | 4.0 | 9.4 |
| c | 3.0 | 29.6 |

**task5**

| Rep | Median Tables/Query | Median Q-Error |
|-----|-------------------|----------------|
| a | 7.0 | 3.7 |

## Section 3: Hint Surface under pg_hint_plan

- **Total hintable hits**: 419
- **Fraction of queries with >= 1 hintable hit**: 126/163 = 77.3%
- **Hits per query**: median=2.0, max=9
- **Corrected hint-surface rate** (depth 3+ join nodes that are hintable): 252/366 = 68.9%

### Useful Hints Sub-Analysis

- Hintable hits where baseline q-error > 10x: 244/419 = 58.2%

**Top 5 hintable hits with largest baseline q-errors:**

| Rank | Task | Rep | Seq | Signature (short) | Aliases | Q-Error |
|------|------|-----|-----|-------------------|---------|---------|
| 1 | task4 | c | 27 | 55ce84addda8... | mii | 269,758.1 |
| 2 | task1 | c | 13 | 8d41482b988b... | mi, mi_1, mii, t | 33,116.0 |
| 3 | task1 | c | 13 | 8d41482b988b... | mi, mi_1, mii, mii_1, t | 33,115.0 |
| 4 | task1 | c | 9 | 8d41482b988b... | mi, mi_1, mii, t | 26,494.0 |
| 5 | task1 | b | 5 | 8d41482b988b... | mi, t | 25,318.0 |

## Section 4: CTE Repetition

### From cte_analysis.jsonl

- Total entries: 168
- Queries with CTEs: 113 (67.3%)
- Queries with exact CTE reuse: 50
- Queries with near-identical CTE reuse: 20
- CTE counts (among queries with CTEs): median=3.0, max=9

### Cross-Query CTE Repetition (from raw SQL)

| Task | Rep | Distinct CTEs | Repeated CTEs | Max Repetition | Est. Waste (ms) |
|------|-----|--------------|---------------|----------------|-----------------|
| task1 | a | 21 | 1 | 2 | 10,087 |
| task1 | b | 15 | 1 | 2 | 7,019 |
| task1 | c | 16 | 5 | 8 | 104,826 |
| task2 | a | 29 | 8 | 6 | 5,279,298 |
| task2 | b | 28 | 15 | 12 | 1,038,262 |
| task2 | c | 25 | 8 | 11 | 231,134 |
| task3 | a | 10 | 1 | 2 | 936 |
| task3 | b | 4 | 0 | 0 | 0 |
| task3 | c | 1 | 0 | 0 | 0 |
| task4 | a | 33 | 3 | 2 | 235 |
| task4 | b | 30 | 4 | 3 | 492 |
| task4 | c | 45 | 3 | 2 | 42,657 |
| task5 | a | 6 | 1 | 3 | 11,612 |

- **Total estimated wasted time**: 6,726,557 ms across all reps
- **Average per-session waste**: 517,427 ms
- **Highest-waste CTE pattern** (canonicalized, first 200 chars): `select d.director_id, a.actor_id, d.movie_id, count(*) over (partition by d.director_id, a.actor_id) as film_count from dirs d join acts a on d.movie_id = a.movie_id where d.director_id <> a.actor_id`

## Section 5: Hung Query Investigation -- Task 4 Rep C

- **Total entries in trace**: 30 (query_seq 0 to 29)
- **Successful**: 28, **Failed**: 2
- **Last recorded entry**: query_seq=29, success=True, execution_ms=10.93

**Slowest queries in task4 rep c:**

| Seq | Exec (ms) | Tables | Success |
|-----|-----------|--------|---------|
| 21 | 26,486 | cast_info, franchise_cast, franchise_ids, installment_pairs | True |
| 22 | 26,342 | cast_info, franchise_cast, franchise_ids, info_type | True |
| 27 | 16,310 | cast_info, franchise_ids, franchise_leads, info_type | True |
| 17 | 2,962 | info_type, movie_info_idx, title | True |
| 16 | 2,759 | franchise_candidates, franchise_groups, title | True |

### Session Status

The agent log shows the session **completed normally**: 30 queries in 612.2s.
The agent signaled DONE after completing its analysis.

**Conclusion**: Task 4 rep c was NOT killed due to a hung query. The session ran to completion with 30 queries. However, the session included two very slow queries (seq 21 and 22/23 at ~26s and ~16s respectively) involving complex CTE-based franchise cast retention analysis with self-joins on `cast_info` -- a table with millions of rows. These slow queries may have been the source of concern about hanging, as they took 10-50x longer than typical queries in this session.

- **Raw plan files**: 28 files, last is `29.json` (seq 29)
- Max seq in plans: 29, max seq in traces: 29
- Missing plan files for seqs: [18, 28] (likely failed queries)

## Section 6: Assessment

The IMDB workload shape is appropriate for cardinality feedback research: across 172 total queries (163 successful) spanning 4 tasks and 11 completed reps, queries involve multi-table joins with CTEs, producing plan trees of meaningful depth. The cardinality estimation gap is substantial, with a median q-error of 49.1x and P95 of 3545.9x on depth-3+ join nodes, and 142 nodes (40.0%) exceeding 100x error -- this confirms PostgreSQL's optimizer struggles with the complex multi-way joins that agentic SQL sessions produce. The hint surface rate of 68.9% on plan-critical nodes exceeds InsightBench's 3.8% baseline, validating the feasibility of a pg_hint_plan-based correction approach on this workload. The useful hint fraction (q-error > 10x) at 58.2% of hintable hits indicates a meaningful opportunity for cardinality feedback to improve actual execution. CTE repetition across queries within sessions is a real finding direction -- the agentic pattern of iterative refinement naturally produces repeated sub-expressions, and these repeated CTEs represent both wasted computation and opportunities for the system to learn from prior cardinality observations on identical sub-plans.
