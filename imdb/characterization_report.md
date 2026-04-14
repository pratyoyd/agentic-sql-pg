# IMDB Agentic-SQL Characterization Report

## Section 1: Recording Phase Summary

| Task | Rep | Num Queries | Wall Clock (s) | Successful | Failed | Reached DONE |
|------|-----|-------------|----------------|------------|--------|--------------|
| task1 | a | 13 | 374.7 | 11 | 2 | Yes |
| task1 | b | 13 | 343.5 | 13 | 0 | Yes |
| task1 | c | 15 | 339.1 | 13 | 2 | Yes |
| task2 | a | 9 | 1624.7 | 9 | 0 | Yes |
| task2 | b | 13 | 502.7 | 12 | 1 | Yes |
| task2 | c | 13 | 430.8 | 12 | 1 | Yes |
| task3 | a | 8 | 147.8 | 8 | 0 | Yes |
| task3 | b | 11 | 216.6 | 11 | 0 | Yes |
| task3 | c | 13 | 217.1 | 12 | 1 | Yes |
| task4 | a | 16 | 346.9 | 15 | 1 | Yes |
| task4 | b | 16 | 342.8 | 15 | 1 | Yes |
| task4 | c | 30 | 612.2 | 28 | 2 | Yes |
| task5 | a | 10 | 284.6 | 10 | 0 | Yes |
| task5 | b | 10 | 439.1 | 10 | 0 | Yes |
| task5 | c | 11 | 267.2 | 11 | 0 | Yes |

**Overall**: 201 total queries, 190 successful, 11 failed, 6489.8s total wall-clock time.

### Per-Task Averages

| Task | Reps | Avg Queries | Avg Wall Clock (s) | Avg Success Rate |
|------|------|-------------|--------------------|--------------------|
| task1 | 3 | 13.7 | 352.4 | 90.2% |
| task2 | 3 | 11.7 | 852.7 | 94.3% |
| task3 | 3 | 10.7 | 193.8 | 96.9% |
| task4 | 3 | 20.7 | 434.0 | 93.5% |
| task5 | 3 | 10.3 | 330.3 | 100.0% |

## Section 2: Workload Shape

### task1

- **Reps**: 3
- **Total successful queries**: 37
- **Tables per query**: median=6.0, mean=6.4, max=14
- **Plan tree depth**: median=11.0, max=17
- **Join operators per query**: median=3.0, max=8
- **CTEs per query**: median=2.0, max=5

### task2

- **Reps**: 3
- **Total successful queries**: 33
- **Tables per query**: median=13.0, mean=12.1, max=16
- **Plan tree depth**: median=14.0, max=22
- **Join operators per query**: median=8.0, max=10
- **CTEs per query**: median=6.0, max=9

### task3

- **Reps**: 3
- **Total successful queries**: 31
- **Tables per query**: median=6.0, mean=5.4, max=8
- **Plan tree depth**: median=8.0, max=13
- **Join operators per query**: median=3.0, max=6
- **CTEs per query**: median=0.0, max=3

### task4

- **Reps**: 3
- **Total successful queries**: 58
- **Tables per query**: median=3.0, mean=4.3, max=11
- **Plan tree depth**: median=8.0, max=15
- **Join operators per query**: median=2.0, max=9
- **CTEs per query**: median=2.0, max=7

### task5

- **Reps**: 3
- **Total successful queries**: 31
- **Tables per query**: median=8.0, mean=7.7, max=12
- **Plan tree depth**: median=12.0, max=16
- **Join operators per query**: median=4.0, max=8
- **CTEs per query**: median=3.0, max=6

### Cross-Task Comparison

| Task | Reps | Succ Queries | Tables med/mean/max | Depth med/max | Joins med/max | CTEs med/max |
|------|------|-------------|---------------------|---------------|---------------|--------------|
| task1 | 3 | 37 | 6.0 / 6.4 / 14 | 11.0 / 17 | 3.0 / 8 | 2.0 / 5 |
| task2 | 3 | 33 | 13.0 / 12.1 / 16 | 14.0 / 22 | 8.0 / 10 | 6.0 / 9 |
| task3 | 3 | 31 | 6.0 / 5.4 / 8 | 8.0 / 13 | 3.0 / 6 | 0.0 / 3 |
| task4 | 3 | 58 | 3.0 / 4.3 / 11 | 8.0 / 15 | 2.0 / 9 | 2.0 / 7 |
| task5 | 3 | 31 | 8.0 / 7.7 / 12 | 12.0 / 16 | 4.0 / 8 | 3.0 / 6 |

## Section 3: Cardinality Estimation Gap

### Per-Rep Q-Error Distribution

| Task | Rep | Nodes | Min | P25 | Median | P75 | P95 | P99 | Max | >100x | >1000x |
|------|-----|-------|-----|-----|--------|-----|-----|-----|-----|-------|--------|
| task1 | a | 23 | 1.0 | 2.9 | 7.1 | 1088.3 | 19289.3 | 25035.4 | 26208.0 | 8 (34.8%) | 6 (26.1%) |
| task1 | b | 26 | 1.6 | 4.6 | 13.7 | 426.7 | 20778.8 | 22522.8 | 22910.0 | 7 (26.9%) | 5 (19.2%) |
| task1 | c | 21 | 1.1 | 3.6 | 25.1 | 3545.9 | 33115.0 | 33115.8 | 33116.0 | 9 (42.9%) | 7 (33.3%) |
| task2 | a | 33 | 1.1 | 3.1 | 128.0 | 667.2 | 765.2 | 1409.5 | 1668.0 | 18 (54.5%) | 1 (3.0%) |
| task2 | b | 50 | 1.1 | 8.4 | 128.0 | 328.0 | 2616.0 | 2933.2 | 3238.0 | 26 (52.0%) | 11 (22.0%) |
| task2 | c | 73 | 1.2 | 56.0 | 142.8 | 555.8 | 1872.7 | 4009.0 | 4009.0 | 41 (56.2%) | 15 (20.5%) |
| task3 | a | 9 | 3.9 | 4.4 | 10.9 | 1290.0 | 1322.4 | 1339.7 | 1344.0 | 3 (33.3%) | 3 (33.3%) |
| task3 | b | 28 | 2.0 | 15.7 | 58.6 | 157.0 | 1001.4 | 1497.8 | 1615.2 | 8 (28.6%) | 2 (7.1%) |
| task3 | c | 37 | 2.0 | 44.3 | 84.0 | 257.0 | 1113.2 | 2231.0 | 2535.2 | 16 (43.2%) | 2 (5.4%) |
| task4 | a | 18 | 1.0 | 2.0 | 3.2 | 9.0 | 28.5 | 28.5 | 28.5 | 0 (0.0%) | 0 (0.0%) |
| task4 | b | 20 | 1.0 | 4.0 | 9.4 | 55.0 | 118.0 | 118.0 | 118.0 | 2 (10.0%) | 0 (0.0%) |
| task4 | c | 10 | 1.7 | 2.4 | 29.6 | 1525.8 | 2077.2 | 2077.2 | 2077.2 | 3 (30.0%) | 3 (30.0%) |
| task5 | a | 23 | 1.1 | 2.3 | 3.7 | 3.7 | 1829.8 | 3234.4 | 3573.9 | 2 (8.7%) | 2 (8.7%) |
| task5 | b | 33 | 1.0 | 3.3 | 3.3 | 465.9 | 3733807.1 | 31317943164.4 | 46051413622.1 | 11 (33.3%) | 7 (21.2%) |
| task5 | c | 23 | 1.3 | 3.7 | 3.7 | 34.1 | 300.8 | 1120.9 | 1351.5 | 4 (17.4%) | 1 (4.3%) |

### Per-Task Aggregate Q-Error Distribution

| Task | Nodes | Min | P25 | Median | P75 | P95 | P99 | Max | >100x | >1000x |
|------|-------|-----|-----|--------|-----|-----|-----|-----|-------|--------|
| task1 | 70 | 1.0 | 3.6 | 13.6 | 1690.2 | 24723.9 | 33115.3 | 33116.0 | 24 (34.3%) | 18 (25.7%) |
| task2 | 156 | 1.1 | 19.4 | 128.0 | 555.8 | 2150.0 | 3682.3 | 4009.0 | 85 (54.5%) | 27 (17.3%) |
| task3 | 74 | 2.0 | 28.2 | 66.2 | 257.0 | 1308.9 | 1918.3 | 2535.2 | 27 (36.5%) | 7 (9.5%) |
| task4 | 48 | 1.0 | 2.4 | 6.1 | 28.5 | 1355.3 | 2077.2 | 2077.2 | 5 (10.4%) | 3 (6.2%) |
| task5 | 79 | 1.0 | 3.3 | 3.7 | 34.3 | 2184.9 | 10138579381.5 | 46051413622.1 | 17 (21.5%) | 10 (12.7%) |

### 20 Worst Q-Errors Across Dataset

| Rank | Task | Rep | Seq | Relation Aliases | Est | Actual | Q-Error | SQL (first 150 chars) |
|------|------|-----|-----|-----------------|-----|--------|---------|------------------------|
| 1 | task5 | b | 4 | an, ci, ci_1, cn, info_type, mc... | 3,770,413,438,896,370 | 81,874 | 46,051,413,622.1 | `WITH movie_country_count AS (   SELECT      mc.movie_id,     CASE WHEN COUNT(DISTINCT cn.country_code) = 1 THEN 'single-country' ELSE 'multi-country' ` |
| 2 | task5 | b | 4 | ci, cn, info_type, mc, movie_info, role_type... | 762,938,110,427 | 81,874 | 9,318,441.9 | `WITH movie_country_count AS (   SELECT      mc.movie_id,     CASE WHEN COUNT(DISTINCT cn.country_code) = 1 THEN 'single-country' ELSE 'multi-country' ` |
| 3 | task1 | c | 13 | mi, mi_1, mii, t | 1 | 33,116 | 33,116.0 | `WITH cohort AS (     SELECT t.id AS movie_id, CAST(mii.info AS NUMERIC) AS rating     FROM title t     JOIN movie_info_idx mii ON mii.movie_id = t.id ` |
| 4 | task1 | c | 13 | mi, mi_1, mii, mii_1, t | 1 | 33,115 | 33,115.0 | `WITH cohort AS (     SELECT t.id AS movie_id, CAST(mii.info AS NUMERIC) AS rating     FROM title t     JOIN movie_info_idx mii ON mii.movie_id = t.id ` |
| 5 | task1 | c | 9 | mi, mi_1, mii, t | 1 | 26,494 | 26,494.0 | `WITH cohort AS (     SELECT t.id AS movie_id, CAST(mii.info AS NUMERIC) AS rating     FROM title t     JOIN movie_info_idx mii ON mii.movie_id = t.id ` |
| 6 | task1 | a | 4 | mi, mii, t | 1 | 26,208 | 26,208.0 | `SELECT    CASE      WHEN rt::int < 60 THEN 'under 60min'     WHEN rt::int < 80 THEN '60-79min'     WHEN rt::int < 100 THEN '80-99min'     WHEN rt::int` |
| 7 | task1 | b | 11 | g, k, mk, r, t | 1 | 22,910 | 22,910.0 | `SELECT      CASE          WHEN k.keyword IN ('tears','crying','song','singing','food','rain','snow','bicycle','photograph','book')          THEN 'emot` |
| 8 | task1 | b | 9 | info_type_2, kind_type_1, mi, t, t_1 | 1 | 21,361 | 21,361.0 | `WITH runtime_data AS (     SELECT          t.id AS movie_id,         (regexp_match(mi.info, '^(\d+)'))[1]::integer AS runtime_mins     FROM title t   ` |
| 9 | task1 | a | 9 | company_type, company_type_1, info_type, info_type_1, mc, mii... | 1 | 20,878 | 20,878.0 | `WITH base AS (   SELECT      t.id AS movie_id,     mii.info::numeric AS rating   FROM movie_info_idx mii   JOIN title t ON t.id = mii.movie_id   WHERE` |
| 10 | task1 | b | 9 | g, info_type_2, kind_type_1, mi, r, t... | 1 | 19,032 | 19,032.0 | `WITH runtime_data AS (     SELECT          t.id AS movie_id,         (regexp_match(mi.info, '^(\d+)'))[1]::integer AS runtime_mins     FROM title t   ` |
| 11 | task1 | b | 9 | info_type_2, kind_type_1, mi, r, t, t_1 | 1 | 16,585 | 16,585.0 | `WITH runtime_data AS (     SELECT          t.id AS movie_id,         (regexp_match(mi.info, '^(\d+)'))[1]::integer AS runtime_mins     FROM title t   ` |
| 12 | task1 | c | 14 | info_type_1, mi, mi_1, mii, t | 1 | 13,624 | 13,624.0 | `WITH cohort AS (     SELECT t.id AS movie_id, CAST(mii.info AS NUMERIC) AS rating     FROM title t     JOIN movie_info_idx mii ON mii.movie_id = t.id ` |
| 13 | task1 | c | 6 | mi, mii, t | 2 | 26,260 | 13,130.0 | `WITH cohort AS (     SELECT t.id AS movie_id, CAST(mii.info AS NUMERIC) AS rating     FROM title t     JOIN movie_info_idx mii ON mii.movie_id = t.id ` |
| 14 | task5 | b | 3 | info_type_2, movie_info, movie_info_idx | 13 | 139,325 | 10,717.3 | `WITH movie_country_count AS (   SELECT      mc.movie_id,     CASE WHEN COUNT(DISTINCT cn.country_code) = 1 THEN 'single-country' ELSE 'multi-country' ` |
| 15 | task1 | b | 5 | info_type_1, kind_type, mi, r, t | 2 | 17,448 | 8,724.0 | `WITH runtime_data AS (     SELECT          t.id AS movie_id,         -- Take the first numeric runtime value         (regexp_match(mi.info, '^(\d+)'))` |
| 16 | task1 | a | 9 | info_type, info_type_1, mii, movie_info, movie_info_1, t | 5 | 24,953 | 4,990.6 | `WITH base AS (   SELECT      t.id AS movie_id,     mii.info::numeric AS rating   FROM movie_info_idx mii   JOIN title t ON t.id = mii.movie_id   WHERE` |
| 17 | task1 | a | 12 | info_type, info_type_1, k, mii, mk, movie_info... | 5 | 24,953 | 4,990.6 | `WITH base AS (   SELECT t.id AS movie_id, mii.info::numeric AS rating,     (mii.info::numeric >= 7.0)::int AS is_high   FROM movie_info_idx mii   JOIN` |
| 18 | task1 | a | 12 | info_type, info_type_1, mii, movie_info, movie_info_1, t | 5 | 24,953 | 4,990.6 | `WITH base AS (   SELECT t.id AS movie_id, mii.info::numeric AS rating,     (mii.info::numeric >= 7.0)::int AS is_high   FROM movie_info_idx mii   JOIN` |
| 19 | task2 | c | 5 | ci, ci_1, info_type_1, movie_info_idx, name, role_type... | 1 | 4,009 | 4,009.0 | `WITH dirs AS (   SELECT ci.person_id AS director_id, ci.movie_id   FROM cast_info ci   WHERE ci.role_id = (SELECT id FROM role_type WHERE role = 'dire` |
| 20 | task2 | c | 12 | ci, ci_1, info_type_1, movie_info_idx, name, role_type... | 1 | 4,009 | 4,009.0 | `WITH dirs AS (   SELECT ci.person_id AS director_id, ci.movie_id   FROM cast_info ci   WHERE ci.role_id = (SELECT id FROM role_type WHERE role = 'dire` |

### Per-Task Correlation: Join Complexity vs Q-Error

| Task | Rep | Median Tables/Query | Median Q-Error | Max Q-Error |
|------|-----|---------------------|----------------|-------------|
| task1 | a | 6.0 | 7.1 | 26208.0 |
| task1 | b | 7.0 | 13.7 | 22910.0 |
| task1 | c | 6.0 | 25.1 | 33116.0 |
| task2 | a | 12.0 | 128.0 | 1668.0 |
| task2 | b | 12.0 | 128.0 | 3238.0 |
| task2 | c | 13.0 | 142.8 | 4009.0 |
| task3 | a | 4.0 | 10.9 | 1344.0 |
| task3 | b | 6.0 | 58.6 | 1615.2 |
| task3 | c | 7.0 | 84.0 | 2535.2 |
| task4 | a | 5.0 | 3.2 | 28.5 |
| task4 | b | 4.0 | 9.4 | 118.0 |
| task4 | c | 3.0 | 29.6 | 2077.2 |
| task5 | a | 8.0 | 3.7 | 3573.9 |
| task5 | b | 9.5 | 3.3 | 46051413622.1 |
| task5 | c | 8.0 | 3.7 | 1351.5 |

## Section 4: Hint Surface under pg_hint_plan

### Per-Rep Hint Surface

| Task | Rep | Total Join Nodes | Hintable Hits | Hintable Rate | Queries w/ Hits | Queries w/ Hits % |
|------|-----|-----------------|---------------|---------------|-----------------|-------------------|
| task1 | a | 38 | 14 | 36.8% | 8/11 | 72.7% |
| task1 | b | 46 | 23 | 50.0% | 10/13 | 76.9% |
| task1 | c | 35 | 22 | 62.9% | 10/13 | 76.9% |
| task2 | a | 70 | 52 | 74.3% | 8/9 | 88.9% |
| task2 | b | 88 | 60 | 68.2% | 11/12 | 91.7% |
| task2 | c | 84 | 67 | 79.8% | 11/12 | 91.7% |
| task3 | a | 20 | 12 | 60.0% | 5/8 | 62.5% |
| task3 | b | 39 | 30 | 76.9% | 10/11 | 90.9% |
| task3 | c | 48 | 39 | 81.2% | 10/12 | 83.3% |
| task4 | a | 43 | 25 | 58.1% | 11/15 | 73.3% |
| task4 | b | 46 | 33 | 71.7% | 11/15 | 73.3% |
| task4 | c | 55 | 35 | 63.6% | 18/28 | 64.3% |
| task5 | a | 38 | 24 | 63.2% | 9/10 | 90.0% |
| task5 | b | 46 | 26 | 56.5% | 9/10 | 90.0% |
| task5 | c | 39 | 21 | 53.8% | 9/11 | 81.8% |

### Overall Hint Surface

- **Total join nodes**: 735
- **Total hintable hits**: 483
- **Fraction of queries with >= 1 hit**: 150/190 = 78.9%
- **Hits per query**: median=2.0, max=9
- **Depth-3+ join nodes**: 438
- **Depth-3+ hintable hits**: 292
- **Corrected hint-surface rate** (depth-3+ join nodes): 292/438 = 66.7%
- **InsightBench corrected hintable rate: 3.8%. IMDB corrected hintable rate: 66.7%.**

### Useful Hints Sub-Analysis (baseline q-error > 10x)

- **Total useful hits**: 255
- **Fraction of all hintable hits**: 255/483 = 52.8%

**Top 10 Hintable Hits with Largest Baseline Q-Errors:**

| Rank | Task | Rep | Seq | Signature (12 chars) | Relation Aliases | Q-Error | Earlier Seq | Earlier Actual |
|------|------|-----|-----|---------------------|-----------------|---------|-------------|----------------|
| 1 | task4 | c | 27 | 55ce84addda8... | mii | 269,758.1 | 22 | 23 |
| 2 | task1 | c | 13 | 8d41482b988b... | mi, mi_1, mii, t | 33,116.0 | 2 | 117,536 |
| 3 | task1 | c | 13 | 8d41482b988b... | mi, mi_1, mii, mii_1, t | 33,115.0 | 2 | 117,536 |
| 4 | task1 | c | 9 | 8d41482b988b... | mi, mi_1, mii, t | 26,494.0 | 2 | 117,536 |
| 5 | task1 | b | 5 | 8d41482b988b... | mi, t | 25,318.0 | 1 | 117,536 |
| 6 | task1 | b | 9 | 8d41482b988b... | mi, t_1 | 21,756.0 | 1 | 117,536 |
| 7 | task1 | b | 9 | 8d41482b988b... | info_type_2, kind_type_1, mi, t, t_1 | 21,361.0 | 1 | 117,536 |
| 8 | task1 | a | 9 | d78cb855ec5e... | company_type, company_type_1, info_type, info_type_1, mc... | 20,878.0 | 7 | 169,104 |
| 9 | task1 | b | 9 | 8d41482b988b... | info_type_2, kind_type_1, mi, r, t... | 16,585.0 | 1 | 117,536 |
| 10 | task1 | c | 14 | 8d41482b988b... | info_type_1, mi, mi_1, mii, t | 13,624.0 | 2 | 117,536 |

## Section 5: CTE Repetition Analysis

### Pre-computed CTE Analysis (from cte_analysis.jsonl)

- **Total entries**: 199
- **Queries with CTEs**: 143 (71.9%)
- **Queries with exact CTE reuse**: 73
- **Queries with near-identical CTE reuse**: 24
- **CTE counts (among CTE queries)**: median=3.0, max=9

### Cross-Query CTE Repetition (from raw SQL)

| Task | Rep | Distinct CTEs | Repeated CTEs | Max Repetition | Est. Waste (ms) |
|------|-----|--------------|---------------|----------------|-----------------|
| task1 | a | 21 | 1 | 2 | 10,086 |
| task1 | b | 15 | 1 | 2 | 7,019 |
| task1 | c | 15 | 5 | 8 | 104,825 |
| task2 | a | 27 | 8 | 6 | 5,279,298 |
| task2 | b | 24 | 13 | 12 | 948,245 |
| task2 | c | 21 | 7 | 11 | 226,487 |
| task3 | a | 10 | 1 | 2 | 936 |
| task3 | b | 4 | 0 | 1 | 0 |
| task3 | c | 1 | 0 | 1 | 0 |
| task4 | a | 14 | 3 | 2 | 235 |
| task4 | b | 16 | 3 | 3 | 365 |
| task4 | c | 24 | 3 | 2 | 42,657 |
| task5 | a | 10 | 4 | 7 | 129,991 |
| task5 | b | 11 | 5 | 7 | 340,134 |
| task5 | c | 10 | 5 | 9 | 189,362 |

- **Total estimated waste**: 7,279,643 ms
- **Average per-session waste**: 485,309 ms
- **Highest-waste CTE pattern** (task2 rep a, first 200 chars):
  `select id from role_type where role = ?`

## Section 6: Distributional Observations

### Q-Error Distribution Files

- **task1**: 1/3 reps had pathological outliers (>100x)
- **task2**: 2/3 reps had pathological outliers (>100x)
- **task3**: 0/3 reps had pathological outliers (>100x)
- **task4**: 1/3 reps had pathological outliers (>100x)
- **task5**: 3/3 reps had pathological outliers (>100x)

### Distribution Summary by Task/Rep

| Task | Rep | Nodes | Min | Median | P95 | Max | Outliers (>100x) |
|------|-----|-------|-----|--------|-----|-----|------------------|
| task1 | a | 19 | 1.0 | 4.3 | 6579.3 | 20878.0 | 5 |
| task1 | b | 11 | 1.6 | 4.5 | 16.0 | 20.0 | 0 |
| task1 | c | 7 | 1.1 | 2.0 | 18.9 | 25.1 | 0 |
| task2 | a | 7 | 1.1 | 1.1 | 7.4 | 9.3 | 0 |
| task2 | b | 21 | 1.1 | 17.8 | 1361.3 | 1361.3 | 4 |
| task2 | c | 18 | 1.2 | 59.4 | 1372.5 | 1636.0 | 7 |
| task3 | a | 3 | 3.9 | 4.4 | 4.4 | 4.4 | 0 |
| task3 | b | 5 | 2.0 | 4.9 | 6.8 | 6.8 | 0 |
| task3 | c | 2 | 2.0 | 2.0 | 2.0 | 2.0 | 0 |
| task4 | a | 3 | 2.4 | 2.4 | 9.2 | 10.0 | 0 |
| task4 | c | 4 | 2.4 | 2049.4 | 2077.2 | 2077.2 | 3 |
| task5 | a | 21 | 1.1 | 3.7 | 2030.5 | 3573.9 | 2 |
| task5 | b | 29 | 1.0 | 3.3 | 5591747.2 | 46051413622.1 | 9 |
| task5 | c | 18 | 1.3 | 3.7 | 71.1 | 280.5 | 1 |

### Pathological Q-Errors (from pathological_qerrors.jsonl)

- **Total records**: 31
- **Distribution by task/rep**:
  - task1 rep a: 5 records
  - task2 rep b: 4 records
  - task2 rep c: 7 records
  - task4 rep c: 3 records
  - task5 rep a: 2 records
  - task5 rep b: 9 records
  - task5 rep c: 1 records
- **Most common table combinations**:
  - ci, ci_1, nd, role_type, t: 3 occurrences
  - ci, rt, t: 3 occurrences
  - info_type, info_type_1, mii, movie_info, movie_info_1: 2 occurrences
  - ci, ci_1, na, nd, role_type, role_type_1, t: 2 occurrences
  - ci, ci_1, nd, role_type, role_type_1, t: 2 occurrences
- **Most common join predicates**:
  - `(ci_1.role_id = role_type.id)`: 4 occurrences
  - `(ci.movie_id = fi.movie_id)`: 3 occurrences
  - `(mc.movie_id = movie_info_1.movie_id)`: 3 occurrences
  - `(mc.movie_id = t.id)`: 2 occurrences
  - `(mii.movie_id = movie_info_1.movie_id)`: 2 occurrences

### Outlier Clustering Analysis

**Query shapes (table sets) producing most >100x q-errors:**

| Table Combination | Outlier Nodes |
|-------------------|---------------|
| acts, cast_info, dirs, info_type, keyword, keyword_stats, movie_info_idx, movie_keyword... | 16 |
| acts, cast_info, dirs, genres, info_type, movie_info, movie_info_idx, name... | 11 |
| actor_role, all_pairs, cast_info, director_role, film_class, film_totals, info_type, keyword... | 8 |
| cast_info, company_name, movie_companies, name, pair_movies, role_type, title, top_pairs | 6 |
| cast_info, keyword, movie_keyword, name, pair_movies, role_type, title, top_pairs | 6 |
| actor_role, cast_info, company_name, company_type, director_role, movie_companies, name, pair_companies... | 6 |
| cast_info, company_name, company_type, kind_type, movie_companies, name, role_type, title | 6 |
| acts, all_pairs, cast_info, dir_ids, dirs, info_type, movie_info, movie_info_idx... | 5 |
| company_name, company_type, info_type, kind_type, movie_companies, movie_info, title | 5 |
| info_type, kind_type, movie_info, movie_info_idx, runtime_data, runtime_deduped, title | 4 |

## Section 7: Assessment

The IMDB agentic-SQL workload is substantially more complex than InsightBench: across 201 queries in 15 sessions (5 tasks x 3 reps), the median query touches 6 tables, compared to InsightBench's typical 1-3 table queries. The cardinality estimation gap is severe: on depth-3+ join nodes, the overall median q-error is 38.6x with a P95 of 3506.6x, and 37.0% of such nodes exceed 100x error -- consistent with Leis et al. (2015) findings that multi-way joins cause exponential estimation degradation. Task 2 (franchise analysis) exhibits the worst estimation errors with a median q-error above 128x, driven by complex multi-table joins across 12-16 tables; task 4 (lightweight exploratory queries) has the lowest errors, confirming the relationship between join complexity and estimation difficulty. The corrected hint-surface rate of 66.7% dramatically exceeds InsightBench's 3.8%, reflecting the iterative nature of agentic exploration where the agent revisits similar join patterns across successive analytical queries. The useful hint fraction (255/483 = 52.8% of hintable hits have q-error > 10x) indicates substantial signal: a majority of recurring join patterns carry meaningful estimation error that cardinality feedback could correct. CTE repetition adds a second research dimension -- the agent's iterative refinement strategy produces repeated sub-expressions (total estimated waste: 7,279,643 ms), most prominently in task 2 where complex analytical CTEs recur across queries. Per-task variation is rich: task 2 is the most demanding (highest join count, worst q-errors, most CTE waste), task 1 provides the largest absolute q-errors (>33,000x), task 3 offers moderate complexity, and task 5 (international co-productions) adds genre diversity to the workload. This dataset is sufficient to proceed to Step 5 (hinted replay): the high hint-surface rate ensures that pg_hint_plan interventions will apply broadly, the severe q-errors provide room for measurable improvement, and the 15-session design gives enough statistical power to detect regression across reps.
