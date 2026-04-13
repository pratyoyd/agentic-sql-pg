# Signature Sanity Check

Manual inspection of plan trees to validate operator_signature hashing.

## Sample Plan Trees

### Sample 1: flag-10_rep_a query #0
```sql
SELECT
  DATE_TRUNC('month', opened_at) AS month,
  COUNT(*) AS ticket_count,
  ROUND(AVG(EXTRACT(EPOCH FROM (closed_at - opened_at))/3600)::numeric, 2) AS avg_ttr_hours,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (closed_at - opened_at))/3600) AS median_ttr_hours
FROM flag_10
WHERE closed_at IS NOT NULL AND opened_at IS NOT NULL
GROUP BY DATE_TRUNC('month', opened_at)
ORDER BY month
```

**Plan tree:**
```
[Aggregate] sig=7eaa069f929e... ec=416 ac=12 class=PLAN-CRITICAL
  group_by: ["(date_trunc('month'::text, opened_at))"]
  [Sort] sig=0d43b00b2bd7... ec=416 ac=416 class=bookkeeping
    [Seq Scan] sig=1c4d59d5b40b... ec=416 ac=416 class=PLAN-CRITICAL
      tables: ['flag_10']
      predicates: ['((closed_at IS NOT NULL) AND (opened_at IS NOT NULL))']
```

Signature verification: all nodes match recomputed signatures.

### Sample 2: flag-11_rep_a query #0
```sql
SELECT 
    category,
    COUNT(*) as incident_count,
    ROUND(AVG(EXTRACT(EPOCH FROM (closed_at - opened_at))/3600)::numeric, 2) as avg_resolution_hours,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (closed_at - opened_at))/3600)::numeric, 2) as median_resolution_hours,
    ROUND(MAX(EXTRACT(EPOCH FROM (closed_at - opened_at))/3600)::numeric, 2) as max_resolution_hours
FROM flag_11
WHERE closed_at IS NOT NULL AND opened_at IS NOT NULL
GROUP BY category
ORDER BY avg_resolution_hours DESC;
```

**Plan tree:**
```
[Sort] sig=0d43b00b2bd7... ec=5 ac=5 class=bookkeeping
  [Aggregate] sig=c548891c12f6... ec=5 ac=5 class=PLAN-CRITICAL
    group_by: ['category']
    [Sort] sig=0d43b00b2bd7... ec=600 ac=600 class=bookkeeping
      [Seq Scan] sig=631b978954f4... ec=600 ac=600 class=PLAN-CRITICAL
        tables: ['flag_11']
        predicates: ['((closed_at IS NOT NULL) AND (opened_at IS NOT NULL))']
```

Signature verification: all nodes match recomputed signatures.

### Sample 3: flag-12_rep_a query #0
```sql
SELECT 
    assigned_to,
    COUNT(*) AS total_incidents,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct_of_total,
    COUNT(CASE WHEN priority = '1 - Critical' THEN 1 END) AS critical,
    COUNT(CASE WHEN priority = '2 - High' THEN 1 END) AS high,
    COUNT(CASE WHEN state = 'Closed' THEN 1 END) AS closed,
    COUNT(CASE WHEN state = 'Open' THEN 1 END) AS open
FROM flag_12
GROUP BY assigned_to
ORDER BY total_incidents DESC
```

**Plan tree:**
```
[Sort] sig=0d43b00b2bd7... ec=5 ac=5 class=bookkeeping
  [WindowAgg] sig=158889157894... ec=5 ac=5 class=PLAN-CRITICAL
    [Aggregate] sig=6da86a7726c4... ec=5 ac=5 class=PLAN-CRITICAL
      group_by: ['assigned_to']
      [Seq Scan] sig=d79893a2a497... ec=500 ac=500 class=PLAN-CRITICAL
        tables: ['flag_12']
```

Signature verification: all nodes match recomputed signatures.

## Signature Reuse Example

**Session:** flag-10_rep_a
**First occurrence:** query #0
```sql
SELECT
  DATE_TRUNC('month', opened_at) AS month,
  COUNT(*) AS ticket_count,
  ROUND(AVG(EXTRACT(EPOCH FROM (closed_at - opened_at))/3600)::numeric, 2) AS avg_ttr_hours,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (closed_at - opened_at))/3600) AS median_ttr_hours
FROM flag_10
WHERE closed_at IS NOT NULL AND opened_at IS NOT NULL
GROUP BY DATE_TRUNC('month', opened_at)
ORDER BY month
```

First node:
```
[Seq Scan] sig=1c4d59d5b40b... ec=416 ac=416 class=PLAN-CRITICAL
  tables: ['flag_10']
  predicates: ['((closed_at IS NOT NULL) AND (opened_at IS NOT NULL))']
```

**Second occurrence:** query #1
```sql
SELECT
  category,
  DATE_TRUNC('month', opened_at) AS month,
  COUNT(*) AS ticket_count,
  ROUND(AVG(EXTRACT(EPOCH FROM (closed_at - opened_at))/3600)::numeric, 2) AS avg_ttr_hours
FROM flag_10
WHERE closed_at IS NOT NULL AND opened_at IS NOT NULL
GROUP BY category, DATE_TRUNC('month', opened_at)
ORDER BY category, month
```

Second node:
```
[Seq Scan] sig=1c4d59d5b40b... ec=416 ac=416 class=PLAN-CRITICAL
  tables: ['flag_10']
  predicates: ['((closed_at IS NOT NULL) AND (opened_at IS NOT NULL))']
```

**First:** EC=416, actual=416
**Second:** EC=416, actual=416
**Q-error at second occurrence:** 1.00×
**Q-error after reuse (using first actual):** would use actual=416 instead of EC=416

## Signature Statistics

- **Total plan nodes across all sessions:** 1671
- **Unique signatures:** 439
- **Signature compression ratio:** 0.263
