# Predicate Pair Analysis

Near-miss operator pairs: same operator type, same tables, same group-by keys,
but different signatures due to different predicates. 20 pairs sampled across
5 sessions and 3 operator types (Seq Scan, Aggregate, Hash Join).

## Pair Classifications

```
Pair 1
  Session: flag-11_rep_a
  Operator type: Seq Scan
  Tables: [flag_11]
  Predicate A: ((closed_at IS NOT NULL) AND (opened_at IS NOT NULL))
  Predicate B: ((closed_at IS NOT NULL) AND (opened_at IS NOT NULL) AND (category = 'Hardware'::text))
  Difference: B adds a category equality filter
  Classification: SEMANTIC
  Reasoning: Additional WHERE predicate restricts result set.

Pair 2
  Session: flag-11_rep_a
  Operator type: Seq Scan
  Tables: [flag_11]
  Predicate A: ((closed_at IS NOT NULL) AND (opened_at IS NOT NULL))
  Predicate B: ((closed_at IS NOT NULL) AND (opened_at IS NOT NULL) AND (opened_at >= '2023-07-01'::timestamp) AND (category = 'Hardware'::text))
  Difference: B adds date range and category filters
  Classification: SEMANTIC
  Reasoning: Two additional conjuncts narrow the scan.

Pair 3
  Session: flag-11_rep_a
  Operator type: Seq Scan
  Tables: [flag_11]
  Predicate A: ((closed_at IS NOT NULL) AND (opened_at IS NOT NULL))
  Predicate B: ((opened_at >= '2023-07-01'::timestamp) AND (category = 'Hardware'::text))
  Difference: A checks NOT NULL on two cols; B filters on date range and category
  Classification: SEMANTIC
  Reasoning: Entirely different predicate sets — different columns, different operators.

Pair 4
  Session: flag-11_rep_a
  Operator type: Seq Scan
  Tables: [flag_11]
  Predicate A: ((closed_at IS NOT NULL) AND (opened_at IS NOT NULL))
  Predicate B: (category = 'Hardware'::text)
  Difference: A checks nullability; B filters on category
  Classification: SEMANTIC
  Reasoning: No predicate overlap at all.

Pair 5
  Session: flag-11_rep_a
  Operator type: Seq Scan
  Tables: [flag_11]
  Predicate A: ((closed_at IS NOT NULL) AND (opened_at IS NOT NULL))
  Predicate B: ((closed_at IS NOT NULL) AND (category = 'Hardware'::text))
  Difference: A has two NOT NULL checks; B replaces one with a category filter
  Classification: SEMANTIC
  Reasoning: Different predicate composition — drops opened_at check, adds category filter.

Pair 6
  Session: flag-11_rep_a
  Operator type: Seq Scan
  Tables: [flag_11]
  Predicate A: ((closed_at IS NOT NULL) AND (opened_at IS NOT NULL))
  Predicate B: ((opened_at >= '2023-07-18'::timestamp) AND (opened_at < '2023-09-01'::timestamp) AND (category = 'Hardware'::text))
  Difference: A checks nullability; B applies date range and category filter
  Classification: SEMANTIC
  Reasoning: Completely different predicate structure and columns.

Pair 7
  Session: flag-12_rep_a
  Operator type: Seq Scan
  Tables: [flag_12]
  Predicate A: (none)
  Predicate B: (assignment_group = 'Hardware'::text)
  Difference: A has no predicate; B filters on assignment_group
  Classification: SEMANTIC
  Reasoning: Unfiltered scan vs filtered scan.

Pair 8
  Session: flag-12_rep_a
  Operator type: Seq Scan
  Tables: [flag_12]
  Predicate A: (none)
  Predicate B: (caller_id = 'ITIL User'::text)
  Difference: A has no predicate; B filters on caller_id
  Classification: SEMANTIC
  Reasoning: Unfiltered scan vs filtered scan on a different column than pair 7.

Pair 9
  Session: flag-12_rep_a
  Operator type: Aggregate
  Tables: []
  Group by: [caller_id]
  Predicate A: (count(*) > 5)
  Predicate B: (none)
  Difference: A has a HAVING clause; B does not
  Classification: SEMANTIC
  Reasoning: HAVING filter changes output row count.

Pair 10
  Session: flag-18_rep_a
  Operator type: Hash Join
  Tables: []
  Predicate A: (a.assigned_to = u.name)
  Predicate B: (a.assigned_to = u.name), (a.purchased_on < u.start_date)
  Difference: B adds an additional join predicate on purchased_on
  Classification: SEMANTIC
  Reasoning: Additional inequality join condition restricts matched rows.

Pair 11
  Session: flag-18_rep_a
  Operator type: Hash Join
  Tables: []
  Predicate A: (a.assigned_to = u.name)
  Predicate B: (a.assigned_to = u.name), (a.warranty_expiration < (u.start_date + '1 year'::interval))
  Difference: B adds warranty_expiration filter alongside join condition
  Classification: SEMANTIC
  Reasoning: Additional predicate on different columns.

Pair 12
  Session: flag-18_rep_a
  Operator type: Hash Join
  Tables: []
  Predicate A: (a.assigned_to = u.name), (a.purchased_on < u.start_date)
  Predicate B: (a.assigned_to = u.name), (a.warranty_expiration < (u.start_date + '1 year'::interval))
  Difference: A filters on purchased_on; B filters on warranty_expiration with interval arithmetic
  Classification: SEMANTIC
  Reasoning: Different columns in the inequality predicate.

Pair 13
  Session: flag-19_rep_a
  Operator type: Aggregate
  Tables: []
  Group by: [ci]
  Predicate A: (none)
  Predicate B: (count(*) > 1)
  Difference: B has a HAVING clause
  Classification: SEMANTIC
  Reasoning: HAVING filter restricts output groups.

Pair 14
  Session: flag-19_rep_a
  Operator type: Aggregate
  Tables: []
  Group by: [ci]
  Predicate A: (none)
  Predicate B: (count(DISTINCT department) > 1)
  Difference: B has a HAVING clause with DISTINCT count
  Classification: SEMANTIC
  Reasoning: HAVING filter on a different aggregate expression than pair 13.

Pair 15
  Session: flag-19_rep_a
  Operator type: Aggregate
  Tables: []
  Group by: [ci]
  Predicate A: (count(*) > 1)
  Predicate B: (count(DISTINCT department) > 1)
  Difference: A counts all rows; B counts distinct departments
  Classification: SEMANTIC
  Reasoning: Different aggregate functions in HAVING — count(*) vs count(DISTINCT col).

Pair 16
  Session: flag-19_rep_a
  Operator type: Aggregate
  Tables: []
  Group by: [department]
  Predicate A: (count(*) FILTER (WHERE (state = 'Declined'::text)) > 0)
  Predicate B: (none)
  Difference: A has a FILTER HAVING clause; B has none
  Classification: SEMANTIC
  Reasoning: FILTER clause restricts counted rows to declined state.

Pair 17
  Session: flag-19_rep_a
  Operator type: Hash Join
  Tables: []
  Predicate A: (flag_19."user" = flag_19_1."user")
  Predicate B: (f2.ci = f1.ci), (f1.department <> f2.department)
  Difference: A is a self-join on user; B is a self-join on ci with department inequality
  Classification: SEMANTIC
  Reasoning: Completely different join columns and semantics.

Pair 18
  Session: flag-20_rep_a
  Operator type: Aggregate
  Tables: []
  Group by: [ci]
  Predicate A: (none)
  Predicate B: ((count(*) FILTER (WHERE ((category = 'Travel') AND (state = 'Declined'))) > 0) AND (count(*) FILTER (WHERE (category <> 'Travel')) > 0))
  Difference: A has no HAVING; B has complex dual-FILTER HAVING
  Classification: SEMANTIC
  Reasoning: Multi-condition HAVING clause filters to specific behavioral pattern.

Pair 19
  Session: flag-20_rep_a
  Operator type: Aggregate
  Tables: []
  Group by: [ci]
  Predicate A: (none)
  Predicate B: (count(*) FILTER (WHERE (state = ANY ('{Declined,Processed}'::text[]))) > 0)
  Difference: A has no HAVING; B filters on state membership in array
  Classification: SEMANTIC
  Reasoning: HAVING clause with array containment check.

Pair 20
  Session: flag-20_rep_a
  Operator type: Aggregate
  Tables: []
  Group by: [ci]
  Predicate A: ((count(*) FILTER (WHERE ((category = 'Travel') AND (state = 'Declined'))) > 0) AND (count(*) FILTER (WHERE (category <> 'Travel')) > 0))
  Predicate B: (count(*) FILTER (WHERE (state = ANY ('{Declined,Processed}'::text[]))) > 0)
  Difference: A checks Travel+Declined co-occurrence; B checks Declined/Processed membership
  Classification: SEMANTIC
  Reasoning: Different FILTER predicates testing different analytical hypotheses.
```

## Summary

```
Total pairs analyzed: 20
Cosmetic: 0
Semantic: 20
Ambiguous: 0
```

## Decision: normalization not implemented

Since cosmetic = 0 (well below the threshold of 12), predicate normalization is
not warranted. The 27-point gap between DuckDB plan-critical reuse (74.8%) and
Postgres plan-critical reuse (47.5%) is entirely driven by genuine semantic
differences in the predicates the agent generates from one query to the next.
The Postgres agent explores analytical angles that produce legitimately different
filter conditions, HAVING clauses, and join predicates — not cosmetic SQL dialect
variation. The `::text` casts and identifier quoting that appear in Postgres plan
output are already handled by the templatization step (which replaces literal
values with placeholders). Every near-miss pair shows the agent adding, removing,
or changing a filter column, operator, or aggregate function — not reformatting
the same logical predicate.

The 47.5% plan-critical reuse rate and 65.8% hintable reuse rate are the real
Postgres numbers and will be used as-is in Stage 3.
