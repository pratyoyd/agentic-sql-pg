# Plan Shape Differences at Scale

**Scale factor:** ×1819 (550 → 1000450 rows)
**Method:** 4 runs per query per database, discard first, median of last 3

## Q0_scan_agg

**Topology changed:** YES
  - Seq Scan → Gather Merge
  - Node count: 3 → 6

**Original plan:**
```
Sort (est=4, actual=4)
  Aggregate [Hashed] (est=4, actual=4)
    Seq Scan (est=550, actual=550)
```

**Scaled plan:**
```
Sort (est=4, actual=4)
  Aggregate [Sorted] (est=4, actual=4)
    Gather Merge (est=8, actual=12)
      Sort (est=4, actual=4)
        Aggregate [Hashed] (est=4, actual=4)
          Seq Scan (est=416861, actual=333483)
```

**Latency:** original=0.34ms, scaled=259.36ms, ratio=751.8×
**Row count ratio:** 1819×
**Latency vs row ratio:** proportional

## Q10_join_agg

**Topology changed:** YES
  - Sort → Merge Join
  - Join: None → Left
  - Hash Join → Gather Merge
  - Join: Right → None
  - Seq Scan → Sort
  - Hash → Seq Scan
  - Seq Scan → Sort
  - Node count: 7 → 8

**Original plan:**
```
Sort (est=4, actual=4)
  Aggregate [Sorted] (est=4, actual=4)
    Sort (est=1194, actual=1194)
      Hash Join [Right] (est=1194, actual=1194)
        Seq Scan (est=54, actual=54)
        Hash (est=550, actual=550)
          Seq Scan (est=550, actual=550)
```

**Scaled plan:**
```
Sort (est=4, actual=4)
  Aggregate [Sorted] (est=4, actual=4)
    Merge Join [Left] (est=2167846, actual=2171886)
      Gather Merge (est=1000467, actual=1000450)
        Sort (est=416861, actual=333483)
          Seq Scan (est=416861, actual=333483)
      Sort (est=54, actual=1902706)
        Seq Scan (est=54, actual=54)
```

**Latency:** original=5.77ms, scaled=2422.39ms, ratio=420.1×
**Row count ratio:** 1819×
**Latency vs row ratio:** sub-proportional

## Q6_cte_join

**Topology changed:** YES
  - Seq Scan → Gather
  - Sort → Seq Scan
  - Hash Join → Aggregate
  - Join: Inner → None
  - Aggregate → Nested Loop
  - Join: None → Inner
  - Seq Scan → Aggregate
  - Hash → Gather Merge
  - Subquery Scan → Sort
  - WindowAgg → Aggregate
  - Aggregate → Seq Scan
  - Seq Scan → Materialize
  - Node count: 13 → 20

**Original plan:**
```
Sort (est=4, actual=4)
  Aggregate [Plain] (est=1, actual=1)
    Seq Scan (est=129, actual=129)
  Aggregate [Sorted] (est=4, actual=4)
    Sort (est=4, actual=16)
      Hash Join [Inner] (est=4, actual=16)
        Aggregate [Hashed] (est=16, actual=16)
          Seq Scan (est=550, actual=550)
        Hash (est=4, actual=4)
          Subquery Scan (est=4, actual=4)
            WindowAgg (est=4, actual=4)
              Aggregate [Hashed] (est=4, actual=4)
                Seq Scan (est=129, actual=129)
```

**Scaled plan:**
```
Sort (est=4, actual=4)
  Aggregate [Plain] (est=1, actual=1)
    Gather (est=2, actual=3)
      Aggregate [Plain] (est=1, actual=1)
        Seq Scan (est=97615, actual=78217)
  Aggregate [Sorted] (est=4, actual=4)
    Nested Loop [Inner] (est=4, actual=16)
      Aggregate [Sorted] (est=16, actual=16)
        Gather Merge (est=32, actual=48)
          Sort (est=16, actual=16)
            Aggregate [Hashed] (est=16, actual=16)
              Seq Scan (est=416861, actual=333483)
      Materialize (est=4, actual=2)
        Subquery Scan (est=4, actual=4)
          WindowAgg (est=4, actual=4)
            Aggregate [Sorted] (est=4, actual=4)
              Gather Merge (est=8, actual=12)
                Sort (est=4, actual=4)
                  Aggregate [Hashed] (est=4, actual=4)
                    Seq Scan (est=97615, actual=78217)
```

**Latency:** original=1.61ms, scaled=685.09ms, ratio=424.5×
**Row count ratio:** 1819×
**Latency vs row ratio:** sub-proportional
