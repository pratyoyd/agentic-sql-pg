# Task 2 Rep A: End-to-End Replay Report

## Executive Summary

Of 12 queries: 0 improved, 0 neutral, 3 regressed, 9 unchanged. Total session wall-clock: baseline 83568ms, hinted 82198ms, delta -1370ms (-1.6%). 
The mechanism produced a net session-level improvement.

## Per-Query Results

| Q | Baseline (ms) | Hinted (ms) | Delta (ms) | Delta % | Hints | Plan Changed | Class | BL QE | H QE |
|---|--------------|-------------|------------|---------|-------|-------------|-------|-------|------|
| 12 | 39138.5 | 37695.3 | -1443.2 | -3.7% | 4 | N | unchanged | 9 | 6 |
| 11 | 3393.7 | 3271.9 | -121.8 | -3.6% | 5 | N | unchanged | 9 | 6 |
| 10 | 4909.1 | 4758.0 | -151.1 | -3.1% | 5 | N | unchanged | 9 | 6 |
| 9 | 6308.3 | 6209.4 | -99.0 | -1.6% | 2 | N | unchanged | 12 | 12 |
| 1 | 6454.6 | 6450.4 | -4.2 | -0.1% | 0 | N | unchanged | 45 | 45 |
| 0 | 10064.0 | 10092.6 | +28.6 | +0.3% | 0 | N | unchanged | 23 | 23 |
| 7 | 4359.2 | 4409.2 | +50.0 | +1.1% | 1 | N | unchanged | 75 | 75 |
| 2 | 6989.8 | 7084.0 | +94.2 | +1.4% | 5 | N | unchanged | 47 | 47 |
| 3 | 746.7 | 757.1 | +10.4 | +1.4% | 0 | N | unchanged | 124 | 124 |
| 4 | 404.5 | 462.3 | +57.8 | +14.3% | 1 | Y | regressed | 2226 | 351 |
| 5 | 413.5 | 495.9 | +82.4 | +19.9% | 2 | Y | regressed | 3990 | 2088 |
| 6 | 386.0 | 511.7 | +125.7 | +32.6% | 5 | Y | regressed | 3531 | 269 |

## Classification Counts

- **improved**: 0
- **neutral**: 0
- **regressed**: 3
- **unchanged**: 9

## Session Wall-Clock Totals

- Baseline total: 83568ms
- Hinted total: 82198ms
- Delta: -1370ms (-1.6%)

## Mechanism Health

- Total hints injected: 30
- Hints per query: min=0, max=5, mean=2.5

## Q-Error Reductions

- Median q-error reduction: 1.3
- Queries with >50% relative q-error reduction: 2/12

## Correlation: Baseline Q-Error vs Latency Delta

| Q | Baseline Max QE | Delta % | Class |
|---|----------------|---------|-------|
| 5 | 3990 | +19.9% | regressed |
| 6 | 3531 | +32.6% | regressed |
| 4 | 2226 | +14.3% | regressed |
| 3 | 124 | +1.4% | unchanged |
| 7 | 75 | +1.1% | unchanged |
| 2 | 47 | +1.4% | unchanged |
| 1 | 45 | -0.1% | unchanged |
| 0 | 23 | +0.3% | unchanged |
| 9 | 12 | -1.6% | unchanged |
| 10 | 9 | -3.1% | unchanged |
| 11 | 9 | -3.6% | unchanged |
| 12 | 9 | -3.7% | unchanged |

## Case Studies

### Best improvement (or smallest regression): Q4

**Baseline median**: 404.5ms | **Hinted median**: 462.3ms | **Delta**: +57.8ms (+14.3%)
**Hints injected**: 1 | **Plan changed**: True | **Baseline QE**: 2226 | **Hinted QE**: 351

Hint block: `/*+ Rows("*VALUES*" nd #27) */`

Baseline joins:
```
  Nested Loop     est=     1 act=   123 qe=  123.0x  ['*VALUES*', 'ci_act', 'ci_dir', 'cn', 'mc', 'na', 'nd', 'role_type', 't']
  Nested Loop     est=     1 act=   123 qe=  123.0x  ['*VALUES*', 'ci_act', 'ci_dir', 'mc', 'na', 'nd', 'role_type', 't']
  Nested Loop     est=     1 act=    45 qe=   45.0x  ['*VALUES*', 'ci_act', 'ci_dir', 'na', 'nd', 'role_type', 't']
  Hash Join       est=     2 act=  4452 qe= 2226.0x  ['*VALUES*', 'ci_act', 'ci_dir', 'nd', 'role_type', 't']
  Nested Loop     est=    12 act=  9384 qe=  782.0x  ['*VALUES*', 'ci_act', 'ci_dir', 'nd', 't']
  Nested Loop     est=     1 act=    72 qe=   72.0x  ['*VALUES*', 'ci_dir', 'nd', 't']
  Nested Loop     est=     4 act=   177 qe=   44.2x  ['*VALUES*', 'ci_dir', 'nd']
```

Hinted joins:
```
  Nested Loop     est=     1 act=   123 qe=  123.0x  ['*VALUES*', 'ci_act', 'ci_dir', 'cn', 'mc', 'na', 'nd', 'role_type', 't']
  Nested Loop     est=     1 act=   123 qe=  123.0x  ['*VALUES*', 'ci_act', 'ci_dir', 'mc', 'na', 'nd', 'role_type', 't']
  Nested Loop     est=     1 act=    45 qe=   45.0x  ['*VALUES*', 'ci_act', 'ci_dir', 'na', 'nd', 'role_type', 't']
  Nested Loop     est=     2 act=   456 qe=  228.0x  ['*VALUES*', 'ci_act', 'ci_dir', 'na', 'role_type', 't']
  Nested Loop     est=     1 act=   351 qe=  351.0x  ['*VALUES*', 'ci_act', 'na', 'role_type', 't']
  Hash Join       est=     7 act=  1476 qe=  210.9x  ['*VALUES*', 'ci_act', 'na', 'role_type']
  Nested Loop     est=    40 act=  1512 qe=   37.8x  ['*VALUES*', 'ci_act', 'na']
```

### Worst regression: Q6

**Baseline median**: 386.0ms | **Hinted median**: 511.7ms | **Delta**: +125.7ms (+32.6%)
**Hints injected**: 5 | **Plan changed**: True | **Baseline QE**: 3531 | **Hinted QE**: 269

Hint block: `/*+ Rows("*VALUES*" ci_act ci_dir k mk na nd role_type t #4176) Rows("*VALUES*" ci_act ci_dir mk na nd role_type t #4176) Rows("*VALUES*" ci_act ci_dir nd role_type t #360) Rows("*VALUES*" ci_dir nd #...`

Baseline joins:
```
  Nested Loop     est=     2 act=  3210 qe= 1605.0x  ['*VALUES*', 'ci_act', 'ci_dir', 'k', 'mk', 'na', 'nd', 'role_type', 't']
  Nested Loop     est=     2 act=  3210 qe= 1605.0x  ['*VALUES*', 'ci_act', 'ci_dir', 'mk', 'na', 'nd', 'role_type', 't']
  Nested Loop     est=     1 act=    27 qe=   27.0x  ['*VALUES*', 'ci_act', 'ci_dir', 'na', 'nd', 'role_type', 't']
  Nested Loop     est=     1 act=  3531 qe= 3531.0x  ['*VALUES*', 'ci_act', 'ci_dir', 'nd', 'role_type', 't']
  Nested Loop     est=     7 act=  7074 qe= 1010.6x  ['*VALUES*', 'ci_act', 'ci_dir', 'nd', 't']
  Nested Loop     est=     1 act=    48 qe=   48.0x  ['*VALUES*', 'ci_dir', 'nd', 't']
  Nested Loop     est=     2 act=   123 qe=   61.5x  ['*VALUES*', 'ci_dir', 'nd']
```

Hinted joins:
```
  Nested Loop     est=  1740 act=  3210 qe=    1.8x  ['*VALUES*', 'ci_act', 'ci_dir', 'k', 'mk', 'na', 'nd', 'role_type', 't']
  Nested Loop     est=  1740 act=  3210 qe=    1.8x  ['*VALUES*', 'ci_act', 'ci_dir', 'mk', 'na', 'nd', 'role_type', 't']
  Nested Loop     est=     1 act=    27 qe=   27.0x  ['*VALUES*', 'ci_act', 'ci_dir', 'na', 'nd', 'role_type', 't']
  Nested Loop     est=     1 act=   264 qe=  264.0x  ['*VALUES*', 'ci_act', 'ci_dir', 'na', 'role_type', 't']
  Nested Loop     est=     1 act=   240 qe=  240.0x  ['*VALUES*', 'ci_act', 'na', 'role_type', 't']
  Hash Join       est=     4 act=  1077 qe=  269.2x  ['*VALUES*', 'ci_act', 'na', 'role_type']
  Nested Loop     est=    22 act=  1113 qe=   50.6x  ['*VALUES*', 'ci_act', 'na']
```

### Hints injected, plan unchanged: Q2

**Baseline median**: 6989.8ms | **Hinted median**: 7084.0ms | **Delta**: +94.2ms (+1.4%)
**Hints injected**: 5 | **Plan changed**: False | **Baseline QE**: 47 | **Hinted QE**: 47

Hint block: `/*+ Rows(ci_act ci_dir info_type kind_type mii na nd role_type role_type_1 t #1598) Rows(ci_act ci_dir info_type kind_type mii nd role_type role_type_1 t #1598) Rows(ci_act mii role_type t #239181) Ro...`

Baseline joins:
```
  Hash Join       est= 71253 act=  1524 qe=   46.8x  ['ci_act', 'ci_dir', 'info_type', 'kind_type', 'mii', 'na', 'nd', 'role_type', 'role_type_1', 't']
  Merge Join      est= 71253 act=  1524 qe=   46.8x  ['ci_act', 'ci_dir', 'info_type', 'kind_type', 'mii', 'nd', 'role_type', 'role_type_1', 't']
  Hash Join       est= 89067 act=257547 qe=    2.9x  ['ci_act', 'ci_dir', 'mii', 'role_type', 't']
  Hash Join       est= 26700 act=239181 qe=    9.0x  ['ci_act', 'mii', 'role_type', 't']
  Nested Loop     est=160200 act=543201 qe=    3.4x  ['ci_act', 'mii', 't']
```

Hinted joins:
```
  Hash Join       est= 71253 act=  1524 qe=   46.8x  ['ci_act', 'ci_dir', 'info_type', 'kind_type', 'mii', 'na', 'nd', 'role_type', 'role_type_1', 't']
  Merge Join      est= 71253 act=  1524 qe=   46.8x  ['ci_act', 'ci_dir', 'info_type', 'kind_type', 'mii', 'nd', 'role_type', 'role_type_1', 't']
  Hash Join       est= 89067 act=257547 qe=    2.9x  ['ci_act', 'ci_dir', 'mii', 'role_type', 't']
  Hash Join       est= 99659 act=239181 qe=    2.4x  ['ci_act', 'mii', 'role_type', 't']
  Nested Loop     est=226334 act=543201 qe=    2.4x  ['ci_act', 'mii', 't']
```
