# Aggregate Report: Scenarios 2–10

- **Total scenarios**: 9
- **Reps per scenario**: 2
- **Model**: Opus

## Per-Scenario Summary

| # | Scenario | Reps done | Queries (total) | Wall-clock | DB compute | Save calls | SAVE_CTE | Reuse count | Protocol % | M2 intents | DONE rate |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 2 | Director Career Trajectory | 2/2 | 24 | 1358s | 713.1s | 2 | 1 | 19 | 100% | 1 | 2/2 |
| 3 | Production Company Genre Shifts | 2/2 | 10 | 524s | 34.3s | 1 | 1 | 3 | 100% | 0 | 2/2 |
| 4 | Cast Size x Rating x Era | 2/2 | 11 | 514s | 107.4s | 1 | 1 | 3 | 100% | 1 | 2/2 |
| 5 | International Co-Production Trends | 2/2 | 13 | 463s | 39.3s | 2 | 1 | 9 | 100% | 2 | 2/2 |
| 6 | Franchise Durability | 2/2 | 29 | 2317s | 1224.2s | 0 | 0 | 0 | 100% | 1 | 2/2 |
| 7 | Writer-Director Separation Impact | 2/2 | 10 | 358s | 25.4s | 2 | 2 | 4 | 100% | 2 | 2/2 |
| 8 | Actor Career Archetypes | 2/2 | 20 | 708s | 199.0s | 1 | 0 | 7 | 95% | 0 | 2/2 |
| 9 | Series-to-Film Spillover | 2/2 | 13 | 677s | 297.7s | 3 | 2 | 5 | 100% | 0 | 2/2 |
| 10 | Budget Era Correlation | 2/2 | 13 | 566s | 16.8s | 2 | 2 | 5 | 100% | 1 | 2/2 |

## Cross-Scenario Adoption Rates

- **M1 save adoption**: 13/18 reps had at least one save (72%)
- **SAVE_CTE trigger rate**: 10/18 reps used SAVE_CTE (56%)
- **M2 trigger rate**: 8/18 reps had MATERIALIZE_INTENT (44%)

## Compute Reduction Estimate

| Scenario | Total DB (s) | Reuse queries | Avg reuse query (ms) | Estimated base cost (ms) | Est. savings |
|---|---|---|---|---|---|
| 2 | 713.1 | 19 | 106826 | 9121 | ~0.0s |
| 3 | 34.3 | 3 | 15 | 4401 | ~13.2s |
| 4 | 107.4 | 3 | 3203 | 6078 | ~8.6s |
| 5 | 39.3 | 9 | 740 | 5003 | ~38.4s |
| 6 | 1224.2 | 0 | — | — | — |
| 7 | 25.4 | 4 | 3545 | 2243 | ~0.0s |
| 8 | 199.0 | 7 | 0 | 21352 | ~149.5s |
| 9 | 297.7 | 5 | 19668 | 26554 | ~34.4s |
| 10 | 16.8 | 5 | 66 | 2238 | ~10.9s |

## M1 Pattern Quality Ranking

- **STRONG**: Scenario 2 (Director Career Trajectory) — score 78/100 (saves=2, reuse=19, protocol=24/24)
- **STRONG**: Scenario 8 (Actor Career Archetypes) — score 64/100 (saves=1, reuse=7, protocol=19/20)
- **STRONG**: Scenario 5 (International Co-Production Trends) — score 64/100 (saves=2, reuse=9, protocol=13/13)
- **STRONG**: Scenario 9 (Series-to-Film Spillover) — score 60/100 (saves=3, reuse=5, protocol=13/13)
- **MODERATE**: Scenario 10 (Budget Era Correlation) — score 58/100 (saves=2, reuse=5, protocol=13/13)
- **MODERATE**: Scenario 7 (Writer-Director Separation Impact) — score 56/100 (saves=2, reuse=4, protocol=10/10)
- **MODERATE**: Scenario 3 (Production Company Genre Shifts) — score 54/100 (saves=1, reuse=3, protocol=10/10)
- **MODERATE**: Scenario 4 (Cast Size x Rating x Era) — score 54/100 (saves=1, reuse=3, protocol=11/11)
- **MODERATE**: Scenario 6 (Franchise Durability) — score 40/100 (saves=0, reuse=0, protocol=29/29)
