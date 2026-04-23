# Scenario 6: Franchise Durability — Report

## Header

- **Scenario**: Franchise Durability
- **Model**: Opus (via claude -p --model opus)
- **Max queries**: 25
- **Reps completed**: 2 / 2
- **Total wall-clock**: 805s
- **Total DB compute**: 11.0s

## Per-Rep Summary

| Rep | Queries | Tables/q (mean/p50/p95) | Plan depth (mean/max) | Wall-clock | DB compute | Final answer | DONE |
|---|---|---|---|---|---|---|---|
| a | 10 | 5.8/6/11 | 13.8/27 | 420s | 3.8s | yes | yes |
| b | 9 | 5.4/4/12 | 15.8/33 | 385s | 7.2s | yes | yes |

## M1 Metrics (Workspace)

- **m1_save_calls**: 1
- **m1_save_cte_calls**: 1
- **m1_state_acknowledged**: 4 (turns where agent acknowledged saved content)
- **m1_state_ack_rate**: 22%
- **m1_reuse_count**: 4 (queries referencing saved temp tables)
- **m1_reuse_rate**: 4.0 (reuse_count / save_calls)
- **m1_save_but_never_reused**: 0
- **m1_qualified_but_not_saved**: 12
- **protocol_turns**: 19 / 19 (turns with any protocol marker)
- **save_decisions_emitted**: 1 SAVE/SAVE_CTE, 8 SKIP

## M2 Metrics (Intent Declaration)

- **m2_declare_calls**: 1
- **m2_declared_variants_total**: 3
- **m2_abandonment_with_justification**: 0

## Signal Assessment

- YELLOW: save_calls 0.5/session (outside 1-3 range)
- YELLOW: state acknowledgment rate 22% < 60%
- GREEN: 1 SAVE_CTE decisions emitted
- GREEN: 1 intent declarations emitted
