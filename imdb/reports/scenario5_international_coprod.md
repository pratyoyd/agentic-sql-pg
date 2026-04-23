# Scenario 5: International Co-Production Trends — Report

## Header

- **Scenario**: International Co-Production Trends
- **Model**: Opus (via claude -p --model opus)
- **Max queries**: 25
- **Reps completed**: 2 / 2
- **Total wall-clock**: 463s
- **Total DB compute**: 39.3s

## Per-Rep Summary

| Rep | Queries | Tables/q (mean/p50/p95) | Plan depth (mean/max) | Wall-clock | DB compute | Final answer | DONE |
|---|---|---|---|---|---|---|---|
| a | 8 | 2.0/1/9 | 7.5/28 | 157s | 19.8s | yes | yes |
| b | 5 | 2.6/1/8 | 11.8/29 | 306s | 19.4s | yes | yes |

## M1 Metrics (Workspace)

- **m1_save_calls**: 2
- **m1_save_cte_calls**: 1
- **m1_state_acknowledged**: 9 (turns where agent acknowledged saved content)
- **m1_state_ack_rate**: 82%
- **m1_reuse_count**: 9 (queries referencing saved temp tables)
- **m1_reuse_rate**: 4.5 (reuse_count / save_calls)
- **m1_save_but_never_reused**: 0
- **m1_qualified_but_not_saved**: 2
- **protocol_turns**: 13 / 13 (turns with any protocol marker)
- **save_decisions_emitted**: 1 SAVE/SAVE_CTE, 6 SKIP

## M2 Metrics (Intent Declaration)

- **m2_declare_calls**: 2
- **m2_declared_variants_total**: 6
- **m2_abandonment_with_justification**: 0

## Signal Assessment

- GREEN: save_calls 1-3/session
- GREEN: state acknowledgment rate 82% ≥ 60%
- GREEN: 1 SAVE_CTE decisions emitted
- GREEN: 2 intent declarations emitted
