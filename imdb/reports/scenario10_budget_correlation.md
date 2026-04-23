# Scenario 10: Budget Era Correlation — Report

## Header

- **Scenario**: Budget Era Correlation
- **Model**: Opus (via claude -p --model opus)
- **Max queries**: 25
- **Reps completed**: 2 / 2
- **Total wall-clock**: 566s
- **Total DB compute**: 16.8s

## Per-Rep Summary

| Rep | Queries | Tables/q (mean/p50/p95) | Plan depth (mean/max) | Wall-clock | DB compute | Final answer | DONE |
|---|---|---|---|---|---|---|---|
| a | 7 | 1.4/1/5 | 5.0/13 | 344s | 7.1s | yes | yes |
| b | 6 | 1.8/1/5 | 7.5/14 | 222s | 9.7s | yes | yes |

## M1 Metrics (Workspace)

- **m1_save_calls**: 2
- **m1_save_cte_calls**: 2
- **m1_state_acknowledged**: 5 (turns where agent acknowledged saved content)
- **m1_state_ack_rate**: 45%
- **m1_reuse_count**: 5 (queries referencing saved temp tables)
- **m1_reuse_rate**: 2.5 (reuse_count / save_calls)
- **m1_save_but_never_reused**: 0
- **m1_qualified_but_not_saved**: 2
- **protocol_turns**: 13 / 13 (turns with any protocol marker)
- **save_decisions_emitted**: 2 SAVE/SAVE_CTE, 2 SKIP

## M2 Metrics (Intent Declaration)

- **m2_declare_calls**: 1
- **m2_declared_variants_total**: 2
- **m2_abandonment_with_justification**: 0

## Signal Assessment

- GREEN: save_calls 1-3/session
- YELLOW: state acknowledgment rate 45% < 60%
- GREEN: 2 SAVE_CTE decisions emitted
- GREEN: 1 intent declarations emitted
