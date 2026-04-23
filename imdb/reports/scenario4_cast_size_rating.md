# Scenario 4: Cast Size x Rating x Era — Report

## Header

- **Scenario**: Cast Size x Rating x Era
- **Model**: Opus (via claude -p --model opus)
- **Max queries**: 25
- **Reps completed**: 2 / 2
- **Total wall-clock**: 514s
- **Total DB compute**: 107.4s

## Per-Rep Summary

| Rep | Queries | Tables/q (mean/p50/p95) | Plan depth (mean/max) | Wall-clock | DB compute | Final answer | DONE |
|---|---|---|---|---|---|---|---|
| a | 5 | 3.2/4/7 | 17.5/24 | 222s | 27.5s | yes | yes |
| b | 6 | 9.8/11/11 | 36.0/46 | 292s | 80.0s | yes | yes |

## M1 Metrics (Workspace)

- **m1_save_calls**: 1
- **m1_save_cte_calls**: 1
- **m1_state_acknowledged**: 3 (turns where agent acknowledged saved content)
- **m1_state_ack_rate**: 30%
- **m1_reuse_count**: 3 (queries referencing saved temp tables)
- **m1_reuse_rate**: 3.0 (reuse_count / save_calls)
- **m1_save_but_never_reused**: 0
- **m1_qualified_but_not_saved**: 7
- **protocol_turns**: 11 / 11 (turns with any protocol marker)
- **save_decisions_emitted**: 1 SAVE/SAVE_CTE, 5 SKIP

## M2 Metrics (Intent Declaration)

- **m2_declare_calls**: 1
- **m2_declared_variants_total**: 3
- **m2_abandonment_with_justification**: 0

## Signal Assessment

- YELLOW: save_calls 0.5/session (outside 1-3 range)
- YELLOW: state acknowledgment rate 30% < 60%
- GREEN: 1 SAVE_CTE decisions emitted
- GREEN: 1 intent declarations emitted
