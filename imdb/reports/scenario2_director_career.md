# Scenario 2: Director Career Trajectory — Report

## Header

- **Scenario**: Director Career Trajectory
- **Model**: Opus (via claude -p --model opus)
- **Max queries**: 25
- **Reps completed**: 2 / 2
- **Total wall-clock**: 1358s
- **Total DB compute**: 713.1s

## Per-Rep Summary

| Rep | Queries | Tables/q (mean/p50/p95) | Plan depth (mean/max) | Wall-clock | DB compute | Final answer | DONE |
|---|---|---|---|---|---|---|---|
| a | 8 | 5.0/6/8 | 17.3/25 | 857s | 679.7s | yes | yes |
| b | 16 | 6.3/8/8 | 19.2/29 | 502s | 33.4s | yes | yes |

## M1 Metrics (Workspace)

- **m1_save_calls**: 2
- **m1_save_cte_calls**: 1
- **m1_state_acknowledged**: 19 (turns where agent acknowledged saved content)
- **m1_state_ack_rate**: 86%
- **m1_reuse_count**: 19 (queries referencing saved temp tables)
- **m1_reuse_rate**: 9.5 (reuse_count / save_calls)
- **m1_save_but_never_reused**: 0
- **m1_qualified_but_not_saved**: 18
- **protocol_turns**: 24 / 24 (turns with any protocol marker)
- **save_decisions_emitted**: 2 SAVE/SAVE_CTE, 16 SKIP

## M2 Metrics (Intent Declaration)

- **m2_declare_calls**: 1
- **m2_declared_variants_total**: 3
- **m2_abandonment_with_justification**: 0

## Signal Assessment

- GREEN: save_calls 1-3/session
- GREEN: state acknowledgment rate 86% ≥ 60%
- GREEN: 1 SAVE_CTE decisions emitted
- GREEN: 1 intent declarations emitted
