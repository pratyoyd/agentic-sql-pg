# Scenario 9: Series-to-Film Spillover — Report

## Header

- **Scenario**: Series-to-Film Spillover
- **Model**: Opus (via claude -p --model opus)
- **Max queries**: 25
- **Reps completed**: 2 / 2
- **Total wall-clock**: 677s
- **Total DB compute**: 297.7s

## Per-Rep Summary

| Rep | Queries | Tables/q (mean/p50/p95) | Plan depth (mean/max) | Wall-clock | DB compute | Final answer | DONE |
|---|---|---|---|---|---|---|---|
| a | 6 | 4.6/4/8 | 14.0/22 | 414s | 191.4s | yes | yes |
| b | 7 | 4.9/5/9 | 22.0/56 | 263s | 106.3s | yes | yes |

## M1 Metrics (Workspace)

- **m1_save_calls**: 3
- **m1_save_cte_calls**: 2
- **m1_state_acknowledged**: 6 (turns where agent acknowledged saved content)
- **m1_state_ack_rate**: 60%
- **m1_reuse_count**: 5 (queries referencing saved temp tables)
- **m1_reuse_rate**: 1.67 (reuse_count / save_calls)
- **m1_save_but_never_reused**: 0
- **m1_qualified_but_not_saved**: 8
- **protocol_turns**: 13 / 13 (turns with any protocol marker)
- **save_decisions_emitted**: 2 SAVE/SAVE_CTE, 6 SKIP

## M2 Metrics (Intent Declaration)

- **m2_declare_calls**: 0
- **m2_declared_variants_total**: 0
- **m2_abandonment_with_justification**: 0

## Signal Assessment

- GREEN: save_calls 1-3/session
- GREEN: state acknowledgment rate 60% ≥ 60%
- GREEN: 2 SAVE_CTE decisions emitted
- YELLOW: no MATERIALIZE_INTENT declarations
