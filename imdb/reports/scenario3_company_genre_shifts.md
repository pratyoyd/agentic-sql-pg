# Scenario 3: Production Company Genre Shifts — Report

## Header

- **Scenario**: Production Company Genre Shifts
- **Model**: Opus (via claude -p --model opus)
- **Max queries**: 25
- **Reps completed**: 2 / 2
- **Total wall-clock**: 524s
- **Total DB compute**: 34.3s

## Per-Rep Summary

| Rep | Queries | Tables/q (mean/p50/p95) | Plan depth (mean/max) | Wall-clock | DB compute | Final answer | DONE |
|---|---|---|---|---|---|---|---|
| a | 7 | 3.9/4/7 | 14.7/31 | 278s | 21.3s | yes | yes |
| b | 3 | 8.3/11/13 | 24.0/46 | 246s | 13.0s | yes | yes |

## M1 Metrics (Workspace)

- **m1_save_calls**: 1
- **m1_save_cte_calls**: 1
- **m1_state_acknowledged**: 3 (turns where agent acknowledged saved content)
- **m1_state_ack_rate**: 33%
- **m1_reuse_count**: 3 (queries referencing saved temp tables)
- **m1_reuse_rate**: 3.0 (reuse_count / save_calls)
- **m1_save_but_never_reused**: 0
- **m1_qualified_but_not_saved**: 6
- **protocol_turns**: 10 / 10 (turns with any protocol marker)
- **save_decisions_emitted**: 1 SAVE/SAVE_CTE, 5 SKIP

## M2 Metrics (Intent Declaration)

- **m2_declare_calls**: 0
- **m2_declared_variants_total**: 0
- **m2_abandonment_with_justification**: 0

## Signal Assessment

- YELLOW: save_calls 0.5/session (outside 1-3 range)
- YELLOW: state acknowledgment rate 33% < 60%
- GREEN: 1 SAVE_CTE decisions emitted
- YELLOW: no MATERIALIZE_INTENT declarations
