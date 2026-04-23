# Scenario 8: Actor Career Archetypes — Report

## Header

- **Scenario**: Actor Career Archetypes
- **Model**: Opus (via claude -p --model opus)
- **Max queries**: 25
- **Reps completed**: 2 / 2
- **Total wall-clock**: 708s
- **Total DB compute**: 199.0s

## Per-Rep Summary

| Rep | Queries | Tables/q (mean/p50/p95) | Plan depth (mean/max) | Wall-clock | DB compute | Final answer | DONE |
|---|---|---|---|---|---|---|---|
| a | 9 | 6.5/7/12 | 17.2/34 | 360s | 125.5s | yes | yes |
| b | 11 | 2.0/2/6 | 5.6/24 | 348s | 73.5s | yes | yes |

## M1 Metrics (Workspace)

- **m1_save_calls**: 1
- **m1_save_cte_calls**: 0
- **m1_state_acknowledged**: 7 (turns where agent acknowledged saved content)
- **m1_state_ack_rate**: 37%
- **m1_reuse_count**: 7 (queries referencing saved temp tables)
- **m1_reuse_rate**: 7.0 (reuse_count / save_calls)
- **m1_save_but_never_reused**: 0
- **m1_qualified_but_not_saved**: 10
- **protocol_turns**: 19 / 20 (turns with any protocol marker)
- **save_decisions_emitted**: 1 SAVE/SAVE_CTE, 14 SKIP

## M2 Metrics (Intent Declaration)

- **m2_declare_calls**: 0
- **m2_declared_variants_total**: 0
- **m2_abandonment_with_justification**: 0

## Signal Assessment

- YELLOW: save_calls 0.5/session (outside 1-3 range)
- YELLOW: state acknowledgment rate 37% < 60%
- YELLOW: no SAVE_CTE decisions
- YELLOW: no MATERIALIZE_INTENT declarations
