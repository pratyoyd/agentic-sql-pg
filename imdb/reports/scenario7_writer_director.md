# Scenario 7: Writer-Director Separation Impact — Report

## Header

- **Scenario**: Writer-Director Separation Impact
- **Model**: Opus (via claude -p --model opus)
- **Max queries**: 25
- **Reps completed**: 2 / 2
- **Total wall-clock**: 358s
- **Total DB compute**: 25.4s

## Per-Rep Summary

| Rep | Queries | Tables/q (mean/p50/p95) | Plan depth (mean/max) | Wall-clock | DB compute | Final answer | DONE |
|---|---|---|---|---|---|---|---|
| a | 5 | 1.6/1/4 | 9.8/19 | 208s | 14.8s | yes | yes |
| b | 5 | 1.8/1/5 | 9.5/18 | 150s | 10.6s | yes | yes |

## M1 Metrics (Workspace)

- **m1_save_calls**: 2
- **m1_save_cte_calls**: 2
- **m1_state_acknowledged**: 4 (turns where agent acknowledged saved content)
- **m1_state_ack_rate**: 50%
- **m1_reuse_count**: 4 (queries referencing saved temp tables)
- **m1_reuse_rate**: 2.0 (reuse_count / save_calls)
- **m1_save_but_never_reused**: 0
- **m1_qualified_but_not_saved**: 4
- **protocol_turns**: 10 / 10 (turns with any protocol marker)
- **save_decisions_emitted**: 2 SAVE/SAVE_CTE, 3 SKIP

## M2 Metrics (Intent Declaration)

- **m2_declare_calls**: 2
- **m2_declared_variants_total**: 4
- **m2_abandonment_with_justification**: 0

## Signal Assessment

- GREEN: save_calls 1-3/session
- YELLOW: state acknowledgment rate 50% < 60%
- GREEN: 2 SAVE_CTE decisions emitted
- GREEN: 2 intent declarations emitted
