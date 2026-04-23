# Scenario 1: Genre Evolution Analysis — Dry Run Report

## Header

- **Scenario**: Genre evolution, 1990–2020
- **Goal**: Identify 3 genres with largest rating shift between 1990s and 2010s
- **Model**: Opus (via claude -p --model opus)
- **Temperature**: default (claude CLI)
- **Reps completed**: 3 / 3
- **Total wall-clock**: 705s
- **Total DB compute**: 32.7s

## Per-Rep Summary

| Rep | Queries | Tables/q (mean/p50/p95) | Plan depth (mean/max) | Wall-clock | DB compute | Final answer | DONE |
|---|---|---|---|---|---|---|---|
| a | 15 | 1.3/1/3 | 4.5/11 | 238s | 11.2s | yes | yes |
| b | 11 | 1.5/1/5 | 5.3/19 | 220s | 4.5s | yes | yes |
| c | 9 | 4.1/4/5 | 14.3/20 | 247s | 16.9s | yes | yes |

## M1 Metrics (Workspace)

- **m1_save_calls**: 2
- **m1_state_acknowledged**: 21 (turns where agent acknowledged saved content)
- **m1_state_ack_rate**: 64%
- **m1_reuse_count**: 20 (queries referencing saved temp tables)
- **m1_reuse_rate**: 10.0 (reuse_count / save_calls)
- **m1_save_but_never_reused**: 0
- **m1_qualified_but_not_saved**: 12
- **protocol_turns**: 35 / 35 (turns with any protocol marker)
- **save_decisions_emitted**: 2 SAVE, 26 SKIP

## M2 Metrics (Intent Declaration)

- **m2_declare_calls**: 1
- **m2_declared_variants_total**: 2
- **m2_actual_variants_issued**: 0 (requires manual count)
- **m2_declaration_precision**: 0.0
- **m2_abandonment_with_justification**: 0

## Ground Truth Reference

Vote-weighted average rating shift by genre (films with ≥100 votes, ≥50 films per decade):

| Genre | Avg 90s | n 90s | Avg 10s | n 10s | Shift |
|---|---|---|---|---|---|
| War | 7.94 | 213 | 6.62 | 139 | -1.326 |
| Sport | 6.33 | 142 | 7.37 | 115 | +1.034 |
| Music | 6.70 | 232 | 5.92 | 163 | -0.788 |
| Documentary | 7.38 | 358 | 6.87 | 609 | -0.507 |
| Family | 6.56 | 554 | 7.04 | 278 | +0.481 |
| Crime | 7.43 | 1126 | 6.99 | 562 | -0.444 |
| Drama | 7.36 | 4304 | 6.93 | 2954 | -0.430 |
| History | 7.78 | 198 | 7.36 | 206 | -0.422 |
| Horror | 6.22 | 678 | 5.82 | 723 | -0.393 |
| Romance | 6.88 | 1555 | 6.55 | 732 | -0.338 |

## Signal Assessment

- YELLOW: save_calls 0.7/session (outside 1-3 range)
- GREEN: state acknowledgment rate 64% ≥ 60%
- GREEN: 1 intent declarations emitted

## Verdict

*(To be written after reviewing all metrics and final answers.)*
