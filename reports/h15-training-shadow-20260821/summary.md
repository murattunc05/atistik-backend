# H15 training-degree +2 prospective shadow

## Frozen contract

- Candidate: `h15-training-degree-plus2-20260821-v1`
- Observation start: `21.08.2026`
- Prospective scope: exact `HANDIKAP15` with visible baseline `v4.25`
- Formula: take the race's persisted/exported `v4Weights` percentage map, add
  `2.0` raw points to `training_degree_score`, then normalize. If the baseline
  metric is zero, its candidate share is exactly `2 / 102` when the exported
  baseline totals `100`.
- Source contract: exported `metricSourceFlags.hasTrainingTimes` and the live
  `_mf._has_training_times` guard must both be explicit booleans and agree.
  Unavailable or neutral-only races are excluded from performance evidence.
- Visible ranking, `aiScore`, Telegram, and all other profiles stay unchanged.
- Replay-baseline score and rank are frozen separately from the visible v4
  score/rank. The prospective monitor follows
  `metric_signal_replay.py`'s `replayTop3SetAgreement` rule: a race whose
  visible Top3 horse set differs from its replay-baseline Top3 set is reported
  as an issue and excluded from performance/checkpoint evidence. Official
  non-runners are removed and visible ranks are collapsed first through the
  same `competitive_race_rows` path used by the replay.

## Historical formula lock

The immutable 19 August replay fixture contains all 34 integrity-clean H15
races used by `PROFILE:HANDIKAP15/training_degree_score/plus2pp`. The exact
order fingerprints reproduce:

| Metric | Visible baseline | +2 candidate |
|---|---:|---:|
| Winner Top1 | 4/34 | 4/34 |
| Winner Top3 | 13/34 | 16/34 |
| Winner Top5 | 26/34 | 27/34 |
| Top3 rescues / damages | - | 3 / 0 |

The fixture spans v4.21 (5), v4.23 (11), v4.24 (6), and v4.25 (12) only to
lock the historical formula. New prospective collection is deliberately
limited to v4.25.

The formal positive fixture uses the real labeled replay rescue `226316`:
FORTUNELLO moves from replay-baseline rank 4 to candidate rank 3. Its official
finish order was reconciled from the Pi prediction log.

The Aug-19 replay evidence freezes build-only baseline and candidate
temperatures at `14.0` (`20/7/7` build/inner/outer split; source report SHA-256
`f438858fe5d17979bda499bfb48c9b047a1366ddfe8e7510b3f83b7e2fe14e2b`).
The deployable frozen evidence is
`automation/evidence/h15_training_degree_plus2_calibration_20260819.json`
(SHA-256
`3e606f6c40f32f22858a24c10caa138f880e78b20494f237f7972d2d727f319e`).
The monitor must read this artifact and validate its hash, schema, candidate
identity, replay/source identity, 20/7/7 split, and build-only 14/14
temperatures. Missing, tampered, or mismatched evidence remains fail-closed.
The monitor calculates runner-level Top3 Plackett-Luce Brier/ECE with those
temperatures. Calibration passes only when Brier delta is at most `0.005`,
candidate ECE is at most `0.10`, and candidate ECE is at most `0.05` or its
delta is at most `0.01`. Missing exact temperature evidence produces
`HOLD_CALIBRATION_EVIDENCE`; no temperature is fitted prospectively.

## Prospective gate

The separate nightly monitor validates race identity, pre-race causality,
formula and normalized weights, visible/replay-baseline/candidate rank order,
source-guard agreement, feature and race snapshot hashes, and complete official
labels. Checkpoints are +5, +10, and +15 clean actionable and replay-compatible
races. Any Top3 damage fails the checkpoint; source, separation,
MAE/rho/NDCG, objective, and calibration guardrails must also pass.

Even a clean +15 result can only produce `SUPPORTED_FOR_FORMAL_REPLAY`.
`liveRolloutEligible` and `telegramVisible` remain hard-coded `false`.

The older HANDIKAP trainer ablation remains queryable as retired evidence and
stops collecting new races.
