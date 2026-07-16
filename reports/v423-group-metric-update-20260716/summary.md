# v4.23 Group Metric Update - 2026-07-16

## Outcome

- Visible update: `MAIDEN` only.
- Shadow/re-evaluate: `GRUP`, `HANDIKAP`, `KV`, `HANDIKAP16|Cim`, `SART3`, and `SART4`.
- Hold current weights: every other active group/profile.
- The existing `4.22-handicap-candidate` remains shadow-only; it is not promoted.
- Visible ranking version becomes `4.23` so new predictions identify the applied MAIDEN change.

`SHADOW` in this report means “retain the candidate for a later chronological replay”; it does not change visible ranking. Raw metric logging continues to accumulate the evidence needed for the next gate.

## Corpus and label policy

- Input: `7,691` prediction rows / `742` race ids.
- Fully labeled and used: `416` races / `4,091` rows.
- Partially labeled and excluded as whole races: `273` races / `3,040` rows.
- Unlabeled and excluded: `53` races / `560` rows.
- Label integrity: `416/416` selected races clean.
- Accepted official edge cases: `3` tied-finish races and `2` terminal/DNF rows with `finish_pos=99`.
- Split: chronological `60% build / 20% inner validation / 20% untouched outer holdout`.
- Objective: `45% NDCG@5 + 35% normalized Spearman + 20% MAE component`.
- AGF candidate policy: allowed only for `MAIDEN` and `SART1`.

The outer holdout was not used to select a candidate.

## Apply gate

An active group/profile is eligible for visible application only when all conditions pass:

- Group sample `n >= 50`; profile sample `n >= 30`; outer holdout `n >= 6`.
- Inner and outer objective delta both `> 0.005`.
- Inner and outer winner Top3 do not regress.
- Outer Top1 loss is at most one race.
- Full-corpus objective delta `> 0.003`.
- Maximum single weight movement `<= 10` percentage points.
- Total weight variation `<= 20` percentage points.
- Any increased source-gated metric has at least `40%` race coverage and `15%` non-neutral rate.

## Main group decisions

| Group | n; build/inner/outer | Baseline Top1/WTop3/WTop5 | Rho / MAE / NDCG@5 | Decision | Evidence |
|---|---:|---:|---:|---|---|
| Overall | 416; 249/83/84 | 109/263/336 | .405 / 2.37 / .808 | SHADOW | weight impact candidate improved outer objective but regressed inner and full WTop3 |
| Global fallback | 1; 1/0/0 | 0/0/1 | -.893 / 3.43 / .507 | HOLD | no usable holdout |
| GRUP | 26; 15/5/6 | 8/18/24 | .376 / 2.94 / .800 | SHADOW | pedigree candidate outer objective `+.0023`; sample and delta below apply gate |
| HANDIKAP | 120; 72/24/24 | 30/71/94 | .346 / 2.68 / .786 | SHADOW | conservative multi candidate outer `+.0051`, but full WTop3 `-4` |
| KV | 52; 30/11/11 | 17/40/46 | .436 / 1.89 / .832 | SHADOW | track-suit candidate outer `+.0041`, below apply threshold |
| MAIDEN | 57; 33/12/12 | 14/28/35 | .341 / 2.85 / .768 | **APPLY** | both validation windows pass; conservative single-metric movement |
| SARTLI | 146; 87/29/30 | 39/100/124 | .487 / 1.98 / .840 | HOLD | candidate inner and full objectives regress |
| SATIS | 14; 8/3/3 | 1/6/12 | .342 / 2.66 / .765 | HOLD | sample too low and outer objective regresses |

## Active profile decisions

| Active profile | Full races | Min threshold | Eligibility | Metric decision |
|---|---:|---:|---|---|
| HANDIKAP category | 120 | 25 | ready | SHADOW |
| HANDIKAP14 | 5 | 12 | below | HOLD |
| HANDIKAP14\|Kum | 19 | 8 | ready | HOLD |
| HANDIKAP15 | 2 | 12 | below | HOLD |
| HANDIKAP15\|Kum | 21 | 8 | ready | HOLD |
| HANDIKAP15\|Cim | 17 | 8 | ready | HOLD |
| HANDIKAP16 | 4 | 12 | below | HOLD |
| HANDIKAP16\|Kum | 17 | 8 | ready | HOLD |
| HANDIKAP16\|Cim | 17 | 8 | ready | SHADOW (`track_suit +8pp`) |
| MAIDEN | 57 | 12 | ready | **APPLY** |
| KV | 52 | 12 | ready | SHADOW (`track_suit +5pp`) |
| GRUP | 26 | 12 | ready | SHADOW (`pedigree +8pp`) |
| SATIS | 14 | 12 | ready | HOLD |
| SARTLI fallback | 13 selected; 146 category | 12 | ready | HOLD |
| SART1 | 7 | 12 | below | HOLD |
| SART2 raw | 2 | 12 | below | HOLD |
| SART3 | 33 | 12 | ready | SHADOW (`jockey +4.86pp`) |
| SART4 | 55 | 12 | ready | SHADOW (`form +7.86pp`) |
| SART5 | 38 | 12 | ready | HOLD |

The runtime `sampleRaces` counters were refreshed from this full-only corpus. The metric decision is stricter than simple sample eligibility: sufficient sample alone does not permit a weight change.

## Applied MAIDEN weights

The raw total stays `93.00`. All existing weights are multiplied by `0.92`, then `7.44` raw points are added to `degree_avg` (an 8% normalized blend).

| Metric | v4.21 raw | v4.23 raw |
|---|---:|---:|
| agf_score | 16.00 | 14.72 |
| pedigree | 18.00 | 16.56 |
| training_fitness | 8.00 | 7.36 |
| training_degree_score | 17.00 | 15.64 |
| jockey_score | 6.00 | 5.52 |
| trainer_score | 7.00 | 6.44 |
| pace_score | 5.00 | 4.60 |
| running_style_proxy_score | 1.00 | 0.92 |
| hp_score | 4.00 | 3.68 |
| weight_impact | 2.00 | 1.84 |
| form_trend | 4.00 | 3.68 |
| degree_avg | 1.00 | **8.36** |
| degree_stability | 1.00 | 0.92 |
| distance_suit | 1.00 | 0.92 |
| surface_transition_score | 1.00 | 0.92 |
| age_score | 1.00 | 0.92 |

Normalized shares move as follows:

- `degree_avg`: `1.075% -> 8.989%`
- `agf_score`: `17.204% -> 15.828%`
- `pedigree`: `19.355% -> 17.806%`
- `training_degree_score`: `18.280% -> 16.817%`

### MAIDEN replay

| Window | Baseline | Candidate | Objective delta |
|---|---|---|---:|
| Inner 12 | WTop3 `5/12`, Rho `.451`, MAE `2.48`, NDCG `.811` | WTop3 `7/12`, Rho `.526`, MAE `2.34`, NDCG `.865` | `+.0400` |
| Outer 12 | Top1 `3/12`, WTop3 `6/12`, WTop5 `7/12`, Rho `.330`, MAE `2.85`, NDCG `.768` | Top1 `3/12`, WTop3 `6/12`, WTop5 `8/12`, Rho `.333`, MAE `2.74`, NDCG `.778` | `+.0071` |
| Full 57 | Top1 `14`, WTop3 `28`, WTop5 `35`, Rho `.341`, MAE `2.85`, NDCG `.768` | Top1 `15`, WTop3 `31`, WTop5 `40`, Rho `.404`, MAE `2.75`, NDCG `.798` | `+.0263` |

## v4.22 HANDIKAP safety result

The fixed v4.22 HANDIKAP candidate is not safe to promote on the repaired corpus:

| Set | Current Top1/WTop3/WTop5 | Candidate Top1/WTop3/WTop5 | Rho | MAE | NDCG@5 | Objective delta |
|---|---|---|---|---|---|---:|
| 120 full HANDIKAP | 30/71/94 | 24/67/90 | .346 -> .362 | 2.68 -> 2.69 | .786 -> .774 | `-.0028` |
| Latest outer 24 | 5/13/18 | 3/14/18 | .405 -> .437 | 2.82 -> 2.69 | .795 -> .782 | `+.0020` |

Real-source coverage remains thin: recent finish `13/120`, handicap load value `12/120`, age curve `9/120`, surface-switch safety `21/120`, and distance transition `53/120`. The candidate therefore stays `shadow_only` until new analyses accumulate and a later chronological gate passes.

## Verification artifacts

- Integrity-guarded full-only opportunity report generated from the repaired corpus: `/tmp/metric-opportunity-repaired-integrity-guard-20260716`.
- Strong three-window audit: `/tmp/group_metric_audit.json`.
- Runtime tests cover version separation, exact MAIDEN weights, AGF share reduction, and refreshed per-profile sample thresholds.
