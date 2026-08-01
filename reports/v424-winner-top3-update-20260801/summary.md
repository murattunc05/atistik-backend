# v4.24 Winner Top3 metric update

Date: 2026-08-01

## Scope

This update changes only the visible v4 ranking weights for MAIDEN and SARTLI
races. The shadow ML model, HANDIKAP, KV, GRUP, SATIS, and Telegram delivery
format are unchanged.

## Evidence corpus

- Live export: 11,143 prediction rows, labeled through 2026-07-31.
- Integrity-clean fully labeled races: 607.
- Feature-complete races used by the replay: 550.
- Excluded: 57 featureless recovered full races, 414 partially labeled races,
  and 70 unlabeled races.
- Candidate selection: chronological 60/20/20 build/inner/outer split.
- Primary objective: Winner Top3. Top1, Winner Top5, rho, MAE, and NDCG@5 were
  retained as guardrails.
- Maximum category overlay: 8 percentage points.

## Visible changes

- MAIDEN: keep 92% of the current profile ratios and allocate 8% to
  `distance_suit`.
- SARTLI, including its SART1/SART3/SART4/SART5 profiles: keep 92% of the
  current profile ratios, allocate 4% to `bounce_score`, and allocate 4% to
  `track_experience_score`.
- Version: `v4.23` -> `v4.24`.

## Chronological replay

| Group | Window | v4.23 Winner Top3 | v4.24 Winner Top3 | Top1 | Winner Top5 |
|---|---:|---:|---:|---:|---:|
| MAIDEN | Build (46) | 26 | 30 | 14 -> 15 | 34 -> 36 |
| MAIDEN | Inner (16) | 6 | 7 | 3 -> 2 | 9 -> 10 |
| MAIDEN | Outer (16) | 4 | 8 | 1 -> 1 | 7 -> 13 |
| MAIDEN | Full (78) | 36 | 45 | 18 -> 18 | 50 -> 59 |
| SARTLI | Build (114) | 74 | 77 | 33 -> 34 | 92 -> 93 |
| SARTLI | Inner (38) | 30 | 32 | 7 -> 7 | 37 -> 37 |
| SARTLI | Outer (38) | 33 | 33 | 16 -> 17 | 34 -> 34 |
| SARTLI | Full (190) | 137 | 142 | 56 -> 58 | 163 -> 164 |

MAIDEN is the strong candidate. SARTLI is a smaller positive move: it passed
the chronological guardrails, but its newest cohort did not add Winner Top3
hits. It is included in this visible rollout by explicit operator decision.

## Verification and rollback

- The implementation reproduces the simulated v4.24 metrics on the full
  feature-complete corpus.
- Scoring and metric tests must pass before deployment.
- Rollback is the immediate parent commit, which restores v4.23 without
  changing prediction history or result labels.
