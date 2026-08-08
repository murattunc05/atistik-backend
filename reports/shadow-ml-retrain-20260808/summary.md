# Shadow ML retrain decision - 2026-08-08

- Trigger: 709 new result-bearing races since the deployed training marker.
- Input: 12,139 prediction rows / 1,194 races.
- Integrity-clean fully labeled races: 670.
- Training-eligible, feature-complete races: 613.
- Train/validation: 492 / 121 chronological date-block split.
- Candidate: no-AGF XGBoost ranker, 55 features.
- Candidate version produced in isolation: `shadow-20260808-1536`.
- Visible ranking impact during training/evaluation: none.

## Chronological validation

| Segment | Model | Top1 | Winner Top3 | Winner Top5 | MAE | Rho | NDCG@5 |
|---|---|---:|---:|---:|---:|---:|---:|
| Overall | live v4 | 36/121 | 77/121 | 96/121 | 2.380 | 0.447 | 0.823 |
| Overall | new ML no-AGF | 30/121 | 73/121 | 95/121 | 2.470 | 0.431 | 0.815 |
| ŞARTLI | live v4 | 14/41 | 29/41 | 34/41 | 1.965 | 0.511 | 0.836 |
| ŞARTLI | new ML no-AGF | 11/41 | 27/41 | 33/41 | 2.110 | 0.455 | 0.826 |

## Latest walk-forward fold

| Segment | Model | Top1 | Winner Top3 | MAE | Rho | NDCG@5 |
|---|---|---:|---:|---:|---:|---:|
| Overall (101 races) | live v4.24 | 28 | 63 | 2.451 | 0.438 | 0.817 |
| Overall (101 races) | new ML no-AGF | 23 | 59 | 2.579 | 0.402 | 0.804 |
| HANDIKAP (29 races) | live v4.24 | 7 | 18 | 2.490 | 0.470 | 0.824 |
| HANDIKAP (29 races) | new ML no-AGF | 6 | 16 | 2.626 | 0.408 | 0.786 |

## Decision

Retraining was completed because the threshold was due, but the generated
artifact failed the primary Winner Top3 and full-order guardrails. It was not
copied over `model_shadow_ranker.json` or `feature_stats_shadow.json`. The
deployed `shadow-20260716-1729` model remains shadow-only and visible ranking
continues to use v4.
