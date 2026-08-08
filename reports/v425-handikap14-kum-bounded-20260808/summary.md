# v4.25 HANDIKAP 14/Kum bounded metric update

- Evidence: 28 integrity-clean, fully labeled races through 2026-08-08.
- Optimization holdout: 6 chronologically latest races.
- Primary guardrail: Winner Top3 must not regress.
- AGF policy: disabled.
- Visible scope: HANDIKAP 14 on Kum only.

## Candidate decision

The unconstrained optimizer was rejected because it moved 58.33% of the
weight mass and changed three individual metrics by more than 10 percentage
points. The accepted candidate keeps 85% of the v4.24 weights and blends only
15% of that proposal:

- Total variation: 8.75 percentage points.
- Maximum single-metric movement: 1.80 percentage points.
- Holdout Winner Top3: 2/6 vs 2/6.
- Holdout Winner Top5: 4/6 vs 3/6.
- Holdout MAE: 2.61 vs 2.78.
- Holdout rho: 0.473 vs 0.435.
- Holdout NDCG@5: 0.776 vs 0.762.

## Expanding walk-forward guardrail

| Fold | Train | Validation | v4.24 Winner Top3 | v4.25 Winner Top3 | v4.24 MAE | v4.25 MAE | v4.24 rho | v4.25 rho |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 14 | 5 | 3 | 3 | 2.47 | 2.55 | 0.579 | 0.567 |
| 2 | 19 | 5 | 3 | 3 | 2.99 | 2.95 | 0.234 | 0.240 |
| 3 | 24 | 4 | 1 | 1 | 2.52 | 2.36 | 0.544 | 0.599 |

Winner Top3 did not regress in any fold. This update does not change any
ŞARTLI, MAIDEN, KV, GRUP, SATIŞ, other HANDIKAP profile, Telegram formatting,
or the separate SART1 prospective shadow candidate.
