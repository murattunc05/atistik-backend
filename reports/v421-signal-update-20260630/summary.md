# v4.21 Signal Update Report (2026-06-30 01:29)

- Analysis-only report from labeled export.
- v4.21 candidate keeps AGF disabled outside MAIDEN + SART1.
- Historical export cannot reconstruct full class_peak_score; GRUP class peak is therefore neutral in this report.

| Segment | Races | Top1 | WTop3 | WTop5 | Rho | MAE | NDCG@5 |
|---|---:|---:|---:|---:|---:|---:|---:|
| v4 logged overall | 585 | 149/585 | 330/585 | 451/585 | 0.365 | 2.573 | 0.796 |
| v4.21 candidate overall | 585 | 155/585 | 337/585 | 449/585 | 0.373 | 2.557 | 0.800 |
| v4 logged GRUP | 26 | 6/26 | 12/26 | 17/26 | 0.209 | 3.320 | 0.726 |
| v4.21 candidate GRUP | 26 | 7/26 | 12/26 | 24/26 | 0.315 | 3.083 | 0.790 |
| v4 logged HANDIKAP | 160 | 36/160 | 78/160 | 113/160 | 0.319 | 3.041 | 0.760 |
| v4.21 candidate HANDIKAP | 160 | 36/160 | 86/160 | 115/160 | 0.322 | 3.009 | 0.764 |

## Notes
- HANDIKAP visible profile stays conservative; new field-relative, pace-map, surface-safety and favorite-risk signals are logged for validation.
- GRUP candidate uses AGF-free elite consensus; full live class history will be stronger than this export-only approximation.
