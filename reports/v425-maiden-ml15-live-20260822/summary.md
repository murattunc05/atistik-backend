# MAIDEN ML15 controlled live rollout - 2026-08-22

## Decision

- Visible scope: MAIDEN only.
- Ranking formula: 85% visible v4.25 base plus 15% strict no-AGF ML.
- Visible ranking id: `v4.25+maiden-ml15-20260823-v2`.
- Other groups and the v4.25 raw metric weights are unchanged.
- The previous MAIDEN shadow remains historical evidence; new predictions use
  the retrained immutable artifact and are monitored at +5/+10/+15 as rollback
  checkpoints.

## Evidence

- Source snapshot SHA-256:
  `2b4686b6e141b6422a0355b83cf2908547792d0be9e03da9ee158237bf76005d`.
- Integrity-clean, feature-complete corpus: 800 races.
- Model training: 637 races, 09.05.2026 through 10.08.2026.
- Untouched outer holdout: 163 races, 11.08.2026 through 21.08.2026.
- MAIDEN evidence: 125 total; 28 inner; 33 outer.
- ML feature policy: 58 features, strict no-AGF. AGF, v4 score/rank, and
  AGF-derived aggregate features are excluded.

## Winner Top3 replay

| Window | v4.25 base | ML15 candidate | Rescue | Damage | Top1 | Boundary gap |
|---|---:|---:|---:|---:|---:|---:|
| Inner 28 | 12 | 13 | 1 | 0 | 3 -> 2 | 1.062x |
| Outer 33 | 15 | 18 | 3 | 0 | 9 -> 10 | 1.831x |

The outer MAIDEN slice improved in each chronological third: +1, +1, +1
Winner Top3, with zero damage in all three windows. Outer rho improved from
0.324 to 0.356, MAE from 3.377 to 3.305, and NDCG@5 from 0.766 to 0.774.

## Calibration and rollout guard

- Outer Winner Top3 Brier improved by 0.0072 and log loss improved by 0.0156.
- Outer ECE is 0.1127, slightly above the preferred 0.10 ceiling. Because the
  user-facing objective is Winner Top3 ranking, the blend is bounded at 15%
  and score/rank separation improved materially, this is accepted as a
  controlled live rollout rather than a probability-calibration rollout.
- Any model load, manifest, feature-schema, score/rank fidelity, or non-finite
  feature failure keeps the visible v4.25 ranking fail-closed.
- The nightly MAIDEN monitor now treats +5/+10/+15 as live rollback guardrails.

## Frozen artifact

- Candidate: `maiden-ml15-20260823-v2`.
- Model: `maiden-live-20260822-v2`.
- Model SHA-256:
  `dc58166972df6b39fd7c01f7b5d173576915b8c1bf6fb5368053e8a8e7c1f29a`.
- Feature schema SHA-256:
  `5267a4de81de7e97ce46556be96af9041816baec0770de180631bc87c52fbae0`.
