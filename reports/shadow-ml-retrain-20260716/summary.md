# Shadow ML Retrain Decision - 2026-07-16

## Decision

- Model artifact: `HOLD` — do not replace or deploy the current shadow model.
- Trainer leakage fix: `APPLY` — keep the stricter no-AGF feature exclusion in code.

The corrected candidate now beats the current shadow overall on most holdout metrics, but it still regresses the HANDIKAP guardrails and does not satisfy the three-consecutive-report activation rule. No live model, feature-stat, Render, Pi, or GitHub model artifact was overwritten.

## Leakage correction

Strict no-AGF now excludes every direct or downstream AGF carrier:

- `agf_score`
- `has_agf`
- `v4_score`
- `v4_rank`
- `top3_feature_avg`
- `feature_variance`

`v4_score` and `v4_rank` are downstream AGF signals because visible v4 may consume AGF in MAIDEN and SART1. The saved candidate has `50` features; all six fields above are absent from its serialized `feature_cols`.

## Exact training command and environment

Working directory: `/tmp/atistik-backend-result-hotfix`

```bash
PYTHONPYCACHEPREFIX=/tmp/atistik-pycache /tmp/atistik-ml-training-venv/bin/python train_shadow_ml.py --input /tmp/atistik-ml-data-audit-019f6ad1/predictions.jsonl --output-dir /tmp/atistik-shadow-candidate-20260716-strict-noagf-v4free --model-variant no-agf
```

Environment:

- Python `3.12.13`, Apple Clang `21.1.4`
- macOS `26.5.1`, arm64
- NumPy `2.3.5`
- scikit-learn `1.9.0`
- XGBoost `3.3.0`
- libomp `22.1.8`

## Corpus and split

- Input SHA-256: `52880f1eb4165b67bdb9c39512cc0a0d762aee1d2adf270ab28cc1b518486103`
- Raw input: `7,691` rows / `742` races.
- Selected: `416` complete races / `4,091` rows.
- Excluded as whole races: `273` partial and `53` unlabeled races.
- Training: `333` races, through `2026-06-26`.
- Validation: `83` races, `2026-06-27` through `2026-07-15`.
- Train/validation date overlap: zero.
- Walk-forward: `3` date-disjoint folds.
- Sparse-source gate: minimum `25` training races and `5%` coverage; all seven new sparse metrics remained excluded.

Post-train integrity re-audit with the production guard:

- Integrity-clean fully labeled races: `416/416`.
- Integrity-invalid races/rows excluded: `0/0`.
- Rank-out-of-range races/rows: `0/0`.
- Invalid competition-ranking patterns: `0`.
- Accepted valid ties: `3` races.
- Accepted terminal `99` status: `2` races / `2` rows.

Selection remained exactly `416` races / `4,091` rows with the same `333/83` split. Therefore retraining was intentionally skipped and the candidate model, stats, metrics, and hashes below remain unchanged.

## Candidate artifacts

- Version: `shadow-20260716-1729`
- Model SHA-256: `c2b34dccba6bacb94eac3a9e96463a911f0f3db9502d63285e1e387a12b50120`
- Feature stats SHA-256: `821c76bf6080f25ca3dd385a280ac1804237503f53e0ceaccf0e27317d6f152c`
- Candidate report SHA-256: `98cd2c21105f3d6405e42ebb233b7e766fb1de2d549bacf9060e7ce983af50db`

Offline files remain under `/tmp/atistik-shadow-candidate-20260716-strict-noagf-v4free/` and are intentionally not installed as active artifacts.

Current shadow model remained unchanged:

- Version: `shadow-20260629-2327`
- Model SHA-256: `ef8c999af0e10203a1da2a6fc961c9e9bf7badcfa0ab701f8845b700f2a889b0`
- Feature stats SHA-256: `51b86736ec8111a1eac2a1aab0c00ecc8a45288aa7f28704cf34ec0db7dacf51`

## Same 83-race holdout

| Model | Top1 | WTop3 | WTop5 | Rho | MAE | NDCG@5 |
|---|---:|---:|---:|---:|---:|---:|
| Visible v4.21 baseline | 18 | 52 | 69 | .401 | 2.880 | .812 |
| Current shadow `shadow-20260629-2327` | 18 | 53 | 71 | .467 | 2.721 | .824 |
| Corrected strict no-AGF `shadow-20260716-1729` | 29 | 55 | 71 | .480 | 2.633 | .835 |

Against the current shadow, the candidate gains `+11` Top1 and `+2` WTop3, ties WTop5, improves rho by `.014`, lowers MAE by `.089`, and improves NDCG@5 by `.011`.

### Per-group comparison against current shadow

| Group | Races | Current Top1/W3/W5 | Candidate Top1/W3/W5 | Current rho/MAE/NDCG | Candidate rho/MAE/NDCG |
|---|---:|---:|---:|---:|---:|
| HANDIKAP | 25 | 4/14/20 | 5/13/19 | .421 / 2.976 / .788 | .413 / 3.045 / .779 |
| MAIDEN | 5 | 1/2/3 | 2/3/4 | .405 / 2.000 / .841 | .495 / 1.917 / .877 |
| SARTLI | 28 | 7/23/28 | 11/22/26 | .604 / 1.916 / .873 | .592 / 1.857 / .884 |
| KV | 10 | 3/8/10 | 5/8/10 | .379 / 1.857 / .844 | .401 / 1.686 / .863 |
| GRUP | 11 | 2/4/6 | 4/6/8 | .324 / 4.137 / .755 | .432 / 3.685 / .796 |
| SATIS | 4 | 1/2/4 | 2/3/4 | .477 / 2.889 / .817 | .432 / 2.933 / .821 |

HANDIKAP remains the blocking guardrail: the candidate loses one WTop3 and one WTop5, with worse rho, MAE, and NDCG@5. SARTLI also loses one WTop3 and two WTop5 despite stronger Top1, MAE, and NDCG.

## Walk-forward result

| Fold | Train | Validation | v4 Top1/W3 | Candidate Top1/W3 | v4 rho/MAE/NDCG | Candidate rho/MAE/NDCG |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 233 | 65 | 13/30 | 16/30 | .361 / 2.741 / .794 | .312 / 2.896 / .789 |
| 2 | 298 | 69 | 18/46 | 16/49 | .421 / 2.734 / .820 | .427 / 2.650 / .817 |
| 3 | 367 | 49 | 11/32 | 17/33 | .411 / 2.697 / .819 | .484 / 2.423 / .841 |

Fold 1 is a broad quality regression, fold 2 is mixed, and only fold 3 is clearly stronger. Therefore the activation condition of three consecutive winning reports is not met. The corpus is below the `1,000` overall-race gate; HANDIKAP has reached `120` full races, while the other active profiles remain below the `120` per-profile gate.

## Verification

```bash
PYTHONPYCACHEPREFIX=/tmp/atistik-pycache /tmp/atistik-ml-training-venv/bin/python -m unittest -v test_train_shadow_ml.py
PYTHONPYCACHEPREFIX=/tmp/atistik-pycache /tmp/atistik-ml-training-venv/bin/python -m unittest -v test_train_shadow_ml.py test_v416_rules.py
```

Results: trainer tests `8/8`; combined trainer and ranking regression tests `35/35`. The trainer suite includes explicit assertions for strict no-AGF exclusion, valid competition ties, terminal `99`, broken patterns, and out-of-range ranks.

See `candidate_training_report.md` for full validation and walk-forward tables.
