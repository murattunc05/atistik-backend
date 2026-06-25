# Metric Opportunity Analysis

- Export entries used: `5012` labeled rows before race filtering.
- Objective: order closeness (`45% NDCG@5`, `35% normalized Spearman`, `20% MAE component`).
- AGF policy: included only for `MAIDEN` and `SART1`; otherwise diagnostic only.
- This report is analysis-only; live v4 weights are unchanged.

## Candidate Summary

| Segment | Level | Races | Status | Verdict | Risk | Holdout WTop3 | Holdout MAE | Holdout Rho | Candidate Weights |
|---|---|---:|---|---|---|---|---|---|---|
| GLOBAL | group | 1 | diagnostic_only | sample_too_low | none | 0 vs 0 | - | - | degree_stability 41.8%, bounce_score 29.9%, training_fitness 18.8%, jockey_score 7.7%, degree_trend 1.8% |
| GRUP | group | 19 | diagnostic_only | sample_too_low | winner_top3_regression | 2 vs 3 | 1.570 vs 1.770 | 0.379 vs 0.337 | hp_score 24.2%, bounce_score 17.1%, distance_suit 13.0%, degree_trend 11.6%, form_trend 11.0%, trainer_score 9.9% |
| HANDIKAP | group | 141 | candidate | not_supported | winner_top3_regression | 9 vs 13 | 2.850 vs 2.810 | 0.235 vs 0.261 | form_trend 31.6%, pace_score 12.6%, training_degree_score 9.6%, distance_suit 8.1%, hp_score 6.8%, training_fitness 6.2% |
| KV | group | 57 | candidate | supported | none | 8 vs 7 | 1.510 vs 1.430 | 0.346 vs 0.233 | hp_score 28.9%, track_suit 19.4%, distance_suit 19.1%, degree_trend 6.3%, pace_score 5.8%, form_trend 5.5% |
| MAIDEN | group | 101 | candidate | not_supported | none | 14 vs 12 | 1.800 vs 1.780 | 0.508 vs 0.532 | agf_score 24.5%, form_trend 19.8%, hp_score 10.7%, degree_avg 10.2%, distance_suit 9.1%, degree_stability 7.9% |
| SARTLI | group | 187 | candidate | not_supported | none | 25 vs 23 | 1.760 vs 1.740 | 0.414 vs 0.427 | form_trend 24.9%, distance_suit 17.3%, hp_score 13.4%, track_suit 9.9%, trainer_score 7.5%, degree_stability 6.4% |
| SATIS | group | 16 | diagnostic_only | sample_too_low | none | 2 vs 2 | 2.920 vs 2.910 | 0.399 vs 0.361 | track_suit 20.4%, bounce_score 17.5%, hp_score 16.6%, degree_trend 13.4%, distance_suit 9.4%, degree_avg 5.3% |
| HANDIKAP14 | profile | 7 | diagnostic_only | sample_too_low | none | 1 vs 1 | 4.190 vs 3.090 | 0.117 vs 0.386 | distance_suit 27.3%, training_degree_score 16.9%, bounce_score 11.0%, track_suit 10.0%, degree_stability 8.6%, training_fitness 6.2% |
| HANDIKAP14|Kum | profile | 20 | candidate | supported | none | 3 vs 3 | 1.740 vs 1.830 | 0.591 vs 0.553 | form_trend 23.5%, training_fitness 14.2%, training_degree_score 10.5%, degree_stability 9.8%, track_suit 8.6%, track_experience_score 7.8% |
| HANDIKAP15 | profile | 5 | diagnostic_only | sample_too_low | none | 0 vs 0 | 4.620 vs 4.620 | -0.093 vs -0.121 | training_degree_score 18.6%, degree_avg 14.5%, track_experience_score 10.9%, weight_impact 9.3%, hp_score 9.1%, degree_trend 8.7% |
| HANDIKAP15|Cim | profile | 25 | candidate | not_supported | none | 0 vs 0 | 3.710 vs 3.400 | -0.049 vs 0.054 | form_trend 31.4%, track_suit 16.4%, degree_avg 12.8%, distance_suit 12.7%, running_style_proxy_score 7.6%, training_degree_score 4.8% |
| HANDIKAP15|Kum | profile | 26 | candidate | not_supported | winner_top3_regression | 5 vs 6 | 2.430 vs 2.100 | 0.387 vs 0.523 | form_trend 24.7%, hp_score 13.1%, trainer_score 11.9%, bounce_score 10.3%, degree_trend 9.9%, track_experience_score 9.7% |
| HANDIKAP16 | profile | 7 | diagnostic_only | sample_too_low | none | 1 vs 1 | 2.430 vs 2.570 | -0.048 vs -0.006 | form_trend 35.5%, trainer_score 25.4%, weight_impact 11.7%, degree_stability 6.5%, pace_score 5.9%, distance_suit 4.7% |
| HANDIKAP16|Cim | profile | 12 | candidate | supported | none | 2 vs 1 | 3.600 vs 3.460 | 0.343 vs 0.366 | form_trend 18.4%, trainer_score 14.5%, weight_impact 10.6%, degree_trend 10.6%, degree_avg 10.3%, training_degree_score 9.1% |
| HANDIKAP16|Kum | profile | 17 | candidate | not_supported | winner_top3_regression | 2 vs 3 | 2.490 vs 2.040 | 0.083 vs 0.247 | form_trend 31.5%, degree_stability 13.2%, training_fitness 10.2%, pace_score 10.1%, training_degree_score 8.6%, weight_impact 5.5% |
| SART1 | profile | 26 | candidate | mixed | none | 2 vs 2 | 2.430 vs 2.510 | 0.418 vs 0.424 | agf_score 81.8%, training_fitness 8.5%, pace_score 6.0%, trainer_score 2.5%, pedigree 1.2% |
| SART3 | profile | 38 | candidate | mixed | none | 7 vs 5 | 1.780 vs 1.810 | 0.427 vs 0.438 | form_trend 40.9%, hp_score 19.5%, distance_suit 15.2%, degree_avg 8.3%, pace_score 5.5%, track_suit 3.8% |
| SART4 | profile | 62 | candidate | supported | none | 10 vs 10 | 1.240 vs 1.480 | 0.601 vs 0.464 | distance_suit 22.6%, hp_score 17.6%, trainer_score 13.6%, form_trend 10.0%, training_fitness 9.3%, bounce_score 9.2% |
| SART5 | profile | 46 | candidate | not_supported | none | 4 vs 4 | 1.720 vs 1.530 | 0.317 vs 0.415 | bounce_score 17.5%, form_trend 16.8%, hp_score 14.8%, track_suit 12.7%, degree_stability 8.4%, degree_trend 6.8% |
