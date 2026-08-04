# SART1 bounded prospective shadow

Date: 2026-08-04

## Decision

- Keep visible v4.24 ranking and Telegram output unchanged.
- Start a frozen, prospective-only SART1 candidate on 2026-08-05.
- Candidate AGF share is capped at 10% and is enabled only when at least 80% of
  the runners have real AGF. Missing runners receive neutral AGF in the
  candidate copy only.
- The same race also records a frozen no-AGF ablation so the 10% AGF overlay is
  judged against its identical AGF-free counterpart, not only against live v4.
- The candidate is structurally inside the metric-update movement gate, but it
  is not live-rollout eligible until new results and the formal replay gate pass.

## Integrity-clean evidence

- SART1 corpus: 15 fully labeled, integrity-clean races / 166 runners.
- Build: 11 races before 2026-07-28.
- Locked recent validation: 4 races from 2026-07-28 through 2026-08-03.
- Other operational races in the recent week were partial/incomplete and were
  excluded as whole races.
- Runtime base-score parity: 131/131 rows reproduced within 0.1 points. The
  earliest 35 legacy rows did not log `v4_base_score`; their source availability
  required the older non-neutral feature heuristic and remains a data limit.

| Window | Ranking | Top1 | Winner Top3 | Winner Top5 |
| --- | --- | ---: | ---: | ---: |
| Build 11 | Current v4.24 replay | 1 | 8 | 8 |
| Build 11 | Aggressive exploratory A | 3 | 8 | 8 |
| Build 11 | Bounded B | 4 | 8 | 8 |
| Locked recent 4 | Current v4.24 replay | 1 | 2 | 4 |
| Locked recent 4 | Aggressive exploratory A | 2 | 3 | 4 |
| Locked recent 4 | Bounded B | 1 | 2 | 4 |
| Full 15 | Current v4.24 replay | 2 | 10 | 12 |
| Full 15 | Aggressive exploratory A | 5 | 11 | 12 |
| Full 15 | Bounded B | 5 | 10 | 12 |

The aggressive A candidate is rejected for rollout: its largest single move is
17.328 percentage points and total variation is 41.394 points. It also lowers
full-order NDCG/rho and raises MAE. The bounded B candidate does not improve the
locked recent Winner Top3 result, but it does not regress Winner Top3 or Top5;
it improves full-corpus NDCG from .808 to .826. It is therefore observation-only.

The four locked recent races had 0/37 real AGF source coverage. Their replay
differences come from redistribution, not from AGF itself. Prospective reporting
keeps AGF-sufficient and AGF-insufficient races identifiable.

## Frozen bounded weights

| Metric | Percent |
| --- | ---: |
| AGF | 10.0000 |
| Pedigree | 15.8574 |
| Training fitness | 11.2900 |
| Training degree | 5.9465 |
| Jockey | 7.9287 |
| Trainer | 4.6800 |
| Pace | 6.9376 |
| Running style | 2.9733 |
| Form trend | 6.9376 |
| Degree average | 4.9554 |
| HP | 2.9733 |
| Weight impact | 2.9733 |
| Distance suitability | 3.9644 |
| Surface transition | 1.9822 |
| Age | 1.9822 |
| Bounce | 4.3091 |
| Track experience | 4.3091 |

Against visible v4.24 SART1, the maximum single movement is 6.56 percentage
points and total variation is 13.12 points. Both remain inside the existing
10-point single-move and 20-point total-variation limits.

## Prospective checkpoints

- +5 clean races: health and source-coverage check; no rollout decision.
- +10 clean races: early signal; reject on clear Winner Top3/Top5 regression.
- +15 clean races: require three non-overlapping five-race windows with no
  Winner Top3/Top5 regression and at least +2 cumulative Winner Top3 hits before
  opening a formal replay review.
- AGF overlay evidence: at least 6 prospective AGF-sufficient races; conditional
  AGF must not regress Winner Top3, Winner Top5, or the full-order objective
  against the logged no-AGF ablation.
- Formal visible review: at least 30 clean profile races, at least 6 outer
  holdout races, both chronological objective deltas above 0.005, full-corpus
  objective delta above 0.003, and all source-coverage/weight guardrails.

The monitor always sets `liveRolloutEligible=false`; promotion requires an
explicit formal replay and operator decision.
