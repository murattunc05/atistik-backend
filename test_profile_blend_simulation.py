import unittest

import profile_blend_simulation as simulation
import train_shadow_ml as training


def race_rows(race_id="R1", winner_v4_rank=4, winner_ml_score=100.0):
    rows = []
    v4_scores = [40.0, 70.0, 60.0, 50.0]
    for index in range(4):
        finish_pos = index + 1
        row = {
            "race_id": race_id,
            "race_date": "01.07.2026",
            "race_no": 1,
            "horse_no": index + 1,
            "finish_pos": finish_pos,
            "field_size": 4,
            "rank_pred": [winner_v4_rank, 1, 2, 3][index],
            "v4_rank": [winner_v4_rank, 1, 2, 3][index],
            "v4_score": v4_scores[index],
            "race_type": "HANDİKAP 16",
            "track": "Kum",
            "v4_profile": {"category": "HANDIKAP", "subtype": "HANDIKAP16"},
            "features": {"form_trend": [winner_ml_score, 30.0, 20.0, 10.0][index]},
            "metric_source_flags": {},
        }
        rows.append(row)
    return rows


class FakeModel:
    def predict(self, matrix):
        return matrix[:, 0]


class ProfileBlendSimulationTests(unittest.TestCase):
    def test_default_alpha_grid_is_bounded_to_fifteen_percent(self):
        self.assertEqual(max(simulation.ALPHA_GRID), 0.15)

    def test_zero_alpha_preserves_visible_v4_ranking(self):
        rows = race_rows()

        scores, faithful = simulation.blended_scores(
            FakeModel(), rows, ["form_trend"], 0.0
        )
        ranks = training.rank_from_scores(rows, scores)
        baseline = training.rank_from_visible_v4(rows)

        self.assertTrue(faithful)
        self.assertEqual(
            [ranks[id(row)] for row in rows],
            [baseline[id(row)] for row in rows],
        )

    def test_blend_comparison_counts_a_rescue(self):
        races = {"R1": race_rows()}

        comparison = simulation.compare_blend_to_existing(
            FakeModel(), races, ["form_trend"], 0.5
        )

        self.assertEqual(comparison["rescues"], 1)
        self.assertEqual(comparison["damages"], 0)
        self.assertEqual(comparison["winnerTop3Net"], 1)
        events = simulation.top3_transition_events(
            FakeModel(), races, ["form_trend"], 0.5
        )
        self.assertEqual(events[0]["event"], "RESCUE")
        self.assertEqual(events[0]["baselineWinnerRank"], 4)
        self.assertLessEqual(events[0]["candidateWinnerRank"], 3)

    def test_gate_requires_evidence_and_zero_outer_damage(self):
        inner = {
            "races": 6,
            "winnerTop3Net": 1,
            "damages": 0,
            "top1Net": 0,
        }
        outer = {
            "races": 6,
            "winnerTop3Net": 1,
            "damages": 0,
            "top1Net": 0,
            "boundaryGapRatio": 1.0,
            "baselineCutoffCrowdMedian": 3.0,
            "candidateCutoffCrowdMedian": 3.0,
            "v4ScoreFallbackRaces": 0,
        }

        accepted = simulation.build_segment_gate(
            "PROFILE:HANDIKAP16", 30, inner, outer, 0.10
        )
        outer["damages"] = 1
        rejected = simulation.build_segment_gate(
            "PROFILE:HANDIKAP16", 30, inner, outer, 0.10
        )

        self.assertEqual(accepted["decision"], "SHADOW_CANDIDATE")
        self.assertEqual(rejected["decision"], "REJECTED")
        self.assertIn("outer_no_damage", rejected["failedChecks"])


if __name__ == "__main__":
    unittest.main()
