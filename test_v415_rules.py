import unittest

from api_server import (
    _V4_VERSION,
    apply_v4_shadow_mode,
    calculate_v4_ranking_penalties,
    extract_v4_race_profile,
    resolve_v4_profile_weights,
)
from train_shadow_ml import feature_dict


class V415RulesTest(unittest.TestCase):
    def test_version(self):
        self.assertEqual(_V4_VERSION, "4.15")

    def test_agf_is_limited_to_maiden_and_sartli_one(self):
        maiden = resolve_v4_profile_weights(
            extract_v4_race_profile("Maiden", "1400", "Kum", 10)
        )
        sart1 = resolve_v4_profile_weights(
            extract_v4_race_profile("Şartlı 1", "1400", "Kum", 10)
        )
        sart4 = resolve_v4_profile_weights(
            extract_v4_race_profile("Şartlı 4", "1400", "Kum", 10)
        )
        handicap = resolve_v4_profile_weights(
            extract_v4_race_profile("Handikap 15", "1400", "Kum", 10)
        )

        self.assertTrue(maiden["agfAllowedForRanking"])
        self.assertGreater(maiden["weights"]["agf_score"], 0)
        self.assertTrue(sart1["agfAllowedForRanking"])
        self.assertGreater(sart1["weights"]["agf_score"], 0)
        self.assertFalse(sart4["agfAllowedForRanking"])
        self.assertEqual(sart4["weights"]["agf_score"], 0)
        self.assertFalse(handicap["agfAllowedForRanking"])
        self.assertEqual(handicap["weights"]["agf_score"], 0)

    def test_shadow_ml_does_not_treat_sartli_19_as_sartli_1(self):
        features = feature_dict({
            "race_type": "Sartli 19",
            "features": {"agf_score": 92},
            "metric_source_flags": {"hasAgf": True},
        })
        self.assertEqual(features["is_sart1"], 0)
        self.assertEqual(features["has_agf"], 0)
        self.assertEqual(features["agf_score"], 50)

    def test_layoff_boundaries(self):
        expected = {
            39: 0,
            40: 5,
            60: 5,
            61: 7,
            90: 7,
            91: 11,
        }
        for days, penalty in expected.items():
            with self.subTest(days=days):
                result = calculate_v4_ranking_penalties([], "14.06.2026", str(days))
                self.assertEqual(result["totalPenalty"], penalty)
                self.assertEqual(result["restDataSource"], "kgs")

    def test_recent_long_race_boundary(self):
        at_1800 = calculate_v4_ranking_penalties(
            [{"date": "09.06.2026", "distance": "1800"}],
            "14.06.2026",
        )
        at_1801 = calculate_v4_ranking_penalties(
            [{"date": "09.06.2026", "distance": "1801"}],
            "14.06.2026",
        )
        self.assertEqual(at_1800["totalPenalty"], 0)
        self.assertEqual(at_1801["totalPenalty"], 6)

    def test_penalty_is_applied_after_base_score(self):
        metrics = {
            "degree_avg": 50,
            "degree_trend": 50,
            "degree_stability": 50,
            "form_trend": 50,
            "track_experience_score": 50,
            "distance_suit": 50,
            "training_fitness": 100,
            "training_degree_score": 50,
            "weight_impact": 50,
            "jockey_score": 50,
            "bounce_score": 50,
            "pace_score": 50,
            "pedigree": 50,
            "hp_score": 50,
            "agf_score": 100,
            "age_score": 50,
            "_has_training": True,
            "_has_agf": True,
        }
        horse = {
            "name": "TEST",
            "_mf": metrics,
            "v4PenaltyTotal": 5,
            "rankingPenalties": [{"code": "long_layoff_40_60", "points": 5}],
        }
        apply_v4_shadow_mode([horse], "Handikap 15", "1400", "Kum")
        self.assertEqual(horse["v4Score"], max(0, horse["v4BaseScore"] - 5))
        self.assertFalse(horse["agfAllowedForRanking"])

    def test_training_does_not_cancel_layoff_penalty(self):
        result = calculate_v4_ranking_penalties([], "14.06.2026", "45")
        horse = {
            "name": "TRAINED TEST",
            "_mf": {
                "degree_avg": 50,
                "training_fitness": 100,
                "_has_training": True,
            },
            "v4PenaltyTotal": result["totalPenalty"],
            "rankingPenalties": result["penalties"],
        }
        apply_v4_shadow_mode([horse], "Handikap 15", "1400", "Kum")
        self.assertEqual(horse["v4PenaltyTotal"], 5)
        self.assertEqual(horse["v4Score"], max(0, horse["v4BaseScore"] - 5))


if __name__ == "__main__":
    unittest.main()
