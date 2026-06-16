import unittest

from api_server import (
    _V4_VERSION,
    _v417_apply_agf_policy,
    apply_v4_shadow_mode,
    calculate_v4_shadow_score,
    calculate_distance_transition_score,
    calculate_surface_transition_score,
    calculate_v4_ranking_penalties,
    extract_v4_race_profile,
    resolve_v4_profile_weights,
)
from train_shadow_ml import feature_dict


class V417RulesTest(unittest.TestCase):
    def test_version(self):
        self.assertEqual(_V4_VERSION, "4.17")

    def test_agf_is_allowed_for_maiden_sartli_one_and_capped_handikap(self):
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
        self.assertTrue(handicap["agfAllowedForRanking"])
        self.assertGreater(handicap["weights"]["agf_score"], 0)
        self.assertGreater(handicap["weights"]["pace_score"], handicap["weights"]["agf_score"])
        self.assertGreater(handicap["weights"]["form_trend"], 0)
        self.assertGreater(handicap["weights"]["trainer_score"], 0)
        self.assertGreater(handicap["weights"]["track_suit"], 0)


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
            "surface_transition_score": 50,
            "distance_suit": 50,
            "distance_transition_score": 50,
            "training_fitness": 100,
            "training_degree_score": 50,
            "weight_impact": 50,
            "handicap_efficiency_score": 50,
            "handicap_class_transition_score": 50,
            "running_style_proxy_score": 50,
            "jockey_score": 50,
            "bounce_score": 50,
            "pace_score": 50,
            "pedigree": 50,
            "hp_score": 50,
            "agf_score": 100,
            "age_score": 50,
            "_has_training": True,
            "_has_agf": True,
            "_has_surface_transition": True,
            "_has_distance_transition": True,
        }
        horse = {
            "name": "TEST",
            "_mf": metrics,
            "v4PenaltyTotal": 5,
            "rankingPenalties": [{"code": "long_layoff_40_60", "points": 5}],
        }
        apply_v4_shadow_mode([horse], "Handikap 15", "1400", "Kum")
        self.assertEqual(horse["v4Score"], max(0, horse["v4BaseScore"] - 5))
        self.assertTrue(horse["agfAllowedForRanking"])

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

    def test_surface_transition_penalizes_chim_to_kum_switch(self):
        races = [
            {"track": "Çim", "rank": "2"},
            {"track": "Çim", "rank": "3"},
            {"track": "Çim", "rank": "4"},
            {"track": "Çim", "rank": "2"},
            {"track": "Çim", "rank": "1"},
        ]
        result = calculate_surface_transition_score(races, "Kum", 50)
        self.assertLessEqual(result["score"], 25)
        self.assertEqual(result["reason"], "strong_surface_switch_penalty")
        self.assertEqual(result["targetTrackRaceCount"], 0)

    def test_surface_transition_is_lighter_between_kum_and_sentetik(self):
        races = [
            {"track": "Kum", "rank": "2"},
            {"track": "Kum", "rank": "3"},
            {"track": "Kum", "rank": "4"},
            {"track": "Kum", "rank": "2"},
            {"track": "Kum", "rank": "1"},
        ]
        kum_to_sentetik = calculate_surface_transition_score(races, "Sentetik", 50)
        kum_to_cim = calculate_surface_transition_score(races, "Çim", 50)
        self.assertGreater(kum_to_sentetik["score"], kum_to_cim["score"])

    def test_surface_transition_allows_proven_target_surface(self):
        races = [
            {"track": "Kum", "rank": "2"},
            {"track": "Çim", "rank": "5"},
            {"track": "Kum", "rank": "3"},
            {"track": "Çim", "rank": "4"},
        ]
        result = calculate_surface_transition_score(races, "Kum", 70)
        self.assertGreaterEqual(result["score"], 55)
        self.assertNotIn("switch_penalty", result["reason"])

    def test_distance_transition_penalizes_big_jump_without_history(self):
        races = [
            {"distance": "1200", "rank": "2"},
            {"distance": "1300", "rank": "3"},
            {"distance": "1200", "rank": "1"},
            {"distance": "1400", "rank": "4"},
        ]
        result = calculate_distance_transition_score(races, "2000", 50)
        self.assertLess(result["score"], 35)
        self.assertEqual(result["similarDistanceRaceCount"], 0)

    def test_handikap_agf_is_allowed_without_redistribution(self):
        profile = extract_v4_race_profile("Handikap 15", "1400", "Kum", 10)
        weights, allowed = _v417_apply_agf_policy(profile, {
            "agf_score": 20,
            "degree_avg": 10,
            "training_fitness": 10,
            "pace_score": 10,
        })
        self.assertTrue(allowed)
        self.assertEqual(weights["agf_score"], 20)
        self.assertEqual(weights["degree_avg"], 10)
        self.assertEqual(weights["pace_score"], 10)

    def test_handikap_agf_value_is_capped_in_score(self):
        weights = {"agf_score": 1.0}
        self.assertEqual(
            calculate_v4_shadow_score(
                {"agf_score": 100, "_has_agf": True, "_v4_handikap_agf_capped": True},
                weights,
            ),
            82.0,
        )
        self.assertEqual(
            calculate_v4_shadow_score(
                {"agf_score": 10, "_has_agf": True, "_v4_handikap_agf_capped": True},
                weights,
            ),
            35.0,
        )


if __name__ == "__main__":
    unittest.main()
