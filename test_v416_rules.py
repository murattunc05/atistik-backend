import unittest

from api_server import (
    _V4_VERSION,
    _v417_apply_agf_policy,
    apply_v421_contextual_metrics,
    apply_v4_shadow_mode,
    calculate_handicap_weight_relief_score,
    calculate_pace_map_edge_score,
    calculate_surface_switch_safety_score,
    calculate_v4_shadow_score,
    calculate_distance_transition_score,
    calculate_surface_transition_score,
    calculate_v4_ranking_penalties,
    extract_v4_race_profile,
    resolve_v4_profile_weights,
)
from train_shadow_ml import feature_dict


class V421RulesTest(unittest.TestCase):
    def test_version(self):
        self.assertEqual(_V4_VERSION, "4.21")

    def test_agf_is_allowed_only_for_maiden_and_sartli_one(self):
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
        self.assertGreater(handicap["weights"]["pace_score"], handicap["weights"]["agf_score"])
        self.assertGreater(handicap["weights"]["form_trend"], 0)
        self.assertGreater(handicap["weights"]["trainer_score"], 0)
        self.assertGreater(handicap["weights"]["handicap_efficiency_score"], 0)

    def test_core_profile_weights_are_normalized_and_agf_policy_is_explicit(self):
        cases = [
            ("Handikap 16", "Kum", False, 0.0),
            ("KV-6", "Cim", False, 0.0),
            ("Grup 2", "Cim", False, 0.0),
            ("Satis 3", "Kum", False, 0.0),
            ("Sartli 4", "Kum", False, 0.0),
            ("Maiden", "Kum", True, 16.0 / 93.0),
            ("Sartli 1", "Kum", True, 0.18),
        ]
        for race_type, track, agf_allowed, expected_agf_weight in cases:
            with self.subTest(race_type=race_type):
                resolved = resolve_v4_profile_weights(
                    extract_v4_race_profile(race_type, "1400", track, 10)
                )
                self.assertEqual(resolved["agfAllowedForRanking"], agf_allowed)
                self.assertAlmostEqual(sum(resolved["weights"].values()), 1.0, places=3)
                self.assertAlmostEqual(
                    resolved["weights"].get("agf_score", 0.0),
                    expected_agf_weight,
                    places=4,
                )

    def test_maiden_profile_uses_training_degree_heavy_revision(self):
        resolved = resolve_v4_profile_weights(
            extract_v4_race_profile("Maiden", "1200", "Cim", 8)
        )

        self.assertEqual(resolved["selectedKey"], "MAIDEN")
        self.assertTrue(resolved["agfAllowedForRanking"])
        self.assertAlmostEqual(resolved["weights"]["agf_score"], 16.0 / 93.0, places=4)
        self.assertAlmostEqual(resolved["weights"]["training_fitness"], 8.0 / 93.0, places=4)
        self.assertAlmostEqual(resolved["weights"]["training_degree_score"], 17.0 / 93.0, places=4)
        self.assertGreater(
            resolved["weights"]["training_degree_score"],
            resolved["weights"]["training_fitness"],
        )
        self.assertAlmostEqual(resolved["weights"]["trainer_score"], 7.0 / 93.0, places=4)

    def test_special_handikap_profiles_are_normalized_without_changing_ratios(self):
        kum = resolve_v4_profile_weights(
            extract_v4_race_profile("Handikap 15", "1500", "Kum", 12)
        )
        cim = resolve_v4_profile_weights(
            extract_v4_race_profile("Handikap 15", "1600", "Cim", 12)
        )

        self.assertAlmostEqual(sum(kum["weights"].values()), 1.0, places=3)
        self.assertAlmostEqual(kum["weights"]["pace_score"], 0.30, places=6)
        self.assertEqual(kum["weights"].get("agf_score", 0.0), 0)

        self.assertAlmostEqual(sum(cim["weights"].values()), 1.0, places=3)
        self.assertAlmostEqual(cim["weights"]["form_trend"], 24.0 / 90.0, places=4)
        self.assertAlmostEqual(cim["weights"]["distance_suit"], 10.0 / 90.0, places=4)
        self.assertEqual(cim["weights"].get("agf_score", 0.0), 0)

    def test_mojibake_sartli_profiles_as_sartli(self):
        profile = extract_v4_race_profile("ŢARTLI 4/DHÖW", "1400", "Kum", 10)
        self.assertEqual(profile["category"], "SARTLI")
        self.assertEqual(profile["subtype"], "SART4")

    def test_handikap15_kum_uses_special_agf_free_profile(self):
        resolved = resolve_v4_profile_weights(
            extract_v4_race_profile("Handikap 15", "1500", "Kum", 12)
        )
        self.assertEqual(resolved["selectedKey"], "HANDIKAP15|Kum")
        self.assertFalse(resolved["agfAllowedForRanking"])
        self.assertEqual(resolved["weights"]["agf_score"], 0)
        self.assertGreater(resolved["weights"]["pace_score"], 0.25)
        self.assertGreater(resolved["weights"]["running_style_proxy_score"], 0.10)

    def test_handikap15_cim_uses_special_agf_free_profile(self):
        resolved = resolve_v4_profile_weights(
            extract_v4_race_profile("Handikap 15", "1600", "Cim", 12)
        )
        self.assertEqual(resolved["selectedKey"], "HANDIKAP15|Cim")
        self.assertFalse(resolved["agfAllowedForRanking"])
        self.assertEqual(resolved["weights"]["agf_score"], 0)
        self.assertGreater(resolved["weights"]["form_trend"], resolved["weights"]["pace_score"])
        self.assertGreater(resolved["weights"]["distance_suit"], 0.10)
        self.assertEqual(resolved["weights"]["handicap_class_transition_score"], 0)
        self.assertEqual(resolved["weights"]["training_fitness"], 0)

    def test_handikap16_kum_uses_v419_profile_without_distance_overconfidence(self):
        resolved = resolve_v4_profile_weights(
            extract_v4_race_profile("Handikap 16", "1500", "Kum", 12)
        )
        self.assertEqual(resolved["selectedKey"], "HANDIKAP16|Kum")
        self.assertFalse(resolved["agfAllowedForRanking"])
        self.assertEqual(resolved["weights"]["agf_score"], 0)
        self.assertEqual(resolved["weights"]["handicap_class_transition_score"], 0)
        self.assertEqual(resolved["weights"]["distance_suit"], 0)
        self.assertGreater(resolved["weights"]["pace_score"], resolved["weights"]["handicap_efficiency_score"])


    def test_kv_profile_uses_agf_free_hp_calibration(self):
        resolved = resolve_v4_profile_weights(
            extract_v4_race_profile("KV-6", "1600", "Cim", 8)
        )
        self.assertEqual(resolved["selectedKey"], "KV")
        self.assertFalse(resolved["agfAllowedForRanking"])
        self.assertEqual(resolved["weights"]["agf_score"], 0)
        self.assertGreater(resolved["weights"]["hp_score"], resolved["weights"]["degree_avg"])
        self.assertEqual(resolved["weights"]["weight_impact"], 0)
        self.assertEqual(resolved["weights"]["training_degree_score"], 0)

    def test_sartli_two_plus_profiles_use_v420_subtype_weights(self):
        resolved = resolve_v4_profile_weights(
            extract_v4_race_profile("Sartli 4", "1400", "Kum", 10)
        )
        self.assertEqual(resolved["selectedKey"], "SART4")
        self.assertFalse(resolved["agfAllowedForRanking"])
        self.assertEqual(resolved["weights"]["agf_score"], 0)
        self.assertAlmostEqual(resolved["weights"]["hp_score"], 0.2838, places=4)
        self.assertAlmostEqual(resolved["weights"]["trainer_score"], 0.1591, places=4)
        self.assertGreater(resolved["weights"]["distance_suit"], resolved["weights"]["form_trend"])

        sart3 = resolve_v4_profile_weights(
            extract_v4_race_profile("Sartli 3", "1400", "Kum", 10)
        )
        self.assertEqual(sart3["selectedKey"], "SART3")
        self.assertAlmostEqual(sart3["weights"]["form_trend"], 0.2824, places=4)
        self.assertGreater(sart3["weights"]["track_experience_score"], 0.15)

        sart5 = resolve_v4_profile_weights(
            extract_v4_race_profile("Sartli 5", "1400", "Kum", 10)
        )
        self.assertEqual(sart5["selectedKey"], "SART5")
        self.assertAlmostEqual(sart5["weights"]["form_trend"], 0.3047, places=4)
        self.assertGreater(sart5["weights"]["track_suit"], 0.13)


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

    def test_handikap_agf_is_removed_and_redistributed(self):
        profile = extract_v4_race_profile("Handikap 15", "1400", "Kum", 10)
        weights, allowed = _v417_apply_agf_policy(profile, {
            "agf_score": 20,
            "degree_avg": 10,
            "training_fitness": 10,
            "pace_score": 10,
        })
        self.assertFalse(allowed)
        self.assertEqual(weights["agf_score"], 0)
        self.assertEqual(weights["degree_avg"], 10)
        self.assertGreater(weights["pace_score"], 10)
        self.assertGreater(weights["handicap_efficiency_score"], 0)
        self.assertGreater(weights["handicap_class_transition_score"], 0)

    def test_handikap_score_ignores_agf_value_after_policy(self):
        profile = extract_v4_race_profile("Handikap 15", "1400", "Kum", 10)
        resolved = resolve_v4_profile_weights(profile)
        low_agf_metrics = {
            "pace_score": 60,
            "pace_map_edge_score": 60,
            "field_relative_value_score": 60,
            "handicap_weight_relief_score": 60,
            "surface_switch_safety_score": 60,
            "favorite_risk_guard_score": 60,
            "handicap_efficiency_score": 60,
            "handicap_class_transition_score": 60,
            "form_trend": 60,
            "distance_suit": 60,
            "distance_transition_score": 60,
            "surface_transition_score": 60,
            "weight_impact": 60,
            "degree_avg": 60,
            "degree_trend": 60,
            "training_fitness": 60,
            "training_degree_score": 60,
            "jockey_score": 60,
            "trainer_score": 60,
            "bounce_score": 60,
            "hp_score": 60,
            "track_experience_score": 60,
            "agf_score": 0,
            "_has_agf": True,
            "_has_pace_map_edge": True,
            "_has_field_relative_value": True,
            "_has_handicap_weight_relief": True,
            "_has_surface_switch_safety": True,
            "_has_favorite_risk_guard": True,
        }
        high_agf_metrics = dict(low_agf_metrics)
        high_agf_metrics["agf_score"] = 100
        self.assertFalse(resolved["agfAllowedForRanking"])
        self.assertEqual(
            calculate_v4_shadow_score(low_agf_metrics, resolved["weights"]),
            calculate_v4_shadow_score(high_agf_metrics, resolved["weights"]),
        )

    def test_v421_handicap_weight_relief_rewards_hp_value_not_raw_weight(self):
        all_hps = [35, 45, 55, 65, 75]
        all_weights = [50, 52, 54, 56, 58]
        efficient = calculate_handicap_weight_relief_score(65, 52, all_hps, all_weights)
        burdened = calculate_handicap_weight_relief_score(65, 58, all_hps, all_weights)
        self.assertGreater(efficient, burdened)
        self.assertGreaterEqual(efficient, 50)

    def test_v421_surface_switch_safety_penalizes_unproven_surface_switch(self):
        unsafe = calculate_surface_switch_safety_score({
            "score": 45,
            "targetTrackRaceCount": 0,
            "otherTrackRaceCount": 5,
            "dominantTrack": "Cim",
            "dominantTrackShare": 0.9,
            "lastTrack": "Cim",
            "lastThreeTargetCount": 0,
        }, 45)
        safe = calculate_surface_switch_safety_score({
            "score": 65,
            "targetTrackRaceCount": 4,
            "otherTrackRaceCount": 2,
            "dominantTrack": "Kum",
            "dominantTrackShare": 0.7,
            "lastTrack": "Kum",
            "lastThreeTargetCount": 2,
        }, 70)
        self.assertLess(unsafe, 40)
        self.assertGreater(safe, unsafe)

    def test_v421_pace_map_edge_rewards_lone_speed_and_hot_pace_closer(self):
        self.assertGreater(
            calculate_pace_map_edge_score("KAÇAK", "YAVAŞ", 65, 8),
            calculate_pace_map_edge_score("BEKLEME", "YAVAŞ", 45, 8),
        )
        self.assertGreater(
            calculate_pace_map_edge_score("BEKLEME", "HIZLI", 65, 60),
            calculate_pace_map_edge_score("KAÇAK", "HIZLI", 40, 60),
        )

    def test_v421_contextual_metrics_attach_without_agf_for_handicap_and_grup(self):
        horses = [
            {
                "name": "A",
                "_mf": {
                    "form_trend": 70, "hp_score": 65, "degree_avg": 80,
                    "distance_suit": 60, "surface_transition_score": 35,
                    "weight_impact": 55, "handicap_efficiency_score": 45,
                    "handicap_weight_relief_score": 50, "pace_score": 55,
                    "pace_pressure": 35, "jockey_score": 70, "agf_score": 100,
                    "distance_transition_score": 50,
                },
                "metricSourceFlags": {
                    "hasSurfaceTransition": True,
                    "targetTrackRaceCount": 0,
                    "otherTrackRaceCount": 4,
                },
                "paceInfo": {"runningStyle": "KAÇAK", "paceScenario": "HIZLI"},
                "scoreBreakdown": {},
            },
            {
                "name": "B",
                "_mf": {
                    "form_trend": 50, "hp_score": 50, "degree_avg": 50,
                    "distance_suit": 50, "surface_transition_score": 65,
                    "weight_impact": 50, "handicap_efficiency_score": 60,
                    "handicap_weight_relief_score": 64, "pace_score": 65,
                    "pace_pressure": 35, "jockey_score": 50, "agf_score": 10,
                    "distance_transition_score": 60,
                },
                "metricSourceFlags": {"hasSurfaceTransition": True, "targetTrackRaceCount": 3},
                "paceInfo": {"runningStyle": "BEKLEME", "paceScenario": "HIZLI"},
                "scoreBreakdown": {},
            },
            {
                "name": "C",
                "_mf": {
                    "form_trend": 40, "hp_score": 40, "degree_avg": 40,
                    "distance_suit": 45, "surface_transition_score": 55,
                    "weight_impact": 50, "handicap_efficiency_score": 50,
                    "handicap_weight_relief_score": 50, "pace_score": 50,
                    "pace_pressure": 35, "jockey_score": 45, "agf_score": 5,
                    "distance_transition_score": 50,
                },
                "metricSourceFlags": {"hasSurfaceTransition": True, "targetTrackRaceCount": 1},
                "paceInfo": {"runningStyle": "TAKİPÇİ", "paceScenario": "HIZLI"},
                "scoreBreakdown": {},
            },
        ]
        apply_v421_contextual_metrics(horses, "Handikap 15")
        self.assertTrue(horses[0]["metricSourceFlags"]["hasFavoriteRiskGuard"])
        self.assertIn("field_relative_value_score", horses[0]["_mf"])
        self.assertLess(horses[0]["_mf"]["favorite_risk_guard_score"], 70)

    def test_shadow_ml_feature_dict_includes_v421_features(self):
        entry = {
            "race_type": "Handikap 15",
            "track": "Kum",
            "distance": "1400",
            "features": {
                "handicap_weight_relief_score": 61,
                "field_relative_value_score": 62,
                "pace_map_edge_score": 63,
                "surface_switch_safety_score": 64,
                "favorite_risk_guard_score": 65,
                "class_peak_score": 66,
                "elite_consensus_score": 67,
            },
            "metric_source_flags": {
                "hasHandicapWeightRelief": True,
                "hasFieldRelativeValue": True,
                "hasPaceMapEdge": True,
                "hasSurfaceSwitchSafety": True,
                "hasFavoriteRiskGuard": True,
                "hasClassPeak": True,
                "hasEliteConsensus": True,
            },
        }
        features = feature_dict(entry)
        self.assertEqual(features["handicap_weight_relief_score"], 61)
        self.assertEqual(features["elite_consensus_score"], 67)
        self.assertEqual(features["has_field_relative_value"], 1.0)


if __name__ == "__main__":
    unittest.main()
