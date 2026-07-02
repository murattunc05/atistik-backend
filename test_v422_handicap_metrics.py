import unittest

from api_server import (
    _parse_rank_value,
    _v417_apply_agf_policy,
    calculate_v422_handicap_candidate_score,
    calculate_recent_finish_position_score,
    calculate_handicap_age_curve_scores,
    calculate_handicap_load_value_score,
    calculate_late_start_risk_score,
    calculate_start_draw_score,
    calculate_track_condition_suit_score,
    calculate_weight_change_risk_score,
)


class V422HandicapMetricTests(unittest.TestCase):
    def test_handicap_load_value_rewards_hp_for_manageable_weight(self):
        high_value = calculate_handicap_load_value_score(60, 54, [30, 45, 60], [52, 54, 60], 1800)
        low_value = calculate_handicap_load_value_score(35, 60, [30, 45, 60], [52, 54, 60], 1800)
        self.assertGreater(high_value, 60)
        self.assertLess(low_value, 40)

    def test_weight_change_risk_is_risk_only(self):
        self.assertLess(calculate_weight_change_risk_score("58", "54", 2000, 50), 40)
        self.assertGreater(calculate_weight_change_risk_score("52", "56", 2000, 60), 50)
        self.assertEqual(calculate_weight_change_risk_score("", "56", 2000, 60), 50)

    def test_age_curve_is_not_old_linear(self):
        curves = calculate_handicap_age_curve_scores(3, [3, 4, 5, 6])
        self.assertEqual(curves["selectedCurve"], "young_middle_blend")
        self.assertGreater(curves["youngEdge"], curves["oldEdge"])
        self.assertGreater(curves["selected"], 50)

    def test_start_draw_and_late_start_are_guarded(self):
        self.assertEqual(calculate_start_draw_score("", 12, 1200, "Kum"), 50)
        self.assertGreater(calculate_start_draw_score("1", 12, 1200, "Kum"), 50)
        self.assertLess(calculate_start_draw_score("12", 12, 1200, "Cim"), 50)
        self.assertLess(calculate_late_start_risk_score([{"lateStart": "Gec cikti"}]), 50)
        self.assertEqual(calculate_late_start_risk_score([{"notes": "Disa cikti"}]), 50)

    def test_recent_finish_position_uses_finish_ranks(self):
        self.assertEqual(_parse_rank_value({"rank": "2/13"}), 2)
        good_recent = calculate_recent_finish_position_score([
            {"rank": "1"},
            {"rank": "2"},
            {"rank": "3"},
            {"rank": "5"},
        ])
        poor_recent = calculate_recent_finish_position_score([
            {"rank": "8"},
            {"rank": "7"},
            {"rank": "6"},
            {"rank": "5"},
        ])
        self.assertGreater(good_recent, 75)
        self.assertLess(poor_recent, 45)

    def test_track_condition_suit_neutral_without_target_condition(self):
        races = [
            {"trackCondition": "Normal", "rank": "1"},
            {"trackCondition": "Islak", "rank": "6"},
        ]
        self.assertEqual(calculate_track_condition_suit_score(races, ""), 50)
        self.assertGreater(calculate_track_condition_suit_score(races, "Normal"), 50)

    def test_visible_v421_handicap_agf_policy_stays_unchanged(self):
        profile = {"category": "HANDIKAP", "subtype": "HANDIKAP15"}
        weights, allowed = _v417_apply_agf_policy(profile, {"agf_score": 10.0})
        self.assertFalse(allowed)
        self.assertEqual(weights["agf_score"], 0.0)
        self.assertGreater(weights["pace_score"], 0.0)
        self.assertGreater(weights["handicap_efficiency_score"], 0.0)
        self.assertGreater(weights["handicap_class_transition_score"], 0.0)
        self.assertEqual(weights["handicap_load_value_score"], 0.0)

    def test_v422_candidate_is_agf_free_and_uses_new_handicap_metrics(self):
        source_flags = {
            "_has_recent_finish_position": True,
            "_has_distance_transition": True,
            "_has_surface_switch_safety": True,
            "_has_handicap_load_value": True,
            "_has_training": True,
            "_has_handicap_age_curve": True,
            "_has_hp": True,
            "_has_jockey": True,
        }
        high = {
            **source_flags,
            "form_trend": 70.0,
            "recent_finish_position_score": 70.0,
            "distance_transition_score": 70.0,
            "surface_switch_safety_score": 70.0,
            "handicap_load_value_score": 70.0,
            "training_fitness": 70.0,
            "handicap_age_curve_score": 70.0,
            "hp_score": 70.0,
            "jockey_score": 70.0,
            "agf_score": 5.0,
        }
        low = {
            **source_flags,
            "form_trend": 30.0,
            "recent_finish_position_score": 30.0,
            "distance_transition_score": 30.0,
            "surface_switch_safety_score": 30.0,
            "handicap_load_value_score": 30.0,
            "training_fitness": 30.0,
            "handicap_age_curve_score": 30.0,
            "hp_score": 30.0,
            "jockey_score": 30.0,
            "agf_score": 95.0,
        }
        self.assertGreater(
            calculate_v422_handicap_candidate_score(high),
            calculate_v422_handicap_candidate_score(low),
        )


if __name__ == "__main__":
    unittest.main()
