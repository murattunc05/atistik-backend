import unittest

from evaluate_v418_agf_free import (
    build_acceptance_checks,
    collect_agf_weight_violations,
    full_order_guardrails,
    gate_has_blocking_failure,
)


class V418EvaluatorTest(unittest.TestCase):
    def test_full_order_guardrails_normalize_finish_positions(self):
        rows = [
            {"finish_pos": 10},
            {"finish_pos": 20},
            {"finish_pos": 30},
        ]
        mae, rho, ndcg5 = full_order_guardrails(rows, rows)
        self.assertEqual(mae, 0)
        self.assertEqual(rho, 1.0)
        self.assertGreater(ndcg5, 0.99)

    def test_gate_failure_modes(self):
        checks = [
            {"check": "ok", "status": "PASS", "evidence": ""},
            {"check": "watch", "status": "WARN", "evidence": ""},
            {"check": "sample", "status": "REVIEW", "evidence": ""},
        ]
        self.assertFalse(gate_has_blocking_failure(checks))
        self.assertTrue(gate_has_blocking_failure(checks, strict=True))
        self.assertTrue(gate_has_blocking_failure([
            {"check": "bad", "status": "FAIL", "evidence": ""}
        ]))

    def test_agf_violation_detection_allows_only_allowed_profiles(self):
        races = [[
            {
                "race_id": "allowed",
                "race_type": "Maiden",
                "_sim_v418_profile": {"category": "MAIDEN"},
                "_sim_v418_resolved": {
                    "agfAllowedForRanking": True,
                    "weights": {"agf_score": 0.22},
                },
            },
            {
                "race_id": "blocked",
                "race_type": "Handikap 15",
                "_sim_v418_profile": {"category": "HANDIKAP"},
                "_sim_v418_resolved": {
                    "agfAllowedForRanking": False,
                    "weights": {"agf_score": 0.10},
                },
            },
        ]]

        violations = collect_agf_weight_violations(races)

        self.assertEqual(len(violations), 1)
        self.assertEqual(violations[0][0], "blocked")

    def test_acceptance_checks_mark_low_sample_review(self):
        races = []
        for index in range(2):
            rows = [
                {
                    "finish_pos": 1,
                    "rank_pred": 1,
                    "v4_rank": 1,
                    "_sim_v418_score": 90,
                    "_sim_v418_profile": {"category": "GRUP"},
                    "_sim_v418_resolved": {
                        "agfAllowedForRanking": False,
                        "weights": {"agf_score": 0},
                    },
                    "features": {"agf_score": 40},
                },
                {
                    "finish_pos": 2,
                    "rank_pred": 2,
                    "v4_rank": 2,
                    "_sim_v418_score": 70,
                    "_sim_v418_profile": {"category": "GRUP"},
                    "_sim_v418_resolved": {
                        "agfAllowedForRanking": False,
                        "weights": {"agf_score": 0},
                    },
                    "features": {"agf_score": 30},
                },
            ]
            races.append(rows)

        checks = build_acceptance_checks(races, {"GRUP": races}, [])
        review = [item for item in checks if item["check"] == "GRUP segment gate"]
        self.assertEqual(review[0]["status"], "REVIEW")


if __name__ == "__main__":
    unittest.main()
