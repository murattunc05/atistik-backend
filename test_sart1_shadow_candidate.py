import copy
import unittest

from api_server import (
    _SART1_SHADOW_AGF_CAP,
    _SART1_SHADOW_MIN_AGF_COVERAGE,
    _SART1_SHADOW_OBSERVATION_START,
    _SART1_SHADOW_VERSION,
    _preserve_sart1_candidate_snapshot,
    attach_sart1_shadow_candidate,
    calculate_v4_shadow_score,
    extract_v4_race_profile,
    resolve_sart1_shadow_candidate_weights,
    resolve_v4_profile_weights,
)


def horse(name, training, trainer, pedigree, agf, has_agf=True, penalty=0.0):
    return {
        "name": name,
        "aiScore": 61.0,
        "rank": 2,
        "v4Score": 61.0,
        "v4Rank": 2,
        "v4AppliedForRanking": True,
        "v4PenaltyTotal": penalty,
        "_mf": {
            "training_fitness": training,
            "trainer_score": trainer,
            "pedigree": pedigree,
            "agf_score": agf,
            "pace_score": 55.0,
            "_has_training": True,
            "_has_training_times": True,
            "_has_trainer": True,
            "_has_pedigree": True,
            "_has_agf": has_agf,
            "_has_hp": False,
            "_has_weight": False,
            "_has_jockey": False,
            "_has_age": False,
            "_has_track_experience": False,
            "_has_surface_transition": False,
            "_has_distance_transition": False,
        },
    }


class Sart1ShadowCandidateTests(unittest.TestCase):
    def test_frozen_candidate_weights_keep_agf_at_ten_percent(self):
        weights = resolve_sart1_shadow_candidate_weights(
            agf_enabled=True,
        )
        self.assertEqual(_SART1_SHADOW_VERSION, "sart1-bounded-top3-20260804-v1")
        self.assertEqual(_SART1_SHADOW_AGF_CAP, 0.10)
        self.assertAlmostEqual(sum(weights.values()), 1.0, places=3)
        self.assertAlmostEqual(weights["agf_score"], 0.10, places=3)
        self.assertAlmostEqual(weights["training_fitness"], 0.1129, places=3)
        self.assertAlmostEqual(weights["trainer_score"], 0.0468, places=3)
        self.assertAlmostEqual(weights["pedigree"], 0.1586, places=3)

    def test_candidate_stays_inside_existing_weight_movement_gate(self):
        profile = extract_v4_race_profile("Şartlı 1", "1200", "Çim", 10)
        visible = resolve_v4_profile_weights(profile)["weights"]
        candidate = resolve_sart1_shadow_candidate_weights(agf_enabled=True)
        deltas = [
            abs(candidate.get(key, 0.0) - visible.get(key, 0.0))
            for key in set(candidate) | set(visible)
        ]
        self.assertLessEqual(max(deltas), 0.10)
        self.assertLessEqual(sum(deltas), 0.20)

    def test_agf_below_threshold_is_disabled_for_every_runner(self):
        self.assertEqual(_SART1_SHADOW_MIN_AGF_COVERAGE, 0.80)
        runners = [
            horse("A", 90, 60, 65, 80, True),
            horse("B", 85, 55, 60, 70, True),
            horse("C", 80, 50, 58, 60, True),
            horse("D", 75, 45, 55, 50, False),
            horse("E", 70, 40, 52, 40, False),
        ]
        original_metrics = copy.deepcopy([item["_mf"] for item in runners])

        attach_sart1_shadow_candidate(runners, "Şartlı 1", "1200", "Çim")

        self.assertTrue(all(not item["sart1CandidateAgfApplied"] for item in runners))
        self.assertTrue(all("agf_score" not in item["sart1CandidateWeights"] for item in runners))
        self.assertEqual([item["_mf"] for item in runners], original_metrics)

    def test_eighty_percent_agf_uses_neutral_value_for_missing_runner(self):
        runners = [
            horse("A", 90, 60, 65, 80, True),
            horse("B", 85, 55, 60, 70, True),
            horse("C", 80, 50, 58, 60, True),
            horse("D", 75, 45, 55, 50, True),
            horse("E", 70, 40, 52, 5, False, penalty=3.0),
        ]
        missing_original = copy.deepcopy(runners[-1]["_mf"])

        attach_sart1_shadow_candidate(runners, "Şartlı 1", "1200", "Çim")

        missing = runners[-1]
        self.assertTrue(missing["sart1CandidateAgfApplied"])
        self.assertEqual(missing["sart1CandidateAgfCoverage"], 0.8)
        self.assertEqual(missing["sart1CandidateWeights"]["agf_score"], 10.0)
        expected_metrics = dict(missing_original)
        expected_metrics["agf_score"] = 50.0
        expected_metrics["_has_agf"] = True
        expected_weights = resolve_sart1_shadow_candidate_weights(
            agf_enabled=True,
        )
        expected_base = calculate_v4_shadow_score(expected_metrics, expected_weights)
        self.assertEqual(missing["sart1CandidateBaseScore"], expected_base)
        self.assertEqual(
            missing["sart1CandidateScore"],
            round(expected_base - 3.0, 1),
        )
        self.assertEqual(missing["_mf"], missing_original)
        self.assertIsNotNone(missing["sart1CandidateNoAgfScore"])
        self.assertIsNotNone(missing["sart1CandidateNoAgfRank"])

    def test_candidate_never_changes_visible_fields(self):
        runners = [
            horse("HIGH", 98, 88, 60, 70, True),
            horse("LOW", 55, 30, 55, 20, True),
        ]
        visible_before = [
            {
                key: item[key]
                for key in ("aiScore", "rank", "v4Score", "v4Rank", "v4AppliedForRanking")
            }
            for item in runners
        ]

        attach_sart1_shadow_candidate(runners, "Şartlı 1", "1200", "Çim")

        visible_after = [
            {
                key: item[key]
                for key in ("aiScore", "rank", "v4Score", "v4Rank", "v4AppliedForRanking")
            }
            for item in runners
        ]
        self.assertEqual(visible_after, visible_before)
        self.assertEqual(runners[0]["sart1CandidateRank"], 1)
        self.assertTrue(all(not item["sart1CandidateUsedForRanking"] for item in runners))
        self.assertTrue(all(not item["sart1CandidateRolloutEligible"] for item in runners))

    def test_non_sart1_profile_is_untouched(self):
        runners = [horse("A", 90, 60, 65, 80, True)]
        attach_sart1_shadow_candidate(runners, "Şartlı 3", "1200", "Çim")
        self.assertNotIn("sart1CandidateVersion", runners[0])

    def test_empty_metric_runner_stays_at_zero(self):
        runners = [horse("SOURCED", 90, 60, 65, 80, True)]
        runners.append({
            "name": "EMPTY",
            "aiScore": 0.0,
            "rank": 2,
            "v4Score": 0.0,
            "v4Rank": 2,
            "v4Version": "4.24",
            "v4AppliedForRanking": True,
            "v4PenaltyTotal": 0.0,
            "_mf": {},
        })

        attach_sart1_shadow_candidate(runners, "Şartlı 1", "1200", "Çim")

        empty = runners[1]
        self.assertEqual(empty["sart1CandidateBaseScore"], 0.0)
        self.assertEqual(empty["sart1CandidateScore"], 0.0)
        self.assertEqual(empty["sart1CandidateObservationStart"], _SART1_SHADOW_OBSERVATION_START)

    def test_same_version_candidate_snapshot_is_immutable_on_retry(self):
        previous = {
            "sart1_candidate_version": _SART1_SHADOW_VERSION,
            "sart1_candidate_rank": 4,
            "sart1_candidate_baseline_rank": 2,
            "sart1_candidate_score": 51.0,
        }
        current = {
            "sart1_candidate_version": _SART1_SHADOW_VERSION,
            "sart1_candidate_rank": 1,
            "sart1_candidate_baseline_rank": 1,
            "sart1_candidate_score": 70.0,
        }

        preserved = _preserve_sart1_candidate_snapshot(current, previous)

        self.assertEqual(preserved["sart1_candidate_rank"], 4)
        self.assertEqual(preserved["sart1_candidate_baseline_rank"], 2)
        self.assertEqual(preserved["sart1_candidate_score"], 51.0)

        unavailable = {
            "sart1_candidate_version": None,
            "sart1_candidate_mode": "unavailable",
            "sart1_candidate_rank": None,
        }
        preserved_unavailable = _preserve_sart1_candidate_snapshot(
            unavailable,
            previous,
        )
        self.assertEqual(
            preserved_unavailable["sart1_candidate_version"],
            _SART1_SHADOW_VERSION,
        )
        self.assertEqual(preserved_unavailable["sart1_candidate_rank"], 4)


if __name__ == "__main__":
    unittest.main()
