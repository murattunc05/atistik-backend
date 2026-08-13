import copy
import unittest

from api_server import (
    _HANDICAP_TRAINER_SHADOW_OBSERVATION_START,
    _HANDICAP_TRAINER_SHADOW_VERSION,
    _preserve_handicap_trainer_candidate_snapshot,
    app,
    attach_handicap_trainer_ablation_candidate,
    calculate_v4_shadow_score,
    extract_v4_race_profile,
    resolve_v4_profile_weights,
)


def runner(name, v4_score, v4_rank, pace, trainer, penalty=0.0):
    return {
        "name": name,
        "aiScore": v4_score,
        "rank": v4_rank,
        "v4Score": v4_score,
        "v4Rank": v4_rank,
        "v4Version": "4.25",
        "v4AppliedForRanking": True,
        "v4PenaltyTotal": penalty,
        "metricSourceFlags": {"hasTrainer": True},
        "_mf": {
            "pace_score": pace,
            "trainer_score": trainer,
            "_has_trainer": True,
        },
    }


class HandicapTrainerShadowCandidateTests(unittest.TestCase):
    def test_status_exposes_frozen_nonranking_identity(self):
        response = app.test_client().get("/api/ml-status")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()["handicap_trainer_shadow"]
        self.assertEqual(payload["version"], _HANDICAP_TRAINER_SHADOW_VERSION)
        self.assertEqual(payload["observation_start"], "14.08.2026")
        self.assertFalse(payload["used_for_ranking"])
        self.assertEqual(payload["checkpoints"], [5, 10, 15])

    def test_candidate_ablates_exact_selected_profile_trainer_weight(self):
        runners = [
            runner("A", 70.0, 1, 70.0, 20.0),
            runner("B", 65.0, 2, 60.0, 90.0, penalty=1.0),
            runner("C", 60.0, 3, 50.0, 50.0),
        ]
        visible = [
            (item["aiScore"], item["rank"], item["v4Score"], item["v4Rank"])
            for item in runners
        ]
        original_metrics = copy.deepcopy([item["_mf"] for item in runners])

        attach_handicap_trainer_ablation_candidate(
            runners,
            "Handikap 15",
            "1400",
            "Kum",
        )

        profile = extract_v4_race_profile("Handikap 15", "1400", "Kum", 3)
        resolved = resolve_v4_profile_weights(profile)
        expected = dict(resolved["weights"])
        removed = expected.pop("trainer_score", 0.0)
        total = sum(expected.values())
        expected = {key: value / total for key, value in expected.items()}
        self.assertGreater(removed, 0.0)
        self.assertTrue(all("trainer_score" not in item["handicapTrainerCandidateWeights"] for item in runners))
        self.assertAlmostEqual(
            runners[0]["handicapTrainerCandidateRemovedWeightPct"],
            removed * 100.0,
            places=3,
        )
        expected_base = calculate_v4_shadow_score(runners[1]["_mf"], expected)
        self.assertEqual(runners[1]["handicapTrainerCandidateBaseScore"], expected_base)
        self.assertEqual(
            runners[1]["handicapTrainerCandidateScore"],
            round(expected_base - 1.0, 1),
        )
        expected_order = sorted(
            runners,
            key=lambda item: (
                -item["handicapTrainerCandidateScore"],
                item["v4Rank"],
            ),
        )
        self.assertEqual(
            [item["handicapTrainerCandidateRank"] for item in expected_order],
            [1, 2, 3],
        )
        self.assertEqual([item["_mf"] for item in runners], original_metrics)
        self.assertEqual(
            [
                (item["aiScore"], item["rank"], item["v4Score"], item["v4Rank"])
                for item in runners
            ],
            visible,
        )
        self.assertTrue(all(not item["handicapTrainerCandidateUsedForRanking"] for item in runners))
        self.assertTrue(all(not item["handicapTrainerCandidateRolloutEligible"] for item in runners))
        self.assertEqual(
            runners[0]["handicapTrainerCandidateObservationStart"],
            _HANDICAP_TRAINER_SHADOW_OBSERVATION_START,
        )

    def test_non_handicap_race_is_untouched(self):
        runners = [runner("A", 70.0, 1, 70.0, 20.0)]
        before = copy.deepcopy(runners)

        attach_handicap_trainer_ablation_candidate(
            runners,
            "Şartlı 4",
            "1400",
            "Kum",
        )

        self.assertEqual(runners, before)
        self.assertNotIn("handicapTrainerCandidateVersion", runners[0])

    def test_all_live_handicap_profiles_receive_the_separate_ablation(self):
        cases = (
            ("Handikap 14", "Kum"),
            ("Handikap 14", "Çim"),
            ("Handikap 15", "Kum"),
            ("Handikap 15", "Çim"),
            ("Handikap 16", "Kum"),
            ("Handikap 16", "Çim"),
            ("Handikap 17", "Sentetik"),
        )
        for race_type, track in cases:
            with self.subTest(race_type=race_type, track=track):
                runners = [
                    runner("A", 70.0, 1, 70.0, 20.0),
                    runner("B", 60.0, 2, 60.0, 80.0),
                ]
                attach_handicap_trainer_ablation_candidate(
                    runners,
                    race_type,
                    "1400",
                    track,
                )
                removed = runners[0]["handicapTrainerCandidateRemovedWeightPct"]
                self.assertGreaterEqual(removed, 0.85)
                self.assertLessEqual(removed, 2.22)
                self.assertNotIn(
                    "trainer_score",
                    runners[0]["handicapTrainerCandidateWeights"],
                )

    def test_same_version_snapshot_is_immutable_across_retry(self):
        previous = {
            "handicap_trainer_candidate_version": _HANDICAP_TRAINER_SHADOW_VERSION,
            "handicap_trainer_candidate_created_ts": 123,
            "handicap_trainer_candidate_baseline_rank": 4,
            "handicap_trainer_candidate_rank": 3,
            "handicap_trainer_candidate_score": 61.0,
        }
        current = {
            "handicap_trainer_candidate_version": _HANDICAP_TRAINER_SHADOW_VERSION,
            "handicap_trainer_candidate_created_ts": 999,
            "handicap_trainer_candidate_baseline_rank": 1,
            "handicap_trainer_candidate_rank": 1,
            "handicap_trainer_candidate_score": 90.0,
        }

        result = _preserve_handicap_trainer_candidate_snapshot(current, previous)

        self.assertEqual(result["handicap_trainer_candidate_created_ts"], 123)
        self.assertEqual(result["handicap_trainer_candidate_baseline_rank"], 4)
        self.assertEqual(result["handicap_trainer_candidate_rank"], 3)
        self.assertEqual(result["handicap_trainer_candidate_score"], 61.0)


if __name__ == "__main__":
    unittest.main()
