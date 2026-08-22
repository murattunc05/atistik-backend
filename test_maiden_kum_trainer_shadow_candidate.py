import copy
import unittest
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from api_server import (
    _MAIDEN_KUM_TRAINER_SHADOW_VERSION,
    _preserve_maiden_kum_trainer_candidate_snapshot,
    app,
    attach_maiden_kum_trainer_candidate,
    maiden_kum_trainer_candidate_log_fields,
    resolve_maiden_kum_trainer_candidate_weights,
)


PROSPECTIVE_TS = int(
    datetime(2026, 8, 23, 10, 0, tzinfo=ZoneInfo("Europe/Istanbul")).timestamp()
)


def runner(name, score, rank, pace, trainer, *, source=True):
    return {
        "name": name,
        "aiScore": score,
        "rank": rank,
        "v4Score": score,
        "v4Rank": rank,
        "v4PenaltyTotal": 0.0,
        "v4Version": "4.25",
        "v4AppliedForRanking": True,
        "v4Profile": {
            "category": "MAIDEN",
            "subtype": "MAIDEN",
            "distanceBucket": "mid",
            "fieldBucket": "small",
            "track": "Kum",
            "profileKey": "MAIDEN|mid|small|Kum",
            "selectedKey": "MAIDEN|Kum",
            "fallbackLevel": "category_track",
        },
        "v4Weights": {"pace_score": 100.0},
        "metricSourceFlags": {"hasTrainer": source},
        "_mf": {
            "pace_score": pace,
            "trainer_score": trainer,
            "_has_trainer": source,
        },
    }


def attach(rows, *, race_type="Maiden", track="Kum", race_date="23.08.2026"):
    with patch("api_server.time.time", return_value=PROSPECTIVE_TS):
        return attach_maiden_kum_trainer_candidate(
            rows,
            race_type=race_type,
            distance="1400",
            track=track,
            race_date=race_date,
            race_time="18:00",
        )


class MaidenKumTrainerCandidateTest(unittest.TestCase):
    def setUp(self):
        self.rows = [
            runner("A", 90.0, 1, 90.0, 20.0),
            runner("B", 80.0, 2, 80.0, 20.0),
            runner("C", 70.0, 3, 70.0, 20.0),
            runner("D", 69.5, 4, 69.5, 100.0),
        ]

    def test_scope_formula_and_visible_ranking_are_frozen(self):
        before = [
            (row["aiScore"], row["rank"], row["v4Score"], row["v4Rank"])
            for row in self.rows
        ]
        attach(self.rows)
        after = [
            (row["aiScore"], row["rank"], row["v4Score"], row["v4Rank"])
            for row in self.rows
        ]
        self.assertEqual(before, after)
        self.assertEqual(self.rows[3]["maidenKumTrainerCandidateRank"], 3)
        self.assertEqual(self.rows[2]["maidenKumTrainerCandidateRank"], 4)
        for row in self.rows:
            self.assertEqual(
                row["maidenKumTrainerCandidateVersion"],
                _MAIDEN_KUM_TRAINER_SHADOW_VERSION,
            )
            self.assertFalse(row["maidenKumTrainerCandidateUsedForRanking"])
            self.assertFalse(row["maidenKumTrainerCandidateTelegramVisible"])
            self.assertFalse(row["maidenKumTrainerCandidateRolloutEligible"])
            self.assertTrue(row["maidenKumTrainerCandidateFormalReplayOnly"])
            self.assertTrue(row["maidenKumTrainerCandidateMl15OverlapNonAdditive"])

    def test_only_maiden_kum_and_prospective_pre_race_attach(self):
        for kwargs in (
            {"race_type": "Şartlı 4"},
            {"track": "Çim"},
            {"race_date": "22.08.2026"},
        ):
            rows = copy.deepcopy(self.rows)
            attach(rows, **kwargs)
            self.assertNotIn("maidenKumTrainerCandidateVersion", rows[0])
        rows = copy.deepcopy(self.rows)
        with patch("api_server.time.time", return_value=PROSPECTIVE_TS + 9 * 3600):
            attach_maiden_kum_trainer_candidate(
                rows,
                race_type="Maiden",
                distance="1400",
                track="Kum",
                race_date="23.08.2026",
                race_time="18:00",
            )
        self.assertNotIn("maidenKumTrainerCandidateVersion", rows[0])

    def test_trainer_source_guard_disagreement_fails_closed(self):
        self.rows[0]["_mf"]["_has_trainer"] = False
        with self.assertRaisesRegex(ValueError, "hasTrainer"):
            attach(self.rows)
        rows = copy.deepcopy(self.rows)
        rows[0]["_mf"].pop("_has_trainer")
        with self.assertRaisesRegex(ValueError, "hasTrainer"):
            attach(rows)

    def test_neutral_only_race_is_not_evidence_eligible(self):
        rows = [runner(f"H{i}", 90 - i, i, 90 - i, 50.0) for i in range(1, 5)]
        attach(rows)
        self.assertTrue(all(not row["maidenKumTrainerCandidateRaceEvidenceEligible"] for row in rows))
        self.assertEqual(rows[0]["maidenKumTrainerCandidateSource"]["actionableCount"], 0)

    def test_log_contract_and_retry_preservation_are_immutable(self):
        attach(self.rows)
        first = maiden_kum_trainer_candidate_log_fields(self.rows[0])
        self.assertEqual(
            first["maiden_kum_trainer_candidate_version"],
            _MAIDEN_KUM_TRAINER_SHADOW_VERSION,
        )
        self.assertFalse(first["maiden_kum_trainer_candidate_used_for_ranking"])
        previous = {**first, "finish_pos": 1}
        replacement = {
            **first,
            "maiden_kum_trainer_candidate_score": -999,
        }
        _preserve_maiden_kum_trainer_candidate_snapshot(replacement, previous)
        self.assertEqual(
            replacement["maiden_kum_trainer_candidate_score"],
            previous["maiden_kum_trainer_candidate_score"],
        )

    def test_weight_formula_is_exact_raw_plus_two(self):
        formula = resolve_maiden_kum_trainer_candidate_weights({"pace_score": 100.0})
        self.assertEqual(formula["candidateRawTotal"], 102.0)
        self.assertAlmostEqual(formula["candidateWeightsPct"]["trainer_score"], 2 / 102 * 100)

        actual_like = resolve_maiden_kum_trainer_candidate_weights({
            "pace_score": 93.6,
            "trainer_score": 6.4,
        })
        self.assertEqual(actual_like["candidateRawWeights"]["trainer_score"], 8.4)
        self.assertEqual(actual_like["candidateRawTotal"], 102.0)
        self.assertAlmostEqual(
            actual_like["candidateWeightsPct"]["trainer_score"],
            8.4 / 102 * 100,
        )
        self.assertAlmostEqual(
            actual_like["candidateWeightsPct"]["pace_score"],
            93.6 / 102 * 100,
        )

    def test_existing_trainer_weight_is_incremented_and_can_rescue_top3(self):
        weights = {"pace_score": 93.6, "trainer_score": 6.4}
        rows = [
            runner("A", 85.52, 1, 90.0, 20.0),
            runner("B", 76.16, 2, 80.0, 20.0),
            runner("C", 68.72, 3, 70.0, 50.0),
            runner("D", 68.596, 4, 68.5, 70.0),
        ]
        for row in rows:
            row["v4Weights"] = weights
        attach(rows)
        self.assertEqual(rows[2]["maidenKumTrainerCandidateRank"], 4)
        self.assertEqual(rows[3]["maidenKumTrainerCandidateRank"], 3)
        self.assertEqual(
            rows[0]["maidenKumTrainerCandidateRawWeights"]["trainer_score"],
            8.4,
        )

    def test_ml_status_exposes_non_live_candidate(self):
        response = app.test_client().get("/api/ml-status")
        self.assertEqual(response.status_code, 200)
        status = response.get_json()["maiden_kum_trainer_shadow"]
        self.assertEqual(status["version"], _MAIDEN_KUM_TRAINER_SHADOW_VERSION)
        self.assertFalse(status["used_for_ranking"])
        self.assertFalse(status["rollout_eligible"])
        self.assertFalse(status["telegram_visible"])
        self.assertTrue(status["historical_evidence"]["maiden_ml15_overlap_non_additive"])


if __name__ == "__main__":
    unittest.main()
