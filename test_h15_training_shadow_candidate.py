import copy
import hashlib
import json
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from automation.h15_training_shadow_monitor import build_report

from api_server import (
    _H15_TRAINING_SHADOW_OBSERVATION_START,
    _H15_TRAINING_SHADOW_VERSION,
    _preserve_h15_training_candidate_snapshot,
    app,
    attach_h15_training_degree_candidate,
    calculate_h15_score_from_replay_totals,
    h15_training_candidate_log_fields,
    resolve_h15_training_candidate_weights,
)


def runner(
    name,
    v4_score,
    v4_rank,
    pace,
    training_degree,
    *,
    source=True,
    version="4.25",
):
    return {
        "name": name,
        "aiScore": v4_score,
        "rank": v4_rank,
        "v4Score": v4_score,
        "v4Rank": v4_rank,
        "v4BaseScore": v4_score,
        "v4PenaltyTotal": 0.0,
        "v4Version": version,
        "v4AppliedForRanking": True,
        "v4Profile": {
            "category": "HANDIKAP",
            "subtype": "HANDIKAP15",
            "distanceBucket": "mid",
            "fieldBucket": "small",
            "track": "Kum",
            "profileKey": "HANDIKAP15|mid|small|Kum",
            "selectedKey": "HANDIKAP15|Kum",
            "fallbackLevel": "subtype_track",
        },
        # The historical candidate was evaluated from this exported percentage
        # map. training_degree_score is intentionally absent/zero here.
        "v4Weights": {"pace_score": 100.0},
        "metricSourceFlags": {"hasTrainingTimes": source},
        "_mf": {
            "pace_score": pace,
            "training_degree_score": training_degree,
            "_has_training_times": source,
        },
    }


PROSPECTIVE_CREATED_TS = int(
    datetime(
        2026,
        8,
        21,
        10,
        0,
        tzinfo=ZoneInfo("Europe/Istanbul"),
    ).timestamp()
)


def attach_candidate(runners, race_type="Handikap 15", version_date="21.08.2026"):
    with patch("api_server.time.time", return_value=PROSPECTIVE_CREATED_TS):
        return attach_h15_training_degree_candidate(
            runners,
            race_type,
            "1400",
            "Kum",
            race_date=version_date,
            race_time="14.00",
        )


class H15TrainingShadowCandidateTests(unittest.TestCase):
    def test_status_exposes_new_nonranking_candidate_and_retired_trainer(self):
        response = app.test_client().get("/api/ml-status")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        candidate = payload["h15_training_shadow"]
        self.assertEqual(candidate["version"], _H15_TRAINING_SHADOW_VERSION)
        self.assertEqual(candidate["observation_start"], "21.08.2026")
        self.assertEqual(candidate["baseline_version"], "4.25")
        self.assertEqual(candidate["raw_weight_add_points"], 2.0)
        self.assertFalse(candidate["used_for_ranking"])
        self.assertFalse(candidate["telegram_visible"])
        self.assertEqual(candidate["promotion_ceiling"], "formal_replay_only")
        self.assertEqual(
            candidate["historical_evidence"]["temperature_fit_scope"],
            "build_only",
        )
        self.assertEqual(
            len(candidate["historical_evidence"]["source_report_sha256"]),
            64,
        )
        self.assertEqual(
            candidate["historical_evidence"]["calibration_evidence_artifact"],
            "automation/evidence/h15_training_degree_plus2_calibration_20260819.json",
        )
        self.assertEqual(
            len(
                candidate["historical_evidence"][
                    "calibration_evidence_artifact_sha256"
                ]
            ),
            64,
        )
        trainer = payload["handicap_trainer_shadow"]
        self.assertEqual(trainer["status"], "retired_regression")
        self.assertFalse(trainer["active"])

    def test_exact_historical_zero_to_two_over_102_formula_and_rank(self):
        formula = resolve_h15_training_candidate_weights({"pace_score": 100.0})
        self.assertAlmostEqual(
            formula["candidateWeights"]["training_degree_score"],
            2.0 / 102.0,
            places=10,
        )
        self.assertAlmostEqual(
            formula["candidateWeights"]["pace_score"],
            100.0 / 102.0,
            places=10,
        )

        runners = [
            runner("A", 70.0, 2, 70.0, 100.0),
            runner("B", 71.0, 1, 71.0, 0.0),
            runner("C", 60.0, 3, 60.0, 50.0),
        ]
        visible = copy.deepcopy([
            (item["aiScore"], item["rank"], item["v4Score"], item["v4Rank"])
            for item in runners
        ])

        attach_candidate(runners)

        self.assertAlmostEqual(
            runners[0]["h15TrainingCandidateBaseScore"],
            (70.0 * 100.0 + 100.0 * 2.0) / 102.0,
            places=10,
        )
        self.assertAlmostEqual(
            runners[1]["h15TrainingCandidateBaseScore"],
            (71.0 * 100.0 + 0.0 * 2.0) / 102.0,
            places=10,
        )
        self.assertEqual(runners[0]["h15TrainingCandidateRank"], 1)
        self.assertEqual(runners[1]["h15TrainingCandidateRank"], 2)
        self.assertEqual(runners[0]["h15TrainingCandidateBaselineWeights"], {"pace_score": 100.0})
        self.assertAlmostEqual(
            runners[0]["h15TrainingCandidateWeights"]["training_degree_score"],
            (2.0 / 102.0) * 100.0,
            places=9,
        )
        self.assertEqual(
            [
                (item["aiScore"], item["rank"], item["v4Score"], item["v4Rank"])
                for item in runners
            ],
            visible,
        )
        self.assertTrue(all(not item["h15TrainingCandidateUsedForRanking"] for item in runners))
        self.assertTrue(all(not item["h15TrainingCandidateTelegramVisible"] for item in runners))
        self.assertTrue(all(not item["h15TrainingCandidateRolloutEligible"] for item in runners))
        self.assertTrue(all(item["h15TrainingCandidateFormalReplayOnly"] for item in runners))
        self.assertTrue(all(len(item["h15TrainingCandidateDefinitionSha256"]) == 64 for item in runners))
        self.assertEqual(
            len({item["h15TrainingCandidateRaceSnapshotSha256"] for item in runners}),
            1,
        )
        self.assertTrue(
            all(item["h15TrainingCandidateReplayTop3SetAgreement"] for item in runners)
        )
        self.assertEqual(
            sorted(item["h15TrainingCandidateReplayBaselineRank"] for item in runners),
            [1, 2, 3],
        )
        self.assertTrue(
            all(item["h15TrainingCandidateCalibrationContract"]["fitScope"] == "build_only" for item in runners)
        )

    def test_aug19_exact_34_race_replay_ranking_fidelity(self):
        path = (
            Path(__file__).parent
            / "tests"
            / "h15_training_plus2_history_20260819.jsonl"
        )
        values = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        meta, races = values[0], values[1:]
        baseline_top3 = 0
        candidate_top3 = 0

        def order_hash(names):
            encoded = json.dumps(
                names,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            return hashlib.sha256(encoded).hexdigest()

        for race in races:
            baseline = []
            candidate = []
            for horse, visible_rank, penalty, numerator, denominator, training_value in race["h"]:
                baseline_base = numerator / denominator if denominator > 0 else 50.0
                baseline_score = max(0.0, min(100.0, baseline_base - penalty))
                candidate_score = calculate_h15_score_from_replay_totals(
                    numerator,
                    denominator,
                    training_value,
                    penalty,
                )["score"]
                baseline.append((horse, visible_rank, baseline_score))
                candidate.append((horse, visible_rank, candidate_score))

            baseline_names = [
                item[0]
                for item in sorted(baseline, key=lambda item: (-item[2], item[1], item[0]))
            ]
            candidate_names = [
                item[0]
                for item in sorted(candidate, key=lambda item: (-item[2], item[1], item[0]))
            ]
            self.assertEqual(order_hash(baseline_names), race["bh"], race["id"])
            self.assertEqual(order_hash(candidate_names), race["ch"], race["id"])
            baseline_winner_rank = baseline_names.index(race["w"]) + 1
            candidate_winner_rank = candidate_names.index(race["w"]) + 1
            self.assertEqual(baseline_winner_rank, race["br"], race["id"])
            self.assertEqual(candidate_winner_rank, race["cr"], race["id"])
            baseline_top3 += baseline_winner_rank <= 3
            candidate_top3 += candidate_winner_rank <= 3

        self.assertEqual(meta["races"], 34)
        self.assertEqual(len(races), 34)
        self.assertEqual(baseline_top3, meta["baselineWinnerTop3"])
        self.assertEqual(candidate_top3, meta["candidateWinnerTop3"])
        self.assertEqual((baseline_top3, candidate_top3), (13, 16))
        self.assertEqual(meta["buildInnerOuter"], [20, 7, 7])
        self.assertEqual(
            (meta["baselineTemperature"], meta["candidateTemperature"]),
            (14.0, 14.0),
        )
        formal_rescue = next(
            race
            for race in races
            if race["id"] == meta["formalRescueRaceId"]
        )
        self.assertEqual(meta["formalRescueWinner"], "FORTUNELLO")
        self.assertEqual(
            (formal_rescue["br"], formal_rescue["cr"]),
            (
                meta["formalRescueBaselineRank"],
                meta["formalRescueCandidateRank"],
            ),
        )
        self.assertEqual((formal_rescue["br"], formal_rescue["cr"]), (4, 3))
        winner_index = next(
            index
            for index, horse in enumerate(formal_rescue["h"])
            if horse[0] == formal_rescue["w"]
        )
        self.assertEqual(formal_rescue["f"][winner_index], 1)

    def test_api_snapshot_log_contract_is_accepted_by_fail_closed_monitor(self):
        runners = [
            runner("A", 70.0, 2, 70.0, 100.0),
            runner("B", 71.0, 1, 71.0, 0.0),
            runner("C", 60.0, 3, 60.0, 60.0),
            runner("D", 50.0, 4, 50.0, 40.0),
        ]
        attach_candidate(runners)

        entries = []
        for finish_pos, item in enumerate(runners, start=1):
            entries.append({
                "race_id": "H15-CONTRACT-1",
                "race_date": "21.08.2026",
                "race_no": "1",
                "race_time": "14.00",
                "city": "Ankara",
                "city_id": "2",
                "race_type": "Handikap 15",
                "track": "Kum",
                "distance": "1400",
                "field_size": 4,
                "horse_name": item["name"],
                "finish_pos": finish_pos,
                "v4_score": item["v4Score"],
                "v4_rank": item["v4Rank"],
                "rank_pred": item["rank"],
                "v4_version": item["v4Version"],
                "v4_applied_for_ranking": item["v4AppliedForRanking"],
                "v4_profile": item["v4Profile"],
                "v4_weights": item["v4Weights"],
                "metric_source_flags": item["metricSourceFlags"],
                **h15_training_candidate_log_fields(item),
            })

        report = build_report(entries, "2026-08-21")

        self.assertEqual(report["coverage"]["fullyLabeledEvidenceRaces"], 1)
        self.assertEqual(report["coverage"]["integrityInvalidRaces"], 0)
        self.assertEqual(report["candidateVersions"], [_H15_TRAINING_SHADOW_VERSION])

    def test_source_unavailable_is_neutral_to_formula_and_not_evidence(self):
        runners = [runner("A", 70.0, 1, 70.0, 99.0, source=False)]

        attach_candidate(runners)

        source = runners[0]["h15TrainingCandidateSource"]
        self.assertFalse(source["hasSource"])
        self.assertFalse(source["actionable"])
        self.assertEqual(source["unavailableCount"], 1)
        self.assertFalse(runners[0]["h15TrainingCandidateRaceEvidenceEligible"])
        self.assertEqual(runners[0]["h15TrainingCandidateBaseScore"], 70.0)

    def test_visible_replay_top3_mismatch_is_logged_and_excluded(self):
        runners = [
            runner("A", 90.0, 1, 90.0, 60.0),
            runner("B", 80.0, 2, 80.0, 60.0),
            runner("C", 70.0, 3, 10.0, 60.0),
            runner("D", 60.0, 4, 75.0, 60.0),
        ]

        attach_candidate(runners)

        self.assertTrue(
            all(
                not item["h15TrainingCandidateReplayTop3SetAgreement"]
                for item in runners
            )
        )
        self.assertTrue(
            all(
                item["h15TrainingCandidateEvidenceIssue"]
                == "visible_replay_baseline_top3_set_mismatch"
                for item in runners
            )
        )
        self.assertTrue(
            all(item["h15TrainingCandidateRaceEvidenceEligible"] for item in runners)
        )
        logged = h15_training_candidate_log_fields(runners[0])
        self.assertFalse(
            logged["h15_training_candidate_replay_top3_set_agreement"]
        )
        self.assertEqual(
            logged["h15_training_candidate_evidence_issue"],
            "visible_replay_baseline_top3_set_mismatch",
        )

    def test_disagreeing_source_guards_fail_closed(self):
        runners = [runner("A", 70.0, 1, 70.0, 99.0)]
        runners[0]["_mf"]["_has_training_times"] = False

        with self.assertRaisesRegex(ValueError, "disagree"):
            attach_candidate(runners)

        self.assertNotIn("h15TrainingCandidateScore", runners[0])

    def test_scope_is_exact_handicap15_and_v425(self):
        h14 = [runner("A", 70.0, 1, 70.0, 80.0)]
        v424 = [runner("A", 70.0, 1, 70.0, 80.0, version="4.24")]

        attach_candidate(h14, "Handikap 14")
        attach_candidate(v424)

        self.assertNotIn("h15TrainingCandidateVersion", h14[0])
        self.assertNotIn("h15TrainingCandidateVersion", v424[0])

    def test_pre_observation_and_post_time_snapshots_are_not_created(self):
        before_start = [runner("A", 70.0, 1, 70.0, 80.0)]
        after_race = [runner("A", 70.0, 1, 70.0, 80.0)]

        attach_candidate(before_start, version_date="20.08.2026")
        race_started_ts = int(
            datetime(
                2026,
                8,
                21,
                15,
                0,
                tzinfo=ZoneInfo("Europe/Istanbul"),
            ).timestamp()
        )
        with patch("api_server.time.time", return_value=race_started_ts):
            attach_h15_training_degree_candidate(
                after_race,
                "Handikap 15",
                "1400",
                "Kum",
                race_date="21.08.2026",
                race_time="14.00",
            )

        self.assertNotIn("h15TrainingCandidateVersion", before_start[0])
        self.assertNotIn("h15TrainingCandidateVersion", after_race[0])

    def test_snapshot_is_immutable_across_same_version_retry(self):
        previous = {
            "h15_training_candidate_version": _H15_TRAINING_SHADOW_VERSION,
            "h15_training_candidate_created_ts": 123,
            "h15_training_candidate_score": 61.0,
            "h15_training_candidate_race_snapshot_sha256": "a" * 64,
        }
        current = {
            "h15_training_candidate_version": _H15_TRAINING_SHADOW_VERSION,
            "h15_training_candidate_created_ts": 999,
            "h15_training_candidate_score": 90.0,
            "h15_training_candidate_race_snapshot_sha256": "b" * 64,
        }

        result = _preserve_h15_training_candidate_snapshot(current, previous)

        self.assertEqual(result["h15_training_candidate_created_ts"], 123)
        self.assertEqual(result["h15_training_candidate_score"], 61.0)
        self.assertEqual(result["h15_training_candidate_race_snapshot_sha256"], "a" * 64)
        self.assertEqual(
            _H15_TRAINING_SHADOW_OBSERVATION_START,
            "21.08.2026",
        )


if __name__ == "__main__":
    unittest.main()
