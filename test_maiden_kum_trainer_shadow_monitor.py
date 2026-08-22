import copy
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from api_server import (
    attach_maiden_kum_trainer_candidate,
    maiden_kum_trainer_candidate_log_fields,
)
from automation.maiden_kum_trainer_shadow_monitor import (
    EXPECTED_VERSION,
    build_report,
    calibration_evidence_status,
    checkpoint_pass,
    formal_support_ready,
)


CREATED_TS = int(
    datetime(2026, 8, 23, 10, 0, tzinfo=ZoneInfo("Europe/Istanbul")).timestamp()
)


def api_runner(name, score, rank, pace, trainer):
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
            "track": "Kum",
            "selectedKey": "MAIDEN|Kum",
        },
        "v4Weights": {"pace_score": 100.0},
        "metricSourceFlags": {"hasTrainer": True},
        "_mf": {
            "pace_score": pace,
            "trainer_score": trainer,
            "_has_trainer": True,
        },
    }


def race_rows(race_id="r1", *, partial=False):
    horses = [
        api_runner("A", 90.0, 1, 90.0, 20.0),
        api_runner("B", 80.0, 2, 80.0, 20.0),
        api_runner("C", 70.0, 3, 70.0, 20.0),
        api_runner("D", 69.5, 4, 69.5, 100.0),
    ]
    with patch("api_server.time.time", return_value=CREATED_TS):
        attach_maiden_kum_trainer_candidate(
            horses,
            race_type="Maiden",
            distance="1400",
            track="Kum",
            race_date="23.08.2026",
            race_time="18:00",
        )
    rows = []
    for finish, horse in enumerate(horses, start=1):
        row = {
            "race_date": "23.08.2026",
            "race_id": race_id,
            "race_no": "1",
            "race_time": "18:00",
            "city": "İstanbul",
            "city_id": "1",
            "race_type": "Maiden",
            "track": "Kum",
            "distance": "1400",
            "field_size": len(horses),
            "horse_name": horse["name"],
            "rank_pred": horse["v4Rank"],
            "v4_rank": horse["v4Rank"],
            "v4_score": horse["v4Score"],
            "v4_version": horse["v4Version"],
            "v4_applied_for_ranking": True,
            "v4_profile": horse["v4Profile"],
            "v4_weights": horse["v4Weights"],
            "metric_source_flags": horse["metricSourceFlags"],
            "finish_pos": None if partial and finish == 4 else finish,
            **maiden_kum_trainer_candidate_log_fields(horse),
        }
        rows.append(row)
    return rows


def passing_summary(*, top1_net=-99):
    return {
        "winnerTop3Net": 1,
        "damages": 0,
        "nonCausalRescues": 0,
        "top1Net": top1_net,
        "top5Net": 0,
        "objectiveDelta": 0.0,
        "baseline": {"mae": 2.0, "rho": 0.3, "ndcg5": 0.5},
        "candidate": {"mae": 2.04, "rho": 0.291, "ndcg5": 0.499},
        "calibration": {"passed": True},
    }


class MaidenKumTrainerMonitorTest(unittest.TestCase):
    def test_frozen_artifacts_are_ready_and_tamper_fails_closed(self):
        status = calibration_evidence_status()
        self.assertTrue(status["ready"])
        self.assertTrue(status["ml15OverlapNonAdditive"])
        with tempfile.TemporaryDirectory() as tmp:
            tampered = Path(tmp) / "audit.json"
            tampered.write_text("{}", encoding="utf-8")
            with patch(
                "automation.maiden_kum_trainer_shadow_monitor.AUDIT_PATH",
                tampered,
            ):
                status = calibration_evidence_status()
        self.assertFalse(status["ready"])
        self.assertEqual(status["reason"], "audit_artifact_sha256_mismatch")

    def test_clean_race_is_counted_and_partial_is_not(self):
        report = build_report(race_rows(), "2026-08-23")
        self.assertEqual(report["coverage"]["fullyLabeledEvidenceRaces"], 1)
        self.assertEqual(report["primaryObjective"], "winner_top3")
        self.assertFalse(report["liveRolloutEligible"])
        partial = build_report(race_rows(partial=True), "2026-08-23")
        self.assertEqual(partial["coverage"]["partialRaces"], 1)
        self.assertEqual(partial["coverage"]["fullyLabeledEvidenceRaces"], 0)

    def test_source_hash_tamper_is_integrity_invalid(self):
        rows = race_rows()
        rows[0]["maiden_kum_trainer_candidate_source"]["metricSourceFlag"] = "bad"
        report = build_report(rows, "2026-08-23")
        self.assertEqual(report["coverage"]["integrityInvalidRaces"], 1)
        self.assertEqual(report["integrityFailureReasons"], {"source_or_feature": 1})

    def test_top1_does_not_veto_early_top3_checkpoint(self):
        self.assertTrue(checkpoint_pass(passing_summary(top1_net=-99)))

    def test_top3_gain_with_one_top1_loss_passes_formal_ceiling(self):
        cumulative = passing_summary(top1_net=-1)
        checkpoints = [{"passed": True}, {"passed": True}, {"passed": True}]
        self.assertTrue(
            formal_support_ready(
                cumulative,
                checkpoints,
                complete_count=15,
                source_gate=True,
                separation_gate=True,
                rank_quality_gate=True,
                calibration_gate=True,
                integrity_invalid_races=0,
                versions={EXPECTED_VERSION},
            )
        )
        cumulative["top1Net"] = -2
        self.assertFalse(
            formal_support_ready(
                cumulative,
                checkpoints,
                complete_count=15,
                source_gate=True,
                separation_gate=True,
                rank_quality_gate=True,
                calibration_gate=True,
                integrity_invalid_races=0,
                versions={EXPECTED_VERSION},
            )
        )

    def test_results_runner_wires_monitor_after_results(self):
        script = Path("scripts/raspberry/run-automation.sh").read_text(encoding="utf-8")
        results_block = script.split('if [[ "$MODE" == "results" ]]', 1)[1]
        self.assertIn("automation/maiden_kum_trainer_shadow_monitor.py", results_block)


if __name__ == "__main__":
    unittest.main()
