import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from api_server import attach_handicap_hp_candidate, handicap_hp_candidate_log_fields
from automation.handicap_hp_shadow_monitor import build_report, persist
from test_handicap_hp_shadow_candidate import horses


def one_race(race_id="H1", *, labeled=True, official=True):
    analyzed = horses()
    with patch("api_server.time.time", return_value=1787475600):
        attach_handicap_hp_candidate(
            analyzed,
            race_type="Handikap 16",
            distance="1400",
            track="Kum",
            race_date="23.08.2026",
            race_time="17:00",
        )
    finish = {"D": 1, "A": 2, "B": 3, "C": 4}
    rows = []
    for horse_no, horse in enumerate(analyzed, start=1):
        rows.append({
            "race_id": race_id,
            "race_date": "23.08.2026",
            "race_no": "1",
            "race_time": "17:00",
            "city": "İstanbul",
            "city_id": "5",
            "race_type": "Handikap 16",
            "track": "Kum",
            "distance": "1400",
            "field_size": 4,
            "horse_name": horse["name"],
            "horse_no": str(horse_no),
            "rank_pred": horse["v4Rank"],
            "v4_rank": horse["v4Rank"],
            "v4_score": horse["v4Score"],
            "v4_penalty_total": horse["v4PenaltyTotal"],
            "finish_pos": finish[horse["name"]] if labeled else None,
            "result_status": "finished" if labeled else None,
            "result_source": (
                "tjk_official_results" if official else "horse_history_fallback"
            ) if labeled else None,
            **handicap_hp_candidate_log_fields(horse),
        })
    return rows


def five_races():
    rows = []
    for index in range(5):
        race = copy.deepcopy(one_race(f"H{index + 1}"))
        for row in race:
            row["race_no"] = str(index + 1)
        rows.extend(race)
    return rows


class HandicapHpShadowMonitorTests(unittest.TestCase):
    def test_five_clean_robust_rescues_pass_first_checkpoint(self):
        report = build_report(five_races(), "2026-08-23")
        self.assertEqual(report["coverage"]["fullyLabeledEvidenceRaces"], 5)
        self.assertEqual(report["status"], "EARLY_SIGNAL")
        self.assertEqual(report["nextCheckpointAt"], 10)
        self.assertEqual(report["cumulative"]["rescues"], 5)
        self.assertEqual(report["cumulative"]["robustTop3Rescues"], 5)
        self.assertEqual(report["cumulative"]["damages"], 0)
        self.assertTrue(report["checkpoints"][0]["passed"])
        self.assertFalse(report["liveRolloutEligible"])
        self.assertFalse(report["telegramVisible"])

    def test_unlabeled_nonofficial_and_mutated_payloads_do_not_become_evidence(self):
        rows = one_race("U1", labeled=False)
        rows.extend(one_race("N1", official=False))
        mutated = one_race("M1")
        mutated[0]["handicap_hp_candidate_score"] += 1.0
        rows.extend(mutated)
        report = build_report(rows, "2026-08-23")

        self.assertEqual(report["coverage"]["unlabeledRaces"], 1)
        self.assertEqual(report["coverage"]["nonOfficialExcludedRaces"], 1)
        self.assertEqual(report["coverage"]["integrityInvalidRaces"], 1)
        self.assertEqual(report["coverage"]["fullyLabeledEvidenceRaces"], 0)

    def test_audit_artifact_hash_mismatch_holds_all_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            bad_path = Path(tmp) / "audit.json"
            bad_path.write_text("{}\n", encoding="utf-8")
            with patch(
                "automation.handicap_hp_shadow_monitor.AUDIT_PATH",
                bad_path,
            ):
                report = build_report(one_race(), "2026-08-23")
        self.assertEqual(report["status"], "HOLD_AUDIT_ARTIFACT_INTEGRITY")
        self.assertFalse(report["artifactIntegrity"]["valid"])
        self.assertEqual(report["coverage"]["fullyLabeledEvidenceRaces"], 0)

    def test_persist_and_pi_results_wiring(self):
        report = build_report([], "2026-08-23")
        with tempfile.TemporaryDirectory() as tmp:
            persist(report, Path(tmp))
            daily = (
                Path(tmp)
                / "automation"
                / "runs"
                / "2026-08-23"
                / "handicap-hp-shadow-checkpoint.json"
            )
            latest = Path(tmp) / "automation" / "handicap-hp-shadow" / "latest.json"
            self.assertEqual(json.loads(daily.read_text(encoding="utf-8")), report)
            self.assertEqual(daily.read_bytes(), latest.read_bytes())

        script = (
            Path(__file__).parent / "scripts" / "raspberry" / "run-automation.sh"
        ).read_text(encoding="utf-8")
        monitor_at = script.index("python3 automation/handicap_hp_shadow_monitor.py")
        point_at = script.index("python3 automation/point_in_time_signal_monitor.py")
        commit_at = script.index('git -C "$DATA_DIR" add automation predictions.jsonl')
        self.assertLess(monitor_at, point_at)
        self.assertLess(monitor_at, commit_at)
        self.assertIn("if ! python3 automation/handicap_hp_shadow_monitor.py", script)


if __name__ == "__main__":
    unittest.main()
