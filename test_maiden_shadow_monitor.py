import json
import tempfile
import subprocess
import sys
import unittest
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from automation.maiden_shadow_monitor import (
    EXPECTED_ALPHA,
    EXPECTED_BASELINE_VERSION,
    EXPECTED_CANDIDATE_VERSION,
    EXPECTED_FEATURE_SCHEMA_SHA256,
    EXPECTED_MODEL_VERSION,
    EXPECTED_MODEL_SHA256,
    EXPECTED_TRAINING_CUTOFF,
    build_report,
    persist,
)


VERSION = EXPECTED_CANDIDATE_VERSION
ISTANBUL = ZoneInfo("Europe/Istanbul")


def race_rows(index, visible_winner_rank=4, candidate_winner_rank=3, partial=False):
    day = date(2026, 8, 10) + timedelta(days=index)
    race_start = datetime.combine(day, time(14, 0), tzinfo=ISTANBUL)
    created_ts = int(race_start.timestamp()) - 3600
    visible_other = [rank for rank in range(1, 5) if rank != visible_winner_rank]
    candidate_other = [rank for rank in range(1, 5) if rank != candidate_winner_rank]
    baseline_by_rank = {1: 100.0, 2: 20.0, 3: 10.0, 4: 0.0}
    ml_by_candidate_rank = {1: 100.0, 2: 80.0, 3: 70.0, 4: 0.0}
    rows = []
    for horse_index in range(4):
        is_winner = horse_index == 0
        baseline_rank = (
            visible_winner_rank if is_winner else visible_other[horse_index - 1]
        )
        candidate_rank = (
            candidate_winner_rank if is_winner else candidate_other[horse_index - 1]
        )
        baseline_component = baseline_by_rank[baseline_rank]
        ml_component = ml_by_candidate_rank[candidate_rank]
        rows.append({
            "race_id": f"M-{index:03d}",
            "race_date": day.strftime("%d.%m.%Y"),
            "race_no": "1",
            "race_time": "14.00",
            "city": "Ankara",
            "city_id": "2",
            "race_type": "MAIDEN",
            "track": "Çim",
            "distance": "1200",
            "field_size": 4,
            "horse_name": f"HORSE-{horse_index}",
            "finish_pos": None if partial and horse_index == 3 else horse_index + 1,
            "maiden_candidate_version": VERSION,
            "maiden_candidate_mode": "prospective_shadow_bounded",
            "maiden_candidate_observation_start": "10.08.2026",
            "maiden_candidate_created_ts": created_ts,
            "maiden_candidate_model_version": "maiden-shadow-20260810-v1",
            "maiden_candidate_model_sha256": EXPECTED_MODEL_SHA256,
            "maiden_candidate_feature_schema_hash": EXPECTED_FEATURE_SCHEMA_SHA256,
            "maiden_candidate_feature_vector_sha256": f"{horse_index + 1:064x}",
            "maiden_candidate_training_cutoff": "25.07.2026",
            "maiden_candidate_alpha": 0.15,
            "maiden_candidate_strict_no_agf_ml": True,
            "maiden_candidate_baseline_version": "4.25",
            "maiden_candidate_baseline_score": baseline_component,
            "maiden_candidate_baseline_rank": baseline_rank,
            "maiden_candidate_baseline_component": baseline_component,
            "maiden_candidate_ml_raw_score": ml_component,
            "maiden_candidate_ml_component": ml_component,
            "maiden_candidate_score": (
                0.85 * baseline_component + 0.15 * ml_component
            ),
            "maiden_candidate_rank": candidate_rank,
            "maiden_candidate_v4_score_faithful": True,
            "maiden_candidate_used_for_ranking": False,
            "maiden_candidate_rollout_eligible": False,
        })
    return rows


class MaidenShadowMonitorTests(unittest.TestCase):
    def test_partial_race_is_reported_but_excluded(self):
        report = build_report(race_rows(0) + race_rows(1, partial=True), "2026-08-12")

        self.assertEqual(report["coverage"]["fullyLabeledRaces"], 1)
        self.assertEqual(report["coverage"]["partialRaces"], 1)
        self.assertEqual(report["cumulative"]["winnerTop3Net"], 1)

    def test_mixed_artifact_hash_is_integrity_invalid(self):
        rows = race_rows(0)
        rows[-1]["maiden_candidate_model_sha256"] = "other-model"

        report = build_report(rows, "2026-08-11")

        self.assertEqual(report["coverage"]["integrityInvalidRaces"], 1)
        self.assertEqual(report["coverage"]["fullyLabeledRaces"], 0)

    def test_missing_score_payload_is_integrity_invalid(self):
        rows = race_rows(0)
        rows[0]["maiden_candidate_score"] = None

        report = build_report(rows, "2026-08-11")

        self.assertEqual(report["coverage"]["integrityInvalidRaces"], 1)
        self.assertEqual(report["coverage"]["fullyLabeledRaces"], 0)

    def test_tampered_blend_formula_is_integrity_invalid(self):
        rows = race_rows(0)
        rows[0]["maiden_candidate_score"] += 1.0

        report = build_report(rows, "2026-08-11")

        self.assertEqual(report["coverage"]["integrityInvalidRaces"], 1)
        self.assertEqual(report["coverage"]["fullyLabeledRaces"], 0)

    def test_nan_alpha_is_integrity_invalid(self):
        rows = race_rows(0)
        rows[0]["maiden_candidate_alpha"] = float("nan")

        report = build_report(rows, "2026-08-11")

        self.assertEqual(report["coverage"]["integrityInvalidRaces"], 1)
        self.assertEqual(report["coverage"]["fullyLabeledRaces"], 0)

    def test_expected_identity_constants_match_frozen_manifest(self):
        manifest = json.loads(
            (Path(__file__).parent / "maiden_shadow_manifest.json").read_text(
                encoding="utf-8"
            )
        )

        self.assertEqual(EXPECTED_CANDIDATE_VERSION, manifest["candidateVersion"])
        self.assertEqual(EXPECTED_MODEL_VERSION, manifest["modelVersion"])
        self.assertEqual(EXPECTED_MODEL_SHA256, manifest["modelSha256"])
        self.assertEqual(
            EXPECTED_FEATURE_SCHEMA_SHA256, manifest["featureSchemaSha256"]
        )
        self.assertEqual(EXPECTED_TRAINING_CUTOFF, manifest["trainingCutoff"])
        self.assertEqual(f"v{EXPECTED_BASELINE_VERSION}", manifest["baselineVersion"])
        self.assertEqual(EXPECTED_ALPHA, manifest["alpha"])

    def test_fifteen_clean_races_reach_formal_replay_only(self):
        entries = []
        for index in range(15):
            entries.extend(race_rows(
                index,
                visible_winner_rank=4 if index == 0 else 2,
                candidate_winner_rank=3 if index == 0 else 2,
            ))

        report = build_report(entries, "2026-08-25")

        self.assertEqual(len(report["checkpoints"]), 3)
        self.assertTrue(all(item["passed"] for item in report["checkpoints"]))
        self.assertEqual(report["status"], "SUPPORTED_FOR_FORMAL_REPLAY")
        self.assertTrue(report["formalReplaySupported"])
        self.assertFalse(report["liveRolloutEligible"])

    def test_monitor_is_nonblocking_between_persist_and_commit(self):
        script = (
            Path(__file__).parent / "scripts" / "raspberry" / "run-automation.sh"
        ).read_text(encoding="utf-8")
        persist_at = script.rindex("persist_state_predictions")
        monitor_at = script.index("python3 automation/maiden_shadow_monitor.py")
        commit_at = script.index('git -C "$DATA_DIR" add automation predictions.jsonl')

        self.assertLess(persist_at, monitor_at)
        self.assertLess(monitor_at, commit_at)
        self.assertIn("if ! python3 automation/maiden_shadow_monitor.py", script)

    def test_production_cli_invocation_loads(self):
        result = subprocess.run(
            [sys.executable, "automation/maiden_shadow_monitor.py", "--help"],
            cwd=Path(__file__).parent,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--predictions", result.stdout)

    def test_persist_writes_daily_and_latest_reports(self):
        report = build_report(race_rows(0), "2026-08-11")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            persist(report, root)

            self.assertTrue(
                (root / "automation" / "runs" / "2026-08-11" / "maiden-shadow-checkpoint.json").exists()
            )
            self.assertTrue((root / "automation" / "maiden-shadow" / "latest.md").exists())


if __name__ == "__main__":
    unittest.main()
