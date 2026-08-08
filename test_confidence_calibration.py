import json
import subprocess
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from automation.confidence_calibration import (
    SCHEMA_VERSION,
    build_report,
    calibration_compatible,
    evaluate_scope,
    persist,
    race_confidence_event,
    weight_fingerprint,
)


ROOT = Path(__file__).parent


def calibration_race(index: int, *, hit: bool = True, weights=None, partial: bool = False):
    day = date(2026, 1, 1) + timedelta(days=index)
    winner_pred_rank = 1 if hit else 4
    other_ranks = [rank for rank in range(1, 5) if rank != winner_pred_rank]
    rows = []
    for horse_index in range(4):
        is_winner = horse_index == 0
        predicted_rank = winner_pred_rank if is_winner else other_ranks[horse_index - 1]
        rows.append({
            "race_id": f"CAL-{index:03d}",
            "race_date": day.strftime("%d.%m.%Y"),
            "race_no": 1,
            "race_time": "14.00",
            "city": "Ankara",
            "city_id": "2",
            "race_type": "ŞARTLI 4",
            "track": "Kum",
            "field_size": 4,
            "horse_name": f"HORSE-{index}-{horse_index}",
            "finish_pos": None if partial and horse_index == 3 else horse_index + 1,
            "rank_pred": predicted_rank,
            "v4_score": 90.0 - (predicted_rank - 1) * 10.0,
            "v4_version": "4.24",
            "v4_applied_for_ranking": True,
            "v4_profile": {"category": "SARTLI", "subtype": "SART4", "track": "Kum"},
            "v4_weights": weights or {"form_trend": 60.0, "degree_avg": 40.0},
            "ts": 1767225600 + index,
        })
    return rows


class ConfidenceCalibrationTests(unittest.TestCase):
    def test_only_integrity_safe_visible_v421_plus_races_are_compatible(self):
        clean = calibration_race(0)
        self.assertTrue(calibration_compatible(clean))

        partial = calibration_race(1, partial=True)
        self.assertFalse(calibration_compatible(partial))

        hidden = calibration_race(2)
        hidden[0]["v4_applied_for_ranking"] = False
        self.assertFalse(calibration_compatible(hidden))

        old = calibration_race(3)
        for row in old:
            row["v4_version"] = "4.20"
        self.assertFalse(calibration_compatible(old))

    def test_temperature_changes_confidence_not_ranking(self):
        rows = calibration_race(0)
        cold = race_confidence_event(rows, 4.0)
        warm = race_confidence_event(rows, 32.0)

        self.assertEqual(cold["winnerTop3Label"], warm["winnerTop3Label"])
        self.assertNotEqual(cold["winnerTop3Probability"], warm["winnerTop3Probability"])

    def test_profile_models_are_partitioned_by_exact_weight_fingerprint(self):
        first_weights = {"form_trend": 60.0, "degree_avg": 40.0}
        second_weights = {"form_trend": 40.0, "degree_avg": 60.0}
        entries = []
        for index in range(35):
            entries.extend(calibration_race(index, hit=index % 5 != 4, weights=first_weights))
        for index in range(35, 70):
            entries.extend(calibration_race(index, hit=index % 5 != 4, weights=second_weights))

        report = build_report(entries, "2026-03-15")
        profile_models = [
            scope for scope in report["scopes"]
            if scope["scopeType"] == "PROFILE" and scope["scopeKey"] == "SART4"
        ]

        self.assertEqual(len(profile_models), 2)
        self.assertNotEqual(profile_models[0]["weightFingerprint"], profile_models[1]["weightFingerprint"])
        self.assertEqual(sum(item["races"] for item in profile_models), 70)

    def test_outer_changes_do_not_refit_build_temperature(self):
        shared = [calibration_race(index, hit=index % 5 != 4) for index in range(48)]
        hit_outer = shared + [calibration_race(index, hit=True) for index in range(48, 60)]
        miss_outer = shared + [calibration_race(index, hit=False) for index in range(48, 60)]

        first = evaluate_scope("GROUP", "SARTLI", hit_outer)
        second = evaluate_scope("GROUP", "SARTLI", miss_outer)

        self.assertEqual(first["temperatureFit"]["candidate"], second["temperatureFit"]["candidate"])
        self.assertNotEqual(
            first["comparisons"]["outer"]["candidate"]["winnerTop3Rate"],
            second["comparisons"]["outer"]["candidate"]["winnerTop3Rate"],
        )

    def test_report_and_persistence_are_deterministic(self):
        entries = []
        for index in range(60):
            entries.extend(calibration_race(index, hit=index % 5 != 4))
        report = build_report(entries, "2026-03-15")

        self.assertEqual(report["schemaVersion"], SCHEMA_VERSION)
        self.assertEqual(report, build_report(entries, "2026-03-15"))
        self.assertEqual(report["input"]["compatibleCleanRaces"], 60)
        self.assertTrue(all(not item["runtimeEligible"] for item in report["scopes"]))

        with tempfile.TemporaryDirectory() as directory:
            paths = persist(report, Path(directory))
            latest = json.loads(Path(paths["latestJson"]).read_text(encoding="utf-8"))
            self.assertEqual(latest["schemaVersion"], SCHEMA_VERSION)
            self.assertTrue(Path(paths["dailyMarkdown"]).exists())

    def test_direct_cli_execution_writes_artifact(self):
        entries = []
        for index in range(12):
            entries.extend(calibration_race(index, hit=index % 5 != 4))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            predictions = root / "predictions.jsonl"
            predictions.write_text(
                "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in entries),
                encoding="utf-8",
            )
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "automation" / "confidence_calibration.py"),
                    "--predictions", str(predictions),
                    "--data-dir", str(root / "data"),
                    "--run-date", "2026-03-15",
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertTrue(
                (root / "data" / "automation" / "confidence-calibration" / "latest.json").exists()
            )

    def test_pi_results_wires_calibration_nonblocking_before_commit(self):
        script = (ROOT / "scripts" / "raspberry" / "run-automation.sh").read_text(encoding="utf-8")
        calibration_position = script.find("automation/confidence_calibration.py")
        commit_position = script.find('git -C "$DATA_DIR" config user.name')

        self.assertGreater(calibration_position, 0)
        self.assertGreater(commit_position, calibration_position)
        self.assertIn("Kalibrasyon raporu uretilemedi; sonuc/backup akisi devam ediyor", script)

    def test_weight_fingerprint_is_order_independent(self):
        first = {"v4_weights": {"a": 60, "b": 40}}
        second = {"v4_weights": {"b": 40.0, "a": 60.0}}
        self.assertEqual(weight_fingerprint(first), weight_fingerprint(second))


if __name__ == "__main__":
    unittest.main()
