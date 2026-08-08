import json
import subprocess
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from automation.winner_top3_failure_diagnostics import (
    SCHEMA_VERSION,
    build_report,
    direction_summary,
    persist,
)


ROOT = Path(__file__).parent


def diagnostic_race(index: int, *, miss: bool = False, partial: bool = False):
    day = date(2026, 1, 1) + timedelta(days=index)
    if miss:
        form_values = [49.0, 52.0, 51.0, 50.0]
        predicted_ranks = [4, 1, 2, 3]
    else:
        form_values = [53.0, 52.0, 51.0, 50.0]
        predicted_ranks = [1, 2, 3, 4]
    rows = []
    for horse_index in range(4):
        rows.append({
            "race_id": f"FD-{index:03d}",
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
            "rank_pred": predicted_ranks[horse_index],
            "ai_score": form_values[horse_index],
            "v4_score": form_values[horse_index],
            "v4_penalty_total": 0.0,
            "v4_version": "4.24",
            "v4_applied_for_ranking": True,
            "v4_profile": {"category": "SARTLI", "subtype": "SART4", "track": "Kum"},
            "v4_weights": {"form_trend": 100.0},
            "features": {
                "form_trend": form_values[horse_index],
                "degree_avg": 100.0 if horse_index == 0 else 0.0,
            },
            "metric_source_flags": {},
            "ts": 1767225600 + index,
        })
    return rows


class WinnerTop3FailureDiagnosticsTests(unittest.TestCase):
    def test_bounded_plus2_reports_rescue_without_live_promotion(self):
        entries = []
        for index in range(60):
            entries.extend(diagnostic_race(index, miss=index % 3 == 2))

        report = build_report(entries, "2026-03-15", ("SARTLI",))
        group = report["groups"][0]
        degree = next(item for item in group["metricDiagnostics"] if item["metric"] == "degree_avg")
        best = degree["directions"][degree["bestDirection"]]

        self.assertEqual(degree["bestDirection"], "plus2")
        self.assertGreaterEqual(best["full"]["rescues"], 2)
        self.assertEqual(best["full"]["damages"], 0)
        self.assertEqual(degree["status"], "REPLAY_PRIORITY")
        self.assertFalse(report["policy"]["automaticWeightChange"])

    def test_damage_is_counted_separately_from_rescue(self):
        outcomes = [
            {"baseline": False, "plus2": True},
            {"baseline": True, "plus2": False},
            {"baseline": True, "plus2": True},
        ]

        summary = direction_summary(outcomes, "plus2")

        self.assertEqual(summary["rescues"], 1)
        self.assertEqual(summary["damages"], 1)
        self.assertEqual(summary["netHits"], 0)

    def test_partial_race_is_excluded_and_miss_cluster_is_visible(self):
        entries = []
        for index in range(12):
            entries.extend(diagnostic_race(index, miss=index >= 8))
        entries.extend(diagnostic_race(99, miss=True, partial=True))

        report = build_report(entries, "2026-03-15", ("SARTLI",))
        group = report["groups"][0]
        profile = group["segments"]["profile"][0]

        self.assertEqual(report["input"]["compatibleCleanRaces"], 12)
        self.assertEqual(group["performance"]["misses"], 4)
        self.assertEqual(profile["key"], "SART4")
        self.assertEqual(profile["misses"], 4)

    def test_report_and_persistence_are_deterministic(self):
        entries = []
        for index in range(20):
            entries.extend(diagnostic_race(index, miss=index % 4 == 3))
        report = build_report(entries, "2026-03-15", ("SARTLI",))

        self.assertEqual(report["schemaVersion"], SCHEMA_VERSION)
        self.assertEqual(report, build_report(entries, "2026-03-15", ("SARTLI",)))
        with tempfile.TemporaryDirectory() as directory:
            paths = persist(report, Path(directory))
            payload = json.loads(Path(paths["latestJson"]).read_text(encoding="utf-8"))
            self.assertEqual(payload["schemaVersion"], SCHEMA_VERSION)
            self.assertTrue(Path(paths["dailyMarkdown"]).exists())

    def test_direct_cli_execution_writes_artifact(self):
        entries = []
        for index in range(12):
            entries.extend(diagnostic_race(index, miss=index % 3 == 2))
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
                    str(ROOT / "automation" / "winner_top3_failure_diagnostics.py"),
                    "--predictions", str(predictions),
                    "--data-dir", str(root / "data"),
                    "--run-date", "2026-03-15",
                    "--groups", "SARTLI",
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertTrue(
                (root / "data" / "automation" / "failure-diagnostics" / "latest.json").exists()
            )

    def test_pi_results_wires_diagnostics_before_commit(self):
        script = (ROOT / "scripts" / "raspberry" / "run-automation.sh").read_text(encoding="utf-8")
        diagnostic_position = script.find("automation/winner_top3_failure_diagnostics.py")
        commit_position = script.find('git -C "$DATA_DIR" config user.name')

        self.assertGreater(diagnostic_position, 0)
        self.assertGreater(commit_position, diagnostic_position)
        self.assertIn("Failure raporu uretilemedi; sonuc/backup akisi devam ediyor", script)


if __name__ == "__main__":
    unittest.main()
