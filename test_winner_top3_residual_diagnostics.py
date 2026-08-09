import json
import subprocess
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from automation.winner_top3_residual_diagnostics import (
    DATASET_SCHEMA_VERSION,
    SCHEMA_VERSION,
    aggregate_metrics,
    build_report,
    persist,
)


ROOT = Path(__file__).parent


def residual_race(
    index: int,
    *,
    miss: bool = False,
    partial: bool = False,
    profile: str = "HANDIKAP16",
    fingerprint_variant: bool = False,
):
    day = date(2026, 1, 1) + timedelta(days=index)
    if miss:
        form_values = [40.0, 52.0, 51.0, 50.0]
        predicted_ranks = [4, 1, 2, 3]
    else:
        form_values = [53.0, 52.0, 51.0, 50.0]
        predicted_ranks = [1, 2, 3, 4]
    rows = []
    for horse_index in range(4):
        winner_edge = 100.0 if horse_index == 0 else 40.0
        weights = {"form_trend": 100.0}
        if fingerprint_variant:
            weights["training_fitness"] = 1.0
        rows.append({
            "race_id": f"RES-{index:03d}",
            "race_date": day.strftime("%d.%m.%Y"),
            "race_no": 1,
            "race_time": "14.00",
            "city": "Ankara",
            "city_id": "2",
            "race_type": "HANDİKAP 16" if profile == "HANDIKAP16" else "ŞARTLI 1",
            "track": "Kum" if index % 2 else "Çim",
            "distance": 1600,
            "last_race_distance": 1400 + horse_index * 100,
            "days_since_last_race": 10 + horse_index,
            "race_count": 5 + horse_index,
            "filtered_race_count": 4 + horse_index,
            "field_size": 4,
            "horse_name": f"HORSE-{index}-{horse_index}",
            "finish_pos": None if partial and horse_index == 3 else horse_index + 1,
            "rank_pred": predicted_ranks[horse_index],
            "ai_score": form_values[horse_index],
            "v4_score": form_values[horse_index],
            "v4_penalty_total": 0.0,
            "v4_version": "4.24",
            "v4_applied_for_ranking": True,
            "v4_profile": {
                "category": "HANDIKAP" if profile == "HANDIKAP16" else "SARTLI",
                "subtype": profile,
                "track": "Kum" if index % 2 else "Çim",
            },
            "v4_weights": weights,
            "features": {
                "form_trend": form_values[horse_index],
                "degree_avg": winner_edge,
                "degree_trend": winner_edge,
                "hp_score": winner_edge,
                "training_fitness": 50.0,
                "agf_score": 90.0 if horse_index == 0 else 10.0,
            },
            "metric_source_flags": {
                "hasHp": True,
                "hasTraining": True,
                "hasAgf": True,
                "rawHp": 90.0 if horse_index == 0 else 70.0,
                "rawCurrentWeight": 55.0 + horse_index,
                "rawStartNo": horse_index + 1,
                "parsedAge": 4,
            },
            "ts": 1767225600 + index,
        })
    return rows


class WinnerTop3ResidualDiagnosticsTests(unittest.TestCase):
    def test_unrescued_misses_expose_consistent_winner_edge(self):
        entries = []
        for index in range(32):
            entries.extend(residual_race(index, miss=index % 3 == 2))

        report, training_rows = build_report(entries, "2026-03-15", ("HANDIKAP16",))
        profile = report["profiles"][0]
        degree = next(item for item in profile["topSignalMetrics"] if item["metric"] == "degree_avg")

        self.assertEqual(profile["status"], "RESIDUAL_SIGNAL_FOUND")
        self.assertGreaterEqual(profile["residualRaces"], 9)
        self.assertEqual(degree["status"], "CONSISTENT_WINNER_EDGE")
        self.assertEqual(degree["currentWeightPctMedian"], 0.0)
        self.assertEqual(degree["winnerEdgeRate"], 1.0)
        self.assertEqual(len(training_rows), 64)
        self.assertFalse(report["policy"]["automaticModelRetrain"])

    def test_training_pairs_are_race_split_and_label_safe(self):
        entries = []
        for index in range(32):
            entries.extend(residual_race(index, miss=index % 4 == 3))

        report, training_rows = build_report(entries, "2026-03-15", ("HANDIKAP16",))
        audit = report["trainingDataset"]["audits"][0]

        self.assertTrue(audit["leakageSafe"])
        self.assertTrue(audit["raceLevelSplit"])
        self.assertTrue(audit["agfExcluded"])
        self.assertEqual(audit["labelCounts"], {"0": 32, "1": 32})
        self.assertEqual(audit["splitRaceCounts"], {"build": 19, "inner": 6, "outer": 7})
        self.assertEqual({row["datasetSchemaVersion"] for row in training_rows}, {DATASET_SCHEMA_VERSION})
        self.assertFalse(any("finish_pos" in row["features"] for row in training_rows))
        self.assertFalse(any("rank_pred" in row["rawNumeric"] for row in training_rows))
        self.assertFalse(any("baselineWinnerTop3" in row["context"] for row in training_rows))

    def test_exact_two_thirds_winner_edge_is_consistent_signal(self):
        residuals = []
        for delta in [10.0] * 6 + [0.0, -5.0, -10.0]:
            residuals.append({
                "allMetricComparisons": [{
                    "metric": "degree_avg",
                    "family": "recent_form_degree",
                    "currentWeightPct": 2.0,
                    "delta": delta,
                    "winnerHasSource": True,
                    "cutoffHasSource": True,
                }],
            })

        degree = next(item for item in aggregate_metrics(residuals) if item["metric"] == "degree_avg")

        self.assertEqual(degree["winnerEdgeRaces"], 6)
        self.assertEqual(degree["winnerEdgeRate"], 0.6667)
        self.assertEqual(degree["status"], "CONSISTENT_WINNER_EDGE")

    def test_residual_detail_includes_raw_hp_and_cutoff(self):
        entries = []
        for index in range(32):
            entries.extend(residual_race(index, miss=index % 3 == 2))

        report, _ = build_report(entries, "2026-03-15", ("HANDIKAP16",))
        detail = report["profiles"][0]["residualDetails"][0]
        raw_hp = next(item for item in detail["rawComparisons"] if item["field"] == "rawHp")

        self.assertEqual(detail["cutoffRank"], 3)
        self.assertEqual(raw_hp["winnerValue"], 90.0)
        self.assertEqual(raw_hp["cutoffValue"], 70.0)
        self.assertEqual(raw_hp["delta"], 20.0)

    def test_latest_fingerprint_isolated_and_partial_excluded(self):
        entries = []
        for index in range(10):
            entries.extend(residual_race(index, miss=True, profile="SART1"))
        for index in range(10, 15):
            entries.extend(residual_race(
                index,
                miss=True,
                profile="SART1",
                fingerprint_variant=True,
            ))
        entries.extend(residual_race(
            99,
            miss=True,
            partial=True,
            profile="SART1",
            fingerprint_variant=True,
        ))

        report, training_rows = build_report(entries, "2026-03-15", ("SART1",))
        profile = report["profiles"][0]

        self.assertEqual(report["input"]["compatibleCleanRaces"], 15)
        self.assertEqual(profile["compatibleProfileRaces"], 15)
        self.assertEqual(profile["historicalFingerprintCount"], 2)
        self.assertEqual(profile["latestLabeledFingerprintRaces"], 5)
        self.assertEqual(len(training_rows), 10)
        self.assertEqual(profile["status"], "COLLECTING")

    def test_no_data_profile_persists(self):
        report, training_rows = build_report([], "2026-03-15", ("SART1",))

        self.assertEqual(report["schemaVersion"], SCHEMA_VERSION)
        self.assertEqual(report["profiles"][0]["status"], "NO_DATA")
        with tempfile.TemporaryDirectory() as directory:
            paths = persist(report, training_rows, Path(directory))
            self.assertTrue(Path(paths["latestJson"]).exists())
            self.assertEqual(Path(paths["latestTrainingPairs"]).read_text(encoding="utf-8"), "")

    def test_cli_writes_report_and_training_pairs(self):
        entries = []
        for index in range(12):
            entries.extend(residual_race(index, miss=index % 3 == 2, profile="SART1"))
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
                    str(ROOT / "automation" / "winner_top3_residual_diagnostics.py"),
                    "--predictions", str(predictions),
                    "--data-dir", str(root / "data"),
                    "--run-date", "2026-03-15",
                    "--profiles", "SART1",
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertTrue(
                (root / "data" / "automation" / "residual-diagnostics" / "latest.json").exists()
            )
            self.assertGreater(
                len((root / "data" / "automation" / "residual-diagnostics" / "training-pairs.latest.jsonl").read_text(encoding="utf-8").splitlines()),
                0,
            )

    def test_pi_results_wires_residual_before_commit(self):
        script = (ROOT / "scripts" / "raspberry" / "run-automation.sh").read_text(encoding="utf-8")
        residual_position = script.find("automation/winner_top3_residual_diagnostics.py")
        commit_position = script.find('git -C "$DATA_DIR" config user.name')

        self.assertGreater(residual_position, 0)
        self.assertGreater(commit_position, residual_position)
        self.assertIn("Residual raporu uretilemedi; sonuc/backup akisi devam ediyor", script)


if __name__ == "__main__":
    unittest.main()
