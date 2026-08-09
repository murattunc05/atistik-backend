import json
import subprocess
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from automation.winner_top3_interaction_diagnostics import (
    SCHEMA_VERSION,
    build_report,
    persist,
    slice_summary,
)


ROOT = Path(__file__).parent


def interaction_race(
    index: int,
    *,
    miss: bool = False,
    outer_harm: bool = False,
    partial: bool = False,
    profile: str = "HANDIKAP16",
    fingerprint_variant: bool = False,
):
    day = date(2026, 1, 1) + timedelta(days=index)
    if miss:
        form_values = [49.5, 52.0, 51.0, 50.0]
        predicted_ranks = [4, 1, 2, 3]
    else:
        form_values = [53.0, 52.0, 51.0, 50.0]
        predicted_ranks = [1, 2, 3, 4]
    rows = []
    for horse_index in range(4):
        if outer_harm:
            interaction_value = 0.0 if horse_index == 0 else 100.0
        else:
            interaction_value = 74.5 if horse_index == 0 else 50.0
        weights = {"form_trend": 100.0}
        if fingerprint_variant:
            weights["training_fitness"] = 1.0
        rows.append({
            "race_id": f"INT-{index:03d}",
            "race_date": day.strftime("%d.%m.%Y"),
            "race_no": 1,
            "race_time": "14.00",
            "city": "Ankara",
            "city_id": "2",
            "race_type": "HANDİKAP 16" if profile == "HANDIKAP16" else "ŞARTLI 1",
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
            "v4_profile": {
                "category": "HANDIKAP" if profile == "HANDIKAP16" else "SARTLI",
                "subtype": profile,
                "track": "Kum",
            },
            "v4_weights": weights,
            "features": {
                "form_trend": form_values[horse_index],
                "degree_avg": interaction_value,
                "degree_trend": interaction_value,
                "training_fitness": 50.0,
            },
            "metric_source_flags": {"hasTraining": True},
            "ts": 1767225600 + index,
        })
    return rows


class WinnerTop3InteractionDiagnosticsTests(unittest.TestCase):
    def test_pair_is_discovered_on_build_and_confirmed_on_outer(self):
        entries = []
        for index in range(32):
            entries.extend(interaction_race(index, miss=index % 3 == 2))

        report = build_report(entries, "2026-03-15", ("HANDIKAP16",))
        profile = report["profiles"][0]
        priority = next(
            item for item in profile["replayPriorities"]
            if item["deltas"] == {"degree_avg": 2.0, "degree_trend": 2.0}
        )

        self.assertEqual(profile["status"], "REPLAY_PRIORITY")
        self.assertGreaterEqual(priority["slices"]["build"]["netHits"], 1)
        self.assertGreaterEqual(priority["slices"]["outer"]["netHits"], 1)
        self.assertEqual(priority["slices"]["outer"]["damages"], 0)
        self.assertFalse(report["policy"]["automaticWeightChange"])
        self.assertFalse(report["policy"]["rankingImpact"])

    def test_outer_damage_blocks_replay_priority(self):
        entries = []
        for index in range(32):
            entries.extend(interaction_race(
                index,
                miss=index < 25 and index % 3 == 2,
                outer_harm=index >= 25,
            ))

        report = build_report(entries, "2026-03-15", ("HANDIKAP16",))
        profile = report["profiles"][0]
        pair = next(
            item for item in profile["candidates"]
            if item["deltas"] == {"degree_avg": 2.0, "degree_trend": 2.0}
        )

        self.assertEqual(pair["status"], "HARM_RISK")
        self.assertGreater(pair["slices"]["outer"]["damages"], 0)
        self.assertFalse(profile["replayPriorities"])

    def test_low_sample_remains_collecting_even_with_rescues(self):
        entries = []
        for index in range(12):
            entries.extend(interaction_race(index, miss=index % 3 == 2, profile="SART1"))

        report = build_report(entries, "2026-03-15", ("SART1",))
        profile = report["profiles"][0]

        self.assertEqual(profile["status"], "COLLECTING")
        self.assertEqual(profile["latestLabeledFingerprintRaces"], 12)
        self.assertFalse(profile["replayPriorities"])
        self.assertGreater(profile["optimisticRescueCeiling"]["rescuedByAnyPair"], 0)

    def test_latest_fingerprint_isolated_and_partial_race_excluded(self):
        entries = []
        for index in range(10):
            entries.extend(interaction_race(index, miss=True, profile="SART1"))
        for index in range(10, 15):
            entries.extend(interaction_race(
                index,
                miss=index % 2 == 0,
                profile="SART1",
                fingerprint_variant=True,
            ))
        entries.extend(interaction_race(
            99,
            miss=True,
            partial=True,
            profile="SART1",
            fingerprint_variant=True,
        ))

        report = build_report(entries, "2026-03-15", ("SART1",))
        profile = report["profiles"][0]

        self.assertEqual(report["input"]["compatibleCleanRaces"], 15)
        self.assertEqual(profile["compatibleProfileRaces"], 15)
        self.assertEqual(profile["historicalFingerprintCount"], 2)
        self.assertEqual(profile["latestLabeledFingerprintRaces"], 5)

    def test_slice_summary_tracks_separation_and_damage(self):
        summary = slice_summary([
            {
                "baseline": False,
                "candidate": True,
                "baselineGap": 1.0,
                "candidateGap": 0.95,
                "baselineCrowd": 4,
                "candidateCrowd": 4,
            },
            {
                "baseline": True,
                "candidate": False,
                "baselineGap": 2.0,
                "candidateGap": 1.8,
                "baselineCrowd": 3,
                "candidateCrowd": 4,
            },
        ])

        self.assertEqual(summary["rescues"], 1)
        self.assertEqual(summary["damages"], 1)
        self.assertEqual(summary["netHits"], 0)
        self.assertEqual(summary["baselineBoundaryGapMedian"], 1.5)
        self.assertEqual(summary["candidateBoundaryGapMedian"], 1.375)

    def test_persistence_and_cli_are_deterministic(self):
        entries = []
        for index in range(12):
            entries.extend(interaction_race(index, miss=index % 3 == 2, profile="SART1"))
        report = build_report(entries, "2026-03-15", ("SART1",))

        self.assertEqual(report["schemaVersion"], SCHEMA_VERSION)
        self.assertEqual(report, build_report(entries, "2026-03-15", ("SART1",)))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = persist(report, root / "data")
            self.assertTrue(Path(paths["latestJson"]).exists())
            predictions = root / "predictions.jsonl"
            predictions.write_text(
                "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in entries),
                encoding="utf-8",
            )
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "automation" / "winner_top3_interaction_diagnostics.py"),
                    "--predictions", str(predictions),
                    "--data-dir", str(root / "cli-data"),
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
                (root / "cli-data" / "automation" / "interaction-diagnostics" / "latest.json").exists()
            )

    def test_no_data_profile_still_persists_operator_report(self):
        report = build_report([], "2026-03-15", ("SART1",))

        self.assertEqual(report["profiles"][0]["status"], "NO_DATA")
        with tempfile.TemporaryDirectory() as directory:
            paths = persist(report, Path(directory))
            markdown = Path(paths["latestMarkdown"]).read_text(encoding="utf-8")
            self.assertIn("Status: `NO_DATA`", markdown)

    def test_pi_results_wires_interaction_before_commit(self):
        script = (ROOT / "scripts" / "raspberry" / "run-automation.sh").read_text(encoding="utf-8")
        interaction_position = script.find("automation/winner_top3_interaction_diagnostics.py")
        commit_position = script.find('git -C "$DATA_DIR" config user.name')

        self.assertGreater(interaction_position, 0)
        self.assertGreater(commit_position, interaction_position)
        self.assertIn("Interaction raporu uretilemedi; sonuc/backup akisi devam ediyor", script)


if __name__ == "__main__":
    unittest.main()
