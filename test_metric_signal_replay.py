import json
import subprocess
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from automation.metric_signal_replay import (
    build_report,
    calibration_metrics,
    deduplicate_candidates,
    evaluate_candidate,
    load_candidates,
    persist,
    plackett_luce_top3_probabilities,
)
from automation.metric_signal_registry import group_races


ROOT = Path(__file__).parent


def replay_race(index: int, *, winner_pred_rank: int = 4, adverse: bool = False) -> list[dict]:
    day = date(2026, 1, 1) + timedelta(days=index)
    other_ranks = [rank for rank in range(1, 5) if rank != winner_pred_rank]
    rows = []
    for horse_index in range(4):
        is_winner = horse_index == 0
        predicted_rank = winner_pred_rank if is_winner else other_ranks[horse_index - 1]
        metric_value = 0.0 if (is_winner and adverse) else 100.0 if is_winner else 100.0 if adverse else 0.0
        rows.append({
            "race_id": f"RF-{index:03d}",
            "race_date": day.strftime("%d.%m.%Y"),
            "race_no": 1,
            "race_time": "14.00",
            "city": "Ankara",
            "city_id": "2",
            "race_type": "ŞARTLI 1",
            "track": "Kum",
            "field_size": 4,
            "horse_name": f"HORSE-{index}-{horse_index}",
            "finish_pos": horse_index + 1,
            "rank_pred": predicted_rank,
            "ai_score": 50.0,
            "v4_score": 50.0,
            "v4_version": "4.25",
            "v4_applied_for_ranking": True,
            "v4_penalty_total": 0.0,
            "v4_profile": {
                "category": "SARTLI",
                "subtype": "SART1",
                "track": "Kum",
                "selectedKey": "SART1",
                "profileKey": "SART1|short|small|Kum",
            },
            "v4_weights": {"form_trend": 100.0},
            "features": {"form_trend": 50.0, "degree_avg": metric_value},
            "metric_source_flags": {},
            "ts": 1767225600 + index,
        })
    return rows


def registry_payload() -> dict:
    metric = {"metric": "degree_avg", "status": "CANDIDATE_FOR_REPLAY"}
    return {
        "schemaVersion": "metric-signal-registry-v1",
        "runDate": "2026-03-01",
        "scopes": [
            {"scopeType": "GROUP", "scopeKey": "SARTLI", "metrics": [metric]},
            {"scopeType": "PROFILE", "scopeKey": "SART1", "metrics": [metric]},
        ],
    }


class MetricSignalReplayTests(unittest.TestCase):
    def test_plackett_luce_top3_probabilities_are_valid(self):
        probabilities = plackett_luce_top3_probabilities([90.0, 70.0, 50.0, 30.0], 12.0)

        self.assertAlmostEqual(sum(probabilities), 3.0, places=6)
        self.assertTrue(all(0.0 <= value <= 1.0 for value in probabilities))
        self.assertGreater(probabilities[0], probabilities[-1])

    def test_calibration_metrics_reward_exact_probabilities(self):
        perfect = calibration_metrics([(0.99, 1), (0.01, 0)])
        weak = calibration_metrics([(0.50, 1), (0.50, 0)])

        self.assertLess(perfect["brier"], weak["brier"])
        self.assertLess(perfect["logLoss"], weak["logLoss"])

    def test_duplicate_group_and_profile_candidate_prefers_profile(self):
        entries = []
        for index in range(12):
            entries.extend(replay_race(index))
        prepared = deduplicate_candidates(
            group_races(entries),
            load_candidates(registry_payload()),
        )

        self.assertEqual(len(prepared), 1)
        self.assertEqual(prepared[0][0]["scopeType"], "PROFILE")
        self.assertEqual(prepared[0][0]["scopeKey"], "SART1")

    def test_strong_candidate_only_reaches_prospective_shadow(self):
        races = [replay_race(index) for index in range(60)]
        result = evaluate_candidate(
            {"scopeType": "PROFILE", "scopeKey": "SART1", "metric": "degree_avg"},
            races,
        )

        self.assertEqual(result["status"], "SUPPORTED_FOR_PROSPECTIVE_SHADOW")
        self.assertFalse(result["liveRolloutEligible"])
        self.assertFalse(result["outerIsUntouched"])
        self.assertGreaterEqual(result["comparisons"]["full"]["delta"]["winnerTop3"], 2)
        self.assertTrue(all(check["passed"] for check in result["checks"]))

    def test_adverse_candidate_is_held(self):
        races = [replay_race(index, winner_pred_rank=1, adverse=True) for index in range(60)]
        result = evaluate_candidate(
            {"scopeType": "PROFILE", "scopeKey": "SART1", "metric": "degree_avg"},
            races,
        )

        self.assertEqual(result["status"], "HOLD")
        self.assertTrue(any(not check["passed"] for check in result["checks"]))

    def test_profile_candidate_below_30_compatible_races_is_held(self):
        result = evaluate_candidate(
            {"scopeType": "PROFILE", "scopeKey": "SART1", "metric": "degree_avg"},
            [replay_race(index) for index in range(20)],
        )

        threshold = next(check for check in result["checks"] if check["name"] == "compatible_evidence_threshold")
        self.assertEqual(result["status"], "HOLD")
        self.assertFalse(threshold["passed"])
        self.assertEqual(threshold["detail"], "races=20/30")

    def test_build_and_persist_are_deterministic(self):
        entries = []
        for index in range(60):
            entries.extend(replay_race(index))
        report = build_report(entries, registry_payload(), "2026-03-01")
        self.assertEqual(report, build_report(entries, registry_payload(), "2026-03-01"))
        self.assertEqual(report["input"]["evaluatedCandidates"], 1)

        with tempfile.TemporaryDirectory() as directory:
            paths = persist(report, Path(directory))
            payload = json.loads(Path(paths["latestJson"]).read_text(encoding="utf-8"))
            self.assertEqual(payload["schemaVersion"], "metric-signal-replay-v1")
            self.assertTrue(Path(paths["dailyMarkdown"]).exists())

    def test_direct_cli_execution_resolves_sibling_registry_module(self):
        entries = []
        for index in range(12):
            entries.extend(replay_race(index))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            predictions = root / "predictions.jsonl"
            predictions.write_text(
                "".join(json.dumps(row) + "\n" for row in entries),
                encoding="utf-8",
            )
            registry = root / "registry.json"
            registry.write_text(json.dumps(registry_payload()), encoding="utf-8")
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "automation" / "metric_signal_replay.py"),
                    "--predictions", str(predictions),
                    "--registry", str(registry),
                    "--data-dir", str(root / "data"),
                    "--run-date", "2026-03-01",
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertTrue((root / "data" / "automation" / "metric-replay" / "latest.json").exists())


if __name__ == "__main__":
    unittest.main()
