import json
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from automation.metric_signal_registry import (
    build_report,
    classify_race,
    competitive_race_rows,
    persist,
    score_diagnostics,
)


ROOT = Path(__file__).parent


def race_rows(
    index: int,
    *,
    scores: tuple[float, float, float, float] = (80.0, 70.0, 60.0, 50.0),
    winner_pred_rank: int = 1,
    partial: bool = False,
    terminal: bool = False,
) -> list[dict]:
    day = date(2026, 8, 1) + timedelta(days=index)
    other_ranks = [rank for rank in range(1, 5) if rank != winner_pred_rank]
    rows = []
    for horse_index in range(4):
        is_winner = horse_index == 0
        finish = horse_index + 1
        if partial and horse_index == 3:
            finish = None
        elif terminal and horse_index == 3:
            finish = 99
        rows.append({
            "race_id": f"R-{index:03d}",
            "race_date": day.strftime("%d.%m.%Y"),
            "race_no": 1,
            "race_time": "14.00",
            "city": "Ankara",
            "city_id": "2",
            "race_type": "ŞARTLI 1",
            "track": "Kum",
            "field_size": 4,
            "horse_name": f"HORSE-{index}-{horse_index}",
            "finish_pos": finish,
            "rank_pred": winner_pred_rank if is_winner else other_ranks[horse_index - 1],
            "ai_score": scores[winner_pred_rank - 1] if is_winner else scores[other_ranks[horse_index - 1] - 1],
            "v4_score": scores[winner_pred_rank - 1] if is_winner else scores[other_ranks[horse_index - 1] - 1],
            "v4_version": "4.25",
            "v4_penalty_total": 0.0,
            "v4_profile": {
                "category": "SARTLI",
                "subtype": "SART1",
                "track": "Kum",
                "selectedKey": "SART1",
                "profileKey": "SART1|short|small|Kum",
            },
            "v4_weights": {"form_trend": 100.0},
            "features": {
                "degree_avg": 100.0 if is_winner else 0.0,
                "form_trend": 50.0,
                "hp_score": 80.0 if is_winner else 40.0,
            },
            "metric_source_flags": {"hasHp": True},
        })
    return rows


class MetricSignalRegistryTests(unittest.TestCase):
    def test_finish_integrity_accepts_terminal_and_excludes_partial(self):
        self.assertEqual(classify_race(race_rows(0, terminal=True)), "fully_labeled")
        self.assertEqual(classify_race(race_rows(1, partial=True)), "partial")

    def test_competitive_rows_exclude_unknown_99_but_keep_verified_derecesiz(self):
        unknown = race_rows(0, terminal=True)
        unknown[-1]["rank_pred"] = 1
        for index, item in enumerate(unknown[:-1], start=2):
            item["rank_pred"] = index

        filtered = competitive_race_rows(unknown)

        self.assertEqual(len(filtered), 3)
        self.assertEqual(sorted(row["rank_pred"] for row in filtered), [1, 2, 3])
        self.assertNotIn(99, [row["finish_pos"] for row in filtered])

        verified = race_rows(1, terminal=True)
        verified[-1].update({
            "result_status": "unranked_terminal",
            "terminal_reason": "Derecesiz",
            "result_source": "tjk_official_results",
        })

        self.assertEqual(len(competitive_race_rows(verified)), 4)

    def test_score_diagnostics_flags_real_compression(self):
        tight = score_diagnostics(
            race_rows(0, scores=(50.3, 50.2, 50.1, 50.0)),
            bootstrap_iterations=0,
        )
        separated = score_diagnostics(
            race_rows(1, scores=(90.0, 75.0, 60.0, 40.0)),
            bootstrap_iterations=0,
        )

        self.assertEqual(tight["separationStatus"], "RED")
        self.assertAlmostEqual(tight["top3Top4Gap"], 0.1)
        self.assertEqual(separated["separationStatus"], "GREEN")
        self.assertEqual(separated["top3Top4Gap"], 20.0)

    def test_report_keeps_partial_out_of_signal_evidence(self):
        entries = race_rows(0) + race_rows(1, partial=True)
        report = build_report(entries, "2026-08-01", bootstrap_iterations=0)

        self.assertEqual(report["inventory"]["metricCount"], 38)
        self.assertEqual(report["inventory"]["sourceGatedMetricCount"], 29)
        self.assertEqual(report["coverage"]["fullyLabeledRaces"], 1)
        self.assertEqual(report["coverage"]["partialRaces"], 1)
        sart1 = next(
            scope for scope in report["scopes"]
            if scope["scopeType"] == "PROFILE" and scope["scopeKey"] == "SART1"
        )
        self.assertEqual(sart1["coverage"]["fullyLabeledRaces"], 1)
        self.assertEqual(sart1["coverage"]["partialRaces"], 1)
        degree = next(metric for metric in sart1["metrics"] if metric["metric"] == "degree_avg")
        self.assertEqual(degree["status"], "COLLECTING")

    def test_gated_metric_requires_explicit_source_proof(self):
        entries = race_rows(0)
        for row in entries:
            row["metric_source_flags"] = {}
        report = build_report(entries, "2026-08-01", bootstrap_iterations=0)
        sart1 = next(
            scope for scope in report["scopes"]
            if scope["scopeType"] == "PROFILE" and scope["scopeKey"] == "SART1"
        )
        hp = next(metric for metric in sart1["metrics"] if metric["metric"] == "hp_score")

        self.assertEqual(hp["coverage"], 0.0)
        self.assertEqual(hp["sourceFlagPresenceRate"], 0.0)

    def test_chronological_winner_top3_gate_marks_replay_candidate(self):
        entries = []
        for index in range(60):
            entries.extend(race_rows(index, winner_pred_rank=4))

        report = build_report(entries, "2026-09-29", bootstrap_iterations=0)
        group = next(
            scope for scope in report["scopes"]
            if scope["scopeType"] == "GROUP" and scope["scopeKey"] == "SARTLI"
        )
        degree = next(metric for metric in group["metrics"] if metric["metric"] == "degree_avg")

        self.assertEqual(degree["status"], "CANDIDATE_FOR_REPLAY")
        self.assertGreaterEqual(degree["boundedPlus2"]["full"]["deltaHits"], 2)
        self.assertGreaterEqual(degree["boundedPlus2"]["outer"]["deltaHits"], 1)
        self.assertEqual(group["thresholds"]["remainingForLive"], 0)

    def test_persist_is_idempotent_and_writes_daily_and_latest(self):
        report = build_report(race_rows(0), "2026-08-01", bootstrap_iterations=0)
        self.assertEqual(
            report,
            build_report(race_rows(0), "2026-08-01", bootstrap_iterations=0),
        )
        with tempfile.TemporaryDirectory() as directory:
            paths = persist(report, Path(directory))
            first = Path(paths["dailyJson"]).read_text(encoding="utf-8")
            persist(report, Path(directory))
            second = Path(paths["dailyJson"]).read_text(encoding="utf-8")

            self.assertEqual(first, second)
            self.assertTrue(Path(paths["latestJson"]).exists())
            self.assertTrue(Path(paths["dailyMarkdown"]).exists())
            daily_payload = json.loads(first)
            self.assertEqual(
                daily_payload["fullRegistryPath"],
                "automation/metric-signals/latest.json",
            )
            self.assertLess(
                Path(paths["dailyJson"]).stat().st_size,
                Path(paths["latestJson"]).stat().st_size,
            )

    def test_pi_results_wires_registry_nonblocking_before_commit(self):
        script = (ROOT / "scripts" / "raspberry" / "run-automation.sh").read_text(encoding="utf-8")
        persist_at = script.rindex("persist_state_predictions")
        registry_at = script.index("python3 automation/metric_signal_registry.py")
        replay_at = script.index("python3 automation/metric_signal_replay.py")
        coupon_at = script.index("python3 automation/six_leg_coupon_scorecard.py")
        commit_at = script.index('git -C "$DATA_DIR" add automation predictions.jsonl')
        self.assertLess(persist_at, registry_at)
        self.assertLess(registry_at, replay_at)
        self.assertLess(replay_at, coupon_at)
        self.assertLess(coupon_at, commit_at)
        self.assertIn("if ! python3 automation/metric_signal_registry.py", script)
        self.assertIn("elif ! python3 automation/metric_signal_replay.py", script)
        self.assertIn(
            '--registry "$DATA_DIR/automation/metric-signals/latest.json"',
            script,
        )
        self.assertIn("if ! python3 automation/six_leg_coupon_scorecard.py", script)


if __name__ == "__main__":
    unittest.main()
