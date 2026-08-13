import subprocess
import sys
import tempfile
import unittest
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from automation.handicap_trainer_shadow_monitor import (
    EXPECTED_OBSERVATION_START,
    EXPECTED_VERSION,
    build_report,
    persist,
)


ISTANBUL = ZoneInfo("Europe/Istanbul")


def race_rows(index, baseline_winner_rank=4, candidate_winner_rank=3, partial=False):
    day = date(2026, 8, 14) + timedelta(days=index)
    race_start = datetime.combine(day, time(14, 0), tzinfo=ISTANBUL)
    created_ts = int(race_start.timestamp()) - 3600
    baseline_other = [rank for rank in range(1, 5) if rank != baseline_winner_rank]
    candidate_other = [rank for rank in range(1, 5) if rank != candidate_winner_rank]
    score_by_rank = {1: 90.0, 2: 80.0, 3: 70.0, 4: 60.0}
    rows = []
    for horse_index in range(4):
        winner = horse_index == 0
        baseline_rank = (
            baseline_winner_rank if winner else baseline_other[horse_index - 1]
        )
        candidate_rank = (
            candidate_winner_rank if winner else candidate_other[horse_index - 1]
        )
        candidate_score = score_by_rank[candidate_rank]
        rows.append({
            "race_id": f"H-{index:03d}",
            "race_date": day.strftime("%d.%m.%Y"),
            "race_no": "1",
            "race_time": "14.00",
            "city": "Ankara",
            "city_id": "2",
            "race_type": "Handikap 15",
            "track": "Kum",
            "distance": "1400",
            "field_size": 4,
            "horse_name": f"HORSE-{horse_index}",
            "finish_pos": None if partial and horse_index == 3 else horse_index + 1,
            "v4_score": score_by_rank[baseline_rank],
            "v4_rank": baseline_rank,
            "handicap_trainer_candidate_version": EXPECTED_VERSION,
            "handicap_trainer_candidate_mode": "prospective_shadow_ablation",
            "handicap_trainer_candidate_observation_start": EXPECTED_OBSERVATION_START,
            "handicap_trainer_candidate_created_ts": created_ts,
            "handicap_trainer_candidate_baseline_version": "4.25",
            "handicap_trainer_candidate_baseline_score": score_by_rank[baseline_rank],
            "handicap_trainer_candidate_baseline_rank": baseline_rank,
            "handicap_trainer_candidate_base_score": candidate_score,
            "handicap_trainer_candidate_penalty_total": 0.0,
            "handicap_trainer_candidate_score": candidate_score,
            "handicap_trainer_candidate_rank": candidate_rank,
            "handicap_trainer_candidate_used_for_ranking": False,
            "handicap_trainer_candidate_rollout_eligible": False,
            "handicap_trainer_candidate_profile": {
                "category": "HANDIKAP",
                "subtype": "HANDIKAP15",
                "distanceBucket": "mid",
                "fieldBucket": "small",
                "track": "Kum",
                "profileKey": "HANDIKAP15|mid|small|Kum",
                "selectedKey": "HANDIKAP15|Kum",
                "fallbackLevel": "subtype_track",
            },
            "handicap_trainer_candidate_baseline_weights": {
                "pace_score": 98.0,
                "trainer_score": 2.0,
            },
            "handicap_trainer_candidate_weights": {"pace_score": 100.0},
            "handicap_trainer_candidate_weight_delta_pct": {
                "pace_score": 2.0,
                "trainer_score": -2.0,
            },
            "handicap_trainer_candidate_ablated_metric": "trainer_score",
            "handicap_trainer_candidate_removed_weight_pct": 2.0,
            "handicap_trainer_candidate_metric_source_flags": {"hasTrainer": True},
            "handicap_trainer_candidate_source": {
                "metric": "trainer_score",
                "guard": "hasTrainer",
                "hasSource": True,
                "sourceCount": 4,
                "runnerCount": 4,
                "coverage": 1.0,
            },
            "handicap_trainer_candidate_feature_snapshot": {
                "pace_score": candidate_score,
                "trainer_score": 70.0,
            },
            "handicap_trainer_candidate_score_components": {
                "pace_score": {
                    "value": candidate_score,
                    "weightPct": 100.0,
                    "guard": None,
                    "included": True,
                }
            },
        })
    return rows


class HandicapTrainerShadowMonitorTests(unittest.TestCase):
    def test_partial_is_counted_but_excluded_from_metrics(self):
        report = build_report(
            race_rows(0) + race_rows(1, partial=True),
            "2026-08-15",
        )

        self.assertEqual(report["coverage"]["fullyLabeledRaces"], 1)
        self.assertEqual(report["coverage"]["partialRaces"], 1)
        self.assertEqual(report["cumulative"]["winnerTop3Net"], 1)

    def test_tampered_score_formula_is_integrity_invalid(self):
        rows = race_rows(0)
        rows[0]["handicap_trainer_candidate_score"] += 1.0

        report = build_report(rows, "2026-08-14")

        self.assertEqual(report["coverage"]["integrityInvalidRaces"], 1)
        self.assertEqual(report["coverage"]["fullyLabeledRaces"], 0)

    def test_components_must_match_frozen_weights_and_feature_snapshot(self):
        rows = race_rows(0)
        rows[0]["handicap_trainer_candidate_score_components"]["pace_score"][
            "weightPct"
        ] = 80.0

        report = build_report(rows, "2026-08-14")

        self.assertEqual(report["coverage"]["integrityInvalidRaces"], 1)
        self.assertEqual(report["coverage"]["fullyLabeledRaces"], 0)

        rows = race_rows(0)
        rows[0]["handicap_trainer_candidate_feature_snapshot"]["pace_score"] = 50.0

        report = build_report(rows, "2026-08-14")

        self.assertEqual(report["coverage"]["integrityInvalidRaces"], 1)

    def test_mixed_profile_snapshot_is_integrity_invalid(self):
        rows = race_rows(0)
        rows[-1]["handicap_trainer_candidate_profile"] = {
            **rows[-1]["handicap_trainer_candidate_profile"],
            "selectedKey": "HANDIKAP16|Kum",
        }

        report = build_report(rows, "2026-08-14")

        self.assertEqual(report["coverage"]["integrityInvalidRaces"], 1)

    def test_pre_prospective_race_is_excluded(self):
        rows = race_rows(0)
        for row in rows:
            row["race_date"] = "13.08.2026"

        report = build_report(rows, "2026-08-14")

        self.assertEqual(report["coverage"]["preProspectiveExcludedRaces"], 1)
        self.assertEqual(report["coverage"]["fullyLabeledRaces"], 0)

    def test_clean_plus_five_ten_fifteen_advances_only_to_formal_replay(self):
        rows = []
        for index in range(15):
            rows.extend(race_rows(
                index,
                baseline_winner_rank=4 if index == 0 else 2,
                candidate_winner_rank=3 if index == 0 else 2,
            ))

        report = build_report(rows, "2026-08-28")

        self.assertEqual([item["atRace"] for item in report["checkpoints"]], [5, 10, 15])
        self.assertTrue(all(item["passed"] for item in report["checkpoints"]))
        self.assertEqual(report["status"], "SUPPORTED_FOR_FORMAL_REPLAY")
        self.assertTrue(report["formalReplaySupported"])
        self.assertFalse(report["liveRolloutEligible"])
        self.assertEqual(report["cumulative"]["rescues"], 1)
        self.assertEqual(report["cumulative"]["damages"], 0)

    def test_monitor_is_nonblocking_after_persist_before_registry_and_commit(self):
        script = (
            Path(__file__).parent / "scripts" / "raspberry" / "run-automation.sh"
        ).read_text(encoding="utf-8")
        persist_at = script.rindex("persist_state_predictions")
        monitor_at = script.index("python3 automation/handicap_trainer_shadow_monitor.py")
        registry_at = script.index("python3 automation/metric_signal_registry.py")
        commit_at = script.index('git -C "$DATA_DIR" add automation predictions.jsonl')

        self.assertLess(persist_at, monitor_at)
        self.assertLess(monitor_at, registry_at)
        self.assertLess(monitor_at, commit_at)
        self.assertIn(
            "if ! python3 automation/handicap_trainer_shadow_monitor.py",
            script,
        )

    def test_cli_loads(self):
        result = subprocess.run(
            [sys.executable, "automation/handicap_trainer_shadow_monitor.py", "--help"],
            cwd=Path(__file__).parent,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--predictions", result.stdout)

    def test_persist_writes_daily_and_latest_reports(self):
        report = build_report(race_rows(0), "2026-08-14")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            persist(report, root)

            self.assertTrue(
                (
                    root
                    / "automation"
                    / "runs"
                    / "2026-08-14"
                    / "handicap-trainer-shadow-checkpoint.json"
                ).exists()
            )
            self.assertTrue(
                (root / "automation" / "handicap-trainer-shadow" / "latest.md").exists()
            )


if __name__ == "__main__":
    unittest.main()
