import json
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from automation.atistik_daily_job import summarize_rankings
from automation.sart1_shadow_monitor import build_report, persist


VERSION = "sart1-bounded-top3-20260804-v1"


def race_rows(index, visible_winner_rank=4, candidate_winner_rank=2, partial=False):
    day = date(2026, 8, 5) + timedelta(days=index)
    visible_other = [rank for rank in range(1, 5) if rank != visible_winner_rank]
    candidate_other = [rank for rank in range(1, 5) if rank != candidate_winner_rank]
    rows = []
    for horse_index in range(4):
        is_winner = horse_index == 0
        rows.append(
            {
                "race_id": f"S1-{index:03d}",
                "race_date": day.strftime("%d.%m.%Y"),
                "race_no": "1",
                "race_time": "14.00",
                "city": "Ankara",
                "city_id": "2",
                "race_type": "ŞARTLI 1",
                "track": "Çim",
                "distance": "1200",
                "field_size": 4,
                "horse_name": f"HORSE-{horse_index}",
                "finish_pos": (
                    None
                    if partial and horse_index == 3
                    else horse_index + 1
                ),
                "rank_pred": (
                    1 if is_winner
                    else visible_other[horse_index - 1]
                ),
                "sart1_candidate_baseline_rank": (
                    visible_winner_rank
                    if is_winner
                    else visible_other[horse_index - 1]
                ),
                "sart1_candidate_rank": (
                    candidate_winner_rank
                    if is_winner
                    else candidate_other[horse_index - 1]
                ),
                "sart1_candidate_no_agf_rank": (
                    candidate_winner_rank
                    if is_winner
                    else candidate_other[horse_index - 1]
                ),
                "sart1_candidate_version": VERSION,
                "sart1_candidate_observation_start": "05.08.2026",
                "sart1_candidate_created_ts": 1785888000 + (index * 86400),
                "sart1_candidate_baseline_version": "4.24",
                "sart1_candidate_agf_coverage": 1.0,
                "sart1_candidate_agf_applied": True,
                "sart1_candidate_metric_source_flags": {
                    "hasTraining": True,
                    "hasTrainer": True,
                    "hasPedigree": True,
                    "hasAgf": True,
                },
                "sart1_candidate_feature_snapshot": {
                    "training_fitness": 80.0,
                    "trainer_score": 70.0,
                    "pedigree": 65.0,
                    "agf_score": 60.0,
                },
                "metric_source_flags": {
                    "hasTraining": False,
                    "hasTrainer": False,
                    "hasPedigree": False,
                    "hasAgf": False,
                },
            }
        )
    return rows


class Sart1ShadowMonitorTests(unittest.TestCase):
    def test_pi_results_wires_nonblocking_monitor_after_state_persist(self):
        script = (
            Path(__file__).parent
            / "scripts"
            / "raspberry"
            / "run-automation.sh"
        ).read_text(encoding="utf-8")
        persist_at = script.rindex("persist_state_predictions")
        monitor_at = script.index("python3 automation/sart1_shadow_monitor.py")
        commit_at = script.index('git -C "$DATA_DIR" add automation predictions.jsonl')
        self.assertLess(persist_at, monitor_at)
        self.assertLess(monitor_at, commit_at)
        self.assertIn("if ! python3 automation/sart1_shadow_monitor.py", script)

    def test_analysis_summary_carries_shadow_fields_without_replacing_v4(self):
        rows = summarize_rankings([
            {
                "name": "TEST",
                "rank": 1,
                "aiScore": 60.0,
                "v4Rank": 1,
                "v4Score": 60.0,
                "v4Version": "4.24",
                "sart1CandidateRank": 2,
                "sart1CandidateScore": 58.0,
                "sart1CandidateNoAgfRank": 2,
                "sart1CandidateNoAgfScore": 57.0,
                "sart1CandidateVersion": VERSION,
                "sart1CandidateAgfCoverage": 0.8,
                "sart1CandidateAgfApplied": True,
                "sart1CandidateUsedForRanking": False,
            }
        ])
        self.assertEqual(rows[0]["rank"], 1)
        self.assertEqual(rows[0]["v4Rank"], 1)
        self.assertEqual(rows[0]["sart1CandidateRank"], 2)
        self.assertEqual(rows[0]["sart1CandidateNoAgfRank"], 2)
        self.assertFalse(rows[0]["sart1CandidateUsedForRanking"])

    def test_partial_races_are_reported_but_not_used(self):
        entries = race_rows(0) + race_rows(1, partial=True)
        report = build_report(entries, "2026-08-06")
        self.assertEqual(report["coverage"]["fullyLabeledRaces"], 1)
        self.assertEqual(report["coverage"]["partialRaces"], 1)
        self.assertEqual(report["cumulative"]["races"], 1)
        self.assertEqual(report["status"], "COLLECTING")
        self.assertIn("ndcg5", report["cumulative"]["candidate"])
        self.assertIn("rho", report["cumulative"]["visible"])
        self.assertIn("mae", report["cumulative"]["candidate"])

    def test_finish_integrity_accepts_ties_and_terminal_status_but_rejects_bad_pattern(self):
        tied = race_rows(0)
        for row, finish in zip(tied, [1, 2, 2, 4]):
            row["finish_pos"] = finish
        terminal = race_rows(1)
        for row, finish in zip(terminal, [1, 2, 3, 99]):
            row["finish_pos"] = finish
        broken = race_rows(2)
        for row, finish in zip(broken, [1, 2, 2, 3]):
            row["finish_pos"] = finish

        report = build_report(tied + terminal + broken, "2026-08-07")

        self.assertEqual(report["coverage"]["fullyLabeledRaces"], 2)
        self.assertEqual(report["coverage"]["integrityInvalidRaces"], 1)
        self.assertEqual(report["cumulative"]["races"], 2)

    def test_pre_prospective_race_is_excluded(self):
        entries = race_rows(0)
        for row in entries:
            row["race_date"] = "04.08.2026"

        report = build_report(entries, "2026-08-05")

        self.assertEqual(report["coverage"]["preProspectiveExcludedRaces"], 1)
        self.assertEqual(report["cumulative"]["races"], 0)

    def test_post_race_or_mixed_identity_snapshot_is_invalid(self):
        post_race = race_rows(0)
        for row in post_race:
            row["sart1_candidate_created_ts"] = 1785940000
        mixed_city = race_rows(1)
        mixed_city[-1]["city_id"] = "99"

        report = build_report(post_race + mixed_city, "2026-08-06")

        self.assertEqual(report["coverage"]["integrityInvalidRaces"], 2)
        self.assertEqual(report["cumulative"]["races"], 0)

    def test_five_race_winner_top3_regression_is_flagged(self):
        entries = []
        for index in range(5):
            entries.extend(race_rows(index, visible_winner_rank=2, candidate_winner_rank=4))

        report = build_report(entries, "2026-08-09")

        self.assertEqual(report["status"], "REGRESSION_SIGNAL")
        self.assertTrue(report["regressionSignal"])
        self.assertFalse(report["liveRolloutEligible"])

    def test_three_clean_checkpoints_support_research_only(self):
        entries = []
        for index in range(15):
            entries.extend(race_rows(index))
        report = build_report(entries, "2026-08-19")
        self.assertEqual(report["status"], "SUPPORTED_FOR_FORMAL_REPLAY")
        self.assertEqual(len(report["checkpoints"]), 3)
        self.assertTrue(all(item["passed"] for item in report["checkpoints"]))
        self.assertTrue(report["researchSupported"])
        self.assertFalse(report["liveRolloutEligible"])
        self.assertEqual(report["cumulative"]["visible"]["winnerTop3"], 0)
        self.assertEqual(report["cumulative"]["candidate"]["winnerTop3"], 15)

    def test_persist_is_idempotent_and_writes_daily_and_latest_reports(self):
        report = build_report(race_rows(0), "2026-08-05")
        self.assertEqual(report, build_report(race_rows(0), "2026-08-05"))
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            persist(report, root)
            persist(report, root)
            daily = root / "automation" / "runs" / "2026-08-05" / "sart1-shadow-checkpoint.json"
            latest = root / "automation" / "sart1-shadow" / "latest.json"
            self.assertTrue(daily.exists())
            self.assertTrue(latest.exists())
            self.assertEqual(json.loads(daily.read_text(encoding="utf-8"))["runDate"], "2026-08-05")


if __name__ == "__main__":
    unittest.main()
