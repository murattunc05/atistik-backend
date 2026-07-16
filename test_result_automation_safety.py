import argparse
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from automation.atistik_daily_job import result_match_stats, results_mode, results_once
from automation.fallback_checker import results_ok


class ResultAutomationSafetyTests(unittest.TestCase):
    def _args(self, data_dir: Path, max_attempts: int = 1):
        return argparse.Namespace(
            mode="results",
            day=date(2026, 7, 15),
            backend_url="https://example.test",
            data_dir=data_dir,
            max_attempts=max_attempts,
        )

    def _write_analysis(self, data_dir: Path):
        out_dir = data_dir / "automation" / "runs" / "2026-07-15"
        out_dir.mkdir(parents=True)
        race = {
            "city": "İstanbul",
            "raceId": "226100",
            "raceNo": "1",
            "raceType": "Handikap 15",
            "horseCount": 3,
            "status": "analyzed",
            "horses": [
                {"name": "SUPER CHIRON", "detailLink": "/a"},
                {"name": "AĞA-SAÇAN", "detailLink": "/b"},
                {"name": "ÜÇÜNCÜ", "detailLink": "/c"},
            ],
        }
        (out_dir / "analysis.json").write_text(
            json.dumps({"mode": "analyze", "cities": [{"city": "İstanbul", "races": [race]}]}),
            encoding="utf-8",
        )

    def test_match_stats_use_same_compact_name_normalization_as_api(self):
        race = {"horses": [{"name": "SUPER CHIRON"}, {"name": "AĞA-SAÇAN"}]}
        fetched = [{"horse_name": "SUPERCHIRON"}, {"horse_name": "AĞA SAÇAN"}]

        stats = result_match_stats(race, fetched)

        self.assertEqual(stats["matchedCount"], 2)
        self.assertEqual(stats["matchRatio"], 1.0)

    def test_idempotent_api_response_counts_as_completed_submission(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            self._write_analysis(data_dir)
            fetched = {
                "success": True,
                "race_id": "226100",
                "results": [
                    {"horse_name": "SUPERCHIRON", "finish_pos": 1},
                    {"horse_name": "AĞA SAÇAN", "finish_pos": 2},
                    {"horse_name": "ÜÇÜNCÜ", "finish_pos": 3},
                ],
            }
            submit = {
                "success": True,
                "updated": 0,
                "idempotent": 3,
                "matched": 3,
                "incoming": 3,
                "conflict_count": 0,
            }
            with patch("automation.atistik_daily_job.fetch_results", return_value=fetched), patch(
                "automation.atistik_daily_job.http_json", return_value=submit
            ):
                report = results_once(self._args(data_dir), {}, False)

        self.assertEqual(report["status"], "completed")
        self.assertEqual(report["totals"]["submitted"], 1)
        self.assertEqual(report["totals"]["idempotent"], 1)
        self.assertEqual(report["races"][0]["status"], "already_labeled")

    def test_conflict_response_is_a_failed_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            self._write_analysis(data_dir)
            fetched = {
                "success": True,
                "race_id": "226100",
                "results": [
                    {"horse_name": "SUPERCHIRON", "finish_pos": 1},
                    {"horse_name": "AĞA SAÇAN", "finish_pos": 2},
                    {"horse_name": "ÜÇÜNCÜ", "finish_pos": 3},
                ],
            }
            conflict = {
                "success": False,
                "updated": 0,
                "idempotent": 0,
                "matched": 3,
                "incoming": 3,
                "conflict_count": 1,
            }
            with patch("automation.atistik_daily_job.fetch_results", return_value=fetched), patch(
                "automation.atistik_daily_job.http_json", return_value=conflict
            ):
                report = results_once(self._args(data_dir), {}, False)

        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["totals"]["failed"], 1)
        self.assertEqual(report["races"][0]["status"], "submit_failed")

    def test_results_mode_retries_failed_as_well_as_pending(self):
        first = {
            "mode": "results",
            "status": "failed",
            "totals": {"checked": 1, "submitted": 0, "pending": 0, "failed": 1},
        }
        second = {
            "mode": "results",
            "status": "completed",
            "totals": {"checked": 1, "submitted": 1, "pending": 0, "failed": 0},
        }
        with patch("automation.atistik_daily_job.results_once", side_effect=[first, second]) as run, patch(
            "automation.atistik_daily_job.time.sleep"
        ):
            report = results_mode(self._args(Path("."), max_attempts=2), {"resultRetryIntervalMinutes": 0})

        self.assertEqual(run.call_count, 2)
        self.assertEqual(report["status"], "completed")
        self.assertEqual(report["attempt"], 2)

    def test_fallback_only_accepts_fully_completed_result_report(self):
        complete = {
            "mode": "results",
            "status": "completed",
            "totals": {"checked": 3, "submitted": 3, "pending": 0, "failed": 0},
        }
        pending = {
            "mode": "results",
            "status": "partial_success",
            "totals": {"checked": 3, "submitted": 1, "pending": 2, "failed": 0},
        }
        incomplete = {
            "mode": "results",
            "status": "completed",
            "totals": {"checked": 3, "submitted": 2, "pending": 0, "failed": 0},
        }

        self.assertTrue(results_ok(complete))
        self.assertFalse(results_ok(pending))
        self.assertFalse(results_ok(incomplete))


if __name__ == "__main__":
    unittest.main()
