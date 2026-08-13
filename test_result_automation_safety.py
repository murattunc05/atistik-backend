import argparse
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from automation.atistik_daily_job import fetch_results, main, result_match_stats, results_mode, results_once
from automation.fallback_checker import results_ok


class ResultAutomationSafetyTests(unittest.TestCase):
    def _args(self, data_dir: Path, max_attempts: int = 1):
        return argparse.Namespace(
            mode="results",
            date="2026-07-15",
            day=date(2026, 7, 15),
            backend_url="https://example.test",
            data_dir=data_dir,
            max_attempts=max_attempts,
            cities=None,
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
        fetched = [
            {"horse_name": "SUPERCHIRON", "finish_pos": 1},
            {"horse_name": "AĞA SAÇAN", "finish_pos": 2},
        ]

        stats = result_match_stats(race, fetched)

        self.assertEqual(stats["matchedCount"], 2)
        self.assertEqual(stats["matchRatio"], 1.0)

    def test_zero_or_unknown_finish_is_not_counted_as_a_label(self):
        race = {"horses": [{"name": "A"}, {"name": "B"}]}
        fetched = [
            {"horse_name": "A", "finish_pos": 1},
            {"horse_name": "B", "finish_pos": 0},
        ]

        stats = result_match_stats(race, fetched)

        self.assertEqual(stats["matchedCount"], 1)
        self.assertEqual(stats["missingHorses"], ["B"])
        self.assertFalse(stats["labelsComplete"])
        self.assertEqual(stats["invalidLabels"], [{"horse": "B", "finishPos": 0}])

    def test_unexpected_official_runner_keeps_race_partial(self):
        race = {"horses": [{"name": "A"}, {"name": "B"}]}
        fetched = [
            {"horse_name": "A", "finish_pos": 1},
            {"horse_name": "B", "finish_pos": 2},
            {"horse_name": "UNEXPECTED", "finish_pos": 3},
        ]

        stats = result_match_stats(race, fetched)

        self.assertEqual(stats["matchedCount"], 2)
        self.assertEqual(stats["extraHorses"], ["UNEXPECTED"])
        self.assertFalse(stats["labelsComplete"])

    def test_fetch_results_sends_manifest_race_identity(self):
        race = {
            "raceId": "226100",
            "raceNo": "1",
            "horses": [{"name": "SUPER CHIRON", "detailLink": "/a"}],
        }
        with patch("automation.atistik_daily_job.http_json", return_value={"success": True}) as post:
            fetch_results("https://example.test", date(2026, 7, 15), race, 30)

        payload = post.call_args.kwargs["payload"]
        self.assertEqual(payload["race_id"], "226100")
        self.assertEqual(payload["race_date"], "15.07.2026")
        self.assertEqual(payload["race_no"], "1")
        self.assertEqual(payload["city_id"], "")
        self.assertEqual(payload["city_name"], "")

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

    def test_partial_labels_are_submitted_but_report_stays_partial(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            self._write_analysis(data_dir)
            fetched = {
                "success": True,
                "race_id": "226100",
                "result_source": "horse_history_fallback",
                "label_status": "partial",
                "unresolved": [{"horse_name": "ÜÇÜNCÜ", "reason": "history_missing"}],
                "results": [
                    {"horse_name": "SUPERCHIRON", "finish_pos": 1},
                    {"horse_name": "AĞA SAÇAN", "finish_pos": 2},
                ],
            }
            submit = {
                "success": True,
                "updated": 2,
                "idempotent": 0,
                "matched": 2,
                "incoming": 2,
                "conflict_count": 0,
            }
            with patch("automation.atistik_daily_job.fetch_results", return_value=fetched), patch(
                "automation.atistik_daily_job.http_json", return_value=submit
            ):
                report = results_once(
                    self._args(data_dir),
                    {"minSubmitMatchRatio": 0.60, "minSubmitMatchedHorses": 2},
                    False,
                )

        self.assertEqual(report["status"], "partial_success")
        self.assertEqual(report["totals"]["submitted"], 1)
        self.assertEqual(report["totals"]["partialLabels"], 1)
        self.assertEqual(report["races"][0]["status"], "partial_label")
        self.assertEqual(report["races"][0]["labelStatus"], "partial")

    def test_explicit_non_runner_completes_the_label_set(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            self._write_analysis(data_dir)
            fetched = {
                "success": True,
                "race_id": "226100",
                "result_source": "tjk_official_results",
                "label_status": "complete",
                "results": [
                    {"horse_name": "SUPERCHIRON", "finish_pos": 1},
                    {"horse_name": "AĞA SAÇAN", "finish_pos": 2},
                    {
                        "horse_name": "ÜÇÜNCÜ",
                        "finish_pos": 99,
                        "result_status": "non_runner",
                        "terminal_reason": "Koşmaz",
                    },
                ],
            }
            submit = {
                "success": True,
                "updated": 3,
                "idempotent": 0,
                "matched": 3,
                "incoming": 3,
                "conflict_count": 0,
            }
            with patch("automation.atistik_daily_job.fetch_results", return_value=fetched), patch(
                "automation.atistik_daily_job.http_json", return_value=submit
            ):
                report = results_once(self._args(data_dir), {}, False)

        self.assertEqual(report["status"], "completed")
        self.assertEqual(report["totals"]["partialLabels"], 0)
        self.assertEqual(report["races"][0]["status"], "submitted")
        self.assertEqual(report["races"][0]["labelStatus"], "complete")

    def test_fetched_race_id_mismatch_fails_closed_before_submission(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            self._write_analysis(data_dir)
            fetched = {
                "success": True,
                "race_id": "WRONG-RACE",
                "results": [
                    {"horse_name": "SUPERCHIRON", "finish_pos": 1},
                    {"horse_name": "AĞA SAÇAN", "finish_pos": 2},
                    {"horse_name": "ÜÇÜNCÜ", "finish_pos": 3},
                ],
            }
            with patch("automation.atistik_daily_job.fetch_results", return_value=fetched), patch(
                "automation.atistik_daily_job.http_json"
            ) as submit:
                report = results_once(self._args(data_dir), {}, False)

        submit.assert_not_called()
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["totals"]["failed"], 1)
        self.assertEqual(report["races"][0]["status"], "race_identity_mismatch")
        self.assertFalse(report["races"][0]["safeToSubmit"])

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

    def test_results_mode_retries_partial_labels(self):
        first = {
            "mode": "results",
            "status": "partial_success",
            "totals": {
                "checked": 1,
                "submitted": 1,
                "partialLabels": 1,
                "pending": 0,
                "failed": 0,
            },
        }
        second = {
            "mode": "results",
            "status": "completed",
            "totals": {
                "checked": 1,
                "submitted": 1,
                "partialLabels": 0,
                "pending": 0,
                "failed": 0,
            },
        }
        with patch("automation.atistik_daily_job.results_once", side_effect=[first, second]) as run, patch(
            "automation.atistik_daily_job.time.sleep"
        ):
            report = results_mode(self._args(Path("."), max_attempts=2), {"resultRetryIntervalMinutes": 0})

        self.assertEqual(run.call_count, 2)
        self.assertEqual(report["status"], "completed")

    def test_main_exits_nonzero_while_partial_labels_remain(self):
        args = self._args(Path("."))
        report = {
            "mode": "results",
            "status": "partial_success",
            "totals": {
                "checked": 1,
                "submitted": 1,
                "partialLabels": 1,
                "pending": 0,
                "failed": 0,
            },
        }
        with patch("automation.atistik_daily_job.parse_args", return_value=args), patch(
            "automation.atistik_daily_job.load_config", return_value={}
        ), patch("automation.atistik_daily_job.results_mode", return_value=report), patch(
            "automation.atistik_daily_job.persist_report"
        ):
            exit_code = main()

        self.assertEqual(exit_code, 1)

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
        mislabeled_complete = {
            "mode": "results",
            "status": "completed",
            "totals": {
                "checked": 3,
                "submitted": 3,
                "partialLabels": 1,
                "pending": 0,
                "failed": 0,
            },
        }

        self.assertTrue(results_ok(complete))
        self.assertFalse(results_ok(pending))
        self.assertFalse(results_ok(incomplete))
        self.assertFalse(results_ok(mislabeled_complete))


if __name__ == "__main__":
    unittest.main()
