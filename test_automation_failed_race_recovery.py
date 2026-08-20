import argparse
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from automation.atistik_daily_job import analyze_mode, build_summary, load_city_program, main


class FailedRaceRecoveryTest(unittest.TestCase):
    def _args(self):
        return argparse.Namespace(
            mode="analyze",
            day=date(2026, 6, 26),
            backend_url="https://example.test",
            data_dir=Path("."),
            cities=None,
        )

    def test_analyze_mode_retries_only_failed_races(self):
        race = {
            "city": "Kocaeli",
            "raceId": "225817",
            "raceNo": "6",
            "raceType": "Handikap 14 /H2",
            "time": "20.30",
            "distance": "1900",
            "track": "Kum",
            "horses": [{"name": "A", "no": "1", "detailLink": "/a"}],
        }
        program = {"city": "Kocaeli", "cityId": "6", "status": "ok", "races": [race]}
        failed = {
            "city": "Kocaeli",
            "raceId": "225817",
            "raceNo": "6",
            "raceType": "Handikap 14 /H2",
            "time": "20.30",
            "distance": "1900",
            "track": "Kum",
            "horseCount": 1,
            "horses": [{"name": "A", "no": "1", "detailLink": "/a"}],
            "status": "failed",
            "error": "",
            "retryErrors": [{"attempt": 1, "http_status": 503, "error": ""}],
        }
        recovered = {
            "city": "Kocaeli",
            "raceId": "225817",
            "raceNo": "6",
            "raceType": "Handikap 14 /H2",
            "time": "20.30",
            "distance": "1900",
            "track": "Kum",
            "horseCount": 1,
            "horses": [{"name": "A", "no": "1", "detailLink": "/a"}],
            "status": "analyzed",
            "rankings": [{"horse": "A", "v4Rank": 1, "v4Score": 70.0}],
        }

        with patch("automation.atistik_daily_job.load_city_program", return_value=program), \
             patch("automation.atistik_daily_job.analyze_race", side_effect=[failed, recovered]) as analyze, \
             patch("automation.atistik_daily_job.time.sleep"):
            report = analyze_mode(
                self._args(),
                {
                    "cities": ["Kocaeli"],
                    "failedRaceRecoveryPasses": 1,
                    "failedRaceRecoveryDelaySeconds": 0,
                },
            )

        self.assertEqual(analyze.call_count, 2)
        self.assertEqual(report["status"], "completed")
        self.assertEqual(report["totals"]["failed"], 0)
        self.assertEqual(report["totals"]["analyzed"], 1)
        self.assertEqual(report["recovery"]["initialFailed"], 1)
        self.assertEqual(report["recovery"]["recovered"], 1)
        self.assertTrue(report["cities"][0]["races"][0]["recoveredFromError"] == "")
        recovered_race_arg = analyze.call_args_list[1].args[2]
        self.assertEqual(recovered_race_arg["city"], "Kocaeli")
        self.assertEqual(recovered_race_arg["cityId"], "6")

    def test_load_city_program_retries_transient_daily_program_failure(self):
        first_failure = {"success": False, "error": "HTTP 503", "http_status": 503}
        city_list = {
            "success": True,
            "cities": [{"id": "9", "name": "Kocaeli"}],
            "races": [],
        }
        city_program = {
            "success": True,
            "city": "Kocaeli",
            "cityId": "9",
            "races": [{"raceId": "225890", "raceNo": "1", "horses": [{"name": "A"}]}],
        }

        with patch("automation.atistik_daily_job.http_json", side_effect=[first_failure, city_list, city_program]) as http, \
             patch("automation.atistik_daily_job.time.sleep"):
            program = load_city_program("https://example.test", date(2026, 6, 30), "Kocaeli", 30)

        self.assertEqual(http.call_count, 3)
        self.assertEqual(program["status"], "ok")
        self.assertEqual(program["cityId"], "9")
        self.assertEqual(len(program["races"]), 1)

    def test_analyzed_city_plus_city_load_failure_is_partial_and_unresolved(self):
        race = {
            "raceId": "226900",
            "raceNo": "1",
            "raceType": "ŞARTLI 4",
            "time": "14.30",
            "distance": "1400",
            "track": "Kum",
            "horses": [{"name": "A", "no": "1"}],
        }
        programs = [
            {"city": "İstanbul", "cityId": "1", "status": "ok", "races": [race]},
            {
                "city": "Ankara",
                "status": "failed",
                "error": "daily-program city failed",
                "races": [],
            },
        ]
        analyzed = {
            "city": "İstanbul",
            "raceId": "226900",
            "raceNo": "1",
            "status": "analyzed",
            "rankings": [{"horse": "A", "v4Rank": 1, "v4Score": 70.0}],
        }

        with patch("automation.atistik_daily_job.load_city_program", side_effect=programs), \
             patch("automation.atistik_daily_job.analyze_race", return_value=analyzed):
            report = analyze_mode(
                self._args(),
                {
                    "cities": ["İstanbul", "Ankara"],
                    "failedRaceRecoveryPasses": 0,
                },
            )

        self.assertEqual(report["status"], "partial_success")
        self.assertEqual(report["totals"]["analyzed"], 1)
        self.assertEqual(report["totals"]["successfulCities"], 1)
        self.assertEqual(report["totals"]["failedCities"], 1)
        self.assertEqual(report["totals"]["unresolved"], 1)
        self.assertEqual(report["unresolvedCities"][0]["city"], "Ankara")

    def test_all_domestic_no_races_is_completed(self):
        programs = [
            {"city": "İstanbul", "status": "no_races", "races": []},
            {"city": "Ankara", "status": "no_races", "races": []},
        ]
        with patch("automation.atistik_daily_job.load_city_program", side_effect=programs):
            report = analyze_mode(
                self._args(),
                {
                    "cities": ["İstanbul", "Ankara"],
                    "failedRaceRecoveryPasses": 0,
                },
            )

        self.assertEqual(report["status"], "completed")
        self.assertEqual(report["totals"]["analyzed"], 0)
        self.assertEqual(report["totals"]["noRaceCities"], 2)
        self.assertEqual(report["totals"]["failedCities"], 0)
        self.assertEqual(report["totals"]["unresolved"], 0)

    def test_city_not_found_without_any_resolved_work_is_failed(self):
        with patch(
            "automation.atistik_daily_job.load_city_program",
            return_value={"city": "Atlantis", "status": "city_not_found", "races": []},
        ):
            report = analyze_mode(
                self._args(),
                {"cities": ["Atlantis"], "failedRaceRecoveryPasses": 0},
            )

        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["totals"]["failedCities"], 1)
        self.assertEqual(report["totals"]["unresolved"], 1)
        self.assertEqual(report["unresolvedCities"][0]["status"], "city_not_found")

    def test_unprocessable_race_keeps_mixed_city_report_partial(self):
        races = [
            {"raceId": "226901", "raceNo": "1", "horses": [{"name": "A"}]},
            {"raceId": "", "raceNo": "2", "horses": []},
        ]
        program = {"city": "İzmir", "cityId": "3", "status": "ok", "races": races}
        analyzed = {"city": "İzmir", "raceId": "226901", "status": "analyzed"}
        skipped = {
            "city": "İzmir",
            "raceId": "",
            "status": "skipped",
            "skipReasons": ["missing_race_id", "empty_horse_list"],
        }

        with patch("automation.atistik_daily_job.load_city_program", return_value=program), \
             patch("automation.atistik_daily_job.analyze_race", side_effect=[analyzed, skipped]):
            report = analyze_mode(
                self._args(),
                {"cities": ["İzmir"], "failedRaceRecoveryPasses": 0},
            )

        self.assertEqual(report["status"], "partial_success")
        self.assertEqual(report["totals"]["analyzed"], 1)
        self.assertEqual(report["totals"]["skippedRaces"], 1)
        self.assertEqual(report["totals"]["unresolvedRaces"], 1)
        self.assertEqual(report["totals"]["unresolved"], 1)

    def test_main_exits_nonzero_when_any_city_is_unresolved(self):
        args = argparse.Namespace(
            mode="analyze",
            date="2026-08-15",
            backend_url="https://example.test",
            data_dir=Path("."),
            cities=None,
        )
        report = {
            "mode": "analyze",
            "status": "partial_success",
            "totals": {
                "analyzed": 1,
                "failed": 0,
                "failedCities": 1,
                "unresolvedRaces": 0,
                "unresolved": 1,
            },
            "cities": [
                {"city": "İstanbul", "status": "ok", "races": [{"status": "analyzed"}]},
                {"city": "Ankara", "status": "failed", "races": []},
            ],
        }
        with patch("automation.atistik_daily_job.parse_args", return_value=args), \
             patch("automation.atistik_daily_job.load_config", return_value={}), \
             patch("automation.atistik_daily_job.analyze_mode", return_value=report), \
             patch("automation.atistik_daily_job.persist_report"):
            exit_code = main()

        self.assertEqual(exit_code, 1)

    def test_summary_exposes_legacy_false_success_city_failure(self):
        legacy_report = {
            "mode": "analyze",
            "status": "completed",
            "totals": {"cities": 2, "analyzed": 1, "failed": 0},
            "cities": [
                {"city": "İzmir", "status": "ok", "races": [{"status": "analyzed"}]},
                {"city": "Ankara", "status": "failed", "error": "timeout", "races": []},
            ],
        }

        summary = build_summary(date(2026, 8, 15), legacy_report, None)

        self.assertIn("| Reported status | completed |", summary)
        self.assertIn("| Effective status | partial_success |", summary)
        self.assertIn("| Failed cities | 1 |", summary)
        self.assertIn("| Unresolved | 1 |", summary)


if __name__ == "__main__":
    unittest.main()
