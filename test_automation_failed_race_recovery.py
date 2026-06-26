import argparse
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from automation.atistik_daily_job import analyze_mode


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


if __name__ == "__main__":
    unittest.main()
