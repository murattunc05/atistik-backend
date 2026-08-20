import builtins
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).parent
for key, value in {
    "TELEGRAM_BOT_TOKEN": "test-token",
    "TELEGRAM_CHAT_ID": "test-chat",
    "GITHUB_TOKEN": "test-github",
    "BACKEND_REPO": "owner/backend",
    "ML_DATA_REPO": "owner/ml-data",
    "STATE_FILE": str(ROOT / "state-test.json"),
}.items():
    os.environ[key] = value

from telegram_bot import bot


def valid_analysis():
    return {
        "mode": "analyze",
        "status": "completed",
        "citiesRequested": ["İstanbul", "Ankara"],
        "totals": {
            "cities": 2,
            "racesFound": 1,
            "analyzed": 1,
            "failed": 0,
            "failedCities": 0,
            "unresolvedRaces": 0,
            "unresolved": 0,
        },
        "cities": [
            {
                "city": "İstanbul",
                "status": "ok",
                "races": [{"status": "analyzed", "rankings": []}],
            },
            {"city": "Ankara", "status": "no_races", "races": []},
        ],
    }


def aug15_false_success():
    report = valid_analysis()
    report["cities"][1] = {
        "city": "Ankara",
        "status": "failed",
        "error": "timeout",
        "races": [],
    }
    # Preserve the old misleading counters/status exactly: failed city was
    # nested but totals.failed remained zero.
    report["status"] = "completed"
    report["totals"]["failed"] = 0
    report["totals"].pop("failedCities")
    report["totals"].pop("unresolved")
    return report


class TelegramAnalysisGateTests(unittest.TestCase):
    def tearDown(self):
        os.environ.pop("TARGET_DATE", None)

    def test_completed_resolved_manifest_is_accepted(self):
        self.assertTrue(bot.analysis_manifest_complete(valid_analysis()))
        self.assertEqual(bot.analysis_manifest_issues(valid_analysis()), [])

    def test_legacy_false_success_failed_city_is_rejected(self):
        report = aug15_false_success()
        self.assertFalse(bot.analysis_manifest_complete(report))
        self.assertIn("unresolved_city_or_race", bot.analysis_manifest_issues(report))

    def test_missing_requested_city_manifest_is_rejected(self):
        report = valid_analysis()
        report.pop("citiesRequested")

        self.assertFalse(bot.analysis_manifest_complete(report))
        self.assertIn("cities_requested_missing", bot.analysis_manifest_issues(report))

    def test_incomplete_manifest_is_neither_sent_nor_marked_notified(self):
        state = {"notified_analysis_dates": ["2026-08-14"]}
        sent = []
        saved = []
        os.environ["TARGET_DATE"] = "2026-08-15"
        with patch.object(bot, "load_state", return_value=state), \
             patch.object(bot, "fetch_analysis", return_value=aug15_false_success()), \
             patch.object(bot, "send_telegram", side_effect=sent.append), \
             patch.object(bot, "save_state", side_effect=lambda payload: saved.append(dict(payload))):
            bot.main()

        self.assertEqual(sent, [])
        self.assertEqual(saved, [])
        self.assertEqual(state, {"notified_analysis_dates": ["2026-08-14"]})

    def test_valid_manifest_is_sent_and_only_then_marked_notified(self):
        state = {"notified_analysis_dates": []}
        events = []
        os.environ["TARGET_DATE"] = "2026-08-16"
        with patch.object(bot, "load_state", return_value=state), \
             patch.object(bot, "fetch_analysis", return_value=valid_analysis()), \
             patch.object(bot, "format_analysis_message_with_confidence", return_value="message"), \
             patch.object(bot, "send_telegram", side_effect=lambda message: events.append(("sent", message))), \
             patch.object(bot, "save_state", side_effect=lambda payload: events.append(("saved", dict(payload)))):
            bot.main()

        self.assertEqual(events[0], ("sent", "message"))
        self.assertEqual(events[1][0], "saved")
        self.assertEqual(state["notified_analysis_dates"], ["2026-08-16"])

    def test_formatter_import_works_in_package_context(self):
        with patch(
            "telegram_bot.message_formatter.format_analysis_message",
            return_value="package-formatter",
        ):
            message = bot.format_analysis_message_with_confidence(
                "2026-08-16", valid_analysis(), None
            )

        self.assertEqual(message, "package-formatter")

    def test_formatter_import_falls_back_in_standalone_context(self):
        real_import = builtins.__import__

        def standalone_import(name, *args, **kwargs):
            if name == "telegram_bot.message_formatter":
                raise ImportError("package unavailable in standalone deployment")
            return real_import(name, *args, **kwargs)

        formatter_dir = str(ROOT / "telegram_bot")
        sys.modules.pop("message_formatter", None)
        with patch.object(sys, "path", [formatter_dir, *sys.path]), \
             patch("builtins.__import__", side_effect=standalone_import):
            message = bot.format_analysis_message_with_confidence(
                "2026-08-16", valid_analysis(), None
            )

        self.assertIn("Atistik gunluk analiz tamamlandi", message)


if __name__ == "__main__":
    unittest.main()
