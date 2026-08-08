import unittest

from telegram_bot.message_formatter import format_analysis_message


def analysis_payload(decision=None, breakdown=None):
    race = {
        "raceNo": "1",
        "raceType": "ŞARTLI 1",
        "horseCount": 4,
        "rankings": [
            {"horse": "DÖRT", "no": "4", "v4Rank": 4, "v4Score": 48.0},
            {"horse": "BİR", "no": "1", "v4Rank": 1, "v4Score": 51.0},
            {"horse": "İKİ", "no": "2", "v4Rank": 2, "v4Score": 50.5},
            {"horse": "ÜÇ", "no": "3", "v4Rank": 3, "v4Score": 50.0},
        ],
    }
    if decision is not None:
        race["decisionConfidence"] = decision
    if breakdown is not None:
        race["confidenceBreakdown"] = breakdown
    return {
        "totals": {"analyzed": 1, "racesFound": 1, "failed": 0},
        "cities": [{"city": "Ankara", "races": [race]}],
    }


class TelegramMessageFormatterTests(unittest.TestCase):
    def test_low_confidence_race_is_explicit_and_explained(self):
        message = format_analysis_message(
            "2026-08-09",
            analysis_payload(
                decision={"label": "LOW", "lowConfidence": True, "openRace": True},
                breakdown={
                    "separation": {"top3Top4Gap": 0.2, "cutoffCrowd2pt": 7},
                    "data": {"weightedRealCoverage": 0.72},
                },
            ),
        )

        self.assertIn("⚠ DÜŞÜK GÜVEN / AÇIK YARIŞ", message)
        self.assertIn("Top3 sınırı 0.2 puan", message)
        self.assertIn("±2 puanda 7 at", message)
        self.assertIn("gerçek kaynak %72", message)

    def test_non_low_race_keeps_message_compact(self):
        message = format_analysis_message(
            "2026-08-09",
            analysis_payload(
                decision={
                    "label": "MEDIUM_UNCALIBRATED",
                    "lowConfidence": False,
                    "openRace": False,
                },
            ),
        )

        self.assertNotIn("DÜŞÜK GÜVEN", message)
        self.assertLess(message.index("1. BİR"), message.index("4. DÖRT"))

    def test_old_analysis_without_confidence_stays_compatible(self):
        message = format_analysis_message("2026-08-09", analysis_payload())

        self.assertIn("Ankara 1. Kosu", message)
        self.assertNotIn("DÜŞÜK GÜVEN", message)


if __name__ == "__main__":
    unittest.main()
