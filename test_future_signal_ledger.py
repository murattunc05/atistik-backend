import unittest

from automation.future_signal_ledger import (
    SCHEMA_VERSION,
    build_horse_signal_telemetry,
    build_race_signal_ledger,
)


def race(date, distance, track, seconds, adjusted=None):
    return {
        "date": date,
        "distance": distance,
        "track": track,
        "trackCondition": "Normal",
        "raceType": "Handikap 16",
        "degreeInSeconds": seconds,
        "adjustedDegreeInSeconds": adjusted if adjusted is not None else seconds,
    }


class FutureSignalLedgerTest(unittest.TestCase):
    def test_rejects_cross_distance_fallback_from_comparable_signal(self):
        result = build_horse_signal_telemetry(
            [
                race("10.08.2026", 1200, "Kum", 72.0),
                race("01.08.2026", 1400, "Kum", 85.0),
                race("20.07.2026", 2000, "Kum", 126.0),
            ],
            target_distance=1800,
            target_track="Kum",
        )

        self.assertEqual(result["source"]["comparableTimedRaceCount"], 0)
        self.assertTrue(result["source"]["legacyFallbackWouldMixDistances"])
        self.assertIn("LEGACY_FALLBACK_MIXES_DISTANCES", result["reasonCodes"])
        self.assertIsNone(result["features"]["recentVsBaselineSpeedPct"])
        self.assertFalse(result["usedForRanking"])

    def test_uses_only_same_surface_and_close_distance(self):
        result = build_horse_signal_telemetry(
            [
                race("10.08.2026", 1900, "Kum", 119.0, 116.0),
                race("01.08.2026", 1800, "Kum", 114.0, 112.0),
                race("20.07.2026", 1900, "Çim", 113.0, 111.0),
                race("10.07.2026", 1600, "Kum", 99.0, 97.0),
            ],
            target_distance="1900 m",
            target_track="Kum Normal",
        )

        self.assertEqual(result["source"]["comparableTimedRaceCount"], 2)
        self.assertEqual(result["reliability"], "LOW")
        self.assertTrue(result["flags"]["hasComparableTimedRaces"])
        self.assertFalse(result["flags"]["hasComparableTrend"])
        self.assertIsNotNone(result["features"]["recent2AdjustedSpeedMps"])
        self.assertTrue(result["source"]["legacyDistancePoolWouldMixSurfaces"])
        self.assertIn("LEGACY_DISTANCE_POOL_MIXES_SURFACES", result["reasonCodes"])

    def test_accepts_tjk_surface_condition_codes(self):
        cases = (("K:Normal", "Kum"), ("Ç:Normal", "Çim"), ("S:Normal", "Sentetik"))
        for history_track, target_track in cases:
            with self.subTest(history_track=history_track):
                result = build_horse_signal_telemetry(
                    [
                        race("10.08.2026", 1600, history_track, 96.0),
                        race("01.08.2026", 1600, history_track, 97.0),
                    ],
                    target_distance=1600,
                    target_track=target_track,
                )

                self.assertEqual(result["source"]["comparableTimedRaceCount"], 2)
                self.assertTrue(result["flags"]["hasComparableTimedRaces"])

    def test_recent_vs_baseline_and_trend_are_pre_race_raw_telemetry(self):
        result = build_horse_signal_telemetry(
            [
                race("10.08.2026", 1600, "Kum", 96.0),
                race("01.08.2026", 1600, "Kum", 97.0),
                race("20.07.2026", 1600, "Kum", 100.0),
                race("10.07.2026", 1600, "Kum", 101.0),
                race("01.07.2026", 1600, "Kum", 102.0),
            ],
            target_distance=1600,
            target_track="Kum",
        )

        self.assertGreater(result["features"]["recentVsBaselineSpeedPct"], 0.0)
        self.assertGreater(result["features"]["speedTrendPctPerRace"], 0.0)
        self.assertEqual(result["reliability"], "HIGH")
        self.assertEqual(result["schemaVersion"], SCHEMA_VERSION)
        self.assertFalse(result["rolloutEligible"])

    def test_bad_time_is_counted_but_not_used(self):
        result = build_horse_signal_telemetry(
            [race("10.08.2026", 1600, "Kum", 20.0)],
            target_distance=1600,
            target_track="Kum",
        )

        self.assertEqual(result["source"]["timedRaceCount"], 1)
        self.assertEqual(result["source"]["rejectedImplausibleSpeedCount"], 1)
        self.assertEqual(result["source"]["comparableTimedRaceCount"], 0)

    def test_race_ledger_marks_large_field_and_never_ranks(self):
        history = [
            race("10.08.2026", 1600, "Kum", 96.0),
            race("01.08.2026", 1600, "Kum", 97.0),
            race("20.07.2026", 1600, "Kum", 100.0),
            race("10.07.2026", 1600, "Kum", 101.0),
        ]
        horses = [
            {"name": f"H{i}", "raceHistory": history}
            for i in range(12)
        ]
        ledger = build_race_signal_ledger(
            horses,
            target_distance=1600,
            target_track="Kum",
            profile="H16",
        )

        self.assertTrue(ledger["context"]["isLargeField"])
        self.assertTrue(ledger["coverage"]["fieldScoresInformative"])
        self.assertFalse(ledger["usedForRanking"])
        self.assertEqual(ledger["promotionPolicy"]["state"], "COLLECT_ONLY")
        self.assertEqual(
            ledger["horses"][0]["fieldDiagnosticScores"]["recentComparableSpeed"],
            50.0,
        )

    def test_maiden_is_flagged_as_data_poor_context(self):
        ledger = build_race_signal_ledger(
            [{"name": "Debutant", "raceHistory": []}],
            target_distance=1200,
            target_track="Çim",
            profile="MAIDEN",
        )

        self.assertTrue(ledger["context"]["isDataPoorProfile"])
        self.assertFalse(ledger["coverage"]["fieldScoresInformative"])
        self.assertTrue(
            ledger["horses"][0]["telemetry"]["limitations"]["historicFieldSizeUnavailable"]
        )

        sart1 = build_race_signal_ledger(
            [{"name": "Runner", "raceHistory": []}],
            target_distance=1200,
            target_track="Kum",
            profile="Şartlı 1",
        )
        self.assertTrue(sart1["context"]["isDataPoorProfile"])

    def test_target_city_coverage_is_telemetry_not_a_hidden_fallback(self):
        result = build_horse_signal_telemetry(
            [
                {**race("10.08.2026", 1600, "Kum", 96.0), "city": "Ankara"},
                {**race("01.08.2026", 1600, "Kum", 97.0), "city": "İstanbul"},
                {**race("20.07.2026", 1600, "Kum", 99.0), "city": "İstanbul"},
            ],
            target_distance=1600,
            target_track="Kum",
            target_city="Ankara",
        )

        self.assertEqual(result["source"]["sameTargetCityComparableCount"], 1)
        self.assertIn("CROSS_CITY_COMPARISON_ONLY", result["reasonCodes"])

if __name__ == "__main__":
    unittest.main()
