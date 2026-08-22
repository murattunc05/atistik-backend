import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from automation.future_signal_ledger import (
    POINT_IN_TIME_SCHEMA_VERSION,
    SCHEMA_VERSION,
    TRACK_VARIANT_REFERENCE_SCHEMA_VERSION,
    build_horse_signal_telemetry,
    build_race_signal_ledger,
    verify_point_in_time_snapshot,
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
    @staticmethod
    def _race_ts(hour, minute):
        return int(
            datetime(2026, 8, 22, hour, minute, tzinfo=ZoneInfo("Europe/Istanbul")).timestamp()
        )

    @staticmethod
    def _pace_horses():
        history = [race("20.08.2026", 1600, "Kum", 96.0)]
        pace_common = {
            "paceScenario": "YAVAŞ",
            "kacakCount": 1,
            "pacePressure": 33.3,
            "styleSource": "recent_finish_position_proxy",
        }
        return [
            {"name": "A", "raceHistory": history, "paceInfo": {**pace_common, "runningStyle": "KAÇAK"}},
            {"name": "B", "raceHistory": history, "paceInfo": {**pace_common, "runningStyle": "TAKİPÇİ"}},
            {"name": "C", "raceHistory": history, "paceInfo": {**pace_common, "runningStyle": "BEKLEME"}},
        ]

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

    def test_point_in_time_snapshot_hashes_pre_race_pace_and_field_context(self):
        ledger = build_race_signal_ledger(
            self._pace_horses(),
            target_distance=1600,
            target_track="Kum",
            target_city="Ankara",
            profile="Satış 2",
            race_id="227001",
            race_date="22.08.2026",
            race_no="4",
            race_time="15:30",
            city_id="2",
            captured_ts=self._race_ts(12, 0),
        )

        point = ledger["pointInTime"]
        snapshot = point["preRaceSnapshot"]
        self.assertEqual(point["schemaVersion"], POINT_IN_TIME_SCHEMA_VERSION)
        self.assertTrue(verify_point_in_time_snapshot(point))
        self.assertEqual(snapshot["timingState"], "VALID_PRE_RACE")
        self.assertTrue(point["coverage"]["identityComplete"])
        self.assertEqual(snapshot["field"]["declaredRunnerCount"], 3)
        self.assertEqual(snapshot["pace"]["styleCounts"]["KAÇAK"], 1)
        self.assertEqual(snapshot["pace"]["sourceProvenRunnerCount"], 3)
        self.assertTrue(snapshot["coverageBuckets"]["isSatis"])
        self.assertFalse(point["usedForRanking"])
        self.assertFalse(point["sentToTelegram"])
        self.assertFalse(point["coverage"]["priorTrackVariantAvailable"])
        self.assertEqual(
            snapshot["trackVariant"]["reasonCodes"],
            ["NO_PRIOR_TRACK_VARIANT_STORE"],
        )

    def test_prior_track_variant_rejects_own_or_nonprior_race(self):
        reference = {
            "schemaVersion": TRACK_VARIANT_REFERENCE_SCHEMA_VERSION,
            "city": "Ankara",
            "surface": "Kum",
            "distanceBandM": 1600,
            "variantSecondsPer1000m": 0.35,
            "sourceRaceCount": 1,
            "sourceRaceIds": ["227001"],
            "sourceMaxRaceStartTs": self._race_ts(15, 30),
            "sourceMaxCompletedTs": self._race_ts(15, 45),
            "asOfTs": self._race_ts(15, 50),
        }
        ledger = build_race_signal_ledger(
            self._pace_horses(),
            target_distance=1600,
            target_track="Kum",
            target_city="Ankara",
            profile="MAIDEN",
            race_id="227001",
            race_date="22.08.2026",
            race_time="17:00",
            captured_ts=self._race_ts(16, 0),
            prior_track_variant=reference,
        )

        track_variant = ledger["pointInTime"]["preRaceSnapshot"]["trackVariant"]
        self.assertEqual(track_variant["state"], "UNAVAILABLE")
        self.assertIn("OWN_RACE_REFERENCE_FORBIDDEN", track_variant["reasonCodes"])

    def test_prior_track_variant_accepts_only_completed_reference_known_at_snapshot(self):
        reference = {
            "schemaVersion": TRACK_VARIANT_REFERENCE_SCHEMA_VERSION,
            "city": "Ankara",
            "surface": "Kum",
            "distanceBandM": 1600,
            "variantSecondsPer1000m": -0.42,
            "sourceRaceCount": 2,
            "sourceRaceIds": ["226999", "227000"],
            "sourceMaxRaceStartTs": self._race_ts(14, 30),
            "sourceMaxCompletedTs": self._race_ts(14, 45),
            "asOfTs": self._race_ts(14, 50),
        }
        ledger = build_race_signal_ledger(
            self._pace_horses(),
            target_distance=1600,
            target_track="Kum",
            target_city="Ankara",
            profile="Şartlı 1",
            race_id="227001",
            race_date="22.08.2026",
            race_time="17:00",
            captured_ts=self._race_ts(16, 0),
            prior_track_variant=reference,
        )

        point = ledger["pointInTime"]
        track_variant = point["preRaceSnapshot"]["trackVariant"]
        self.assertEqual(track_variant["state"], "AVAILABLE_PRIOR_ONLY")
        self.assertTrue(point["coverage"]["priorTrackVariantAvailable"])
        self.assertFalse(track_variant["usedForRanking"])

    def test_prior_track_variant_rejects_duplicate_sources_and_wrong_distance_band(self):
        reference = {
            "schemaVersion": TRACK_VARIANT_REFERENCE_SCHEMA_VERSION,
            "city": "Ankara",
            "surface": "Kum",
            "distanceBandM": 1400,
            "variantSecondsPer1000m": 0.1,
            "sourceRaceCount": 2,
            "sourceRaceIds": ["226999", "226999"],
            "sourceMaxRaceStartTs": self._race_ts(14, 30),
            "sourceMaxCompletedTs": self._race_ts(14, 45),
            "asOfTs": self._race_ts(14, 50),
        }
        ledger = build_race_signal_ledger(
            self._pace_horses(),
            target_distance=1600,
            target_track="Kum",
            target_city="Ankara",
            profile="Satış 2",
            race_id="227001",
            race_date="22.08.2026",
            race_time="17:00",
            captured_ts=self._race_ts(16, 0),
            prior_track_variant=reference,
        )

        reasons = ledger["pointInTime"]["preRaceSnapshot"]["trackVariant"]["reasonCodes"]
        self.assertIn("SOURCE_RACE_SET_INVALID", reasons)
        self.assertIn("DISTANCE_BAND_MISMATCH", reasons)

    def test_target_date_cutoff_rejects_same_day_future_and_target_identity_history(self):
        horses = self._pace_horses()
        horses[0]["raceHistory"] = [
            {**race("21.08.2026", 1600, "Kum", 96.0), "raceId": "PRIOR"},
            {**race("22.08.2026", 1600, "Kum", 95.0), "raceId": "SAME_DAY"},
            {**race("23.08.2026", 1600, "Kum", 94.0), "raceId": "FUTURE"},
            {**race("20.08.2026", 1600, "Kum", 93.0), "raceId": "TARGET"},
            {**race("", 1600, "Kum", 92.0), "raceId": "UNDATED"},
        ]
        ledger = build_race_signal_ledger(
            horses,
            target_distance=1600,
            target_track="Kum",
            target_city="Ankara",
            profile="Maiden",
            race_id="TARGET",
            race_date="22.08.2026",
            race_no="1",
            race_time="17:00",
            city_id="2",
            captured_ts=self._race_ts(12, 0),
        )
        source = ledger["horses"][0]["telemetry"]["source"]

        self.assertEqual(source["rawHistoryRaceCount"], 5)
        self.assertEqual(source["historyRaceCount"], 1)
        self.assertTrue(source["historyCutoffApplied"])
        self.assertTrue(source["historyCutoffValid"])
        self.assertEqual(source["rejectedSameOrFutureHistoryCount"], 2)
        self.assertEqual(source["rejectedTargetRaceIdentityCount"], 1)
        self.assertEqual(source["rejectedUndatedHistoryCount"], 1)

    def test_sartli_19_is_not_misclassified_as_sartli_1(self):
        ledger = build_race_signal_ledger(
            self._pace_horses(),
            target_distance=1600,
            target_track="Kum",
            target_city="Ankara",
            profile="Şartlı 19",
            race_id="227019",
            race_date="22.08.2026",
            race_no="1",
            race_time="17:00",
            city_id="2",
            captured_ts=self._race_ts(12, 0),
        )

        self.assertFalse(ledger["pointInTime"]["preRaceSnapshot"]["coverageBuckets"]["isSart1"])
        self.assertFalse(ledger["context"]["isDataPoorProfile"])

if __name__ == "__main__":
    unittest.main()
