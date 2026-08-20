import hashlib
import json
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from automation.late_market_agf_shadow import (
    ALPHA,
    VERSION,
    build_snapshot,
    collect,
    parse_at_id,
    persist,
)


ISTANBUL = ZoneInfo("Europe/Istanbul")
DAY = date(2026, 8, 14)
SOURCE_URL = (
    "http://127.0.0.1:5000/daily-program?"
    "date=14%2F08%2F2026&cityId=3&cityName=Bursa"
)


def horse(index, agf):
    percent = None
    if isinstance(agf, str) and agf.startswith("%"):
        percent = float(agf[1:].split("(", 1)[0].replace(",", "."))
    return {
        "no": str(index),
        "name": f"HORSE {index}",
        "detailLink": f"/TR/AtKosuBilgileri?AtId={1000 + index}",
        "agf": agf,
        "agfDisplay": agf,
        "agfPools": (
            [{
                "poolNo": 1,
                "percent": percent,
                "rank": index,
                "raw": agf,
                "display": agf,
                "source": "title",
            }]
            if percent is not None else []
        ),
    }


def analysis_manifest():
    return {
        "date": DAY.isoformat(),
        "cities": [
            {
                "city": "Bursa",
                "cityId": "3",
                "races": [
                    {
                        "status": "analyzed",
                        "city": "Bursa",
                        "cityId": "3",
                        "raceId": "300001",
                        "raceNo": "1",
                        "raceType": "MAIDEN",
                        "time": "14.30",
                        "horses": [horse(i, "-") for i in range(1, 6)],
                    }
                ],
            }
        ],
    }


def prediction_rows():
    rows = []
    for index, score in enumerate((81, 72, 64, 59, 51), start=1):
        rows.append(
            {
                "race_id": "300001",
                "race_date": "14.08.2026",
                "race_no": "1",
                "race_time": "14.30",
                "city": "Bursa",
                "city_id": "3",
                "race_type": "MAIDEN",
                "horse_name": f"HORSE {index}",
                "horse_no": str(index),
                "v4_score": score,
                "v4_rank": index,
                "v4_version": "4.25",
                "v4_applied_for_ranking": True,
                "v4_profile": {"category": "MAIDEN", "subtype": "MAIDEN"},
                "v4_weights": {"agf_score": 0.0, "degree_avg": 100.0},
                "metric_source_flags": {"hasAgf": False},
                "ts": 1786678620,
                "finish_pos": None,
            }
        )
    return rows


def live_race(agfs=("%8,33(4)", "%31,25(1)", "%20,00(2)", "%15,42(3)", "%5,00(5)")):
    return {
        "raceId": "300001",
        "raceNo": "1",
        "raceNumber": "1",
        "raceType": "MAIDEN",
        "time": "14.30",
        "horses": [horse(index, agf) for index, agf in enumerate(agfs, start=1)],
    }


class LateMarketAgfShadowTests(unittest.TestCase):
    def test_collector_rejects_nonlocal_or_malformed_backend_before_fetch(self):
        for backend_url in (
            "https://evil.example:5000",
            "http://127.0.0.1:bad",
            "http://127.0.0.1:5001",
            "https://127.0.0.1:5000",
        ):
            with self.subTest(backend_url=backend_url), self.assertRaises(Exception):
                collect(
                    analysis=analysis_manifest(),
                    prediction_rows=prediction_rows(),
                    day=DAY,
                    backend_url=backend_url,
                    collected_at=datetime(2026, 8, 14, 10, 0, tzinfo=ISTANBUL),
                    program_loader=lambda *_args: self.fail("network loader called"),
                )

    def test_real_tjk_query_parameter_at_id_is_supported(self):
        self.assertEqual(
            parse_at_id(
                "/TR/YarisSever/Query/Page/AtKosuBilgileri?QueryParameter_AtId=116203"
            ),
            "116203",
        )
        self.assertEqual(
            parse_at_id(
                "https://www.tjk.org/x?foo=1&queryparameter_atid=116203&bar=2"
            ),
            "116203",
        )

    def test_snapshot_is_bounded_and_never_visible(self):
        snapshot, reason = build_snapshot(
            day=DAY,
            manifest=analysis_manifest()["cities"][0]["races"][0],
            baseline_rows=prediction_rows(),
            live_race=live_race(),
            city_id="3",
            city_name="Bursa",
            source_url=SOURCE_URL,
            collected_at=datetime(2026, 8, 14, 10, 0, tzinfo=ISTANBUL),
        )

        self.assertEqual(reason, "accepted")
        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot["version"], VERSION)
        self.assertEqual(snapshot["policy"]["alpha"], ALPHA)
        self.assertLessEqual(snapshot["policy"]["alpha"], 0.10)
        self.assertFalse(snapshot["usedForRanking"])
        self.assertFalse(snapshot["rolloutEligible"])
        self.assertFalse(snapshot["telegramVisible"])
        self.assertEqual(snapshot["coverage"]["ratio"], 1.0)
        self.assertEqual(snapshot["market"]["selectedPoolNo"], 1)
        self.assertTrue(snapshot["market"]["distinctOfficialPoolsPreserved"])
        self.assertEqual({r["atId"] for r in snapshot["runners"]}, {"1001", "1002", "1003", "1004", "1005"})

    def test_four_of_five_coverage_uses_neutral_missing_component(self):
        snapshot, reason = build_snapshot(
            day=DAY,
            manifest=analysis_manifest()["cities"][0]["races"][0],
            baseline_rows=prediction_rows(),
            live_race=live_race(("%8", "%31", "%20", "%15", "-")),
            city_id="3",
            city_name="Bursa",
            source_url=SOURCE_URL,
            collected_at=datetime(2026, 8, 14, 10, 0, tzinfo=ISTANBUL),
        )

        self.assertEqual(reason, "accepted")
        self.assertEqual(snapshot["coverage"]["ratio"], 0.8)
        missing = next(row for row in snapshot["runners"] if not row["hasAgf"])
        self.assertEqual(missing["agfComponent"], 50.0)

    def test_below_coverage_and_below_lead_time_are_rejected(self):
        common = dict(
            day=DAY,
            manifest=analysis_manifest()["cities"][0]["races"][0],
            baseline_rows=prediction_rows(),
            city_id="3",
            city_name="Bursa",
            source_url=SOURCE_URL,
        )
        snapshot, reason = build_snapshot(
            **common,
            live_race=live_race(("%8", "%31", "%20", "-", "-")),
            collected_at=datetime(2026, 8, 14, 10, 0, tzinfo=ISTANBUL),
        )
        self.assertIsNone(snapshot)
        self.assertEqual(reason, "agf_coverage_below_0_80")

        snapshot, reason = build_snapshot(
            **common,
            live_race=live_race(),
            collected_at=datetime(2026, 8, 14, 13, 1, tzinfo=ISTANBUL),
        )
        self.assertIsNone(snapshot)
        self.assertEqual(reason, "lead_time_below_90m")

    def test_at_id_or_name_change_fails_closed(self):
        changed = live_race()
        changed["horses"][0]["detailLink"] = "/TR/AtKosuBilgileri?AtId=9999"
        with self.assertRaisesRegex(Exception, "runner_identity_mismatch"):
            build_snapshot(
                day=DAY,
                manifest=analysis_manifest()["cities"][0]["races"][0],
                baseline_rows=prediction_rows(),
                live_race=changed,
                city_id="3",
                city_name="Bursa",
                source_url=SOURCE_URL,
                collected_at=datetime(2026, 8, 14, 10, 0, tzinfo=ISTANBUL),
            )

    def test_collection_causality_and_baseline_no_agf_are_fail_closed(self):
        common = dict(
            day=DAY,
            manifest=analysis_manifest()["cities"][0]["races"][0],
            live_race=live_race(),
            city_id="3",
            city_name="Bursa",
            source_url=SOURCE_URL,
        )
        with self.assertRaisesRegex(Exception, "collection_date_mismatch"):
            build_snapshot(
                **common,
                baseline_rows=prediction_rows(),
                collected_at=datetime(2026, 8, 13, 10, 0, tzinfo=ISTANBUL),
            )
        future_baseline = prediction_rows()
        future_baseline[0]["ts"] = int(
            datetime(2026, 8, 14, 10, 1, tzinfo=ISTANBUL).timestamp()
        )
        with self.assertRaisesRegex(Exception, "baseline_after_collection"):
            build_snapshot(
                **common,
                baseline_rows=future_baseline,
                collected_at=datetime(2026, 8, 14, 10, 0, tzinfo=ISTANBUL),
            )
        market_baseline = prediction_rows()
        for row in market_baseline:
            row["v4_weights"]["agf_score"] = 10.0
            row["metric_source_flags"]["hasAgf"] = True
        with self.assertRaisesRegex(Exception, "baseline_agf_already_applied"):
            build_snapshot(
                **common,
                baseline_rows=market_baseline,
                collected_at=datetime(2026, 8, 14, 10, 0, tzinfo=ISTANBUL),
            )
        missing_guard = prediction_rows()
        for row in missing_guard:
            row["metric_source_flags"].pop("hasAgf")
        with self.assertRaisesRegex(Exception, "baseline_agf_already_applied"):
            build_snapshot(
                **common,
                baseline_rows=missing_guard,
                collected_at=datetime(2026, 8, 14, 10, 0, tzinfo=ISTANBUL),
            )

    def test_subsecond_collection_uses_one_frozen_instant(self):
        collected_at = datetime(
            2026, 8, 14, 10, 0, 0, 730000, tzinfo=ISTANBUL
        )
        snapshot, reason = build_snapshot(
            day=DAY,
            manifest=analysis_manifest()["cities"][0]["races"][0],
            baseline_rows=prediction_rows(),
            live_race=live_race(),
            city_id="3",
            city_name="Bursa",
            source_url=SOURCE_URL,
            collected_at=collected_at,
        )

        self.assertEqual(reason, "accepted")
        self.assertIsNotNone(snapshot)
        calculated = (
            snapshot["identity"]["raceStartTs"] - snapshot["collectedTs"]
        ) / 60.0
        self.assertAlmostEqual(snapshot["leadMinutes"], round(calculated, 3), places=6)

    def test_non_runner_and_pool_raw_mismatch_fail_closed(self):
        changed = live_race()
        changed["horses"][0]["isNonRunner"] = True
        with self.assertRaisesRegex(Exception, "late_non_runner_present"):
            build_snapshot(
                day=DAY,
                manifest=analysis_manifest()["cities"][0]["races"][0],
                baseline_rows=prediction_rows(),
                live_race=changed,
                city_id="3",
                city_name="Bursa",
                source_url=SOURCE_URL,
                collected_at=datetime(2026, 8, 14, 10, 0, tzinfo=ISTANBUL),
            )
        changed = live_race()
        changed["horses"][0]["agfPools"][0]["percent"] += 1.0
        with self.assertRaisesRegex(Exception, "agf_pool_raw_invalid"):
            build_snapshot(
                day=DAY,
                manifest=analysis_manifest()["cities"][0]["races"][0],
                baseline_rows=prediction_rows(),
                live_race=changed,
                city_id="3",
                city_name="Bursa",
                source_url=SOURCE_URL,
                collected_at=datetime(2026, 8, 14, 10, 0, tzinfo=ISTANBUL),
            )

    def test_pool_aware_payload_is_required_and_pool_identity_cannot_mix(self):
        missing = live_race()
        del missing["horses"][0]["agfPools"]
        with self.assertRaisesRegex(Exception, "pool_aware_agf_payload_missing"):
            build_snapshot(
                day=DAY,
                manifest=analysis_manifest()["cities"][0]["races"][0],
                baseline_rows=prediction_rows(),
                live_race=missing,
                city_id="3",
                city_name="Bursa",
                source_url=SOURCE_URL,
                collected_at=datetime(2026, 8, 14, 10, 0, tzinfo=ISTANBUL),
            )

        mixed = live_race()
        mixed["horses"][0]["agfPools"][0]["poolNo"] = 2
        with self.assertRaisesRegex(Exception, "agf_primary_pool_identity_mismatch"):
            build_snapshot(
                day=DAY,
                manifest=analysis_manifest()["cities"][0]["races"][0],
                baseline_rows=prediction_rows(),
                live_race=mixed,
                city_id="3",
                city_name="Bursa",
                source_url=SOURCE_URL,
                collected_at=datetime(2026, 8, 14, 10, 0, tzinfo=ISTANBUL),
            )

    def test_collect_and_persist_leave_predictions_immutable_and_deduplicate(self):
        def loader(_url, _day, city_id, city_name, _timeout):
            return {
                "success": True,
                "url": SOURCE_URL,
                "cityId": city_id,
                "cityName": city_name,
                "races": [live_race()],
            }

        rows = prediction_rows()
        snapshots, report = collect(
            analysis=analysis_manifest(),
            prediction_rows=rows,
            day=DAY,
            backend_url="http://127.0.0.1:5000",
            collected_at=datetime(2026, 8, 14, 10, 0, tzinfo=ISTANBUL),
            program_loader=loader,
        )
        self.assertEqual(report["totals"]["accepted"], 1)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            predictions = root / "predictions.jsonl"
            predictions.write_text(
                "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
            )
            before = hashlib.sha256(predictions.read_bytes()).hexdigest()
            first = persist(root, DAY, snapshots, report)
            second = persist(root, DAY, snapshots, report)
            after = hashlib.sha256(predictions.read_bytes()).hexdigest()

            self.assertEqual(first["newSnapshots"], 1)
            self.assertEqual(second["newSnapshots"], 0)
            self.assertEqual(before, after)
            ledger = root / "automation" / "late-market-agf" / "snapshots.jsonl"
            self.assertEqual(len(ledger.read_text(encoding="utf-8").splitlines()), 1)


if __name__ == "__main__":
    unittest.main()
