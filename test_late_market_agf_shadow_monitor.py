import copy
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from automation.late_market_agf_shadow import build_snapshot, clean_name, sha256_payload
from automation.late_market_agf_shadow_monitor import (
    _competitive_evaluation_rows,
    build_report,
    persist,
)
from test_late_market_agf_shadow import analysis_manifest, live_race, prediction_rows


ISTANBUL = ZoneInfo("Europe/Istanbul")


def race(index, labeled=True):
    day = date(2026, 8, 14) + timedelta(days=index)
    race_id = f"LM-{index:03d}"
    analysis = copy.deepcopy(analysis_manifest())
    manifest = analysis["cities"][0]["races"][0]
    manifest["raceId"] = race_id
    rows = copy.deepcopy(prediction_rows())
    for row in rows:
        row["race_id"] = race_id
        row["race_date"] = day.strftime("%d.%m.%Y")
        row["finish_pos"] = row["v4_rank"] if labeled else None
        row["ts"] = int(
            datetime(day.year, day.month, day.day, 6, 37, tzinfo=ISTANBUL).timestamp()
        )
    live = copy.deepcopy(live_race())
    live["raceId"] = race_id
    snapshot, reason = build_snapshot(
        day=day,
        manifest=manifest,
        baseline_rows=rows,
        live_race=live,
        city_id="3",
        city_name="Bursa",
        source_url=(
            "http://127.0.0.1:5000/daily-program?"
            f"date={day.strftime('%d%%2F%m%%2F%Y')}&cityId=3&cityName=Bursa"
        ),
        collected_at=datetime(day.year, day.month, day.day, 10, 0, tzinfo=ISTANBUL),
    )
    assert reason == "accepted" and snapshot is not None
    return snapshot, rows


class LateMarketAgfShadowMonitorTests(unittest.TestCase):
    def test_legacy_subsecond_lead_rounding_is_accepted_but_larger_drift_is_not(self):
        snapshot, rows = race(0)
        calculated = (
            snapshot["identity"]["raceStartTs"] - snapshot["collectedTs"]
        ) / 60.0

        legacy = copy.deepcopy(snapshot)
        legacy["leadMinutes"] = round(calculated - 0.7 / 60.0, 3)
        unhashed = dict(legacy)
        unhashed.pop("snapshotSha256", None)
        legacy["snapshotSha256"] = sha256_payload(unhashed)
        accepted = build_report([legacy], rows, "2026-08-14")
        self.assertEqual(accepted["coverage"]["fullyLabeledRaces"], 1)
        self.assertEqual(accepted["coverage"]["integrityInvalidRaces"], 0)

        drifted = copy.deepcopy(snapshot)
        drifted["leadMinutes"] = round(calculated - 1.2 / 60.0, 3)
        unhashed = dict(drifted)
        unhashed.pop("snapshotSha256", None)
        drifted["snapshotSha256"] = sha256_payload(unhashed)
        rejected = build_report([drifted], rows, "2026-08-14")
        self.assertEqual(rejected["coverage"]["fullyLabeledRaces"], 0)
        self.assertEqual(
            rejected["coverage"]["integrityInvalidReasons"],
            {"causality": 1},
        )

    def test_unlabeled_is_kept_out_of_performance(self):
        snapshot, rows = race(0, labeled=False)
        report = build_report([snapshot], rows, "2026-08-14")

        self.assertEqual(report["coverage"]["unlabeledRaces"], 1)
        self.assertEqual(report["overall"]["races"], 0)
        self.assertFalse(report["liveRolloutEligible"])

    def test_tampered_formula_or_hash_is_integrity_invalid(self):
        snapshot, rows = race(0)
        snapshot["runners"][0]["candidateScore"] += 1.0
        report = build_report([snapshot], rows, "2026-08-14")

        self.assertEqual(report["coverage"]["integrityInvalidRaces"], 1)
        self.assertEqual(report["coverage"]["fullyLabeledRaces"], 0)

    def test_rehashed_component_tamper_is_still_formula_invalid(self):
        snapshot, rows = race(0)
        snapshot["runners"][0]["agfComponent"] = 50.0
        snapshot["runners"][0]["candidateScore"] = round(
            0.9 * snapshot["runners"][0]["baselineComponent"] + 5.0, 6
        )
        unhashed = dict(snapshot)
        unhashed.pop("snapshotSha256", None)
        snapshot["snapshotSha256"] = sha256_payload(unhashed)

        report = build_report([snapshot], rows, "2026-08-14")
        self.assertEqual(
            report["coverage"]["integrityInvalidReasons"],
            {"agf_component_formula": 1},
        )

    def test_malformed_race_date_is_invalid_instead_of_crashing(self):
        snapshot, rows = race(0)
        snapshot["identity"]["raceDate"] = "not-a-date"
        unhashed = dict(snapshot)
        unhashed.pop("snapshotSha256", None)
        snapshot["snapshotSha256"] = sha256_payload(unhashed)

        report = build_report([snapshot], rows, "2026-08-14")
        self.assertEqual(report["coverage"]["integrityInvalidRaces"], 1)
        self.assertEqual(report["coverage"]["fullyLabeledRaces"], 0)

    def test_rehashed_non_loopback_source_is_invalid(self):
        snapshot, rows = race(0)
        snapshot["source"]["requestUrl"] = (
            "https://evil.example/daily-program?date=14%2F08%2F2026&cityId=3"
        )
        unhashed = dict(snapshot)
        unhashed.pop("snapshotSha256", None)
        snapshot["snapshotSha256"] = sha256_payload(unhashed)

        report = build_report([snapshot], rows, "2026-08-14")
        self.assertEqual(
            report["coverage"]["integrityInvalidReasons"],
            {"source_request_identity": 1},
        )

    def test_rehashed_malformed_source_port_is_invalid_not_crash(self):
        snapshot, rows = race(0)
        snapshot["source"]["requestUrl"] = (
            "http://127.0.0.1:bad/daily-program?date=14%2F08%2F2026&cityId=3"
        )
        unhashed = dict(snapshot)
        unhashed.pop("snapshotSha256", None)
        snapshot["snapshotSha256"] = sha256_payload(unhashed)

        report = build_report([snapshot], rows, "2026-08-14")
        self.assertEqual(
            report["coverage"]["integrityInvalidReasons"],
            {"source_request_identity": 1},
        )

    def test_verified_derecesiz_is_kept_as_competitive_last_rank(self):
        snapshot, rows = race(0)
        rows[-1].update(
            {
                "finish_pos": 99,
                "result_status": "unranked_terminal",
                "terminal_reason": "Derecesiz",
                "result_source": "tjk_official_results",
            }
        )

        report = build_report([snapshot], rows, "2026-08-14")

        self.assertEqual(report["coverage"]["fullyLabeledRaces"], 1)
        self.assertEqual(report["coverage"]["integrityInvalidRaces"], 0)
        self.assertEqual(report["overall"]["races"], 1)
        self.assertEqual(report["overall"]["baseline"]["mae"], 0.0)

    def test_post_snapshot_non_runner_is_not_performance_or_promotion_evidence(self):
        snapshot, rows = race(0)
        rows[1].update(
            {
                "finish_pos": 99,
                "result_status": "non_runner",
                "terminal_reason": "Koşmaz",
                "result_source": "tjk_official_results",
            }
        )
        for finish_pos, row in enumerate((rows[0], *rows[2:]), start=1):
            row["finish_pos"] = finish_pos

        predictions = {clean_name(row["horse_name"]): row for row in rows}
        competitive = _competitive_evaluation_rows(snapshot, predictions)
        collapsed = {row["horse_name"]: row for row in competitive}
        report = build_report([snapshot], rows, "2026-08-14")

        self.assertNotIn("HORSE2", collapsed)
        self.assertEqual(collapsed["HORSE3"]["baseline_rank"], 2)
        self.assertEqual(collapsed["HORSE3"]["candidate_rank"], 2)
        self.assertEqual(report["coverage"]["postSnapshotNonRunnerRaces"], 1)
        self.assertEqual(report["coverage"]["fullyLabeledRaces"], 0)
        self.assertEqual(report["overall"]["races"], 0)
        self.assertEqual(report["profiles"]["MAIDEN"]["cumulative"]["races"], 0)
        self.assertFalse(report["profiles"]["MAIDEN"]["formalReplaySupported"])

    def test_metadata_less_99_is_excluded_and_fails_closed(self):
        snapshot, rows = race(0)
        rows[1]["finish_pos"] = 99
        for finish_pos, row in enumerate((rows[0], *rows[2:]), start=1):
            row["finish_pos"] = finish_pos

        predictions = {clean_name(row["horse_name"]): row for row in rows}
        competitive = _competitive_evaluation_rows(snapshot, predictions)
        report = build_report([snapshot], rows, "2026-08-14")

        self.assertEqual(len(competitive), 4)
        self.assertEqual(report["coverage"]["fullyLabeledRaces"], 0)
        self.assertEqual(report["coverage"]["integrityInvalidRaces"], 1)
        self.assertEqual(
            report["coverage"]["integrityInvalidReasons"],
            {"unverified_terminal_99": 1},
        )
        self.assertEqual(report["overall"]["races"], 0)

    def test_fifteen_clean_but_neutral_races_do_not_unlock_formal_replay(self):
        snapshots = []
        rows = []
        for index in range(15):
            snapshot, prediction_rows_for_race = race(index)
            snapshots.append(snapshot)
            rows.extend(prediction_rows_for_race)
        report = build_report(snapshots, rows, "2026-08-28")
        maiden = report["profiles"]["MAIDEN"]

        self.assertEqual(report["coverage"]["fullyLabeledRaces"], 15)
        self.assertEqual([item["raceCount"] for item in maiden["checkpoints"]], [5, 10, 15])
        self.assertTrue(all(not item["passed"] for item in maiden["checkpoints"]))
        self.assertIn(
            "winner_top3_no_genuine_gain",
            maiden["checkpoints"][-1]["failures"],
        )
        self.assertFalse(maiden["formalReplaySupported"])
        self.assertEqual(maiden["status"], "COLLECTING_OR_REJECTED")
        self.assertFalse(maiden["liveRolloutEligible"])
        self.assertFalse(report["liveRolloutEligible"])

    def test_persist_writes_daily_and_latest_without_predictions(self):
        snapshot, rows = race(0)
        report = build_report([snapshot], rows, "2026-08-14")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            persist(report, root)
            self.assertTrue(
                (root / "automation" / "runs" / "2026-08-14" / "late-market-agf-checkpoint.json").exists()
            )
            self.assertTrue((root / "automation" / "late-market-agf" / "latest.json").exists())
            self.assertTrue((root / "automation" / "late-market-agf" / "latest.md").exists())
            self.assertFalse((root / "predictions.jsonl").exists())


if __name__ == "__main__":
    unittest.main()
