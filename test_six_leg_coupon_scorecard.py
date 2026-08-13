from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from automation.six_leg_coupon_scorecard import (
    SCHEMA_VERSION,
    build_report,
    persist,
)


ROOT = Path(__file__).parent


def race_rows(
    race_date: date,
    race_no: int,
    winner_rank: int,
    *,
    city: str = "Ankara",
    city_id: str = "2",
    partial: bool = False,
    race_id_prefix: str = "R",
) -> list[dict]:
    field_size = 6
    other_ranks = [rank for rank in range(1, field_size + 1) if rank != winner_rank]
    rows = []
    for horse_index in range(field_size):
        is_winner = horse_index == 0
        predicted_rank = winner_rank if is_winner else other_ranks[horse_index - 1]
        finish_pos = horse_index + 1
        if partial and horse_index == field_size - 1:
            finish_pos = None
        rows.append({
            "race_id": f"{race_id_prefix}-{race_date.isoformat()}-{city_id}-{race_no}",
            "race_date": race_date.strftime("%d.%m.%Y"),
            "race_no": race_no,
            "race_time": f"{12 + race_no:02d}.00",
            "city": city,
            "city_id": city_id,
            "field_size": field_size,
            "horse_name": f"HORSE-{race_no}-{horse_index}",
            "finish_pos": finish_pos,
            "rank_pred": predicted_rank,
            "v4_version": "4.25",
            "ts": 1770000000 + race_no,
        })
    return rows


def card_rows(
    race_date: date,
    winner_ranks: list[int],
    *,
    partial_race_no: int | None = None,
    city: str = "Ankara",
    city_id: str = "2",
) -> list[dict]:
    rows = []
    for race_no, winner_rank in enumerate(winner_ranks, start=1):
        rows.extend(race_rows(
            race_date,
            race_no,
            winner_rank,
            city=city,
            city_id=city_id,
            partial=race_no == partial_race_no,
        ))
    return rows


class SixLegCouponScorecardTests(unittest.TestCase):
    def test_top_k_leg_hits_and_coupon_windows_are_counted(self):
        run_day = date(2026, 3, 30)
        entries = card_rows(run_day, [1, 2, 3, 4, 5, 6])

        report = build_report(entries, run_day.isoformat())
        summary = report["horizons"]["last7Days"]["cleanFull"]

        self.assertEqual(report["schemaVersion"], SCHEMA_VERSION)
        self.assertEqual(summary["windows"], 1)
        self.assertEqual(summary["topK"]["1"]["legHits"], 1)
        self.assertEqual(summary["topK"]["3"]["legHits"], 3)
        self.assertEqual(summary["topK"]["5"]["legHits"], 5)
        self.assertEqual(summary["topK"]["5"]["fivePlusWindows"], 1)
        self.assertEqual(summary["topK"]["5"]["sixOfSixWindows"], 0)
        self.assertFalse(report["policy"]["automaticWeightChange"])
        self.assertFalse(report["policy"]["usedForRanking"])

    def test_partial_winner_known_window_never_enters_clean_full(self):
        run_day = date(2026, 3, 30)
        entries = card_rows(run_day, [1, 1, 1, 1, 1, 1], partial_race_no=4)

        report = build_report(entries, run_day.isoformat())
        horizon = report["horizons"]["last7Days"]

        self.assertEqual(horizon["winnerKnown"]["windows"], 1)
        self.assertEqual(horizon["winnerKnown"]["topK"]["1"]["sixOfSixWindows"], 1)
        self.assertEqual(horizon["cleanFull"]["windows"], 0)
        self.assertEqual(report["input"]["raceQualityCounts"]["winner_known_partial"], 1)

    def test_all_contiguous_six_race_windows_are_measured(self):
        run_day = date(2026, 3, 30)
        entries = card_rows(run_day, [1, 1, 1, 1, 1, 1, 1])

        report = build_report(entries, run_day.isoformat())
        clean = report["horizons"]["last7Days"]["cleanFull"]
        details = report["windowDetailsLast30Days"]["cleanFull"]

        self.assertEqual(clean["windows"], 2)
        self.assertEqual([item["startRaceNo"] for item in details], [1, 2])
        self.assertEqual(clean["topK"]["1"]["sixOfSixWindows"], 2)

    def test_horizons_are_inclusive_and_cards_do_not_cross_city_or_day(self):
        run_day = date(2026, 3, 30)
        entries = []
        entries.extend(card_rows(run_day - timedelta(days=6), [1] * 6, city="Ankara", city_id="2"))
        entries.extend(card_rows(run_day - timedelta(days=7), [2] * 6, city="İzmir", city_id="6"))
        entries.extend(card_rows(run_day - timedelta(days=14), [3] * 6, city="Adana", city_id="1"))

        report = build_report(entries, run_day.isoformat())

        self.assertEqual(report["horizons"]["last7Days"]["winnerKnown"]["windows"], 1)
        self.assertEqual(report["horizons"]["last14Days"]["winnerKnown"]["windows"], 2)
        self.assertEqual(report["horizons"]["last30Days"]["winnerKnown"]["windows"], 3)
        self.assertEqual(report["input"]["cards"], 3)

    def test_missing_race_number_prevents_a_synthetic_window(self):
        run_day = date(2026, 3, 30)
        entries = []
        for race_no in (1, 2, 3, 5, 6, 7):
            entries.extend(race_rows(run_day, race_no, 1))

        report = build_report(entries, run_day.isoformat())

        self.assertEqual(report["input"]["structuralSixLegWindows"], 0)
        self.assertEqual(report["horizons"]["last7Days"]["winnerKnown"]["windows"], 0)

    def test_missing_city_is_reported_and_excluded_from_cards(self):
        run_day = date(2026, 3, 30)
        entries = card_rows(run_day, [1] * 6)
        for row in entries:
            row["city"] = None
            row["city_id"] = None

        report = build_report(entries, run_day.isoformat())

        self.assertEqual(report["input"]["missingCardIdentityRaces"], 6)
        self.assertEqual(report["input"]["cards"], 0)
        self.assertEqual(report["input"]["winnerKnownSixLegWindows"], 0)

    def test_impossible_partial_finish_position_is_not_winner_known_evidence(self):
        run_day = date(2026, 3, 30)
        entries = card_rows(run_day, [1] * 6, partial_race_no=4)
        bad_row = next(
            row for row in entries
            if row["race_no"] == 4 and row["finish_pos"] == 2
        )
        bad_row["finish_pos"] = 7

        report = build_report(entries, run_day.isoformat())

        self.assertEqual(report["input"]["raceQualityCounts"]["integrity_invalid"], 1)
        self.assertEqual(report["input"]["winnerKnownSixLegWindows"], 0)

    def test_persistence_and_direct_cli_write_daily_and_latest_artifacts(self):
        run_day = date(2026, 3, 30)
        entries = card_rows(run_day, [1, 2, 3, 4, 5, 6])
        report = build_report(entries, run_day.isoformat())

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = persist(report, root / "direct-data")
            self.assertTrue(Path(paths["dailyJson"]).exists())
            self.assertTrue(Path(paths["latestMarkdown"]).exists())

            predictions = root / "predictions.jsonl"
            predictions.write_text(
                "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in entries),
                encoding="utf-8",
            )
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "automation" / "six_leg_coupon_scorecard.py"),
                    "--predictions", str(predictions),
                    "--data-dir", str(root / "cli-data"),
                    "--run-date", run_day.isoformat(),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            output = json.loads(completed.stdout)
            self.assertTrue(output["success"])
            self.assertTrue(
                (root / "cli-data" / "automation" / "coupon-scorecard" / "latest.json").exists()
            )


if __name__ == "__main__":
    unittest.main()
