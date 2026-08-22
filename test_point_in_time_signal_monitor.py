import unittest
from datetime import datetime
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from zoneinfo import ZoneInfo

from automation.future_signal_ledger import build_race_signal_ledger
from automation.point_in_time_signal_monitor import build_report, persist


ISTANBUL = ZoneInfo("Europe/Istanbul")


def ts(hour, minute):
    return int(datetime(2026, 8, 22, hour, minute, tzinfo=ISTANBUL).timestamp())


def make_race(race_id, profile, category, subtype, field_size, *, captured_ts=None, start="17:00"):
    scenario = "YAVAŞ"
    pressure = round(100.0 / field_size, 1)
    history = [{
        "date": "20.08.2026",
        "distance": 1600,
        "track": "Kum",
        "trackCondition": "Normal",
        "raceType": profile,
        "degreeInSeconds": 96.0,
        "adjustedDegreeInSeconds": 96.0,
    }]
    horses = []
    for index in range(field_size):
        horses.append({
            "name": f"{race_id}-H{index + 1}",
            "raceHistory": history,
            "paceInfo": {
                "runningStyle": "KAÇAK" if index == 0 else "TAKİPÇİ",
                "paceScenario": scenario,
                "kacakCount": 1,
                "pacePressure": pressure,
                "styleSource": "recent_finish_position_proxy",
            },
        })
    ledger = build_race_signal_ledger(
        horses,
        target_distance=1600,
        target_track="Kum",
        target_city="Ankara",
        profile=profile,
        race_id=race_id,
        race_date="22.08.2026",
        race_no="1",
        race_time=start,
        city_id="2",
        captured_ts=ts(12, 0) if captured_ts is None else captured_ts,
    )
    rows = []
    # Winner is visible rank 2, so the baseline Winner Top3 outcome is a hit.
    finish_by_index = [2, 1] + list(range(3, field_size + 1))
    for index, horse in enumerate(horses):
        signal_row = ledger["horses"][index]
        rows.append({
            "race_id": race_id,
            "race_date": "22.08.2026",
            "race_no": "1",
            "race_time": start,
            "city": "Ankara",
            "city_id": "2",
            "distance": 1600,
            "horse_name": horse["name"],
            "rank_pred": index + 1,
            "finish_pos": finish_by_index[index],
            "result_status": "finished",
            "result_source": "tjk_official_results",
            "field_size": field_size,
            "race_type": profile,
            "track": "Kum",
            "v4_profile": {"category": category, "subtype": subtype, "track": "Kum"},
            "future_signal_ledger": {
                "pointInTime": ledger["pointInTime"],
                "telemetry": signal_row["telemetry"],
                "fieldDiagnosticScores": signal_row["fieldDiagnosticScores"],
            },
        })
    return rows


class PointInTimeSignalMonitorTest(unittest.TestCase):
    def test_reports_required_profile_and_big_field_coverage_separately(self):
        entries = [
            *make_race("M1", "Maiden", "MAIDEN", "MAIDEN", 3),
            *make_race("S1", "Şartlı 1", "SARTLI", "SART1", 12),
            *make_race("T1", "Satış 2", "SATIS", "SATIS", 3),
        ]
        report = build_report(entries, generated_at="2026-08-22T12:00:00Z")
        scopes = {scope["scope"]: scope for scope in report["scopes"]}

        self.assertEqual(scopes["ALL"]["races"], 3)
        self.assertEqual(scopes["MAIDEN"]["races"], 1)
        self.assertEqual(scopes["SART1"]["races"], 1)
        self.assertEqual(scopes["SATIS"]["races"], 1)
        self.assertEqual(scopes["BIG_FIELD"]["races"], 1)
        for name in ("MAIDEN", "SART1", "SATIS", "BIG_FIELD"):
            self.assertEqual(scopes[name]["immutableValidRaces"], 1)
            self.assertEqual(scopes[name]["identityCompleteRaces"], 1)
            self.assertEqual(scopes[name]["winnerTop3"]["baselineEvaluableRaces"], 1)
            self.assertEqual(scopes[name]["winnerTop3"]["baselineHits"], 1)
            self.assertEqual(scopes[name]["priorTrackVariantAvailableRaces"], 0)
            self.assertEqual(scopes[name]["winnerTop3"]["candidateEvaluableRaces"], 0)

    def test_late_snapshot_is_counted_but_excluded_from_winner_top3_evidence(self):
        entries = make_race(
            "LATE",
            "Maiden",
            "MAIDEN",
            "MAIDEN",
            3,
            captured_ts=ts(18, 0),
            start="17:00",
        )
        scope = build_report(entries)["scopes"][0]

        self.assertEqual(scope["fullyLabeledRaces"], 1)
        self.assertEqual(scope["preRaceTimingValidRaces"], 0)
        self.assertEqual(scope["evidenceFullyLabeledRaces"], 0)
        self.assertEqual(scope["winnerTop3"]["baselineEvaluableRaces"], 0)
        self.assertEqual(scope["winnerTop3"]["baselineHits"], 0)

    def test_partial_snapshot_is_observed_but_fails_immutable_evidence_gate(self):
        entries = make_race("PARTIAL", "Maiden", "MAIDEN", "MAIDEN", 3)
        entries[0].pop("future_signal_ledger")
        scope = build_report(entries)["scopes"][0]

        self.assertEqual(scope["races"], 1)
        self.assertEqual(scope["immutableValidRaces"], 0)
        self.assertEqual(scope["evidenceFullyLabeledRaces"], 0)
        self.assertEqual(scope["winnerTop3"]["baselineEvaluableRaces"], 0)

    def test_outer_coverage_flags_cannot_turn_late_snapshot_into_evidence(self):
        entries = make_race(
            "LATE-TAMPER",
            "Maiden",
            "MAIDEN",
            "MAIDEN",
            3,
            captured_ts=ts(18, 0),
            start="17:00",
        )
        for row in entries:
            point = row["future_signal_ledger"]["pointInTime"]
            point["coverage"]["identityComplete"] = True
            point["coverage"]["snapshotTimingValid"] = True
            point["coverage"]["fieldSizeAvailable"] = True
        scope = build_report(entries)["scopes"][0]

        self.assertEqual(scope["immutableValidRaces"], 0)
        self.assertEqual(scope["preRaceTimingValidRaces"], 0)
        self.assertEqual(scope["evidenceFullyLabeledRaces"], 0)

    def test_snapshot_roster_count_must_match_complete_prediction_rows(self):
        entries = make_race("ROSTER", "Maiden", "MAIDEN", "MAIDEN", 3)[:2]
        for row in entries:
            row["field_size"] = 2
        scope = build_report(entries)["scopes"][0]

        self.assertEqual(scope["fullyLabeledRaces"], 1)
        self.assertEqual(scope["fieldSizeAvailableRaces"], 0)
        self.assertEqual(scope["immutableValidRaces"], 0)
        self.assertEqual(scope["evidenceFullyLabeledRaces"], 0)

    def test_same_size_runner_substitution_fails_roster_contract(self):
        entries = make_race("SUBSTITUTE", "Maiden", "MAIDEN", "MAIDEN", 3)
        entries[1]["horse_name"] = "UNRELATED HORSE"
        scope = build_report(entries)["scopes"][0]

        self.assertEqual(scope["fieldSizeAvailableRaces"], 0)
        self.assertEqual(scope["immutableValidRaces"], 0)
        self.assertEqual(scope["evidenceFullyLabeledRaces"], 0)

    def test_runner_signal_mutation_fails_immutable_evidence_gate(self):
        entries = make_race("SIGNAL-MUTATE", "Maiden", "MAIDEN", "MAIDEN", 3)
        telemetry = entries[1]["future_signal_ledger"]["telemetry"]
        telemetry["features"]["recent2RawSpeedMps"] = 999.0
        scope = build_report(entries)["scopes"][0]

        self.assertEqual(scope["fieldSizeAvailableRaces"], 0)
        self.assertEqual(scope["immutableValidRaces"], 0)
        self.assertEqual(scope["evidenceFullyLabeledRaces"], 0)

    def test_every_runner_wrapper_must_remain_nonranking(self):
        entries = make_race("WRAPPER", "Maiden", "MAIDEN", "MAIDEN", 3)
        entries[1]["future_signal_ledger"]["pointInTime"] = {
            **entries[1]["future_signal_ledger"]["pointInTime"],
            "usedForRanking": True,
        }
        scope = build_report(entries)["scopes"][0]

        self.assertEqual(scope["immutableValidRaces"], 0)
        self.assertEqual(scope["evidenceFullyLabeledRaces"], 0)

    def test_nonofficial_labels_never_enter_winner_top3_evidence(self):
        entries = make_race("FALLBACK", "Maiden", "MAIDEN", "MAIDEN", 3)
        entries[0]["result_source"] = "horse_history_fallback"
        scope = build_report(entries)["scopes"][0]

        self.assertEqual(scope["fullyLabeledRaces"], 1)
        self.assertEqual(scope["officialFullyLabeledRaces"], 0)
        self.assertEqual(scope["evidenceFullyLabeledRaces"], 0)
        self.assertEqual(scope["winnerTop3"]["baselineEvaluableRaces"], 0)

    def test_persist_writes_atomic_daily_and_latest_json(self):
        report = build_report(
            make_race("M1", "Maiden", "MAIDEN", "MAIDEN", 3),
            generated_at="2026-08-22T12:00:00Z",
            run_date="2026-08-22",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = persist(report, root, "2026-08-22")
            daily = root / "automation" / "runs" / "2026-08-22" / "point-in-time-signal-coverage.json"
            latest = root / "automation" / "point-in-time-signals" / "latest.json"
            self.assertEqual(Path(paths["dailyJson"]), daily)
            self.assertEqual(Path(paths["latestJson"]), latest)
            self.assertEqual(json.loads(daily.read_text(encoding="utf-8")), report)
            self.assertEqual(daily.read_bytes(), latest.read_bytes())
            self.assertFalse(daily.with_suffix(".json.tmp").exists())

    def test_cli_help_and_pi_results_wiring_are_nonblocking(self):
        root = Path(__file__).parent
        completed = subprocess.run(
            [sys.executable, "automation/point_in_time_signal_monitor.py", "--help"],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

        script = (root / "scripts" / "raspberry" / "run-automation.sh").read_text(
            encoding="utf-8"
        )
        persist_at = script.rindex("persist_state_predictions")
        monitor_at = script.index("python3 automation/point_in_time_signal_monitor.py")
        commit_at = script.index('git -C "$DATA_DIR" add automation predictions.jsonl')
        self.assertLess(persist_at, monitor_at)
        self.assertLess(monitor_at, commit_at)
        self.assertIn("if ! python3 automation/point_in_time_signal_monitor.py", script)
        self.assertIn('--data-dir "$DATA_DIR"', script[monitor_at:])
        self.assertIn('--run-date "$MONITOR_DATE"', script[monitor_at:])


if __name__ == "__main__":
    unittest.main()
