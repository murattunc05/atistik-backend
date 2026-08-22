import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from automation.recent_top3_metric_audit import build_report, persist


def race_rows(day, race_id, *, missed=False, official=True):
    horses = [
        ("A", 80.0, 50.0, 50.0),
        ("B", 70.0, 50.0, 50.0),
        ("C", 60.0, 50.0, 50.0),
        ("WINNER", 55.0 if missed else 61.0, 100.0, 100.0),
    ]
    baseline = sorted(
        horses,
        key=lambda item: (-item[1], item[0]),
    )
    rank_by_name = {name: index for index, (name, *_rest) in enumerate(baseline, 1)}
    finish = {"WINNER": 1, "A": 2, "B": 3, "C": 4}
    return [
        {
            "race_id": race_id,
            "race_date": f"{day:02d}.08.2026",
            "race_no": "1",
            "race_time": "17:00",
            "city": "Ankara",
            "city_id": "2",
            "horse_name": name,
            "horse_no": str(index),
            "rank_pred": rank_by_name[name],
            "finish_pos": finish[name],
            "result_status": "finished",
            "result_source": "tjk_official_results" if official else "horse_history_fallback",
            "field_size": 4,
            "race_type": "Maiden",
            "track": "Kum",
            "v4_version": "4.25",
            "v4_applied_for_ranking": True,
            "v4_profile": {"category": "MAIDEN", "subtype": "MAIDEN", "track": "Kum"},
            "v4_weights": {"degree_avg": 10.0},
            "v4_penalty_total": 0.0,
            "metric_source_flags": {"hasTrainer": True, "hasHp": True},
            "agf_allowed_for_ranking": False,
            "features": {
                "degree_avg": degree,
                "trainer_score": trainer,
                "hp_score": hp,
            },
        }
        for index, (name, degree, trainer, hp) in enumerate(horses, 1)
    ]


def corpus():
    entries = []
    for day in range(9, 21):
        entries.extend(
            race_rows(
                day,
                f"R{day}",
                missed=day in {10, 18},
            )
        )
    return entries


class RecentTop3MetricAuditTests(unittest.TestCase):
    def test_positive_signal_requires_both_seven_day_blocks_and_has_no_top1_gate(self):
        report = build_report(
            corpus(),
            generated_at="2026-08-22T12:00:00Z",
            run_date="2026-08-22",
        )
        group = next(
            scope for scope in report["scopes"]
            if scope["scopeType"] == "GROUP" and scope["scopeKey"] == "MAIDEN"
        )
        candidate = next(
            item for item in group["singleCandidates"]
            if item["adjustments"] == {"trainer_score": 2.0}
        )

        self.assertEqual(candidate["status"], "CANDIDATE_FOR_FROZEN_PROSPECTIVE_SHADOW")
        self.assertEqual(candidate["windows"]["last14"]["rescues"], 2)
        self.assertEqual(candidate["windows"]["last14"]["damages"], 0)
        self.assertEqual(candidate["windows"]["previous7"]["netHits"], 1)
        self.assertEqual(candidate["windows"]["latest7"]["netHits"], 1)
        self.assertEqual(candidate["windows"]["last14"]["robustTop3Rescues"], 2)
        self.assertEqual(candidate["windows"]["last14"]["robustHitDamages"], 0)
        self.assertEqual(candidate["sourceCoverage"]["minimum"], 1.0)
        self.assertFalse(report["policy"]["top1Gate"])
        self.assertEqual(report["policy"]["robustTop3BoundaryPercentile"], 0.25)
        self.assertFalse(report["usedForRanking"])

    def test_fragile_top3_rescues_cannot_become_shadow_candidates(self):
        entries = corpus()
        for row in entries:
            if row["race_id"] in {"R10", "R18"} and row["horse_name"] == "WINNER":
                row["features"]["trainer_score"] = 66.8
                row["features"]["hp_score"] = 66.8
        report = build_report(entries)
        group = next(
            scope for scope in report["scopes"]
            if scope["scopeType"] == "GROUP" and scope["scopeKey"] == "MAIDEN"
        )
        candidate = next(
            item for item in group["singleCandidates"]
            if item["adjustments"] == {"trainer_score": 3.0}
        )

        self.assertGreater(candidate["windows"]["last14"]["rescues"], 0)
        self.assertEqual(candidate["windows"]["last14"]["robustTop3Rescues"], 0)
        self.assertEqual(candidate["status"], "RECENT_POSITIVE_FRAGILE_TOP3_MARGIN")

    def test_nonofficial_and_partial_races_are_excluded(self):
        entries = corpus()
        for row in entries:
            if row["race_id"] == "R9":
                row["result_source"] = "horse_history_fallback"
            if row["race_id"] == "R11" and row["horse_name"] == "C":
                row["finish_pos"] = None
        report = build_report(entries)

        self.assertEqual(report["input"]["last14Races"], 10)
        self.assertEqual(report["input"]["exclusions"]["notOfficial"], 1)
        self.assertEqual(report["input"]["exclusions"]["notFullyLabeled"], 1)

    def test_pair_search_is_discovery_only(self):
        report = build_report(corpus())
        group = next(
            scope for scope in report["scopes"]
            if scope["scopeType"] == "GROUP" and scope["scopeKey"] == "MAIDEN"
        )

        self.assertTrue(group["pairCandidates"])
        self.assertTrue(all(candidate["kind"] == "PAIR" for candidate in group["pairCandidates"]))
        self.assertTrue(all(
            candidate["status"] != "CANDIDATE_FOR_FROZEN_PROSPECTIVE_SHADOW"
            for candidate in group["pairCandidates"]
        ))

    def test_persist_and_pi_results_wiring(self):
        report = build_report(
            corpus(),
            generated_at="2026-08-22T12:00:00Z",
            run_date="2026-08-22",
        )
        with tempfile.TemporaryDirectory() as tmp:
            paths = persist(report, Path(tmp), "2026-08-22")
            daily = Path(paths["dailyJson"])
            latest = Path(paths["latestJson"])
            self.assertEqual(json.loads(daily.read_text(encoding="utf-8")), report)
            self.assertEqual(daily.read_bytes(), latest.read_bytes())

        root = Path(__file__).parent
        completed = subprocess.run(
            [sys.executable, "automation/recent_top3_metric_audit.py", "--help"],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        script = (root / "scripts" / "raspberry" / "run-automation.sh").read_text(
            encoding="utf-8"
        )
        audit_at = script.index("python3 automation/recent_top3_metric_audit.py")
        coupon_at = script.index("python3 automation/six_leg_coupon_scorecard.py")
        commit_at = script.index('git -C "$DATA_DIR" add automation predictions.jsonl')
        self.assertLess(audit_at, coupon_at)
        self.assertLess(audit_at, commit_at)
        self.assertIn("if ! python3 automation/recent_top3_metric_audit.py", script)


if __name__ == "__main__":
    unittest.main()
