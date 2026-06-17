import argparse
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from automation.atistik_daily_job import evaluate_gate_mode, persist_report


class AutomationGateTest(unittest.TestCase):
    def _args(self, data_dir=None, strict=False):
        return argparse.Namespace(
            mode="evaluate-gate",
            day=date(2026, 6, 17),
            data_dir=Path(data_dir or tempfile.mkdtemp()),
            backend_url="https://example.test",
            strict_gate=strict,
        )

    def test_evaluate_gate_mode_builds_report_without_live_backend(self):
        races = [[{"_sim_v418_profile": {"category": "HANDIKAP"}}]]
        checks = [
            {"check": "AGF-free policy", "status": "PASS", "evidence": "0 violations"},
            {"check": "Top1 vs AGF-only", "status": "WARN", "evidence": "watch"},
        ]
        summary = {
            "models": {"v418_agf_free": {"races": 1, "winner_top3": 1}},
            "acceptance_gate": checks,
        }

        with patch("evaluate_v418_agf_free.load_entries", return_value=[]), \
             patch("evaluate_v418_agf_free.group_races", return_value=races), \
             patch("evaluate_v418_agf_free.segment_key", return_value="HANDIKAP"), \
             patch("evaluate_v418_agf_free.collect_agf_weight_violations", return_value=[]), \
             patch("evaluate_v418_agf_free.build_acceptance_checks", return_value=checks), \
             patch("evaluate_v418_agf_free.build_summary", return_value=summary), \
             patch("evaluate_v418_agf_free.gate_has_blocking_failure", side_effect=lambda _checks, strict=False: strict):
            report = evaluate_gate_mode(self._args(strict=False), {})
            strict_report = evaluate_gate_mode(self._args(strict=True), {})

        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["totals"]["pass"], 1)
        self.assertEqual(report["totals"]["warn"], 1)
        self.assertEqual(report["totals"]["fail"], 0)
        self.assertEqual(strict_report["status"], "failed")

    def test_persist_report_writes_evaluation_json_and_summary_section(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            args = self._args(data_dir=temp_dir)
            report = {
                "mode": "evaluate-gate",
                "date": "2026-06-17",
                "status": "passed",
                "totals": {"races": 2, "agfWeightViolations": 0},
                "evaluation": {
                    "models": {
                        "v418_agf_free": {
                            "races": 2,
                            "top1": 1,
                            "winner_top3": 2,
                            "winner_top5": 2,
                            "mae": 1.2,
                            "rho": 0.5,
                            "ndcg5": 0.9,
                        }
                    },
                    "acceptance_gate": [
                        {"check": "AGF-free policy", "status": "PASS", "evidence": "0 violations"}
                    ],
                },
            }

            persist_report(args, report)
            out_dir = Path(temp_dir) / "automation" / "runs" / "2026-06-17"
            self.assertTrue((out_dir / "v418-evaluation.json").exists())
            summary = (out_dir / "summary.md").read_text(encoding="utf-8")

        self.assertIn("## v4.18 AGF-Free Gate", summary)
        self.assertIn("| Winner Top3 | 2 |", summary)
        self.assertIn("| AGF-free policy | PASS | 0 violations |", summary)


if __name__ == "__main__":
    unittest.main()
