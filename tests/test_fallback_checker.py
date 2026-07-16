import json
import subprocess
import sys
from pathlib import Path

from automation.fallback_checker import results_ok


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "automation" / "fallback_checker.py"


def run_checker(tmp_path, *args):
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args, "--data-dir", str(tmp_path)],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def test_analyze_fallback_skips_successful_report(tmp_path):
    run_dir = tmp_path / "automation" / "runs" / "2026-06-30"
    run_dir.mkdir(parents=True)
    (run_dir / "analysis.json").write_text(
        json.dumps({"mode": "analyze", "totals": {"analyzed": 3, "failed": 0}}),
        encoding="utf-8",
    )

    result = run_checker(tmp_path, "--kind", "analyze", "--date", "2026-06-30")

    assert result.returncode == 0
    assert "already exists" in result.stdout


def test_results_fallback_skips_successful_report(tmp_path):
    run_dir = tmp_path / "automation" / "runs" / "2026-06-30"
    run_dir.mkdir(parents=True)
    (run_dir / "results.json").write_text(
        json.dumps(
            {
                "mode": "results",
                "status": "completed",
                "totals": {"checked": 3, "submitted": 3, "pending": 0, "failed": 0},
            }
        ),
        encoding="utf-8",
    )

    result = run_checker(tmp_path, "--kind", "results", "--date", "2026-06-30")

    assert result.returncode == 0
    assert "already exist" in result.stdout


def test_results_ok_rejects_pending_or_incomplete_reports():
    assert not results_ok(
        {
            "mode": "results",
            "status": "partial_success",
            "totals": {"checked": 3, "submitted": 1, "pending": 2, "failed": 0},
        }
    )
    assert not results_ok(
        {
            "mode": "results",
            "status": "completed",
            "totals": {"checked": 3, "submitted": 2, "pending": 0, "failed": 0},
        }
    )


def test_analyze_fallback_records_failed_primary_report(tmp_path):
    run_dir = tmp_path / "automation" / "runs" / "2026-06-30"
    run_dir.mkdir(parents=True)
    (run_dir / "analysis.json").write_text(
        json.dumps({"mode": "analyze", "status": "partial_success", "totals": {"analyzed": 2, "failed": 1}}),
        encoding="utf-8",
    )

    result = run_checker(tmp_path, "--kind", "analyze", "--date", "2026-06-30")

    assert result.returncode == 3
    decision = json.loads((run_dir / "analyze-fallback-decision.json").read_text(encoding="utf-8"))
    preserved = json.loads((run_dir / "analysis-before-render-fallback.json").read_text(encoding="utf-8"))
    assert decision["primaryReport"]["reason"] == "primary_report_not_successful"
    assert decision["primaryReport"]["totals"]["failed"] == 1
    assert decision["fallbackExitCode"] == 3
    assert preserved["status"] == "partial_success"
