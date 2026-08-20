import json
import os
import subprocess
import sys
from pathlib import Path

from automation.fallback_checker import analysis_ok, results_ok


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "automation" / "fallback_checker.py"
GUARD_SCRIPT = ROOT / "scripts" / "raspberry" / "ensure-automation.sh"


def complete_analysis_report():
    return {
        "mode": "analyze",
        "status": "completed",
        "citiesRequested": ["İzmir"],
        "totals": {"cities": 1, "racesFound": 1, "analyzed": 1, "failed": 0},
        "cities": [
            {"city": "İzmir", "status": "ok", "races": [{"status": "analyzed"}]},
        ],
    }


def incomplete_analysis_report():
    report = complete_analysis_report()
    report["citiesRequested"].append("Ankara")
    report["totals"]["cities"] = 2
    report["cities"].append({"city": "Ankara", "status": "failed", "races": []})
    return report


def all_no_races_analysis_report():
    return {
        "mode": "analyze",
        "status": "completed",
        "citiesRequested": ["İstanbul", "Ankara"],
        "totals": {
            "cities": 2,
            "racesFound": 0,
            "analyzed": 0,
            "failed": 0,
            "failedCities": 0,
            "noRaceCities": 2,
            "unresolvedRaces": 0,
            "unresolved": 0,
        },
        "cities": [
            {"city": "İstanbul", "status": "no_races", "races": []},
            {"city": "Ankara", "status": "no_races", "races": []},
        ],
    }


def run_checker(tmp_path, *args):
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args, "--data-dir", str(tmp_path)],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def run_guard(tmp_path, report):
    root = tmp_path / "backend"
    data_dir = tmp_path / "ml-data"
    run_dir = data_dir / "automation" / "runs" / "2026-08-15"
    run_dir.mkdir(parents=True)
    (run_dir / "analysis.json").write_text(json.dumps(report), encoding="utf-8")
    retry = root / "scripts" / "raspberry" / "run-automation.sh"
    retry.parent.mkdir(parents=True)
    retry.write_text("#!/usr/bin/env bash\nexit 42\n", encoding="utf-8")
    retry.chmod(0o755)
    env = os.environ.copy()
    env["ATISTIK_ROOT"] = str(root)
    env["ATISTIK_ML_DATA_DIR"] = str(data_dir)
    return subprocess.run(
        ["bash", str(GUARD_SCRIPT), "analyze", "2026-08-15"],
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def run_results_guard(tmp_path, report, analysis=None):
    root = tmp_path / "backend"
    data_dir = tmp_path / "ml-data"
    run_dir = data_dir / "automation" / "runs" / "2026-08-15"
    run_dir.mkdir(parents=True)
    (run_dir / "results.json").write_text(json.dumps(report), encoding="utf-8")
    if analysis is not None:
        (run_dir / "analysis.json").write_text(json.dumps(analysis), encoding="utf-8")
    retry = root / "scripts" / "raspberry" / "run-automation.sh"
    retry.parent.mkdir(parents=True)
    retry.write_text("#!/usr/bin/env bash\nexit 42\n", encoding="utf-8")
    retry.chmod(0o755)
    env = os.environ.copy()
    env["ATISTIK_ROOT"] = str(root)
    env["ATISTIK_ML_DATA_DIR"] = str(data_dir)
    return subprocess.run(
        ["bash", str(GUARD_SCRIPT), "results", "2026-08-15"],
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def test_analyze_fallback_skips_successful_report(tmp_path):
    run_dir = tmp_path / "automation" / "runs" / "2026-06-30"
    run_dir.mkdir(parents=True)
    (run_dir / "analysis.json").write_text(json.dumps(complete_analysis_report()), encoding="utf-8")

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
    (run_dir / "analysis.json").write_text(
        json.dumps(complete_analysis_report()),
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


def test_analysis_ok_rejects_aug15_shape_with_analyzed_city_and_failed_city():
    report = {
        "mode": "analyze",
        "status": "completed",
        # This deliberately reproduces the legacy false-success totals: the
        # city failure was present only in the nested city report.
        "totals": {"analyzed": 1, "failed": 0},
        "cities": [
            {"city": "İstanbul", "status": "ok", "races": [{"status": "analyzed"}]},
            {"city": "Ankara", "status": "failed", "races": []},
        ],
    }

    assert not analysis_ok(report)


def test_analysis_ok_accepts_all_resolved_no_races():
    report = {
        "mode": "analyze",
        "status": "completed",
        "citiesRequested": ["İstanbul", "Ankara"],
        "totals": {
            "cities": 2,
            "analyzed": 0,
            "failed": 0,
            "failedCities": 0,
            "noRaceCities": 2,
            "unresolvedRaces": 0,
            "unresolved": 0,
        },
        "cities": [
            {"city": "İstanbul", "status": "no_races", "races": []},
            {"city": "Ankara", "status": "no_races", "races": []},
        ],
    }

    assert analysis_ok(report)


def test_pi_guard_retries_aug15_shape_with_failed_city(tmp_path):
    report = {
        "mode": "analyze",
        "status": "completed",
        "totals": {"analyzed": 1, "failed": 0},
        "cities": [
            {"city": "İstanbul", "status": "ok", "races": [{"status": "analyzed"}]},
            {"city": "Ankara", "status": "failed", "races": []},
        ],
    }

    result = run_guard(tmp_path, report)

    assert result.returncode == 42
    assert "retry" in result.stdout


def test_pi_guard_accepts_all_resolved_no_races(tmp_path):
    report = {
        "mode": "analyze",
        "status": "completed",
        "citiesRequested": ["İstanbul", "Ankara"],
        "totals": {
            "cities": 2,
            "analyzed": 0,
            "failed": 0,
            "failedCities": 0,
            "noRaceCities": 2,
            "unresolvedRaces": 0,
            "unresolved": 0,
        },
        "cities": [
            {"city": "İstanbul", "status": "no_races", "races": []},
            {"city": "Ankara", "status": "no_races", "races": []},
        ],
    }

    result = run_guard(tmp_path, report)

    assert result.returncode == 0
    assert "zaten basarili" in result.stdout


def test_results_fallback_rejects_incomplete_analysis_manifest_marker():
    report = {
        "mode": "results",
        "status": "completed",
        "reason": "analysis_manifest_incomplete",
        "analysisManifestComplete": False,
        # Even forged success counters may not override the source manifest gate.
        "totals": {"checked": 8, "submitted": 8, "pending": 0, "failed": 0},
    }

    assert not results_ok(report)


def test_pi_results_guard_retries_incomplete_analysis_manifest_marker(tmp_path):
    report = {
        "mode": "results",
        "status": "completed",
        "reason": "analysis_manifest_incomplete",
        "analysisManifestComplete": False,
        "totals": {"checked": 8, "submitted": 8, "pending": 0, "failed": 0},
    }

    result = run_results_guard(tmp_path, report, incomplete_analysis_report())

    assert result.returncode == 42
    assert "retry" in result.stdout


def test_completed_results_are_rejected_when_sibling_analysis_has_failed_city(tmp_path):
    run_dir = tmp_path / "automation" / "runs" / "2026-08-15"
    run_dir.mkdir(parents=True)
    results = {
        "mode": "results",
        "status": "completed",
        "totals": {"checked": 8, "submitted": 8, "pending": 0, "failed": 0},
    }
    (run_dir / "results.json").write_text(json.dumps(results), encoding="utf-8")
    (run_dir / "analysis.json").write_text(
        json.dumps(incomplete_analysis_report()),
        encoding="utf-8",
    )

    result = run_checker(tmp_path, "--kind", "results", "--date", "2026-08-15")

    assert result.returncode == 3
    decision = json.loads((run_dir / "results-fallback-decision.json").read_text(encoding="utf-8"))
    assert decision["primaryReport"]["analysisManifest"]["complete"] is False


def test_pi_results_guard_retries_completed_results_with_failed_city_source(tmp_path):
    results = {
        "mode": "results",
        "status": "completed",
        "totals": {"checked": 8, "submitted": 8, "pending": 0, "failed": 0},
    }

    result = run_results_guard(tmp_path, results, incomplete_analysis_report())

    assert result.returncode == 42
    assert "retry" in result.stdout


def test_results_fallback_and_pi_guard_accept_verified_all_no_races(tmp_path):
    results = {
        "mode": "results",
        "status": "completed",
        "reason": "analysis_manifest_no_races",
        "analysisManifestComplete": True,
        "totals": {
            "checked": 0,
            "submitted": 0,
            "partialLabels": 0,
            "pending": 0,
            "failed": 0,
        },
    }
    analysis = all_no_races_analysis_report()
    self_contained_ok = results_ok(results, analysis, require_analysis=True)
    guard = run_results_guard(tmp_path, results, analysis)

    assert self_contained_ok
    assert guard.returncode == 0
    assert "zaten basarili" in guard.stdout
