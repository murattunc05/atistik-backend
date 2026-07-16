#!/usr/bin/env python3
"""Fallback runner for Raspberry-first Atistik automation.

This script is intentionally conservative: it only calls the Render backend
when the expected ML-data report for the day is missing or does not show a
successful primary run.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_RENDER_BACKEND = "https://atistik-backend.onrender.com"


def parse_date(value: str | None) -> date:
    if not value:
        return datetime.now().date()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            pass
    raise SystemExit(f"Invalid date: {value}")


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        with path.open("r", encoding="utf-8-sig") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except FileNotFoundError:
        return None
    except json.JSONDecodeError:
        return None


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def run_dir(data_dir: Path, day: date) -> Path:
    return data_dir / "automation" / "runs" / day.isoformat()


def analysis_ok(data: dict[str, Any] | None) -> bool:
    if not data:
        return False
    totals = data.get("totals") or {}
    return (
        str(data.get("mode")) == "analyze"
        and int(totals.get("analyzed", 0) or 0) > 0
        and int(totals.get("failed", 0) or 0) == 0
    )


def results_ok(data: dict[str, Any] | None) -> bool:
    if not data:
        return False
    totals = data.get("totals") or {}
    checked = int(totals.get("checked", 0) or 0)
    submitted = int(totals.get("submitted", 0) or 0)
    return (
        str(data.get("mode")) == "results"
        and str(data.get("status")) == "completed"
        and checked > 0
        and submitted == checked
        and int(totals.get("pending", 0) or 0) == 0
        and int(totals.get("failed", 0) or 0) == 0
    )


def report_diagnosis(kind: str, path: Path, data: dict[str, Any] | None) -> dict[str, Any]:
    if not path.exists():
        return {"ok": False, "reason": "report_missing", "path": str(path)}
    if data is None:
        return {"ok": False, "reason": "report_unreadable", "path": str(path)}

    totals = data.get("totals") or {}
    ok = analysis_ok(data) if kind == "analyze" else results_ok(data)
    return {
        "ok": ok,
        "reason": "successful_primary_report" if ok else "primary_report_not_successful",
        "path": str(path),
        "mode": data.get("mode"),
        "status": data.get("status"),
        "totals": totals,
        "startedAt": data.get("startedAt"),
        "finishedAt": data.get("finishedAt"),
    }


def preserve_primary_report(out_dir: Path, kind: str, data: dict[str, Any] | None) -> str | None:
    if not data:
        return None
    name = "analysis-before-render-fallback.json" if kind == "analyze" else "results-before-render-fallback.json"
    path = out_dir / name
    write_json(path, data)
    return str(path)


def run_automation(
    mode: str,
    day: date,
    backend_url: str,
    data_dir: Path,
    max_attempts: int,
    allow_render_run: bool,
) -> int:
    if not allow_render_run:
        print("[FALLBACK] Render fallback is required but --allow-render-run was not provided.")
        return 3

    cmd = [
        sys.executable,
        "automation/atistik_daily_job.py",
        "--mode",
        mode,
        "--date",
        day.isoformat(),
        "--backend-url",
        backend_url,
        "--data-dir",
        str(data_dir),
        "--max-attempts",
        str(max_attempts),
    ]
    print("[FALLBACK] Running:", " ".join(cmd))
    return subprocess.call(cmd)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Render fallback only when Raspberry output is missing.")
    parser.add_argument("--kind", choices=["analyze", "results"], required=True)
    parser.add_argument("--date")
    parser.add_argument("--data-dir", type=Path, default=Path("ml-data"))
    parser.add_argument("--backend-url", default=DEFAULT_RENDER_BACKEND)
    parser.add_argument("--max-attempts", type=int, default=1)
    parser.add_argument("--allow-render-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    day = parse_date(args.date)
    data_dir = args.data_dir.resolve()
    out_dir = run_dir(data_dir, day)

    if args.kind == "analyze":
        report_path = out_dir / "analysis.json"
        primary_report = read_json(report_path)
        diagnosis = report_diagnosis(args.kind, report_path, primary_report)
        if diagnosis["ok"]:
            print(f"[FALLBACK] Analysis already exists and is successful: {report_path}")
            return 0
        preserved_path = preserve_primary_report(out_dir, args.kind, primary_report)
        decision_path = out_dir / "analyze-fallback-decision.json"
        decision = {
            "kind": args.kind,
            "date": day.isoformat(),
            "createdAt": now_utc_iso(),
            "backendUrl": args.backend_url,
            "primaryReport": diagnosis,
            "preservedPrimaryReport": preserved_path,
        }
        write_json(decision_path, decision)
        rc = run_automation("analyze", day, args.backend_url, data_dir, args.max_attempts, args.allow_render_run)
        decision["finishedAt"] = now_utc_iso()
        decision["fallbackExitCode"] = rc
        write_json(decision_path, decision)
        return rc

    report_path = out_dir / "results.json"
    primary_report = read_json(report_path)
    diagnosis = report_diagnosis(args.kind, report_path, primary_report)
    if diagnosis["ok"]:
        print(f"[FALLBACK] Results already exist and are successful: {report_path}")
        return 0
    preserved_path = preserve_primary_report(out_dir, args.kind, primary_report)
    decision_path = out_dir / "results-fallback-decision.json"
    decision = {
        "kind": args.kind,
        "date": day.isoformat(),
        "createdAt": now_utc_iso(),
        "backendUrl": args.backend_url,
        "primaryReport": diagnosis,
        "preservedPrimaryReport": preserved_path,
    }
    write_json(decision_path, decision)
    rc = run_automation("results", day, args.backend_url, data_dir, args.max_attempts, args.allow_render_run)
    decision["finishedAt"] = now_utc_iso()
    decision["fallbackExitCode"] = rc
    write_json(decision_path, decision)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
