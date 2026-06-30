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
from datetime import date, datetime
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
    return (
        str(data.get("mode")) == "results"
        and int(totals.get("checked", 0) or 0) > 0
        and int(totals.get("failed", 0) or 0) == 0
    )


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
        if analysis_ok(read_json(report_path)):
            print(f"[FALLBACK] Analysis already exists and is successful: {report_path}")
            return 0
        return run_automation("analyze", day, args.backend_url, data_dir, args.max_attempts, args.allow_render_run)

    report_path = out_dir / "results.json"
    if results_ok(read_json(report_path)):
        print(f"[FALLBACK] Results already exist and are successful: {report_path}")
        return 0
    return run_automation("results", day, args.backend_url, data_dir, args.max_attempts, args.allow_render_run)


if __name__ == "__main__":
    raise SystemExit(main())
