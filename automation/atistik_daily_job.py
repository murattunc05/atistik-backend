#!/usr/bin/env python3
"""Daily Atistik automation runner.

The runner intentionally uses the public backend API flow instead of editing
predictions.jsonl directly:

daily-program -> analyze-race -> fetch-race-results -> submit-results
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import unicodedata
from collections import defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_BACKEND_URL = "https://atistik-backend.onrender.com"
DEFAULT_CITIES = ["İstanbul", "Ankara", "İzmir", "Bursa", "Kocaeli"]
DEFAULT_CONFIG = {
    "enabled": True,
    "timezone": "Europe/Istanbul",
    "cities": ["İstanbul", "Ankara", "İzmir", "Bursa", "Kocaeli"],
    "analysisTime": "06:37",
    "resultTime": "22:45",
    "resultRetryUntil": "23:30",
    "resultRetryIntervalMinutes": 10,
    "minSubmitMatchRatio": 0.60,
    "minSubmitMatchedHorses": 3,
    "requestTimeoutSeconds": 180,
    "analyzeMaxAttempts": 3,
    "analyzeRetryDelaySeconds": 12,
    "failedRaceRecoveryPasses": 2,
    "failedRaceRecoveryDelaySeconds": 90,
}


class JobError(RuntimeError):
    pass


def tr_fold(value: str) -> str:
    value = unicodedata.normalize("NFKC", value or "")
    table = str.maketrans(
        {
            "İ": "i",
            "I": "i",
            "ı": "i",
            "Ğ": "g",
            "ğ": "g",
            "Ü": "u",
            "ü": "u",
            "Ş": "s",
            "ş": "s",
            "Ö": "o",
            "ö": "o",
            "Ç": "c",
            "ç": "c",
        }
    )
    value = value.translate(table).lower()
    return re.sub(r"[^a-z0-9]+", "", value)


KNOWN_DOMESTIC_CITIES = {
    tr_fold(city)
    for city in [
        "İstanbul",
        "Ankara",
        "İzmir",
        "Adana",
        "Bursa",
        "Şanlıurfa",
        "Diyarbakır",
        "Elazığ",
        "Kocaeli",
    ]
}


def clean_name(value: str) -> str:
    value = str(value or "").split("\n")[0].strip().upper()
    value = re.sub(r"\s*\(\s*\d+\s*\)\s*$", "", value)
    return re.sub(r"[\W_]+", "", value, flags=re.UNICODE)


def parse_date(value: str | None) -> date:
    if not value:
        return date.today()
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise JobError(f"Unsupported date format: {value}")


def date_dot(day: date) -> str:
    return day.strftime("%d.%m.%Y")


def date_slash(day: date) -> str:
    return day.strftime("%d/%m/%Y")


def date_iso(day: date) -> str:
    return day.strftime("%Y-%m-%d")


def now_utc_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def http_json(
    method: str,
    url: str,
    *,
    payload: dict[str, Any] | None = None,
    timeout: int = 120,
) -> dict[str, Any]:
    data = None
    headers = {"Accept": "application/json", "User-Agent": "atistik-automation/1.0"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = Request(url, data=data, headers=headers, method=method.upper())
    try:
        with urlopen(req, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else {}
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body)
        except Exception:
            parsed = {"error": body}
        parsed["http_status"] = exc.code
        if not str(parsed.get("error", "")).strip():
            parsed["error"] = f"HTTP {exc.code}"
        return {"success": False, **parsed}
    except URLError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:
        return {"success": False, "error": repr(exc)}


def load_config(data_dir: Path) -> dict[str, Any]:
    config_path = data_dir / "automation" / "config.json"
    if not config_path.exists():
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(
            json.dumps(DEFAULT_CONFIG, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        return dict(DEFAULT_CONFIG)

    with config_path.open("r", encoding="utf-8") as f:
        loaded = json.load(f)

    config = dict(DEFAULT_CONFIG)
    config.update(loaded or {})
    return config


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def run_dir(data_dir: Path, day: date) -> Path:
    return data_dir / "automation" / "runs" / date_iso(day)


def endpoint(base_url: str, path: str, params: dict[str, str] | None = None) -> str:
    url = base_url.rstrip("/") + path
    if params:
        url += "?" + urlencode(params)
    return url


def find_city(cities: list[dict[str, Any]], wanted: str) -> dict[str, str] | None:
    wanted_folded = tr_fold(wanted)
    for city in cities:
        if tr_fold(str(city.get("name", ""))) == wanted_folded:
            return {"id": str(city.get("id", "")), "name": str(city.get("name", ""))}
    return None


def get_json_with_retries(
    method: str,
    url: str,
    *,
    timeout: int,
    attempts: int = 3,
    delay_seconds: int = 5,
) -> dict[str, Any]:
    last: dict[str, Any] = {}
    for attempt in range(1, max(1, attempts) + 1):
        last = http_json(method, url, timeout=timeout)
        if last.get("success"):
            if attempt > 1:
                last["_retryAttempts"] = attempt
            return last
        if attempt < attempts:
            time.sleep(delay_seconds)
    if attempts > 1:
        last["_retryAttempts"] = attempts
    return last


def load_city_program(base_url: str, day: date, city: str, timeout: int) -> dict[str, Any]:
    first_url = endpoint(base_url, "/daily-program", {"date": date_slash(day)})
    first = get_json_with_retries(
        "GET",
        first_url,
        timeout=timeout,
    )
    if not first.get("success"):
        return {
            "city": city,
            "status": "failed",
            "error": first.get("error", "daily-program failed"),
            "url": first_url,
            "retryAttempts": first.get("_retryAttempts"),
            "races": [],
        }

    match = find_city(first.get("cities", []) or [], city)
    if not match:
        status = "no_races" if tr_fold(city) in KNOWN_DOMESTIC_CITIES else "city_not_found"
        return {
            "city": city,
            "status": status,
            "availableCities": first.get("cities", []),
            "races": [],
        }

    program_url = endpoint(
        base_url,
        "/daily-program",
        {"date": date_slash(day), "cityId": match["id"], "cityName": match["name"]},
    )
    program = get_json_with_retries(
        "GET",
        program_url,
        timeout=timeout,
    )
    if not program.get("success"):
        return {
            "city": match["name"],
            "cityId": match["id"],
            "status": "failed",
            "error": program.get("error", "daily-program city failed"),
            "url": program_url,
            "retryAttempts": program.get("_retryAttempts"),
            "races": [],
        }

    races = program.get("races", []) or []
    return {
        "city": match["name"],
        "cityId": match["id"],
        "status": "ok" if races else "no_races",
        "races": races,
    }


def validate_race(race: dict[str, Any]) -> tuple[bool, list[str]]:
    reasons = []
    if not str(race.get("raceId", "")).strip():
        reasons.append("missing_race_id")
    if not str(race.get("distance", "")).strip():
        reasons.append("missing_distance")
    if not (str(race.get("trackType", "")).strip() or str(race.get("track", "")).strip()):
        reasons.append("missing_track")
    horses = race.get("horses", []) or []
    if not horses:
        reasons.append("empty_horse_list")
    return not reasons, reasons


def horse_payload(horses: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [dict(horse) for horse in horses if horse.get("name")]


def summarize_rankings(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for horse in sorted(results, key=lambda h: int(h.get("rank", 999) or 999)):
        rows.append(
            {
                "horse": horse.get("name", ""),
                "no": horse.get("no", ""),
                "rank": horse.get("rank"),
                "aiScore": horse.get("aiScore"),
                "v4Rank": horse.get("v4Rank"),
                "v4Score": horse.get("v4Score"),
                "v4Version": horse.get("v4Version"),
                "winProbability": horse.get("winProbability"),
            }
        )
    return rows


def analyze_race(
    base_url: str,
    day: date,
    race: dict[str, Any],
    timeout: int,
    dry_run: bool,
    max_attempts: int,
    retry_delay_seconds: int,
) -> dict[str, Any]:
    valid, reasons = validate_race(race)
    summary = {
        "city": race.get("city", ""),
        "raceId": str(race.get("raceId", "")),
        "raceNo": str(race.get("raceNo") or race.get("raceNumber") or ""),
        "raceType": race.get("raceType") or race.get("raceName") or "",
        "time": race.get("time", ""),
        "distance": race.get("distance", ""),
        "track": race.get("trackType") or race.get("track") or "",
        "horseCount": len(race.get("horses", []) or []),
        "status": "pending",
        "skipReasons": reasons,
        "horses": [
            {
                "name": h.get("name", ""),
                "no": h.get("no", ""),
                "detailLink": h.get("detailLink", ""),
            }
            for h in (race.get("horses", []) or [])
        ],
    }
    if not valid:
        summary["status"] = "skipped"
        return summary
    if dry_run:
        summary["status"] = "ready"
        return summary

    payload = {
        "horses": horse_payload(race.get("horses", []) or []),
        "targetDistance": str(race.get("distance", "")),
        "targetTrack": str(race.get("trackType") or race.get("track") or ""),
        "raceId": str(race.get("raceId", "")),
        "raceType": str(race.get("raceType") or race.get("raceName") or ""),
        "raceDate": date_dot(day),
        "raceNo": summary["raceNo"],
    }
    response: dict[str, Any] = {}
    retry_errors = []
    for attempt in range(1, max(1, max_attempts) + 1):
        response = http_json(
            "POST",
            endpoint(base_url, "/api/analyze-race"),
            payload=payload,
            timeout=timeout,
        )
        if response.get("success"):
            break
        retry_errors.append(
            {
                "attempt": attempt,
                "http_status": response.get("http_status"),
                "error": response.get("error", "analyze failed"),
            }
        )
        if attempt < max(1, max_attempts):
            time.sleep(max(0, retry_delay_seconds))

    if not response.get("success"):
        summary["status"] = "failed"
        summary["error"] = response.get("error", "analyze failed")
        summary["attempts"] = max(1, max_attempts)
        summary["retryErrors"] = retry_errors
        summary["response"] = response
        return summary

    rankings = summarize_rankings(response.get("results", []) or [])
    summary.update(
        {
            "status": "analyzed",
            "processTime": response.get("processTime"),
            "blendMode": response.get("blendMode"),
            "paceScenario": response.get("paceScenario"),
            "rankings": rankings,
        }
    )
    return summary


def refresh_analysis_totals(report: dict[str, Any]) -> None:
    totals = report["totals"]
    totals["ready"] = 0
    totals["analyzed"] = 0
    totals["skipped"] = 0
    totals["failed"] = 0
    for city in report.get("cities", []) or []:
        for race in city.get("races", []) or []:
            status = race.get("status")
            if status == "ready":
                totals["ready"] += 1
            elif status == "analyzed":
                totals["analyzed"] += 1
            elif status == "failed":
                totals["failed"] += 1
            else:
                totals["skipped"] += 1


def set_analysis_status(report: dict[str, Any]) -> None:
    totals = report["totals"]
    if totals["failed"] and totals["analyzed"]:
        report["status"] = "partial_success"
    elif totals["failed"]:
        report["status"] = "failed"
    else:
        report["status"] = "completed"


def recover_failed_analysis_races(
    args: argparse.Namespace,
    report: dict[str, Any],
    timeout: int,
    dry_run: bool,
    analyze_attempts: int,
    retry_delay_seconds: int,
    recovery_passes: int,
    recovery_delay_seconds: int,
) -> None:
    if dry_run or recovery_passes <= 0:
        return

    failed_slots: list[tuple[dict[str, Any], int, dict[str, Any], dict[str, Any]]] = []
    for city_entry in report.get("cities", []) or []:
        for index, race_result in enumerate(city_entry.get("races", []) or []):
            if race_result.get("status") == "failed":
                failed_slots.append((city_entry, index, race_result, race_result))

    if not failed_slots:
        return

    report["recovery"] = {
        "enabled": True,
        "passes": recovery_passes,
        "delaySeconds": recovery_delay_seconds,
        "initialFailed": len(failed_slots),
        "recovered": 0,
    }

    for recovery_pass in range(1, recovery_passes + 1):
        remaining = [
            (city_entry, index, original, current)
            for city_entry, index, original, current in failed_slots
            if current.get("status") == "failed"
        ]
        if not remaining:
            break
        if recovery_delay_seconds > 0:
            time.sleep(recovery_delay_seconds)

        for city_entry, index, original, current in remaining:
            source_race = dict(original)
            source_race["city"] = city_entry.get("city", source_race.get("city", ""))
            source_race["horses"] = source_race.get("horses", [])
            recovered = analyze_race(
                args.backend_url,
                args.day,
                source_race,
                timeout,
                dry_run,
                analyze_attempts,
                retry_delay_seconds,
            )
            recovered["recoveryPass"] = recovery_pass
            if recovered.get("status") == "analyzed":
                recovered["recoveredFromError"] = current.get("error", "")
                recovered["previousRetryErrors"] = current.get("retryErrors", [])
                city_entry["races"][index] = recovered
                failed_slots = [
                    (slot_city, slot_index, slot_original, recovered if slot_index == index and slot_city is city_entry else slot_current)
                    for slot_city, slot_index, slot_original, slot_current in failed_slots
                ]
            else:
                current["recoveryPassesTried"] = recovery_pass
                current["latestRecoveryError"] = recovered.get("error", "")
                current["latestRecoveryRetryErrors"] = recovered.get("retryErrors", [])

    refresh_analysis_totals(report)
    set_analysis_status(report)
    report["recovery"]["finalFailed"] = report["totals"]["failed"]
    report["recovery"]["recovered"] = report["recovery"]["initialFailed"] - report["recovery"]["finalFailed"]


def analyze_mode(args: argparse.Namespace, config: dict[str, Any]) -> dict[str, Any]:
    dry_run = args.mode == "analyze-dry-run"
    cities = args.cities or config.get("cities") or DEFAULT_CITIES
    timeout = int(config.get("requestTimeoutSeconds", 180))
    analyze_attempts = int(config.get("analyzeMaxAttempts", 3))
    analyze_retry_delay = int(config.get("analyzeRetryDelaySeconds", 12))
    recovery_passes = int(config.get("failedRaceRecoveryPasses", 2))
    recovery_delay = int(config.get("failedRaceRecoveryDelaySeconds", 90))
    started = now_utc_iso()

    report: dict[str, Any] = {
        "mode": args.mode,
        "dryRun": dry_run,
        "date": date_iso(args.day),
        "raceDate": date_dot(args.day),
        "startedAt": started,
        "backendUrl": args.backend_url,
        "citiesRequested": cities,
        "cities": [],
        "totals": {
            "cities": 0,
            "racesFound": 0,
            "ready": 0,
            "analyzed": 0,
            "skipped": 0,
            "failed": 0,
        },
    }

    if not config.get("enabled", True):
        report["status"] = "disabled"
        return report

    for wanted_city in cities:
        city_report = load_city_program(args.backend_url, args.day, wanted_city, timeout)
        city_entry = {
            "city": city_report.get("city", wanted_city),
            "cityId": city_report.get("cityId"),
            "status": city_report.get("status"),
            "error": city_report.get("error"),
            "races": [],
        }
        races = city_report.get("races", []) or []
        report["totals"]["cities"] += 1
        report["totals"]["racesFound"] += len(races)

        if not races:
            report["totals"]["skipped"] += 1
            report["cities"].append(city_entry)
            continue

        for race in races:
            race["city"] = city_entry["city"]
            race_result = analyze_race(
                args.backend_url,
                args.day,
                race,
                timeout,
                dry_run,
                analyze_attempts,
                analyze_retry_delay,
            )
            city_entry["races"].append(race_result)
            status = race_result["status"]
            if status == "ready":
                report["totals"]["ready"] += 1
            elif status == "analyzed":
                report["totals"]["analyzed"] += 1
            elif status == "failed":
                report["totals"]["failed"] += 1
            else:
                report["totals"]["skipped"] += 1
        report["cities"].append(city_entry)

    recover_failed_analysis_races(
        args,
        report,
        timeout,
        dry_run,
        analyze_attempts,
        analyze_retry_delay,
        recovery_passes,
        recovery_delay,
    )
    report["finishedAt"] = now_utc_iso()
    set_analysis_status(report)
    return report


def fetch_results(base_url: str, day: date, race: dict[str, Any], timeout: int) -> dict[str, Any]:
    horses = [
        {"name": h.get("name", ""), "detailLink": h.get("detailLink", "")}
        for h in (race.get("horses", []) or [])
        if h.get("name") and h.get("detailLink")
    ]
    if not horses:
        return {"success": False, "error": "no_horses_with_detail_link", "results": []}

    return http_json(
        "POST",
        endpoint(base_url, "/api/fetch-race-results"),
        payload={"race_date": date_dot(day), "race_no": str(race.get("raceNo", "")), "horses": horses},
        timeout=timeout,
    )


def result_match_stats(race: dict[str, Any], fetched: list[dict[str, Any]]) -> dict[str, Any]:
    expected = {clean_name(h.get("name", "")) for h in race.get("horses", []) or [] if h.get("name")}
    incoming = {clean_name(r.get("horse_name", "")) for r in fetched if r.get("horse_name")}
    matched = expected & incoming
    ratio = (len(matched) / len(expected)) if expected else 0.0
    return {
        "expectedCount": len(expected),
        "incomingCount": len(incoming),
        "matchedCount": len(matched),
        "matchRatio": round(ratio, 3),
        "missingHorses": sorted(expected - incoming),
        "extraHorses": sorted(incoming - expected),
    }


def submit_safe(config: dict[str, Any], stats: dict[str, Any]) -> bool:
    min_ratio = float(config.get("minSubmitMatchRatio", 0.60))
    min_horses = int(config.get("minSubmitMatchedHorses", 3))
    return stats["matchedCount"] >= min_horses and stats["matchRatio"] >= min_ratio


def set_results_status(report: dict[str, Any], dry_run: bool) -> None:
    totals = report.get("totals", {}) or {}
    checked = int(totals.get("checked", 0) or 0)
    submitted = int(totals.get("submitted", 0) or 0)
    unresolved = int(totals.get("pending", 0) or 0) + int(totals.get("failed", 0) or 0)
    if checked == 0:
        report["status"] = "skipped"
    elif dry_run:
        report["status"] = "ready" if unresolved == 0 else "partial_ready"
    elif unresolved == 0 and submitted == checked:
        report["status"] = "completed"
    elif submitted > 0:
        report["status"] = "partial_success"
    else:
        report["status"] = "failed"


def iter_manifest_races(analysis: dict[str, Any]) -> list[dict[str, Any]]:
    races = []
    for city in analysis.get("cities", []) or []:
        for race in city.get("races", []) or []:
            if race.get("status") in ("ready", "analyzed"):
                races.append(race)
    return races


def results_once(args: argparse.Namespace, config: dict[str, Any], dry_run: bool) -> dict[str, Any]:
    timeout = int(config.get("requestTimeoutSeconds", 180))
    analysis_path = run_dir(args.data_dir, args.day) / "analysis.json"
    analysis = read_json(analysis_path)
    if not analysis:
        return {
            "mode": args.mode,
            "dryRun": dry_run,
            "date": date_iso(args.day),
            "status": "skipped",
            "reason": "analysis_manifest_missing",
            "error": f"analysis manifest not found: {analysis_path}",
            "races": [],
            "totals": {
                "checked": 0,
                "found": 0,
                "submitted": 0,
                "idempotent": 0,
                "pending": 0,
                "failed": 0,
            },
        }

    report: dict[str, Any] = {
        "mode": args.mode,
        "dryRun": dry_run,
        "date": date_iso(args.day),
        "raceDate": date_dot(args.day),
        "startedAt": now_utc_iso(),
        "backendUrl": args.backend_url,
        "races": [],
        "totals": {
            "checked": 0,
            "found": 0,
            "submitted": 0,
            "idempotent": 0,
            "pending": 0,
            "failed": 0,
        },
    }
    for race in iter_manifest_races(analysis):
        entry = {
            "city": race.get("city"),
            "raceId": race.get("raceId"),
            "raceNo": race.get("raceNo"),
            "raceType": race.get("raceType"),
            "horseCount": race.get("horseCount"),
            "status": "pending",
        }
        report["totals"]["checked"] += 1
        fetched = fetch_results(args.backend_url, args.day, race, timeout)
        if not fetched.get("success"):
            entry["status"] = "pending_result"
            entry["error"] = fetched.get("error")
            entry["details"] = fetched.get("details") or fetched.get("errors")
            report["totals"]["pending"] += 1
            report["races"].append(entry)
            continue

        fetched_results = fetched.get("results", []) or []
        stats = result_match_stats(race, fetched_results)
        safe = submit_safe(config, stats)
        entry.update(
            {
                "status": "results_found",
                "fetchedRaceId": fetched.get("race_id"),
                "fetchedRaceIdMismatch": bool(
                    fetched.get("race_id") and str(fetched.get("race_id")) != str(race.get("raceId"))
                ),
                "match": stats,
                "results": fetched_results,
                "safeToSubmit": safe,
            }
        )
        report["totals"]["found"] += 1
        if not safe:
            entry["status"] = "unsafe_match"
            report["totals"]["failed"] += 1
            report["races"].append(entry)
            continue
        if dry_run:
            entry["status"] = "would_submit"
            report["races"].append(entry)
            continue

        submit_payload = {
            "race_id": str(race.get("raceId")),
            "race_date": date_dot(args.day),
            "race_no": str(race.get("raceNo", "")),
            "results": fetched_results,
        }
        entry["submitRaceId"] = submit_payload["race_id"]
        submit = http_json(
            "POST",
            endpoint(args.backend_url, "/api/submit-results"),
            payload=submit_payload,
            timeout=timeout,
        )
        entry["submitResponse"] = submit
        updated = int(submit.get("updated", 0) or 0)
        idempotent = int(submit.get("idempotent", 0) or 0)
        conflict_count = int(submit.get("conflict_count", 0) or 0)
        if "matched" in submit or "incoming" in submit or "idempotent" in submit:
            matched = int(submit.get("matched", 0) or 0)
            incoming_count = int(submit.get("incoming", len(fetched_results)) or 0)
            submit_ok = (
                bool(submit.get("success"))
                and conflict_count == 0
                and matched > 0
                and matched == incoming_count
                and updated + idempotent >= matched
            )
        else:
            # Backward compatibility while a backend rolls from the old response shape.
            submit_ok = bool(submit.get("success")) and updated > 0

        if submit_ok:
            entry["status"] = "submitted" if updated > 0 else "already_labeled"
            report["totals"]["submitted"] += 1
            if updated == 0:
                report["totals"]["idempotent"] += 1
        else:
            entry["status"] = "submit_failed"
            report["totals"]["failed"] += 1
        report["races"].append(entry)

    report["finishedAt"] = now_utc_iso()
    set_results_status(report, dry_run)
    return report


def results_mode(args: argparse.Namespace, config: dict[str, Any]) -> dict[str, Any]:
    dry_run = args.mode == "results-dry-run"
    max_attempts = max(1, int(args.max_attempts or 1))
    interval = int(config.get("resultRetryIntervalMinutes", 10)) * 60
    last_report: dict[str, Any] | None = None
    for attempt in range(1, max_attempts + 1):
        report = results_once(args, config, dry_run)
        report["attempt"] = attempt
        last_report = report
        totals = report.get("totals", {}) or {}
        unresolved = int(totals.get("pending", 0) or 0) + int(totals.get("failed", 0) or 0)
        if dry_run or unresolved == 0 or attempt >= max_attempts:
            return report
        time.sleep(interval)
    return last_report or {}


def fetch_export(base_url: str, timeout: int) -> list[dict[str, Any]]:
    data = http_json("GET", endpoint(base_url, "/api/ml-export", {"labeled_only": "true"}), timeout=timeout)
    if isinstance(data.get("entries"), list):
        return data["entries"]
    if isinstance(data.get("predictions"), list):
        return data["predictions"]
    if isinstance(data, list):
        return data
    return []


def grouped_labeled_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        if entry.get("finish_pos") is None:
            continue
        rid = str(entry.get("race_id", ""))
        if rid:
            groups[rid].append(entry)
    out = []
    for rid, rows in groups.items():
        if len(rows) < 3:
            continue
        sample = rows[0]
        if not sample.get("race_date") or not sample.get("race_no"):
            continue
        out.append(
            {
                "raceId": rid,
                "raceDate": sample.get("race_date"),
                "raceNo": str(sample.get("race_no", "")),
                "distance": str(sample.get("distance", "")),
                "track": str(sample.get("track", "")),
                "labels": {clean_name(r.get("horse_name", "")): r.get("finish_pos") for r in rows},
                "labelCount": len(rows),
            }
        )
    out.sort(key=lambda g: datetime.strptime(g["raceDate"], "%d.%m.%Y"), reverse=True)
    return out


def find_race_in_programs(
    base_url: str,
    config: dict[str, Any],
    group: dict[str, Any],
    timeout: int,
) -> dict[str, Any] | None:
    day = parse_date(group["raceDate"])
    for city in config.get("cities", DEFAULT_CITIES):
        program = load_city_program(base_url, day, city, timeout)
        for race in program.get("races", []) or []:
            race_id_match = str(race.get("raceId", "")) == str(group.get("raceId", ""))
            race_no_match = str(race.get("raceNo") or race.get("raceNumber") or "") == str(group.get("raceNo", ""))
            if group.get("raceId"):
                if not race_id_match:
                    continue
            elif not race_no_match:
                continue
            if race_id_match or race_no_match:
                race["city"] = program.get("city", city)
                race["raceNo"] = str(race.get("raceNo") or race.get("raceNumber") or group.get("raceNo", ""))
                return race
    return None


def replay_results_mode(args: argparse.Namespace, config: dict[str, Any]) -> dict[str, Any]:
    timeout = int(config.get("requestTimeoutSeconds", 180))
    entries = fetch_export(args.backend_url, timeout)
    groups = grouped_labeled_entries(entries)
    if args.replay_race_id:
        groups = [g for g in groups if str(g["raceId"]) == str(args.replay_race_id)]

    report: dict[str, Any] = {
        "mode": args.mode,
        "dryRun": True,
        "date": date_iso(args.day),
        "startedAt": now_utc_iso(),
        "checkedGroups": 0,
        "status": "not_found",
        "replays": [],
    }
    for group in groups[:30]:
        report["checkedGroups"] += 1
        race = find_race_in_programs(args.backend_url, config, group, timeout)
        if not race:
            continue
        fetched = fetch_results(args.backend_url, parse_date(group["raceDate"]), race, timeout)
        replay = {
            "raceId": group["raceId"],
            "raceDate": group["raceDate"],
            "raceNo": group["raceNo"],
            "city": race.get("city"),
            "fetchSuccess": bool(fetched.get("success")),
            "status": "fetch_failed",
            "error": fetched.get("error"),
        }
        if fetched.get("success"):
            fetched_positions = {
                clean_name(r.get("horse_name", "")): r.get("finish_pos")
                for r in fetched.get("results", []) or []
            }
            labels = group["labels"]
            common = sorted(set(labels) & set(fetched_positions))
            mismatches = [
                {
                    "horse": name,
                    "stored": labels[name],
                    "fetched": fetched_positions[name],
                }
                for name in common
                if int(labels[name]) != int(fetched_positions[name])
            ]
            replay.update(
                {
                    "status": "compared",
                    "labelCount": group["labelCount"],
                    "fetchedCount": len(fetched_positions),
                    "matchedCount": len(common),
                    "matchRatio": round(len(common) / group["labelCount"], 3),
                    "mismatchCount": len(mismatches),
                    "mismatches": mismatches[:20],
                }
            )
            report["status"] = "compared"
            report["replays"].append(replay)
            break
        report["replays"].append(replay)

    report["finishedAt"] = now_utc_iso()
    return report


def markdown_table(headers: list[str], rows: list[list[Any]]) -> str:
    out = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        out.append("| " + " | ".join(str(x) for x in row) + " |")
    return "\n".join(out)


def format_v4_rankings(rankings: list[dict[str, Any]]) -> list[str]:
    lines = []
    for item in rankings:
        rank = item.get("v4Rank", "")
        horse = str(item.get("horse") or "").strip()
        horse_no = str(item.get("no") or "").strip()
        has_number_suffix = bool(re.search(r"\(\d+\)$", horse))
        display_name = horse if has_number_suffix or not horse_no else f"{horse} ({horse_no})"
        score = item.get("v4Score", "")
        if isinstance(score, float):
            score = f"{score:.1f}"
        lines.append(f"{rank}. {display_name} - v4 puan: {score}")
    return lines


def build_summary(
    day: date,
    analysis: dict[str, Any] | None,
    results: dict[str, Any] | None,
    evaluation: dict[str, Any] | None = None,
) -> str:
    lines = [f"# Atistik Automation Summary - {date_iso(day)}", ""]

    if analysis:
        totals = analysis.get("totals", {})
        lines.extend(
            [
                "## Analysis",
                "",
                markdown_table(
                    ["Metric", "Value"],
                    [
                        ["Mode", analysis.get("mode", "")],
                        ["Cities", totals.get("cities", 0)],
                        ["Races found", totals.get("racesFound", 0)],
                        ["Ready", totals.get("ready", 0)],
                        ["Analyzed", totals.get("analyzed", 0)],
                        ["Skipped", totals.get("skipped", 0)],
                        ["Failed", totals.get("failed", 0)],
                    ],
                ),
                "",
            ]
        )
        for city in analysis.get("cities", []) or []:
            lines.extend([f"### {city.get('city')} ({city.get('status')})", ""])
            race_rows = []
            for race in city.get("races", []) or []:
                race_rows.append(
                    [
                        race.get("raceNo", ""),
                        race.get("raceId", ""),
                        race.get("raceType", ""),
                        race.get("horseCount", 0),
                        race.get("status", ""),
                        ",".join(race.get("skipReasons", []) or []),
                    ]
                )
            if race_rows:
                lines.extend([markdown_table(["No", "RaceId", "Type", "Horses", "Status", "Skip"], race_rows), ""])
            for race in city.get("races", []) or []:
                rankings = race.get("rankings", []) or []
                if not rankings:
                    continue
                lines.extend([f"#### {city.get('city')} {race.get('raceNo')}. Kosu", ""])
                lines.extend(format_v4_rankings(rankings))
                lines.append("")

    if results:
        totals = results.get("totals", {})
        lines.extend(
            [
                "## Results",
                "",
                markdown_table(
                    ["Metric", "Value"],
                    [
                        ["Mode", results.get("mode", "")],
                        ["Checked", totals.get("checked", 0)],
                        ["Found", totals.get("found", 0)],
                        ["Submitted", totals.get("submitted", 0)],
                        ["Pending", totals.get("pending", 0)],
                        ["Failed", totals.get("failed", 0)],
                    ],
                ),
                "",
            ]
        )
        rows = []
        for race in results.get("races", []) or []:
            match = race.get("match", {}) or {}
            rows.append(
                [
                    race.get("city", ""),
                    race.get("raceNo", ""),
                    race.get("raceId", ""),
                    race.get("status", ""),
                    match.get("matchedCount", ""),
                    match.get("matchRatio", ""),
                ]
            )
        if rows:
            lines.extend([markdown_table(["City", "No", "RaceId", "Status", "Matched", "Ratio"], rows), ""])

    if evaluation:
        totals = evaluation.get("totals", {}) or {}
        eval_data = evaluation.get("evaluation", {}) or {}
        model = (eval_data.get("models", {}) or {}).get("v418_agf_free", {}) or {}
        lines.extend(
            [
                "## v4.18 AGF-Free Gate",
                "",
                markdown_table(
                    ["Metric", "Value"],
                    [
                        ["Status", evaluation.get("status", "")],
                        ["Races", model.get("races", totals.get("races", 0))],
                        ["Winner Top3", model.get("winner_top3", "")],
                        ["Winner Top5", model.get("winner_top5", "")],
                        ["Top1", model.get("top1", "")],
                        ["MAE", model.get("mae", "")],
                        ["Rho", model.get("rho", "")],
                        ["NDCG@5", model.get("ndcg5", "")],
                        ["AGF violations", totals.get("agfWeightViolations", "")],
                    ],
                ),
                "",
            ]
        )
        gate_rows = [
            [item.get("check", ""), item.get("status", ""), item.get("evidence", "")]
            for item in eval_data.get("acceptance_gate", []) or []
        ]
        if gate_rows:
            lines.extend([markdown_table(["Check", "Status", "Evidence"], gate_rows), ""])

    return "\n".join(lines).rstrip() + "\n"


def evaluate_gate_mode(args: argparse.Namespace, config: dict[str, Any]) -> dict[str, Any]:
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    from evaluate_v418_agf_free import (
        build_acceptance_checks,
        build_summary as build_evaluation_summary,
        collect_agf_weight_violations,
        gate_has_blocking_failure,
        group_races,
        load_entries,
        segment_key,
    )

    export_url = endpoint(args.backend_url, "/api/ml-export", {"labeled_only": "true"})
    report: dict[str, Any] = {
        "mode": args.mode,
        "date": date_iso(args.day),
        "startedAt": now_utc_iso(),
        "exportUrl": export_url,
        "status": "failed",
        "totals": {},
    }

    entries = load_entries(export_url)
    races = group_races(entries)
    by_segment: dict[str, list[list[dict[str, Any]]]] = defaultdict(list)
    for rows in races:
        by_segment[segment_key(rows)].append(rows)

    violations = collect_agf_weight_violations(races)
    checks = build_acceptance_checks(races, by_segment, violations)
    evaluation = build_evaluation_summary(races, by_segment, violations, checks)
    status_counts = defaultdict(int)
    for item in checks:
        status_counts[str(item.get("status", "UNKNOWN"))] += 1

    strict = bool(getattr(args, "strict_gate", False))
    failed = gate_has_blocking_failure(checks, strict=strict)
    report.update(
        {
            "finishedAt": now_utc_iso(),
            "status": "failed" if failed else "passed",
            "strictGate": strict,
            "totals": {
                "races": evaluation.get("models", {}).get("v418_agf_free", {}).get("races", len(races)),
                "checks": len(checks),
                "pass": status_counts.get("PASS", 0),
                "warn": status_counts.get("WARN", 0),
                "review": status_counts.get("REVIEW", 0),
                "fail": status_counts.get("FAIL", 0),
                "agfWeightViolations": len(violations),
            },
            "evaluation": evaluation,
        }
    )
    return report


def persist_report(args: argparse.Namespace, report: dict[str, Any]) -> None:
    out_dir = run_dir(args.data_dir, args.day)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.mode in ("analyze", "analyze-dry-run"):
        analysis_path = out_dir / "analysis.json"
        write_json(analysis_path, report)
        analysis = report
        results = read_json(out_dir / "results.json")
    elif args.mode in ("results", "results-dry-run"):
        results_path = out_dir / "results.json"
        write_json(results_path, report)
        analysis = read_json(out_dir / "analysis.json")
        results = report
        evaluation = read_json(out_dir / "v418-evaluation.json")
    elif args.mode == "evaluate-gate":
        evaluation_path = out_dir / "v418-evaluation.json"
        write_json(evaluation_path, report)
        analysis = read_json(out_dir / "analysis.json")
        results = read_json(out_dir / "results.json")
        evaluation = report
    else:
        replay_path = out_dir / "replay-results.json"
        write_json(replay_path, report)
        analysis = read_json(out_dir / "analysis.json")
        results = read_json(out_dir / "results.json")
        evaluation = read_json(out_dir / "v418-evaluation.json")

    if args.mode in ("analyze", "analyze-dry-run"):
        evaluation = read_json(out_dir / "v418-evaluation.json")

    summary = build_summary(args.day, analysis, results, evaluation)
    (out_dir / "summary.md").write_text(summary, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Atistik daily automation")
    parser.add_argument(
        "--mode",
        choices=["analyze-dry-run", "analyze", "results-dry-run", "results", "replay-results", "evaluate-gate"],
        default=os.environ.get("AUTOMATION_MODE", "analyze-dry-run"),
    )
    parser.add_argument("--date", default=os.environ.get("AUTOMATION_DATE"))
    parser.add_argument("--backend-url", default=os.environ.get("ATISTIK_BACKEND_URL", DEFAULT_BACKEND_URL))
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(os.environ.get("AUTOMATION_DATA_DIR", "ml-data")),
    )
    parser.add_argument("--cities", nargs="*", help="Override configured city list for this run")
    parser.add_argument("--max-attempts", type=int, default=int(os.environ.get("RESULT_MAX_ATTEMPTS", "1")))
    parser.add_argument("--replay-race-id", default=os.environ.get("REPLAY_RACE_ID", ""))
    parser.add_argument(
        "--strict-gate",
        action="store_true",
        default=os.environ.get("STRICT_GATE", "").lower() in {"1", "true", "yes"},
        help="Fail evaluate-gate mode on WARN or REVIEW as well as FAIL.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.day = parse_date(args.date)
    args.data_dir = args.data_dir.resolve()
    args.backend_url = args.backend_url.rstrip("/")

    config = load_config(args.data_dir)
    if args.cities:
        config["cities"] = args.cities

    if args.mode in ("analyze", "analyze-dry-run"):
        report = analyze_mode(args, config)
    elif args.mode in ("results", "results-dry-run"):
        report = results_mode(args, config)
    elif args.mode == "evaluate-gate":
        report = evaluate_gate_mode(args, config)
    else:
        report = replay_results_mode(args, config)

    persist_report(args, report)
    print(json.dumps(report.get("totals", report), ensure_ascii=False, indent=2))

    if report.get("status") == "failed":
        return 1
    totals = report.get("totals", {})
    if args.mode == "results" and (
        int(totals.get("checked", 0) or 0) == 0
        or int(totals.get("pending", 0) or 0) > 0
        or int(totals.get("failed", 0) or 0) > 0
    ):
        return 1
    if (
        args.mode == "analyze"
        and int(totals.get("failed", 0) or 0) > 0
        and int(totals.get("analyzed", 0) or 0) == 0
        and int(totals.get("racesFound", 0) or 0) > 0
    ):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
