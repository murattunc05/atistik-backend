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
    "analysisTime": "10:30",
    "resultTime": "22:45",
    "resultRetryUntil": "23:30",
    "resultRetryIntervalMinutes": 10,
    "minSubmitMatchRatio": 0.60,
    "minSubmitMatchedHorses": 3,
    "requestTimeoutSeconds": 180,
    "analyzeMaxAttempts": 3,
    "analyzeRetryDelaySeconds": 12,
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
    return re.sub(r"\s+", " ", value).strip()


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
        return {"success": False, **parsed}
    except URLError as exc:
        return {"success": False, "error": str(exc)}


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


def load_city_program(base_url: str, day: date, city: str, timeout: int) -> dict[str, Any]:
    first = http_json(
        "GET",
        endpoint(base_url, "/daily-program", {"date": date_slash(day)}),
        timeout=timeout,
    )
    if not first.get("success"):
        return {
            "city": city,
            "status": "failed",
            "error": first.get("error", "daily-program failed"),
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

    program = http_json(
        "GET",
        endpoint(
            base_url,
            "/daily-program",
            {"date": date_slash(day), "cityId": match["id"], "cityName": match["name"]},
        ),
        timeout=timeout,
    )
    if not program.get("success"):
        return {
            "city": match["name"],
            "cityId": match["id"],
            "status": "failed",
            "error": program.get("error", "daily-program city failed"),
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


def analyze_mode(args: argparse.Namespace, config: dict[str, Any]) -> dict[str, Any]:
    dry_run = args.mode == "analyze-dry-run"
    cities = args.cities or config.get("cities") or DEFAULT_CITIES
    timeout = int(config.get("requestTimeoutSeconds", 180))
    analyze_attempts = int(config.get("analyzeMaxAttempts", 3))
    analyze_retry_delay = int(config.get("analyzeRetryDelaySeconds", 12))
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

    report["finishedAt"] = now_utc_iso()
    totals = report["totals"]
    if totals["failed"] and totals["analyzed"]:
        report["status"] = "partial_success"
    elif totals["failed"]:
        report["status"] = "failed"
    else:
        report["status"] = "completed"
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
            "totals": {"checked": 0, "found": 0, "submitted": 0, "pending": 0, "failed": 0},
        }

    report: dict[str, Any] = {
        "mode": args.mode,
        "dryRun": dry_run,
        "date": date_iso(args.day),
        "raceDate": date_dot(args.day),
        "startedAt": now_utc_iso(),
        "backendUrl": args.backend_url,
        "races": [],
        "totals": {"checked": 0, "found": 0, "submitted": 0, "pending": 0, "failed": 0},
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
            "race_id": str(fetched.get("race_id") or race.get("raceId")),
            "race_date": date_dot(args.day),
            "race_no": str(race.get("raceNo", "")),
            "results": fetched_results,
        }
        submit = http_json(
            "POST",
            endpoint(args.backend_url, "/api/submit-results"),
            payload=submit_payload,
            timeout=timeout,
        )
        entry["submitResponse"] = submit
        if submit.get("success") and int(submit.get("updated", 0) or 0) > 0:
            entry["status"] = "submitted"
            report["totals"]["submitted"] += 1
        else:
            entry["status"] = "submit_failed"
            report["totals"]["failed"] += 1
        report["races"].append(entry)

    report["finishedAt"] = now_utc_iso()
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
        pending = int(report.get("totals", {}).get("pending", 0) or 0)
        if dry_run or pending == 0 or attempt >= max_attempts:
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


def build_summary(day: date, analysis: dict[str, Any] | None, results: dict[str, Any] | None) -> str:
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
                lines.extend([f"#### {city.get('city')} {race.get('raceNo')}. Koşu", ""])
                rows = [
                    [
                        r.get("rank", ""),
                        r.get("horse", ""),
                        r.get("aiScore", ""),
                        r.get("v4Rank", ""),
                        r.get("v4Score", ""),
                    ]
                    for r in rankings
                ]
                lines.extend([markdown_table(["Old Rank", "Horse", "Old Score", "v4 Rank", "v4 Score"], rows), ""])

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

    return "\n".join(lines).rstrip() + "\n"


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
    else:
        replay_path = out_dir / "replay-results.json"
        write_json(replay_path, report)
        analysis = read_json(out_dir / "analysis.json")
        results = read_json(out_dir / "results.json")

    summary = build_summary(args.day, analysis, results)
    (out_dir / "summary.md").write_text(summary, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Atistik daily automation")
    parser.add_argument(
        "--mode",
        choices=["analyze-dry-run", "analyze", "results-dry-run", "results", "replay-results"],
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
    else:
        report = replay_results_mode(args, config)

    persist_report(args, report)
    print(json.dumps(report.get("totals", report), ensure_ascii=False, indent=2))

    if report.get("status") == "failed":
        return 1
    totals = report.get("totals", {})
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
