#!/usr/bin/env python3
"""Collect a bounded, read-only late-market AGF shadow.

This job intentionally never calls ``/api/analyze-race`` and never writes to
``predictions.jsonl``.  It joins the immutable morning analysis manifest and
prediction rows to a fresh, official TJK daily-program snapshot obtained via
the backend's read-only ``/daily-program`` route.  Eligible race snapshots are
appended once to a separate, hash-protected ledger.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import tempfile
import unicodedata
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


VERSION = "late-market-agf-20260814-v1"
OBSERVATION_START = "14.08.2026"
SCHEMA_VERSION = 1
ALPHA = 0.10
MIN_COVERAGE = 0.80
PREFERRED_COVERAGE = 1.00
MIN_LEAD_MINUTES = 90
ISTANBUL = ZoneInfo("Europe/Istanbul")
TJK_DAILY_PAGE_URL = (
    "https://www.tjk.org/TR/YarisSever/Info/Page/GunlukYarisProgrami"
)
ALLOWED_PROFILES = {"MAIDEN", "SART1"}


class IntegrityError(RuntimeError):
    """Raised when a race cannot be joined without ambiguity."""


def canonical(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def sha256_payload(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fold_text(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(char for char in text if not unicodedata.combining(char))
    table = str.maketrans({"ı": "i", "İ": "I", "ş": "s", "Ş": "S"})
    return re.sub(r"[^A-Z0-9]+", "", text.translate(table).upper())


def clean_name(value: Any) -> str:
    text = str(value or "").split("\n", 1)[0].strip()
    text = re.sub(r"\s*\(\s*\d+\s*\)\s*$", "", text)
    return fold_text(text)


def clean_id(value: Any) -> str:
    return str(value if value is not None else "").strip()


def finite(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def safe_int(value: Any, default: int = 0) -> int:
    number = finite(value)
    return int(number) if number is not None else default


def parse_day(value: str) -> date:
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"unsupported date: {value}")


def parse_instant(value: str | None) -> datetime:
    if not value:
        return datetime.now(ISTANBUL)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ISTANBUL)
    return parsed.astimezone(ISTANBUL)


def parse_race_start(day: date, value: Any) -> datetime | None:
    text = clean_id(value).replace(".", ":")
    try:
        hour, minute = (int(part) for part in text.split(":", 1))
        return datetime(day.year, day.month, day.day, hour, minute, tzinfo=ISTANBUL)
    except (TypeError, ValueError):
        return None


def parse_at_id(detail_link: Any) -> str:
    text = clean_id(detail_link)
    if not text:
        return ""
    query = parse_qs(urlparse(text).query)
    for key, values in query.items():
        if key.casefold() not in {"atid", "queryparameter_atid"}:
            continue
        value = clean_id((values or [""])[0])
        if value:
            return value
    match = re.search(
        r"(?:\?|&)(?:QueryParameter_)?AtId=([^&#]+)",
        text,
        re.IGNORECASE,
    )
    return clean_id(match.group(1)) if match else ""


def parse_agf_percent(value: Any) -> float | None:
    match = re.search(r"%\s*([0-9]+(?:[.,][0-9]+)?)", clean_id(value))
    if not match:
        return None
    number = finite(match.group(1).replace(",", "."))
    if number is None or not (0.0 <= number <= 100.0):
        return None
    return number


def selected_pool_agf(horse: dict[str, Any]) -> tuple[float | None, int | None, str, list[dict[str, Any]]]:
    """Select the first official TJK pool without merging distinct markets."""
    if "agfPools" not in horse:
        raise IntegrityError("pool_aware_agf_payload_missing")
    raw_pools = horse.get("agfPools")
    if not isinstance(raw_pools, list):
        raise IntegrityError("agf_pools_not_list")
    if len(raw_pools) > 2:
        raise IntegrityError("agf_pool_count_unsupported")
    pools: list[dict[str, Any]] = []
    for raw in raw_pools[:2]:
        if not isinstance(raw, dict):
            raise IntegrityError("agf_pool_not_object")
        percent = finite(raw.get("percent"))
        pool_no_raw = raw.get("poolNo")
        pool_no = None if pool_no_raw in (None, "") else safe_int(pool_no_raw, -1)
        if percent is None or not (0.0 <= percent <= 100.0) or pool_no not in (None, 1, 2):
            raise IntegrityError("agf_pool_value_invalid")
        raw_text = clean_id(raw.get("raw"))
        raw_percent = parse_agf_percent(raw_text)
        if raw_percent is None or abs(raw_percent - percent) > 1e-9:
            raise IntegrityError("agf_pool_raw_invalid")
        rank_raw = raw.get("rank")
        rank = None if rank_raw in (None, "") else safe_int(rank_raw, -1)
        if rank is not None and rank <= 0:
            raise IntegrityError("agf_pool_rank_invalid")
        pools.append(
            {
                "poolNo": pool_no,
                "percent": percent,
                "rank": rank,
                "raw": raw_text,
                "display": clean_id(raw.get("display")),
                "source": clean_id(raw.get("source")),
            }
        )
    pool_numbers = [pool["poolNo"] for pool in pools if pool["poolNo"] is not None]
    if len(pool_numbers) != len(set(pool_numbers)) or pool_numbers != sorted(pool_numbers):
        raise IntegrityError("agf_pool_order_invalid")
    if len(pools) > 1 and pool_numbers != [1, 2]:
        raise IntegrityError("agf_dual_pool_identity_invalid")
    if not pools:
        return None, None, "", []
    selected = pools[0]
    return selected["percent"], selected["poolNo"], selected["raw"], pools


def profile_from_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    profile = rows[0].get("v4_profile") or {}
    category = fold_text(profile.get("category")) if isinstance(profile, dict) else ""
    subtype = fold_text(profile.get("subtype")) if isinstance(profile, dict) else ""
    race_type = fold_text(rows[0].get("race_type"))
    if category == "MAIDEN" or "MAIDEN" in race_type:
        return "MAIDEN"
    if subtype == "SART1" or re.search(r"SARTLI?0?1(?:\D|$)", race_type):
        return "SART1"
    return ""


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise IntegrityError(f"object expected: {path}")
    return payload


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as exc:
                raise IntegrityError(f"invalid JSONL {path}:{line_no}: {exc}") from exc
            if not isinstance(item, dict):
                raise IntegrityError(f"object expected {path}:{line_no}")
            rows.append(item)
    return rows


def load_prediction_day(path: Path, day: date) -> list[dict[str, Any]]:
    """Stream the archive while retaining only rows for one race day."""
    wanted = day.strftime("%d.%m.%Y")
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as exc:
                raise IntegrityError(f"invalid JSONL {path}:{line_no}: {exc}") from exc
            if not isinstance(item, dict):
                raise IntegrityError(f"object expected {path}:{line_no}")
            if clean_id(item.get("race_date")) == wanted:
                rows.append(item)
    return rows


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def append_unique_snapshot(path: Path, snapshot: dict[str, Any]) -> bool:
    existing = load_jsonl(path) if path.exists() else []
    key = clean_id(snapshot.get("snapshotKey"))
    if not key:
        raise IntegrityError("snapshotKey missing")
    by_key = {clean_id(item.get("snapshotKey")): item for item in existing}
    if key in by_key:
        if canonical(by_key[key]) != canonical(snapshot):
            # First accepted observation is immutable; a later market move must
            # never replace it or silently create retrospective evidence.
            return False
        return False
    existing.append(snapshot)
    atomic_write(path, "".join(canonical(item) + "\n" for item in existing))
    return True


def http_json(url: str, timeout: int = 45) -> dict[str, Any]:
    request = Request(
        url,
        method="GET",
        headers={
            "Accept": "application/json",
            "User-Agent": "atistik-late-market-shadow/1.0",
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return payload if isinstance(payload, dict) else {"success": False, "error": "non_object"}
    except HTTPError as exc:
        return {"success": False, "error": f"HTTP {exc.code}"}
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        return {"success": False, "error": str(exc)}


def validate_local_backend_url(value: str) -> str:
    parsed = urlparse(clean_id(value))
    try:
        port = parsed.port
    except ValueError as exc:
        raise IntegrityError("backend_url_invalid") from exc
    if (
        parsed.scheme != "http"
        or (parsed.hostname or "").casefold() not in {"127.0.0.1", "localhost", "::1"}
        or port != 5000
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise IntegrityError("backend_url_not_local_http_5000")
    return value.rstrip("/")


def validate_backend_daily_program_url(value: str, day: date, city_id: str) -> str:
    parsed = urlparse(clean_id(value))
    try:
        port = parsed.port
    except ValueError as exc:
        raise IntegrityError("daily_program_url_invalid") from exc
    query = parse_qs(parsed.query)
    if (
        parsed.scheme != "http"
        or (parsed.hostname or "").casefold() not in {"127.0.0.1", "localhost", "::1"}
        or port != 5000
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != "/daily-program"
        or parsed.fragment
        or set(query) - {"date", "cityId", "cityName"}
        or query.get("date") != [day.strftime("%d/%m/%Y")]
        or query.get("cityId") != [clean_id(city_id)]
    ):
        raise IntegrityError("daily_program_url_identity_mismatch")
    return value


def load_official_program(
    backend_url: str,
    day: date,
    city_id: str,
    city_name: str,
    timeout: int,
) -> dict[str, Any]:
    backend_url = validate_local_backend_url(backend_url)
    params = urlencode(
        {
            "date": day.strftime("%d/%m/%Y"),
            "cityId": city_id,
            "cityName": city_name,
        }
    )
    url = f"{backend_url.rstrip('/')}/daily-program?{params}"
    validate_backend_daily_program_url(url, day, city_id)
    payload = http_json(url, timeout=timeout)
    if not payload.get("success"):
        return {"success": False, "error": payload.get("error"), "url": url, "races": []}
    if clean_id(payload.get("cityId")) != city_id:
        return {"success": False, "error": "city_id_mismatch", "url": url, "races": []}
    if clean_id(payload.get("date")) != day.strftime("%d/%m/%Y"):
        return {"success": False, "error": "date_mismatch", "url": url, "races": []}
    return {
        "success": True,
        "url": url,
        "cityId": city_id,
        "cityName": clean_id(payload.get("cityName")) or city_name,
        "races": payload.get("races") or [],
    }


def manifest_races(analysis: dict[str, Any], day: date) -> list[dict[str, Any]]:
    if clean_id(analysis.get("date")) != day.isoformat():
        raise IntegrityError("analysis_date_mismatch")
    output: list[dict[str, Any]] = []
    for city in analysis.get("cities") or []:
        if not isinstance(city, dict):
            continue
        city_id = clean_id(city.get("cityId"))
        city_name = clean_id(city.get("city"))
        for race in city.get("races") or []:
            if not isinstance(race, dict) or race.get("status") != "analyzed":
                continue
            item = dict(race)
            item["cityId"] = city_id
            item["city"] = city_name
            output.append(item)
    return output


def prediction_groups(
    rows: list[dict[str, Any]], day: date
) -> dict[tuple[str, str, str, str], list[dict[str, Any]]]:
    groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    day_dot = day.strftime("%d.%m.%Y")
    for row in rows:
        if clean_id(row.get("race_date")) != day_dot:
            continue
        key = (
            clean_id(row.get("city_id")),
            clean_id(row.get("race_id")),
            clean_id(row.get("race_no")),
            day_dot,
        )
        if all(key[:3]):
            groups[key].append(row)
    return groups


def race_key(city_id: Any, race_id: Any, race_no: Any, day: date) -> tuple[str, str, str, str]:
    return clean_id(city_id), clean_id(race_id), clean_id(race_no), day.strftime("%d.%m.%Y")


def _validate_common_rows(rows: list[dict[str, Any]], key: tuple[str, str, str, str]) -> None:
    if not rows:
        raise IntegrityError("baseline_predictions_missing")
    if any(race_key(row.get("city_id"), row.get("race_id"), row.get("race_no"), parse_day(row.get("race_date"))) != key for row in rows):
        raise IntegrityError("baseline_identity_mismatch")
    names = [clean_name(row.get("horse_name")) for row in rows]
    numbers = [clean_id(row.get("horse_no")) for row in rows]
    if any(not value for value in names + numbers):
        raise IntegrityError("baseline_runner_identity_missing")
    if len(set(names)) != len(names) or len(set(numbers)) != len(numbers):
        raise IntegrityError("baseline_runner_identity_duplicate")
    ranks = [safe_int(row.get("v4_rank")) for row in rows]
    if sorted(ranks) != list(range(1, len(rows) + 1)):
        raise IntegrityError("baseline_rank_integrity")
    if any(not bool(row.get("v4_applied_for_ranking")) for row in rows):
        raise IntegrityError("baseline_not_visible_v4")
    versions = {clean_id(row.get("v4_version")) for row in rows}
    profiles = {profile_from_rows([row]) for row in rows}
    if len(versions) != 1 or not next(iter(versions)):
        raise IntegrityError("baseline_version_mismatch")
    if len(profiles) != 1 or not profiles.issubset(ALLOWED_PROFILES):
        raise IntegrityError("baseline_profile_mismatch")
    # The late overlay is only valid when the immutable morning visible score
    # had no market component; otherwise alpha=10% would double-count AGF.
    for row in rows:
        weights = row.get("v4_weights") or {}
        flags = row.get("metric_source_flags") or {}
        if not isinstance(weights, dict) or not isinstance(flags, dict):
            raise IntegrityError("baseline_market_guard_missing")
        if flags.get("hasAgf") is not False:
            raise IntegrityError("baseline_agf_already_applied")


def _runner_map(horses: list[dict[str, Any]], label: str) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    seen_numbers: set[str] = set()
    seen_at_ids: set[str] = set()
    for horse in horses:
        name = clean_name(horse.get("name"))
        number = clean_id(horse.get("no"))
        at_id = parse_at_id(horse.get("detailLink"))
        if not name or not number or not at_id:
            raise IntegrityError(f"{label}_runner_identity_missing")
        if name in result or number in seen_numbers or at_id in seen_at_ids:
            raise IntegrityError(f"{label}_runner_identity_duplicate")
        status = clean_id(horse.get("runnerStatus")).casefold()
        if bool(horse.get("isNonRunner")) or status not in {"", "declared"}:
            raise IntegrityError(f"{label}_non_runner_present")
        result[name] = {**horse, "_name": name, "_number": number, "_at_id": at_id}
        seen_numbers.add(number)
        seen_at_ids.add(at_id)
    return result


def _minmax(values: list[float]) -> list[float]:
    low, high = min(values), max(values)
    if high - low <= 1e-9:
        return [50.0 for _ in values]
    return [100.0 * (value - low) / (high - low) for value in values]


def build_snapshot(
    *,
    day: date,
    manifest: dict[str, Any],
    baseline_rows: list[dict[str, Any]],
    live_race: dict[str, Any],
    city_id: str,
    city_name: str,
    source_url: str,
    collected_at: datetime,
) -> tuple[dict[str, Any] | None, str]:
    race_id = clean_id(manifest.get("raceId"))
    race_no = clean_id(manifest.get("raceNo"))
    key = race_key(city_id, race_id, race_no, day)
    validate_backend_daily_program_url(source_url, day, city_id)
    _validate_common_rows(baseline_rows, key)
    profile = profile_from_rows(baseline_rows)
    if profile not in ALLOWED_PROFILES:
        return None, "profile_not_allowed"
    if clean_id(live_race.get("raceId")) != race_id:
        raise IntegrityError("live_race_id_mismatch")
    if clean_id(live_race.get("raceNo") or live_race.get("raceNumber")) != race_no:
        raise IntegrityError("live_race_no_mismatch")

    race_time = clean_id(manifest.get("time"))
    if race_time != clean_id(live_race.get("time")):
        raise IntegrityError("race_time_mismatch")
    normalized_race_time = race_time.replace(".", ":")
    if {
        clean_id(row.get("race_time")).replace(".", ":")
        for row in baseline_rows
    } != {normalized_race_time}:
        raise IntegrityError("baseline_race_time_mismatch")
    if {fold_text(row.get("city")) for row in baseline_rows} != {fold_text(city_name)}:
        raise IntegrityError("baseline_city_mismatch")
    start = parse_race_start(day, race_time)
    if start is None:
        raise IntegrityError("race_time_invalid")
    if day < parse_day(OBSERVATION_START):
        return None, "before_observation_start"
    collected_at = collected_at.astimezone(ISTANBUL)
    if collected_at.date() != day:
        raise IntegrityError("collection_date_mismatch")
    collected_ts = int(collected_at.timestamp())
    prediction_timestamps = [safe_int(row.get("ts"), 0) for row in baseline_rows]
    if any(value <= 0 for value in prediction_timestamps):
        raise IntegrityError("baseline_prediction_ts_invalid")
    if max(prediction_timestamps) >= collected_ts:
        raise IntegrityError("baseline_after_collection")
    if any(datetime.fromtimestamp(value, ISTANBUL).date() != day for value in prediction_timestamps):
        raise IntegrityError("baseline_prediction_date_mismatch")
    lead_minutes = (start - collected_at).total_seconds() / 60.0
    if lead_minutes < MIN_LEAD_MINUTES:
        return None, "lead_time_below_90m"

    morning = _runner_map(manifest.get("horses") or [], "morning")
    live = _runner_map(live_race.get("horses") or [], "late")
    baseline_by_name = {clean_name(row.get("horse_name")): row for row in baseline_rows}
    if set(morning) != set(live) or set(morning) != set(baseline_by_name):
        raise IntegrityError("runner_set_mismatch")
    for name in sorted(morning):
        if (
            morning[name]["_number"] != live[name]["_number"]
            or morning[name]["_at_id"] != live[name]["_at_id"]
            or morning[name]["_number"] != clean_id(baseline_by_name[name].get("horse_no"))
        ):
            raise IntegrityError("runner_identity_mismatch")

    ordered_names = sorted(
        baseline_by_name,
        key=lambda name: safe_int(baseline_by_name[name].get("v4_rank"), 999),
    )
    v4_scores: list[float] = []
    agf_values: list[float | None] = []
    selected_pool_numbers: list[int | None] = []
    raw_selected_agf: list[str] = []
    all_agf_pools: list[list[dict[str, Any]]] = []
    for name in ordered_names:
        score = finite(baseline_by_name[name].get("v4_score"))
        if score is None:
            raise IntegrityError("baseline_score_invalid")
        v4_scores.append(score)
        agf_value, pool_no, selected_raw, pools = selected_pool_agf(live[name])
        agf_values.append(agf_value)
        selected_pool_numbers.append(pool_no)
        raw_selected_agf.append(selected_raw)
        all_agf_pools.append(pools)
    source_count = sum(value is not None for value in agf_values)
    coverage = source_count / len(ordered_names)
    if coverage + 1e-9 < MIN_COVERAGE:
        return None, "agf_coverage_below_0_80"

    sourced_pool_numbers = {
        pool_no
        for value, pool_no in zip(agf_values, selected_pool_numbers)
        if value is not None
    }
    if len(sourced_pool_numbers) != 1:
        raise IntegrityError("agf_primary_pool_identity_mismatch")
    selected_pool_no = next(iter(sourced_pool_numbers))

    sourced_values = [value for value in agf_values if value is not None]
    if len(sourced_values) < 2 or max(sourced_values) - min(sourced_values) <= 1e-9:
        return None, "agf_not_discriminative"
    base_components = _minmax(v4_scores)
    agf_components = _minmax(sourced_values)
    agf_iter = iter(agf_components)
    resolved_agf_components = [next(agf_iter) if value is not None else 50.0 for value in agf_values]
    candidate_scores = [
        round((1.0 - ALPHA) * base + ALPHA * market, 6)
        for base, market in zip(base_components, resolved_agf_components)
    ]
    candidate_order = sorted(
        range(len(ordered_names)),
        key=lambda index: (
            -candidate_scores[index],
            safe_int(baseline_by_name[ordered_names[index]].get("v4_rank"), 999),
            safe_int(baseline_by_name[ordered_names[index]].get("horse_no"), 999),
        ),
    )
    candidate_rank = {index: rank + 1 for rank, index in enumerate(candidate_order)}

    collected_iso = collected_at.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    runners = []
    for index, name in enumerate(ordered_names):
        row = baseline_by_name[name]
        late = live[name]
        runners.append(
            {
                "atId": late["_at_id"],
                "horseName": clean_id(row.get("horse_name")),
                "horseNameKey": name,
                "horseNo": clean_id(row.get("horse_no")),
                "rawAgf": clean_id(late.get("agf")),
                "rawSelectedAgf": raw_selected_agf[index],
                "agfDisplay": clean_id(late.get("agfDisplay")),
                "agfPools": all_agf_pools[index],
                "selectedPoolNo": selected_pool_numbers[index],
                "agfPercent": agf_values[index],
                "hasAgf": agf_values[index] is not None,
                "agfComponent": round(resolved_agf_components[index], 6),
                "baselineV4Score": v4_scores[index],
                "baselineRank": safe_int(row.get("v4_rank")),
                "baselineComponent": round(base_components[index], 6),
                "candidateScore": candidate_scores[index],
                "candidateRank": candidate_rank[index],
            }
        )

    snapshot: dict[str, Any] = {
        "schemaVersion": SCHEMA_VERSION,
        "version": VERSION,
        "mode": "prospective_shadow_bounded",
        "observationStart": OBSERVATION_START,
        "snapshotKey": "|".join((VERSION, day.isoformat(), city_id, race_id, race_no)),
        "collectedAt": collected_iso,
        "collectedTs": collected_ts,
        "usedForRanking": False,
        "rolloutEligible": False,
        "telegramVisible": False,
        "identity": {
            "raceDate": day.strftime("%d.%m.%Y"),
            "city": city_name,
            "cityId": city_id,
            "raceId": race_id,
            "raceNo": race_no,
            "raceTime": race_time,
            "raceStartTs": int(start.timestamp()),
            "profile": profile,
            "fieldSize": len(runners),
        },
        "source": {
            "provider": "TJK",
            "dataset": "official_daily_program",
            "officialPage": TJK_DAILY_PAGE_URL,
            "transport": "backend_read_only_daily_program",
            "requestUrl": source_url,
        },
        "policy": {
            "alpha": ALPHA,
            "minCoverage": MIN_COVERAGE,
            "preferredCoverage": PREFERRED_COVERAGE,
            "minLeadMinutes": MIN_LEAD_MINUTES,
            "missingAgfComponent": 50.0,
        },
        "coverage": {
            "sourceCount": source_count,
            "runnerCount": len(runners),
            "ratio": round(coverage, 6),
            "preferredReached": coverage + 1e-9 >= PREFERRED_COVERAGE,
        },
        "market": {
            "selectionPolicy": "first_official_tjk_anchor",
            "selectedPoolNo": selected_pool_no,
            "distinctOfficialPoolsPreserved": True,
        },
        "leadMinutes": round(lead_minutes, 3),
        "baseline": {
            "version": clean_id(baseline_rows[0].get("v4_version")),
            "predictionTsMin": min(safe_int(row.get("ts")) for row in baseline_rows),
            "predictionTsMax": max(safe_int(row.get("ts")) for row in baseline_rows),
            "appliedForRanking": True,
        },
        "runners": runners,
    }
    snapshot["snapshotSha256"] = sha256_payload(snapshot)
    return snapshot, "accepted"


ProgramLoader = Callable[[str, date, str, str, int], dict[str, Any]]


def collect(
    *,
    analysis: dict[str, Any],
    prediction_rows: list[dict[str, Any]],
    day: date,
    backend_url: str,
    collected_at: datetime,
    timeout: int = 45,
    program_loader: ProgramLoader = load_official_program,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    backend_url = validate_local_backend_url(backend_url)
    groups = prediction_groups(prediction_rows, day)
    manifests = manifest_races(analysis, day)
    city_programs: dict[str, dict[str, Any]] = {}
    probes: list[dict[str, Any]] = []
    accepted: list[dict[str, Any]] = []

    for manifest in manifests:
        key = race_key(manifest.get("cityId"), manifest.get("raceId"), manifest.get("raceNo"), day)
        rows = groups.get(key, [])
        profile = profile_from_rows(rows)
        if profile not in ALLOWED_PROFILES:
            continue
        city_id, race_id, race_no, _ = key
        city_name = clean_id(manifest.get("city"))
        if city_id not in city_programs:
            city_programs[city_id] = program_loader(
                backend_url, day, city_id, city_name, timeout
            )
        program = city_programs[city_id]
        probe = {
            "city": city_name,
            "cityId": city_id,
            "raceId": race_id,
            "raceNo": race_no,
            "profile": profile,
            "status": "rejected",
        }
        if not program.get("success"):
            probe["reason"] = f"program_fetch_failed:{program.get('error') or 'unknown'}"
            probes.append(probe)
            continue
        live_matches = [
            race for race in program.get("races") or []
            if clean_id(race.get("raceId")) == race_id
            and clean_id(race.get("raceNo") or race.get("raceNumber")) == race_no
        ]
        if len(live_matches) != 1:
            probe["reason"] = "live_race_missing_or_ambiguous"
            probes.append(probe)
            continue
        try:
            snapshot, reason = build_snapshot(
                day=day,
                manifest=manifest,
                baseline_rows=rows,
                live_race=live_matches[0],
                city_id=city_id,
                city_name=city_name,
                source_url=clean_id(program.get("url")),
                collected_at=collected_at,
            )
        except (IntegrityError, ValueError) as exc:
            probe["reason"] = f"integrity:{exc}"
            probes.append(probe)
            continue
        probe["reason"] = reason
        if snapshot is not None:
            probe.update(
                {
                    "status": "accepted",
                    "coverage": snapshot["coverage"],
                    "leadMinutes": snapshot["leadMinutes"],
                    "snapshotKey": snapshot["snapshotKey"],
                }
            )
            accepted.append(snapshot)
        probes.append(probe)

    report = {
        "schemaVersion": SCHEMA_VERSION,
        "version": VERSION,
        "mode": "prospective_shadow_bounded",
        "date": day.isoformat(),
        "collectedAt": collected_at.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "usedForRanking": False,
        "telegramVisible": False,
        "totals": {
            "eligibleManifestRaces": len(probes),
            "accepted": len(accepted),
            "rejected": len(probes) - len(accepted),
        },
        "probes": probes,
    }
    return accepted, report


def persist(data_dir: Path, day: date, snapshots: list[dict[str, Any]], report: dict[str, Any]) -> dict[str, int]:
    ledger = data_dir / "automation" / "late-market-agf" / "snapshots.jsonl"
    added = sum(append_unique_snapshot(ledger, snapshot) for snapshot in snapshots)
    report = dict(report)
    report["persisted"] = {"newSnapshots": added, "candidateSnapshots": len(snapshots)}
    run_path = data_dir / "automation" / "runs" / day.isoformat() / "late-market-agf-probe.json"
    latest_path = data_dir / "automation" / "late-market-agf" / "latest-probe.json"
    payload = json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    atomic_write(run_path, payload)
    atomic_write(latest_path, payload)
    return {"newSnapshots": added, "candidateSnapshots": len(snapshots)}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--analysis", type=Path, required=True)
    parser.add_argument("--predictions", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--backend-url", default="http://127.0.0.1:5000")
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--now", help="ISO-8601 override for deterministic replay/tests")
    parser.add_argument("--timeout", type=int, default=45)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    day = parse_day(args.run_date)
    predictions_hash_before = sha256_file(args.predictions)
    snapshots, report = collect(
        analysis=load_json(args.analysis),
        prediction_rows=load_prediction_day(args.predictions, day),
        day=day,
        backend_url=args.backend_url,
        collected_at=parse_instant(args.now),
        timeout=args.timeout,
    )
    predictions_hash_after = sha256_file(args.predictions)
    if predictions_hash_before != predictions_hash_after:
        raise IntegrityError("predictions_file_changed")
    persisted = persist(args.data_dir, day, snapshots, report)
    print(
        json.dumps(
            {
                "success": True,
                "version": VERSION,
                "date": day.isoformat(),
                **persisted,
                "predictionsImmutable": True,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
