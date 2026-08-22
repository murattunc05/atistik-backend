#!/usr/bin/env python3
"""Pre-race telemetry for future, non-ranking timing signals.

This module intentionally does not change a horse's score or rank.  Existing
prediction exports do not contain the raw race histories needed to reproduce
these features, so they must first be collected prospectively and evaluated
only after a clean observation window.

The current scoring pipeline may fall back from target-distance histories to
all historical distances when it calculates ``degree_avg`` and
``degree_trend``.  Absolute times from materially different distances are not
comparable.  The ledger below is deliberately stricter: a timed run is usable
only when its surface matches the target surface and its distance is within
the configured tolerance (100 m by default).
"""

from __future__ import annotations

from datetime import datetime
import hashlib
import json
import math
import re
import statistics
from typing import Any, Iterable
from zoneinfo import ZoneInfo


SCHEMA_VERSION = "future-comparable-speed-ledger-v1"
POINT_IN_TIME_SCHEMA_VERSION = "point-in-time-race-signal-v1"
TRACK_VARIANT_REFERENCE_SCHEMA_VERSION = "track-variant-reference-v1"
DEFAULT_DISTANCE_TOLERANCE_M = 100
DEFAULT_LARGE_FIELD_THRESHOLD = 12
ISTANBUL_TZ = ZoneInfo("Europe/Istanbul")
_RUNNING_STYLES = ("KAÇAK", "TAKİPÇİ", "BEKLEME")


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _parse_distance(value: Any) -> int | None:
    digits = re.sub(r"[^0-9]", "", str(value or ""))
    if not digits:
        return None
    try:
        distance = int(digits)
    except ValueError:
        return None
    return distance if 600 <= distance <= 5000 else None


def _fold_text(value: Any) -> str:
    folded = str(value or "").strip().upper()
    return folded.translate(
        str.maketrans(
            {
                "Ç": "C",
                "Ğ": "G",
                "İ": "I",
                "Ö": "O",
                "Ş": "S",
                "Ü": "U",
            }
        )
    )


def _surface_key(value: Any) -> str:
    folded = _fold_text(value)
    # TJK history rows commonly encode surface + condition as `K:Normal`,
    # `Ç:Normal` or `S:Normal`; daily programs use the long surface names.
    # Treat only the leading code (with its colon) as authoritative so a
    # condition word cannot accidentally determine the surface.
    if "SENTETIK" in folded or folded in {"S", "S:"} or folded.startswith("S:"):
        return "sentetik"
    if "KUM" in folded or folded in {"K", "K:"} or folded.startswith("K:"):
        return "kum"
    if "CIM" in folded or folded in {"C", "C:"} or folded.startswith("C:"):
        return "cim"
    return ""


def _parse_date(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _median(values: Iterable[float]) -> float | None:
    clean = []
    for value in values:
        number = _safe_float(value)
        if number is not None:
            clean.append(number)
    return statistics.median(clean) if clean else None


def _linear_slope(values_oldest_to_newest: list[float]) -> float | None:
    count = len(values_oldest_to_newest)
    if count < 2:
        return None
    x_mean = (count - 1) / 2.0
    y_mean = sum(values_oldest_to_newest) / count
    denominator = sum((idx - x_mean) ** 2 for idx in range(count))
    if denominator <= 0:
        return None
    return sum(
        (idx - x_mean) * (value - y_mean)
        for idx, value in enumerate(values_oldest_to_newest)
    ) / denominator


def _round(value: float | None, digits: int = 4) -> float | None:
    return round(value, digits) if value is not None and math.isfinite(value) else None


def _safe_int(value: Any) -> int | None:
    number = _safe_float(value)
    if number is None or not number.is_integer():
        return None
    return int(number)


def _race_start_ts(race_date: Any, race_time: Any) -> int | None:
    race_day = _parse_date(race_date)
    time_text = str(race_time or "").strip().replace(".", ":")
    if race_day is None or not re.fullmatch(r"\d{1,2}:\d{2}", time_text):
        return None
    try:
        hour, minute = (int(part) for part in time_text.split(":", 1))
        instant = datetime(
            race_day.year,
            race_day.month,
            race_day.day,
            hour,
            minute,
            tzinfo=ISTANBUL_TZ,
        )
    except (TypeError, ValueError):
        return None
    return int(instant.timestamp())


def _distance_band_m(value: Any) -> int | None:
    """Return the inclusive 200 m lower-bound band used by variant references."""
    distance = _parse_distance(value)
    return (distance // 200) * 200 if distance is not None else None


def point_in_time_snapshot_sha256(snapshot: dict[str, Any]) -> str:
    """Return the canonical hash stored with one immutable pre-race snapshot."""
    encoded = json.dumps(
        snapshot,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def runner_identity_key(value: Any) -> str:
    """Return the stable race-roster identity used by snapshot verification."""
    return re.sub(r"[^A-Z0-9]+", "", _fold_text(value))


def verify_point_in_time_snapshot(point_in_time: Any) -> bool:
    if not isinstance(point_in_time, dict):
        return False
    snapshot = point_in_time.get("preRaceSnapshot")
    expected = str(point_in_time.get("preRaceSnapshotSha256") or "")
    return bool(
        isinstance(snapshot, dict)
        and re.fullmatch(r"[0-9a-f]{64}", expected)
        and point_in_time_snapshot_sha256(snapshot) == expected
    )


def _chronological_history(races: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], bool]:
    """Return newest-first history and whether every row had a valid date."""
    dated: list[tuple[datetime, int, dict[str, Any]]] = []
    undated: list[tuple[int, dict[str, Any]]] = []
    for index, race in enumerate(races):
        parsed = _parse_date((race or {}).get("date"))
        if parsed is None:
            undated.append((index, race or {}))
        else:
            dated.append((parsed, index, race or {}))
    dated.sort(key=lambda item: (item[0], -item[1]), reverse=True)
    ordered = [race for _, _, race in dated]
    ordered.extend(race for _, race in undated)
    return ordered, not undated


def _strict_prior_history(
    races: list[dict[str, Any]],
    *,
    target_race_date: Any = None,
    target_race_id: Any = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Keep only history known to be strictly before the target race date."""
    ordered, dates_complete = _chronological_history(races)
    cutoff_requested = target_race_date not in (None, "") or target_race_id not in (None, "")
    if not cutoff_requested:
        return ordered, {
            "cutoffApplied": False,
            "cutoffValid": None,
            "rawHistoryRaceCount": len(races),
            "rejectedSameOrFutureHistoryCount": 0,
            "rejectedTargetRaceIdentityCount": 0,
            "rejectedUndatedHistoryCount": 0,
            "datesComplete": dates_complete,
        }

    cutoff_day = _parse_date(target_race_date)
    target_id = str(target_race_id or "").strip()
    retained: list[dict[str, Any]] = []
    rejected_same_or_future = 0
    rejected_identity = 0
    rejected_undated = 0
    for race in ordered:
        history_id = str(
            race.get("raceId") or race.get("race_id") or race.get("id") or ""
        ).strip()
        if target_id and history_id and history_id == target_id:
            rejected_identity += 1
            continue
        history_day = _parse_date(race.get("date"))
        if history_day is None:
            rejected_undated += 1
            continue
        if cutoff_day is None or history_day >= cutoff_day:
            rejected_same_or_future += 1
            continue
        retained.append(race)
    return retained, {
        "cutoffApplied": True,
        "cutoffValid": cutoff_day is not None,
        "rawHistoryRaceCount": len(races),
        "rejectedSameOrFutureHistoryCount": rejected_same_or_future,
        "rejectedTargetRaceIdentityCount": rejected_identity,
        "rejectedUndatedHistoryCount": rejected_undated,
        "datesComplete": dates_complete,
    }


def build_horse_signal_telemetry(
    races: list[dict[str, Any]] | None,
    *,
    target_distance: Any,
    target_track: Any,
    target_city: Any = None,
    target_race_date: Any = None,
    target_race_id: Any = None,
    distance_tolerance_m: int = DEFAULT_DISTANCE_TOLERANCE_M,
) -> dict[str, Any]:
    """Build raw, prospective timing diagnostics for one horse.

    No score is emitted here.  Values remain in physical units or percentages
    so a later chronological evaluation can choose a stable transformation.
    """
    history, cutoff = _strict_prior_history(
        list(races or []),
        target_race_date=target_race_date,
        target_race_id=target_race_id,
    )
    dates_complete = bool(cutoff["datesComplete"])
    target_distance_m = _parse_distance(target_distance)
    target_surface = _surface_key(target_track)
    target_city_key = _fold_text(target_city)
    tolerance = max(0, int(distance_tolerance_m))

    timed_count = 0
    parseable_distance_count = 0
    target_surface_timed_count = 0
    target_distance_timed_count = 0
    legacy_distance_pool_other_surface_count = 0
    rejected_implausible_speed_count = 0
    comparable: list[dict[str, Any]] = []
    timed_distances: list[int] = []
    condition_keys: set[str] = set()
    class_keys: set[str] = set()

    for race in history:
        distance_m = _parse_distance(race.get("distance"))
        if distance_m is not None:
            parseable_distance_count += 1

        raw_seconds = _safe_float(race.get("degreeInSeconds"))
        adjusted_seconds = _safe_float(race.get("adjustedDegreeInSeconds"))
        if raw_seconds is None or raw_seconds <= 0:
            continue
        timed_count += 1
        if distance_m is not None:
            timed_distances.append(distance_m)

        surface = _surface_key(race.get("track"))
        if target_surface and surface == target_surface:
            target_surface_timed_count += 1
        distance_matches = bool(
            target_distance_m is not None
            and distance_m is not None
            and abs(distance_m - target_distance_m) <= tolerance
        )
        if distance_matches:
            target_distance_timed_count += 1
            if target_surface and surface != target_surface:
                legacy_distance_pool_other_surface_count += 1

        # Unknown target context cannot produce a comparable-time signal.
        if not target_surface or surface != target_surface or not distance_matches:
            continue

        raw_speed = distance_m / raw_seconds
        adjusted_speed = (
            distance_m / adjusted_seconds
            if adjusted_seconds is not None and adjusted_seconds > 0
            else None
        )
        # Bad parses should be visible in coverage rather than silently
        # becoming an extreme signal.
        if not 8.0 <= raw_speed <= 25.0:
            rejected_implausible_speed_count += 1
            continue
        if adjusted_speed is not None and not 8.0 <= adjusted_speed <= 27.0:
            adjusted_speed = None

        condition_keys.add(_fold_text(race.get("trackCondition")) or "UNKNOWN")
        class_keys.add(_fold_text(race.get("raceType") or race.get("group")) or "UNKNOWN")
        comparable.append(
            {
                "date": str(race.get("date") or ""),
                "city": str(race.get("city") or ""),
                "sameTargetCity": bool(
                    target_city_key and _fold_text(race.get("city")) == target_city_key
                ),
                "distanceM": distance_m,
                "rawSpeedMps": raw_speed,
                "adjustedSpeedMps": adjusted_speed,
            }
        )

    adjusted = [
        row["adjustedSpeedMps"]
        for row in comparable
        if row.get("adjustedSpeedMps") is not None
    ]
    raw = [row["rawSpeedMps"] for row in comparable]
    same_city_count = sum(1 for row in comparable if row["sameTargetCity"])
    primary = adjusted if len(adjusted) == len(comparable) else raw

    recent_two = primary[:2]
    prior_four = primary[2:6]
    recent_median = _median(recent_two)
    prior_median = _median(prior_four)
    recent_vs_baseline_pct = None
    if len(recent_two) >= 2 and len(prior_four) >= 2 and prior_median:
        recent_vs_baseline_pct = ((recent_median / prior_median) - 1.0) * 100.0

    trend = _linear_slope(list(reversed(primary[:6]))) if len(primary) >= 3 else None
    primary_median = _median(primary[:6])
    trend_pct = (trend / primary_median * 100.0) if trend is not None and primary_median else None

    volatility_pct = None
    if len(primary) >= 3 and primary_median:
        mad = _median(abs(value - primary_median) for value in primary[:6])
        if mad is not None:
            volatility_pct = mad / primary_median * 100.0

    comparable_count = len(comparable)
    if comparable_count >= 5:
        reliability = "HIGH"
    elif comparable_count >= 3:
        reliability = "MEDIUM"
    elif comparable_count >= 2:
        reliability = "LOW"
    else:
        reliability = "NONE"

    reason_codes: list[str] = []
    if not history:
        reason_codes.append("NO_HISTORY")
    if target_distance_m is None:
        reason_codes.append("TARGET_DISTANCE_UNKNOWN")
    if not target_surface:
        reason_codes.append("TARGET_SURFACE_UNKNOWN")
    if history and not timed_count:
        reason_codes.append("NO_TIMED_HISTORY")
    if timed_count and not comparable_count:
        reason_codes.append("NO_SAME_SURFACE_DISTANCE_TIME")
    if 0 < comparable_count < 3:
        reason_codes.append("INSUFFICIENT_FOR_TREND")
    if comparable_count < 4:
        reason_codes.append("INSUFFICIENT_FOR_RECENT_BASELINE")
    if rejected_implausible_speed_count:
        reason_codes.append("IMPLAUSIBLE_SPEED_REJECTED")
    if not dates_complete:
        reason_codes.append("HISTORY_DATE_INCOMPLETE")
    if cutoff["cutoffApplied"] and cutoff["cutoffValid"] is False:
        reason_codes.append("TARGET_RACE_DATE_INVALID")
    if cutoff["rejectedSameOrFutureHistoryCount"]:
        reason_codes.append("SAME_OR_FUTURE_HISTORY_REJECTED")
    if cutoff["rejectedTargetRaceIdentityCount"]:
        reason_codes.append("TARGET_RACE_HISTORY_REJECTED")
    if cutoff["rejectedUndatedHistoryCount"]:
        reason_codes.append("UNDATED_HISTORY_REJECTED")

    distance_span = max(timed_distances) - min(timed_distances) if timed_distances else None
    fallback_mixes_distances = bool(
        timed_count
        and not comparable_count
        and distance_span is not None
        and distance_span > tolerance * 2
    )
    if fallback_mixes_distances:
        reason_codes.append("LEGACY_FALLBACK_MIXES_DISTANCES")
    if legacy_distance_pool_other_surface_count:
        reason_codes.append("LEGACY_DISTANCE_POOL_MIXES_SURFACES")
    if target_city_key and comparable_count and same_city_count < 2:
        reason_codes.append("CROSS_CITY_COMPARISON_ONLY")

    recent_two_adjusted = [row.get("adjustedSpeedMps") for row in comparable[:2]]
    if len(recent_two_adjusted) < 2 or any(value is None for value in recent_two_adjusted):
        recent_two_adjusted_median = None
    else:
        recent_two_adjusted_median = _median(recent_two_adjusted)

    return {
        "schemaVersion": SCHEMA_VERSION,
        "mode": "prospective_diagnostic_only",
        "usedForRanking": False,
        "rolloutEligible": False,
        "target": {
            "distanceM": target_distance_m,
            "surface": target_surface or None,
            "city": str(target_city or "") or None,
            "distanceToleranceM": tolerance,
        },
        "source": {
            "historyRaceCount": len(history),
            "rawHistoryRaceCount": cutoff["rawHistoryRaceCount"],
            "historyCutoffApplied": cutoff["cutoffApplied"],
            "historyCutoffValid": cutoff["cutoffValid"],
            "rejectedSameOrFutureHistoryCount": cutoff["rejectedSameOrFutureHistoryCount"],
            "rejectedTargetRaceIdentityCount": cutoff["rejectedTargetRaceIdentityCount"],
            "rejectedUndatedHistoryCount": cutoff["rejectedUndatedHistoryCount"],
            "timedRaceCount": timed_count,
            "parseableDistanceCount": parseable_distance_count,
            "targetSurfaceTimedRaceCount": target_surface_timed_count,
            "targetDistanceTimedRaceCount": target_distance_timed_count,
            "legacyDistancePoolOtherSurfaceCount": legacy_distance_pool_other_surface_count,
            "comparableTimedRaceCount": comparable_count,
            "sameTargetCityComparableCount": same_city_count,
            "adjustedComparableCount": len(adjusted),
            "rejectedImplausibleSpeedCount": rejected_implausible_speed_count,
            "timedDistanceSpanM": distance_span,
            "trackConditionBucketCount": len(condition_keys),
            "classBucketCount": len(class_keys),
            "historyDatesComplete": dates_complete,
            "legacyFallbackWouldMixDistances": fallback_mixes_distances,
            "legacyDistancePoolWouldMixSurfaces": legacy_distance_pool_other_surface_count > 0,
        },
        "flags": {
            "hasComparableTimedRaces": comparable_count >= 2,
            "hasComparableTrend": comparable_count >= 3,
            "hasRecentBaselineComparison": comparable_count >= 4,
            "hasFullyAdjustedComparableTimes": bool(comparable) and len(adjusted) == comparable_count,
        },
        "features": {
            "recent2RawSpeedMps": _round(_median(raw[:2])),
            "recent2AdjustedSpeedMps": _round(recent_two_adjusted_median),
            "recentVsBaselineSpeedPct": _round(recent_vs_baseline_pct, 3),
            "speedTrendPctPerRace": _round(trend_pct, 3),
            "speedVolatilityPct": _round(volatility_pct, 3),
        },
        "reliability": reliability,
        "reasonCodes": reason_codes,
        "limitations": {
            "historicFieldSizeUnavailable": True,
            "sectionalTimingUnavailable": True,
            "courseParTimeUnavailable": True,
            "finishRankNotFieldNormalized": True,
        },
    }


def _percentile_scores(values: dict[str, float], *, higher_is_better: bool = True) -> dict[str, float]:
    if len(values) < 3:
        return {}
    ordered = sorted(values.items(), key=lambda item: (item[1], item[0]))
    distinct = sorted(set(value for _, value in ordered))
    if len(distinct) < 2:
        return {name: 50.0 for name in values}
    denominator = len(distinct) - 1
    score_by_value = {
        value: (index / denominator) * 100.0
        for index, value in enumerate(distinct)
    }
    if not higher_is_better:
        score_by_value = {value: 100.0 - score for value, score in score_by_value.items()}
    return {name: round(score_by_value[value], 1) for name, value in values.items()}


def _profile_coverage_buckets(profile: Any, field_size: int, large_threshold: int) -> dict[str, bool]:
    folded = _fold_text(profile).replace(" ", "")
    is_sart1 = bool(re.match(r"^(?:SART1|SARTLI1)(?:$|[^0-9])", folded))
    return {
        "isMaiden": folded.startswith("MAIDEN"),
        "isSart1": is_sart1,
        "isSatis": folded.startswith("SATIS"),
        "isLargeField": field_size >= large_threshold,
    }


def _pace_field_snapshot(
    horses: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    field_size = len(horses)
    style_counts = {style: 0 for style in _RUNNING_STYLES}
    valid_style_count = 0
    source_proven_count = 0
    scenarios: set[str] = set()
    kacak_counts: set[int] = set()
    pressures: set[float] = set()

    for horse, row in zip(horses, rows):
        pace = horse.get("paceInfo") or {}
        style = str(pace.get("runningStyle") or "").strip().upper()
        source = str(pace.get("styleSource") or "").strip()
        if style in style_counts:
            style_counts[style] += 1
            valid_style_count += 1
            history_count = _safe_int(
                ((row.get("telemetry") or {}).get("source") or {}).get("historyRaceCount")
            ) or 0
            if source == "recent_finish_position_proxy" and history_count > 0:
                source_proven_count += 1
        scenario = str(pace.get("paceScenario") or "").strip().upper()
        if scenario:
            scenarios.add(scenario)
        kacak = _safe_int(pace.get("kacakCount"))
        if kacak is not None:
            kacak_counts.add(kacak)
        pressure = _safe_float(pace.get("pacePressure"))
        if pressure is not None:
            pressures.add(round(pressure, 3))

    internally_consistent = bool(
        field_size
        and valid_style_count == field_size
        and len(scenarios) == 1
        and len(kacak_counts) == 1
        and len(pressures) == 1
        and next(iter(kacak_counts)) == style_counts["KAÇAK"]
    )
    return {
        "state": "AVAILABLE" if internally_consistent else "UNAVAILABLE_INCONSISTENT",
        "scenario": next(iter(scenarios)) if len(scenarios) == 1 else None,
        "styleCounts": style_counts,
        "kacakCount": next(iter(kacak_counts)) if len(kacak_counts) == 1 else None,
        "pacePressurePct": next(iter(pressures)) if len(pressures) == 1 else None,
        "validStyleRunnerCount": valid_style_count,
        "sourceProvenRunnerCount": source_proven_count,
        "validStyleRunnerPct": round(valid_style_count / field_size * 100.0, 1) if field_size else 0.0,
        "sourceProvenRunnerPct": round(source_proven_count / field_size * 100.0, 1) if field_size else 0.0,
        "source": "recent_finish_position_proxy",
        "limitations": {
            "sectionalTimingUnavailable": True,
            "runningStyleIsFinishPositionProxy": True,
        },
    }


def _prior_track_variant_snapshot(
    reference: Any,
    *,
    current_race_id: Any,
    current_race_start_ts: int | None,
    captured_ts: int | None,
    current_city: Any,
    current_surface: Any,
    current_distance: Any,
    pre_race_timing_valid: bool,
) -> dict[str, Any]:
    unavailable = {
        "state": "UNAVAILABLE",
        "usedForTelemetry": False,
        "usedForRanking": False,
        "reasonCodes": ["NO_PRIOR_TRACK_VARIANT_STORE"],
        "reference": None,
    }
    if reference in (None, {}):
        return unavailable
    if not isinstance(reference, dict):
        return {**unavailable, "reasonCodes": ["REFERENCE_NOT_OBJECT"]}

    reasons: list[str] = []
    if reference.get("schemaVersion") != TRACK_VARIANT_REFERENCE_SCHEMA_VERSION:
        reasons.append("REFERENCE_SCHEMA_MISMATCH")
    source_ids = [str(value).strip() for value in (reference.get("sourceRaceIds") or []) if str(value).strip()]
    source_count = _safe_int(reference.get("sourceRaceCount"))
    source_start_ts = _safe_int(reference.get("sourceMaxRaceStartTs"))
    source_completed_ts = _safe_int(reference.get("sourceMaxCompletedTs"))
    as_of_ts = _safe_int(reference.get("asOfTs"))
    variant = _safe_float(reference.get("variantSecondsPer1000m"))
    current_id = str(current_race_id or "").strip()

    if not pre_race_timing_valid:
        reasons.append("CURRENT_SNAPSHOT_NOT_VERIFIED_PRE_RACE")
    if not source_ids or source_count != len(source_ids) or len(source_ids) != len(set(source_ids)):
        reasons.append("SOURCE_RACE_SET_INVALID")
    if current_id and current_id in source_ids:
        reasons.append("OWN_RACE_REFERENCE_FORBIDDEN")
    if current_race_start_ts is None or source_start_ts is None:
        reasons.append("RACE_START_CUTOFF_UNKNOWN")
    elif source_start_ts >= current_race_start_ts:
        reasons.append("SOURCE_RACE_NOT_STRICTLY_EARLIER")
    if captured_ts is None or source_completed_ts is None or as_of_ts is None:
        reasons.append("OBSERVATION_CUTOFF_UNKNOWN")
    elif not (source_completed_ts <= as_of_ts <= captured_ts):
        reasons.append("REFERENCE_NOT_AVAILABLE_AT_SNAPSHOT")
    if _fold_text(reference.get("city")) != _fold_text(current_city):
        reasons.append("CITY_MISMATCH")
    if _surface_key(reference.get("surface")) != _surface_key(current_surface):
        reasons.append("SURFACE_MISMATCH")
    reference_band = _safe_int(reference.get("distanceBandM"))
    current_band = _distance_band_m(current_distance)
    if reference_band is None or current_band is None or reference_band != current_band:
        reasons.append("DISTANCE_BAND_MISMATCH")
    if variant is None:
        reasons.append("VARIANT_VALUE_MISSING")

    if reasons:
        return {**unavailable, "reasonCodes": reasons}

    safe_reference = {
        "schemaVersion": TRACK_VARIANT_REFERENCE_SCHEMA_VERSION,
        "city": str(reference.get("city") or ""),
        "surface": _surface_key(reference.get("surface")),
        "distanceBandM": reference_band,
        "variantSecondsPer1000m": round(float(variant), 4),
        "sourceRaceCount": source_count,
        "sourceRaceIds": source_ids,
        "sourceMaxRaceStartTs": source_start_ts,
        "sourceMaxCompletedTs": source_completed_ts,
        "asOfTs": as_of_ts,
    }
    return {
        "state": "AVAILABLE_PRIOR_ONLY",
        "usedForTelemetry": True,
        "usedForRanking": False,
        "reasonCodes": [],
        "reference": safe_reference,
        "referenceSha256": point_in_time_snapshot_sha256(safe_reference),
    }


def _build_point_in_time_context(
    horses: list[dict[str, Any]],
    rows: list[dict[str, Any]],
    *,
    race_id: Any,
    race_date: Any,
    race_no: Any,
    race_time: Any,
    city: Any,
    city_id: Any,
    target_distance: Any,
    target_track: Any,
    profile: Any,
    captured_ts: Any,
    large_field_threshold: int,
    prior_track_variant: Any,
) -> dict[str, Any]:
    captured = _safe_int(captured_ts)
    start_ts = _race_start_ts(race_date, race_time)
    if captured is None:
        timing_state = "UNAVAILABLE_CAPTURE_TIME"
    elif start_ts is None:
        timing_state = "UNAVAILABLE_RACE_START"
    elif captured >= start_ts:
        timing_state = "INVALID_NOT_PRE_RACE"
    else:
        timing_state = "VALID_PRE_RACE"
    timing_valid = timing_state == "VALID_PRE_RACE"

    field_size = len(horses)
    threshold = max(1, int(large_field_threshold))
    runner_snapshots = []
    for horse, row in zip(horses, rows):
        signal = {
            "telemetry": row.get("telemetry") or {},
            "fieldDiagnosticScores": row.get("fieldDiagnosticScores") or {},
        }
        runner_snapshots.append({
            "horseName": str(row.get("horseName") or ""),
            "horseKey": runner_identity_key(row.get("horseName")),
            "horseNo": str(horse.get("no") or horse.get("horse_no") or "") or None,
            "signalSha256": point_in_time_snapshot_sha256(signal),
        })
    runner_snapshots.sort(key=lambda item: (item["horseKey"], item["horseNo"] or ""))
    pace = _pace_field_snapshot(horses, rows)
    track_variant = _prior_track_variant_snapshot(
        prior_track_variant,
        current_race_id=race_id,
        current_race_start_ts=start_ts,
        captured_ts=captured,
        current_city=city,
        current_surface=target_track,
        current_distance=target_distance,
        pre_race_timing_valid=timing_valid,
    )
    identity = {
        "raceId": str(race_id or "") or None,
        "raceDate": str(race_date or "") or None,
        "raceNo": str(race_no or "") or None,
        "raceTime": str(race_time or "") or None,
        "raceStartTs": start_ts,
        "city": str(city or "") or None,
        "cityId": str(city_id or "") or None,
        "distanceM": _parse_distance(target_distance),
        "distanceBandM": _distance_band_m(target_distance),
        "surface": _surface_key(target_track) or None,
        "trackCondition": None,
        "profile": str(profile or "") or None,
    }
    identity_complete = all(
        identity.get(key) not in (None, "")
        for key in (
            "raceId",
            "raceDate",
            "raceNo",
            "raceTime",
            "raceStartTs",
            "city",
            "cityId",
            "distanceM",
            "distanceBandM",
            "surface",
            "profile",
        )
    )
    snapshot = {
        "schemaVersion": POINT_IN_TIME_SCHEMA_VERSION,
        "identity": identity,
        "capturedTs": captured,
        "timingState": timing_state,
        "field": {
            "declaredRunnerCount": field_size,
            "largeFieldThreshold": threshold,
            "isLargeField": field_size >= threshold,
            "source": "analyze_request_horse_list",
            "runners": runner_snapshots,
        },
        "pace": pace,
        "trackVariant": track_variant,
        "coverageBuckets": _profile_coverage_buckets(profile, field_size, threshold),
    }
    return {
        "schemaVersion": POINT_IN_TIME_SCHEMA_VERSION,
        "mode": "telemetry_only",
        "usedForRanking": False,
        "sentToTelegram": False,
        "rolloutEligible": False,
        "preRaceSnapshot": snapshot,
        "preRaceSnapshotSha256": point_in_time_snapshot_sha256(snapshot),
        "source": {
            "fieldSize": "analyze_request_horse_list",
            "pace": "recent_finish_position_proxy",
            "trackCondition": "unavailable_not_captured",
            "trackVariant": (
                "validated_prior_only_reference"
                if track_variant["state"] == "AVAILABLE_PRIOR_ONLY"
                else "unavailable_no_prior_store"
            ),
        },
        "coverage": {
            "identityComplete": identity_complete,
            "snapshotTimingValid": timing_valid,
            "fieldSizeAvailable": field_size > 0,
            "paceAvailable": pace["state"] == "AVAILABLE",
            "paceSourceProvenRunnerCount": pace["sourceProvenRunnerCount"],
            "paceSourceProvenRunnerPct": pace["sourceProvenRunnerPct"],
            "trackConditionAvailable": False,
            "priorTrackVariantAvailable": track_variant["state"] == "AVAILABLE_PRIOR_ONLY",
        },
        "evaluationPolicy": {
            "winnerTop3ImpactState": "WAITING_FOR_FUTURE_FULL_LABELS",
            "requiresImmutableHash": True,
            "requiresFullyLabeledRace": True,
            "requiresChronologicalOuterWindow": True,
        },
    }


def build_race_signal_ledger(
    horses: list[dict[str, Any]],
    *,
    target_distance: Any,
    target_track: Any,
    target_city: Any = None,
    profile: str = "",
    large_field_threshold: int = DEFAULT_LARGE_FIELD_THRESHOLD,
    race_id: Any = None,
    race_date: Any = None,
    race_no: Any = None,
    race_time: Any = None,
    city_id: Any = None,
    captured_ts: Any = None,
    prior_track_variant: Any = None,
) -> dict[str, Any]:
    """Build a race-level research ledger without mutating input rankings."""
    rows: list[dict[str, Any]] = []
    for index, horse in enumerate(horses or []):
        name = str(horse.get("name") or horse.get("horse_name") or f"runner-{index + 1}")
        races = horse.get("raceHistory")
        if races is None:
            races = horse.get("races")
        telemetry = build_horse_signal_telemetry(
            races if isinstance(races, list) else [],
            target_distance=target_distance,
            target_track=target_track,
            target_city=target_city,
            target_race_date=race_date,
            target_race_id=race_id,
        )
        rows.append({"horseName": name, "telemetry": telemetry})

    field_size = len(rows)
    comparable_count = sum(
        1 for row in rows if row["telemetry"]["flags"]["hasComparableTimedRaces"]
    )
    coverage = comparable_count / field_size if field_size else 0.0

    feature_specs = {
        "recentComparableSpeed": ("recent2AdjustedSpeedMps", True),
        "recentVsBaseline": ("recentVsBaselineSpeedPct", True),
        "comparableSpeedTrend": ("speedTrendPctPerRace", True),
        "comparableSpeedStability": ("speedVolatilityPct", False),
    }
    for score_name, (feature_name, higher_is_better) in feature_specs.items():
        values = {}
        for row in rows:
            feature_value = row["telemetry"]["features"].get(feature_name)
            if feature_value is not None:
                values[row["horseName"]] = float(feature_value)
        scores = _percentile_scores(values, higher_is_better=higher_is_better)
        for row in rows:
            row.setdefault("fieldDiagnosticScores", {})[score_name] = scores.get(row["horseName"])

    profile_key = _fold_text(profile).replace(" ", "")
    is_sart1_profile = bool(
        re.match(r"^(?:SART1|SARTLI1)(?:$|[^0-9])", profile_key)
    )
    point_in_time = _build_point_in_time_context(
        horses,
        rows,
        race_id=race_id,
        race_date=race_date,
        race_no=race_no,
        race_time=race_time,
        city=target_city,
        city_id=city_id,
        target_distance=target_distance,
        target_track=target_track,
        profile=profile,
        captured_ts=captured_ts,
        large_field_threshold=large_field_threshold,
        prior_track_variant=prior_track_variant,
    )
    return {
        "schemaVersion": SCHEMA_VERSION,
        "mode": "prospective_diagnostic_only",
        "usedForRanking": False,
        "rolloutEligible": False,
        "profile": profile,
        "context": {
            "fieldSize": field_size,
            "largeFieldThreshold": int(large_field_threshold),
            "isLargeField": field_size >= int(large_field_threshold),
            "isDataPoorProfile": (
                profile_key.startswith("MAIDEN")
                or is_sart1_profile
            ),
        },
        "coverage": {
            "comparableRunnerCount": comparable_count,
            "comparableRunnerPct": round(coverage * 100.0, 1),
            "fieldScoresInformative": comparable_count >= 3 and coverage >= 0.60,
        },
        "pointInTime": point_in_time,
        "horses": rows,
        "promotionPolicy": {
            "state": "COLLECT_ONLY",
            "minimumFullyLabeledRaces": 30,
            "minimumOuterRaces": 12,
            "minimumRecentRaces": 12,
            "requiresChronologicalReplay": True,
            "requiresNoTop1Top5Damage": True,
            "requiresCalibrationNoWorse": True,
            "reason": "Raw histories were not stored in historical prediction rows.",
        },
    }


__all__ = [
    "SCHEMA_VERSION",
    "POINT_IN_TIME_SCHEMA_VERSION",
    "TRACK_VARIANT_REFERENCE_SCHEMA_VERSION",
    "build_horse_signal_telemetry",
    "build_race_signal_ledger",
    "point_in_time_snapshot_sha256",
    "runner_identity_key",
    "verify_point_in_time_snapshot",
]
