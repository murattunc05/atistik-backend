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
import math
import re
import statistics
from typing import Any, Iterable


SCHEMA_VERSION = "future-comparable-speed-ledger-v1"
DEFAULT_DISTANCE_TOLERANCE_M = 100
DEFAULT_LARGE_FIELD_THRESHOLD = 12


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
    if "SENTETIK" in folded or folded in {"S", "S:"}:
        return "sentetik"
    if "KUM" in folded or folded in {"K", "K:"}:
        return "kum"
    if "CIM" in folded or folded in {"C", "C:"}:
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


def build_horse_signal_telemetry(
    races: list[dict[str, Any]] | None,
    *,
    target_distance: Any,
    target_track: Any,
    target_city: Any = None,
    distance_tolerance_m: int = DEFAULT_DISTANCE_TOLERANCE_M,
) -> dict[str, Any]:
    """Build raw, prospective timing diagnostics for one horse.

    No score is emitted here.  Values remain in physical units or percentages
    so a later chronological evaluation can choose a stable transformation.
    """
    history, dates_complete = _chronological_history(list(races or []))
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


def build_race_signal_ledger(
    horses: list[dict[str, Any]],
    *,
    target_distance: Any,
    target_track: Any,
    target_city: Any = None,
    profile: str = "",
    large_field_threshold: int = DEFAULT_LARGE_FIELD_THRESHOLD,
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
                or profile_key.startswith("SART1")
                or profile_key.startswith("SARTLI1")
            ),
        },
        "coverage": {
            "comparableRunnerCount": comparable_count,
            "comparableRunnerPct": round(coverage * 100.0, 1),
            "fieldScoresInformative": comparable_count >= 3 and coverage >= 0.60,
        },
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
    "build_horse_signal_telemetry",
    "build_race_signal_ledger",
]
