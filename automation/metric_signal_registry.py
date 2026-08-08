"""Daily, analysis-only metric signal registry for the active v4 ranking.

The registry never changes ranking weights.  It consumes the persisted
``predictions.jsonl`` file, keeps partial/invalid races out of performance
evidence, and records two distinct questions:

* Did a metric help Winner Top3 on clean historical races?
* Was the visible Top3 sufficiently separated to be useful to a person?

The JSON output is intentionally verbose and machine-readable.  The Markdown
output is a compact operator summary for the nightly Raspberry workflow.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import re
import statistics
import tempfile
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


TERMINAL_FINISH_POSITIONS = {99}
NEUTRAL_SCORE = 50.0
NON_NEUTRAL_EPSILON = 1.0
PLUS_WEIGHT_POINTS = 2.0

GROUP_ANALYSIS_RACES = 30
PROFILE_ANALYSIS_RACES = 12
GROUP_LIVE_RACES = 60
PROFILE_LIVE_RACES = 30
GROUP_OUTER_RACES = 12
PROFILE_OUTER_RACES = 6

METRIC_KEYS = [
    "degree_avg", "degree_trend", "degree_stability",
    "form_trend", "track_suit", "track_experience_score",
    "surface_transition_score", "distance_suit", "distance_transition_score",
    "training_fitness", "training_degree_score",
    "weight_impact", "handicap_efficiency_score",
    "handicap_weight_relief_score", "handicap_class_transition_score",
    "handicap_load_value_score", "weight_change_risk_score",
    "handicap_class_load_transition_score",
    "field_relative_value_score", "pace_map_edge_score",
    "surface_switch_safety_score", "favorite_risk_guard_score",
    "class_peak_score", "elite_consensus_score",
    "running_style_proxy_score", "recent_finish_position_score",
    "start_draw_score", "late_start_risk_score",
    "track_condition_suit_score", "handicap_age_curve_score",
    "jockey_score", "bounce_score", "pace_score", "pedigree",
    "hp_score", "trainer_score", "agf_score", "age_score",
]

# These are the exported camelCase forms of calculate_v4_shadow_score's source
# guards.  A missing flag falls back to feature presence for old rows; an
# explicit false flag always wins.
METRIC_SOURCE_FLAGS = {
    "agf_score": "hasAgf",
    "hp_score": "hasHp",
    "weight_impact": "hasWeight",
    "jockey_score": "hasJockey",
    "trainer_score": "hasTrainer",
    "training_fitness": "hasTraining",
    "training_degree_score": "hasTrainingTimes",
    "pedigree": "hasPedigree",
    "age_score": "hasAgeActionable",
    "track_experience_score": "hasTrackExperience",
    "surface_transition_score": "hasSurfaceTransition",
    "distance_transition_score": "hasDistanceTransition",
    "handicap_efficiency_score": "hasHandicapEfficiency",
    "handicap_weight_relief_score": "hasHandicapWeightRelief",
    "handicap_class_transition_score": "hasHandicapClassHistory",
    "handicap_load_value_score": "hasHandicapLoadValue",
    "weight_change_risk_score": "hasWeightChangeRisk",
    "handicap_class_load_transition_score": "hasHandicapClassLoadTransition",
    "field_relative_value_score": "hasFieldRelativeValue",
    "pace_map_edge_score": "hasPaceMapEdge",
    "surface_switch_safety_score": "hasSurfaceSwitchSafety",
    "favorite_risk_guard_score": "hasFavoriteRiskGuard",
    "class_peak_score": "hasClassPeak",
    "elite_consensus_score": "hasEliteConsensus",
    "recent_finish_position_score": "hasRecentFinishPosition",
    "start_draw_score": "hasStartDraw",
    "late_start_risk_score": "hasLateStartRisk",
    "track_condition_suit_score": "hasTrackConditionSuit",
    "handicap_age_curve_score": "hasHandicapAgeCurve",
}

UNGATED_METRICS = sorted(set(METRIC_KEYS) - set(METRIC_SOURCE_FLAGS))


def safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, ""):
            return default
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError):
        return default


def safe_int(value: Any, default: int = 999) -> int:
    numeric = safe_float(value)
    return int(numeric) if numeric is not None else default


def rounded(value: float | None, digits: int = 4) -> float | None:
    return round(value, digits) if value is not None and math.isfinite(value) else None


def rate(numerator: int, denominator: int) -> float | None:
    return rounded(numerator / denominator, 4) if denominator else None


def parse_race_date(value: Any) -> datetime | None:
    for pattern in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(value or "").strip(), pattern)
        except ValueError:
            continue
    return None


def fold_text(value: Any) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    ascii_text = "".join(char for char in normalized if not unicodedata.combining(char))
    return ascii_text.upper().replace("Ş", "S").replace("İ", "I")


def track_bucket(value: Any) -> str:
    folded = fold_text(value)
    if "CIM" in folded:
        return "Cim"
    if "SENTETIK" in folded:
        return "Sentetik"
    if "KUM" in folded:
        return "Kum"
    return str(value or "UNKNOWN").strip() or "UNKNOWN"


def load_jsonl(path: Path) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    invalid = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                invalid += 1
                continue
            if isinstance(value, dict):
                rows.append(value)
            else:
                invalid += 1
    return rows, invalid


def race_key(row: dict[str, Any]) -> tuple[str, str]:
    race_id = str(row.get("race_id") or "").strip()
    if not race_id:
        race_id = "|".join(
            str(row.get(key) or "").strip()
            for key in ("city_id", "city", "race_no", "race_time")
        )
    return str(row.get("race_date") or "").strip(), race_id


def group_races(entries: Iterable[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        grouped[race_key(entry)].append(entry)
    return sorted(grouped.values(), key=race_sort_key)


def race_sort_key(rows: list[dict[str, Any]]) -> tuple[datetime, str, int, str]:
    first = rows[0]
    return (
        parse_race_date(first.get("race_date")) or datetime.min,
        str(first.get("city") or ""),
        safe_int(first.get("race_no"), 0),
        str(first.get("race_id") or ""),
    )


def profile_of(rows: list[dict[str, Any]]) -> dict[str, str]:
    first = rows[0]
    profile = first.get("v4_profile") or {}
    if not isinstance(profile, dict):
        profile = {}
    folded_type = fold_text(first.get("race_type"))
    category = str(profile.get("category") or "").upper()
    subtype = str(profile.get("subtype") or "").upper()
    if not category:
        for candidate in ("HANDIKAP", "MAIDEN", "SARTLI", "SATIS", "GRUP", "KV"):
            if candidate in folded_type:
                category = candidate
                break
    if not subtype:
        match = re.search(r"(HANDIKAP|SARTLI)\s*(\d+)", folded_type)
        subtype = f"{match.group(1)}{match.group(2)}" if match else category
    # Older exports used both SART4 and SARTLI4 for the same profile.
    subtype = re.sub(r"^SARTLI(?=\d)", "SART", subtype)
    return {
        "category": category or "OTHER",
        "subtype": subtype or category or "OTHER",
        "track": track_bucket(profile.get("track") or first.get("track")),
        "selectedKey": str(profile.get("selectedKey") or ""),
        "profileKey": str(profile.get("profileKey") or ""),
    }


def scope_keys(rows: list[dict[str, Any]]) -> list[tuple[str, str]]:
    profile = profile_of(rows)
    return [
        ("GROUP", profile["category"]),
        ("PROFILE", profile["subtype"]),
        ("PROFILE_SURFACE", f'{profile["subtype"]}|{profile["track"]}'),
    ]


def classify_race(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "unlabeled"
    expected_values = {
        safe_int(row.get("field_size"), len(rows))
        for row in rows
    }
    names = [str(row.get("horse_name") or "").strip().casefold() for row in rows]
    if len(expected_values) != 1 or next(iter(expected_values)) != len(rows):
        return "integrity_invalid"
    if any(not name for name in names) or len(set(names)) != len(names):
        return "integrity_invalid"

    labels = [safe_int(row.get("finish_pos"), 0) for row in rows]
    labeled = sum(value > 0 for value in labels)
    if labeled == 0:
        return "unlabeled"
    if labeled != len(rows):
        return "partial"
    if labels.count(1) != 1:
        return "integrity_invalid"

    ranked = [value for value in labels if value not in TERMINAL_FINISH_POSITIONS]
    if any(value < 1 or value > len(rows) for value in ranked):
        return "integrity_invalid"
    expected_rank = 1
    for rank_value, tied_count in sorted(Counter(ranked).items()):
        if rank_value != expected_rank:
            return "integrity_invalid"
        expected_rank += tied_count

    predicted = [safe_int(row.get("rank_pred"), 0) for row in rows]
    if sorted(predicted) != list(range(1, len(rows) + 1)):
        return "integrity_invalid"
    return "fully_labeled"


def feature_value(row: dict[str, Any], metric: str) -> float | None:
    features = row.get("features") or {}
    return safe_float(features.get(metric)) if isinstance(features, dict) else None


def metric_has_source(row: dict[str, Any], metric: str) -> bool:
    """Mirror live scoring availability for control-replay fidelity."""
    value = feature_value(row, metric)
    if value is None:
        return False
    flag_key = METRIC_SOURCE_FLAGS.get(metric)
    flags = row.get("metric_source_flags") or {}
    if flag_key and isinstance(flags, dict) and flag_key in flags:
        return bool(flags.get(flag_key))
    return True


def metric_source_proven(row: dict[str, Any], metric: str) -> bool:
    """Return conservative evidence coverage; missing explicit flags are false."""
    if feature_value(row, metric) is None:
        return False
    flag_key = METRIC_SOURCE_FLAGS.get(metric)
    if not flag_key:
        return True
    flags = row.get("metric_source_flags") or {}
    return bool(isinstance(flags, dict) and flags.get(flag_key) is True)


def active_weights(row: dict[str, Any]) -> dict[str, float]:
    raw = row.get("v4_weights") or {}
    if not isinstance(raw, dict):
        return {}
    return {
        metric: max(0.0, safe_float(raw.get(metric), 0.0) or 0.0)
        for metric in METRIC_KEYS
        if (safe_float(raw.get(metric), 0.0) or 0.0) > 0.0
    }


def score_with_weights(
    row: dict[str, Any],
    weights: dict[str, float],
    *,
    excluded_metric: str | None = None,
    added_metric: str | None = None,
    added_points: float = 0.0,
) -> float:
    weighted_sum = 0.0
    total = 0.0
    for metric in METRIC_KEYS:
        weight = weights.get(metric, 0.0)
        if metric == excluded_metric:
            weight = 0.0
        if metric == added_metric:
            weight += added_points
        if weight <= 0.0 or not metric_has_source(row, metric):
            continue
        value = feature_value(row, metric)
        if value is None:
            continue
        weighted_sum += value * weight
        total += weight
    base = weighted_sum / total if total > 0.0 else NEUTRAL_SCORE
    penalty = safe_float(row.get("v4_penalty_total"), 0.0) or 0.0
    return max(0.0, min(100.0, base - penalty))


def winner(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    return next((row for row in rows if safe_int(row.get("finish_pos"), 0) == 1), None)


def rank_of_row(rows: list[dict[str, Any]], target: dict[str, Any], scores: dict[int, float]) -> int:
    ordered = sorted(
        rows,
        key=lambda row: (
            -(scores.get(id(row), -1.0)),
            safe_int(row.get("rank_pred"), 999),
            str(row.get("horse_name") or ""),
        ),
    )
    return next(index for index, row in enumerate(ordered, start=1) if row is target)


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    low = math.floor(position)
    high = math.ceil(position)
    if low == high:
        return ordered[low]
    return ordered[low] * (high - position) + ordered[high] * (position - low)


def normalized_entropy(scores: list[float], temperature: float = 18.0) -> float | None:
    if len(scores) < 2:
        return None
    peak = max(scores)
    exponentials = [math.exp((score - peak) / temperature) for score in scores]
    total = sum(exponentials)
    probabilities = [value / total for value in exponentials]
    raw = -sum(probability * math.log(probability) for probability in probabilities if probability > 0)
    return raw / math.log(len(scores))


def bootstrap_top3_stability(rows: list[dict[str, Any]], iterations: int) -> float | None:
    if iterations <= 0 or len(rows) < 4:
        return None
    baseline_top3 = {
        id(row)
        for row in sorted(rows, key=lambda row: safe_int(row.get("rank_pred")))[:3]
    }
    weights = active_weights(rows[0])
    if not weights:
        return None
    seed_text = "|".join(str(part) for part in race_key(rows[0]))
    seed = int.from_bytes(hashlib.sha256(seed_text.encode("utf-8")).digest()[:8], "big")
    generator = random.Random(seed)
    retained = 0.0
    for _ in range(iterations):
        perturbed = {
            metric: weight * math.exp(generator.gauss(0.0, 0.10))
            for metric, weight in weights.items()
        }
        scores = {id(row): score_with_weights(row, perturbed) for row in rows}
        simulated = {
            id(row)
            for row in sorted(
                rows,
                key=lambda row: (-scores[id(row)], safe_int(row.get("rank_pred"))),
            )[:3]
        }
        retained += len(baseline_top3 & simulated) / 3.0
    return retained / iterations


def score_diagnostics(rows: list[dict[str, Any]], bootstrap_iterations: int = 40) -> dict[str, Any] | None:
    ordered = sorted(rows, key=lambda row: safe_int(row.get("rank_pred")))
    scores = [safe_float(row.get("ai_score")) for row in ordered]
    if len(scores) < 4 or any(score is None for score in scores):
        return None
    numeric_scores = [float(score) for score in scores if score is not None]
    cutoff = numeric_scores[2]
    top3_gap = numeric_scores[2] - numeric_scores[3]
    crowd = sum(abs(score - cutoff) <= 2.0 for score in numeric_scores)
    entropy = normalized_entropy(numeric_scores)

    weights = active_weights(ordered[0])
    total_weight = sum(weights.values())
    informative_weight = 0.0
    weighted_source = 0.0
    weighted_neutral = 0.0
    for metric, weight in weights.items():
        values = [feature_value(row, metric) for row in ordered]
        source = [metric_source_proven(row, metric) for row in ordered]
        covered_values = [
            value for value, has_source in zip(values, source)
            if has_source and value is not None
        ]
        coverage = sum(source) / len(ordered)
        non_neutral = sum(
            abs(value - NEUTRAL_SCORE) >= NON_NEUTRAL_EPSILON
            for value in covered_values
        ) / len(ordered)
        weighted_source += weight * coverage
        weighted_neutral += weight * max(0.0, coverage - non_neutral)
        if coverage >= 0.5 and len(covered_values) >= 2 and statistics.pstdev(covered_values) >= 1.0:
            informative_weight += weight

    if top3_gap < 0.5 or crowd >= 7 or (entropy is not None and entropy >= 0.985):
        separation = "RED"
    elif top3_gap < 1.5 or crowd >= 5 or (entropy is not None and entropy >= 0.970):
        separation = "YELLOW"
    else:
        separation = "GREEN"

    first = ordered[0]
    profile = profile_of(rows)
    return {
        "raceId": str(first.get("race_id") or ""),
        "raceDate": first.get("race_date"),
        "raceNo": first.get("race_no"),
        "raceTime": first.get("race_time"),
        "city": first.get("city"),
        "raceType": first.get("race_type"),
        "category": profile["category"],
        "profile": profile["subtype"],
        "track": profile["track"],
        "fieldSize": len(rows),
        "scoreStd": rounded(statistics.pstdev(numeric_scores), 3),
        "scoreRange": rounded(max(numeric_scores) - min(numeric_scores), 3),
        "top1Top2Gap": rounded(numeric_scores[0] - numeric_scores[1], 3),
        "top3Top4Gap": rounded(top3_gap, 3),
        "cutoffCrowd2pt": crowd,
        "normalizedEntropy": rounded(entropy, 4),
        "effectiveFieldRatio": rounded(math.exp((entropy or 0.0) * math.log(len(rows))) / len(rows), 4),
        "informativeWeightShare": rounded(informative_weight / total_weight if total_weight else 0.0, 4),
        "weightedRealCoverage": rounded(weighted_source / total_weight if total_weight else 0.0, 4),
        "neutralFallbackWeightShare": rounded(weighted_neutral / total_weight if total_weight else 0.0, 4),
        "top3BootstrapStability": rounded(bootstrap_top3_stability(rows, bootstrap_iterations), 4),
        "separationStatus": separation,
    }


def race_metric_outcome(rows: list[dict[str, Any]], metric: str) -> dict[str, Any] | None:
    race_winner = winner(rows)
    if race_winner is None:
        return None
    source_rows = [row for row in rows if metric_source_proven(row, metric)]
    values = [feature_value(row, metric) for row in source_rows]
    numeric = [float(value) for value in values if value is not None]
    baseline_hit = safe_int(race_winner.get("rank_pred"), 999) <= 3

    univariate_hit: bool | None = None
    if metric_source_proven(race_winner, metric) and len(source_rows) >= 3:
        metric_scores = {
            id(row): feature_value(row, metric)
            if metric_source_proven(row, metric)
            else -1.0
            for row in rows
        }
        univariate_hit = rank_of_row(rows, race_winner, metric_scores) <= 3

    weights = active_weights(rows[0])
    replay_scores = {
        id(row): score_with_weights(row, weights)
        for row in rows
    }
    replay_hit = rank_of_row(rows, race_winner, replay_scores) <= 3
    visible_top3 = {
        id(row)
        for row in sorted(rows, key=lambda row: safe_int(row.get("rank_pred")))[:3]
    }
    replay_top3 = {
        id(row)
        for row in sorted(
            rows,
            key=lambda row: (-replay_scores[id(row)], safe_int(row.get("rank_pred"))),
        )[:3]
    }
    plus_scores = {
        id(row): score_with_weights(
            row,
            weights,
            added_metric=metric,
            added_points=PLUS_WEIGHT_POINTS,
        )
        for row in rows
    }
    plus_hit = rank_of_row(rows, race_winner, plus_scores) <= 3

    ablation_hit: bool | None = None
    if weights.get(metric, 0.0) > 0.0:
        ablation_scores = {
            id(row): score_with_weights(row, weights, excluded_metric=metric)
            for row in rows
        }
        ablation_hit = rank_of_row(rows, race_winner, ablation_scores) <= 3

    return {
        "race": rows,
        "baseline": baseline_hit,
        "replayBaseline": replay_hit,
        "replayWinnerTop3Agreement": baseline_hit == replay_hit,
        "replayTop3SetAgreement": visible_top3 == replay_top3,
        "univariate": univariate_hit,
        "plus2": plus_hit,
        "ablation": ablation_hit,
        "coverageRows": len(source_rows),
        "sourceFlagPresentRows": sum(
            isinstance(row.get("metric_source_flags"), dict)
            and METRIC_SOURCE_FLAGS.get(metric) in (row.get("metric_source_flags") or {})
            for row in rows
        ) if metric in METRIC_SOURCE_FLAGS else len(rows),
        "totalRows": len(rows),
        "nonNeutralRows": sum(
            abs(value - NEUTRAL_SCORE) >= NON_NEUTRAL_EPSILON
            for value in numeric
        ),
        "withinRaceStd": statistics.pstdev(numeric) if len(numeric) >= 2 else None,
        "weight": weights.get(metric, 0.0),
    }


def hit_summary(
    outcomes: list[dict[str, Any]],
    key: str,
    *,
    baseline_key: str = "baseline",
) -> dict[str, Any]:
    comparable = [item for item in outcomes if item.get(key) is not None]
    baseline = sum(bool(item[baseline_key]) for item in comparable)
    candidate = sum(bool(item[key]) for item in comparable)
    return {
        "races": len(comparable),
        "baselineWinnerTop3": baseline,
        "candidateWinnerTop3": candidate,
        "deltaHits": candidate - baseline,
        "baselineRate": rate(baseline, len(comparable)),
        "candidateRate": rate(candidate, len(comparable)),
    }


def chronological_slices(outcomes: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    count = len(outcomes)
    build_end = max(1, math.floor(count * 0.60)) if count else 0
    inner_end = max(build_end + 1, math.floor(count * 0.80)) if count > build_end else build_end
    inner_end = min(inner_end, count)
    return {
        "build": outcomes[:build_end],
        "inner": outcomes[build_end:inner_end],
        "outer": outcomes[inner_end:],
    }


def metric_status(
    *,
    clean_races: int,
    analysis_threshold: int,
    live_threshold: int,
    outer_threshold: int,
    coverage: float,
    non_neutral: float,
    plus2_full: dict[str, Any],
    plus2_inner: dict[str, Any],
    plus2_outer: dict[str, Any],
) -> tuple[str, list[str]]:
    blockers: list[str] = []
    if clean_races < analysis_threshold:
        blockers.append(f"clean_races<{analysis_threshold}")
        return "COLLECTING", blockers
    if coverage < 0.25:
        blockers.append("coverage<0.25")
    if non_neutral < 0.10:
        blockers.append("non_neutral<0.10")
    if blockers:
        return "DATA_LOW", blockers
    live_ready = clean_races >= live_threshold and plus2_outer["races"] >= outer_threshold
    if not live_ready:
        if clean_races < live_threshold:
            blockers.append(f"live_clean_races<{live_threshold}")
        if plus2_outer["races"] < outer_threshold:
            blockers.append(f"outer_races<{outer_threshold}")

    if plus2_outer["deltaHits"] < 0 or plus2_inner["deltaHits"] < 0:
        blockers.append("chronological_winner_top3_regression")
        return "HOLD", blockers
    if plus2_full["deltaHits"] < 0:
        blockers.append("full_winner_top3_regression")
        return "HOLD", blockers
    if (
        live_ready
        and plus2_full["deltaHits"] >= 2
        and plus2_outer["deltaHits"] >= 1
        and plus2_inner["deltaHits"] >= 0
    ):
        return "CANDIDATE_FOR_REPLAY", blockers
    return "WATCH", blockers


def summarize_metric(
    clean_races: list[list[dict[str, Any]]],
    metric: str,
    *,
    analysis_threshold: int,
    live_threshold: int,
    outer_threshold: int,
) -> dict[str, Any]:
    outcomes = [
        outcome
        for rows in clean_races
        if (outcome := race_metric_outcome(rows, metric)) is not None
    ]
    evidence_outcomes = [item for item in outcomes if item["replayTop3SetAgreement"]]
    total_rows = sum(item["totalRows"] for item in outcomes)
    coverage_rows = sum(item["coverageRows"] for item in outcomes)
    non_neutral_rows = sum(item["nonNeutralRows"] for item in outcomes)
    source_flag_present_rows = sum(item["sourceFlagPresentRows"] for item in outcomes)
    coverage = coverage_rows / total_rows if total_rows else 0.0
    non_neutral = non_neutral_rows / total_rows if total_rows else 0.0
    std_values = [
        float(item["withinRaceStd"])
        for item in outcomes
        if item["withinRaceStd"] is not None
    ]
    weight_values = [float(item["weight"]) for item in outcomes]

    slices = chronological_slices(evidence_outcomes)
    full_plus = hit_summary(evidence_outcomes, "plus2", baseline_key="replayBaseline")
    inner_plus = hit_summary(slices["inner"], "plus2", baseline_key="replayBaseline")
    outer_plus = hit_summary(slices["outer"], "plus2", baseline_key="replayBaseline")
    replay_winner_agreement = rate(
        sum(bool(item["replayWinnerTop3Agreement"]) for item in outcomes),
        len(outcomes),
    ) or 0.0
    replay_top3_set_agreement = rate(
        sum(bool(item["replayTop3SetAgreement"]) for item in outcomes),
        len(outcomes),
    ) or 0.0
    status, blockers = metric_status(
        clean_races=len(evidence_outcomes),
        analysis_threshold=analysis_threshold,
        live_threshold=live_threshold,
        outer_threshold=outer_threshold,
        coverage=coverage,
        non_neutral=non_neutral,
        plus2_full=full_plus,
        plus2_inner=inner_plus,
        plus2_outer=outer_plus,
    )

    rolling = {
        str(window): hit_summary(
            evidence_outcomes[-window:],
            "plus2",
            baseline_key="replayBaseline",
        )
        for window in (30, 60, 120)
    }
    return {
        "metric": metric,
        "sourceGate": METRIC_SOURCE_FLAGS.get(metric),
        "coverageKind": "explicit_flag" if metric in METRIC_SOURCE_FLAGS else "feature_presence",
        "coverage": rounded(coverage, 4),
        "sourceFlagPresenceRate": rounded(source_flag_present_rows / total_rows if total_rows else 0.0, 4),
        "nonNeutralRate": rounded(non_neutral, 4),
        "withinRaceStdMedian": rounded(statistics.median(std_values), 3) if std_values else None,
        "currentWeightPctMedian": rounded(statistics.median(weight_values), 3) if weight_values else 0.0,
        "univariate": hit_summary(outcomes, "univariate"),
        "controlReplay": {
            "races": len(outcomes),
            "signalEvidenceRaces": len(evidence_outcomes),
            "excludedRaces": len(outcomes) - len(evidence_outcomes),
            "winnerTop3AgreementRate": replay_winner_agreement,
            "top3SetAgreementRate": replay_top3_set_agreement,
        },
        "boundedPlus2": {
            "full": full_plus,
            "build": hit_summary(slices["build"], "plus2", baseline_key="replayBaseline"),
            "inner": inner_plus,
            "outer": outer_plus,
            "rolling": rolling,
        },
        "ablation": hit_summary(evidence_outcomes, "ablation", baseline_key="replayBaseline"),
        "status": status,
        "blockers": blockers,
    }


def summarize_scope(
    scope_type: str,
    scope_key: str,
    races: list[list[dict[str, Any]]],
    states: dict[tuple[str, str], str],
) -> dict[str, Any]:
    coverage_counts = Counter(states[race_key(rows[0])] for rows in races)
    clean = [rows for rows in races if states[race_key(rows[0])] == "fully_labeled"]
    if scope_type == "GROUP":
        analysis_threshold = GROUP_ANALYSIS_RACES
        live_threshold = GROUP_LIVE_RACES
        outer_threshold = GROUP_OUTER_RACES
    else:
        analysis_threshold = PROFILE_ANALYSIS_RACES
        live_threshold = PROFILE_LIVE_RACES
        outer_threshold = PROFILE_OUTER_RACES

    metrics = [
        summarize_metric(
            clean,
            metric,
            analysis_threshold=analysis_threshold,
            live_threshold=live_threshold,
            outer_threshold=outer_threshold,
        )
        for metric in METRIC_KEYS
    ]
    status_counts = Counter(item["status"] for item in metrics)
    return {
        "scopeType": scope_type,
        "scopeKey": scope_key,
        "coverage": {
            "totalRaces": len(races),
            "fullyLabeledRaces": coverage_counts["fully_labeled"],
            "partialRaces": coverage_counts["partial"],
            "unlabeledRaces": coverage_counts["unlabeled"],
            "integrityInvalidRaces": coverage_counts["integrity_invalid"],
        },
        "thresholds": {
            "analysisCleanRaces": analysis_threshold,
            "liveCleanRaces": live_threshold,
            "outerRaces": outer_threshold,
            "remainingForAnalysis": max(0, analysis_threshold - len(clean)),
            "remainingForLive": max(0, live_threshold - len(clean)),
        },
        "metricStatusCounts": dict(sorted(status_counts.items())),
        "metrics": metrics,
    }


def build_report(
    entries: list[dict[str, Any]],
    run_date: str,
    *,
    invalid_json_lines: int = 0,
    bootstrap_iterations: int = 40,
) -> dict[str, Any]:
    races = group_races(entries)
    states = {race_key(rows[0]): classify_race(rows) for rows in races}
    overall = Counter(states.values())

    scope_races: dict[tuple[str, str], list[list[dict[str, Any]]]] = defaultdict(list)
    for rows in races:
        for scope in scope_keys(rows):
            scope_races[scope].append(rows)

    scopes = [
        summarize_scope(scope_type, scope_key, rows, states)
        for (scope_type, scope_key), rows in sorted(scope_races.items())
    ]

    requested_day = parse_race_date(run_date)
    requested_display = requested_day.strftime("%d.%m.%Y") if requested_day else run_date
    daily_diagnostics = [
        diagnostic
        for rows in races
        if str(rows[0].get("race_date") or "") == requested_display
        if (diagnostic := score_diagnostics(rows, bootstrap_iterations)) is not None
    ]
    separation_counts = Counter(item["separationStatus"] for item in daily_diagnostics)

    versions = Counter(
        str(row.get("v4_version") or "unknown")
        for row in entries
    )
    source_timestamp = max(
        (safe_int(row.get("ts"), 0) for row in entries),
        default=0,
    )
    source_snapshot_at = (
        datetime.fromtimestamp(source_timestamp, tz=timezone.utc).isoformat(timespec="seconds")
        if source_timestamp > 0
        else None
    )
    return {
        "schemaVersion": "metric-signal-registry-v1",
        "runDate": requested_day.strftime("%Y-%m-%d") if requested_day else run_date,
        # Stable source timestamp keeps repeated nightly/recovery runs idempotent.
        "sourceSnapshotAt": source_snapshot_at,
        "rankingVersions": dict(sorted(versions.items())),
        "inventory": {
            "metricCount": len(METRIC_KEYS),
            "sourceGatedMetricCount": len(METRIC_SOURCE_FLAGS),
            "ungatedMetrics": UNGATED_METRICS,
            "plusWeightPoints": PLUS_WEIGHT_POINTS,
        },
        "coverage": {
            "totalRows": len(entries),
            "validJsonRows": len(entries),
            "invalidJsonLines": invalid_json_lines,
            "totalRaces": len(races),
            "fullyLabeledRaces": overall["fully_labeled"],
            "partialRaces": overall["partial"],
            "unlabeledRaces": overall["unlabeled"],
            "integrityInvalidRaces": overall["integrity_invalid"],
        },
        "dailyScoreDiagnostics": {
            "races": len(daily_diagnostics),
            "statusCounts": dict(sorted(separation_counts.items())),
            "items": daily_diagnostics,
        },
        "scopes": scopes,
        "policy": {
            "analysisThresholds": {"group": GROUP_ANALYSIS_RACES, "profile": PROFILE_ANALYSIS_RACES},
            "liveThresholds": {"group": GROUP_LIVE_RACES, "profile": PROFILE_LIVE_RACES},
            "outerThresholds": {"group": GROUP_OUTER_RACES, "profile": PROFILE_OUTER_RACES},
            "metricCoverageMinimum": 0.25,
            "metricNonNeutralMinimum": 0.10,
            "winnerTop3HardGate": True,
            "automaticWeightChange": False,
            "partialRacesUsedForSignal": False,
        },
    }


def format_pct(value: Any) -> str:
    numeric = safe_float(value)
    return f"%{numeric * 100:.1f}" if numeric is not None else "-"


def render_markdown(report: dict[str, Any]) -> str:
    coverage = report["coverage"]
    daily = report["dailyScoreDiagnostics"]
    lines = [
        f'# Metric Signal Registry — {report["runDate"]}',
        "",
        "Bu rapor analiz-only'dir; canlı ağırlıkları veya görünür sıralamayı değiştirmez.",
        "",
        "## Veri kalitesi",
        "",
        f'- Satır: `{coverage["validJsonRows"]}` geçerli, `{coverage["invalidJsonLines"]}` bozuk JSON.',
        (
            f'- Yarış: `{coverage["fullyLabeledRaces"]}` tam, '
            f'`{coverage["partialRaces"]}` kısmi, `{coverage["unlabeledRaces"]}` etiketsiz, '
            f'`{coverage["integrityInvalidRaces"]}` bütünlük hatalı.'
        ),
        "- Sinyal kararlarında yalnız tam ve bütünlük kontrolünü geçen yarışlar kullanıldı.",
        "",
        "## Günlük skor ayrışması",
        "",
        (
            f'- Yarış: `{daily["races"]}`; '
            f'kırmızı `{daily["statusCounts"].get("RED", 0)}`, '
            f'sarı `{daily["statusCounts"].get("YELLOW", 0)}`, '
            f'yeşil `{daily["statusCounts"].get("GREEN", 0)}`.'
        ),
        "",
        "| Şehir/R | Profil | Std | Top3–4 | ±2 puan aday | Ayırıcı ağırlık | Gerçek kaynak | Stabilite | Durum |",
        "|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for item in daily["items"]:
        race_label = f'{item.get("city") or "-"}/{item.get("raceNo") or "-"}'
        lines.append(
            "| "
            + " | ".join([
                race_label,
                str(item.get("profile") or "-"),
                str(item.get("scoreStd") if item.get("scoreStd") is not None else "-"),
                str(item.get("top3Top4Gap") if item.get("top3Top4Gap") is not None else "-"),
                str(item.get("cutoffCrowd2pt") or 0),
                format_pct(item.get("informativeWeightShare")),
                format_pct(item.get("weightedRealCoverage")),
                format_pct(item.get("top3BootstrapStability")),
                str(item.get("separationStatus") or "-"),
            ])
            + " |"
        )

    lines.extend([
        "",
        "## Grup/profil eşikleri",
        "",
        "| Kapsam | Tam | Kısmi | Analize kalan | Canlı kapısına kalan | Replay adayı | Hold |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ])
    for scope in report["scopes"]:
        if scope["scopeType"] == "PROFILE_SURFACE":
            continue
        counts = scope["metricStatusCounts"]
        lines.append(
            "| "
            + " | ".join([
                f'{scope["scopeType"]}:{scope["scopeKey"]}',
                str(scope["coverage"]["fullyLabeledRaces"]),
                str(scope["coverage"]["partialRaces"]),
                str(scope["thresholds"]["remainingForAnalysis"]),
                str(scope["thresholds"]["remainingForLive"]),
                str(counts.get("CANDIDATE_FOR_REPLAY", 0)),
                str(counts.get("HOLD", 0)),
            ])
            + " |"
        )

    candidates: list[tuple[str, dict[str, Any]]] = []
    holds: list[tuple[str, dict[str, Any]]] = []
    for scope in report["scopes"]:
        if scope["scopeType"] == "PROFILE_SURFACE":
            continue
        label = f'{scope["scopeType"]}:{scope["scopeKey"]}'
        for metric in scope["metrics"]:
            if metric["status"] == "CANDIDATE_FOR_REPLAY":
                candidates.append((label, metric))
            elif metric["status"] == "HOLD" and metric["boundedPlus2"]["full"]["deltaHits"] != 0:
                holds.append((label, metric))

    candidates.sort(
        key=lambda item: (
            item[1]["boundedPlus2"]["outer"]["deltaHits"],
            item[1]["boundedPlus2"]["full"]["deltaHits"],
        ),
        reverse=True,
    )
    holds.sort(key=lambda item: item[1]["boundedPlus2"]["outer"]["deltaHits"])
    lines.extend([
        "",
        "## Sinyal kuyruğu",
        "",
        "Replay adayları yalnız kronolojik Winner Top3 kapısını geçen analiz adaylarıdır; otomatik canlıya alınmaz.",
        "",
    ])
    if candidates:
        for label, metric in candidates[:15]:
            full = metric["boundedPlus2"]["full"]
            outer = metric["boundedPlus2"]["outer"]
            lines.append(
                f'- `CANDIDATE_FOR_REPLAY` {label} / `{metric["metric"]}`: '
                f'full `{full["deltaHits"]:+d}`, outer `{outer["deltaHits"]:+d}`, '
                f'coverage {format_pct(metric["coverage"])}.'
            )
    else:
        lines.append("- Bu çalıştırmada replay kapısını geçen yeni metrik yok.")

    if holds:
        lines.extend(["", "Kronolojik gerileme nedeniyle hold edilen güçlü uyarılar:", ""])
        for label, metric in holds[:10]:
            full = metric["boundedPlus2"]["full"]
            outer = metric["boundedPlus2"]["outer"]
            lines.append(
                f'- `HOLD` {label} / `{metric["metric"]}`: '
                f'full `{full["deltaHits"]:+d}`, outer `{outer["deltaHits"]:+d}`.'
            )

    lines.extend([
        "",
        "## Politika",
        "",
        "- Grup/profil 30/12 yalnız analiz eşiğidir.",
        "- Canlı değerlendirme grupta 60+12 outer, profilde 30+6 outer ister.",
        "- Kısmi/etiketsiz/bütünlük hatalı yarışlar performans sinyaline katılmaz.",
        "- Bu rapor ağırlık değiştirmez; adaylar ayrı replay ve shadow kapısından geçer.",
        "",
    ])
    return "\n".join(lines)


def atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=path.parent,
        delete=False,
    ) as handle:
        handle.write(content)
        temp_path = Path(handle.name)
    temp_path.replace(path)


def persist(report: dict[str, Any], data_dir: Path) -> dict[str, str]:
    run_date = report["runDate"]
    daily_dir = data_dir / "automation" / "runs" / run_date
    latest_dir = data_dir / "automation" / "metric-signals"
    json_text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    markdown = render_markdown(report)
    paths = {
        "dailyJson": daily_dir / "metric-signal-registry.json",
        "dailyMarkdown": daily_dir / "metric-signal-summary.md",
        "latestJson": latest_dir / "latest.json",
        "latestMarkdown": latest_dir / "latest.md",
    }
    for key, path in paths.items():
        atomic_write(path, json_text if key.endswith("Json") else markdown)
    return {key: str(path) for key, path in paths.items()}


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the analysis-only v4 metric signal registry.")
    parser.add_argument("--predictions", required=True, type=Path)
    parser.add_argument("--data-dir", required=True, type=Path)
    parser.add_argument("--run-date", default=datetime.now().astimezone().strftime("%Y-%m-%d"))
    parser.add_argument("--bootstrap-iterations", type=int, default=40)
    arguments = parser.parse_args()

    entries, invalid_lines = load_jsonl(arguments.predictions)
    report = build_report(
        entries,
        arguments.run_date,
        invalid_json_lines=invalid_lines,
        bootstrap_iterations=max(0, arguments.bootstrap_iterations),
    )
    paths = persist(report, arguments.data_dir)
    print(json.dumps({
        "success": True,
        "runDate": report["runDate"],
        "coverage": report["coverage"],
        "dailyScoreDiagnostics": {
            "races": report["dailyScoreDiagnostics"]["races"],
            "statusCounts": report["dailyScoreDiagnostics"]["statusCounts"],
        },
        "paths": paths,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
