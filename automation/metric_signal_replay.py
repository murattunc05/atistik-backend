"""Full retrospective replay for Metric Signal Registry candidates.

This script is deliberately promotion-safe: a positive result can only move a
metric into prospective shadow observation.  It never changes live weights.
Registry discovery and this replay use overlapping historical data, so the
outer split is diagnostic rather than a truly untouched promotion holdout.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from automation.metric_signal_registry import (
        METRIC_KEYS,
        PLUS_WEIGHT_POINTS,
        active_weights,
        classify_race,
        group_races,
        load_jsonl,
        profile_of,
        race_key,
        race_metric_outcome,
        rounded,
        safe_float,
        safe_int,
        score_with_weights,
        winner,
    )
except ModuleNotFoundError:  # direct: python automation/metric_signal_replay.py
    from metric_signal_registry import (
        METRIC_KEYS,
        PLUS_WEIGHT_POINTS,
        active_weights,
        classify_race,
        group_races,
        load_jsonl,
        profile_of,
        race_key,
        race_metric_outcome,
        rounded,
        safe_float,
        safe_int,
        score_with_weights,
        winner,
    )


MIN_COMPATIBLE_VERSION = (4, 21)
TEMPERATURE_GRID = (
    2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0,
    11.0, 12.0, 14.0, 16.0, 18.0, 20.0, 24.0, 28.0, 32.0,
)


def version_tuple(value: Any) -> tuple[int, ...]:
    parts = []
    for part in str(value or "").lstrip("vV").split("."):
        digits = "".join(character for character in part if character.isdigit())
        if not digits:
            break
        parts.append(int(digits))
    return tuple(parts)


def compatible_race(rows: list[dict[str, Any]]) -> bool:
    versions = {version_tuple(row.get("v4_version")) for row in rows}
    applied = {row.get("v4_applied_for_ranking") for row in rows}
    return bool(
        len(versions) == 1
        and next(iter(versions), ()) >= MIN_COMPATIBLE_VERSION
        and False not in applied
    )


def rows_in_scope(rows: list[dict[str, Any]], scope_type: str, scope_key: str) -> bool:
    profile = profile_of(rows)
    if scope_type == "GROUP":
        return profile["category"] == scope_key
    if scope_type == "PROFILE":
        return profile["subtype"] == scope_key
    if scope_type == "PROFILE_SURFACE":
        return f'{profile["subtype"]}|{profile["track"]}' == scope_key
    return False


def load_candidates(registry: dict[str, Any]) -> list[dict[str, str]]:
    candidates = []
    for scope in registry.get("scopes") or []:
        scope_type = str(scope.get("scopeType") or "")
        if scope_type not in {"GROUP", "PROFILE", "PROFILE_SURFACE"}:
            continue
        scope_key = str(scope.get("scopeKey") or "")
        for metric in scope.get("metrics") or []:
            if metric.get("status") != "CANDIDATE_FOR_REPLAY":
                continue
            metric_name = str(metric.get("metric") or "")
            if metric_name in METRIC_KEYS:
                candidates.append({
                    "scopeType": scope_type,
                    "scopeKey": scope_key,
                    "metric": metric_name,
                })
    return candidates


def candidate_races(
    races: list[list[dict[str, Any]]],
    candidate: dict[str, str],
) -> list[list[dict[str, Any]]]:
    selected = []
    for rows in races:
        if classify_race(rows) != "fully_labeled":
            continue
        if not compatible_race(rows):
            continue
        if not rows_in_scope(rows, candidate["scopeType"], candidate["scopeKey"]):
            continue
        control = race_metric_outcome(rows, candidate["metric"])
        if control and control["replayTop3SetAgreement"]:
            selected.append(rows)
    return selected


def deduplicate_candidates(
    races: list[list[dict[str, Any]]],
    candidates: list[dict[str, str]],
) -> list[tuple[dict[str, str], list[list[dict[str, Any]]]]]:
    # Prefer the more specific scope if category and profile resolve to exactly
    # the same historical race set (for example GROUP:MAIDEN and PROFILE:MAIDEN).
    priority = {"PROFILE_SURFACE": 0, "PROFILE": 1, "GROUP": 2}
    prepared = [
        (candidate, candidate_races(races, candidate))
        for candidate in sorted(candidates, key=lambda item: priority[item["scopeType"]])
    ]
    seen: set[tuple[str, tuple[tuple[str, str], ...]]] = set()
    unique = []
    for candidate, selected in prepared:
        fingerprint = (
            candidate["metric"],
            tuple(sorted(race_key(rows[0]) for rows in selected)),
        )
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        unique.append((candidate, selected))
    return unique


def score_race(
    rows: list[dict[str, Any]],
    metric: str,
    *,
    candidate: bool,
) -> dict[int, float]:
    weights = active_weights(rows[0])
    return {
        id(row): score_with_weights(
            row,
            weights,
            added_metric=metric if candidate else None,
            added_points=PLUS_WEIGHT_POINTS if candidate else 0.0,
        )
        for row in rows
    }


def ordered_rows(rows: list[dict[str, Any]], scores: dict[int, float]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            -scores[id(row)],
            safe_int(row.get("rank_pred"), 999),
            str(row.get("horse_name") or ""),
        ),
    )


def official_rank(row: dict[str, Any], field_size: int) -> int:
    value = safe_int(row.get("finish_pos"), field_size)
    return field_size if value == 99 else max(1, min(field_size, value))


def pearson(values_a: list[float], values_b: list[float]) -> float | None:
    if len(values_a) < 2 or len(values_a) != len(values_b):
        return None
    mean_a = statistics.mean(values_a)
    mean_b = statistics.mean(values_b)
    numerator = sum((a - mean_a) * (b - mean_b) for a, b in zip(values_a, values_b))
    denominator_a = sum((a - mean_a) ** 2 for a in values_a)
    denominator_b = sum((b - mean_b) ** 2 for b in values_b)
    denominator = math.sqrt(denominator_a * denominator_b)
    return numerator / denominator if denominator > 0 else None


def separation_values(scores: list[float]) -> dict[str, float | str]:
    ordered = sorted(scores, reverse=True)
    score_std = statistics.pstdev(ordered) if len(ordered) >= 2 else 0.0
    gap = ordered[2] - ordered[3] if len(ordered) >= 4 else 0.0
    cutoff = ordered[2] if len(ordered) >= 3 else ordered[-1]
    crowd = sum(abs(value - cutoff) <= 2.0 for value in ordered)
    peak = max(ordered)
    exponentials = [math.exp((value - peak) / 18.0) for value in ordered]
    total = sum(exponentials) or 1.0
    probabilities = [value / total for value in exponentials]
    entropy = -sum(value * math.log(value) for value in probabilities if value > 0)
    normalized = entropy / math.log(len(ordered)) if len(ordered) >= 2 else 0.0
    if gap < 0.5 or crowd >= 7 or normalized >= 0.985:
        status = "RED"
    elif gap < 1.5 or crowd >= 5 or normalized >= 0.970:
        status = "YELLOW"
    else:
        status = "GREEN"
    return {
        "scoreStd": score_std,
        "top3Top4Gap": gap,
        "cutoffCrowd2pt": float(crowd),
        "normalizedEntropy": normalized,
        "status": status,
    }


def plackett_luce_top3_probabilities(scores: list[float], temperature: float) -> list[float]:
    if not scores:
        return []
    if len(scores) <= 3:
        return [1.0] * len(scores)
    peak = max(scores)
    weights = [math.exp((score - peak) / max(temperature, 0.1)) for score in scores]
    total = sum(weights)
    inclusion = [0.0] * len(scores)
    for first, first_weight in enumerate(weights):
        first_probability = first_weight / total
        inclusion[first] += first_probability
        remaining_after_first = total - first_weight
        for second, second_weight in enumerate(weights):
            if second == first:
                continue
            second_probability = first_probability * second_weight / remaining_after_first
            inclusion[second] += second_probability
            remaining_after_second = remaining_after_first - second_weight
            for third, third_weight in enumerate(weights):
                if third in (first, second):
                    continue
                inclusion[third] += second_probability * third_weight / remaining_after_second
    return [max(0.0, min(1.0, value)) for value in inclusion]


def calibration_rows(
    races: list[list[dict[str, Any]]],
    metric: str,
    *,
    candidate: bool,
    temperature: float,
) -> list[tuple[float, int]]:
    values = []
    for rows in races:
        scores = score_race(rows, metric, candidate=candidate)
        ordered = ordered_rows(rows, scores)
        probabilities = plackett_luce_top3_probabilities(
            [scores[id(row)] for row in ordered],
            temperature,
        )
        for row, probability in zip(ordered, probabilities):
            label = int(official_rank(row, len(rows)) <= 3)
            values.append((probability, label))
    return values


def calibration_metrics(values: list[tuple[float, int]], bins: int = 10) -> dict[str, float | int | None]:
    if not values:
        return {"rows": 0, "brier": None, "logLoss": None, "ece": None}
    clipped = [(max(1e-8, min(1.0 - 1e-8, probability)), label) for probability, label in values]
    brier = statistics.mean((probability - label) ** 2 for probability, label in clipped)
    log_loss = -statistics.mean(
        label * math.log(probability) + (1 - label) * math.log(1.0 - probability)
        for probability, label in clipped
    )
    bucketed: list[list[tuple[float, int]]] = [[] for _ in range(bins)]
    for probability, label in clipped:
        index = min(bins - 1, int(probability * bins))
        bucketed[index].append((probability, label))
    ece = sum(
        len(bucket) / len(clipped)
        * abs(statistics.mean(probability for probability, _ in bucket) - statistics.mean(label for _, label in bucket))
        for bucket in bucketed
        if bucket
    )
    return {
        "rows": len(clipped),
        "brier": rounded(brier, 5),
        "logLoss": rounded(log_loss, 5),
        "ece": rounded(ece, 5),
    }


def fit_temperature(
    races: list[list[dict[str, Any]]],
    metric: str,
    *,
    candidate: bool,
) -> float:
    if not races:
        return 18.0
    scored = []
    for temperature in TEMPERATURE_GRID:
        metrics = calibration_metrics(
            calibration_rows(
                races,
                metric,
                candidate=candidate,
                temperature=temperature,
            )
        )
        scored.append((safe_float(metrics["logLoss"], 999.0) or 999.0, temperature))
    return min(scored)[1]


def ranking_metrics(
    races: list[list[dict[str, Any]]],
    metric: str,
    *,
    candidate: bool,
    temperature: float,
) -> dict[str, Any]:
    winner_ranks = []
    maes = []
    correlations = []
    ndcg_values = []
    score_stds = []
    boundary_gaps = []
    crowds = []
    red_races = 0
    for rows in races:
        scores = score_race(rows, metric, candidate=candidate)
        ordered = ordered_rows(rows, scores)
        predicted = {id(row): index for index, row in enumerate(ordered, start=1)}
        race_winner = winner(rows)
        if race_winner is None:
            continue
        winner_ranks.append(predicted[id(race_winner)])
        official = [official_rank(row, len(rows)) for row in rows]
        predicted_values = [predicted[id(row)] for row in rows]
        maes.append(statistics.mean(abs(a - b) for a, b in zip(predicted_values, official)))
        correlation = pearson([float(value) for value in predicted_values], [float(value) for value in official])
        if correlation is not None:
            correlations.append(correlation)

        relevance = {
            id(row): max(0.0, (len(rows) - official_rank(row, len(rows)) + 1) / len(rows))
            for row in rows
        }

        def dcg(sequence: list[dict[str, Any]]) -> float:
            return sum(
                relevance[id(row)] / math.log2(index + 1)
                for index, row in enumerate(sequence[:5], start=1)
            )

        ideal = sorted(rows, key=lambda row: official_rank(row, len(rows)))
        ideal_dcg = dcg(ideal)
        if ideal_dcg > 0:
            ndcg_values.append(dcg(ordered) / ideal_dcg)

        separation = separation_values([scores[id(row)] for row in rows])
        score_stds.append(float(separation["scoreStd"]))
        boundary_gaps.append(float(separation["top3Top4Gap"]))
        crowds.append(float(separation["cutoffCrowd2pt"]))
        red_races += separation["status"] == "RED"

    calibration = calibration_metrics(
        calibration_rows(
            races,
            metric,
            candidate=candidate,
            temperature=temperature,
        )
    )
    count = len(winner_ranks)
    return {
        "races": count,
        "top1": sum(rank == 1 for rank in winner_ranks),
        "winnerTop3": sum(rank <= 3 for rank in winner_ranks),
        "winnerTop5": sum(rank <= 5 for rank in winner_ranks),
        "winnerTop3Rate": rounded(sum(rank <= 3 for rank in winner_ranks) / count, 4) if count else None,
        "avgWinnerRank": rounded(statistics.mean(winner_ranks), 3) if winner_ranks else None,
        "mae": rounded(statistics.mean(maes), 4) if maes else None,
        "rho": rounded(statistics.mean(correlations), 4) if correlations else None,
        "ndcg5": rounded(statistics.mean(ndcg_values), 4) if ndcg_values else None,
        "scoreStdMedian": rounded(statistics.median(score_stds), 3) if score_stds else None,
        "top3Top4GapMedian": rounded(statistics.median(boundary_gaps), 3) if boundary_gaps else None,
        "cutoffCrowd2ptMedian": rounded(statistics.median(crowds), 3) if crowds else None,
        "redRaceRate": rounded(red_races / count, 4) if count else None,
        "temperature": temperature,
        "calibration": calibration,
    }


def split_60_20_20(
    races: list[list[dict[str, Any]]],
) -> dict[str, list[list[dict[str, Any]]]]:
    count = len(races)
    build_end = max(1, math.floor(count * 0.60)) if count else 0
    inner_end = max(build_end + 1, math.floor(count * 0.80)) if count > build_end else build_end
    inner_end = min(inner_end, count)
    return {
        "build": races[:build_end],
        "inner": races[build_end:inner_end],
        "outer": races[inner_end:],
    }


def walk_forward_windows(
    races: list[list[dict[str, Any]]],
) -> list[list[list[dict[str, Any]]]]:
    if len(races) < 5:
        return [races] if races else []
    start = max(1, math.floor(len(races) * 0.40))
    validation = races[start:]
    base_size, remainder = divmod(len(validation), 3)
    windows = []
    cursor = 0
    for index in range(3):
        size = base_size + (1 if index < remainder else 0)
        if size:
            windows.append(validation[cursor:cursor + size])
        cursor += size
    return windows


def metric_delta(candidate: dict[str, Any], baseline: dict[str, Any], key: str) -> float | int | None:
    candidate_value = safe_float(candidate.get(key))
    baseline_value = safe_float(baseline.get(key))
    if candidate_value is None or baseline_value is None:
        return None
    difference = candidate_value - baseline_value
    return int(difference) if key in {"top1", "winnerTop3", "winnerTop5"} else rounded(difference, 5)


def compare_metrics(baseline: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    delta = {
        key: metric_delta(candidate, baseline, key)
        for key in (
            "top1", "winnerTop3", "winnerTop5", "avgWinnerRank", "mae",
            "rho", "ndcg5", "scoreStdMedian", "top3Top4GapMedian",
            "cutoffCrowd2ptMedian", "redRaceRate",
        )
    }
    delta["brier"] = metric_delta(candidate["calibration"], baseline["calibration"], "brier")
    delta["logLoss"] = metric_delta(candidate["calibration"], baseline["calibration"], "logLoss")
    delta["ece"] = metric_delta(candidate["calibration"], baseline["calibration"], "ece")
    return {"baseline": baseline, "candidate": candidate, "delta": delta}


def gate_result(
    full: dict[str, Any],
    inner: dict[str, Any],
    outer: dict[str, Any],
    walk_forward: list[dict[str, Any]],
    *,
    scope_type: str,
) -> tuple[str, list[dict[str, Any]]]:
    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    live_races = 60 if scope_type == "GROUP" else 30
    outer_races = 12 if scope_type == "GROUP" else 6
    check(
        "compatible_evidence_threshold",
        full["baseline"]["races"] >= live_races,
        f'races={full["baseline"]["races"]}/{live_races}',
    )
    check(
        "outer_evidence_threshold",
        outer["baseline"]["races"] >= outer_races,
        f'races={outer["baseline"]["races"]}/{outer_races}',
    )
    check(
        "full_winner_top3_plus_2",
        (full["delta"]["winnerTop3"] or 0) >= 2,
        f'delta={full["delta"]["winnerTop3"]}',
    )
    check(
        "inner_winner_top3_no_regression",
        (inner["delta"]["winnerTop3"] or 0) >= 0,
        f'delta={inner["delta"]["winnerTop3"]}',
    )
    check(
        "outer_winner_top3_plus_1",
        (outer["delta"]["winnerTop3"] or 0) >= 1,
        f'delta={outer["delta"]["winnerTop3"]}',
    )
    check(
        "outer_top1_loss_at_most_1",
        (outer["delta"]["top1"] or 0) >= -1,
        f'delta={outer["delta"]["top1"]}',
    )
    check(
        "outer_top5_no_regression",
        (outer["delta"]["winnerTop5"] or 0) >= 0,
        f'delta={outer["delta"]["winnerTop5"]}',
    )
    check(
        "outer_ndcg_no_material_regression",
        (outer["delta"]["ndcg5"] or 0.0) >= -0.002,
        f'delta={outer["delta"]["ndcg5"]}',
    )
    check(
        "outer_brier_no_material_regression",
        (outer["delta"]["brier"] or 0.0) <= 0.005,
        f'delta={outer["delta"]["brier"]}',
    )
    candidate_ece = safe_float(outer["candidate"]["calibration"].get("ece"))
    delta_ece = safe_float(outer["delta"].get("ece"))
    check(
        "outer_ece_guard",
        candidate_ece is not None
        and delta_ece is not None
        and candidate_ece <= 0.10
        and (candidate_ece <= 0.05 or delta_ece <= 0.01),
        (
            f'candidate={outer["candidate"]["calibration"]["ece"]}, '
            f'delta={outer["delta"]["ece"]}'
        ),
    )
    baseline_gap = safe_float(outer["baseline"].get("top3Top4GapMedian"), 0.0) or 0.0
    candidate_gap = safe_float(outer["candidate"].get("top3Top4GapMedian"), 0.0) or 0.0
    check(
        "outer_boundary_margin_not_narrowed_10pct",
        baseline_gap <= 0.0 or candidate_gap >= baseline_gap * 0.90,
        f"baseline={baseline_gap}, candidate={candidate_gap}",
    )
    check(
        "outer_red_race_rate_not_worse_5pp",
        (outer["delta"]["redRaceRate"] or 0.0) <= 0.05,
        f'delta={outer["delta"]["redRaceRate"]}',
    )
    walk_deltas = [window["delta"]["winnerTop3"] or 0 for window in walk_forward]
    check(
        "walk_forward_winner_top3_no_regression",
        bool(walk_deltas) and all(delta >= 0 for delta in walk_deltas),
        f"deltas={walk_deltas}",
    )
    status = "SUPPORTED_FOR_PROSPECTIVE_SHADOW" if all(item["passed"] for item in checks) else "HOLD"
    return status, checks


def evaluate_candidate(
    candidate_definition: dict[str, str],
    races: list[list[dict[str, Any]]],
) -> dict[str, Any]:
    split = split_60_20_20(races)
    metric = candidate_definition["metric"]
    baseline_temperature = fit_temperature(split["build"], metric, candidate=False)
    candidate_temperature = fit_temperature(split["build"], metric, candidate=True)

    comparisons = {}
    for key, subset in {"full": races, **split}.items():
        comparisons[key] = compare_metrics(
            ranking_metrics(
                subset,
                metric,
                candidate=False,
                temperature=baseline_temperature,
            ),
            ranking_metrics(
                subset,
                metric,
                candidate=True,
                temperature=candidate_temperature,
            ),
        )

    walk_forward = []
    for index, window in enumerate(walk_forward_windows(races), start=1):
        comparison = compare_metrics(
            ranking_metrics(window, metric, candidate=False, temperature=baseline_temperature),
            ranking_metrics(window, metric, candidate=True, temperature=candidate_temperature),
        )
        comparison["window"] = index
        walk_forward.append(comparison)

    status, checks = gate_result(
        comparisons["full"],
        comparisons["inner"],
        comparisons["outer"],
        walk_forward,
        scope_type=candidate_definition["scopeType"],
    )
    versions = Counter(
        str(rows[0].get("v4_version") or "unknown")
        for rows in races
    )
    return {
        "candidateId": (
            f'{candidate_definition["scopeType"]}:{candidate_definition["scopeKey"]}'
            f'/{metric}/plus{PLUS_WEIGHT_POINTS:g}pp'
        ),
        "scopeType": candidate_definition["scopeType"],
        "scopeKey": candidate_definition["scopeKey"],
        "metric": metric,
        "addedWeightPoints": PLUS_WEIGHT_POINTS,
        "compatibleMinimumVersion": ".".join(str(value) for value in MIN_COMPATIBLE_VERSION),
        "rankingVersions": dict(sorted(versions.items())),
        "races": len(races),
        "splitRaces": {key: len(value) for key, value in split.items()},
        "temperatureFit": {
            "fitSplit": "build",
            "baseline": baseline_temperature,
            "candidate": candidate_temperature,
        },
        "comparisons": comparisons,
        "walkForward": walk_forward,
        "status": status,
        "checks": checks,
        "liveRolloutEligible": False,
        "outerIsUntouched": False,
        "reason": (
            "Historical registry discovery overlaps this replay; passing only permits a frozen prospective shadow."
        ),
    }


def build_report(
    entries: list[dict[str, Any]],
    registry: dict[str, Any],
    run_date: str,
    *,
    invalid_json_lines: int = 0,
) -> dict[str, Any]:
    races = group_races(entries)
    raw_candidates = load_candidates(registry)
    prepared = deduplicate_candidates(races, raw_candidates)
    evaluated = [evaluate_candidate(candidate, selected) for candidate, selected in prepared if selected]
    source_timestamp = max((safe_int(row.get("ts"), 0) for row in entries), default=0)
    status_counts = Counter(item["status"] for item in evaluated)
    return {
        "schemaVersion": "metric-signal-replay-v1",
        "runDate": run_date,
        "sourceSnapshotAt": (
            datetime.fromtimestamp(source_timestamp, tz=timezone.utc).isoformat(timespec="seconds")
            if source_timestamp > 0 else None
        ),
        "registryRunDate": registry.get("runDate"),
        "registrySchemaVersion": registry.get("schemaVersion"),
        "input": {
            "validJsonRows": len(entries),
            "invalidJsonLines": invalid_json_lines,
            "rawRegistryCandidates": len(raw_candidates),
            "deduplicatedCandidates": len(prepared),
            "evaluatedCandidates": len(evaluated),
            "minimumCompatibleVersion": ".".join(str(value) for value in MIN_COMPATIBLE_VERSION),
        },
        "statusCounts": dict(sorted(status_counts.items())),
        "candidates": evaluated,
        "policy": {
            "liveWeightChanged": False,
            "liveRolloutEligible": False,
            "outerIsUntouched": False,
            "winnerTop3HardGate": True,
            "calibrationTemperatureFitOnBuildOnly": True,
            "passingMeaning": "prospective_shadow_only",
        },
    }


def signed(value: Any, digits: int = 4) -> str:
    numeric = safe_float(value)
    if numeric is None:
        return "-"
    if float(numeric).is_integer():
        return f"{int(numeric):+d}"
    return f"{numeric:+.{digits}f}"


def render_markdown(report: dict[str, Any]) -> str:
    input_summary = report["input"]
    lines = [
        f'# Metric Signal Full Replay — {report["runDate"]}',
        "",
        "Bu rapor canlı ağırlıkları değiştirmez. Geçen aday yalnız prospective shadow hakkı kazanır.",
        "",
        "## Kapsam",
        "",
        f'- Registry adayı: `{input_summary["rawRegistryCandidates"]}`; tekrar temizliği sonrası `{input_summary["deduplicatedCandidates"]}`.',
        f'- Değerlendirilen aday: `{input_summary["evaluatedCandidates"]}`; minimum uyumlu sürüm `v{input_summary["minimumCompatibleVersion"]}`.',
        "- Registry keşfi ile replay geçmişi çakıştığı için outer pencere promotion için untouched sayılmaz.",
        "",
        "## Sonuç",
        "",
        "| Aday | Yarış | Full WTop3 | Inner | Outer | Outer Top1 | NDCG | Brier | ECE | Top3–4 | Durum |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for item in report["candidates"]:
        full = item["comparisons"]["full"]["delta"]
        inner = item["comparisons"]["inner"]["delta"]
        outer = item["comparisons"]["outer"]["delta"]
        lines.append(
            "| "
            + " | ".join([
                f'`{item["scopeType"]}:{item["scopeKey"]}/{item["metric"]}`',
                str(item["races"]),
                signed(full["winnerTop3"]),
                signed(inner["winnerTop3"]),
                signed(outer["winnerTop3"]),
                signed(outer["top1"]),
                signed(outer["ndcg5"]),
                signed(outer["brier"]),
                signed(outer["ece"]),
                signed(outer["top3Top4GapMedian"]),
                item["status"],
            ])
            + " |"
        )

    for item in report["candidates"]:
        lines.extend([
            "",
            f'### {item["candidateId"]}',
            "",
            (
                f'- Yarış/split: `{item["races"]}` — build `{item["splitRaces"]["build"]}`, '
                f'inner `{item["splitRaces"]["inner"]}`, outer `{item["splitRaces"]["outer"]}`.'
            ),
            (
                f'- Kalibrasyon sıcaklığı: baseline `{item["temperatureFit"]["baseline"]}`, '
                f'candidate `{item["temperatureFit"]["candidate"]}`; yalnız build üzerinde fit edildi.'
            ),
            f'- Walk-forward WinnerTop3 farkları: `{[window["delta"]["winnerTop3"] for window in item["walkForward"]]}`.',
            "- Kapılar:",
        ])
        for check in item["checks"]:
            marker = "PASS" if check["passed"] else "FAIL"
            lines.append(f'  - `{marker}` {check["name"]}: {check["detail"]}')

    lines.extend([
        "",
        "## Karar politikası",
        "",
        "- Winner Top3 gerilemesini başka metrik ortalaması örtemez.",
        "- Kalibrasyon build dışında fit edilmez.",
        "- Skor ayrışması daralırsa aday hold edilir.",
        "- Bu replay geçse bile canlıya çıkış için yeni temiz prospective shadow gerekir.",
        "",
    ])
    return "\n".join(lines)


def atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        handle.write(content)
        temporary = Path(handle.name)
    temporary.replace(path)


def persist(report: dict[str, Any], data_dir: Path) -> dict[str, str]:
    daily_dir = data_dir / "automation" / "runs" / report["runDate"]
    latest_dir = data_dir / "automation" / "metric-replay"
    json_text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    markdown = render_markdown(report)
    paths = {
        "dailyJson": daily_dir / "metric-signal-replay.json",
        "dailyMarkdown": daily_dir / "metric-signal-replay.md",
        "latestJson": latest_dir / "latest.json",
        "latestMarkdown": latest_dir / "latest.md",
    }
    for key, path in paths.items():
        atomic_write(path, json_text if key.endswith("Json") else markdown)
    return {key: str(path) for key, path in paths.items()}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run full replay for registry candidates.")
    parser.add_argument("--predictions", required=True, type=Path)
    parser.add_argument("--registry", required=True, type=Path)
    parser.add_argument("--data-dir", required=True, type=Path)
    parser.add_argument("--run-date", default=datetime.now().astimezone().strftime("%Y-%m-%d"))
    arguments = parser.parse_args()

    entries, invalid_lines = load_jsonl(arguments.predictions)
    registry = json.loads(arguments.registry.read_text(encoding="utf-8"))
    report = build_report(
        entries,
        registry,
        arguments.run_date,
        invalid_json_lines=invalid_lines,
    )
    paths = persist(report, arguments.data_dir)
    print(json.dumps({
        "success": True,
        "runDate": report["runDate"],
        "statusCounts": report["statusCounts"],
        "candidates": [
            {"candidateId": item["candidateId"], "status": item["status"], "races": item["races"]}
            for item in report["candidates"]
        ],
        "paths": paths,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
