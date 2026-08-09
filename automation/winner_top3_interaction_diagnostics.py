"""Diagnose bounded pairwise Winner-Top3 interactions without changing ranking.

The producer deliberately separates discovery from validation.  Metric pairs
are ordered only by the chronological build slice; inner and outer results are
then used as promotion gates.  The artifact is diagnostic-only and never
changes live weights, scores, ranks, or Telegram ordering.
"""

from __future__ import annotations

import argparse
import itertools
import json
import statistics
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from automation.confidence_calibration import (
        calibration_compatible,
        split_60_20_20,
        weight_fingerprint,
    )
    from automation.metric_signal_registry import (
        METRIC_KEYS,
        NON_NEUTRAL_EPSILON,
        active_weights,
        feature_value,
        group_races,
        load_jsonl,
        metric_source_proven,
        profile_of,
        rank_of_row,
        rounded,
        safe_int,
        score_diagnostics,
        score_with_weights,
        winner,
    )
    from automation.winner_top3_failure_diagnostics import (
        summarize_performance,
        visible_winner_rank,
    )
except ModuleNotFoundError:  # direct: python automation/winner_top3_interaction_diagnostics.py
    from confidence_calibration import (
        calibration_compatible,
        split_60_20_20,
        weight_fingerprint,
    )
    from metric_signal_registry import (
        METRIC_KEYS,
        NON_NEUTRAL_EPSILON,
        active_weights,
        feature_value,
        group_races,
        load_jsonl,
        metric_source_proven,
        profile_of,
        rank_of_row,
        rounded,
        safe_int,
        score_diagnostics,
        score_with_weights,
        winner,
    )
    from winner_top3_failure_diagnostics import summarize_performance, visible_winner_rank


SCHEMA_VERSION = "winner-top3-interaction-diagnostics-v1"
DEFAULT_PROFILES = ("SART1", "HANDIKAP16")
EXCLUDED_METRICS = frozenset({"agf_score"})
BOUNDED_WEIGHT_POINTS = 2.0
TOTAL_VARIATION_POINTS = 4.0
PROFILE_MIN_RACES = 30
PROFILE_MIN_OUTER = 6
MIN_COVERAGE = 0.40
MIN_NON_NEUTRAL = 0.15
DISCOVERY_SHORTLIST = 25
OUTPUT_CANDIDATES = 15


def score_with_deltas(
    row: dict[str, Any],
    weights: dict[str, float],
    deltas: dict[str, float],
) -> float:
    candidate_weights = dict(weights)
    for metric, delta in deltas.items():
        candidate_weights[metric] = max(0.0, candidate_weights.get(metric, 0.0) + delta)
    return score_with_weights(row, candidate_weights)


def score_separation(scores: dict[int, float], rows: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(
        rows,
        key=lambda row: (
            -scores[id(row)],
            safe_int(row.get("rank_pred")),
            str(row.get("horse_name") or ""),
        ),
    )
    values = [scores[id(row)] for row in ordered]
    if len(values) < 4:
        return {"top3Top4Gap": None, "cutoffCrowd2pt": len(values)}
    cutoff = values[2]
    return {
        "top3Top4Gap": max(0.0, cutoff - values[3]),
        "cutoffCrowd2pt": sum(abs(value - cutoff) <= 2.0 for value in values),
    }


def _median(values: list[float]) -> float | None:
    return rounded(statistics.median(values), 4) if values else None


def metric_inventory(
    races: list[list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[str]]:
    total_rows = sum(len(rows) for rows in races)
    items = []
    for metric in METRIC_KEYS:
        if metric in EXCLUDED_METRICS:
            continue
        source_values = [
            float(value)
            for rows in races
            for row in rows
            if metric_source_proven(row, metric)
            and (value := feature_value(row, metric)) is not None
        ]
        weights = [active_weights(rows[0]).get(metric, 0.0) for rows in races]
        coverage = len(source_values) / total_rows if total_rows else 0.0
        non_neutral = (
            sum(abs(value - 50.0) >= NON_NEUTRAL_EPSILON for value in source_values)
            / total_rows
            if total_rows else 0.0
        )
        median_weight = statistics.median(weights) if weights else 0.0
        eligible = coverage >= MIN_COVERAGE and non_neutral >= MIN_NON_NEUTRAL
        items.append({
            "metric": metric,
            "coverage": rounded(coverage, 4),
            "nonNeutralRate": rounded(non_neutral, 4),
            "currentWeightPctMedian": rounded(median_weight, 3),
            "active": median_weight > 0.0,
            "eligible": eligible,
        })
    items.sort(
        key=lambda item: (
            item["eligible"],
            item["active"],
            item["nonNeutralRate"],
            item["coverage"],
            item["metric"],
        ),
        reverse=True,
    )
    return items, sorted(item["metric"] for item in items if item["eligible"])


def baseline_race_event(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    race_winner = winner(rows)
    if race_winner is None:
        return None
    weights = active_weights(rows[0])
    scores = {id(row): score_with_weights(row, weights) for row in rows}
    ordered = sorted(
        rows,
        key=lambda row: (-scores[id(row)], safe_int(row.get("rank_pred"))),
    )
    replay_top3 = {id(row) for row in ordered[:3]}
    visible_top3 = {
        id(row)
        for row in sorted(rows, key=lambda row: safe_int(row.get("rank_pred")))[:3]
    }
    separation = score_separation(scores, rows)
    first = rows[0]
    return {
        "rows": rows,
        "winner": race_winner,
        "weights": weights,
        "baselineScores": scores,
        "replayAgreement": replay_top3 == visible_top3,
        "baselineHit": rank_of_row(rows, race_winner, scores) <= 3,
        "baselineGap": separation["top3Top4Gap"],
        "baselineCrowd": separation["cutoffCrowd2pt"],
        "raceKey": f'{first.get("race_date")}|{first.get("race_id")}',
    }


def pair_directions(
    first_metric: str,
    second_metric: str,
    inventory_by_metric: dict[str, dict[str, Any]],
) -> list[dict[str, float]]:
    first_options = [BOUNDED_WEIGHT_POINTS]
    second_options = [BOUNDED_WEIGHT_POINTS]
    if inventory_by_metric[first_metric]["active"]:
        first_options.append(-BOUNDED_WEIGHT_POINTS)
    if inventory_by_metric[second_metric]["active"]:
        second_options.append(-BOUNDED_WEIGHT_POINTS)
    return [
        {first_metric: first_delta, second_metric: second_delta}
        for first_delta in first_options
        for second_delta in second_options
    ]


def candidate_event(
    baseline: dict[str, Any],
    deltas: dict[str, float],
) -> dict[str, Any]:
    rows = baseline["rows"]
    scores = {
        id(row): score_with_deltas(row, baseline["weights"], deltas)
        for row in rows
    }
    separation = score_separation(scores, rows)
    return {
        "raceKey": baseline["raceKey"],
        "baseline": baseline["baselineHit"],
        "candidate": rank_of_row(rows, baseline["winner"], scores) <= 3,
        "baselineGap": baseline["baselineGap"],
        "candidateGap": separation["top3Top4Gap"],
        "baselineCrowd": baseline["baselineCrowd"],
        "candidateCrowd": separation["cutoffCrowd2pt"],
    }


def slice_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    rescues = sum(not item["baseline"] and item["candidate"] for item in events)
    damages = sum(item["baseline"] and not item["candidate"] for item in events)
    baseline_gaps = [
        float(item["baselineGap"])
        for item in events
        if item.get("baselineGap") is not None
    ]
    candidate_gaps = [
        float(item["candidateGap"])
        for item in events
        if item.get("candidateGap") is not None
    ]
    baseline_crowds = [float(item["baselineCrowd"]) for item in events]
    candidate_crowds = [float(item["candidateCrowd"]) for item in events]
    return {
        "races": len(events),
        "baselineWinnerTop3": sum(bool(item["baseline"]) for item in events),
        "candidateWinnerTop3": sum(bool(item["candidate"]) for item in events),
        "rescues": rescues,
        "damages": damages,
        "netHits": rescues - damages,
        "baselineBoundaryGapMedian": _median(baseline_gaps),
        "candidateBoundaryGapMedian": _median(candidate_gaps),
        "baselineCutoffCrowdMedian": _median(baseline_crowds),
        "candidateCutoffCrowdMedian": _median(candidate_crowds),
    }


def boundary_not_compressed(summary: dict[str, Any]) -> bool:
    baseline = summary.get("baselineBoundaryGapMedian")
    candidate = summary.get("candidateBoundaryGapMedian")
    if baseline is None or candidate is None:
        return False
    return candidate + 1e-9 >= baseline * 0.90


def crowd_not_worse(summary: dict[str, Any]) -> bool:
    baseline = summary.get("baselineCutoffCrowdMedian")
    candidate = summary.get("candidateCutoffCrowdMedian")
    if baseline is None or candidate is None:
        return False
    return candidate <= baseline + 1.0


def _delta_label(deltas: dict[str, float]) -> str:
    return ",".join(
        f'{metric}:{delta:+.0f}' for metric, delta in sorted(deltas.items())
    )


def summarize_candidate(
    deltas: dict[str, float],
    events: list[dict[str, Any]],
    metric_stats: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    slices = split_60_20_20(events)
    summaries = {
        "build": slice_summary(slices["build"]),
        "inner": slice_summary(slices["inner"]),
        "outer": slice_summary(slices["outer"]),
        "full": slice_summary(events),
        "recent20": slice_summary(events[-20:]),
    }
    minimum_coverage = min(metric_stats[metric]["coverage"] for metric in deltas)
    minimum_non_neutral = min(metric_stats[metric]["nonNeutralRate"] for metric in deltas)
    checks = [
        {"name": "evidence_threshold", "passed": len(events) >= PROFILE_MIN_RACES,
         "detail": f"races={len(events)}/{PROFILE_MIN_RACES}"},
        {"name": "outer_threshold", "passed": summaries["outer"]["races"] >= PROFILE_MIN_OUTER,
         "detail": f'races={summaries["outer"]["races"]}/{PROFILE_MIN_OUTER}'},
        {"name": "metric_coverage", "passed": minimum_coverage >= MIN_COVERAGE,
         "detail": f"minimum={minimum_coverage}/{MIN_COVERAGE}"},
        {"name": "metric_non_neutral", "passed": minimum_non_neutral >= MIN_NON_NEUTRAL,
         "detail": f"minimum={minimum_non_neutral}/{MIN_NON_NEUTRAL}"},
        {"name": "build_discovery", "passed": summaries["build"]["netHits"] >= 1,
         "detail": f'net={summaries["build"]["netHits"]}'},
        {"name": "inner_no_regression", "passed": summaries["inner"]["netHits"] >= 0,
         "detail": f'net={summaries["inner"]["netHits"]}'},
        {"name": "inner_no_damage", "passed": summaries["inner"]["damages"] == 0,
         "detail": f'damages={summaries["inner"]["damages"]}'},
        {"name": "outer_confirmed_gain", "passed": summaries["outer"]["netHits"] >= 1,
         "detail": f'net={summaries["outer"]["netHits"]}'},
        {"name": "outer_no_damage", "passed": summaries["outer"]["damages"] == 0,
         "detail": f'damages={summaries["outer"]["damages"]}'},
        {"name": "full_net_plus_2", "passed": summaries["full"]["netHits"] >= 2,
         "detail": f'net={summaries["full"]["netHits"]}'},
        {"name": "recent20_plus_1", "passed": summaries["recent20"]["netHits"] >= 1,
         "detail": f'net={summaries["recent20"]["netHits"]}'},
        {"name": "outer_boundary_not_compressed", "passed": boundary_not_compressed(summaries["outer"]),
         "detail": f'baseline={summaries["outer"]["baselineBoundaryGapMedian"]},candidate={summaries["outer"]["candidateBoundaryGapMedian"]}'},
        {"name": "outer_crowd_not_worse", "passed": crowd_not_worse(summaries["outer"]),
         "detail": f'baseline={summaries["outer"]["baselineCutoffCrowdMedian"]},candidate={summaries["outer"]["candidateCutoffCrowdMedian"]}'},
    ]
    sample_ready = all(item["passed"] for item in checks[:2])
    data_ready = all(item["passed"] for item in checks[2:4])
    build_discovered = checks[4]["passed"]
    holdout_harm = (
        summaries["inner"]["netHits"] < 0
        or summaries["inner"]["damages"] > 0
        or summaries["outer"]["netHits"] < 0
        or summaries["outer"]["damages"] > 0
        or not checks[-2]["passed"]
        or not checks[-1]["passed"]
    )
    if all(item["passed"] for item in checks):
        status = "REPLAY_PRIORITY"
    elif not sample_ready:
        status = "COLLECTING"
    elif not data_ready:
        status = "DATA_LOW"
    elif build_discovered and holdout_harm:
        status = "HARM_RISK"
    elif build_discovered:
        status = "DOES_NOT_GENERALIZE"
    else:
        status = "NO_INTERACTION_SIGNAL"
    return {
        "candidateId": _delta_label(deltas),
        "deltas": dict(sorted(deltas.items())),
        "totalVariationPoints": sum(abs(delta) for delta in deltas.values()),
        "minimumCoverage": minimum_coverage,
        "minimumNonNeutralRate": minimum_non_neutral,
        "slices": summaries,
        "checks": checks,
        "status": status,
    }


def _discovery_key(candidate: dict[str, Any]) -> tuple[int, int, int, str]:
    build = candidate["slices"]["build"]
    return (
        -build["netHits"],
        build["damages"],
        -build["rescues"],
        candidate["candidateId"],
    )


def _output_key(candidate: dict[str, Any]) -> tuple[int, int, int, int, int, str]:
    status_order = {
        "REPLAY_PRIORITY": 0,
        "DOES_NOT_GENERALIZE": 1,
        "HARM_RISK": 2,
        "COLLECTING": 3,
        "DATA_LOW": 4,
        "NO_INTERACTION_SIGNAL": 5,
    }
    slices = candidate["slices"]
    return (
        status_order.get(candidate["status"], 9),
        -slices["outer"]["netHits"],
        -slices["inner"]["netHits"],
        -slices["full"]["netHits"],
        slices["full"]["damages"],
        candidate["candidateId"],
    )


def race_identity(rows: list[dict[str, Any]]) -> dict[str, Any]:
    first = rows[0]
    race_winner = winner(rows) or {}
    return {
        "raceDate": first.get("race_date"),
        "raceId": first.get("race_id"),
        "city": first.get("city"),
        "raceNo": first.get("race_no"),
        "track": profile_of(rows)["track"],
        "fieldSize": len(rows),
        "winner": race_winner.get("horse_name"),
        "winnerRank": visible_winner_rank(rows),
    }


def build_profile_report(
    profile: str,
    all_profile_races: list[list[dict[str, Any]]],
) -> dict[str, Any]:
    if not all_profile_races:
        return {
            "profile": profile,
            "status": "NO_DATA",
            "compatibleProfileRaces": 0,
            "latestLabeledFingerprintRaces": 0,
            "replayPriorities": [],
            "candidates": [],
        }
    fingerprint_buckets: dict[str, list[list[dict[str, Any]]]] = defaultdict(list)
    for rows in all_profile_races:
        fingerprint = weight_fingerprint(rows[0])
        if fingerprint:
            fingerprint_buckets[fingerprint].append(rows)
    current_fingerprint, current_races = max(
        fingerprint_buckets.items(),
        key=lambda item: max(safe_int(row.get("ts"), 0) for rows in item[1] for row in rows),
    )
    baseline_events = [
        event
        for rows in current_races
        if (event := baseline_race_event(rows)) is not None
    ]
    evidence = [event for event in baseline_events if event["replayAgreement"]]
    inventory, eligible_metrics = metric_inventory(current_races)
    inventory_by_metric = {item["metric"]: item for item in inventory}
    all_candidates = []
    optimistic_rescues: set[str] = set()
    baseline_miss_keys = {
        item["raceKey"] for item in evidence if not item["baselineHit"]
    }
    for first_metric, second_metric in itertools.combinations(eligible_metrics, 2):
        for deltas in pair_directions(first_metric, second_metric, inventory_by_metric):
            events = [candidate_event(item, deltas) for item in evidence]
            optimistic_rescues.update(
                item["raceKey"]
                for item in events
                if not item["baseline"] and item["candidate"]
            )
            all_candidates.append(
                summarize_candidate(deltas, events, inventory_by_metric)
            )
    discovery_ranked = sorted(all_candidates, key=_discovery_key)
    discovered = [
        item for item in discovery_ranked
        if item["slices"]["build"]["netHits"] >= 1
    ][:DISCOVERY_SHORTLIST]
    shortlist = discovered or discovery_ranked[:min(10, DISCOVERY_SHORTLIST)]
    shortlist.sort(key=_output_key)
    replay_priorities = [
        {
            "candidateId": item["candidateId"],
            "deltas": item["deltas"],
            "slices": item["slices"],
        }
        for item in shortlist
        if item["status"] == "REPLAY_PRIORITY"
    ]
    diagnostics = [score_diagnostics(rows, bootstrap_iterations=0) or {} for rows in current_races]
    weighted_coverage = [
        float(item["weightedRealCoverage"])
        for item in diagnostics
        if item.get("weightedRealCoverage") is not None
    ]
    informative_share = [
        float(item["informativeWeightShare"])
        for item in diagnostics
        if item.get("informativeWeightShare") is not None
    ]
    separation_counts: dict[str, int] = defaultdict(int)
    for item in diagnostics:
        separation_counts[str(item.get("separationStatus") or "UNKNOWN")] += 1
    unrescued_keys = baseline_miss_keys - optimistic_rescues
    status = "REPLAY_PRIORITY" if replay_priorities else (
        "COLLECTING" if len(evidence) < PROFILE_MIN_RACES else "NO_GENERALIZING_PAIR"
    )
    return {
        "profile": profile,
        "status": status,
        "compatibleProfileRaces": len(all_profile_races),
        "historicalFingerprintCount": len(fingerprint_buckets),
        "latestLabeledWeightFingerprint": current_fingerprint,
        "latestLabeledFingerprintRaces": len(current_races),
        "latestLabeledRankingVersions": dict(sorted(Counter(
            str(rows[0].get("v4_version")) for rows in current_races
        ).items())),
        "performance": summarize_performance(current_races),
        "recent20": summarize_performance(current_races[-20:]),
        "replayEvidenceRaces": len(evidence),
        "excludedReplayMismatchRaces": len(baseline_events) - len(evidence),
        "metricInventory": inventory,
        "eligibleMetrics": eligible_metrics,
        "eligibleMetricCount": len(eligible_metrics),
        "pairDirectionUniverseCount": len(all_candidates),
        "discoveryShortlistCount": len(shortlist),
        "scoreQuality": {
            "weightedRealCoverageMedian": _median(weighted_coverage),
            "informativeWeightShareMedian": _median(informative_share),
            "separationStatusCounts": dict(sorted(separation_counts.items())),
        },
        "optimisticRescueCeiling": {
            "baselineMisses": len(baseline_miss_keys),
            "rescuedByAnyPair": len(optimistic_rescues & baseline_miss_keys),
            "unrescuedByAnyPair": len(unrescued_keys),
            "interpretation": "upper_bound_only_not_promotion_evidence",
        },
        "unrescuedMisses": [
            race_identity(item["rows"])
            for item in evidence
            if item["raceKey"] in unrescued_keys
        ][-20:],
        "candidates": shortlist[:OUTPUT_CANDIDATES],
        "replayPriorities": replay_priorities,
    }


def build_report(
    entries: list[dict[str, Any]],
    run_date: str,
    profiles: tuple[str, ...] = DEFAULT_PROFILES,
    *,
    invalid_json_lines: int = 0,
) -> dict[str, Any]:
    all_races = group_races(entries)
    compatible = [rows for rows in all_races if calibration_compatible(rows)]
    buckets: dict[str, list[list[dict[str, Any]]]] = defaultdict(list)
    for rows in compatible:
        subtype = profile_of(rows)["subtype"]
        if subtype in profiles:
            buckets[subtype].append(rows)
    profile_reports = [build_profile_report(profile, buckets.get(profile, [])) for profile in profiles]
    source_timestamp = max((safe_int(row.get("ts"), 0) for row in entries), default=0)
    return {
        "schemaVersion": SCHEMA_VERSION,
        "runDate": run_date,
        "sourceSnapshotAt": (
            datetime.fromtimestamp(source_timestamp, tz=timezone.utc).isoformat(timespec="seconds")
            if source_timestamp > 0 else None
        ),
        "input": {
            "validJsonRows": len(entries),
            "invalidJsonLines": invalid_json_lines,
            "allRaces": len(all_races),
            "compatibleCleanRaces": len(compatible),
            "targetProfiles": list(profiles),
        },
        "profiles": profile_reports,
        "replayPriorityCount": sum(len(item["replayPriorities"]) for item in profile_reports),
        "policy": {
            "discoverySlice": "chronological_first_60_percent",
            "innerSlice": "chronological_next_20_percent",
            "outerSlice": "chronological_latest_20_percent",
            "boundedWeightPointsPerMetric": BOUNDED_WEIGHT_POINTS,
            "maximumTotalVariationPoints": TOTAL_VARIATION_POINTS,
            "profileMinimumEvidenceRaces": PROFILE_MIN_RACES,
            "profileMinimumOuterRaces": PROFILE_MIN_OUTER,
            "minimumCoverage": MIN_COVERAGE,
            "minimumNonNeutral": MIN_NON_NEUTRAL,
            "outerWinnerTop3GainRequired": 1,
            "outerDamageAllowed": 0,
            "outerBoundaryGapMinimumRatio": 0.90,
            "outerCutoffCrowdMaximumIncrease": 1,
            "excludedMetrics": sorted(EXCLUDED_METRICS),
            "automaticWeightChange": False,
            "rankingImpact": False,
            "priorityMeaning": "full_replay_required_before_prospective_shadow",
        },
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Winner Top3 Interaction Diagnostics",
        "",
        f'- Run date: `{report["runDate"]}`',
        f'- Compatible clean races: `{report["input"]["compatibleCleanRaces"]}`',
        f'- Replay priorities: `{report["replayPriorityCount"]}`',
        "- Ranking impact: `false`",
        "",
    ]
    for profile in report["profiles"]:
        lines.extend([
            f'## {profile["profile"]}',
            "",
            f'- Status: `{profile["status"]}`.',
            f'- Latest labeled fingerprint evidence: `{profile["latestLabeledFingerprintRaces"]}`; '
            f'compatible profile history: `{profile["compatibleProfileRaces"]}`.',
        ])
        if profile.get("performance"):
            performance = profile["performance"]
            lines.append(
                f'- WTop3: `{performance["winnerTop3"]}/{performance["races"]}` '
                f'(`{performance["winnerTop3Rate"]}`); misses `{performance["misses"]}`.'
            )
        ceiling = profile.get("optimisticRescueCeiling") or {}
        lines.extend([
            f'- Eligible metrics: `{profile.get("eligibleMetricCount", 0)}`; '
            f'pair-direction universe: `{profile.get("pairDirectionUniverseCount", 0)}`.',
            f'- Optimistic pair rescue ceiling: `{ceiling.get("rescuedByAnyPair", 0)}/'
            f'{ceiling.get("baselineMisses", 0)}` misses; this is not promotion evidence.',
            "",
        ])
        candidates = profile.get("candidates") or []
        if candidates:
            lines.extend([
                "| Candidate | Build net | Inner net/damage | Outer net/damage | Full net | Gap base/candidate | Status |",
                "|---|---:|---:|---:|---:|---:|---|",
            ])
            for item in candidates[:10]:
                slices = item["slices"]
                lines.append(
                    f'| {item["candidateId"]} | {slices["build"]["netHits"]:+d} | '
                    f'{slices["inner"]["netHits"]:+d}/{slices["inner"]["damages"]} | '
                    f'{slices["outer"]["netHits"]:+d}/{slices["outer"]["damages"]} | '
                    f'{slices["full"]["netHits"]:+d} | '
                    f'{slices["outer"]["baselineBoundaryGapMedian"]}/'
                    f'{slices["outer"]["candidateBoundaryGapMedian"]} | {item["status"]} |'
                )
            lines.append("")
    lines.extend([
        "## Policy",
        "",
        "- Candidate discovery uses only the chronological first 60 percent.",
        "- Inner and outer holdouts must have no Winner-Top3 damage.",
        "- Outer holdout must add at least one Winner-Top3 hit.",
        "- Top3 boundary separation may not degrade by more than 10 percent.",
        "- AGF is excluded here; the separate bounded SART1 shadow owns that signal.",
        "- REPLAY_PRIORITY changes no live weight; full replay is still required.",
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
    latest_dir = data_dir / "automation" / "interaction-diagnostics"
    paths = {
        "dailyJson": daily_dir / "winner-top3-interaction-diagnostics.json",
        "dailyMarkdown": daily_dir / "winner-top3-interaction-diagnostics.md",
        "latestJson": latest_dir / "latest.json",
        "latestMarkdown": latest_dir / "latest.md",
    }
    latest_json = json.dumps(report, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
    daily_json = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    markdown = render_markdown(report)
    for key, path in paths.items():
        if key == "latestJson":
            content = latest_json
        elif key == "dailyJson":
            content = daily_json
        else:
            content = markdown
        atomic_write(path, content)
    return {key: str(path) for key, path in paths.items()}


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose bounded Winner-Top3 metric interactions.")
    parser.add_argument("--predictions", required=True, type=Path)
    parser.add_argument("--data-dir", required=True, type=Path)
    parser.add_argument("--run-date", default=datetime.now().astimezone().strftime("%Y-%m-%d"))
    parser.add_argument("--profiles", default=",".join(DEFAULT_PROFILES))
    arguments = parser.parse_args()
    profiles = tuple(item.strip().upper() for item in arguments.profiles.split(",") if item.strip())
    entries, invalid_lines = load_jsonl(arguments.predictions)
    report = build_report(
        entries,
        arguments.run_date,
        profiles,
        invalid_json_lines=invalid_lines,
    )
    paths = persist(report, arguments.data_dir)
    print(json.dumps({
        "success": True,
        "runDate": report["runDate"],
        "input": report["input"],
        "replayPriorityCount": report["replayPriorityCount"],
        "profiles": [
            {
                "profile": item["profile"],
                "status": item["status"],
                "latestLabeledFingerprintRaces": item.get("latestLabeledFingerprintRaces", 0),
                "performance": item.get("performance"),
                "eligibleMetricCount": item.get("eligibleMetricCount", 0),
                "pairDirectionUniverseCount": item.get("pairDirectionUniverseCount", 0),
                "optimisticRescueCeiling": item.get("optimisticRescueCeiling"),
                "replayPriorities": item["replayPriorities"],
            }
            for item in report["profiles"]
        ],
        "paths": paths,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
