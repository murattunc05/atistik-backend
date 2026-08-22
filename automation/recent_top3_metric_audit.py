#!/usr/bin/env python3
"""Rolling Winner-Top3 counterfactual audit for the active v4 ranking.

This is a discovery-only monitor.  It replays small raw weight-point changes
over the latest official, fully-labelled v4 corpus and reports rescues and
damages separately.  A positive result can only nominate a frozen prospective
shadow; it never changes ranking or Telegram output.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from itertools import combinations
import json
from pathlib import Path
from typing import Any

try:
    from automation.metric_signal_registry import (
        METRIC_KEYS,
        active_weights,
        classify_race,
        competitive_race_rows,
        group_races,
        load_jsonl,
        metric_source_proven,
        parse_race_date,
        profile_of,
        race_key,
        race_sort_key,
        rounded,
        safe_int,
        score_with_weights,
        winner,
    )
    from automation.metric_signal_replay import compatible_race, version_tuple
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from metric_signal_registry import (
        METRIC_KEYS,
        active_weights,
        classify_race,
        competitive_race_rows,
        group_races,
        load_jsonl,
        metric_source_proven,
        parse_race_date,
        profile_of,
        race_key,
        race_sort_key,
        rounded,
        safe_int,
        score_with_weights,
        winner,
    )
    from metric_signal_replay import compatible_race, version_tuple


SCHEMA_VERSION = "recent-top3-metric-audit-v1"
SINGLE_POINT_DELTAS = (-3.0, -2.0, -1.0, 1.0, 2.0, 3.0)
MARKET_DERIVED_METRICS = {
    "agf_score",
    "favorite_risk_guard_score",
    "elite_consensus_score",
}
SCOPE_MIN_RACES = {"GROUP": 12, "PROFILE": 10, "PROFILE_SURFACE": 8}
SPLIT_MIN_RACES = 4
MAX_SINGLE_RESULTS_PER_SCOPE = 12
MAX_PAIR_METRICS = 6
MAX_PAIR_RESULTS_PER_SCOPE = 8
ROBUST_TOP3_BOUNDARY_PERCENTILE = 0.25


def _official_full_labels(rows: list[dict[str, Any]]) -> bool:
    return bool(rows) and all(
        str(row.get("result_source") or "") == "tjk_official_results"
        for row in rows
    )


def _score_map(
    rows: list[dict[str, Any]],
    adjustments: dict[str, float],
) -> dict[int, float]:
    scores: dict[int, float] = {}
    for row in rows:
        weights = active_weights(row)
        for metric, delta in adjustments.items():
            weights[metric] = max(0.0, weights.get(metric, 0.0) + float(delta))
        scores[id(row)] = score_with_weights(row, weights)
    return scores


def _ordered(
    rows: list[dict[str, Any]],
    scores: dict[int, float],
) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            -scores[id(row)],
            safe_int(row.get("rank_pred"), 999),
            str(row.get("horse_name") or ""),
        ),
    )


def _baseline_faithful(rows: list[dict[str, Any]]) -> bool:
    scores = _score_map(rows, {})
    replay = {id(row) for row in _ordered(rows, scores)[:3]}
    visible = {
        id(row)
        for row in sorted(rows, key=lambda row: safe_int(row.get("rank_pred"), 999))[:3]
    }
    return replay == visible


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + ((ordered[upper] - ordered[lower]) * fraction)


def _top3_boundary_margin(
    ordered_rows: list[dict[str, Any]],
    scores: dict[int, float],
) -> float | None:
    if len(ordered_rows) < 4:
        return None
    return scores[id(ordered_rows[2])] - scores[id(ordered_rows[3])]


def _winner_top3_margin(
    race_winner: dict[str, Any],
    winner_rank: int,
    ordered_rows: list[dict[str, Any]],
    scores: dict[int, float],
) -> float | None:
    if winner_rank > 3:
        return None
    if len(ordered_rows) < 4:
        return float("inf")
    return scores[id(race_winner)] - scores[id(ordered_rows[3])]


def _robust_top3_floor(races: list[list[dict[str, Any]]]) -> float:
    margins = []
    for rows in races:
        scores = _score_map(rows, {})
        margin = _top3_boundary_margin(_ordered(rows, scores), scores)
        if margin is not None:
            margins.append(margin)
    return _percentile(margins, ROBUST_TOP3_BOUNDARY_PERCENTILE) or 0.0


def _latest_compatible_races(
    entries: list[dict[str, Any]],
) -> tuple[list[list[dict[str, Any]]], tuple[int, ...], dict[str, int]]:
    prepared: list[list[dict[str, Any]]] = []
    exclusions = {
        "notFullyLabeled": 0,
        "notOfficial": 0,
        "notCompatible": 0,
        "invalidDate": 0,
        "baselineReplayMismatch": 0,
        "olderRankingVersion": 0,
    }
    for rows in group_races(entries):
        if classify_race(rows) != "fully_labeled":
            exclusions["notFullyLabeled"] += 1
            continue
        if not _official_full_labels(rows):
            exclusions["notOfficial"] += 1
            continue
        competitive = competitive_race_rows(rows)
        if len(competitive) < 2 or not compatible_race(competitive):
            exclusions["notCompatible"] += 1
            continue
        if parse_race_date(competitive[0].get("race_date")) is None:
            exclusions["invalidDate"] += 1
            continue
        prepared.append(competitive)

    latest_version = max(
        (version_tuple(rows[0].get("v4_version")) for rows in prepared),
        default=(),
    )
    selected = []
    for rows in prepared:
        if version_tuple(rows[0].get("v4_version")) != latest_version:
            exclusions["olderRankingVersion"] += 1
            continue
        if not _baseline_faithful(rows):
            exclusions["baselineReplayMismatch"] += 1
            continue
        selected.append(rows)
    return sorted(selected, key=race_sort_key), latest_version, exclusions


def _scope_keys(rows: list[dict[str, Any]]) -> list[tuple[str, str]]:
    profile = profile_of(rows)
    return [
        ("GROUP", profile["category"]),
        ("PROFILE", profile["subtype"]),
        ("PROFILE_SURFACE", f'{profile["subtype"]}|{profile["track"]}'),
    ]


def _market_metric_allowed(races: list[list[dict[str, Any]]], metric: str) -> bool:
    if metric not in MARKET_DERIVED_METRICS:
        return True
    return bool(races) and all(
        all(row.get("agf_allowed_for_ranking") is True for row in rows)
        for rows in races
    )


def _candidate_id(adjustments: dict[str, float]) -> str:
    return "+".join(
        f"{metric}:{delta:+g}pp"
        for metric, delta in sorted(adjustments.items())
    )


def _source_coverage(
    races: list[list[dict[str, Any]]],
    adjustments: dict[str, float],
) -> dict[str, Any]:
    by_metric: dict[str, float | None] = {}
    for metric in adjustments:
        rows = [row for race_rows in races for row in race_rows]
        by_metric[metric] = rounded(
            sum(metric_source_proven(row, metric) for row in rows) / len(rows),
            4,
        ) if rows else None
    values = [value for value in by_metric.values() if value is not None]
    return {
        "byMetric": by_metric,
        "minimum": min(values) if values else None,
    }


def _evaluate(
    races: list[list[dict[str, Any]]],
    adjustments: dict[str, float],
    *,
    robust_top3_floor: float,
) -> dict[str, Any]:
    rescues = damages = 0
    robust_hit_rescues = robust_hit_damages = 0
    robust_top3_rescues = fragile_top3_rescues = 0
    baseline_top1 = candidate_top1 = 0
    baseline_top3 = candidate_top3 = 0
    baseline_top5 = candidate_top5 = 0
    baseline_rank_sum = candidate_rank_sum = 0
    changed_top3 = 0
    baseline_boundary_margins: list[float] = []
    candidate_boundary_margins: list[float] = []
    rescue_races: list[dict[str, Any]] = []
    damage_races: list[dict[str, Any]] = []
    for rows in races:
        race_winner = winner(rows)
        if race_winner is None:
            continue
        baseline_scores = _score_map(rows, {})
        candidate_scores = _score_map(rows, adjustments)
        baseline_order = _ordered(rows, baseline_scores)
        candidate_order = _ordered(rows, candidate_scores)
        baseline_rank = next(
            index for index, row in enumerate(baseline_order, start=1)
            if row is race_winner
        )
        candidate_rank = next(
            index for index, row in enumerate(candidate_order, start=1)
            if row is race_winner
        )
        baseline_hit = baseline_rank <= 3
        candidate_hit = candidate_rank <= 3
        baseline_winner_margin = _winner_top3_margin(
            race_winner,
            baseline_rank,
            baseline_order,
            baseline_scores,
        )
        candidate_winner_margin = _winner_top3_margin(
            race_winner,
            candidate_rank,
            candidate_order,
            candidate_scores,
        )
        baseline_robust_hit = bool(
            baseline_hit
            and baseline_winner_margin is not None
            and baseline_winner_margin >= robust_top3_floor
        )
        candidate_robust_hit = bool(
            candidate_hit
            and candidate_winner_margin is not None
            and candidate_winner_margin >= robust_top3_floor
        )
        baseline_boundary_margin = _top3_boundary_margin(
            baseline_order,
            baseline_scores,
        )
        candidate_boundary_margin = _top3_boundary_margin(
            candidate_order,
            candidate_scores,
        )
        if baseline_boundary_margin is not None:
            baseline_boundary_margins.append(baseline_boundary_margin)
        if candidate_boundary_margin is not None:
            candidate_boundary_margins.append(candidate_boundary_margin)
        if not baseline_hit and candidate_hit:
            rescues += 1
            if candidate_robust_hit:
                robust_top3_rescues += 1
            else:
                fragile_top3_rescues += 1
            rescue_races.append({
                "raceDate": rows[0].get("race_date"),
                "raceId": str(rows[0].get("race_id") or ""),
                "city": rows[0].get("city"),
                "raceNo": rows[0].get("race_no"),
                "winner": race_winner.get("horse_name"),
                "baselineRank": baseline_rank,
                "candidateRank": candidate_rank,
                "candidateWinnerTop3Margin": rounded(candidate_winner_margin, 4),
                "robustTop3Rescue": candidate_robust_hit,
            })
        elif baseline_hit and not candidate_hit:
            damages += 1
            damage_races.append({
                "raceDate": rows[0].get("race_date"),
                "raceId": str(rows[0].get("race_id") or ""),
                "city": rows[0].get("city"),
                "raceNo": rows[0].get("race_no"),
                "winner": race_winner.get("horse_name"),
                "baselineRank": baseline_rank,
                "candidateRank": candidate_rank,
                "baselineWinnerTop3Margin": rounded(baseline_winner_margin, 4),
            })
        if not baseline_robust_hit and candidate_robust_hit:
            robust_hit_rescues += 1
        elif baseline_robust_hit and not candidate_robust_hit:
            robust_hit_damages += 1
        baseline_top1 += baseline_rank == 1
        candidate_top1 += candidate_rank == 1
        baseline_top3 += baseline_hit
        candidate_top3 += candidate_hit
        baseline_top5 += baseline_rank <= 5
        candidate_top5 += candidate_rank <= 5
        baseline_rank_sum += baseline_rank
        candidate_rank_sum += candidate_rank
        changed_top3 += (
            {id(row) for row in baseline_order[:3]}
            != {id(row) for row in candidate_order[:3]}
        )
    count = len(races)
    return {
        "races": count,
        "baselineWinnerTop3": baseline_top3,
        "candidateWinnerTop3": candidate_top3,
        "baselineWinnerTop3Rate": rounded(baseline_top3 / count, 4) if count else None,
        "candidateWinnerTop3Rate": rounded(candidate_top3 / count, 4) if count else None,
        "rescues": rescues,
        "damages": damages,
        "netHits": rescues - damages,
        "robustTop3Floor": rounded(robust_top3_floor, 4),
        "robustHitRescues": robust_hit_rescues,
        "robustHitDamages": robust_hit_damages,
        "robustHitNet": robust_hit_rescues - robust_hit_damages,
        "robustTop3Rescues": robust_top3_rescues,
        "fragileTop3Rescues": fragile_top3_rescues,
        "top1Delta": candidate_top1 - baseline_top1,
        "top5Delta": candidate_top5 - baseline_top5,
        "averageWinnerRankDelta": rounded(
            (candidate_rank_sum - baseline_rank_sum) / count,
            4,
        ) if count else None,
        "changedTop3SetRaces": changed_top3,
        "baselineAverageTop3BoundaryMargin": rounded(
            sum(baseline_boundary_margins) / len(baseline_boundary_margins),
            4,
        ) if baseline_boundary_margins else None,
        "candidateAverageTop3BoundaryMargin": rounded(
            sum(candidate_boundary_margins) / len(candidate_boundary_margins),
            4,
        ) if candidate_boundary_margins else None,
        "rescueRaces": rescue_races,
        "damageRaces": damage_races,
    }


def _candidate_sort_key(candidate: dict[str, Any]) -> tuple[Any, ...]:
    full = candidate["windows"]["last14"]
    latest = candidate["windows"]["latest7"]
    coverage = candidate["sourceCoverage"].get("minimum") or 0.0
    return (
        -full["netHits"],
        -latest["netHits"],
        full["damages"],
        -full["rescues"],
        full["averageWinnerRankDelta"] or 0.0,
        -coverage,
        candidate["candidateId"],
    )


def _status(
    scope_type: str,
    windows: dict[str, dict[str, Any]],
    coverage: dict[str, Any],
) -> str:
    full = windows["last14"]
    previous = windows["previous7"]
    latest = windows["latest7"]
    minimum = SCOPE_MIN_RACES[scope_type]
    strong = bool(
        full["races"] >= minimum
        and previous["races"] >= SPLIT_MIN_RACES
        and latest["races"] >= SPLIT_MIN_RACES
        and full["netHits"] >= 2
        and full["rescues"] >= 2
        and full["damages"] == 0
        and previous["netHits"] >= 0
        and previous["damages"] == 0
        and latest["netHits"] >= 1
        and latest["damages"] == 0
        and full["top5Delta"] >= 0
        and full["robustTop3Rescues"] >= 1
        and full["robustHitDamages"] == 0
        and (coverage.get("minimum") or 0.0) >= 0.60
    )
    if strong:
        return "CANDIDATE_FOR_FROZEN_PROSPECTIVE_SHADOW"
    if (
        full["netHits"] > 0
        and full["damages"] == 0
        and full["robustTop3Rescues"] == 0
    ):
        return "RECENT_POSITIVE_FRAGILE_TOP3_MARGIN"
    if full["netHits"] > 0 and full["damages"] == 0:
        return "RECENT_POSITIVE_LOW_EVIDENCE"
    return "HOLD"


def _evaluate_definition(
    scope_type: str,
    scope_key: str,
    periods: dict[str, list[list[dict[str, Any]]]],
    adjustments: dict[str, float],
    *,
    kind: str,
) -> dict[str, Any]:
    coverage = _source_coverage(periods["last14"], adjustments)
    robust_top3_floor = _robust_top3_floor(periods["last14"])
    windows = {
        name: _evaluate(
            races,
            adjustments,
            robust_top3_floor=robust_top3_floor,
        )
        for name, races in periods.items()
    }
    status = _status(scope_type, windows, coverage)
    if kind == "PAIR" and status == "CANDIDATE_FOR_FROZEN_PROSPECTIVE_SHADOW":
        status = "PAIR_DISCOVERY_REQUIRES_FROZEN_REPLAY"
    return {
        "candidateId": _candidate_id(adjustments),
        "kind": kind,
        "scopeType": scope_type,
        "scopeKey": scope_key,
        "adjustments": adjustments,
        "sourceCoverage": coverage,
        "windows": windows,
        "status": status,
        "usedForRanking": False,
        "telegramVisible": False,
    }


def _scope_report(
    scope_type: str,
    scope_key: str,
    periods: dict[str, list[list[dict[str, Any]]]],
) -> dict[str, Any]:
    full = periods["last14"]
    robust_top3_floor = _robust_top3_floor(full)
    baseline = _evaluate(full, {}, robust_top3_floor=robust_top3_floor)
    singles = []
    for metric in METRIC_KEYS:
        if not _market_metric_allowed(full, metric):
            continue
        for delta in SINGLE_POINT_DELTAS:
            candidate = _evaluate_definition(
                scope_type,
                scope_key,
                periods,
                {metric: delta},
                kind="SINGLE",
            )
            outcome = candidate["windows"]["last14"]
            if outcome["changedTop3SetRaces"] or outcome["averageWinnerRankDelta"]:
                singles.append(candidate)
    singles.sort(key=_candidate_sort_key)
    singles = singles[:MAX_SINGLE_RESULTS_PER_SCOPE]

    best_by_metric: dict[str, dict[str, Any]] = {}
    for candidate in singles:
        metric = next(iter(candidate["adjustments"]))
        best_by_metric.setdefault(metric, candidate)
    pair_seeds = list(best_by_metric.values())[:MAX_PAIR_METRICS]
    pairs = []
    for left, right in combinations(pair_seeds, 2):
        adjustments = {
            **left["adjustments"],
            **right["adjustments"],
        }
        candidate = _evaluate_definition(
            scope_type,
            scope_key,
            periods,
            adjustments,
            kind="PAIR",
        )
        if candidate["windows"]["last14"]["changedTop3SetRaces"]:
            pairs.append(candidate)
    pairs.sort(key=_candidate_sort_key)
    pairs = pairs[:MAX_PAIR_RESULTS_PER_SCOPE]
    return {
        "scopeType": scope_type,
        "scopeKey": scope_key,
        "raceCounts": {name: len(races) for name, races in periods.items()},
        "baseline": baseline,
        "singleCandidates": singles,
        "pairCandidates": pairs,
    }


def build_report(
    entries: list[dict[str, Any]],
    *,
    invalid_json_lines: int = 0,
    generated_at: str | None = None,
    run_date: str | None = None,
) -> dict[str, Any]:
    races, latest_version, exclusions = _latest_compatible_races(entries)
    latest_day = max(
        (parse_race_date(rows[0].get("race_date")) for rows in races),
        default=None,
    )
    if latest_day is None:
        last14: list[list[dict[str, Any]]] = []
        previous7: list[list[dict[str, Any]]] = []
        latest7: list[list[dict[str, Any]]] = []
    else:
        start14 = latest_day - timedelta(days=13)
        start_latest7 = latest_day - timedelta(days=6)
        last14 = [
            rows for rows in races
            if start14 <= parse_race_date(rows[0].get("race_date")) <= latest_day
        ]
        previous7 = [
            rows for rows in last14
            if parse_race_date(rows[0].get("race_date")) < start_latest7
        ]
        latest7 = [
            rows for rows in last14
            if parse_race_date(rows[0].get("race_date")) >= start_latest7
        ]

    scoped: dict[tuple[str, str], dict[str, list[list[dict[str, Any]]]]] = {}
    for period_name, period_races in (
        ("last14", last14),
        ("previous7", previous7),
        ("latest7", latest7),
    ):
        for rows in period_races:
            for scope in _scope_keys(rows):
                scoped.setdefault(
                    scope,
                    {"last14": [], "previous7": [], "latest7": []},
                )[period_name].append(rows)

    scopes = [
        _scope_report(scope_type, scope_key, periods)
        for (scope_type, scope_key), periods in sorted(scoped.items())
        if len(periods["last14"]) >= 4
    ]
    opportunities = [
        candidate
        for scope in scopes
        for candidate in scope["singleCandidates"] + scope["pairCandidates"]
        if candidate["status"] != "HOLD"
    ]
    opportunities.sort(
        key=lambda candidate: (
            0 if candidate["status"] == "CANDIDATE_FOR_FROZEN_PROSPECTIVE_SHADOW" else 1,
            *_candidate_sort_key(candidate),
        )
    )
    generated = generated_at or datetime.now(timezone.utc).isoformat(
        timespec="seconds"
    ).replace("+00:00", "Z")
    return {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": generated,
        "runDate": run_date,
        "mode": "analysis_only",
        "usedForRanking": False,
        "telegramVisible": False,
        "primaryObjective": "winner_top3",
        "input": {
            "rows": len(entries),
            "invalidJsonLines": invalid_json_lines,
            "latestRankingVersion": ".".join(str(part) for part in latest_version),
            "compatibleLatestVersionRaces": len(races),
            "lastFullyLabeledOfficialDate": latest_day.strftime("%Y-%m-%d") if latest_day else None,
            "last14Races": len(last14),
            "previous7Races": len(previous7),
            "latest7Races": len(latest7),
            "exclusions": exclusions,
        },
        "policy": {
            "singlePointDeltas": list(SINGLE_POINT_DELTAS),
            "pairSearch": "best six distinct single-metric seeds per scope",
            "top1Gate": False,
            "robustTop3BoundaryPercentile": ROBUST_TOP3_BOUNDARY_PERCENTILE,
            "robustTop3Definition": "winner is Top3 and its score margin over rank 4 is at least the scope baseline p25 boundary margin",
            "damageGate": 0,
            "officialFullLabelsRequired": True,
            "visibleReplayTop3SetAgreementRequired": True,
            "positiveStatusCeiling": "frozen_prospective_shadow_only",
        },
        "scopes": scopes,
        "opportunities": opportunities[:40],
    }


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    temporary.replace(path)


def _markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Rolling Winner Top3 Metric Audit",
        "",
        f"- Latest official date: {report['input']['lastFullyLabeledOfficialDate']}",
        f"- Latest v4 version: {report['input']['latestRankingVersion']}",
        f"- Last 14 / previous 7 / latest 7 races: {report['input']['last14Races']} / {report['input']['previous7Races']} / {report['input']['latest7Races']}",
        "- Ranking impact: none",
        "",
        "| Scope | Candidate | Kind | 14d rescue/damage/net | Robust/fragile Top3 rescues | Prev7 net | Latest7 net | Source | Status |",
        "|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for candidate in report["opportunities"][:25]:
        full = candidate["windows"]["last14"]
        previous = candidate["windows"]["previous7"]
        latest = candidate["windows"]["latest7"]
        lines.append(
            f"| {candidate['scopeType']}:{candidate['scopeKey']} | "
            f"{candidate['candidateId']} | {candidate['kind']} | "
            f"{full['rescues']}/{full['damages']}/{full['netHits']} | "
            f"{full['robustTop3Rescues']}/{full['fragileTop3Rescues']} | "
            f"{previous['netHits']} | {latest['netHits']} | "
            f"{candidate['sourceCoverage'].get('minimum')} | {candidate['status']} |"
        )
    return "\n".join(lines) + "\n"


def persist(report: dict[str, Any], data_dir: Path, run_date: str) -> dict[str, str]:
    rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    markdown = _markdown(report)
    paths = {
        "dailyJson": data_dir / "automation" / "runs" / run_date / "recent-top3-metric-audit.json",
        "dailyMarkdown": data_dir / "automation" / "runs" / run_date / "recent-top3-metric-audit.md",
        "latestJson": data_dir / "automation" / "recent-top3-metrics" / "latest.json",
        "latestMarkdown": data_dir / "automation" / "recent-top3-metrics" / "latest.md",
    }
    for key, path in paths.items():
        _atomic_write(path, markdown if key.endswith("Markdown") else rendered)
    return {key: str(path) for key, path in paths.items()}


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit rolling Winner Top3 metric changes.")
    parser.add_argument("--predictions", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--run-date", required=True)
    args = parser.parse_args()
    entries, invalid = load_jsonl(args.predictions)
    report = build_report(
        entries,
        invalid_json_lines=invalid,
        run_date=args.run_date,
    )
    paths = persist(report, args.data_dir, args.run_date)
    print(json.dumps({
        "success": True,
        "runDate": args.run_date,
        "input": report["input"],
        "opportunityCount": len(report["opportunities"]),
        "paths": paths,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
