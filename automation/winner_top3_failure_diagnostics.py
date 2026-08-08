"""Diagnose chronological Winner-Top3 failures without changing live ranking."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from automation.confidence_calibration import calibration_compatible, weight_fingerprint
    from automation.metric_signal_registry import (
        METRIC_KEYS,
        METRIC_SOURCE_FLAGS,
        NON_NEUTRAL_EPSILON,
        active_weights,
        feature_value,
        group_races,
        load_jsonl,
        metric_source_proven,
        profile_of,
        rank_of_row,
        rounded,
        safe_float,
        safe_int,
        score_diagnostics,
        score_with_weights,
        winner,
    )
except ModuleNotFoundError:  # direct: python automation/winner_top3_failure_diagnostics.py
    from confidence_calibration import calibration_compatible, weight_fingerprint
    from metric_signal_registry import (
        METRIC_KEYS,
        METRIC_SOURCE_FLAGS,
        NON_NEUTRAL_EPSILON,
        active_weights,
        feature_value,
        group_races,
        load_jsonl,
        metric_source_proven,
        profile_of,
        rank_of_row,
        rounded,
        safe_float,
        safe_int,
        score_diagnostics,
        score_with_weights,
        winner,
    )


SCHEMA_VERSION = "winner-top3-failure-diagnostics-v1"
DEFAULT_GROUPS = ("SARTLI", "HANDIKAP")
BOUNDED_WEIGHT_POINTS = 2.0
GROUP_MIN_RACES = 60
GROUP_MIN_OUTER = 12
PROFILE_MIN_RACES = 30
PROFILE_MIN_OUTER = 6
MIN_COVERAGE = 0.40
MIN_NON_NEUTRAL = 0.15


def field_bucket(size: int) -> str:
    if size <= 8:
        return "small"
    if size <= 12:
        return "medium"
    return "large"


def visible_winner_rank(rows: list[dict[str, Any]]) -> int:
    race_winner = winner(rows)
    return safe_int(race_winner.get("rank_pred"), 999) if race_winner else 999


def summarize_performance(races: list[list[dict[str, Any]]]) -> dict[str, Any]:
    ranks = [visible_winner_rank(rows) for rows in races]
    count = len(ranks)
    return {
        "races": count,
        "top1": sum(rank == 1 for rank in ranks),
        "winnerTop3": sum(rank <= 3 for rank in ranks),
        "winnerTop5": sum(rank <= 5 for rank in ranks),
        "winnerTop3Rate": rounded(sum(rank <= 3 for rank in ranks) / count, 4) if count else None,
        "misses": sum(rank > 3 for rank in ranks),
        "averageWinnerRank": rounded(statistics.mean(ranks), 3) if ranks else None,
    }


def _visible_top3(rows: list[dict[str, Any]]) -> set[int]:
    return {
        id(row)
        for row in sorted(rows, key=lambda row: safe_int(row.get("rank_pred")))[:3]
    }


def metric_race_outcome(rows: list[dict[str, Any]], metric: str) -> dict[str, Any] | None:
    race_winner = winner(rows)
    if race_winner is None:
        return None
    weights = active_weights(rows[0])
    control_scores = {id(row): score_with_weights(row, weights) for row in rows}
    control_top3 = {
        id(row)
        for row in sorted(
            rows,
            key=lambda row: (-control_scores[id(row)], safe_int(row.get("rank_pred"))),
        )[:3]
    }
    replay_agreement = control_top3 == _visible_top3(rows)
    baseline_hit = rank_of_row(rows, race_winner, control_scores) <= 3

    direction_hits = {}
    for label, delta in (("plus2", BOUNDED_WEIGHT_POINTS), ("minus2", -BOUNDED_WEIGHT_POINTS)):
        candidate_scores = {
            id(row): score_with_weights(
                row,
                weights,
                added_metric=metric,
                added_points=delta,
            )
            for row in rows
        }
        direction_hits[label] = rank_of_row(rows, race_winner, candidate_scores) <= 3

    source_rows = [row for row in rows if metric_source_proven(row, metric)]
    source_values = [
        float(value)
        for row in source_rows
        if (value := feature_value(row, metric)) is not None
    ]
    univariate_hit = None
    if metric_source_proven(race_winner, metric) and len(source_rows) >= 3:
        metric_scores = {
            id(row): (
                feature_value(row, metric)
                if metric_source_proven(row, metric)
                else -1.0
            )
            for row in rows
        }
        univariate_hit = rank_of_row(rows, race_winner, metric_scores) <= 3

    return {
        "race": rows,
        "replayAgreement": replay_agreement,
        "baseline": baseline_hit,
        "plus2": direction_hits["plus2"],
        "minus2": direction_hits["minus2"],
        "univariate": univariate_hit,
        "winnerHasSource": metric_source_proven(race_winner, metric),
        "coverageRows": len(source_rows),
        "nonNeutralRows": sum(abs(value - 50.0) >= NON_NEUTRAL_EPSILON for value in source_values),
        "totalRows": len(rows),
        "weight": weights.get(metric, 0.0),
    }


def direction_summary(outcomes: list[dict[str, Any]], direction: str) -> dict[str, Any]:
    comparable = [item for item in outcomes if item.get(direction) is not None]
    rescues = sum(not item["baseline"] and item[direction] for item in comparable)
    damages = sum(item["baseline"] and not item[direction] for item in comparable)
    baseline_hits = sum(bool(item["baseline"]) for item in comparable)
    candidate_hits = sum(bool(item[direction]) for item in comparable)
    return {
        "races": len(comparable),
        "baselineWinnerTop3": baseline_hits,
        "candidateWinnerTop3": candidate_hits,
        "rescues": rescues,
        "damages": damages,
        "netHits": rescues - damages,
    }


def chronological_slices(outcomes: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    count = len(outcomes)
    outer_size = math.ceil(count * 0.20) if count else 0
    outer = outcomes[-outer_size:] if outer_size else []
    return {"full": outcomes, "outer": outer, "recent20": outcomes[-20:]}


def summarize_metric(
    races: list[list[dict[str, Any]]],
    metric: str,
    *,
    min_races: int = GROUP_MIN_RACES,
    min_outer: int = GROUP_MIN_OUTER,
) -> dict[str, Any]:
    outcomes = [
        outcome
        for rows in races
        if (outcome := metric_race_outcome(rows, metric)) is not None
    ]
    evidence = [item for item in outcomes if item["replayAgreement"]]
    slices = chronological_slices(evidence)
    directions = {}
    for direction in ("plus2", "minus2"):
        directions[direction] = {
            key: direction_summary(subset, direction)
            for key, subset in slices.items()
        }

    def direction_key(direction: str) -> tuple[int, int, int, int]:
        values = directions[direction]
        return (
            values["recent20"]["netHits"],
            values["outer"]["netHits"],
            values["full"]["netHits"],
            1 if direction == "plus2" else 0,
        )

    best_direction = max(("plus2", "minus2"), key=direction_key)
    best = directions[best_direction]
    total_rows = sum(item["totalRows"] for item in outcomes)
    coverage_rows = sum(item["coverageRows"] for item in outcomes)
    non_neutral_rows = sum(item["nonNeutralRows"] for item in outcomes)
    misses = [item for item in evidence if not item["baseline"]]
    univariate_rescues = sum(item.get("univariate") is True for item in misses)
    winner_source_missing = sum(not item["winnerHasSource"] for item in misses)
    coverage = coverage_rows / total_rows if total_rows else 0.0
    non_neutral = non_neutral_rows / total_rows if total_rows else 0.0
    weights = [float(item["weight"]) for item in evidence]

    checks = [
        {"name": "evidence_threshold", "passed": len(evidence) >= min_races,
         "detail": f"races={len(evidence)}/{min_races}"},
        {"name": "outer_threshold", "passed": best["outer"]["races"] >= min_outer,
         "detail": f'races={best["outer"]["races"]}/{min_outer}'},
        {"name": "coverage", "passed": coverage >= MIN_COVERAGE,
         "detail": f"coverage={rounded(coverage, 4)}/{MIN_COVERAGE}"},
        {"name": "non_neutral", "passed": non_neutral >= MIN_NON_NEUTRAL,
         "detail": f"rate={rounded(non_neutral, 4)}/{MIN_NON_NEUTRAL}"},
        {"name": "full_net_plus_2", "passed": best["full"]["netHits"] >= 2,
         "detail": f'net={best["full"]["netHits"]}'},
        {"name": "outer_no_regression", "passed": best["outer"]["netHits"] >= 0,
         "detail": f'net={best["outer"]["netHits"]}'},
        {"name": "outer_no_damage", "passed": best["outer"]["damages"] == 0,
         "detail": f'damages={best["outer"]["damages"]}'},
        {"name": "recent20_plus_1", "passed": best["recent20"]["netHits"] >= 1,
         "detail": f'net={best["recent20"]["netHits"]}'},
    ]
    sample_ready = all(item["passed"] for item in checks[:2])
    data_ready = all(item["passed"] for item in checks[2:4])
    if all(item["passed"] for item in checks):
        status = "REPLAY_PRIORITY"
    elif not sample_ready:
        status = "COLLECTING"
    elif not data_ready:
        status = "DATA_LOW"
    elif best["full"]["netHits"] > 0 or best["recent20"]["netHits"] > 0:
        status = "WATCH_RESCUE"
    elif best["full"]["netHits"] < 0 or best["outer"]["netHits"] < 0:
        status = "HARM_RISK"
    else:
        status = "NO_BOUNDED_SIGNAL"
    return {
        "metric": metric,
        "sourceGate": METRIC_SOURCE_FLAGS.get(metric),
        "coverage": rounded(coverage, 4),
        "nonNeutralRate": rounded(non_neutral, 4),
        "currentWeightPctMedian": rounded(statistics.median(weights), 3) if weights else 0.0,
        "evidenceRaces": len(evidence),
        "excludedReplayMismatchRaces": len(outcomes) - len(evidence),
        "misses": len(misses),
        "univariateMissRescues": univariate_rescues,
        "winnerSourceMissingOnMisses": winner_source_missing,
        "bestDirection": best_direction,
        "directions": directions,
        "status": status,
        "checks": checks,
        "thresholds": {"races": min_races, "outerRaces": min_outer},
    }


def partition_summaries(
    races: list[list[dict[str, Any]]],
    key_function,
) -> list[dict[str, Any]]:
    buckets: dict[str, list[list[dict[str, Any]]]] = defaultdict(list)
    for rows in races:
        buckets[str(key_function(rows))].append(rows)
    items = []
    for key, subset in buckets.items():
        summary = summarize_performance(subset)
        items.append({"key": key, **summary})
    return sorted(items, key=lambda item: (-item["misses"], -item["races"], item["key"]))


def race_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    first = rows[0]
    race_winner = winner(rows) or {}
    diagnostic = score_diagnostics(rows, bootstrap_iterations=0) or {}
    profile = profile_of(rows)
    return {
        "raceDate": first.get("race_date"),
        "raceId": first.get("race_id"),
        "city": first.get("city"),
        "raceNo": first.get("race_no"),
        "raceType": first.get("race_type"),
        "profile": profile["subtype"],
        "track": profile["track"],
        "fieldSize": len(rows),
        "winner": race_winner.get("horse_name"),
        "winnerRank": visible_winner_rank(rows),
        "separationStatus": diagnostic.get("separationStatus"),
        "top3Top4Gap": diagnostic.get("top3Top4Gap"),
        "cutoffCrowd2pt": diagnostic.get("cutoffCrowd2pt"),
        "weightedRealCoverage": diagnostic.get("weightedRealCoverage"),
        "informativeWeightShare": diagnostic.get("informativeWeightShare"),
        "v4Version": first.get("v4_version"),
        "weightFingerprint": weight_fingerprint(first),
    }


def build_profile_report(
    profile_key: str,
    fingerprint: str,
    races: list[list[dict[str, Any]]],
) -> dict[str, Any]:
    metrics = [
        summarize_metric(
            races,
            metric,
            min_races=PROFILE_MIN_RACES,
            min_outer=PROFILE_MIN_OUTER,
        )
        for metric in METRIC_KEYS
    ]
    metrics.sort(
        key=lambda item: (
            item["status"] == "REPLAY_PRIORITY",
            item["directions"][item["bestDirection"]]["recent20"]["netHits"],
            item["directions"][item["bestDirection"]]["outer"]["netHits"],
            item["directions"][item["bestDirection"]]["full"]["netHits"],
        ),
        reverse=True,
    )
    return {
        "profile": profile_key,
        "weightFingerprint": fingerprint,
        "firstRaceDate": races[0][0].get("race_date") if races else None,
        "lastRaceDate": races[-1][0].get("race_date") if races else None,
        "sourceTsMax": max((safe_int(row.get("ts"), 0) for rows in races for row in rows), default=0),
        "rankingVersions": dict(
            sorted(Counter(str(rows[0].get("v4_version")) for rows in races).items())
        ),
        "performance": summarize_performance(races),
        "recent20": summarize_performance(races[-20:]),
        "metricInventoryCount": len(metrics),
        "metricDiagnostics": metrics[:12],
        "replayPriorities": [
            {
                "metric": item["metric"],
                "direction": item["bestDirection"],
                "coverage": item["coverage"],
                "full": item["directions"][item["bestDirection"]]["full"],
                "outer": item["directions"][item["bestDirection"]]["outer"],
                "recent20": item["directions"][item["bestDirection"]]["recent20"],
            }
            for item in metrics
            if item["status"] == "REPLAY_PRIORITY"
        ],
    }


def build_group_report(group: str, races: list[list[dict[str, Any]]]) -> dict[str, Any]:
    performance = summarize_performance(races)
    metrics = [summarize_metric(races, metric) for metric in METRIC_KEYS]
    metrics.sort(
        key=lambda item: (
            item["status"] == "REPLAY_PRIORITY",
            item["directions"][item["bestDirection"]]["recent20"]["netHits"],
            item["directions"][item["bestDirection"]]["outer"]["netHits"],
            item["directions"][item["bestDirection"]]["full"]["netHits"],
        ),
        reverse=True,
    )
    diagnostics = {id(rows): score_diagnostics(rows, bootstrap_iterations=0) or {} for rows in races}
    misses = [rows for rows in races if visible_winner_rank(rows) > 3]
    profile_buckets: dict[tuple[str, str], list[list[dict[str, Any]]]] = defaultdict(list)
    for rows in races:
        fingerprint = weight_fingerprint(rows[0])
        if fingerprint:
            profile_buckets[(profile_of(rows)["subtype"], fingerprint)].append(rows)
    profile_diagnostics = [
        build_profile_report(profile_key, fingerprint, subset)
        for (profile_key, fingerprint), subset in sorted(profile_buckets.items())
    ]
    profile_diagnostics.sort(
        key=lambda item: (-item["performance"]["races"], item["profile"], item["weightFingerprint"])
    )
    latest_source_by_profile = {
        profile_key: max(
            item["sourceTsMax"]
            for item in profile_diagnostics
            if item["profile"] == profile_key
        )
        for profile_key in {item["profile"] for item in profile_diagnostics}
    }
    for item in profile_diagnostics:
        item["latestObservedWeightFingerprint"] = (
            item["sourceTsMax"] == latest_source_by_profile[item["profile"]]
        )
        if not item["latestObservedWeightFingerprint"]:
            item["historicalReplayPriorities"] = item["replayPriorities"]
            item["replayPriorities"] = []
            for metric in item["metricDiagnostics"]:
                if metric["status"] == "REPLAY_PRIORITY":
                    metric["status"] = "HISTORICAL_ONLY"
    return {
        "group": group,
        "rankingVersions": dict(sorted(Counter(str(rows[0].get("v4_version")) for rows in races).items())),
        "performance": performance,
        "recent20": summarize_performance(races[-20:]),
        "outer20Pct": summarize_performance(races[-math.ceil(len(races) * 0.20):]),
        "segments": {
            "profile": partition_summaries(races, lambda rows: profile_of(rows)["subtype"]),
            "track": partition_summaries(races, lambda rows: profile_of(rows)["track"]),
            "field": partition_summaries(races, lambda rows: field_bucket(len(rows))),
            "separation": partition_summaries(
                races,
                lambda rows: diagnostics[id(rows)].get("separationStatus") or "UNKNOWN",
            ),
        },
        "recentMisses": [race_summary(rows) for rows in misses[-20:]],
        "metricDiagnostics": metrics,
        "profileDiagnostics": profile_diagnostics,
        "replayPriorities": [
            {
                "metric": item["metric"],
                "direction": item["bestDirection"],
                "coverage": item["coverage"],
                "full": item["directions"][item["bestDirection"]]["full"],
                "outer": item["directions"][item["bestDirection"]]["outer"],
                "recent20": item["directions"][item["bestDirection"]]["recent20"],
            }
            for item in metrics
            if item["status"] == "REPLAY_PRIORITY"
        ],
    }


def build_report(
    entries: list[dict[str, Any]],
    run_date: str,
    groups: tuple[str, ...] = DEFAULT_GROUPS,
    *,
    invalid_json_lines: int = 0,
) -> dict[str, Any]:
    all_races = group_races(entries)
    compatible = [rows for rows in all_races if calibration_compatible(rows)]
    buckets: dict[str, list[list[dict[str, Any]]]] = defaultdict(list)
    for rows in compatible:
        category = profile_of(rows)["category"]
        if category in groups:
            buckets[category].append(rows)
    group_reports = [build_group_report(group, buckets.get(group, [])) for group in groups]
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
            "targetGroups": list(groups),
        },
        "groups": group_reports,
        "replayPriorityCount": sum(
            len(item["replayPriorities"])
            + sum(len(profile["replayPriorities"]) for profile in item["profileDiagnostics"])
            for item in group_reports
        ),
        "policy": {
            "boundedWeightPoints": BOUNDED_WEIGHT_POINTS,
            "chronologicalOuter": "latest_20_percent",
            "recentWindow": 20,
            "minimumEvidenceRaces": GROUP_MIN_RACES,
            "minimumOuterRaces": GROUP_MIN_OUTER,
            "profileMinimumEvidenceRaces": PROFILE_MIN_RACES,
            "profileMinimumOuterRaces": PROFILE_MIN_OUTER,
            "minimumCoverage": MIN_COVERAGE,
            "minimumNonNeutral": MIN_NON_NEUTRAL,
            "automaticWeightChange": False,
            "priorityMeaning": "full_replay_required_before_prospective_shadow",
        },
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Winner Top3 Failure Diagnostics",
        "",
        f'- Run date: `{report["runDate"]}`',
        f'- Compatible clean races: `{report["input"]["compatibleCleanRaces"]}`',
        f'- Replay priorities: `{report["replayPriorityCount"]}`',
        "- Ranking impact: `false`",
        "",
    ]
    for group in report["groups"]:
        performance = group["performance"]
        recent = group["recent20"]
        lines.extend([
            f'## {group["group"]}',
            "",
            f'- Full WTop3: `{performance["winnerTop3"]}/{performance["races"]}` '
            f'(`{performance["winnerTop3Rate"]}`); misses `{performance["misses"]}`.',
            f'- Recent20 WTop3: `{recent["winnerTop3"]}/{recent["races"]}`; misses `{recent["misses"]}`.',
            "",
            "| Metric | Direction | Full R/D/Net | Outer R/D/Net | Recent R/D/Net | Coverage | Status |",
            "|---|---|---:|---:|---:|---:|---|",
        ])
        for metric in group["metricDiagnostics"][:15]:
            direction = metric["bestDirection"]
            full = metric["directions"][direction]["full"]
            outer = metric["directions"][direction]["outer"]
            recent_window = metric["directions"][direction]["recent20"]
            lines.append(
                f'| {metric["metric"]} | {direction} | '
                f'{full["rescues"]}/{full["damages"]}/{full["netHits"]:+d} | '
                f'{outer["rescues"]}/{outer["damages"]}/{outer["netHits"]:+d} | '
                f'{recent_window["rescues"]}/{recent_window["damages"]}/{recent_window["netHits"]:+d} | '
                f'{metric["coverage"]} | {metric["status"]} |'
            )
        lines.extend(["", "Worst profile clusters:", ""])
        for item in group["segments"]["profile"][:8]:
            lines.append(
                f'- `{item["key"]}`: misses `{item["misses"]}/{item["races"]}`, '
                f'WTop3 `{item["winnerTop3Rate"]}`.'
            )
        profile_priorities = [
            (profile, priority)
            for profile in group["profileDiagnostics"]
            for priority in profile["replayPriorities"]
        ]
        if profile_priorities:
            lines.extend(["", "Profile replay priorities:", ""])
            for profile, priority in profile_priorities[:10]:
                lines.append(
                    f'- `{profile["profile"]}@{profile["weightFingerprint"]}` / '
                    f'`{priority["metric"]}` `{priority["direction"]}`: '
                    f'full net `{priority["full"]["netHits"]:+d}`, '
                    f'outer `{priority["outer"]["netHits"]:+d}`, '
                    f'recent20 `{priority["recent20"]["netHits"]:+d}`.'
                )
        lines.append("")
    lines.extend([
        "## Policy",
        "",
        "- Only integrity-safe, visible v4.21+ races are used.",
        "- Both +2pp and -2pp bounded movements are measured.",
        "- Rescue is never reported without damage and chronological outer checks.",
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


def build_daily_snapshot(report: dict[str, Any]) -> dict[str, Any]:
    groups = []
    for group in report["groups"]:
        groups.append({
            "group": group["group"],
            "performance": group["performance"],
            "recent20": group["recent20"],
            "outer20Pct": group["outer20Pct"],
            "segments": group["segments"],
            "recentMisses": group["recentMisses"],
            "replayPriorities": group["replayPriorities"],
            "topMetricDiagnostics": group["metricDiagnostics"][:12],
            "profileDiagnostics": [
                {
                    "profile": item["profile"],
                    "weightFingerprint": item["weightFingerprint"],
                    "latestObservedWeightFingerprint": item["latestObservedWeightFingerprint"],
                    "firstRaceDate": item["firstRaceDate"],
                    "lastRaceDate": item["lastRaceDate"],
                    "rankingVersions": item["rankingVersions"],
                    "performance": item["performance"],
                    "recent20": item["recent20"],
                    "replayPriorities": item["replayPriorities"],
                    "topMetricDiagnostics": item["metricDiagnostics"][:5],
                }
                for item in group["profileDiagnostics"]
            ],
        })
    return {
        "schemaVersion": report["schemaVersion"],
        "runDate": report["runDate"],
        "sourceSnapshotAt": report["sourceSnapshotAt"],
        "input": report["input"],
        "groups": groups,
        "replayPriorityCount": report["replayPriorityCount"],
        "policy": report["policy"],
        "fullArtifactPath": "automation/failure-diagnostics/latest.json",
    }


def persist(report: dict[str, Any], data_dir: Path) -> dict[str, str]:
    daily_dir = data_dir / "automation" / "runs" / report["runDate"]
    latest_dir = data_dir / "automation" / "failure-diagnostics"
    latest_json_text = json.dumps(
        report,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ) + "\n"
    daily_json_text = json.dumps(
        build_daily_snapshot(report),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ) + "\n"
    markdown = render_markdown(report)
    paths = {
        "dailyJson": daily_dir / "winner-top3-failure-diagnostics.json",
        "dailyMarkdown": daily_dir / "winner-top3-failure-diagnostics.md",
        "latestJson": latest_dir / "latest.json",
        "latestMarkdown": latest_dir / "latest.md",
    }
    for key, path in paths.items():
        if key == "latestJson":
            content = latest_json_text
        elif key == "dailyJson":
            content = daily_json_text
        else:
            content = markdown
        atomic_write(path, content)
    return {key: str(path) for key, path in paths.items()}


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose chronological Winner-Top3 failures.")
    parser.add_argument("--predictions", required=True, type=Path)
    parser.add_argument("--data-dir", required=True, type=Path)
    parser.add_argument("--run-date", default=datetime.now().astimezone().strftime("%Y-%m-%d"))
    parser.add_argument("--groups", default=",".join(DEFAULT_GROUPS))
    arguments = parser.parse_args()
    groups = tuple(item.strip().upper() for item in arguments.groups.split(",") if item.strip())

    entries, invalid_lines = load_jsonl(arguments.predictions)
    report = build_report(
        entries,
        arguments.run_date,
        groups,
        invalid_json_lines=invalid_lines,
    )
    paths = persist(report, arguments.data_dir)
    print(json.dumps({
        "success": True,
        "runDate": report["runDate"],
        "input": report["input"],
        "replayPriorityCount": report["replayPriorityCount"],
        "groups": [
            {
                "group": item["group"],
                "performance": item["performance"],
                "recent20": item["recent20"],
                "replayPriorities": item["replayPriorities"],
            }
            for item in report["groups"]
        ],
        "paths": paths,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
