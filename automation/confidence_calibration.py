"""Build chronological Winner-Top3 confidence calibration candidates.

The artifact is analysis-only. Temperatures change displayed confidence only;
they never change v4 scores, ranks, metric weights, or Telegram ordering.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from automation.metric_signal_registry import (
        classify_race,
        group_races,
        load_jsonl,
        profile_of,
        rounded,
        safe_float,
        safe_int,
    )
    from automation.metric_signal_replay import TEMPERATURE_GRID, calibration_metrics, version_tuple
except ModuleNotFoundError:  # direct: python automation/confidence_calibration.py
    from metric_signal_registry import (
        classify_race,
        group_races,
        load_jsonl,
        profile_of,
        rounded,
        safe_float,
        safe_int,
    )
    from metric_signal_replay import TEMPERATURE_GRID, calibration_metrics, version_tuple


SCHEMA_VERSION = "confidence-calibration-v1"
MIN_COMPATIBLE_VERSION = (4, 21)
BASELINE_TEMPERATURE = 18.0
LOGIT_BIAS_GRID = tuple(round(-3.0 + index * 0.25, 2) for index in range(25))
GROUP_MIN_RACES = 60
GROUP_MIN_OUTER = 12
PROFILE_MIN_RACES = 30
PROFILE_MIN_OUTER = 6


def weight_fingerprint(row: dict[str, Any]) -> str | None:
    raw = row.get("v4_weights") or {}
    if not isinstance(raw, dict):
        return None
    normalized = {}
    for metric, value in sorted(raw.items()):
        numeric = safe_float(value)
        if numeric is not None and numeric > 0.0:
            normalized[str(metric)] = round(numeric, 3)
    if not normalized:
        return None
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def calibration_compatible(rows: list[dict[str, Any]]) -> bool:
    if classify_race(rows) != "fully_labeled":
        return False
    versions = {version_tuple(row.get("v4_version")) for row in rows}
    if len(versions) != 1 or next(iter(versions), ()) < MIN_COMPATIBLE_VERSION:
        return False
    if not all(row.get("v4_applied_for_ranking") is True for row in rows):
        return False
    if weight_fingerprint(rows[0]) is None:
        return False
    return all(safe_float(row.get("v4_score")) is not None for row in rows)


def split_60_20_20(races: list[list[dict[str, Any]]]) -> dict[str, list[list[dict[str, Any]]]]:
    count = len(races)
    if not count:
        return {"build": [], "inner": [], "outer": []}
    build_end = max(1, math.floor(count * 0.60))
    inner_end = min(count, max(build_end + 1, math.floor(count * 0.80)))
    return {
        "build": races[:build_end],
        "inner": races[build_end:inner_end],
        "outer": races[inner_end:],
    }


def _winner_index(rows: list[dict[str, Any]]) -> int | None:
    return next(
        (index for index, row in enumerate(rows) if safe_int(row.get("finish_pos"), 0) == 1),
        None,
    )


def _apply_logit_bias(probability: float, bias: float) -> float:
    clipped = max(1e-8, min(1.0 - 1e-8, probability))
    logit = math.log(clipped / (1.0 - clipped))
    return 1.0 / (1.0 + math.exp(-(logit + bias)))


def race_confidence_event(
    rows: list[dict[str, Any]],
    temperature: float,
    top3_logit_bias: float = 0.0,
) -> dict[str, Any] | None:
    winner_index = _winner_index(rows)
    if winner_index is None:
        return None
    scores = [safe_float(row.get("v4_score")) for row in rows]
    if any(score is None for score in scores):
        return None
    numeric = [float(score) for score in scores if score is not None]
    peak = max(numeric)
    exponentials = [math.exp((score - peak) / max(temperature, 0.1)) for score in numeric]
    total = sum(exponentials) or 1.0
    win_probabilities = [value / total for value in exponentials]
    ordered_indices = sorted(
        range(len(rows)),
        key=lambda index: (
            -numeric[index],
            safe_int(rows[index].get("rank_pred"), 999),
            str(rows[index].get("horse_name") or ""),
        ),
    )
    top3_indices = set(ordered_indices[:3])
    top1_index = ordered_indices[0]
    raw_top3_probability = sum(win_probabilities[index] for index in top3_indices)
    return {
        "winnerTop3Probability": _apply_logit_bias(raw_top3_probability, top3_logit_bias),
        "rawWinnerTop3Probability": raw_top3_probability,
        "winnerTop3Label": int(winner_index in top3_indices),
        "top1Probability": win_probabilities[top1_index],
        "top1Label": int(winner_index == top1_index),
    }


def event_rows(
    races: list[list[dict[str, Any]]],
    temperature: float,
    target: str,
    top3_logit_bias: float = 0.0,
) -> list[tuple[float, int]]:
    values = []
    probability_key = "winnerTop3Probability" if target == "winnerTop3" else "top1Probability"
    label_key = "winnerTop3Label" if target == "winnerTop3" else "top1Label"
    for rows in races:
        event = race_confidence_event(rows, temperature, top3_logit_bias)
        if event:
            values.append((float(event[probability_key]), int(event[label_key])))
    return values


def fit_calibrator(races: list[list[dict[str, Any]]]) -> dict[str, float]:
    if not races:
        return {"temperature": BASELINE_TEMPERATURE, "top3LogitBias": 0.0}
    scored = []
    for temperature in TEMPERATURE_GRID:
        for bias in LOGIT_BIAS_GRID:
            metrics = calibration_metrics(
                event_rows(races, temperature, "winnerTop3", bias),
                bins=5,
            )
            scored.append((
                safe_float(metrics.get("logLoss"), 999.0) or 999.0,
                safe_float(metrics.get("brier"), 999.0) or 999.0,
                abs(bias),
                temperature,
                bias,
            ))
    _, _, _, temperature, bias = min(scored)
    return {"temperature": temperature, "top3LogitBias": bias}


def evaluate_calibrator(
    races: list[list[dict[str, Any]]],
    temperature: float,
    top3_logit_bias: float = 0.0,
) -> dict[str, Any]:
    winner_top3 = calibration_metrics(
        event_rows(races, temperature, "winnerTop3", top3_logit_bias),
        bins=5,
    )
    top1 = calibration_metrics(event_rows(races, temperature, "top1"), bins=5)
    winner_hits = sum(
        label for _, label in event_rows(races, temperature, "winnerTop3", top3_logit_bias)
    )
    top1_hits = sum(label for _, label in event_rows(races, temperature, "top1"))
    return {
        "races": len(races),
        "winnerTop3Hits": winner_hits,
        "winnerTop3Rate": rounded(winner_hits / len(races), 4) if races else None,
        "top1Hits": top1_hits,
        "top1Rate": rounded(top1_hits / len(races), 4) if races else None,
        "winnerTop3Calibration": winner_top3,
        "top1Calibration": top1,
    }


def metric_delta(candidate: dict[str, Any], baseline: dict[str, Any], key: str) -> float | None:
    candidate_value = safe_float(candidate.get(key))
    baseline_value = safe_float(baseline.get(key))
    if candidate_value is None or baseline_value is None:
        return None
    return rounded(candidate_value - baseline_value, 5)


def compare_calibration(
    races: list[list[dict[str, Any]]],
    candidate_temperature: float,
    candidate_top3_logit_bias: float,
) -> dict[str, Any]:
    baseline = evaluate_calibrator(races, BASELINE_TEMPERATURE, 0.0)
    candidate = evaluate_calibrator(
        races,
        candidate_temperature,
        candidate_top3_logit_bias,
    )
    return {
        "baselineTemperature": BASELINE_TEMPERATURE,
        "candidateTemperature": candidate_temperature,
        "baselineTop3LogitBias": 0.0,
        "candidateTop3LogitBias": candidate_top3_logit_bias,
        "baseline": baseline,
        "candidate": candidate,
        "delta": {
            "winnerTop3Brier": metric_delta(
                candidate["winnerTop3Calibration"], baseline["winnerTop3Calibration"], "brier"
            ),
            "winnerTop3LogLoss": metric_delta(
                candidate["winnerTop3Calibration"], baseline["winnerTop3Calibration"], "logLoss"
            ),
            "winnerTop3Ece": metric_delta(
                candidate["winnerTop3Calibration"], baseline["winnerTop3Calibration"], "ece"
            ),
            "top1Brier": metric_delta(candidate["top1Calibration"], baseline["top1Calibration"], "brier"),
            "top1LogLoss": metric_delta(
                candidate["top1Calibration"], baseline["top1Calibration"], "logLoss"
            ),
            "top1Ece": metric_delta(candidate["top1Calibration"], baseline["top1Calibration"], "ece"),
        },
    }


def _check(checks: list[dict[str, Any]], name: str, passed: bool, detail: str) -> None:
    checks.append({"name": name, "passed": bool(passed), "detail": detail})


def evaluate_scope(
    scope_type: str,
    scope_key: str,
    races: list[list[dict[str, Any]]],
    *,
    fingerprint: str | None = None,
) -> dict[str, Any]:
    split = split_60_20_20(races)
    calibrator = fit_calibrator(split["build"])
    temperature = calibrator["temperature"]
    top3_logit_bias = calibrator["top3LogitBias"]
    inner = compare_calibration(split["inner"], temperature, top3_logit_bias)
    outer = compare_calibration(split["outer"], temperature, top3_logit_bias)
    full = compare_calibration(races, temperature, top3_logit_bias)
    min_races = GROUP_MIN_RACES if scope_type == "GROUP" else PROFILE_MIN_RACES
    min_outer = GROUP_MIN_OUTER if scope_type == "GROUP" else PROFILE_MIN_OUTER
    checks: list[dict[str, Any]] = []
    _check(checks, "clean_race_threshold", len(races) >= min_races, f"races={len(races)}/{min_races}")
    _check(
        checks,
        "untouched_outer_threshold",
        len(split["outer"]) >= min_outer,
        f'races={len(split["outer"])}/{min_outer}',
    )
    outer_candidate = outer["candidate"]["winnerTop3Calibration"]
    outer_delta = outer["delta"]
    inner_delta = inner["delta"]
    outer_ece = safe_float(outer_candidate.get("ece"))
    _check(
        checks,
        "inner_winner_top3_brier_no_material_regression",
        inner_delta["winnerTop3Brier"] is not None and inner_delta["winnerTop3Brier"] <= 0.005,
        f'delta={inner_delta["winnerTop3Brier"]}',
    )
    _check(
        checks,
        "outer_winner_top3_brier_no_material_regression",
        outer_delta["winnerTop3Brier"] is not None and outer_delta["winnerTop3Brier"] <= 0.005,
        f'delta={outer_delta["winnerTop3Brier"]}',
    )
    _check(
        checks,
        "outer_winner_top3_logloss_no_material_regression",
        outer_delta["winnerTop3LogLoss"] is not None and outer_delta["winnerTop3LogLoss"] <= 0.01,
        f'delta={outer_delta["winnerTop3LogLoss"]}',
    )
    _check(
        checks,
        "outer_winner_top3_ece_at_most_10pct",
        outer_ece is not None and outer_ece <= 0.10,
        f"ece={outer_ece}",
    )
    _check(
        checks,
        "outer_top1_brier_no_material_regression",
        outer_delta["top1Brier"] is not None and outer_delta["top1Brier"] <= 0.01,
        f'delta={outer_delta["top1Brier"]}',
    )
    outcome_labels = [
        label
        for _, label in event_rows(
            split["outer"],
            temperature,
            "winnerTop3",
            top3_logit_bias,
        )
    ]
    _check(
        checks,
        "outer_contains_hits_and_misses",
        bool(outcome_labels) and len(set(outcome_labels)) == 2,
        f"hits={sum(outcome_labels)}/{len(outcome_labels)}",
    )
    sample_ready = all(item["passed"] for item in checks[:2])
    quality_ready = all(item["passed"] for item in checks[2:])
    if sample_ready and quality_ready:
        status = "READY_FOR_RUNTIME_SHADOW"
    elif not sample_ready and not quality_ready:
        status = "HOLD_SAMPLE_AND_QUALITY"
    elif not sample_ready:
        status = "HOLD_SAMPLE"
    else:
        status = "HOLD_QUALITY"
    remaining_races = max(0, min_races - len(races))
    remaining_outer = max(0, min_outer - len(split["outer"]))
    versions = Counter(str(rows[0].get("v4_version") or "unknown") for rows in races)
    fingerprints = Counter(weight_fingerprint(rows[0]) or "missing" for rows in races)
    quality = "RED"
    if outer_ece is not None:
        quality = "GREEN" if outer_ece <= 0.05 else "YELLOW" if outer_ece <= 0.10 else "RED"
    return {
        "modelId": f"{scope_type}:{scope_key}" + (f"@{fingerprint}" if fingerprint else ""),
        "scopeType": scope_type,
        "scopeKey": scope_key,
        "weightFingerprint": fingerprint,
        "races": len(races),
        "splitRaces": {key: len(value) for key, value in split.items()},
        "rankingVersions": dict(sorted(versions.items())),
        "weightFingerprints": dict(sorted(fingerprints.items())),
        "temperatureFit": {
            "fitSplit": "build",
            "objective": "winner_top3_set_logloss",
            "baseline": BASELINE_TEMPERATURE,
            "candidate": temperature,
            "top3LogitBias": top3_logit_bias,
        },
        "comparisons": {"full": full, "inner": inner, "outer": outer},
        "calibrationQuality": quality,
        "status": status,
        "checks": checks,
        "thresholds": {"cleanRaces": min_races, "outerRaces": min_outer},
        "remaining": {
            "cleanRaces": remaining_races,
            "outerRaces": remaining_outer,
            "nextReviewAfterNewCleanRaces": max(
                remaining_races,
                remaining_outer,
                5 if not quality_ready else 1,
            ),
        },
        "runtimeEligible": False,
        "rankingImpact": False,
    }


def build_report(
    entries: list[dict[str, Any]],
    run_date: str,
    *,
    invalid_json_lines: int = 0,
) -> dict[str, Any]:
    all_races = group_races(entries)
    states = Counter(classify_race(rows) for rows in all_races)
    compatible = [rows for rows in all_races if calibration_compatible(rows)]
    group_buckets: dict[str, list[list[dict[str, Any]]]] = defaultdict(list)
    profile_buckets: dict[tuple[str, str], list[list[dict[str, Any]]]] = defaultdict(list)
    for rows in compatible:
        profile = profile_of(rows)
        group_buckets[profile["category"]].append(rows)
        fingerprint = weight_fingerprint(rows[0])
        if fingerprint:
            profile_buckets[(profile["subtype"], fingerprint)].append(rows)

    scopes = []
    for key, races in sorted(group_buckets.items()):
        scopes.append(evaluate_scope("GROUP", key, races))
    for (key, fingerprint), races in sorted(profile_buckets.items()):
        scopes.append(evaluate_scope("PROFILE", key, races, fingerprint=fingerprint))

    source_timestamp = max((safe_int(row.get("ts"), 0) for row in entries), default=0)
    status_counts = Counter(item["status"] for item in scopes)
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
            "fullyLabeledRaces": states.get("fully_labeled", 0),
            "compatibleCleanRaces": len(compatible),
            "raceStates": dict(sorted(states.items())),
        },
        "statusCounts": dict(sorted(status_counts.items())),
        "scopes": scopes,
        "runtimeShadowCandidates": [
            {
                "modelId": item["modelId"],
                "temperature": item["temperatureFit"]["candidate"],
                "top3LogitBias": item["temperatureFit"]["top3LogitBias"],
                "outerEce": item["comparisons"]["outer"]["candidate"]["winnerTop3Calibration"]["ece"],
                "races": item["races"],
            }
            for item in scopes
            if item["status"] == "READY_FOR_RUNTIME_SHADOW"
        ],
        "policy": {
            "compatibleMinimumVersion": ".".join(str(value) for value in MIN_COMPATIBLE_VERSION),
            "profileModelsRequireExactWeightFingerprint": True,
            "chronologicalSplit": "60/20/20",
            "temperatureFitSplit": "build_only",
            "outerIsUntouched": True,
            "groupMinimumRaces": GROUP_MIN_RACES,
            "groupMinimumOuterRaces": GROUP_MIN_OUTER,
            "profileMinimumRaces": PROFILE_MIN_RACES,
            "profileMinimumOuterRaces": PROFILE_MIN_OUTER,
            "rankingImpact": False,
            "promotion": "artifact_only_then_runtime_shadow",
        },
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Winner Top3 Confidence Calibration",
        "",
        f'- Run date: `{report["runDate"]}`',
        f'- Full clean races: `{report["input"]["fullyLabeledRaces"]}`',
        f'- Compatible clean races: `{report["input"]["compatibleCleanRaces"]}`',
        f'- Status: `{report["statusCounts"]}`',
        "- Ranking impact: `false`",
        "",
        "| Scope | Races | Build/Inner/Outer | T / bias | Outer WTop3 | Outer ECE | Brier Δ | Status |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for item in report["scopes"]:
        outer = item["comparisons"]["outer"]
        candidate = outer["candidate"]
        lines.append(
            "| {model} | {races} | {build}/{inner}/{outer_count} | {temperature} / {bias} | {hits}/{count} | {ece} | {brier} | {status} |".format(
                model=item["modelId"],
                races=item["races"],
                build=item["splitRaces"]["build"],
                inner=item["splitRaces"]["inner"],
                outer_count=item["splitRaces"]["outer"],
                temperature=item["temperatureFit"]["candidate"],
                bias=item["temperatureFit"]["top3LogitBias"],
                hits=candidate["winnerTop3Hits"],
                count=candidate["races"],
                ece=candidate["winnerTop3Calibration"]["ece"],
                brier=outer["delta"]["winnerTop3Brier"],
                status=item["status"],
            )
        )
    lines.extend([
        "",
        "## Policy",
        "",
        "- Only fully labelled, integrity-safe races are used.",
        "- Temperature and Winner-Top3 logit bias are fitted on the chronological build split only.",
        "- Profile models require the exact logged weight fingerprint.",
        "- The outer split is untouched and must contain both Top3 hits and misses.",
        "- READY only permits runtime shadow observation; no ranking or probability display changes here.",
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
    latest_dir = data_dir / "automation" / "confidence-calibration"
    json_text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    markdown = render_markdown(report)
    paths = {
        "dailyJson": daily_dir / "confidence-calibration.json",
        "dailyMarkdown": daily_dir / "confidence-calibration.md",
        "latestJson": latest_dir / "latest.json",
        "latestMarkdown": latest_dir / "latest.md",
    }
    for key, path in paths.items():
        atomic_write(path, json_text if key.endswith("Json") else markdown)
    return {key: str(path) for key, path in paths.items()}


def main() -> int:
    parser = argparse.ArgumentParser(description="Build chronological Winner Top3 confidence calibration.")
    parser.add_argument("--predictions", required=True, type=Path)
    parser.add_argument("--data-dir", required=True, type=Path)
    parser.add_argument("--run-date", default=datetime.now().astimezone().strftime("%Y-%m-%d"))
    arguments = parser.parse_args()

    entries, invalid_lines = load_jsonl(arguments.predictions)
    report = build_report(entries, arguments.run_date, invalid_json_lines=invalid_lines)
    paths = persist(report, arguments.data_dir)
    print(json.dumps({
        "success": True,
        "runDate": report["runDate"],
        "input": report["input"],
        "statusCounts": report["statusCounts"],
        "runtimeShadowCandidates": report["runtimeShadowCandidates"],
        "paths": paths,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
