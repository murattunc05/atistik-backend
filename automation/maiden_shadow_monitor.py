"""Live rollback checkpoints for the bounded MAIDEN v4 + 15% no-AGF ML overlay."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

try:
    from automation.sart1_shadow_monitor import (
        atomic_write,
        load_jsonl,
        parse_race_date,
        race_sort_key,
        ranking_guardrails,
        safe_float,
        safe_int,
    )
except ModuleNotFoundError as exc:
    if exc.name != "automation":
        raise
    from sart1_shadow_monitor import (  # type: ignore[no-redef]
        atomic_write,
        load_jsonl,
        parse_race_date,
        race_sort_key,
        ranking_guardrails,
        safe_float,
        safe_int,
    )


CHECKPOINT_RACES = 5
REVIEW_RACES = 15
EXPECTED_ALPHA = 0.15
EXPECTED_CANDIDATE_VERSION = "maiden-ml15-20260823-v2"
EXPECTED_MODEL_VERSION = "maiden-live-20260822-v2"
EXPECTED_MODEL_SHA256 = "dc58166972df6b39fd7c01f7b5d173576915b8c1bf6fb5368053e8a8e7c1f29a"
EXPECTED_FEATURE_SCHEMA_SHA256 = (
    "5267a4de81de7e97ce46556be96af9041816baec0770de180631bc87c52fbae0"
)
EXPECTED_TRAINING_CUTOFF = "10.08.2026"
EXPECTED_BASELINE_VERSION = "4.25"
EXPECTED_OBSERVATION_START = "23.08.2026"
TERMINAL_FINISH_POSITIONS = {99}
ISTANBUL_TZ = ZoneInfo("Europe/Istanbul")


def finite_number(row: dict[str, Any], key: str) -> float | None:
    try:
        value = float(row.get(key))
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def expected_alpha(row: dict[str, Any]) -> bool:
    value = finite_number(row, "maiden_candidate_alpha")
    return value is not None and abs(value - EXPECTED_ALPHA) <= 1e-9


def minmax_100(values: list[float]) -> list[float]:
    if not values:
        return []
    low, high = min(values), max(values)
    if high - low <= 1e-12:
        return [50.0 for _ in values]
    return [(value - low) * 100.0 / (high - low) for value in values]


def candidate_payload_valid(rows: list[dict[str, Any]]) -> bool:
    vector_hashes = [
        str(row.get("maiden_candidate_feature_vector_sha256") or "").lower()
        for row in rows
    ]
    if any(
        len(value) != 64 or any(char not in "0123456789abcdef" for char in value)
        for value in vector_hashes
    ):
        return False
    keys = (
        "maiden_candidate_baseline_score",
        "maiden_candidate_baseline_component",
        "maiden_candidate_ml_raw_score",
        "maiden_candidate_ml_component",
        "maiden_candidate_score",
    )
    parsed = {
        key: [finite_number(row, key) for row in rows]
        for key in keys
    }
    if any(value is None for values in parsed.values() for value in values):
        return False

    baseline_scores = [float(value) for value in parsed["maiden_candidate_baseline_score"]]
    baseline_components = [
        float(value) for value in parsed["maiden_candidate_baseline_component"]
    ]
    ml_raw_scores = [float(value) for value in parsed["maiden_candidate_ml_raw_score"]]
    ml_components = [float(value) for value in parsed["maiden_candidate_ml_component"]]
    candidate_scores = [float(value) for value in parsed["maiden_candidate_score"]]
    expected_baseline = minmax_100(baseline_scores)
    expected_ml = minmax_100(ml_raw_scores)
    if any(
        abs(actual - expected) > 1e-3
        for actual, expected in zip(baseline_components, expected_baseline)
    ):
        return False
    if any(
        abs(actual - expected) > 1e-3
        for actual, expected in zip(ml_components, expected_ml)
    ):
        return False
    if any(
        abs(candidate - ((1.0 - EXPECTED_ALPHA) * baseline + EXPECTED_ALPHA * ml))
        > 1e-3
        for candidate, baseline, ml in zip(
            candidate_scores, baseline_components, ml_components
        )
    ):
        return False

    baseline_order = sorted(
        range(len(rows)),
        key=lambda idx: safe_int(rows[idx].get("maiden_candidate_baseline_rank"), 0),
    )
    if any(
        baseline_scores[left] + 1e-9 < baseline_scores[right]
        for left, right in zip(baseline_order, baseline_order[1:])
    ):
        return False
    candidate_order = sorted(
        range(len(rows)),
        key=lambda idx: (-candidate_scores[idx], safe_int(
            rows[idx].get("maiden_candidate_baseline_rank"), 0
        )),
    )
    expected_candidate_ranks = {
        idx: rank + 1 for rank, idx in enumerate(candidate_order)
    }
    return all(
        safe_int(row.get("maiden_candidate_rank"), 0) == expected_candidate_ranks[idx]
        for idx, row in enumerate(rows)
    )


def candidate_identity_valid(rows: list[dict[str, Any]]) -> bool:
    identity_keys = (
        "race_date",
        "race_id",
        "race_no",
        "race_time",
        "city",
        "city_id",
        "race_type",
        "track",
        "distance",
        "field_size",
        "maiden_candidate_version",
        "maiden_candidate_observation_start",
        "maiden_candidate_created_ts",
        "maiden_candidate_model_version",
        "maiden_candidate_model_sha256",
        "maiden_candidate_feature_schema_hash",
        "maiden_candidate_training_cutoff",
        "maiden_candidate_baseline_version",
    )
    for key in identity_keys:
        values = {
            str(row.get(key) if row.get(key) is not None else "").strip()
            for row in rows
        }
        if len(values) != 1 or not next(iter(values)):
            return False

    if any(
        str(row.get("maiden_candidate_mode") or "") != "controlled_live_bounded"
        or str(row.get("maiden_candidate_version") or "") != EXPECTED_CANDIDATE_VERSION
        or str(row.get("maiden_candidate_observation_start") or "")
        != EXPECTED_OBSERVATION_START
        or str(row.get("maiden_candidate_model_version") or "") != EXPECTED_MODEL_VERSION
        or str(row.get("maiden_candidate_model_sha256") or "") != EXPECTED_MODEL_SHA256
        or str(row.get("maiden_candidate_feature_schema_hash") or "")
        != EXPECTED_FEATURE_SCHEMA_SHA256
        or str(row.get("maiden_candidate_training_cutoff") or "")
        != EXPECTED_TRAINING_CUTOFF
        or str(row.get("maiden_candidate_baseline_version") or "")
        != EXPECTED_BASELINE_VERSION
        or not bool(row.get("maiden_candidate_used_for_ranking"))
        or not bool(row.get("maiden_candidate_rollout_eligible"))
        or not bool(row.get("maiden_candidate_telegram_visible"))
        or not bool(row.get("maiden_candidate_strict_no_agf_ml"))
        or not bool(row.get("maiden_candidate_v4_score_faithful"))
        or not expected_alpha(row)
        for row in rows
    ):
        return False

    horse_names = [str(row.get("horse_name") or "").strip().casefold() for row in rows]
    if any(not name for name in horse_names) or len(set(horse_names)) != len(horse_names):
        return False
    if not candidate_payload_valid(rows):
        return False

    race_day = parse_race_date(rows[0].get("race_date"))
    race_time = str(rows[0].get("race_time") or "").strip().replace(".", ":")
    created_ts = safe_int(rows[0].get("maiden_candidate_created_ts"), 0)
    if race_day is None or created_ts <= 0:
        return False
    try:
        hour, minute = (int(part) for part in race_time.split(":", 1))
        race_start = race_day.replace(hour=hour, minute=minute, tzinfo=ISTANBUL_TZ)
    except (TypeError, ValueError):
        return False
    return created_ts < int(race_start.timestamp())


def classify_race(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "unlabeled"
    expected = safe_int(rows[0].get("field_size"), len(rows))
    labels = [safe_int(row.get("finish_pos"), 0) for row in rows]
    labeled = sum(value > 0 for value in labels)
    if labeled == 0:
        return "unlabeled"
    if labeled != len(rows) or expected != len(rows):
        return "partial"
    if labels.count(1) != 1:
        return "integrity_invalid"

    ranked = [value for value in labels if value not in TERMINAL_FINISH_POSITIONS]
    if any(value < 1 or value > expected for value in ranked):
        return "integrity_invalid"
    expected_rank = 1
    for rank, tied_count in sorted(Counter(ranked).items()):
        if rank != expected_rank:
            return "integrity_invalid"
        expected_rank += tied_count

    valid_ranks = list(range(1, len(rows) + 1))
    visible = [safe_int(row.get("maiden_candidate_baseline_rank"), 0) for row in rows]
    candidate = [safe_int(row.get("maiden_candidate_rank"), 0) for row in rows]
    if sorted(visible) != valid_ranks or sorted(candidate) != valid_ranks:
        return "integrity_invalid"
    return "fully_labeled"


def normalized_boundary(values: list[float]) -> dict[str, float | int | None]:
    ordered = sorted((float(value) for value in values), reverse=True)
    if len(ordered) < 4:
        return {"gap": None, "cutoffCrowd": len(ordered)}
    score_range = ordered[0] - ordered[-1]
    if score_range <= 1e-12:
        return {"gap": 0.0, "cutoffCrowd": len(ordered)}
    normalized = [(value - ordered[-1]) / score_range for value in ordered]
    cutoff = normalized[2]
    return {
        "gap": round(max(0.0, cutoff - normalized[3]), 6),
        "cutoffCrowd": sum(abs(value - cutoff) <= 0.10 for value in normalized),
    }


def summarize_races(races: list[dict[str, Any]]) -> dict[str, Any]:
    visible_ranks = [race["visibleWinnerRank"] for race in races]
    candidate_ranks = [race["candidateWinnerRank"] for race in races]

    def metrics(ranks: list[int], guardrail_key: str) -> dict[str, Any]:
        guardrails = [race[guardrail_key] for race in races]

        def average(key: str) -> float | None:
            values = [item[key] for item in guardrails if item.get(key) is not None]
            return round(statistics.mean(values), 4) if values else None

        return {
            "top1": sum(rank == 1 for rank in ranks),
            "winnerTop3": sum(rank <= 3 for rank in ranks),
            "winnerTop5": sum(rank <= 5 for rank in ranks),
            "avgWinnerRank": round(statistics.mean(ranks), 3) if ranks else None,
            "mae": average("mae"),
            "rho": average("rho"),
            "ndcg5": average("ndcg5"),
        }

    visible = metrics(visible_ranks, "visibleGuardrails")
    candidate = metrics(candidate_ranks, "candidateGuardrails")
    rescues = sum(v > 3 and c <= 3 for v, c in zip(visible_ranks, candidate_ranks))
    damages = sum(v <= 3 and c > 3 for v, c in zip(visible_ranks, candidate_ranks))
    baseline_gaps = [race["baselineBoundary"]["gap"] for race in races]
    candidate_gaps = [race["candidateBoundary"]["gap"] for race in races]
    baseline_gaps = [value for value in baseline_gaps if value is not None]
    candidate_gaps = [value for value in candidate_gaps if value is not None]
    baseline_gap = statistics.median(baseline_gaps) if baseline_gaps else None
    candidate_gap = statistics.median(candidate_gaps) if candidate_gaps else None
    baseline_crowd = statistics.median(
        race["baselineBoundary"]["cutoffCrowd"] for race in races
    ) if races else None
    candidate_crowd = statistics.median(
        race["candidateBoundary"]["cutoffCrowd"] for race in races
    ) if races else None
    return {
        "races": len(races),
        "visible": visible,
        "candidate": candidate,
        "rescues": rescues,
        "damages": damages,
        "winnerTop3Net": rescues - damages,
        "top1Net": candidate["top1"] - visible["top1"],
        "baselineBoundaryGapMedian": round(baseline_gap, 6) if baseline_gap is not None else None,
        "candidateBoundaryGapMedian": round(candidate_gap, 6) if candidate_gap is not None else None,
        "boundaryGapRatio": (
            round(candidate_gap / baseline_gap, 4)
            if baseline_gap is not None and baseline_gap > 1e-12 and candidate_gap is not None
            else None
        ),
        "baselineCutoffCrowdMedian": baseline_crowd,
        "candidateCutoffCrowdMedian": candidate_crowd,
    }


def checkpoint_pass(summary: dict[str, Any]) -> bool:
    return bool(
        summary["winnerTop3Net"] >= 0
        and summary["damages"] == 0
        and summary["top1Net"] >= -1
        and summary["boundaryGapRatio"] is not None
        and summary["boundaryGapRatio"] >= 0.90
        and summary["candidateCutoffCrowdMedian"]
        <= summary["baselineCutoffCrowdMedian"] + 1
    )


def build_report(entries: list[dict[str, Any]], run_date: str) -> dict[str, Any]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        version = str(entry.get("maiden_candidate_version") or "").strip()
        if version == EXPECTED_CANDIDATE_VERSION:
            grouped[(
                str(entry.get("race_date") or ""),
                str(entry.get("race_id") or ""),
                version,
            )].append(entry)

    coverage = {
        "fullyLabeledRaces": 0,
        "partialRaces": 0,
        "unlabeledRaces": 0,
        "integrityInvalidRaces": 0,
        "preProspectiveExcludedRaces": 0,
    }
    complete = []
    versions, model_hashes, schema_hashes = set(), set(), set()
    for ((_, _, version), rows) in sorted(grouped.items(), key=lambda item: race_sort_key(item[1])):
        versions.add(version)
        model_hashes.add(str(rows[0].get("maiden_candidate_model_sha256") or ""))
        schema_hashes.add(str(rows[0].get("maiden_candidate_feature_schema_hash") or ""))
        race_day = parse_race_date(rows[0].get("race_date"))
        start_day = parse_race_date(rows[0].get("maiden_candidate_observation_start"))
        if race_day is None or start_day is None:
            coverage["integrityInvalidRaces"] += 1
            continue
        if race_day < start_day:
            coverage["preProspectiveExcludedRaces"] += 1
            continue
        if not candidate_identity_valid(rows):
            coverage["integrityInvalidRaces"] += 1
            continue
        state = classify_race(rows)
        if state == "partial":
            coverage["partialRaces"] += 1
            continue
        if state == "unlabeled":
            coverage["unlabeledRaces"] += 1
            continue
        if state != "fully_labeled":
            coverage["integrityInvalidRaces"] += 1
            continue
        coverage["fullyLabeledRaces"] += 1
        winner = next(row for row in rows if safe_int(row.get("finish_pos"), 0) == 1)
        complete.append({
            "raceId": str(winner.get("race_id") or ""),
            "raceDate": winner.get("race_date"),
            "raceNo": winner.get("race_no"),
            "raceTime": winner.get("race_time"),
            "city": winner.get("city"),
            "cityId": winner.get("city_id"),
            "raceType": winner.get("race_type"),
            "track": winner.get("track"),
            "distance": winner.get("distance"),
            "fieldSize": len(rows),
            "winner": winner.get("horse_name"),
            "candidateVersion": version,
            "modelVersion": winner.get("maiden_candidate_model_version"),
            "modelSha256": winner.get("maiden_candidate_model_sha256"),
            "featureSchemaHash": winner.get("maiden_candidate_feature_schema_hash"),
            "trainingCutoff": winner.get("maiden_candidate_training_cutoff"),
            "alpha": winner.get("maiden_candidate_alpha"),
            "createdTs": safe_int(winner.get("maiden_candidate_created_ts"), 0),
            "visibleWinnerRank": safe_int(winner.get("maiden_candidate_baseline_rank")),
            "candidateWinnerRank": safe_int(winner.get("maiden_candidate_rank")),
            "visibleGuardrails": ranking_guardrails(rows, "maiden_candidate_baseline_rank"),
            "candidateGuardrails": ranking_guardrails(rows, "maiden_candidate_rank"),
            "baselineBoundary": normalized_boundary([
                safe_float(row.get("maiden_candidate_baseline_score"), 0.0) for row in rows
            ]),
            "candidateBoundary": normalized_boundary([
                safe_float(row.get("maiden_candidate_score"), 0.0) for row in rows
            ]),
        })

    complete.sort(key=lambda race: (
        parse_race_date(race["raceDate"]) or datetime.min,
        safe_int(race["raceNo"], 0),
        race["raceId"],
    ))
    cumulative = summarize_races(complete)
    checkpoints = []
    for end in range(CHECKPOINT_RACES, len(complete) + 1, CHECKPOINT_RACES):
        summary = summarize_races(complete[:end])
        checkpoints.append({
            "index": len(checkpoints) + 1,
            "startRace": 1,
            "endRace": end,
            **summary,
            "passed": checkpoint_pass(summary),
        })
    last_three_pass = len(checkpoints) >= 3 and all(
        checkpoint["passed"] for checkpoint in checkpoints[-3:]
    )
    live_health_supported = bool(
        len(complete) >= REVIEW_RACES
        and last_three_pass
        and cumulative["winnerTop3Net"] >= 1
        and cumulative["damages"] == 0
        and cumulative["top1Net"] >= -1
        and cumulative["boundaryGapRatio"] is not None
        and cumulative["boundaryGapRatio"] >= 0.90
        and cumulative["candidateCutoffCrowdMedian"]
        <= cumulative["baselineCutoffCrowdMedian"] + 1
    )
    regression_signal = bool(
        checkpoints and (
            checkpoints[-1]["winnerTop3Net"] < 0
            or checkpoints[-1]["damages"] > 0
        )
    )
    if len(complete) < CHECKPOINT_RACES:
        status = "LIVE_COLLECTING"
    elif regression_signal:
        status = "ROLLBACK_REVIEW"
    elif len(complete) < REVIEW_RACES:
        status = "LIVE_EARLY_HEALTHY"
    elif live_health_supported:
        status = "LIVE_HEALTHY"
    else:
        status = "LIVE_REVIEW"
    return {
        "runDate": run_date,
        "mode": "controlled_live_rollback_monitor",
        "status": status,
        "candidateVersions": sorted(versions),
        "modelHashes": sorted(value for value in model_hashes if value),
        "featureSchemaHashes": sorted(value for value in schema_hashes if value),
        "coverage": coverage,
        "cumulative": cumulative,
        "checkpoints": checkpoints,
        "regressionSignal": regression_signal,
        "formalReplaySupported": live_health_supported,
        "liveRolloutEligible": True,
        "liveRolloutReason": (
            "MAIDEN v4.25 + 15% strict no-AGF ML is live; +5/+10/+15 "
            "checkpoints are rollback guardrails."
        ),
        "nextCheckpointAt": ((len(complete) // CHECKPOINT_RACES) + 1) * CHECKPOINT_RACES,
        "races": complete,
    }


def markdown(report: dict[str, Any]) -> str:
    summary = report["cumulative"]
    coverage = report["coverage"]
    visible = summary["visible"]
    candidate = summary["candidate"]
    lines = [
        f"# MAIDEN Controlled Live Monitor - {report['runDate']}",
        "",
        f"- Status: **{report['status']}**",
        f"- Fully labeled prospective races: {coverage['fullyLabeledRaces']}",
        f"- Partial / unlabeled / invalid: {coverage['partialRaces']} / "
        f"{coverage['unlabeledRaces']} / {coverage['integrityInvalidRaces']}",
        f"- Next checkpoint: {report['nextCheckpointAt']}",
        "- Live ranking and Telegram: MAIDEN ML15 enabled",
        "",
        "| Ranking | Top1 | Winner Top3 | Winner Top5 | Avg winner rank |",
        "|---|---:|---:|---:|---:|",
        f"| Visible v4 | {visible['top1']} | {visible['winnerTop3']} | "
        f"{visible['winnerTop5']} | {visible['avgWinnerRank']} |",
        f"| MAIDEN candidate | {candidate['top1']} | {candidate['winnerTop3']} | "
        f"{candidate['winnerTop5']} | {candidate['avgWinnerRank']} |",
        "",
        f"- Rescue / damage / net: {summary['rescues']} / {summary['damages']} / "
        f"{summary['winnerTop3Net']}",
        f"- Top1 net: {summary['top1Net']}",
        f"- Boundary gap ratio: {summary['boundaryGapRatio']}",
        f"- Cutoff crowd baseline/candidate: {summary['baselineCutoffCrowdMedian']} / "
        f"{summary['candidateCutoffCrowdMedian']}",
        "",
        "| Checkpoint | Races | Visible WTop3 | Candidate WTop3 | Damage | Passed |",
        "|---:|---:|---:|---:|---:|---|",
    ]
    for checkpoint in report["checkpoints"]:
        lines.append(
            f"| {checkpoint['index']} | {checkpoint['races']} | "
            f"{checkpoint['visible']['winnerTop3']} | "
            f"{checkpoint['candidate']['winnerTop3']} | {checkpoint['damages']} | "
            f"{'yes' if checkpoint['passed'] else 'no'} |"
        )
    if not report["checkpoints"]:
        lines.append("| - | 0 | 0 | 0 | 0 | collecting |")
    return "\n".join(lines) + "\n"


def persist(report: dict[str, Any], data_dir: Path) -> None:
    run_dir = data_dir / "automation" / "runs" / report["runDate"]
    latest_dir = data_dir / "automation" / "maiden-shadow"
    encoded = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    rendered = markdown(report)
    for path, content in (
        (run_dir / "maiden-shadow-checkpoint.json", encoded),
        (run_dir / "maiden-shadow-checkpoint.md", rendered),
        (latest_dir / "latest.json", encoded),
        (latest_dir / "latest.md", rendered),
    ):
        atomic_write(path, content)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--predictions", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--run-date", required=True)
    args = parser.parse_args()
    report = build_report(load_jsonl(args.predictions), args.run_date)
    persist(report, args.data_dir)
    print(json.dumps({
        "status": report["status"],
        "fullyLabeledRaces": report["coverage"]["fullyLabeledRaces"],
        "nextCheckpointAt": report["nextCheckpointAt"],
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
