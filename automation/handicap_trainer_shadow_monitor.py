"""Fail-closed prospective monitor for the HANDIKAP trainer-score ablation."""

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


EXPECTED_VERSION = "handicap-trainer-ablation-20260814-v1"
EXPECTED_OBSERVATION_START = "14.08.2026"
EXPECTED_BASELINE_VERSION = "4.25"
EXPECTED_METRIC = "trainer_score"
CHECKPOINTS = (5, 10, 15)
TERMINAL_FINISH_POSITIONS = {99}
ISTANBUL_TZ = ZoneInfo("Europe/Istanbul")


def finite(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def numeric_weights(value: Any) -> dict[str, float] | None:
    if not isinstance(value, dict) or not value:
        return None
    parsed: dict[str, float] = {}
    for key, raw in value.items():
        number = finite(raw)
        if not str(key).strip() or number is None or number < 0.0:
            return None
        parsed[str(key)] = number
    return parsed


def _race_identity_valid(rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return False
    common_keys = (
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
        "handicap_trainer_candidate_version",
        "handicap_trainer_candidate_observation_start",
        "handicap_trainer_candidate_created_ts",
        "handicap_trainer_candidate_baseline_version",
    )
    for key in common_keys:
        values = {
            str(row.get(key) if row.get(key) is not None else "").strip()
            for row in rows
        }
        if len(values) != 1 or not next(iter(values)):
            return False

    common_objects = (
        "handicap_trainer_candidate_profile",
        "handicap_trainer_candidate_baseline_weights",
        "handicap_trainer_candidate_weights",
        "handicap_trainer_candidate_weight_delta_pct",
    )
    if any(len({canonical(row.get(key)) for row in rows}) != 1 for key in common_objects):
        return False
    names = [str(row.get("horse_name") or "").strip().casefold() for row in rows]
    if any(not name for name in names) or len(names) != len(set(names)):
        return False

    first = rows[0]
    race_day = parse_race_date(first.get("race_date"))
    race_time = str(first.get("race_time") or "").strip().replace(".", ":")
    created_ts = safe_int(first.get("handicap_trainer_candidate_created_ts"), 0)
    if race_day is None or created_ts <= 0:
        return False
    try:
        hour, minute = (int(part) for part in race_time.split(":", 1))
        race_start = race_day.replace(hour=hour, minute=minute, tzinfo=ISTANBUL_TZ)
    except (TypeError, ValueError):
        return False
    return created_ts < int(race_start.timestamp())


def _candidate_definition_valid(rows: list[dict[str, Any]]) -> bool:
    first = rows[0]
    profile = first.get("handicap_trainer_candidate_profile")
    baseline = numeric_weights(first.get("handicap_trainer_candidate_baseline_weights"))
    candidate = numeric_weights(first.get("handicap_trainer_candidate_weights"))
    delta = first.get("handicap_trainer_candidate_weight_delta_pct")
    removed = finite(first.get("handicap_trainer_candidate_removed_weight_pct"))
    if (
        not isinstance(profile, dict)
        or profile.get("category") != "HANDIKAP"
        or not str(profile.get("subtype") or "").startswith("HANDIKAP")
        or not str(profile.get("selectedKey") or "")
        or baseline is None
        or candidate is None
        or not isinstance(delta, dict)
        or removed is None
        # Most live profiles resolve to 0.85-1.14%. HANDIKAP15|Cim is 2.22%
        # after the profile's other zeroed metrics are normalized; the frozen
        # ablation still removes that complete trainer component.
        or not (0.0 < removed <= 2.3)
        or abs(sum(baseline.values()) - 100.0) > 0.15
        or abs(sum(candidate.values()) - 100.0) > 0.15
        or abs(baseline.get(EXPECTED_METRIC, 0.0) - removed) > 1e-3
        or candidate.get(EXPECTED_METRIC, 0.0) > 1e-9
    ):
        return False

    all_metrics = set(baseline) | set(candidate)
    for metric in all_metrics:
        expected = round(candidate.get(metric, 0.0) - baseline.get(metric, 0.0), 4)
        actual = finite(delta.get(metric, 0.0))
        if actual is None or abs(actual - expected) > 1e-3:
            return False
    trainer_delta = finite(delta.get(EXPECTED_METRIC))
    if trainer_delta is None or abs(trainer_delta + removed) > 1e-3:
        return False

    source_count = 0
    for row in rows:
        if (
            str(row.get("handicap_trainer_candidate_version") or "") != EXPECTED_VERSION
            or str(row.get("handicap_trainer_candidate_mode") or "")
            != "prospective_shadow_ablation"
            or str(row.get("handicap_trainer_candidate_observation_start") or "")
            != EXPECTED_OBSERVATION_START
            or str(row.get("handicap_trainer_candidate_baseline_version") or "")
            != EXPECTED_BASELINE_VERSION
            or str(row.get("handicap_trainer_candidate_ablated_metric") or "")
            != EXPECTED_METRIC
            or bool(row.get("handicap_trainer_candidate_used_for_ranking"))
            or bool(row.get("handicap_trainer_candidate_rollout_eligible"))
        ):
            return False
        source = row.get("handicap_trainer_candidate_source")
        flags = row.get("handicap_trainer_candidate_metric_source_flags")
        if (
            not isinstance(source, dict)
            or not isinstance(flags, dict)
            or source.get("metric") != EXPECTED_METRIC
            or source.get("guard") != "hasTrainer"
            or bool(source.get("hasSource")) != bool(flags.get("hasTrainer"))
        ):
            return False
        source_count += bool(source.get("hasSource"))

    expected_coverage = round(source_count / len(rows), 4)
    return all(
        safe_int((row.get("handicap_trainer_candidate_source") or {}).get("sourceCount"), -1)
        == source_count
        and safe_int((row.get("handicap_trainer_candidate_source") or {}).get("runnerCount"), -1)
        == len(rows)
        and abs(
            safe_float((row.get("handicap_trainer_candidate_source") or {}).get("coverage"), -1.0)
            - expected_coverage
        )
        <= 1e-4
        for row in rows
    )


def _score_payload_valid(rows: list[dict[str, Any]]) -> bool:
    candidate_order: list[tuple[float, int, int]] = []
    for index, row in enumerate(rows):
        baseline_score = finite(row.get("handicap_trainer_candidate_baseline_score"))
        baseline_rank = safe_int(row.get("handicap_trainer_candidate_baseline_rank"), 0)
        base_score = finite(row.get("handicap_trainer_candidate_base_score"))
        penalty = finite(row.get("handicap_trainer_candidate_penalty_total"))
        score = finite(row.get("handicap_trainer_candidate_score"))
        rank = safe_int(row.get("handicap_trainer_candidate_rank"), 0)
        components = row.get("handicap_trainer_candidate_score_components")
        candidate_weights = numeric_weights(
            row.get("handicap_trainer_candidate_weights")
        )
        snapshot = row.get("handicap_trainer_candidate_feature_snapshot")
        if (
            None in (baseline_score, base_score, penalty, score)
            or baseline_rank <= 0
            or rank <= 0
            or not isinstance(components, dict)
            or not components
            or candidate_weights is None
            or not isinstance(snapshot, dict)
            or set(components) != set(candidate_weights)
            or abs(baseline_score - safe_float(row.get("v4_score"), -999.0)) > 1e-6
            or baseline_rank != safe_int(row.get("v4_rank"), 0)
        ):
            return False

        weighted_sum = 0.0
        weight_total = 0.0
        for metric, component in components.items():
            if not isinstance(component, dict):
                return False
            value = finite(component.get("value"))
            weight = finite(component.get("weightPct"))
            snapshot_value = finite(snapshot.get(metric))
            expected_weight = candidate_weights.get(metric)
            if (
                value is None
                or weight is None
                or weight <= 0.0
                or snapshot_value is None
                or expected_weight is None
                or abs(weight - expected_weight) > 1e-5
                or abs(value - snapshot_value) > 1e-6
            ):
                return False
            if bool(component.get("included")):
                weighted_sum += value * weight
                weight_total += weight
        if weight_total <= 0.0:
            return False
        expected_base = round(max(0.0, min(100.0, weighted_sum / weight_total)), 1)
        expected_score = round(max(0.0, min(100.0, expected_base - penalty)), 1)
        if abs(base_score - expected_base) > 1e-6 or abs(score - expected_score) > 1e-6:
            return False
        candidate_order.append((-score, baseline_rank, index))

    valid_ranks = list(range(1, len(rows) + 1))
    baseline_ranks = [
        safe_int(row.get("handicap_trainer_candidate_baseline_rank"), 0)
        for row in rows
    ]
    candidate_ranks = [
        safe_int(row.get("handicap_trainer_candidate_rank"), 0)
        for row in rows
    ]
    if sorted(baseline_ranks) != valid_ranks or sorted(candidate_ranks) != valid_ranks:
        return False
    baseline_order = sorted(
        rows,
        key=lambda row: safe_int(row.get("handicap_trainer_candidate_baseline_rank"), 0),
    )
    if any(
        safe_float(left.get("handicap_trainer_candidate_baseline_score"), -1.0)
        + 1e-9
        < safe_float(right.get("handicap_trainer_candidate_baseline_score"), -1.0)
        for left, right in zip(baseline_order, baseline_order[1:])
    ):
        return False
    expected_ranks = {
        original_index: rank + 1
        for rank, (_, _, original_index) in enumerate(sorted(candidate_order))
    }
    return all(
        safe_int(row.get("handicap_trainer_candidate_rank"), 0)
        == expected_ranks[index]
        for index, row in enumerate(rows)
    )


def candidate_valid(rows: list[dict[str, Any]]) -> bool:
    return bool(
        _race_identity_valid(rows)
        and _candidate_definition_valid(rows)
        and _score_payload_valid(rows)
    )


def classify_race(rows: list[dict[str, Any]]) -> str:
    expected = safe_int(rows[0].get("field_size"), len(rows)) if rows else 0
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
    next_rank = 1
    for rank, tied_count in sorted(Counter(ranked).items()):
        if rank != next_rank:
            return "integrity_invalid"
        next_rank += tied_count
    return "fully_labeled"


def _metrics(races: list[dict[str, Any]], prefix: str) -> dict[str, Any]:
    ranks = [race[f"{prefix}WinnerRank"] for race in races]
    guards = [race[f"{prefix}Guardrails"] for race in races]

    def average(key: str) -> float | None:
        values = [guard[key] for guard in guards if guard.get(key) is not None]
        return round(statistics.mean(values), 4) if values else None

    mae, rho, ndcg5 = average("mae"), average("rho"), average("ndcg5")
    avg_field = statistics.mean(race["fieldSize"] for race in races) if races else 1.0
    objective = None
    if None not in (mae, rho, ndcg5):
        objective = round(
            0.45 * ndcg5
            + 0.35 * ((rho + 1.0) / 2.0)
            + 0.20 * max(0.0, 1.0 - mae / max(avg_field, 1.0)),
            4,
        )
    return {
        "top1": sum(rank == 1 for rank in ranks),
        "winnerTop3": sum(rank <= 3 for rank in ranks),
        "winnerTop5": sum(rank <= 5 for rank in ranks),
        "avgWinnerRank": round(statistics.mean(ranks), 3) if ranks else None,
        "mae": mae,
        "rho": rho,
        "ndcg5": ndcg5,
        "objective": objective,
    }


def summarize(races: list[dict[str, Any]]) -> dict[str, Any]:
    baseline = _metrics(races, "baseline")
    candidate = _metrics(races, "candidate")
    rescues = sum(
        race["baselineWinnerRank"] > 3 and race["candidateWinnerRank"] <= 3
        for race in races
    )
    damages = sum(
        race["baselineWinnerRank"] <= 3 and race["candidateWinnerRank"] > 3
        for race in races
    )
    return {
        "races": len(races),
        "baseline": baseline,
        "candidate": candidate,
        "rescues": rescues,
        "damages": damages,
        "winnerTop3Net": rescues - damages,
        "top1Net": candidate["top1"] - baseline["top1"],
        "top5Net": candidate["winnerTop5"] - baseline["winnerTop5"],
        "objectiveDelta": (
            round(candidate["objective"] - baseline["objective"], 4)
            if candidate["objective"] is not None and baseline["objective"] is not None
            else None
        ),
    }


def checkpoint_pass(summary: dict[str, Any]) -> bool:
    baseline, candidate = summary["baseline"], summary["candidate"]
    return bool(
        summary["winnerTop3Net"] >= 0
        and summary["damages"] == 0
        and summary["top1Net"] >= -1
        and summary["top5Net"] >= 0
        and summary["objectiveDelta"] is not None
        and summary["objectiveDelta"] >= 0.0
        and candidate["mae"] <= baseline["mae"] + 0.10
        and candidate["rho"] >= baseline["rho"] - 0.02
        and candidate["ndcg5"] >= baseline["ndcg5"] - 0.005
    )


def build_report(entries: list[dict[str, Any]], run_date: str) -> dict[str, Any]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        version = str(entry.get("handicap_trainer_candidate_version") or "").strip()
        if version:
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
    complete: list[dict[str, Any]] = []
    versions: set[str] = set()
    for ((_, _, version), rows) in sorted(grouped.items(), key=lambda item: race_sort_key(item[1])):
        versions.add(version)
        race_day = parse_race_date(rows[0].get("race_date"))
        start_day = parse_race_date(rows[0].get("handicap_trainer_candidate_observation_start"))
        if race_day is None or start_day is None:
            coverage["integrityInvalidRaces"] += 1
            continue
        if race_day < start_day:
            coverage["preProspectiveExcludedRaces"] += 1
            continue
        if not candidate_valid(rows):
            coverage["integrityInvalidRaces"] += 1
            continue
        state = classify_race(rows)
        if state != "fully_labeled":
            key = {
                "partial": "partialRaces",
                "unlabeled": "unlabeledRaces",
            }.get(state, "integrityInvalidRaces")
            coverage[key] += 1
            continue
        coverage["fullyLabeledRaces"] += 1
        winner = next(row for row in rows if safe_int(row.get("finish_pos"), 0) == 1)
        source_rows = [row.get("handicap_trainer_candidate_source") or {} for row in rows]
        snapshots = [row.get("handicap_trainer_candidate_feature_snapshot") or {} for row in rows]
        complete.append({
            "raceId": str(winner.get("race_id") or ""),
            "raceDate": winner.get("race_date"),
            "raceNo": winner.get("race_no"),
            "raceTime": winner.get("race_time"),
            "city": winner.get("city"),
            "raceType": winner.get("race_type"),
            "track": winner.get("track"),
            "distance": winner.get("distance"),
            "fieldSize": len(rows),
            "winner": winner.get("horse_name"),
            "candidateVersion": version,
            "candidateCreatedTs": safe_int(winner.get("handicap_trainer_candidate_created_ts"), 0),
            "profile": winner.get("handicap_trainer_candidate_profile"),
            "removedTrainerWeightPct": winner.get("handicap_trainer_candidate_removed_weight_pct"),
            "baselineWinnerRank": safe_int(winner.get("handicap_trainer_candidate_baseline_rank")),
            "candidateWinnerRank": safe_int(winner.get("handicap_trainer_candidate_rank")),
            "baselineGuardrails": ranking_guardrails(rows, "handicap_trainer_candidate_baseline_rank"),
            "candidateGuardrails": ranking_guardrails(rows, "handicap_trainer_candidate_rank"),
            "trainerSourceCount": sum(bool(source.get("hasSource")) for source in source_rows),
            "trainerNonNeutralCount": sum(
                bool(source.get("hasSource"))
                and abs(safe_float(snapshot.get(EXPECTED_METRIC), 50.0) - 50.0) >= 1.0
                for source, snapshot in zip(source_rows, snapshots)
            ),
        })

    complete.sort(key=lambda race: (
        parse_race_date(race["raceDate"]) or datetime.min,
        safe_int(race["raceNo"], 0),
        race["raceId"],
    ))
    cumulative = summarize(complete)
    checkpoints = []
    for end in CHECKPOINTS:
        if len(complete) < end:
            break
        summary = summarize(complete[:end])
        checkpoints.append({"atRace": end, **summary, "passed": checkpoint_pass(summary)})

    total_runners = sum(race["fieldSize"] for race in complete)
    source_count = sum(race["trainerSourceCount"] for race in complete)
    non_neutral_count = sum(race["trainerNonNeutralCount"] for race in complete)
    source_coverage = {
        "runnerCount": total_runners,
        "trainerCount": source_count,
        "trainerCoverage": round(source_count / total_runners, 4) if total_runners else 0.0,
        "trainerNonNeutralCount": non_neutral_count,
        "trainerNonNeutralRatio": (
            round(non_neutral_count / total_runners, 4) if total_runners else 0.0
        ),
    }
    source_gate = bool(
        source_coverage["trainerCoverage"] >= 0.40
        and source_coverage["trainerNonNeutralRatio"] >= 0.15
    )
    formal_supported = bool(
        len(complete) >= 15
        and len(checkpoints) == 3
        and all(checkpoint["passed"] for checkpoint in checkpoints)
        and cumulative["winnerTop3Net"] >= 1
        and cumulative["damages"] == 0
        and cumulative["top1Net"] >= 0
        and cumulative["top5Net"] >= 0
        and cumulative["objectiveDelta"] is not None
        and cumulative["objectiveDelta"] >= 0.0
        and source_gate
    )
    regression = bool(checkpoints and not checkpoints[-1]["passed"])
    if len(complete) < 5:
        status = "COLLECTING"
    elif regression:
        status = "REGRESSION_SIGNAL"
    elif len(complete) < 15:
        status = "EARLY_SIGNAL"
    elif formal_supported:
        status = "SUPPORTED_FOR_FORMAL_REPLAY"
    else:
        status = "REVIEW"

    next_checkpoint = next((value for value in CHECKPOINTS if value > len(complete)), None)
    return {
        "runDate": run_date,
        "mode": "prospective_shadow_only",
        "status": status,
        "candidateVersions": sorted(versions),
        "coverage": coverage,
        "cumulative": cumulative,
        "checkpoints": checkpoints,
        "sourceCoverage": source_coverage,
        "sourceGateReady": source_gate,
        "regressionSignal": regression,
        "formalReplaySupported": formal_supported,
        "liveRolloutEligible": False,
        "liveRolloutReason": (
            "This ablation remains non-ranking. Clean +5/+10/+15 prospective "
            "checkpoints can only advance it to a new formal replay."
        ),
        "nextCheckpointAt": next_checkpoint,
        "races": complete,
    }


def markdown(report: dict[str, Any]) -> str:
    coverage = report["coverage"]
    summary = report["cumulative"]
    baseline, candidate = summary["baseline"], summary["candidate"]
    lines = [
        f"# HANDIKAP Trainer Ablation Shadow - {report['runDate']}",
        "",
        f"- Status: **{report['status']}**",
        f"- Fully labeled: {coverage['fullyLabeledRaces']}",
        f"- Partial / unlabeled / invalid: {coverage['partialRaces']} / "
        f"{coverage['unlabeledRaces']} / {coverage['integrityInvalidRaces']}",
        f"- Pre-prospective excluded: {coverage['preProspectiveExcludedRaces']}",
        f"- Next checkpoint: {report['nextCheckpointAt']}",
        f"- Trainer source gate ready: {report['sourceGateReady']}",
        "- Live ranking and Telegram: unchanged",
        "",
        "| Ranking | Top1 | Winner Top3 | Winner Top5 | Avg winner rank | MAE | Rho | NDCG@5 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
        f"| Visible v4.25 | {baseline['top1']} | {baseline['winnerTop3']} | "
        f"{baseline['winnerTop5']} | {baseline['avgWinnerRank']} | {baseline['mae']} | "
        f"{baseline['rho']} | {baseline['ndcg5']} |",
        f"| Trainer ablation | {candidate['top1']} | {candidate['winnerTop3']} | "
        f"{candidate['winnerTop5']} | {candidate['avgWinnerRank']} | {candidate['mae']} | "
        f"{candidate['rho']} | {candidate['ndcg5']} |",
        "",
        f"- Rescue / damage / net: {summary['rescues']} / {summary['damages']} / "
        f"{summary['winnerTop3Net']}",
        f"- Objective delta: {summary['objectiveDelta']}",
        "",
        "| Checkpoint | Baseline WTop3 | Candidate WTop3 | Damage | Objective delta | Passed |",
        "|---:|---:|---:|---:|---:|---|",
    ]
    for checkpoint in report["checkpoints"]:
        lines.append(
            f"| +{checkpoint['atRace']} | {checkpoint['baseline']['winnerTop3']} | "
            f"{checkpoint['candidate']['winnerTop3']} | {checkpoint['damages']} | "
            f"{checkpoint['objectiveDelta']} | {'yes' if checkpoint['passed'] else 'no'} |"
        )
    if not report["checkpoints"]:
        lines.append("| +5 | 0 | 0 | 0 | - | collecting |")
    return "\n".join(lines) + "\n"


def persist(report: dict[str, Any], data_dir: Path) -> None:
    run_dir = data_dir / "automation" / "runs" / report["runDate"]
    latest_dir = data_dir / "automation" / "handicap-trainer-shadow"
    encoded = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    rendered = markdown(report)
    for path, content in (
        (run_dir / "handicap-trainer-shadow-checkpoint.json", encoded),
        (run_dir / "handicap-trainer-shadow-checkpoint.md", rendered),
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
