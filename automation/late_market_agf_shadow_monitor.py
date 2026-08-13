#!/usr/bin/env python3
"""Validate and score prospective late-market AGF sidecar snapshots."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

try:
    from automation.late_market_agf_shadow import (
        ALPHA,
        IntegrityError,
        MIN_COVERAGE,
        MIN_LEAD_MINUTES,
        OBSERVATION_START,
        TJK_DAILY_PAGE_URL,
        VERSION,
        atomic_write,
        canonical,
        clean_id,
        clean_name,
        finite,
        load_jsonl,
        safe_int,
        sha256_payload,
        parse_agf_percent,
        profile_from_rows,
        validate_backend_daily_program_url,
    )
    from automation.sart1_shadow_monitor import ranking_guardrails
except ModuleNotFoundError as exc:
    if exc.name != "automation":
        raise
    from late_market_agf_shadow import (  # type: ignore[no-redef]
        ALPHA,
        IntegrityError,
        MIN_COVERAGE,
        MIN_LEAD_MINUTES,
        OBSERVATION_START,
        TJK_DAILY_PAGE_URL,
        VERSION,
        atomic_write,
        canonical,
        clean_id,
        clean_name,
        finite,
        load_jsonl,
        safe_int,
        sha256_payload,
        parse_agf_percent,
        profile_from_rows,
        validate_backend_daily_program_url,
    )
    from sart1_shadow_monitor import ranking_guardrails  # type: ignore[no-redef]


CHECKPOINTS = (5, 10, 15)
TERMINAL_FINISH_POSITIONS = {99}
ISTANBUL = ZoneInfo("Europe/Istanbul")


def _prediction_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        clean_id(row.get("race_date")),
        clean_id(row.get("city_id")),
        clean_id(row.get("race_id")),
        clean_id(row.get("race_no")),
    )


def _snapshot_key(snapshot: dict[str, Any]) -> tuple[str, str, str, str]:
    identity = snapshot.get("identity") or {}
    return (
        clean_id(identity.get("raceDate")),
        clean_id(identity.get("cityId")),
        clean_id(identity.get("raceId")),
        clean_id(identity.get("raceNo")),
    )


def _prediction_groups(rows: list[dict[str, Any]]) -> dict[tuple[str, str, str, str], list[dict[str, Any]]]:
    groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = _prediction_key(row)
        if all(key):
            groups[key].append(row)
    return groups


def _valid_snapshot_hash(snapshot: dict[str, Any]) -> bool:
    expected = clean_id(snapshot.get("snapshotSha256"))
    unhashed = dict(snapshot)
    unhashed.pop("snapshotSha256", None)
    return len(expected) == 64 and expected == sha256_payload(unhashed)


def validate_snapshot(
    snapshot: dict[str, Any],
    prediction_rows: list[dict[str, Any]],
) -> tuple[bool, str]:
    identity = snapshot.get("identity") or {}
    policy = snapshot.get("policy") or {}
    coverage = snapshot.get("coverage") or {}
    market = snapshot.get("market") or {}
    source = snapshot.get("source") or {}
    baseline = snapshot.get("baseline") or {}
    runners = snapshot.get("runners") or []
    if (
        not all(
            isinstance(value, dict)
            for value in (identity, policy, coverage, market, source, baseline)
        )
        or not isinstance(runners, list)
        or any(not isinstance(row, dict) for row in runners)
    ):
        return False, "payload_types"
    if (
        safe_int(snapshot.get("schemaVersion"), -1) != 1
        or
        snapshot.get("version") != VERSION
        or snapshot.get("mode") != "prospective_shadow_bounded"
        or snapshot.get("observationStart") != OBSERVATION_START
        or bool(snapshot.get("usedForRanking"))
        or bool(snapshot.get("rolloutEligible"))
        or bool(snapshot.get("telegramVisible"))
        or not _valid_snapshot_hash(snapshot)
    ):
        return False, "candidate_identity_or_hash"
    if (
        finite(policy.get("alpha")) != ALPHA
        or ALPHA > 0.10
        or finite(policy.get("minCoverage")) != MIN_COVERAGE
        or finite(policy.get("preferredCoverage")) != 1.0
        or safe_int(policy.get("minLeadMinutes")) != MIN_LEAD_MINUTES
        or finite(policy.get("missingAgfComponent")) != 50.0
        or source.get("provider") != "TJK"
        or source.get("dataset") != "official_daily_program"
        or source.get("officialPage") != TJK_DAILY_PAGE_URL
        or source.get("transport") != "backend_read_only_daily_program"
        or not clean_id(source.get("requestUrl"))
    ):
        return False, "policy_or_source"
    if (
        not all(_snapshot_key(snapshot))
        or identity.get("profile") not in {"MAIDEN", "SART1"}
        or safe_int(identity.get("fieldSize")) != len(runners)
        or len(runners) < 2
        or not bool(baseline.get("appliedForRanking"))
        or market.get("selectionPolicy") != "first_official_tjk_anchor"
        or not bool(market.get("distinctOfficialPoolsPreserved"))
    ):
        return False, "race_identity"
    collected_ts = safe_int(snapshot.get("collectedTs"), 0)
    race_start_ts = safe_int(identity.get("raceStartTs"), 0)
    prediction_ts_min = safe_int(baseline.get("predictionTsMin"), 0)
    prediction_ts_max = safe_int(baseline.get("predictionTsMax"), 0)
    try:
        collected_at = datetime.fromisoformat(
            clean_id(snapshot.get("collectedAt")).replace("Z", "+00:00")
        )
        if collected_at.tzinfo is None:
            return False, "collected_at_timezone"
        collected_at_ts = int(collected_at.timestamp())
        race_day = datetime.strptime(identity["raceDate"], "%d.%m.%Y").date()
        observation_day = datetime.strptime(OBSERVATION_START, "%d.%m.%Y").date()
        race_time_text = clean_id(identity.get("raceTime")).replace(".", ":")
        race_hour, race_minute = (int(part) for part in race_time_text.split(":", 1))
        expected_race_start_ts = int(
            datetime(
                race_day.year,
                race_day.month,
                race_day.day,
                race_hour,
                race_minute,
                tzinfo=ISTANBUL,
            ).timestamp()
        )
    except (TypeError, ValueError):
        return False, "time_parse"
    expected_snapshot_key = "|".join(
        (
            VERSION,
            race_day.isoformat(),
            clean_id(identity.get("cityId")),
            clean_id(identity.get("raceId")),
            clean_id(identity.get("raceNo")),
        )
    )
    if clean_id(snapshot.get("snapshotKey")) != expected_snapshot_key:
        return False, "snapshot_key"
    calculated_lead = (race_start_ts - collected_ts) / 60.0
    if (
        race_day < observation_day
        or collected_at_ts != collected_ts
        or race_start_ts != expected_race_start_ts
        or datetime.fromtimestamp(collected_ts, ISTANBUL).date() != race_day
        or prediction_ts_min <= 0
        or prediction_ts_max < prediction_ts_min
        or prediction_ts_max >= collected_ts
        or datetime.fromtimestamp(prediction_ts_min, ISTANBUL).date() != race_day
        or datetime.fromtimestamp(prediction_ts_max, ISTANBUL).date() != race_day
        or collected_ts >= race_start_ts
        or calculated_lead + 1e-9 < MIN_LEAD_MINUTES
        or abs((finite(snapshot.get("leadMinutes")) or -999.0) - round(calculated_lead, 3)) > 1e-6
    ):
        return False, "causality"
    try:
        validate_backend_daily_program_url(
            clean_id(source.get("requestUrl")),
            race_day,
            clean_id(identity.get("cityId")),
        )
    except (IntegrityError, ValueError):
        return False, "source_request_identity"
    source_count = sum(bool(row.get("hasAgf")) for row in runners)
    ratio = source_count / len(runners)
    if (
        source_count != safe_int(coverage.get("sourceCount"), -1)
        or len(runners) != safe_int(coverage.get("runnerCount"), -1)
        or abs(ratio - (finite(coverage.get("ratio")) or -1.0)) > 1e-6
        or ratio + 1e-9 < MIN_COVERAGE
        or safe_int(identity.get("raceStartTs")) - safe_int(snapshot.get("collectedTs"))
        < MIN_LEAD_MINUTES * 60
        or bool(coverage.get("preferredReached")) != (ratio + 1e-9 >= 1.0)
    ):
        return False, "coverage_or_lead"

    names = [clean_name(row.get("horseName")) for row in runners]
    numbers = [clean_id(row.get("horseNo")) for row in runners]
    at_ids = [clean_id(row.get("atId")) for row in runners]
    if (
        any(not value for value in names + numbers + at_ids)
        or len(set(names)) != len(runners)
        or len(set(numbers)) != len(runners)
        or len(set(at_ids)) != len(runners)
    ):
        return False, "runner_identity"
    baseline_ranks = [safe_int(row.get("baselineRank")) for row in runners]
    candidate_ranks = [safe_int(row.get("candidateRank")) for row in runners]
    if sorted(baseline_ranks) != list(range(1, len(runners) + 1)) or sorted(candidate_ranks) != list(range(1, len(runners) + 1)):
        return False, "rank_permutation"
    for row in runners:
        base = finite(row.get("baselineComponent"))
        market_component = finite(row.get("agfComponent"))
        score = finite(row.get("candidateScore"))
        agf = finite(row.get("agfPercent"))
        if None in (base, market_component, score):
            return False, "score_non_finite"
        expected = round((1.0 - ALPHA) * base + ALPHA * market_component, 6)
        # Stored components are rounded to six decimals; allow the resulting
        # one-unit last-decimal reconstruction difference.
        if abs(score - expected) > 2e-6:
            return False, "score_formula"
        if bool(row.get("hasAgf")) != (agf is not None):
            return False, "agf_guard"
        pools = row.get("agfPools")
        if not isinstance(pools, list):
            return False, "agf_pools_missing"
        if agf is not None:
            if not pools or not isinstance(pools[0], dict):
                return False, "agf_selected_pool_missing"
            if len(pools) > 2:
                return False, "agf_pool_count"
            pool_numbers = []
            for pool in pools:
                if not isinstance(pool, dict):
                    return False, "agf_pool_object"
                pool_percent = finite(pool.get("percent"))
                pool_no = pool.get("poolNo")
                if pool_no not in (None, 1, 2) or pool_percent is None or not (0.0 <= pool_percent <= 100.0):
                    return False, "agf_pool_value"
                raw_percent = parse_agf_percent(pool.get("raw"))
                if raw_percent is None or abs(raw_percent - pool_percent) > 1e-9:
                    return False, "agf_pool_raw"
                if pool.get("rank") not in (None, "") and safe_int(pool.get("rank"), -1) <= 0:
                    return False, "agf_pool_rank"
                if pool_no is not None:
                    pool_numbers.append(pool_no)
            if pool_numbers != sorted(set(pool_numbers)):
                return False, "agf_pool_order"
            if len(pools) > 1 and pool_numbers != [1, 2]:
                return False, "agf_dual_pool_identity"
            first_percent = finite(pools[0].get("percent"))
            first_pool_no = pools[0].get("poolNo")
            selected_pool_no = row.get("selectedPoolNo")
            if (
                first_percent is None
                or abs(first_percent - agf) > 1e-9
                or first_pool_no != selected_pool_no
                or selected_pool_no != market.get("selectedPoolNo")
                or clean_id(row.get("rawSelectedAgf")) != clean_id(pools[0].get("raw"))
            ):
                return False, "agf_selected_pool_mismatch"
        elif (
            pools
            or row.get("selectedPoolNo") is not None
            or clean_id(row.get("rawSelectedAgf"))
        ):
            return False, "agf_missing_runner_payload"

    sourced_agf = [finite(row.get("agfPercent")) for row in runners if bool(row.get("hasAgf"))]
    if len(sourced_agf) < 2 or any(value is None for value in sourced_agf):
        return False, "agf_source_values"
    agf_low, agf_high = min(sourced_agf), max(sourced_agf)
    if agf_high - agf_low <= 1e-9:
        return False, "agf_not_discriminative"
    for row in runners:
        agf = finite(row.get("agfPercent"))
        expected_component = (
            50.0
            if agf is None
            else 100.0 * (agf - agf_low) / (agf_high - agf_low)
        )
        if abs((finite(row.get("agfComponent")) or 0.0) - round(expected_component, 6)) > 1e-6:
            return False, "agf_component_formula"

    baseline_scores = [finite(row.get("baselineV4Score")) for row in runners]
    if any(value is None for value in baseline_scores):
        return False, "baseline_score_non_finite"
    score_low = min(baseline_scores)  # type: ignore[arg-type]
    score_high = max(baseline_scores)  # type: ignore[arg-type]
    expected_base_components = (
        [50.0 for _ in runners]
        if score_high - score_low <= 1e-9
        else [
            100.0 * (score - score_low) / (score_high - score_low)  # type: ignore[operator]
            for score in baseline_scores
        ]
    )
    if any(
        abs((finite(row.get("baselineComponent")) or 0.0) - expected) > 1e-5
        for row, expected in zip(runners, expected_base_components)
    ):
        return False, "baseline_component_formula"
    expected_order = sorted(
        range(len(runners)),
        key=lambda index: (
            -(finite(runners[index].get("candidateScore")) or 0.0),
            safe_int(runners[index].get("baselineRank"), 999),
            safe_int(runners[index].get("horseNo"), 999),
        ),
    )
    expected_ranks = {index: rank + 1 for rank, index in enumerate(expected_order)}
    if any(safe_int(row.get("candidateRank")) != expected_ranks[index] for index, row in enumerate(runners)):
        return False, "candidate_rank_formula"

    by_name = {clean_name(row.get("horse_name")): row for row in prediction_rows}
    if set(by_name) != set(names) or len(by_name) != len(prediction_rows):
        return False, "prediction_runner_set"
    for runner in runners:
        prediction = by_name[clean_name(runner.get("horseName"))]
        prediction_score = finite(prediction.get("v4_score"))
        runner_score = finite(runner.get("baselineV4Score"))
        if (
            clean_id(prediction.get("horse_no")) != clean_id(runner.get("horseNo"))
            or safe_int(prediction.get("v4_rank")) != safe_int(runner.get("baselineRank"))
            or prediction_score is None
            or runner_score is None
            or abs(prediction_score - runner_score) > 1e-6
            or not bool(prediction.get("v4_applied_for_ranking"))
            or clean_id(prediction.get("v4_version")) != clean_id(baseline.get("version"))
            or profile_from_rows([prediction]) != identity.get("profile")
            or clean_id(prediction.get("race_date")) != clean_id(identity.get("raceDate"))
            or clean_id(prediction.get("city_id")) != clean_id(identity.get("cityId"))
            or clean_id(prediction.get("race_id")) != clean_id(identity.get("raceId"))
            or clean_id(prediction.get("race_no")) != clean_id(identity.get("raceNo"))
            or clean_id(prediction.get("race_time")).replace(".", ":")
            != clean_id(identity.get("raceTime")).replace(".", ":")
            or clean_name(prediction.get("city")) != clean_name(identity.get("city"))
        ):
            return False, "prediction_baseline_mismatch"
        weights = prediction.get("v4_weights") or {}
        flags = prediction.get("metric_source_flags") or {}
        if (
            not isinstance(weights, dict)
            or not isinstance(flags, dict)
            or flags.get("hasAgf") is not False
        ):
            return False, "prediction_market_already_applied"
    return True, "valid"


def classify_labels(rows: list[dict[str, Any]], field_size: int) -> str:
    if len(rows) != field_size:
        return "partial"
    positions: list[int] = []
    for row in rows:
        value = row.get("finish_pos")
        if value is None:
            return "partial" if any(item.get("finish_pos") is not None for item in rows) else "unlabeled"
        position = safe_int(value, 0)
        if position <= 0:
            return "integrity_invalid"
        positions.append(position)
    if positions.count(1) != 1:
        return "integrity_invalid"
    ranked = [value for value in positions if value not in TERMINAL_FINISH_POSITIONS]
    expected = 1
    for rank, tied_count in sorted(Counter(ranked).items()):
        if rank != expected:
            return "integrity_invalid"
        expected += tied_count
    return "fully_labeled"


def evaluate_snapshot(snapshot: dict[str, Any], prediction_rows: list[dict[str, Any]]) -> tuple[str, dict[str, Any] | None, str]:
    valid, reason = validate_snapshot(snapshot, prediction_rows)
    if not valid:
        return "integrity_invalid", None, reason
    identity = snapshot["identity"]
    label_status = classify_labels(prediction_rows, safe_int(identity.get("fieldSize")))
    if label_status != "fully_labeled":
        return label_status, None, label_status
    predictions = {clean_name(row.get("horse_name")): row for row in prediction_rows}
    evaluation_rows = []
    for runner in snapshot["runners"]:
        row = predictions[clean_name(runner.get("horseName"))]
        evaluation_rows.append(
            {
                "finish_pos": row.get("finish_pos"),
                "baseline_rank": runner.get("baselineRank"),
                "candidate_rank": runner.get("candidateRank"),
            }
        )
    winner = next(row for row in evaluation_rows if safe_int(row.get("finish_pos")) == 1)
    baseline_rank = safe_int(winner.get("baseline_rank"))
    candidate_rank = safe_int(winner.get("candidate_rank"))

    def separation(rank_key: str, score_key: str) -> dict[str, Any]:
        ordered = sorted(
            snapshot["runners"],
            key=lambda row: safe_int(row.get(rank_key), 999),
        )
        scores = [finite(row.get(score_key)) for row in ordered]
        if any(score is None for score in scores):
            return {"top1Margin": None, "top3CutoffGap": None, "cutoffCrowd5": None}
        top1_margin = scores[0] - scores[1] if len(scores) >= 2 else None
        cutoff_gap = scores[2] - scores[3] if len(scores) >= 4 else None
        cutoff_crowd = (
            sum(score >= scores[2] - 5.0 for score in scores)
            if len(scores) >= 3
            else None
        )
        return {
            "top1Margin": round(top1_margin, 4) if top1_margin is not None else None,
            "top3CutoffGap": round(cutoff_gap, 4) if cutoff_gap is not None else None,
            "cutoffCrowd5": cutoff_crowd,
        }

    return (
        "fully_labeled",
        {
            "raceDate": identity["raceDate"],
            "city": identity["city"],
            "cityId": identity["cityId"],
            "raceId": identity["raceId"],
            "raceNo": identity["raceNo"],
            "profile": identity["profile"],
            "fieldSize": identity["fieldSize"],
            "coverage": snapshot["coverage"]["ratio"],
            "preferredCoverage": snapshot["coverage"]["preferredReached"],
            "baselineWinnerRank": baseline_rank,
            "candidateWinnerRank": candidate_rank,
            "rescue": baseline_rank > 3 and candidate_rank <= 3,
            "damage": baseline_rank <= 3 and candidate_rank > 3,
            "baselineGuardrails": ranking_guardrails(evaluation_rows, "baseline_rank"),
            "candidateGuardrails": ranking_guardrails(evaluation_rows, "candidate_rank"),
            "baselineSeparation": separation("baselineRank", "baselineComponent"),
            "candidateSeparation": separation("candidateRank", "candidateScore"),
        },
        "valid",
    )


def _summarize(races: list[dict[str, Any]]) -> dict[str, Any]:
    def metrics(rank_key: str, guard_key: str) -> dict[str, Any]:
        ranks = [safe_int(race[rank_key]) for race in races]
        guards = [race[guard_key] for race in races]

        def average(key: str) -> float | None:
            values = [finite(item.get(key)) for item in guards]
            clean = [value for value in values if value is not None]
            return round(statistics.mean(clean), 4) if clean else None

        mae, rho, ndcg5 = average("mae"), average("rho"), average("ndcg5")
        avg_field = statistics.mean(race["fieldSize"] for race in races) if races else 1.0
        objective = None
        if mae is not None and rho is not None and ndcg5 is not None:
            objective = round(
                0.45 * ndcg5
                + 0.35 * ((rho + 1.0) / 2.0)
                + 0.20 * max(0.0, 1.0 - mae / max(avg_field, 1.0)),
                4,
            )
        return {
            "races": len(ranks),
            "top1": sum(rank == 1 for rank in ranks),
            "winnerTop3": sum(rank <= 3 for rank in ranks),
            "winnerTop5": sum(rank <= 5 for rank in ranks),
            "avgWinnerRank": round(statistics.mean(ranks), 3) if ranks else None,
            "mae": mae,
            "rho": rho,
            "ndcg5": ndcg5,
            "objective": objective,
        }

    def separation_summary(key: str) -> dict[str, Any]:
        def average(metric: str) -> float | None:
            values = [finite(race[key].get(metric)) for race in races]
            clean = [value for value in values if value is not None]
            return round(statistics.mean(clean), 4) if clean else None

        return {
            "avgTop1Margin": average("top1Margin"),
            "avgTop3CutoffGap": average("top3CutoffGap"),
            "avgCutoffCrowd5": average("cutoffCrowd5"),
        }

    return {
        "races": len(races),
        "baseline": metrics("baselineWinnerRank", "baselineGuardrails"),
        "candidate": metrics("candidateWinnerRank", "candidateGuardrails"),
        "rescues": sum(bool(race["rescue"]) for race in races),
        "damages": sum(bool(race["damage"]) for race in races),
        "averageCoverage": round(statistics.mean(race["coverage"] for race in races), 4) if races else None,
        "preferredCoverageRaces": sum(bool(race["preferredCoverage"]) for race in races),
        "baselineSeparation": separation_summary("baselineSeparation"),
        "candidateSeparation": separation_summary("candidateSeparation"),
    }


def _checkpoint_pass(summary: dict[str, Any]) -> tuple[bool, list[str]]:
    baseline, candidate = summary["baseline"], summary["candidate"]
    failures = []
    required_net = 2 if summary["races"] >= 15 else 1
    if candidate["winnerTop3"] < baseline["winnerTop3"] + required_net:
        failures.append("winner_top3_no_genuine_gain")
    if candidate["top1"] < baseline["top1"] - (1 if summary["races"] >= 15 else 0):
        failures.append("top1_regressed")
    if candidate["winnerTop5"] < baseline["winnerTop5"]:
        failures.append("winner_top5_regressed")
    if candidate["avgWinnerRank"] > baseline["avgWinnerRank"]:
        failures.append("avg_winner_rank_regressed")
    if summary["damages"] > 0:
        failures.append("top3_damage_present")
    if summary["rescues"] - summary["damages"] < required_net:
        failures.append("rescue_no_genuine_gain")
    if (
        candidate["ndcg5"] is None
        or baseline["ndcg5"] is None
        or candidate["ndcg5"] < baseline["ndcg5"] - 0.005
    ):
        failures.append("ndcg5_regressed")
    if (
        candidate["mae"] is None
        or baseline["mae"] is None
        or candidate["mae"] > baseline["mae"] + 0.05
    ):
        failures.append("mae_regressed")
    baseline_separation = summary["baselineSeparation"]
    candidate_separation = summary["candidateSeparation"]
    baseline_gap = baseline_separation["avgTop3CutoffGap"]
    candidate_gap = candidate_separation["avgTop3CutoffGap"]
    if (
        baseline_gap is not None
        and (candidate_gap is None or candidate_gap + 0.5 < baseline_gap)
    ):
        failures.append("top3_cutoff_gap_regressed")
    baseline_crowd = baseline_separation["avgCutoffCrowd5"]
    candidate_crowd = candidate_separation["avgCutoffCrowd5"]
    if (
        baseline_crowd is not None
        and (candidate_crowd is None or candidate_crowd > baseline_crowd + 0.25)
    ):
        failures.append("top3_cutoff_crowd_regressed")
    return not failures, failures


def _profile_report(races: list[dict[str, Any]]) -> dict[str, Any]:
    races = sorted(
        races,
        key=lambda race: (
            datetime.strptime(race["raceDate"], "%d.%m.%Y"),
            safe_int(race["raceNo"]),
            race["raceId"],
        ),
    )
    cumulative = _summarize(races)
    checkpoints = []
    for count in CHECKPOINTS:
        if len(races) < count:
            continue
        summary = _summarize(races[:count])
        passed, failures = _checkpoint_pass(summary)
        checkpoints.append({"raceCount": count, "passed": passed, "failures": failures, **summary})
    formal = len(races) >= CHECKPOINTS[-1] and all(item["passed"] for item in checkpoints)
    next_checkpoint = next((count for count in CHECKPOINTS if len(races) < count), None)
    return {
        "status": "SUPPORTED_FOR_FORMAL_REPLAY" if formal else "COLLECTING_OR_REJECTED",
        "formalReplaySupported": formal,
        "liveRolloutEligible": False,
        "nextCheckpoint": next_checkpoint,
        "remainingToNextCheckpoint": max(0, next_checkpoint - len(races)) if next_checkpoint else 0,
        "checkpoints": checkpoints,
        "cumulative": cumulative,
    }


def build_report(
    snapshots: list[dict[str, Any]],
    predictions: list[dict[str, Any]],
    run_date: str,
) -> dict[str, Any]:
    groups = _prediction_groups(predictions)
    statuses: Counter[str] = Counter()
    invalid_reasons: Counter[str] = Counter()
    evaluated: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for snapshot in snapshots:
        logical_key = clean_id(snapshot.get("snapshotKey"))
        if not logical_key or logical_key in seen_keys:
            statuses["integrity_invalid"] += 1
            invalid_reasons["duplicate_or_missing_snapshot_key"] += 1
            continue
        seen_keys.add(logical_key)
        status, race, reason = evaluate_snapshot(snapshot, groups.get(_snapshot_key(snapshot), []))
        statuses[status] += 1
        if status == "integrity_invalid":
            invalid_reasons[reason] += 1
        if race is not None:
            evaluated.append(race)

    by_profile: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for race in evaluated:
        by_profile[race["profile"]].append(race)
    profiles = {profile: _profile_report(by_profile.get(profile, [])) for profile in ("MAIDEN", "SART1")}
    overall = _summarize(evaluated)
    return {
        "schemaVersion": 1,
        "version": VERSION,
        "mode": "prospective_shadow_bounded",
        "runDate": run_date,
        "generatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "usedForRanking": False,
        "telegramVisible": False,
        "coverage": {
            "snapshotRaces": len(snapshots),
            "fullyLabeledRaces": statuses["fully_labeled"],
            "partialRaces": statuses["partial"],
            "unlabeledRaces": statuses["unlabeled"],
            "integrityInvalidRaces": statuses["integrity_invalid"],
            "integrityInvalidReasons": dict(sorted(invalid_reasons.items())),
        },
        "overall": overall,
        "profiles": profiles,
        "formalReplaySupportedProfiles": [
            profile for profile, item in profiles.items() if item["formalReplaySupported"]
        ],
        "liveRolloutEligible": False,
    }


def markdown(report: dict[str, Any]) -> str:
    coverage = report["coverage"]
    lines = [
        "# Late-market AGF shadow",
        "",
        f"Version: `{report['version']}`",
        "",
        "Visible ranking and Telegram: **unchanged**.",
        "",
        f"Snapshots: {coverage['snapshotRaces']}; fully labeled: {coverage['fullyLabeledRaces']}; "
        f"partial: {coverage['partialRaces']}; invalid: {coverage['integrityInvalidRaces']}.",
        "",
        "| Profile | Clean | Baseline WTop3 | Candidate WTop3 | B/C cutoff gap | Rescue/Damage | Next | Status |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for profile in ("MAIDEN", "SART1"):
        item = report["profiles"][profile]
        total = item["cumulative"]
        lines.append(
            f"| {profile} | {total['races']} | {total['baseline']['winnerTop3']} | "
            f"{total['candidate']['winnerTop3']} | "
            f"{total['baselineSeparation']['avgTop3CutoffGap'] or '-'}/"
            f"{total['candidateSeparation']['avgTop3CutoffGap'] or '-'} | "
            f"{total['rescues']}/{total['damages']} | "
            f"{item['nextCheckpoint'] or '-'} | {item['status']} |"
        )
    lines.extend(
        [
            "",
            "No checkpoint automatically promotes this candidate; 15 clean races only unlock formal replay.",
            "",
        ]
    )
    return "\n".join(lines)


def persist(report: dict[str, Any], data_dir: Path) -> None:
    run_date = report["runDate"]
    daily = data_dir / "automation" / "runs" / run_date / "late-market-agf-checkpoint.json"
    latest = data_dir / "automation" / "late-market-agf" / "latest.json"
    latest_md = data_dir / "automation" / "late-market-agf" / "latest.md"
    payload = json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    atomic_write(daily, payload)
    atomic_write(latest, payload)
    atomic_write(latest_md, markdown(report))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshots", type=Path, required=True)
    parser.add_argument("--predictions", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--run-date", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    snapshots = load_jsonl(args.snapshots) if args.snapshots.exists() else []
    report = build_report(snapshots, load_jsonl(args.predictions), args.run_date)
    persist(report, args.data_dir)
    print(json.dumps({"success": True, **report["coverage"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
