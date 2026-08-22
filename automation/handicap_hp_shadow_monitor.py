#!/usr/bin/env python3
"""Fail-closed prospective monitor for the HANDIKAP hp-score -3 shadow."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

try:
    from automation.metric_signal_registry import (
        classify_race,
        competitive_race_rows,
        parse_race_date,
        race_sort_key,
        safe_float,
        safe_int,
    )
    from automation.sart1_shadow_monitor import (
        atomic_write,
        load_jsonl,
        ranking_guardrails,
    )
except ModuleNotFoundError as exc:  # pragma: no cover - direct execution
    if exc.name != "automation":
        raise
    from metric_signal_registry import (  # type: ignore[no-redef]
        classify_race,
        competitive_race_rows,
        parse_race_date,
        race_sort_key,
        safe_float,
        safe_int,
    )
    from sart1_shadow_monitor import (  # type: ignore[no-redef]
        atomic_write,
        load_jsonl,
        ranking_guardrails,
    )


EXPECTED_VERSION = "handicap-hp-minus3-20260823-v1"
EXPECTED_OBSERVATION_START = "23.08.2026"
EXPECTED_BASELINE_VERSION = "4.25"
EXPECTED_MODE = "prospective_shadow_bounded"
EXPECTED_METRIC = "hp_score"
EXPECTED_RAW_DELTA = -3.0
EXPECTED_ROBUST_TOP3_FLOOR = 0.4908
EXPECTED_AUDIT_SHA256 = (
    "28e7a0fd0cac6fe6d869b187bd3a139d18404e718385d56c7fe7338d1c579940"
)
EXPECTED_SOURCE_SNAPSHOT_SHA256 = (
    "2b4686b6e141b6422a0355b83cf2908547792d0be9e03da9ee158237bf76005d"
)
AUDIT_PATH = (
    Path(__file__).resolve().parent
    / "evidence"
    / "handicap_hp_minus3_audit_20260822.json"
)
CHECKPOINTS = (5, 10, 15)
ISTANBUL_TZ = ZoneInfo("Europe/Istanbul")
SOURCE_HASH_KEYS = (
    "metric",
    "metricSourceFlag",
    "metricSourceFlagPresent",
    "metricSourceFlagValue",
    "mfGuard",
    "mfGuardPresent",
    "mfGuardValue",
    "guardsAgree",
    "hasSource",
    "metricValue",
    "neutral",
    "actionable",
)


def canonical(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def file_sha256(path: Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def finite(value: Any) -> float | None:
    parsed = safe_float(value)
    return parsed if parsed is not None and math.isfinite(parsed) else None


def numeric_map(value: Any, *, allow_empty: bool = False) -> dict[str, float] | None:
    if not isinstance(value, dict) or (not value and not allow_empty):
        return None
    parsed: dict[str, float] = {}
    for key, raw in value.items():
        number = finite(raw)
        if not str(key).strip() or number is None or number < 0.0:
            return None
        if number > 0.0:
            parsed[str(key)] = number
    return parsed if parsed or allow_empty else None


def _same_scalar(rows: list[dict[str, Any]], key: str) -> bool:
    values = {
        str(row.get(key) if row.get(key) is not None else "").strip()
        for row in rows
    }
    return len(values) == 1 and bool(next(iter(values), ""))


def _same_object(rows: list[dict[str, Any]], key: str) -> bool:
    return len({canonical(row.get(key)) for row in rows}) == 1


def _identity_valid(rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return False
    scalar_keys = (
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
        "handicap_hp_candidate_version",
        "handicap_hp_candidate_observation_start",
        "handicap_hp_candidate_created_ts",
        "handicap_hp_candidate_baseline_version",
        "handicap_hp_candidate_definition_sha256",
        "handicap_hp_candidate_race_snapshot_sha256",
    )
    if any(not _same_scalar(rows, key) for key in scalar_keys):
        return False
    object_keys = (
        "handicap_hp_candidate_profile",
        "handicap_hp_candidate_baseline_weights",
        "handicap_hp_candidate_candidate_raw_weights",
        "handicap_hp_candidate_candidate_weights",
        "handicap_hp_candidate_weight_delta_pct",
    )
    if any(not _same_object(rows, key) for key in object_keys):
        return False
    names = [str(row.get("horse_name") or "").strip().casefold() for row in rows]
    if any(not name for name in names) or len(names) != len(set(names)):
        return False
    if safe_int(rows[0].get("field_size"), 0) != len(rows):
        return False
    race_day = parse_race_date(rows[0].get("race_date"))
    created_ts = safe_int(rows[0].get("handicap_hp_candidate_created_ts"), 0)
    try:
        hour, minute = (
            int(part)
            for part in str(rows[0].get("race_time") or "").replace(".", ":").split(":", 1)
        )
        race_start = race_day.replace(
            hour=hour,
            minute=minute,
            tzinfo=ISTANBUL_TZ,
        ) if race_day else None
    except (TypeError, ValueError):
        return False
    return bool(race_start and created_ts > 0 and created_ts < int(race_start.timestamp()))


def _definition_valid(rows: list[dict[str, Any]]) -> bool:
    first = rows[0]
    profile = first.get("handicap_hp_candidate_profile")
    baseline = numeric_map(first.get("handicap_hp_candidate_baseline_weights"))
    candidate_raw = numeric_map(
        first.get("handicap_hp_candidate_candidate_raw_weights"),
        allow_empty=True,
    )
    candidate_pct = numeric_map(first.get("handicap_hp_candidate_candidate_weights"))
    delta_pct = first.get("handicap_hp_candidate_weight_delta_pct")
    requested_delta = finite(
        first.get("handicap_hp_candidate_requested_raw_weight_delta_points")
    )
    actual_delta = finite(first.get("handicap_hp_candidate_actual_hp_raw_delta_points"))
    robust_floor = finite(first.get("handicap_hp_candidate_robust_top3_floor"))
    if (
        not isinstance(profile, dict)
        or profile.get("category") != "HANDIKAP"
        or not str(profile.get("selectedKey") or "")
        or baseline is None
        or candidate_raw is None
        or candidate_pct is None
        or not isinstance(delta_pct, dict)
        or requested_delta != EXPECTED_RAW_DELTA
        or actual_delta is None
        or robust_floor != EXPECTED_ROBUST_TOP3_FLOOR
        or abs(sum(baseline.values()) - 100.0) > 0.15
        or abs(sum(candidate_pct.values()) - 100.0) > 1e-5
    ):
        return False
    baseline_hp = baseline.get(EXPECTED_METRIC, 0.0)
    expected_candidate_hp = max(0.0, baseline_hp + EXPECTED_RAW_DELTA)
    expected_actual_delta = expected_candidate_hp - baseline_hp
    if (
        baseline_hp <= 0.0
        or abs(candidate_raw.get(EXPECTED_METRIC, 0.0) - expected_candidate_hp) > 1e-8
        or abs(actual_delta - expected_actual_delta) > 1e-8
        or abs(sum(candidate_raw.values()) - (100.0 + expected_actual_delta)) > 0.15
    ):
        return False
    raw_total = sum(candidate_raw.values())
    expected_pct = {
        metric: (weight / raw_total) * 100.0
        for metric, weight in candidate_raw.items()
    }
    all_metrics = set(baseline) | set(expected_pct)
    for metric in all_metrics:
        if abs(candidate_pct.get(metric, 0.0) - expected_pct.get(metric, 0.0)) > 1e-6:
            return False
        actual = finite(delta_pct.get(metric, 0.0))
        expected = candidate_pct.get(metric, 0.0) - baseline.get(metric, 0.0)
        if actual is None or abs(actual - expected) > 1e-6:
            return False

    definition_payload = {
        "schemaVersion": "handicap-hp-shadow-v1",
        "candidateVersion": EXPECTED_VERSION,
        "observationStart": EXPECTED_OBSERVATION_START,
        "baselineVersion": EXPECTED_BASELINE_VERSION,
        "profile": profile,
        "metric": EXPECTED_METRIC,
        "requestedRawWeightDeltaPoints": EXPECTED_RAW_DELTA,
        "actualHpRawDeltaPoints": actual_delta,
        "baselineWeights": baseline,
        "baselineRawTotal": round(sum(baseline.values()), 10),
        "candidateRawWeights": candidate_raw,
        "candidateRawTotal": round(raw_total, 10),
        "candidateWeights": candidate_pct,
        "weightDeltaPct": delta_pct,
        "normalization": "exported_v4_weights_minus_raw_points_floor_zero_then_normalize",
        "robustTop3Floor": EXPECTED_ROBUST_TOP3_FLOOR,
        "auditArtifact": "automation/evidence/handicap_hp_minus3_audit_20260822.json",
        "auditArtifactSha256": EXPECTED_AUDIT_SHA256,
        "sourceSnapshotSha256": EXPECTED_SOURCE_SNAPSHOT_SHA256,
    }
    expected_definition_sha = sha256_json(definition_payload)
    for row in rows:
        source = row.get("handicap_hp_candidate_source")
        if (
            str(row.get("handicap_hp_candidate_version") or "") != EXPECTED_VERSION
            or str(row.get("handicap_hp_candidate_mode") or "") != EXPECTED_MODE
            or str(row.get("handicap_hp_candidate_observation_start") or "")
            != EXPECTED_OBSERVATION_START
            or str(row.get("handicap_hp_candidate_baseline_version") or "")
            != EXPECTED_BASELINE_VERSION
            or str(row.get("handicap_hp_candidate_metric") or "") != EXPECTED_METRIC
            or bool(row.get("handicap_hp_candidate_used_for_ranking"))
            or bool(row.get("handicap_hp_candidate_telegram_visible"))
            or bool(row.get("handicap_hp_candidate_rollout_eligible"))
            or row.get("handicap_hp_candidate_formal_replay_only") is not True
            or row.get("handicap_hp_candidate_replay_top3_set_agreement") is not True
            or row.get("handicap_hp_candidate_race_evidence_eligible") is not True
            or row.get("handicap_hp_candidate_evidence_issue") not in (None, "")
            or str(row.get("handicap_hp_candidate_definition_sha256") or "")
            != expected_definition_sha
            or str(row.get("handicap_hp_candidate_audit_artifact_sha256") or "")
            != EXPECTED_AUDIT_SHA256
            or str(row.get("handicap_hp_candidate_source_snapshot_sha256") or "")
            != EXPECTED_SOURCE_SNAPSHOT_SHA256
            or not isinstance(source, dict)
            or source.get("metric") != EXPECTED_METRIC
            or source.get("metricSourceFlag") != "hasHp"
            or source.get("mfGuard") != "_has_hp"
            or source.get("guardsAgree") is not True
            or source.get("hasSource") is not True
        ):
            return False
    return True


def _runner_payload_valid(row: dict[str, Any]) -> bool:
    baseline_weights = numeric_map(row.get("handicap_hp_candidate_baseline_weights"))
    candidate_raw = numeric_map(
        row.get("handicap_hp_candidate_candidate_raw_weights"),
        allow_empty=True,
    )
    features = row.get("handicap_hp_candidate_feature_snapshot")
    guards = row.get("handicap_hp_candidate_source_guard_snapshot")
    components = row.get("handicap_hp_candidate_score_components")
    source = row.get("handicap_hp_candidate_source")
    if (
        baseline_weights is None
        or candidate_raw is None
        or not isinstance(features, dict)
        or not isinstance(guards, dict)
        or not isinstance(components, dict)
        or not isinstance(source, dict)
        or set(components) != set(baseline_weights) | set(candidate_raw)
    ):
        return False
    baseline_numerator = baseline_total = 0.0
    candidate_numerator = candidate_total = 0.0
    for metric, component in components.items():
        if not isinstance(component, dict):
            return False
        value = finite(component.get("value"))
        baseline_weight = finite(component.get("baselineRawWeightPoints"))
        candidate_weight = finite(component.get("candidateRawWeightPoints"))
        feature_value = finite(features.get(metric))
        if (
            None in (value, baseline_weight, candidate_weight, feature_value)
            or abs(value - feature_value) > 1e-8
            or abs(baseline_weight - baseline_weights.get(metric, 0.0)) > 1e-8
            or abs(candidate_weight - candidate_raw.get(metric, 0.0)) > 1e-8
        ):
            return False
        if bool(component.get("included")):
            baseline_numerator += value * baseline_weight
            baseline_total += baseline_weight
            candidate_numerator += value * candidate_weight
            candidate_total += candidate_weight
    if baseline_total <= 0.0 or candidate_total <= 0.0:
        return False
    penalty = max(0.0, safe_float(row.get("v4_penalty_total"), 0.0) or 0.0)
    replay_baseline = max(
        0.0,
        min(100.0, (baseline_numerator / baseline_total) - penalty),
    )
    candidate = max(
        0.0,
        min(100.0, (candidate_numerator / candidate_total) - penalty),
    )
    if (
        abs(replay_baseline - safe_float(
            row.get("handicap_hp_candidate_replay_baseline_score"), -999.0
        )) > 1e-7
        or abs(candidate - safe_float(
            row.get("handicap_hp_candidate_score"), -999.0
        )) > 1e-7
        or abs(safe_float(row.get("handicap_hp_candidate_baseline_score"), -999.0)
               - safe_float(row.get("v4_score"), -998.0)) > 1e-7
        or safe_int(row.get("handicap_hp_candidate_baseline_rank"), 0)
        != safe_int(row.get("v4_rank"), -1)
    ):
        return False
    source_hash = {key: source.get(key) for key in SOURCE_HASH_KEYS}
    expected_feature_hash = sha256_json({
        "horseName": str(row.get("horse_name") or "").strip(),
        "features": features,
        "sourceGuards": guards,
        "hpSource": source_hash,
    })
    return (
        expected_feature_hash
        == str(row.get("handicap_hp_candidate_feature_vector_sha256") or "")
    )


def _rank_and_race_hash_valid(rows: list[dict[str, Any]]) -> bool:
    if any(not _runner_payload_valid(row) for row in rows):
        return False
    expected_ranks = list(range(1, len(rows) + 1))
    for prefix in (
        "handicap_hp_candidate_baseline_rank",
        "handicap_hp_candidate_replay_baseline_rank",
        "handicap_hp_candidate_rank",
    ):
        if sorted(safe_int(row.get(prefix), 0) for row in rows) != expected_ranks:
            return False
    replay_ranked = sorted(
        rows,
        key=lambda row: (
            -safe_float(row.get("handicap_hp_candidate_replay_baseline_score"), -1.0),
            safe_int(row.get("v4_rank"), 999),
            str(row.get("horse_name") or ""),
        ),
    )
    candidate_ranked = sorted(
        rows,
        key=lambda row: (
            -safe_float(row.get("handicap_hp_candidate_score"), -1.0),
            safe_int(row.get("v4_rank"), 999),
            str(row.get("horse_name") or ""),
        ),
    )
    if any(
        safe_int(row.get("handicap_hp_candidate_replay_baseline_rank"), 0) != rank
        for rank, row in enumerate(replay_ranked, start=1)
    ) or any(
        safe_int(row.get("handicap_hp_candidate_rank"), 0) != rank
        for rank, row in enumerate(candidate_ranked, start=1)
    ):
        return False
    visible_top3 = {
        str(row.get("horse_name") or "").casefold()
        for row in sorted(rows, key=lambda row: safe_int(row.get("v4_rank"), 999))[:3]
    }
    replay_top3 = {
        str(row.get("horse_name") or "").casefold() for row in replay_ranked[:3]
    }
    if visible_top3 != replay_top3:
        return False
    boundary_margin = None
    if len(candidate_ranked) >= 4:
        boundary_margin = (
            safe_float(candidate_ranked[2].get("handicap_hp_candidate_score"), 0.0)
            - safe_float(candidate_ranked[3].get("handicap_hp_candidate_score"), 0.0)
        )
    if any(
        abs(
            safe_float(row.get("handicap_hp_candidate_top3_boundary_margin"), -999.0)
            - (boundary_margin if boundary_margin is not None else -999.0)
        ) > 1e-7
        for row in rows
    ):
        return False
    race_payload = {
        "definitionSha256": rows[0].get("handicap_hp_candidate_definition_sha256"),
        "createdTs": safe_int(rows[0].get("handicap_hp_candidate_created_ts"), 0),
        "replayTop3SetAgreement": True,
        "evidenceIssue": None,
        "candidateTop3BoundaryMargin": boundary_margin,
        "horses": sorted([
            {
                "horseName": str(row.get("horse_name") or "").strip(),
                "baselineScore": row.get("handicap_hp_candidate_baseline_score"),
                "baselineRank": row.get("handicap_hp_candidate_baseline_rank"),
                "replayBaselineScore": row.get("handicap_hp_candidate_replay_baseline_score"),
                "replayBaselineRank": row.get("handicap_hp_candidate_replay_baseline_rank"),
                "candidateScore": row.get("handicap_hp_candidate_score"),
                "candidateRank": row.get("handicap_hp_candidate_rank"),
                "featureVectorSha256": row.get("handicap_hp_candidate_feature_vector_sha256"),
            }
            for row in rows
        ], key=lambda item: item["horseName"].casefold()),
    }
    expected_hash = sha256_json(race_payload)
    return all(
        str(row.get("handicap_hp_candidate_race_snapshot_sha256") or "")
        == expected_hash
        for row in rows
    )


def candidate_valid(rows: list[dict[str, Any]]) -> bool:
    return bool(
        _identity_valid(rows)
        and _definition_valid(rows)
        and _rank_and_race_hash_valid(rows)
    )


def _competitive_order(
    rows: list[dict[str, Any]],
    rank_key: str,
) -> list[dict[str, Any]]:
    competitive_ids = {
        (str(row.get("horse_name") or "").casefold(), str(row.get("horse_no") or ""))
        for row in competitive_race_rows(rows)
    }
    selected = [
        row for row in rows
        if (str(row.get("horse_name") or "").casefold(), str(row.get("horse_no") or ""))
        in competitive_ids
    ]
    return sorted(
        selected,
        key=lambda row: (
            safe_int(row.get(rank_key), 999),
            str(row.get("horse_name") or ""),
        ),
    )


def _race_record(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    baseline_order = _competitive_order(rows, "handicap_hp_candidate_replay_baseline_rank")
    candidate_order = _competitive_order(rows, "handicap_hp_candidate_rank")
    winner = next((row for row in rows if safe_int(row.get("finish_pos"), 0) == 1), None)
    if winner is None or winner not in baseline_order or winner not in candidate_order:
        return None
    baseline_rank = baseline_order.index(winner) + 1
    candidate_rank = candidate_order.index(winner) + 1
    baseline_margin = None
    candidate_margin = None
    baseline_boundary = candidate_boundary = None
    if len(baseline_order) >= 4:
        baseline_fourth = safe_float(
            baseline_order[3].get("handicap_hp_candidate_replay_baseline_score"), 0.0
        )
        baseline_boundary = (
            safe_float(baseline_order[2].get("handicap_hp_candidate_replay_baseline_score"), 0.0)
            - baseline_fourth
        )
        if baseline_rank <= 3:
            baseline_margin = (
                safe_float(winner.get("handicap_hp_candidate_replay_baseline_score"), 0.0)
                - baseline_fourth
            )
    if len(candidate_order) >= 4:
        candidate_fourth = safe_float(
            candidate_order[3].get("handicap_hp_candidate_score"), 0.0
        )
        candidate_boundary = (
            safe_float(candidate_order[2].get("handicap_hp_candidate_score"), 0.0)
            - candidate_fourth
        )
        if candidate_rank <= 3:
            candidate_margin = (
                safe_float(winner.get("handicap_hp_candidate_score"), 0.0)
                - candidate_fourth
            )
    baseline_robust = bool(
        baseline_rank <= 3
        and (baseline_margin is None or baseline_margin >= EXPECTED_ROBUST_TOP3_FLOOR)
    )
    candidate_robust = bool(
        candidate_rank <= 3
        and (candidate_margin is None or candidate_margin >= EXPECTED_ROBUST_TOP3_FLOOR)
    )
    baseline_rank_rows = [
        {**row, "_monitor_rank": rank}
        for rank, row in enumerate(baseline_order, start=1)
    ]
    candidate_rank_rows = [
        {**row, "_monitor_rank": rank}
        for rank, row in enumerate(candidate_order, start=1)
    ]
    return {
        "raceId": str(winner.get("race_id") or ""),
        "raceDate": winner.get("race_date"),
        "raceNo": winner.get("race_no"),
        "city": winner.get("city"),
        "raceType": winner.get("race_type"),
        "track": winner.get("track"),
        "fieldSize": len(baseline_order),
        "winner": winner.get("horse_name"),
        "baselineWinnerRank": baseline_rank,
        "candidateWinnerRank": candidate_rank,
        "baselineWinnerTop3Margin": baseline_margin,
        "candidateWinnerTop3Margin": candidate_margin,
        "baselineTop3BoundaryMargin": baseline_boundary,
        "candidateTop3BoundaryMargin": candidate_boundary,
        "baselineRobustTop3": baseline_robust,
        "candidateRobustTop3": candidate_robust,
        "baselineGuardrails": ranking_guardrails(baseline_rank_rows, "_monitor_rank"),
        "candidateGuardrails": ranking_guardrails(candidate_rank_rows, "_monitor_rank"),
    }


def _ranking_metrics(races: list[dict[str, Any]], prefix: str) -> dict[str, Any]:
    ranks = [race[f"{prefix}WinnerRank"] for race in races]
    guards = [race[f"{prefix}Guardrails"] for race in races]
    def average(key: str) -> float | None:
        values = [value[key] for value in guards if value.get(key) is not None]
        return round(statistics.mean(values), 4) if values else None
    return {
        "top1": sum(rank == 1 for rank in ranks),
        "winnerTop3": sum(rank <= 3 for rank in ranks),
        "winnerTop5": sum(rank <= 5 for rank in ranks),
        "avgWinnerRank": round(statistics.mean(ranks), 4) if ranks else None,
        "mae": average("mae"),
        "rho": average("rho"),
        "ndcg5": average("ndcg5"),
    }


def summarize(races: list[dict[str, Any]]) -> dict[str, Any]:
    baseline = _ranking_metrics(races, "baseline")
    candidate = _ranking_metrics(races, "candidate")
    rescues = sum(
        race["baselineWinnerRank"] > 3 and race["candidateWinnerRank"] <= 3
        for race in races
    )
    damages = sum(
        race["baselineWinnerRank"] <= 3 and race["candidateWinnerRank"] > 3
        for race in races
    )
    robust_rescues = sum(
        race["baselineWinnerRank"] > 3
        and race["candidateWinnerRank"] <= 3
        and race["candidateRobustTop3"]
        for race in races
    )
    robust_hit_damages = sum(
        race["baselineRobustTop3"] and not race["candidateRobustTop3"]
        for race in races
    )
    baseline_boundaries = [
        race["baselineTop3BoundaryMargin"] for race in races
        if race["baselineTop3BoundaryMargin"] is not None
    ]
    candidate_boundaries = [
        race["candidateTop3BoundaryMargin"] for race in races
        if race["candidateTop3BoundaryMargin"] is not None
    ]
    return {
        "races": len(races),
        "baseline": baseline,
        "candidate": candidate,
        "rescues": rescues,
        "damages": damages,
        "winnerTop3Net": rescues - damages,
        "robustTop3Rescues": robust_rescues,
        "fragileTop3Rescues": rescues - robust_rescues,
        "robustHitDamages": robust_hit_damages,
        "top1Net": candidate["top1"] - baseline["top1"],
        "top5Net": candidate["winnerTop5"] - baseline["winnerTop5"],
        "averageBoundaryMargin": {
            "baseline": round(statistics.mean(baseline_boundaries), 4)
            if baseline_boundaries else None,
            "candidate": round(statistics.mean(candidate_boundaries), 4)
            if candidate_boundaries else None,
        },
    }


def checkpoint_pass(summary: dict[str, Any]) -> bool:
    baseline_margin = summary["averageBoundaryMargin"]["baseline"]
    candidate_margin = summary["averageBoundaryMargin"]["candidate"]
    return bool(
        summary["winnerTop3Net"] >= 0
        and summary["damages"] == 0
        and summary["robustHitDamages"] == 0
        and summary["top1Net"] >= -1
        and summary["top5Net"] >= 0
        and (
            baseline_margin is None
            or candidate_margin is None
            or candidate_margin >= baseline_margin * 0.90
        )
    )


def build_report(entries: list[dict[str, Any]], run_date: str) -> dict[str, Any]:
    artifact_sha = file_sha256(AUDIT_PATH)
    artifact_valid = artifact_sha == EXPECTED_AUDIT_SHA256
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        version = str(entry.get("handicap_hp_candidate_version") or "").strip()
        if version:
            grouped[(
                str(entry.get("race_date") or ""),
                str(entry.get("race_id") or ""),
                version,
            )].append(entry)
    coverage = {
        "fullyLabeledEvidenceRaces": 0,
        "partialRaces": 0,
        "unlabeledRaces": 0,
        "integrityInvalidRaces": 0,
        "preProspectiveExcludedRaces": 0,
        "nonOfficialExcludedRaces": 0,
    }
    complete: list[dict[str, Any]] = []
    versions: set[str] = set()
    for (_, _, version), rows in sorted(grouped.items(), key=lambda item: race_sort_key(item[1])):
        versions.add(version)
        race_day = parse_race_date(rows[0].get("race_date"))
        start_day = parse_race_date(rows[0].get("handicap_hp_candidate_observation_start"))
        if race_day is None or start_day is None:
            coverage["integrityInvalidRaces"] += 1
            continue
        if race_day < start_day:
            coverage["preProspectiveExcludedRaces"] += 1
            continue
        if not artifact_valid or not candidate_valid(rows):
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
        if not all(
            str(row.get("result_source") or "") == "tjk_official_results"
            for row in rows
        ):
            coverage["nonOfficialExcludedRaces"] += 1
            continue
        race = _race_record(rows)
        if race is None:
            coverage["integrityInvalidRaces"] += 1
            continue
        coverage["fullyLabeledEvidenceRaces"] += 1
        complete.append(race)
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
    regression = bool(checkpoints and not checkpoints[-1]["passed"])
    formal_supported = bool(
        len(complete) >= 15
        and len(checkpoints) == 3
        and all(checkpoint["passed"] for checkpoint in checkpoints)
        and cumulative["winnerTop3Net"] >= 1
        and cumulative["damages"] == 0
        and cumulative["robustTop3Rescues"] >= 1
        and cumulative["robustHitDamages"] == 0
    )
    if not artifact_valid:
        status = "HOLD_AUDIT_ARTIFACT_INTEGRITY"
    elif len(complete) < 5:
        status = "COLLECTING"
    elif regression:
        status = "REGRESSION_SIGNAL"
    elif len(complete) < 15:
        status = "EARLY_SIGNAL"
    elif formal_supported:
        status = "SUPPORTED_FOR_FORMAL_REPLAY"
    else:
        status = "REVIEW"
    return {
        "schemaVersion": "handicap-hp-shadow-monitor-v1",
        "runDate": run_date,
        "mode": "prospective_shadow_only",
        "primaryObjective": "winner_top3",
        "status": status,
        "artifactIntegrity": {
            "path": str(AUDIT_PATH),
            "expectedSha256": EXPECTED_AUDIT_SHA256,
            "actualSha256": artifact_sha,
            "valid": artifact_valid,
        },
        "candidateVersions": sorted(versions),
        "coverage": coverage,
        "cumulative": cumulative,
        "checkpoints": checkpoints,
        "separationPolicy": {
            "robustWinnerTop3Floor": EXPECTED_ROBUST_TOP3_FLOOR,
            "checkpointAverageBoundaryRetentionMin": 0.90,
            "robustHitDamageMax": 0,
        },
        "top1Policy": {
            "earlyCheckpointVeto": False,
            "formalCatastrophicLossMax": 1,
        },
        "regressionSignal": regression,
        "formalReplaySupported": formal_supported,
        "liveRolloutEligible": False,
        "telegramVisible": False,
        "promotionCeiling": "formal_replay_only",
        "nextCheckpointAt": next(
            (value for value in CHECKPOINTS if value > len(complete)),
            None,
        ),
        "races": complete,
    }


def markdown(report: dict[str, Any]) -> str:
    coverage = report["coverage"]
    summary = report["cumulative"]
    lines = [
        f"# HANDIKAP HP -3 Shadow - {report['runDate']}",
        "",
        f"- Status: **{report['status']}**",
        f"- Fully labeled official evidence: {coverage['fullyLabeledEvidenceRaces']}",
        f"- Partial / unlabeled / invalid: {coverage['partialRaces']} / "
        f"{coverage['unlabeledRaces']} / {coverage['integrityInvalidRaces']}",
        f"- Audit artifact valid: {report['artifactIntegrity']['valid']}",
        f"- Next checkpoint: {report['nextCheckpointAt']}",
        "- Visible ranking and Telegram: unchanged",
        "",
        "| Ranking | Top1 | Winner Top3 | Winner Top5 | Avg winner rank |",
        "|---|---:|---:|---:|---:|",
        f"| Replay baseline | {summary['baseline']['top1']} | "
        f"{summary['baseline']['winnerTop3']} | {summary['baseline']['winnerTop5']} | "
        f"{summary['baseline']['avgWinnerRank']} |",
        f"| HP -3 | {summary['candidate']['top1']} | "
        f"{summary['candidate']['winnerTop3']} | {summary['candidate']['winnerTop5']} | "
        f"{summary['candidate']['avgWinnerRank']} |",
        "",
        f"- Top3 rescue / damage / net: {summary['rescues']} / "
        f"{summary['damages']} / {summary['winnerTop3Net']}",
        f"- Robust / fragile rescues: {summary['robustTop3Rescues']} / "
        f"{summary['fragileTop3Rescues']}",
        f"- Robust-hit damages: {summary['robustHitDamages']}",
        "- Primary objective: Winner Top3. Top1 has no early checkpoint veto.",
        "",
        "| Checkpoint | Baseline WTop3 | Candidate WTop3 | Robust/fragile rescue | Damage | Passed |",
        "|---:|---:|---:|---:|---:|---|",
    ]
    for checkpoint in report["checkpoints"]:
        lines.append(
            f"| +{checkpoint['atRace']} | {checkpoint['baseline']['winnerTop3']} | "
            f"{checkpoint['candidate']['winnerTop3']} | "
            f"{checkpoint['robustTop3Rescues']}/{checkpoint['fragileTop3Rescues']} | "
            f"{checkpoint['damages']} | {'yes' if checkpoint['passed'] else 'no'} |"
        )
    if not report["checkpoints"]:
        lines.append("| +5 | 0 | 0 | 0/0 | 0 | collecting |")
    return "\n".join(lines) + "\n"


def persist(report: dict[str, Any], data_dir: Path) -> None:
    run_dir = data_dir / "automation" / "runs" / report["runDate"]
    latest_dir = data_dir / "automation" / "handicap-hp-shadow"
    encoded = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    rendered = markdown(report)
    for path, content in (
        (run_dir / "handicap-hp-shadow-checkpoint.json", encoded),
        (run_dir / "handicap-hp-shadow-checkpoint.md", rendered),
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
        "fullyLabeledEvidenceRaces": report["coverage"]["fullyLabeledEvidenceRaces"],
        "nextCheckpointAt": report["nextCheckpointAt"],
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
