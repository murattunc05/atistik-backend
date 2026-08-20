"""Fail-closed prospective monitor for the exact H15 training +2 shadow."""

from __future__ import annotations

import argparse
import hashlib
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

try:
    from automation.metric_signal_registry import competitive_race_rows
except ModuleNotFoundError as exc:
    if exc.name != "automation":
        raise
    from metric_signal_registry import competitive_race_rows  # type: ignore[no-redef]


EXPECTED_VERSION = "h15-training-degree-plus2-20260821-v1"
EXPECTED_OBSERVATION_START = "21.08.2026"
EXPECTED_BASELINE_VERSION = "4.25"
EXPECTED_PROFILE = "HANDIKAP15"
EXPECTED_METRIC = "training_degree_score"
EXPECTED_RAW_ADD_POINTS = 2.0
EXPECTED_MODE = "prospective_shadow_bounded"
EXPECTED_NORMALIZATION = "exported_v4_weights_plus_raw_points_then_normalize"
EXPECTED_BASELINE_TEMPERATURE = 14.0
EXPECTED_CANDIDATE_TEMPERATURE = 14.0
EXPECTED_REPLAY_SOURCE = "automation/runs/2026-08-19/metric-signal-replay.json"
EXPECTED_REPLAY_SHA256 = "f438858fe5d17979bda499bfb48c9b047a1366ddfe8e7510b3f83b7e2fe14e2b"
EXPECTED_CALIBRATION_EVIDENCE_ARTIFACT = (
    "automation/evidence/h15_training_degree_plus2_calibration_20260819.json"
)
EXPECTED_CALIBRATION_EVIDENCE_SHA256 = (
    "3e606f6c40f32f22858a24c10caa138f880e78b20494f237f7972d2d727f319e"
)
CALIBRATION_EVIDENCE_PATH = (
    Path(__file__).resolve().parent
    / "evidence"
    / "h15_training_degree_plus2_calibration_20260819.json"
)
EXPECTED_CALIBRATION_CONTRACT = {
    "fitScope": "build_only",
    "buildInnerOuter": [20, 7, 7],
    "baselineTemperature": EXPECTED_BASELINE_TEMPERATURE,
    "candidateTemperature": EXPECTED_CANDIDATE_TEMPERATURE,
    "sourceReport": EXPECTED_REPLAY_SOURCE,
    "sourceReportSha256": EXPECTED_REPLAY_SHA256,
    "evidenceArtifact": EXPECTED_CALIBRATION_EVIDENCE_ARTIFACT,
    "evidenceArtifactSha256": EXPECTED_CALIBRATION_EVIDENCE_SHA256,
}
CHECKPOINTS = (5, 10, 15)
TERMINAL_FINISH_POSITIONS = {99}
ISTANBUL_TZ = ZoneInfo("Europe/Istanbul")

SOURCE_GUARDS = {
    "agf_score": "_has_agf",
    "hp_score": "_has_hp",
    "weight_impact": "_has_weight",
    "jockey_score": "_has_jockey",
    "trainer_score": "_has_trainer",
    "training_fitness": "_has_training",
    "training_degree_score": "_has_training_times",
    "pedigree": "_has_pedigree",
    "age_score": "_has_age",
    "track_experience_score": "_has_track_experience",
    "surface_transition_score": "_has_surface_transition",
    "distance_transition_score": "_has_distance_transition",
    "handicap_efficiency_score": "_has_handicap_efficiency",
    "handicap_weight_relief_score": "_has_handicap_weight_relief",
    "handicap_class_transition_score": "_has_handicap_class_history",
    "handicap_load_value_score": "_has_handicap_load_value",
    "weight_change_risk_score": "_has_weight_change_risk",
    "handicap_class_load_transition_score": "_has_handicap_class_load_transition",
    "field_relative_value_score": "_has_field_relative_value",
    "pace_map_edge_score": "_has_pace_map_edge",
    "surface_switch_safety_score": "_has_surface_switch_safety",
    "favorite_risk_guard_score": "_has_favorite_risk_guard",
    "class_peak_score": "_has_class_peak",
    "elite_consensus_score": "_has_elite_consensus",
    "recent_finish_position_score": "_has_recent_finish_position",
    "start_draw_score": "_has_start_draw",
    "late_start_risk_score": "_has_late_start_risk",
    "track_condition_suit_score": "_has_track_condition_suit",
    "handicap_age_curve_score": "_has_handicap_age_curve",
}

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


def finite(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def numeric_weights(value: Any, *, allow_empty: bool = False) -> dict[str, float] | None:
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


def expected_formula(baseline_value: Any) -> dict[str, Any] | None:
    baseline = numeric_weights(baseline_value)
    if baseline is None:
        return None
    baseline_total = sum(baseline.values())
    if not (99.5 <= baseline_total <= 100.5):
        return None
    raw = dict(baseline)
    raw[EXPECTED_METRIC] = raw.get(EXPECTED_METRIC, 0.0) + EXPECTED_RAW_ADD_POINTS
    raw_total = sum(raw.values())
    candidate = {
        key: round(value / raw_total * 100.0, 10)
        for key, value in raw.items()
        if value > 0.0
    }
    delta = {
        key: round(candidate.get(key, 0.0) - baseline.get(key, 0.0), 10)
        for key in sorted(set(baseline) | set(candidate))
        if abs(candidate.get(key, 0.0) - baseline.get(key, 0.0)) > 1e-12
    }
    return {
        "baselineWeights": baseline,
        "baselineRawTotal": round(baseline_total, 10),
        "candidateRawWeights": raw,
        "candidateRawTotal": round(raw_total, 10),
        "candidateWeights": candidate,
        "weightDeltaPct": delta,
    }


def _common_value(rows: list[dict[str, Any]], key: str) -> Any | None:
    values = {canonical(row.get(key)) for row in rows}
    return rows[0].get(key) if len(values) == 1 else None


def _identity_valid(rows: list[dict[str, Any]]) -> bool:
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
        "h15_training_candidate_version",
        "h15_training_candidate_observation_start",
        "h15_training_candidate_created_ts",
        "h15_training_candidate_baseline_version",
    )
    for key in common_keys:
        values = {
            str(row.get(key) if row.get(key) is not None else "").strip()
            for row in rows
        }
        if len(values) != 1 or not next(iter(values)):
            return False
    common_objects = (
        "h15_training_candidate_profile",
        "h15_training_candidate_baseline_weights",
        "h15_training_candidate_raw_weights",
        "h15_training_candidate_weights",
        "h15_training_candidate_weight_delta_pct",
        "h15_training_candidate_definition_sha256",
        "h15_training_candidate_calibration_contract",
        "h15_training_candidate_race_snapshot_sha256",
    )
    if any(_common_value(rows, key) is None for key in common_objects):
        return False
    names = [str(row.get("horse_name") or "").strip().casefold() for row in rows]
    if any(not name for name in names) or len(names) != len(set(names)):
        return False
    if safe_int(rows[0].get("field_size"), 0) != len(rows):
        return False

    race_day = parse_race_date(rows[0].get("race_date"))
    race_time = str(rows[0].get("race_time") or "").strip().replace(".", ":")
    created_ts = safe_int(rows[0].get("h15_training_candidate_created_ts"), 0)
    if race_day is None or created_ts <= 0:
        return False
    try:
        hour, minute = (int(part) for part in race_time.split(":", 1))
        race_start = race_day.replace(hour=hour, minute=minute, tzinfo=ISTANBUL_TZ)
    except (TypeError, ValueError):
        return False
    return created_ts < int(race_start.timestamp())


def _definition_valid(rows: list[dict[str, Any]]) -> bool:
    first = rows[0]
    profile = first.get("h15_training_candidate_profile")
    formula = expected_formula(first.get("h15_training_candidate_baseline_weights"))
    if (
        not isinstance(profile, dict)
        or profile.get("category") != "HANDIKAP"
        or profile.get("subtype") != EXPECTED_PROFILE
        or not str(profile.get("selectedKey") or "")
        or formula is None
    ):
        return False
    observed_raw = numeric_weights(first.get("h15_training_candidate_raw_weights"))
    observed_weights = numeric_weights(first.get("h15_training_candidate_weights"))
    observed_delta = first.get("h15_training_candidate_weight_delta_pct")
    if (
        observed_raw is None
        or observed_weights is None
        or not isinstance(observed_delta, dict)
        or canonical(observed_raw) != canonical(formula["candidateRawWeights"])
        or abs(
            safe_float(first.get("h15_training_candidate_baseline_raw_total"), -1.0)
            - formula["baselineRawTotal"]
        ) > 1e-8
        or abs(
            safe_float(first.get("h15_training_candidate_raw_total"), -1.0)
            - formula["candidateRawTotal"]
        ) > 1e-8
    ):
        return False
    for metric in set(observed_weights) | set(formula["candidateWeights"]):
        if abs(
            observed_weights.get(metric, 0.0)
            - formula["candidateWeights"].get(metric, 0.0)
        ) > 1e-8:
            return False
    for metric in set(observed_delta) | set(formula["weightDeltaPct"]):
        actual = finite(observed_delta.get(metric, 0.0))
        if actual is None or abs(
            actual - formula["weightDeltaPct"].get(metric, 0.0)
        ) > 1e-8:
            return False

    definition_payload = {
        "schemaVersion": "h15-training-shadow-v1",
        "candidateVersion": EXPECTED_VERSION,
        "observationStart": EXPECTED_OBSERVATION_START,
        "baselineVersion": EXPECTED_BASELINE_VERSION,
        "profile": profile,
        "metric": EXPECTED_METRIC,
        "rawWeightAddPoints": EXPECTED_RAW_ADD_POINTS,
        "baselineWeights": formula["baselineWeights"],
        "baselineRawTotal": formula["baselineRawTotal"],
        "candidateRawWeights": formula["candidateRawWeights"],
        "candidateRawTotal": formula["candidateRawTotal"],
        "candidateWeights": formula["candidateWeights"],
        "weightDeltaPct": formula["weightDeltaPct"],
        "normalization": EXPECTED_NORMALIZATION,
        "calibrationContract": EXPECTED_CALIBRATION_CONTRACT,
    }
    expected_definition_hash = sha256(definition_payload)

    for row in rows:
        v4_profile = row.get("v4_profile")
        v4_weights = numeric_weights(row.get("v4_weights"))
        if (
            str(row.get("h15_training_candidate_version") or "") != EXPECTED_VERSION
            or str(row.get("h15_training_candidate_mode") or "") != EXPECTED_MODE
            or str(row.get("h15_training_candidate_observation_start") or "")
            != EXPECTED_OBSERVATION_START
            or str(row.get("h15_training_candidate_baseline_version") or "")
            != EXPECTED_BASELINE_VERSION
            or str(row.get("v4_version") or "") != EXPECTED_BASELINE_VERSION
            or str(row.get("h15_training_candidate_metric") or "") != EXPECTED_METRIC
            or abs(
                safe_float(row.get("h15_training_candidate_raw_weight_add_points"), -1.0)
                - EXPECTED_RAW_ADD_POINTS
            ) > 1e-9
            or bool(row.get("h15_training_candidate_used_for_ranking"))
            or bool(row.get("h15_training_candidate_telegram_visible"))
            or bool(row.get("h15_training_candidate_rollout_eligible"))
            or not bool(row.get("h15_training_candidate_formal_replay_only"))
            or not isinstance(v4_profile, dict)
            or canonical(v4_profile) != canonical(profile)
            or v4_weights is None
            or canonical(v4_weights) != canonical(formula["baselineWeights"])
            or str(row.get("h15_training_candidate_definition_sha256") or "")
            != expected_definition_hash
            or canonical(
                row.get("h15_training_candidate_calibration_contract")
            )
            != canonical(EXPECTED_CALIBRATION_CONTRACT)
        ):
            return False
    return True


def _source_and_feature_valid(rows: list[dict[str, Any]]) -> bool:
    source_count = 0
    actionable_count = 0
    neutral_count = 0
    for row in rows:
        source = row.get("h15_training_candidate_source")
        snapshot = row.get("h15_training_candidate_feature_snapshot")
        guards = row.get("h15_training_candidate_source_guard_snapshot")
        flags = row.get("metric_source_flags")
        if (
            not isinstance(source, dict)
            or not isinstance(snapshot, dict)
            or not isinstance(guards, dict)
            or not isinstance(flags, dict)
            or source.get("metric") != EXPECTED_METRIC
            or source.get("metricSourceFlag") != "hasTrainingTimes"
            or source.get("mfGuard") != "_has_training_times"
            or source.get("metricSourceFlagPresent") is not True
            or source.get("mfGuardPresent") is not True
            or source.get("guardsAgree") is not True
            or not isinstance(source.get("metricSourceFlagValue"), bool)
            or not isinstance(source.get("mfGuardValue"), bool)
            or source.get("metricSourceFlagValue") != source.get("mfGuardValue")
            or flags.get("hasTrainingTimes") != source.get("metricSourceFlagValue")
        ):
            return False
        metric_guard = guards.get("_has_training_times")
        if (
            not isinstance(metric_guard, dict)
            or metric_guard.get("present") is not True
            or metric_guard.get("value") != source.get("mfGuardValue")
        ):
            return False
        metric_value = finite(snapshot.get(EXPECTED_METRIC))
        source_metric_value = finite(source.get("metricValue"))
        if metric_value is None or source_metric_value is None or abs(metric_value - source_metric_value) > 1e-9:
            return False
        expected_has_source = bool(
            source.get("metricSourceFlagValue") and source.get("mfGuardValue")
        )
        expected_neutral = bool(expected_has_source and abs(metric_value - 50.0) < 1.0)
        expected_actionable = bool(expected_has_source and not expected_neutral)
        if (
            bool(source.get("hasSource")) != expected_has_source
            or bool(source.get("neutral")) != expected_neutral
            or bool(source.get("actionable")) != expected_actionable
        ):
            return False
        source_count += expected_has_source
        neutral_count += expected_neutral
        actionable_count += expected_actionable

        source_for_hash = {key: source.get(key) for key in SOURCE_HASH_KEYS}
        feature_payload = {
            "horseName": str(row.get("horse_name") or "").strip(),
            "features": snapshot,
            "sourceGuards": guards,
            "trainingSource": source_for_hash,
        }
        if str(row.get("h15_training_candidate_feature_vector_sha256") or "") != sha256(feature_payload):
            return False

    runner_count = len(rows)
    unavailable_count = runner_count - source_count
    for row in rows:
        source = row.get("h15_training_candidate_source") or {}
        if (
            safe_int(source.get("sourceCount"), -1) != source_count
            or safe_int(source.get("actionableCount"), -1) != actionable_count
            or safe_int(source.get("neutralCount"), -1) != neutral_count
            or safe_int(source.get("unavailableCount"), -1) != unavailable_count
            or safe_int(source.get("runnerCount"), -1) != runner_count
            or abs(safe_float(source.get("coverage"), -1.0) - source_count / runner_count) > 1e-6
            or abs(
                safe_float(source.get("actionableCoverage"), -1.0)
                - actionable_count / runner_count
            ) > 1e-6
        ):
            return False
    return True


def _score_and_rank_valid(rows: list[dict[str, Any]]) -> bool:
    first_weights = numeric_weights(rows[0].get("h15_training_candidate_weights"))
    formula = expected_formula(rows[0].get("h15_training_candidate_baseline_weights"))
    if first_weights is None or formula is None:
        return False
    replay_baseline_order: list[tuple[float, int, str, int]] = []
    candidate_order: list[tuple[float, int, str, int]] = []
    for index, row in enumerate(rows):
        baseline_score = finite(row.get("h15_training_candidate_baseline_score"))
        baseline_rank = safe_int(row.get("h15_training_candidate_baseline_rank"), 0)
        replay_baseline_rank = safe_int(
            row.get("h15_training_candidate_replay_baseline_rank"),
            0,
        )
        base_score = finite(row.get("h15_training_candidate_base_score"))
        penalty = finite(row.get("h15_training_candidate_penalty_total"))
        score = finite(row.get("h15_training_candidate_score"))
        rank = safe_int(row.get("h15_training_candidate_rank"), 0)
        snapshot = row.get("h15_training_candidate_feature_snapshot")
        guards = row.get("h15_training_candidate_source_guard_snapshot")
        components = row.get("h15_training_candidate_score_components")
        if (
            None in (baseline_score, base_score, penalty, score)
            or baseline_rank <= 0
            or replay_baseline_rank <= 0
            or rank <= 0
            or not isinstance(snapshot, dict)
            or not isinstance(guards, dict)
            or not isinstance(components, dict)
            or set(components) != set(first_weights)
            or abs(baseline_score - safe_float(row.get("v4_score"), -999.0)) > 1e-9
            or baseline_rank != safe_int(row.get("v4_rank"), 0)
            or baseline_rank != safe_int(row.get("rank_pred"), 0)
            or row.get("v4_applied_for_ranking") is not True
        ):
            return False

        baseline_numerator = 0.0
        baseline_denominator = 0.0
        for metric, weight_pct in first_weights.items():
            component = components.get(metric)
            if not isinstance(component, dict):
                return False
            value = finite(component.get("value"))
            snapshot_value = finite(snapshot.get(metric))
            component_weight = finite(component.get("weightPct"))
            baseline_raw_weight = finite(component.get("baselineRawWeightPoints"))
            candidate_raw_weight = finite(component.get("candidateRawWeightPoints"))
            guard = SOURCE_GUARDS.get(metric)
            guard_state = guards.get(guard) if guard else None
            expected_included = not (
                guard
                and isinstance(guard_state, dict)
                and bool(guard_state.get("present"))
                and not bool(guard_state.get("value"))
            )
            if (
                value is None
                or snapshot_value is None
                or component_weight is None
                or baseline_raw_weight is None
                or candidate_raw_weight is None
                or abs(value - snapshot_value) > 1e-9
                or abs(component_weight - weight_pct) > 1e-8
                or abs(
                    baseline_raw_weight
                    - formula["baselineWeights"].get(metric, 0.0)
                ) > 1e-9
                or abs(
                    candidate_raw_weight
                    - formula["candidateRawWeights"].get(metric, 0.0)
                ) > 1e-9
                or component.get("guard") != guard
                or bool(component.get("included")) != expected_included
            ):
                return False
            if expected_included and baseline_raw_weight > 0.0:
                baseline_numerator += value * baseline_raw_weight
                baseline_denominator += baseline_raw_weight
        source = row.get("h15_training_candidate_source") or {}
        added_value = finite(row.get("h15_training_candidate_added_metric_value"))
        if bool(source.get("hasSource")):
            if added_value is None or abs(added_value - finite(snapshot.get(EXPECTED_METRIC))) > 1e-9:
                return False
        elif row.get("h15_training_candidate_added_metric_value") is not None:
            return False
        candidate_numerator = baseline_numerator
        candidate_denominator = baseline_denominator
        if added_value is not None:
            candidate_numerator += added_value * EXPECTED_RAW_ADD_POINTS
            candidate_denominator += EXPECTED_RAW_ADD_POINTS
        expected_base = candidate_numerator / candidate_denominator if candidate_denominator > 0.0 else 50.0
        expected_score = max(0.0, min(100.0, expected_base - penalty))
        expected_replay_baseline_base = (
            baseline_numerator / baseline_denominator
            if baseline_denominator > 0.0
            else 50.0
        )
        expected_replay_baseline_score = max(
            0.0,
            min(100.0, expected_replay_baseline_base - penalty),
        )
        if (
            abs(
                safe_float(row.get("h15_training_candidate_baseline_weighted_numerator"), -1.0)
                - baseline_numerator
            ) > 1e-8
            or abs(
                safe_float(row.get("h15_training_candidate_baseline_available_weight_total"), -1.0)
                - baseline_denominator
            ) > 1e-8
            or abs(
                safe_float(row.get("h15_training_candidate_replay_baseline_base_score"), -1.0)
                - expected_replay_baseline_base
            ) > 1e-8
            or abs(
                safe_float(row.get("h15_training_candidate_replay_baseline_score"), -1.0)
                - expected_replay_baseline_score
            ) > 1e-8
            or abs(
                safe_float(row.get("h15_training_candidate_weighted_numerator"), -1.0)
                - candidate_numerator
            ) > 1e-8
            or abs(
                safe_float(row.get("h15_training_candidate_available_weight_total"), -1.0)
                - candidate_denominator
            ) > 1e-8
            or abs(base_score - expected_base) > 1e-8
            or abs(score - expected_score) > 1e-8
        ):
            return False
        horse_name = str(row.get("horse_name") or "")
        replay_baseline_order.append(
            (-expected_replay_baseline_score, baseline_rank, horse_name, index)
        )
        candidate_order.append((-score, baseline_rank, horse_name, index))

    valid_ranks = list(range(1, len(rows) + 1))
    baseline_ranks = [safe_int(row.get("h15_training_candidate_baseline_rank"), 0) for row in rows]
    replay_baseline_ranks = [
        safe_int(row.get("h15_training_candidate_replay_baseline_rank"), 0)
        for row in rows
    ]
    candidate_ranks = [safe_int(row.get("h15_training_candidate_rank"), 0) for row in rows]
    if (
        sorted(baseline_ranks) != valid_ranks
        or sorted(replay_baseline_ranks) != valid_ranks
        or sorted(candidate_ranks) != valid_ranks
    ):
        return False
    expected_replay_baseline_ranks = {
        original_index: rank + 1
        for rank, (_, _, _, original_index) in enumerate(
            sorted(replay_baseline_order)
        )
    }
    expected_candidate_ranks = {
        original_index: rank + 1
        for rank, (_, _, _, original_index) in enumerate(sorted(candidate_order))
    }
    if any(
        replay_baseline_ranks[index] != expected_replay_baseline_ranks[index]
        or candidate_ranks[index] != expected_candidate_ranks[index]
        for index in range(len(rows))
    ):
        return False

    visible_top3 = {
        index
        for index, rank in enumerate(baseline_ranks)
        if rank <= 3
    }
    replay_baseline_top3 = {
        index
        for index, rank in enumerate(replay_baseline_ranks)
        if rank <= 3
    }
    replay_top3_set_agreement = visible_top3 == replay_baseline_top3
    expected_issue = (
        None
        if replay_top3_set_agreement
        else "visible_replay_baseline_top3_set_mismatch"
    )
    actionable_count = safe_int(
        (rows[0].get("h15_training_candidate_source") or {}).get(
            "actionableCount"
        ),
        0,
    )
    # Pre-race eligibility records source actionability only. Final replay
    # compatibility is evaluated after official non-runners are filtered with
    # metric_signal_registry.competitive_race_rows.
    expected_evidence_eligible = actionable_count > 0
    for row in rows:
        if (
            row.get("h15_training_candidate_replay_top3_set_agreement")
            is not replay_top3_set_agreement
            or row.get("h15_training_candidate_evidence_issue")
            != expected_issue
            or row.get("h15_training_candidate_race_evidence_eligible")
            is not expected_evidence_eligible
        ):
            return False

    definition_hash = str(rows[0].get("h15_training_candidate_definition_sha256") or "")
    created_ts = safe_int(rows[0].get("h15_training_candidate_created_ts"), 0)
    race_payload = {
        "definitionSha256": definition_hash,
        "createdTs": created_ts,
        "replayTop3SetAgreement": replay_top3_set_agreement,
        "evidenceIssue": expected_issue,
        "horses": sorted(
            [
                {
                    "horseName": str(row.get("horse_name") or "").strip(),
                    "baselineScore": row.get("h15_training_candidate_baseline_score"),
                    "baselineRank": row.get("h15_training_candidate_baseline_rank"),
                    "replayBaselineScore": row.get(
                        "h15_training_candidate_replay_baseline_score"
                    ),
                    "replayBaselineRank": row.get(
                        "h15_training_candidate_replay_baseline_rank"
                    ),
                    "candidateScore": row.get("h15_training_candidate_score"),
                    "candidateRank": row.get("h15_training_candidate_rank"),
                    "featureVectorSha256": row.get("h15_training_candidate_feature_vector_sha256"),
                }
                for row in rows
            ],
            key=lambda item: item["horseName"].casefold(),
        ),
    }
    expected_race_hash = sha256(race_payload)
    return all(
        str(row.get("h15_training_candidate_race_snapshot_sha256") or "")
        == expected_race_hash
        for row in rows
    )


def validate_candidate(rows: list[dict[str, Any]]) -> tuple[bool, str | None]:
    validators = (
        ("identity", _identity_valid),
        ("definition", _definition_valid),
        ("source_or_feature", _source_and_feature_valid),
        ("score_rank_or_hash", _score_and_rank_valid),
    )
    for reason, validator in validators:
        if not validator(rows):
            return False, reason
    return True, None


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


def separation(rows: list[dict[str, Any]], rank_key: str, score_key: str) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda row: safe_int(row.get(rank_key), 999))
    if len(ordered) < 4:
        return {"top3CutoffGap": None, "cutoffCrowd1pt": None}
    top3_score = safe_float(ordered[2].get(score_key), 0.0)
    top4_score = safe_float(ordered[3].get(score_key), 0.0)
    gap = max(0.0, top3_score - top4_score)
    crowd = sum(
        abs(safe_float(row.get(score_key), 0.0) - top3_score) <= 1.0
        for row in rows
    )
    return {"top3CutoffGap": round(gap, 3), "cutoffCrowd1pt": crowd}


def plackett_luce_top3_probabilities(
    scores: list[float],
    temperature: float,
) -> list[float]:
    """Exact top-three inclusion probabilities used by metric_signal_replay."""
    if len(scores) <= 3:
        return [1.0] * len(scores)
    peak = max(scores)
    weights = [
        math.exp((score - peak) / max(temperature, 0.1))
        for score in scores
    ]
    total = sum(weights)
    inclusion = [0.0] * len(scores)
    for first, first_weight in enumerate(weights):
        first_probability = first_weight / total
        inclusion[first] += first_probability
        remaining_after_first = total - first_weight
        for second, second_weight in enumerate(weights):
            if second == first:
                continue
            second_probability = (
                first_probability * second_weight / remaining_after_first
            )
            inclusion[second] += second_probability
            remaining_after_second = remaining_after_first - second_weight
            for third, third_weight in enumerate(weights):
                if third in (first, second):
                    continue
                inclusion[third] += (
                    second_probability
                    * third_weight
                    / remaining_after_second
                )
    return [max(0.0, min(1.0, value)) for value in inclusion]


def calibration_metrics(
    values: list[tuple[float, int]],
    bins: int = 10,
) -> dict[str, Any]:
    if not values:
        return {"rows": 0, "brier": None, "ece": None}
    clipped = [
        (max(1e-8, min(1.0 - 1e-8, probability)), label)
        for probability, label in values
    ]
    brier = statistics.mean(
        (probability - label) ** 2
        for probability, label in clipped
    )
    bucketed: list[list[tuple[float, int]]] = [[] for _ in range(bins)]
    for probability, label in clipped:
        index = min(bins - 1, int(probability * bins))
        bucketed[index].append((probability, label))
    ece = sum(
        len(bucket) / len(clipped)
        * abs(
            statistics.mean(probability for probability, _ in bucket)
            - statistics.mean(label for _, label in bucket)
        )
        for bucket in bucketed
        if bucket
    )
    return {
        "rows": len(clipped),
        "brier": round(brier, 5),
        "ece": round(ece, 5),
    }


def calibration_evidence_status() -> dict[str, Any]:
    """Verify the deployed frozen replay artifact before opening calibration."""
    base = {
        "ready": False,
        "artifact": EXPECTED_CALIBRATION_EVIDENCE_ARTIFACT,
        "expectedArtifactSha256": EXPECTED_CALIBRATION_EVIDENCE_SHA256,
        "artifactSha256": None,
        "reason": None,
    }
    expected_hash = str(EXPECTED_CALIBRATION_EVIDENCE_SHA256 or "").lower()
    if (
        len(expected_hash) != 64
        or any(character not in "0123456789abcdef" for character in expected_hash)
    ):
        return {**base, "reason": "expected_artifact_sha256_invalid"}
    try:
        raw = CALIBRATION_EVIDENCE_PATH.read_bytes()
    except FileNotFoundError:
        return {**base, "reason": "artifact_missing"}
    except OSError:
        return {**base, "reason": "artifact_unreadable"}
    artifact_hash = hashlib.sha256(raw).hexdigest()
    with_hash = {**base, "artifactSha256": artifact_hash}
    if artifact_hash != expected_hash:
        return {**with_hash, "reason": "artifact_sha256_mismatch"}
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {**with_hash, "reason": "artifact_json_invalid"}
    if not isinstance(payload, dict):
        return {**with_hash, "reason": "artifact_schema_mismatch"}
    if payload.get("schemaVersion") != "h15-training-calibration-evidence-v1":
        return {**with_hash, "reason": "artifact_schema_mismatch"}

    source = payload.get("sourceReplayIdentity")
    candidate = payload.get("candidateIdentity")
    split = payload.get("splitRaces")
    temperature = payload.get("temperatureFit")
    policy = payload.get("policy")
    if not all(
        isinstance(value, dict)
        for value in (source, candidate, split, temperature, policy)
    ):
        return {**with_hash, "reason": "artifact_schema_mismatch"}

    identity_checks = (
        ("candidateId", payload.get("candidateId"), "PROFILE:HANDIKAP15/training_degree_score/plus2pp"),
        ("sourceReport", payload.get("sourceReport"), EXPECTED_REPLAY_SOURCE),
        ("sourceReportSha256", payload.get("sourceReportSha256"), EXPECTED_REPLAY_SHA256),
        ("replaySchema", source.get("schemaVersion"), "metric-signal-replay-v1"),
        ("replayRunDate", source.get("runDate"), "2026-08-19"),
        ("registryRunDate", source.get("registryRunDate"), "2026-08-19"),
        ("registrySchema", source.get("registrySchemaVersion"), "metric-signal-registry-v1"),
        ("sourceSnapshotAt", source.get("sourceSnapshotAt"), "2026-08-19T03:38:22+00:00"),
        ("minimumVersion", source.get("minimumCompatibleVersion"), "4.21"),
        ("scopeType", candidate.get("scopeType"), "PROFILE"),
        ("scopeKey", candidate.get("scopeKey"), EXPECTED_PROFILE),
        ("metric", candidate.get("metric"), EXPECTED_METRIC),
        ("addedWeightPoints", candidate.get("addedWeightPoints"), EXPECTED_RAW_ADD_POINTS),
        ("races", candidate.get("races"), 34),
        ("rankingVersions", candidate.get("rankingVersions"), {"4.21": 5, "4.23": 11, "4.24": 6, "4.25": 12}),
        ("candidateStatus", candidate.get("status"), "SUPPORTED_FOR_PROSPECTIVE_SHADOW"),
        ("candidateLive", candidate.get("liveRolloutEligible"), False),
        ("splitRaces", split, {"build": 20, "inner": 7, "outer": 7}),
        ("baselineTemperature", temperature.get("baseline"), EXPECTED_BASELINE_TEMPERATURE),
        ("candidateTemperature", temperature.get("candidate"), EXPECTED_CANDIDATE_TEMPERATURE),
        ("temperatureFitSplit", temperature.get("fitSplit"), "build"),
        ("buildOnly", policy.get("calibrationTemperatureFitOnBuildOnly"), True),
        ("passingMeaning", policy.get("passingMeaning"), "prospective_shadow_only"),
        ("liveWeightChanged", policy.get("liveWeightChanged"), False),
        ("policyLive", policy.get("liveRolloutEligible"), False),
        ("outerUntouched", policy.get("outerIsUntouched"), False),
    )
    for field, actual, expected in identity_checks:
        if actual != expected:
            return {
                **with_hash,
                "reason": "artifact_identity_mismatch",
                "mismatchField": field,
            }
    return {
        **with_hash,
        "ready": True,
        "reason": None,
        "candidateId": payload["candidateId"],
        "sourceReportSha256": payload["sourceReportSha256"],
        "splitRaces": split,
        "temperatureFit": temperature,
    }


def calibration_summary(races: list[dict[str, Any]]) -> dict[str, Any]:
    baseline_values = [
        value
        for race in races
        for value in race.get("baselineCalibrationRows", [])
    ]
    candidate_values = [
        value
        for race in races
        for value in race.get("candidateCalibrationRows", [])
    ]
    baseline = calibration_metrics(baseline_values)
    candidate = calibration_metrics(candidate_values)
    brier_delta = (
        round(candidate["brier"] - baseline["brier"], 5)
        if candidate["brier"] is not None and baseline["brier"] is not None
        else None
    )
    ece_delta = (
        round(candidate["ece"] - baseline["ece"], 5)
        if candidate["ece"] is not None and baseline["ece"] is not None
        else None
    )
    evidence = calibration_evidence_status()
    evidence_ready = bool(evidence["ready"])
    passed = bool(
        evidence_ready
        and brier_delta is not None
        and brier_delta <= 0.005
        and candidate["ece"] is not None
        and candidate["ece"] <= 0.10
        and ece_delta is not None
        and (candidate["ece"] <= 0.05 or ece_delta <= 0.01)
    )
    return {
        "evidenceReady": evidence_ready,
        "evidence": evidence,
        "fitScope": "build_only" if evidence_ready else None,
        "sourceReport": EXPECTED_REPLAY_SOURCE if evidence_ready else None,
        "sourceReportSha256": EXPECTED_REPLAY_SHA256 if evidence_ready else None,
        "baselineTemperature": EXPECTED_BASELINE_TEMPERATURE if evidence_ready else None,
        "candidateTemperature": EXPECTED_CANDIDATE_TEMPERATURE if evidence_ready else None,
        "baseline": baseline,
        "candidate": candidate,
        "brierDelta": brier_delta,
        "eceDelta": ece_delta,
        "thresholds": {
            "brierDeltaMax": 0.005,
            "candidateEceMax": 0.10,
            "candidateEceStrongMax": 0.05,
            "eceDeltaMaxOtherwise": 0.01,
        },
        "passed": passed,
    }


def race_calibration_rows(
    rows: list[dict[str, Any]],
    rank_key: str,
    score_key: str,
    temperature: float,
) -> list[tuple[float, int]]:
    ordered = sorted(rows, key=lambda row: safe_int(row.get(rank_key), 999))
    scores = [safe_float(row.get(score_key), 0.0) for row in ordered]
    probabilities = plackett_luce_top3_probabilities(scores, temperature)
    return [
        (
            probability,
            int(
                1 <= safe_int(row.get("finish_pos"), 999) <= 3
            ),
        )
        for row, probability in zip(ordered, probabilities)
    ]


def _metrics(races: list[dict[str, Any]], prefix: str) -> dict[str, Any]:
    ranks = [race[f"{prefix}WinnerRank"] for race in races]
    guardrails = [race[f"{prefix}Guardrails"] for race in races]

    def average(key: str) -> float | None:
        values = [guard[key] for guard in guardrails if guard.get(key) is not None]
        return round(statistics.mean(values), 4) if values else None

    mae, rho, ndcg5 = average("mae"), average("rho"), average("ndcg5")
    average_field = statistics.mean(race["fieldSize"] for race in races) if races else 1.0
    objective = None
    if None not in (mae, rho, ndcg5):
        objective = round(
            0.45 * ndcg5
            + 0.35 * ((rho + 1.0) / 2.0)
            + 0.20 * max(0.0, 1.0 - mae / max(average_field, 1.0)),
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
    non_causal_rescues = sum(
        race["baselineWinnerRank"] > 3
        and race["candidateWinnerRank"] <= 3
        and not race.get("winnerSourceActionable")
        for race in races
    )
    return {
        "races": len(races),
        "baseline": baseline,
        "candidate": candidate,
        "rescues": rescues,
        "damages": damages,
        "nonCausalRescues": non_causal_rescues,
        "winnerTop3Net": rescues - damages,
        "top1Net": candidate["top1"] - baseline["top1"],
        "top5Net": candidate["winnerTop5"] - baseline["winnerTop5"],
        "objectiveDelta": (
            round(candidate["objective"] - baseline["objective"], 4)
            if candidate["objective"] is not None and baseline["objective"] is not None
            else None
        ),
        "calibration": calibration_summary(races),
    }


def checkpoint_pass(summary: dict[str, Any]) -> bool:
    baseline, candidate = summary["baseline"], summary["candidate"]
    return bool(
        summary["winnerTop3Net"] >= 0
        and summary["damages"] == 0
        and summary["nonCausalRescues"] == 0
        and summary["top1Net"] >= -1
        and summary["top5Net"] >= 0
        and summary["objectiveDelta"] is not None
        and summary["objectiveDelta"] >= -0.002
        and candidate["mae"] is not None
        and baseline["mae"] is not None
        and candidate["mae"] <= baseline["mae"] + 0.10
        and candidate["rho"] is not None
        and baseline["rho"] is not None
        and candidate["rho"] >= baseline["rho"] - 0.02
        and candidate["ndcg5"] is not None
        and baseline["ndcg5"] is not None
        and candidate["ndcg5"] >= baseline["ndcg5"] - 0.005
        and summary["calibration"]["passed"]
    )


def _separation_quality(races: list[dict[str, Any]]) -> dict[str, Any]:
    baseline_gaps = [
        race["baselineSeparation"]["top3CutoffGap"]
        for race in races
        if race["baselineSeparation"]["top3CutoffGap"] is not None
    ]
    candidate_gaps = [
        race["candidateSeparation"]["top3CutoffGap"]
        for race in races
        if race["candidateSeparation"]["top3CutoffGap"] is not None
    ]
    baseline_tight = sum(value < 0.5 for value in baseline_gaps)
    candidate_tight = sum(value < 0.5 for value in candidate_gaps)
    baseline_avg = round(statistics.mean(baseline_gaps), 4) if baseline_gaps else None
    candidate_avg = round(statistics.mean(candidate_gaps), 4) if candidate_gaps else None
    comparable = min(len(baseline_gaps), len(candidate_gaps))
    passed = bool(
        comparable > 0
        and baseline_avg is not None
        and candidate_avg is not None
        and candidate_avg >= max(0.25, baseline_avg - 0.25)
        and candidate_tight <= baseline_tight + max(1, math.ceil(comparable * 0.10))
    )
    return {
        "comparableRaces": comparable,
        "baselineAvgTop3CutoffGap": baseline_avg,
        "candidateAvgTop3CutoffGap": candidate_avg,
        "baselineTightBoundaryRaces": baseline_tight,
        "candidateTightBoundaryRaces": candidate_tight,
        "passed": passed,
    }


def evidence_rankings(
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], bool]:
    """Reproduce metric_signal_replay's post-result competitive ordering."""
    competitive = competitive_race_rows(rows)
    if len(competitive) < 2:
        return competitive, False
    replay_order = sorted(
        competitive,
        key=lambda row: (
            -safe_float(
                row.get("h15_training_candidate_replay_baseline_score"),
                0.0,
            ),
            safe_int(row.get("rank_pred"), 999),
            str(row.get("horse_name") or ""),
        ),
    )
    candidate_order = sorted(
        competitive,
        key=lambda row: (
            -safe_float(row.get("h15_training_candidate_score"), 0.0),
            safe_int(row.get("rank_pred"), 999),
            str(row.get("horse_name") or ""),
        ),
    )
    replay_rank = {
        str(row.get("horse_name") or "").casefold(): rank
        for rank, row in enumerate(replay_order, start=1)
    }
    candidate_rank = {
        str(row.get("horse_name") or "").casefold(): rank
        for rank, row in enumerate(candidate_order, start=1)
    }
    evidence_rows = []
    for row in competitive:
        name_key = str(row.get("horse_name") or "").casefold()
        evidence_rows.append({
            **row,
            "h15_evidence_visible_rank": safe_int(row.get("rank_pred"), 999),
            "h15_evidence_replay_baseline_rank": replay_rank[name_key],
            "h15_evidence_candidate_rank": candidate_rank[name_key],
        })
    visible_top3 = {
        str(row.get("horse_name") or "").casefold()
        for row in sorted(
            evidence_rows,
            key=lambda row: safe_int(row.get("h15_evidence_visible_rank"), 999),
        )[:3]
    }
    replay_top3 = {
        str(row.get("horse_name") or "").casefold()
        for row in sorted(
            evidence_rows,
            key=lambda row: safe_int(
                row.get("h15_evidence_replay_baseline_rank"),
                999,
            ),
        )[:3]
    }
    return evidence_rows, visible_top3 == replay_top3


def build_report(entries: list[dict[str, Any]], run_date: str) -> dict[str, Any]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        version = str(entry.get("h15_training_candidate_version") or "").strip()
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
        "insufficientCompetitiveRowsExcludedRaces": 0,
        "replayBaselineTop3MismatchExcludedRaces": 0,
        "sourceUnavailableExcludedRaces": 0,
        "neutralOnlyExcludedRaces": 0,
    }
    failure_reasons: Counter[str] = Counter()
    issues: list[dict[str, Any]] = []
    complete: list[dict[str, Any]] = []
    versions: set[str] = set()
    for ((_, _, version), rows) in sorted(grouped.items(), key=lambda item: race_sort_key(item[1])):
        versions.add(version)
        race_day = parse_race_date(rows[0].get("race_date"))
        start_day = parse_race_date(rows[0].get("h15_training_candidate_observation_start"))
        if race_day is None or start_day is None:
            coverage["integrityInvalidRaces"] += 1
            failure_reasons["date"] += 1
            continue
        if race_day < start_day:
            coverage["preProspectiveExcludedRaces"] += 1
            continue
        valid, reason = validate_candidate(rows)
        if not valid:
            coverage["integrityInvalidRaces"] += 1
            failure_reasons[str(reason or "unknown")] += 1
            continue
        state = classify_race(rows)
        if state != "fully_labeled":
            key = {
                "partial": "partialRaces",
                "unlabeled": "unlabeledRaces",
            }.get(state, "integrityInvalidRaces")
            coverage[key] += 1
            continue

        evidence_rows, replay_top3_set_agreement = evidence_rankings(rows)
        if len(evidence_rows) < 2:
            coverage["insufficientCompetitiveRowsExcludedRaces"] += 1
            continue
        if not replay_top3_set_agreement:
            coverage["replayBaselineTop3MismatchExcludedRaces"] += 1
            visible_top3 = [
                str(row.get("horse_name") or "")
                for row in sorted(
                    evidence_rows,
                    key=lambda row: safe_int(
                        row.get("h15_evidence_visible_rank"),
                        999,
                    ),
                )[:3]
            ]
            replay_top3 = [
                str(row.get("horse_name") or "")
                for row in sorted(
                    evidence_rows,
                    key=lambda row: safe_int(
                        row.get("h15_evidence_replay_baseline_rank"),
                        999,
                    ),
                )[:3]
            ]
            issues.append({
                "code": "visible_replay_baseline_top3_set_mismatch",
                "raceId": str(rows[0].get("race_id") or ""),
                "raceDate": rows[0].get("race_date"),
                "raceNo": rows[0].get("race_no"),
                "city": rows[0].get("city"),
                "visibleTop3": visible_top3,
                "replayBaselineTop3": replay_top3,
                "effect": "excluded_from_performance_and_checkpoints",
            })
            continue

        source_rows = [
            row.get("h15_training_candidate_source") or {}
            for row in evidence_rows
        ]
        available = sum(bool(source.get("hasSource")) for source in source_rows)
        actionable = sum(bool(source.get("actionable")) for source in source_rows)
        neutral = sum(bool(source.get("neutral")) for source in source_rows)
        if available == 0:
            coverage["sourceUnavailableExcludedRaces"] += 1
            continue
        if actionable == 0:
            coverage["neutralOnlyExcludedRaces"] += 1
            continue

        coverage["fullyLabeledEvidenceRaces"] += 1
        winner = next(
            row
            for row in evidence_rows
            if safe_int(row.get("finish_pos"), 0) == 1
        )
        baseline_separation = separation(
            evidence_rows,
            "h15_evidence_replay_baseline_rank",
            "h15_training_candidate_replay_baseline_score",
        )
        candidate_separation = separation(
            evidence_rows,
            "h15_evidence_candidate_rank",
            "h15_training_candidate_score",
        )
        complete.append({
            "raceId": str(winner.get("race_id") or ""),
            "raceDate": winner.get("race_date"),
            "raceNo": winner.get("race_no"),
            "raceTime": winner.get("race_time"),
            "city": winner.get("city"),
            "raceType": winner.get("race_type"),
            "track": winner.get("track"),
            "distance": winner.get("distance"),
            "fieldSize": len(evidence_rows),
            "snapshotFieldSize": len(rows),
            "winner": winner.get("horse_name"),
            "candidateVersion": version,
            "candidateCreatedTs": safe_int(winner.get("h15_training_candidate_created_ts"), 0),
            "profile": winner.get("h15_training_candidate_profile"),
            "definitionSha256": winner.get("h15_training_candidate_definition_sha256"),
            "raceSnapshotSha256": winner.get("h15_training_candidate_race_snapshot_sha256"),
            "visibleWinnerRank": safe_int(
                winner.get("h15_evidence_visible_rank"),
                0,
            ),
            "baselineWinnerRank": safe_int(
                winner.get("h15_evidence_replay_baseline_rank"),
                0,
            ),
            "candidateWinnerRank": safe_int(
                winner.get("h15_evidence_candidate_rank"),
                0,
            ),
            "baselineGuardrails": ranking_guardrails(
                evidence_rows,
                "h15_evidence_replay_baseline_rank",
            ),
            "candidateGuardrails": ranking_guardrails(
                evidence_rows,
                "h15_evidence_candidate_rank",
            ),
            "baselineSeparation": baseline_separation,
            "candidateSeparation": candidate_separation,
            "baselineCalibrationRows": race_calibration_rows(
                evidence_rows,
                "h15_evidence_replay_baseline_rank",
                "h15_training_candidate_replay_baseline_score",
                EXPECTED_BASELINE_TEMPERATURE,
            ),
            "candidateCalibrationRows": race_calibration_rows(
                evidence_rows,
                "h15_evidence_candidate_rank",
                "h15_training_candidate_score",
                EXPECTED_CANDIDATE_TEMPERATURE,
            ),
            "winnerSourceActionable": bool(
                (winner.get("h15_training_candidate_source") or {}).get("actionable")
            ),
            "sourceCount": available,
            "actionableCount": actionable,
            "neutralCount": neutral,
            "scoreChangedCount": sum(
                abs(
                    safe_float(row.get("h15_training_candidate_score"), 0.0)
                    - safe_float(row.get("h15_training_candidate_replay_baseline_score"), 0.0)
                ) > 1e-9
                for row in evidence_rows
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

    runner_count = sum(race["fieldSize"] for race in complete)
    source_count = sum(race["sourceCount"] for race in complete)
    actionable_count = sum(race["actionableCount"] for race in complete)
    neutral_count = sum(race["neutralCount"] for race in complete)
    actionable_races = sum(race["actionableCount"] > 0 for race in complete)
    changed_races = sum(race["scoreChangedCount"] > 0 for race in complete)
    source_quality = {
        "runnerCount": runner_count,
        "sourceCount": source_count,
        "sourceCoverage": round(source_count / runner_count, 4) if runner_count else 0.0,
        "actionableCount": actionable_count,
        "actionableCoverage": round(actionable_count / runner_count, 4) if runner_count else 0.0,
        "neutralCount": neutral_count,
        "actionableRaceCount": actionable_races,
        "scoreChangedRaceCount": changed_races,
        "scoreChangedRaceRatio": round(changed_races / len(complete), 4) if complete else 0.0,
    }
    source_gate = bool(
        source_quality["sourceCoverage"] >= 0.50
        and source_quality["actionableCoverage"] >= 0.25
        and source_quality["scoreChangedRaceRatio"] >= 0.60
    )
    separation_quality = _separation_quality(complete)
    rank_quality = bool(
        cumulative["objectiveDelta"] is not None
        and cumulative["objectiveDelta"] >= 0.0
        and cumulative["candidate"]["mae"] is not None
        and cumulative["baseline"]["mae"] is not None
        and cumulative["candidate"]["mae"] <= cumulative["baseline"]["mae"]
        and cumulative["candidate"]["rho"] is not None
        and cumulative["baseline"]["rho"] is not None
        and cumulative["candidate"]["rho"] >= cumulative["baseline"]["rho"]
        and cumulative["candidate"]["ndcg5"] is not None
        and cumulative["baseline"]["ndcg5"] is not None
        and cumulative["candidate"]["ndcg5"] >= cumulative["baseline"]["ndcg5"]
    )
    formal_supported = bool(
        len(complete) >= 15
        and len(checkpoints) == 3
        and all(checkpoint["passed"] for checkpoint in checkpoints)
        and cumulative["winnerTop3Net"] >= 1
        and cumulative["damages"] == 0
        and cumulative["nonCausalRescues"] == 0
        and cumulative["top1Net"] >= 0
        and cumulative["top5Net"] >= 0
        and source_gate
        and separation_quality["passed"]
        and rank_quality
        and cumulative["calibration"]["passed"]
        and coverage["integrityInvalidRaces"] == 0
        and versions == {EXPECTED_VERSION}
    )
    regression = bool(checkpoints and not checkpoints[-1]["passed"])
    if not cumulative["calibration"]["evidenceReady"]:
        status = "HOLD_CALIBRATION_EVIDENCE"
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
    next_checkpoint = next((value for value in CHECKPOINTS if value > len(complete)), None)
    return {
        "runDate": run_date,
        "mode": "prospective_shadow_only",
        "status": status,
        "candidateVersions": sorted(versions),
        "coverage": coverage,
        "integrityFailureReasons": dict(sorted(failure_reasons.items())),
        "issues": issues,
        "cumulative": cumulative,
        "checkpoints": checkpoints,
        "sourceQuality": source_quality,
        "sourceGateReady": source_gate,
        "separationQuality": separation_quality,
        "rankQualityReady": rank_quality,
        "calibration": cumulative["calibration"],
        "calibrationGateReady": cumulative["calibration"]["passed"],
        "regressionSignal": regression,
        "formalReplaySupported": formal_supported,
        "liveRolloutEligible": False,
        "telegramVisible": False,
        "promotionCeiling": "formal_replay_only",
        "liveRolloutReason": (
            "Clean +5/+10/+15 prospective checkpoints only authorize a new "
            "formal replay. They never activate ranking or Telegram output."
        ),
        "nextCheckpointAt": next_checkpoint,
        "races": complete,
    }


def markdown(report: dict[str, Any]) -> str:
    coverage = report["coverage"]
    summary = report["cumulative"]
    baseline, candidate = summary["baseline"], summary["candidate"]
    lines = [
        f"# H15 Training Degree +2 Shadow - {report['runDate']}",
        "",
        f"- Status: **{report['status']}**",
        f"- Fully labeled actionable evidence: {coverage['fullyLabeledEvidenceRaces']}",
        f"- Partial / unlabeled / invalid: {coverage['partialRaces']} / "
        f"{coverage['unlabeledRaces']} / {coverage['integrityInvalidRaces']}",
        f"- Source unavailable / neutral-only excluded: "
        f"{coverage['sourceUnavailableExcludedRaces']} / {coverage['neutralOnlyExcludedRaces']}",
        f"- Visible/replay-baseline Top3 mismatch excluded: "
        f"{coverage['replayBaselineTop3MismatchExcludedRaces']}",
        f"- Pre-prospective excluded: {coverage['preProspectiveExcludedRaces']}",
        f"- Next checkpoint: {report['nextCheckpointAt']}",
        f"- Source / separation / rank quality: {report['sourceGateReady']} / "
        f"{report['separationQuality']['passed']} / {report['rankQualityReady']}",
        f"- Calibration evidence / gate: "
        f"{report['calibration']['evidenceReady']} / {report['calibrationGateReady']}",
        f"- Calibration Brier delta / candidate ECE / ECE delta: "
        f"{report['calibration']['brierDelta']} / "
        f"{report['calibration']['candidate']['ece']} / "
        f"{report['calibration']['eceDelta']}",
        "- Live ranking and Telegram: unchanged",
        "",
        "| Ranking | Top1 | Winner Top3 | Winner Top5 | Avg winner rank | MAE | Rho | NDCG@5 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
        f"| Replay baseline v4.25 | {baseline['top1']} | {baseline['winnerTop3']} | "
        f"{baseline['winnerTop5']} | {baseline['avgWinnerRank']} | {baseline['mae']} | "
        f"{baseline['rho']} | {baseline['ndcg5']} |",
        f"| Training degree +2 | {candidate['top1']} | {candidate['winnerTop3']} | "
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
    if report["issues"]:
        lines.extend(["", "## Evidence issues", ""])
        for issue in report["issues"]:
            lines.append(
                f"- `{issue['code']}`: {issue['raceDate']} "
                f"{issue['city']} race {issue['raceNo']} "
                f"(id={issue['raceId']}); checkpoint evidence excluded."
            )
    return "\n".join(lines) + "\n"


def persist(report: dict[str, Any], data_dir: Path) -> None:
    run_dir = data_dir / "automation" / "runs" / report["runDate"]
    latest_dir = data_dir / "automation" / "h15-training-shadow"
    encoded = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    rendered = markdown(report)
    for path, content in (
        (run_dir / "h15-training-shadow-checkpoint.json", encoded),
        (run_dir / "h15-training-shadow-checkpoint.md", rendered),
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
