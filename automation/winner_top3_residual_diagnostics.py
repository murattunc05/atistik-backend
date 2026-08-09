"""Explain unrescued Winner-Top3 misses and prepare race-safe pair data.

The report focuses on latest-labeled weight regimes for SART1 and HANDIKAP16.
It reuses the optimistic pair universe from the interaction diagnostic, then
compares each still-unrescued winner with the recomputed Top3 cutoff runner.
The emitted training pairs are split by whole race and contain no finish/rank
label inside their feature payload.  Nothing produced here affects ranking.
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
        active_weights,
        feature_value,
        group_races,
        load_jsonl,
        metric_source_proven,
        profile_of,
        rounded,
        safe_float,
        safe_int,
        score_diagnostics,
    )
    from automation.winner_top3_failure_diagnostics import field_bucket, summarize_performance
    from automation.winner_top3_interaction_diagnostics import (
        EXCLUDED_METRICS,
        baseline_race_event,
        candidate_event,
        metric_inventory,
        pair_directions,
    )
except ModuleNotFoundError:  # direct: python automation/winner_top3_residual_diagnostics.py
    from confidence_calibration import calibration_compatible, split_60_20_20, weight_fingerprint
    from metric_signal_registry import (
        METRIC_KEYS,
        active_weights,
        feature_value,
        group_races,
        load_jsonl,
        metric_source_proven,
        profile_of,
        rounded,
        safe_float,
        safe_int,
        score_diagnostics,
    )
    from winner_top3_failure_diagnostics import field_bucket, summarize_performance
    from winner_top3_interaction_diagnostics import (
        EXCLUDED_METRICS,
        baseline_race_event,
        candidate_event,
        metric_inventory,
        pair_directions,
    )


SCHEMA_VERSION = "winner-top3-residual-diagnostics-v1"
DATASET_SCHEMA_VERSION = "winner-top3-race-pairs-v1"
DEFAULT_PROFILES = ("SART1", "HANDIKAP16")
MIN_RESIDUAL_RACES = 6
EDGE_POINTS = 5.0
STRONG_EDGE_POINTS = 10.0
CONSISTENT_EDGE_RATE = 2.0 / 3.0
PROFILE_REPLAY_RACES = 30
PROFILE_OUTER_RACES = 6


METRIC_FAMILIES = {
    "recent_form_degree": {
        "degree_avg", "degree_trend", "degree_stability", "form_trend",
        "recent_finish_position_score", "bounce_score", "class_peak_score",
        "elite_consensus_score",
    },
    "surface_distance": {
        "track_suit", "track_experience_score", "surface_transition_score",
        "surface_switch_safety_score", "track_condition_suit_score",
        "distance_suit", "distance_transition_score",
    },
    "pace_position": {
        "pace_score", "pace_map_edge_score", "running_style_proxy_score",
        "start_draw_score", "late_start_risk_score", "favorite_risk_guard_score",
        "field_relative_value_score",
    },
    "handicap_load": {
        "weight_impact", "handicap_efficiency_score", "handicap_weight_relief_score",
        "handicap_class_transition_score", "handicap_load_value_score",
        "weight_change_risk_score", "handicap_class_load_transition_score",
        "handicap_age_curve_score", "hp_score", "age_score",
    },
    "connections_preparation": {
        "jockey_score", "trainer_score", "training_fitness", "training_degree_score",
        "pedigree",
    },
}

RAW_ROW_FIELDS = (
    "days_since_last_race",
    "distance",
    "last_race_distance",
    "race_count",
    "filtered_race_count",
)
RAW_FLAG_FIELDS = (
    "rawHp",
    "rawCurrentWeight",
    "rawStartNo",
    "parsedAge",
    "trainerRaceCount",
    "pedigreeOffspringRaces",
    "similarDistanceRaceCount",
    "targetTrackRaceCount",
    "handicapClassSampleCount",
    "recentFinishPositionSampleCount",
)
FORBIDDEN_FEATURE_KEYS = frozenset({
    "finish_pos", "is_winner", "horse_name", "rank_pred", "v4_rank", "label",
})


def metric_family(metric: str) -> str:
    for family, metrics in METRIC_FAMILIES.items():
        if metric in metrics:
            return family
    return "other"


def latest_labeled_regime(
    races: list[list[dict[str, Any]]],
) -> tuple[str | None, list[list[dict[str, Any]]], int]:
    buckets: dict[str, list[list[dict[str, Any]]]] = defaultdict(list)
    for rows in races:
        fingerprint = weight_fingerprint(rows[0])
        if fingerprint:
            buckets[fingerprint].append(rows)
    if not buckets:
        return None, [], 0
    fingerprint, latest = max(
        buckets.items(),
        key=lambda item: max(safe_int(row.get("ts"), 0) for rows in item[1] for row in rows),
    )
    return fingerprint, latest, len(buckets)


def replay_evidence(
    races: list[list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], int]:
    events = [
        event
        for rows in races
        if (event := baseline_race_event(rows)) is not None
    ]
    evidence = [event for event in events if event["replayAgreement"]]
    return evidence, len(events) - len(evidence)


def optimistic_unrescued(
    races: list[list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    evidence, replay_mismatches = replay_evidence(races)
    misses = [item for item in evidence if not item["baselineHit"]]
    inventory, eligible_metrics = metric_inventory(races)
    inventory_by_metric = {item["metric"]: item for item in inventory}
    rescued_keys: set[str] = set()
    direction_count = 0
    for first_metric, second_metric in itertools.combinations(eligible_metrics, 2):
        for deltas in pair_directions(first_metric, second_metric, inventory_by_metric):
            direction_count += 1
            for item in misses:
                if item["raceKey"] in rescued_keys:
                    continue
                candidate = candidate_event(item, deltas)
                if candidate["candidate"]:
                    rescued_keys.add(item["raceKey"])
    unrescued = [item for item in misses if item["raceKey"] not in rescued_keys]
    return unrescued, {
        "replayEvidenceRaces": len(evidence),
        "excludedReplayMismatchRaces": replay_mismatches,
        "baselineMisses": len(misses),
        "eligibleMetrics": eligible_metrics,
        "pairDirectionUniverseCount": direction_count,
        "rescuedByAnyPair": len(rescued_keys),
        "unrescuedByAnyPair": len(unrescued),
        "interpretation": "optimistic_upper_bound_only",
    }


def baseline_order(event: dict[str, Any]) -> list[dict[str, Any]]:
    return sorted(
        event["rows"],
        key=lambda row: (
            -event["baselineScores"][id(row)],
            safe_int(row.get("rank_pred")),
            str(row.get("horse_name") or ""),
        ),
    )


def cutoff_runner(event: dict[str, Any]) -> dict[str, Any]:
    return baseline_order(event)[2]


def comparison_runner(event: dict[str, Any]) -> tuple[dict[str, Any], str]:
    ordered = baseline_order(event)
    if not event["baselineHit"]:
        return ordered[2], "top3_cutoff"
    return next(row for row in ordered if row is not event["winner"]), "highest_scored_non_winner"


def raw_numeric(row: dict[str, Any]) -> dict[str, float | None]:
    flags = row.get("metric_source_flags") or {}
    if not isinstance(flags, dict):
        flags = {}
    values = {key: safe_float(row.get(key)) for key in RAW_ROW_FIELDS}
    values.update({key: safe_float(flags.get(key)) for key in RAW_FLAG_FIELDS})
    return values


def feature_comparison(
    winner_row: dict[str, Any],
    cutoff_row: dict[str, Any],
    weights: dict[str, float],
) -> list[dict[str, Any]]:
    comparisons = []
    for metric in METRIC_KEYS:
        if metric in EXCLUDED_METRICS:
            continue
        winner_value = feature_value(winner_row, metric)
        cutoff_value = feature_value(cutoff_row, metric)
        winner_source = metric_source_proven(winner_row, metric)
        cutoff_source = metric_source_proven(cutoff_row, metric)
        delta = (
            winner_value - cutoff_value
            if winner_source and cutoff_source
            and winner_value is not None and cutoff_value is not None
            else None
        )
        comparisons.append({
            "metric": metric,
            "family": metric_family(metric),
            "currentWeightPct": rounded(weights.get(metric, 0.0), 3),
            "winnerValue": rounded(winner_value, 3),
            "cutoffValue": rounded(cutoff_value, 3),
            "delta": rounded(delta, 3),
            "winnerHasSource": winner_source,
            "cutoffHasSource": cutoff_source,
        })
    return comparisons


def raw_comparison(
    winner_row: dict[str, Any],
    cutoff_row: dict[str, Any],
) -> list[dict[str, Any]]:
    winner_values = raw_numeric(winner_row)
    cutoff_values = raw_numeric(cutoff_row)
    rows = []
    for field in (*RAW_ROW_FIELDS, *RAW_FLAG_FIELDS):
        winner_value = winner_values.get(field)
        cutoff_value = cutoff_values.get(field)
        delta = (
            winner_value - cutoff_value
            if winner_value is not None and cutoff_value is not None
            else None
        )
        rows.append({
            "field": field,
            "winnerValue": rounded(winner_value, 3),
            "cutoffValue": rounded(cutoff_value, 3),
            "delta": rounded(delta, 3),
        })
    return rows


def residual_race(event: dict[str, Any]) -> dict[str, Any]:
    rows = event["rows"]
    first = rows[0]
    race_winner = event["winner"]
    cutoff = cutoff_runner(event)
    comparisons = feature_comparison(race_winner, cutoff, event["weights"])
    positive = sorted(
        (item for item in comparisons if item["delta"] is not None and item["delta"] > 0),
        key=lambda item: (-item["delta"], item["metric"]),
    )[:8]
    negative = sorted(
        (item for item in comparisons if item["delta"] is not None and item["delta"] < 0),
        key=lambda item: (item["delta"], item["metric"]),
    )[:8]
    winner_missing = sorted(
        item["metric"] for item in comparisons
        if not item["winnerHasSource"] and item["cutoffHasSource"]
    )
    cutoff_missing = sorted(
        item["metric"] for item in comparisons
        if item["winnerHasSource"] and not item["cutoffHasSource"]
    )
    score_detail = score_diagnostics(rows, bootstrap_iterations=0) or {}
    winner_score = event["baselineScores"][id(race_winner)]
    cutoff_score = event["baselineScores"][id(cutoff)]
    return {
        "raceKey": event["raceKey"],
        "raceDate": first.get("race_date"),
        "raceId": first.get("race_id"),
        "city": first.get("city"),
        "raceNo": first.get("race_no"),
        "raceType": first.get("race_type"),
        "track": profile_of(rows)["track"],
        "distance": safe_float(first.get("distance")),
        "fieldSize": len(rows),
        "fieldBucket": field_bucket(len(rows)),
        "winner": race_winner.get("horse_name"),
        "winnerRank": safe_int(race_winner.get("rank_pred")),
        "winnerScore": rounded(winner_score, 3),
        "cutoffRunner": cutoff.get("horse_name"),
        "cutoffRank": safe_int(cutoff.get("rank_pred")),
        "cutoffScore": rounded(cutoff_score, 3),
        "scoreDeficit": rounded(cutoff_score - winner_score, 3),
        "separationStatus": score_detail.get("separationStatus"),
        "weightedRealCoverage": score_detail.get("weightedRealCoverage"),
        "informativeWeightShare": score_detail.get("informativeWeightShare"),
        "positiveMetricEdges": positive,
        "negativeMetricEdges": negative,
        "winnerMissingSources": winner_missing,
        "cutoffMissingSources": cutoff_missing,
        "rawComparisons": raw_comparison(race_winner, cutoff),
        "allMetricComparisons": comparisons,
    }


def aggregate_metrics(residuals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for race in residuals:
        for item in race["allMetricComparisons"]:
            buckets[item["metric"]].append(item)
    metrics = []
    for metric in METRIC_KEYS:
        if metric in EXCLUDED_METRICS:
            continue
        items = buckets.get(metric, [])
        deltas = [float(item["delta"]) for item in items if item["delta"] is not None]
        comparable = len(deltas)
        winner_higher = sum(delta >= EDGE_POINTS for delta in deltas)
        cutoff_higher = sum(delta <= -EDGE_POINTS for delta in deltas)
        median_delta = statistics.median(deltas) if deltas else None
        positive_rate = winner_higher / comparable if comparable else 0.0
        winner_missing = sum(
            not item["winnerHasSource"] and item["cutoffHasSource"] for item in items
        )
        cutoff_missing = sum(
            item["winnerHasSource"] and not item["cutoffHasSource"] for item in items
        )
        weights = [float(item["currentWeightPct"]) for item in items]
        if (
            comparable >= MIN_RESIDUAL_RACES
            and positive_rate >= CONSISTENT_EDGE_RATE
            and median_delta is not None
            and median_delta >= EDGE_POINTS
        ):
            status = "CONSISTENT_WINNER_EDGE"
        elif winner_missing >= MIN_RESIDUAL_RACES and winner_missing > cutoff_missing:
            status = "WINNER_SOURCE_GAP"
        elif comparable < MIN_RESIDUAL_RACES:
            status = "COLLECTING"
        else:
            status = "INCONSISTENT"
        metrics.append({
            "metric": metric,
            "family": metric_family(metric),
            "residualRaces": len(items),
            "comparableRaces": comparable,
            "winnerEdgeRaces": winner_higher,
            "cutoffEdgeRaces": cutoff_higher,
            "strongWinnerEdgeRaces": sum(delta >= STRONG_EDGE_POINTS for delta in deltas),
            "strongCutoffEdgeRaces": sum(delta <= -STRONG_EDGE_POINTS for delta in deltas),
            "winnerEdgeRate": rounded(positive_rate, 4),
            "medianDelta": rounded(median_delta, 3),
            "meanDelta": rounded(statistics.mean(deltas), 3) if deltas else None,
            "winnerMissingSourceRaces": winner_missing,
            "cutoffMissingSourceRaces": cutoff_missing,
            "currentWeightPctMedian": rounded(statistics.median(weights), 3) if weights else 0.0,
            "status": status,
        })
    metrics.sort(
        key=lambda item: (
            item["status"] == "CONSISTENT_WINNER_EDGE",
            item["status"] == "WINNER_SOURCE_GAP",
            item["winnerEdgeRaces"],
            item["medianDelta"] if item["medianDelta"] is not None else -999.0,
            item["metric"],
        ),
        reverse=True,
    )
    return metrics


def aggregate_families(residuals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    results = []
    families = sorted(set(METRIC_FAMILIES) | {"other"})
    for family in families:
        best_positive = []
        best_negative = []
        for race in residuals:
            deltas = [
                float(item["delta"])
                for item in race["allMetricComparisons"]
                if item["family"] == family and item["delta"] is not None
            ]
            if deltas:
                best_positive.append(max(deltas))
                best_negative.append(min(deltas))
        results.append({
            "family": family,
            "comparableRaces": len(best_positive),
            "winnerStrongEdgeRaces": sum(value >= STRONG_EDGE_POINTS for value in best_positive),
            "cutoffStrongEdgeRaces": sum(value <= -STRONG_EDGE_POINTS for value in best_negative),
            "medianBestWinnerEdge": rounded(statistics.median(best_positive), 3) if best_positive else None,
            "medianWorstCutoffEdge": rounded(statistics.median(best_negative), 3) if best_negative else None,
        })
    return sorted(
        results,
        key=lambda item: (
            item["winnerStrongEdgeRaces"],
            item["medianBestWinnerEdge"] if item["medianBestWinnerEdge"] is not None else -999.0,
            item["family"],
        ),
        reverse=True,
    )


def cohort_summary(residuals: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for race in residuals:
        value = race.get(key)
        if key == "winnerRank":
            rank = safe_int(value)
            value = "rank4_5" if rank <= 5 else ("rank6_8" if rank <= 8 else "rank9_plus")
        buckets[str(value or "UNKNOWN")].append(race)
    return sorted(
        ({
            "key": bucket,
            "races": len(items),
            "averageWinnerRank": rounded(statistics.mean(item["winnerRank"] for item in items), 3),
            "averageScoreDeficit": rounded(statistics.mean(item["scoreDeficit"] for item in items), 3),
        } for bucket, items in buckets.items()),
        key=lambda item: (-item["races"], item["key"]),
    )


def runner_feature_payload(row: dict[str, Any]) -> dict[str, float | None]:
    return {
        metric: rounded(feature_value(row, metric), 4)
        if metric not in EXCLUDED_METRICS and metric_source_proven(row, metric)
        else None
        for metric in METRIC_KEYS
        if metric not in EXCLUDED_METRICS
    }


def runner_source_mask(row: dict[str, Any]) -> dict[str, int]:
    return {
        metric: int(metric_source_proven(row, metric))
        for metric in METRIC_KEYS
        if metric not in EXCLUDED_METRICS
    }


def build_training_pairs(
    profile: str,
    races: list[list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    evidence, replay_mismatches = replay_evidence(races)
    slices = split_60_20_20(evidence)
    split_by_event = {
        id(event): split_name
        for split_name, items in slices.items()
        for event in items
    }
    output = []
    pair_splits: dict[str, set[str]] = defaultdict(set)
    for event in evidence:
        opponent, opponent_role = comparison_runner(event)
        first = event["rows"][0]
        pair_id = event["raceKey"]
        split_name = split_by_event[id(event)]
        pair_splits[pair_id].add(split_name)
        for row, label, role in (
            (event["winner"], 1, "winner"),
            (opponent, 0, opponent_role),
        ):
            output.append({
                "datasetSchemaVersion": DATASET_SCHEMA_VERSION,
                "pairId": pair_id,
                "profile": profile,
                "split": split_name,
                "label": label,
                "role": role,
                "runnerKey": str(row.get("horse_name") or ""),
                "context": {
                    "raceDate": first.get("race_date"),
                    "raceId": first.get("race_id"),
                    "track": profile_of(event["rows"])["track"],
                    "distance": safe_float(first.get("distance")),
                    "fieldSize": len(event["rows"]),
                },
                "features": runner_feature_payload(row),
                "sourceMask": runner_source_mask(row),
                "rawNumeric": raw_numeric(row),
            })
    feature_keys = set()
    for item in output:
        feature_keys.update(item["features"])
        feature_keys.update(item["rawNumeric"])
    prohibited_inside_features = sorted(feature_keys & FORBIDDEN_FEATURE_KEYS)
    split_counts = Counter(item["split"] for item in output if item["label"] == 1)
    label_counts = Counter(str(item["label"]) for item in output)
    duplicate_split_pairs = sorted(
        pair_id for pair_id, names in pair_splits.items() if len(names) != 1
    )
    audit = {
        "profile": profile,
        "evidenceRaces": len(evidence),
        "excludedReplayMismatchRaces": replay_mismatches,
        "datasetRows": len(output),
        "pairs": len(pair_splits),
        "splitRaceCounts": dict(sorted(split_counts.items())),
        "labelCounts": dict(sorted(label_counts.items())),
        "raceLevelSplit": not duplicate_split_pairs,
        "pairsAppearingInMultipleSplits": duplicate_split_pairs,
        "prohibitedKeysInsideFeaturePayload": prohibited_inside_features,
        "agfExcluded": "agf_score" not in feature_keys,
        "leakageSafe": not duplicate_split_pairs and not prohibited_inside_features,
    }
    return output, audit


def build_profile_report(
    profile: str,
    all_profile_races: list[list[dict[str, Any]]],
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    fingerprint, latest_races, fingerprint_count = latest_labeled_regime(all_profile_races)
    if not latest_races:
        empty_audit = {
            "profile": profile,
            "evidenceRaces": 0,
            "datasetRows": 0,
            "pairs": 0,
            "raceLevelSplit": True,
            "prohibitedKeysInsideFeaturePayload": [],
            "agfExcluded": True,
            "leakageSafe": True,
        }
        return ({
            "profile": profile,
            "status": "NO_DATA",
            "compatibleProfileRaces": len(all_profile_races),
            "latestLabeledFingerprintRaces": 0,
            "residualRaces": 0,
            "topSignalMetrics": [],
            "residualDetails": [],
        }, [], empty_audit)
    unrescued_events, interaction = optimistic_unrescued(latest_races)
    residuals = [residual_race(event) for event in unrescued_events]
    metrics = aggregate_metrics(residuals)
    families = aggregate_families(residuals)
    training_rows, training_audit = build_training_pairs(profile, latest_races)
    signals = [
        item for item in metrics
        if item["status"] in {"CONSISTENT_WINNER_EDGE", "WINNER_SOURCE_GAP"}
    ]
    if len(latest_races) < PROFILE_REPLAY_RACES:
        status = "COLLECTING"
    elif signals:
        status = "RESIDUAL_SIGNAL_FOUND"
    elif residuals:
        status = "FEATURE_ENGINEERING_REQUIRED"
    else:
        status = "NO_UNRESCUED_MISS"
    return ({
        "profile": profile,
        "status": status,
        "compatibleProfileRaces": len(all_profile_races),
        "historicalFingerprintCount": fingerprint_count,
        "latestLabeledWeightFingerprint": fingerprint,
        "latestLabeledFingerprintRaces": len(latest_races),
        "latestLabeledRankingVersions": dict(sorted(Counter(
            str(rows[0].get("v4_version")) for rows in latest_races
        ).items())),
        "performance": summarize_performance(latest_races),
        "interactionContext": interaction,
        "residualRaces": len(residuals),
        "metricResiduals": metrics,
        "familyResiduals": families,
        "topSignalMetrics": signals[:12],
        "cohorts": {
            "track": cohort_summary(residuals, "track"),
            "field": cohort_summary(residuals, "fieldBucket"),
            "separation": cohort_summary(residuals, "separationStatus"),
            "winnerRank": cohort_summary(residuals, "winnerRank"),
        },
        "residualDetails": residuals,
        "trainingPairAudit": training_audit,
        "promotionGate": {
            "profileRaces": {"current": len(latest_races), "required": PROFILE_REPLAY_RACES},
            "outerRaces": {
                "current": training_audit.get("splitRaceCounts", {}).get("outer", 0),
                "required": PROFILE_OUTER_RACES,
            },
            "rankingImpact": False,
        },
    }, training_rows, training_audit)


def build_report(
    entries: list[dict[str, Any]],
    run_date: str,
    profiles: tuple[str, ...] = DEFAULT_PROFILES,
    *,
    invalid_json_lines: int = 0,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    all_races = group_races(entries)
    compatible = [rows for rows in all_races if calibration_compatible(rows)]
    buckets: dict[str, list[list[dict[str, Any]]]] = defaultdict(list)
    for rows in compatible:
        subtype = profile_of(rows)["subtype"]
        if subtype in profiles:
            buckets[subtype].append(rows)
    profile_reports = []
    training_rows = []
    audits = []
    for profile in profiles:
        profile_report, profile_training, audit = build_profile_report(
            profile, buckets.get(profile, [])
        )
        profile_reports.append(profile_report)
        training_rows.extend(profile_training)
        audits.append(audit)
    source_timestamp = max((safe_int(row.get("ts"), 0) for row in entries), default=0)
    report = {
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
        "trainingDataset": {
            "schemaVersion": DATASET_SCHEMA_VERSION,
            "rows": len(training_rows),
            "pairs": len(training_rows) // 2,
            "normalizedMetricColumns": [
                metric for metric in METRIC_KEYS if metric not in EXCLUDED_METRICS
            ],
            "rawNumericColumns": [*RAW_ROW_FIELDS, *RAW_FLAG_FIELDS],
            "audits": audits,
            "allProfilesLeakageSafe": all(item["leakageSafe"] for item in audits),
            "rankingImpact": False,
        },
        "policy": {
            "latestLabeledWeightRegimeOnly": True,
            "integritySafeVisibleV421PlusOnly": True,
            "optimisticPairRescueExcludesDamageFromPromotionEvidence": True,
            "minimumResidualRacesForConsistentSignal": MIN_RESIDUAL_RACES,
            "winnerEdgePoints": EDGE_POINTS,
            "strongWinnerEdgePoints": STRONG_EDGE_POINTS,
            "consistentEdgeRate": CONSISTENT_EDGE_RATE,
            "agfExcluded": True,
            "automaticWeightChange": False,
            "automaticModelRetrain": False,
            "rankingImpact": False,
            "nextMeaning": "chronological_shadow_retrain_input_only",
        },
    }
    return report, training_rows


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Winner Top3 Residual Diagnostics",
        "",
        f'- Run date: `{report["runDate"]}`',
        f'- Compatible clean races: `{report["input"]["compatibleCleanRaces"]}`',
        f'- Training pairs: `{report["trainingDataset"]["pairs"]}`; '
        f'leakage safe: `{str(report["trainingDataset"]["allProfilesLeakageSafe"]).lower()}`.',
        "- Ranking impact: `false`",
        "",
    ]
    for profile in report["profiles"]:
        lines.extend([
            f'## {profile["profile"]}',
            "",
            f'- Status: `{profile["status"]}`.',
            f'- Latest labeled regime: `{profile["latestLabeledFingerprintRaces"]}` races; '
            f'unrescued residuals: `{profile["residualRaces"]}`.',
        ])
        performance = profile.get("performance") or {}
        if performance:
            lines.append(
                f'- WTop3: `{performance["winnerTop3"]}/{performance["races"]}` '
                f'(`{performance["winnerTop3Rate"]}`).'
            )
        interaction = profile.get("interactionContext") or {}
        if interaction:
            lines.append(
                f'- Pair rescue upper bound: `{interaction.get("rescuedByAnyPair", 0)}/'
                f'{interaction.get("baselineMisses", 0)}`; still unrescued '
                f'`{interaction.get("unrescuedByAnyPair", 0)}`.'
            )
        lines.append("")
        top_metrics = (profile.get("topSignalMetrics") or profile.get("metricResiduals") or [])[:10]
        if top_metrics:
            lines.extend([
                "| Metric | Family | Comparable | Winner edge | Median delta | Weight | Status |",
                "|---|---|---:|---:|---:|---:|---|",
            ])
            for item in top_metrics:
                lines.append(
                    f'| {item["metric"]} | {item["family"]} | {item["comparableRaces"]} | '
                    f'{item["winnerEdgeRaces"]} | {item["medianDelta"]} | '
                    f'{item["currentWeightPctMedian"]} | {item["status"]} |'
                )
            lines.append("")
        if profile.get("residualDetails"):
            lines.extend(["Unrescued races:", ""])
            for race in profile["residualDetails"]:
                positive = ", ".join(
                    f'{item["metric"]} {item["delta"]:+.1f}'
                    for item in race["positiveMetricEdges"][:3]
                ) or "none"
                lines.append(
                    f'- `{race["raceDate"]}` `{race["raceId"]}` {race["track"]} '
                    f'{race["fieldSize"]} runners: `{race["winner"]}` rank '
                    f'`{race["winnerRank"]}`, deficit `{race["scoreDeficit"]}`; '
                    f'positive edges: {positive}.'
                )
            lines.append("")
    lines.extend([
        "## Policy",
        "",
        "- Only latest-labeled, integrity-safe, visible v4.21+ regimes are used.",
        "- AGF is excluded; its bounded SART1 shadow remains separate.",
        "- Entire races stay in one chronological dataset split.",
        "- Finish position and rank labels never enter the feature payload.",
        "- This artifact prepares shadow retraining and changes no ranking.",
        "",
    ])
    return "\n".join(lines)


def atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        handle.write(content)
        temporary = Path(handle.name)
    temporary.replace(path)


def persist(
    report: dict[str, Any],
    training_rows: list[dict[str, Any]],
    data_dir: Path,
) -> dict[str, str]:
    daily_dir = data_dir / "automation" / "runs" / report["runDate"]
    latest_dir = data_dir / "automation" / "residual-diagnostics"
    paths = {
        "dailyJson": daily_dir / "winner-top3-residual-diagnostics.json",
        "dailyMarkdown": daily_dir / "winner-top3-residual-diagnostics.md",
        "dailyTrainingPairs": daily_dir / "winner-top3-residual-training-pairs.jsonl",
        "latestJson": latest_dir / "latest.json",
        "latestMarkdown": latest_dir / "latest.md",
        "latestTrainingPairs": latest_dir / "training-pairs.latest.jsonl",
    }
    compact_json = json.dumps(report, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
    pretty_json = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    markdown = render_markdown(report)
    training_jsonl = "".join(
        json.dumps(item, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
        for item in training_rows
    )
    for key, path in paths.items():
        if key == "latestJson":
            content = compact_json
        elif key == "dailyJson":
            content = pretty_json
        elif "TrainingPairs" in key:
            content = training_jsonl
        else:
            content = markdown
        atomic_write(path, content)
    return {key: str(path) for key, path in paths.items()}


def main() -> int:
    parser = argparse.ArgumentParser(description="Explain unrescued Winner-Top3 residuals.")
    parser.add_argument("--predictions", required=True, type=Path)
    parser.add_argument("--data-dir", required=True, type=Path)
    parser.add_argument("--run-date", default=datetime.now().astimezone().strftime("%Y-%m-%d"))
    parser.add_argument("--profiles", default=",".join(DEFAULT_PROFILES))
    arguments = parser.parse_args()
    profiles = tuple(item.strip().upper() for item in arguments.profiles.split(",") if item.strip())
    entries, invalid_lines = load_jsonl(arguments.predictions)
    report, training_rows = build_report(
        entries,
        arguments.run_date,
        profiles,
        invalid_json_lines=invalid_lines,
    )
    paths = persist(report, training_rows, arguments.data_dir)
    print(json.dumps({
        "success": True,
        "runDate": report["runDate"],
        "input": report["input"],
        "trainingDataset": report["trainingDataset"],
        "profiles": [
            {
                "profile": item["profile"],
                "status": item["status"],
                "latestLabeledFingerprintRaces": item["latestLabeledFingerprintRaces"],
                "residualRaces": item["residualRaces"],
                "topSignalMetrics": item["topSignalMetrics"][:5],
                "promotionGate": item.get("promotionGate"),
            }
            for item in report["profiles"]
        ],
        "paths": paths,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
