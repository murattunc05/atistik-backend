"""Evaluate visible ranking, AGF-only, and local v4.18 AGF-free profiles.

This script is read-only by default. It consumes the ML export JSON, simulates
the current local v4 profile weights from logged feature values, and prints a
compact Markdown report for rollout decisions.
"""

import argparse
import json
import math
import statistics
import sys
import urllib.request
from collections import defaultdict

from api_server import (
    _V4_VERSION,
    calculate_v4_shadow_score,
    extract_v4_race_profile,
    resolve_v4_profile_weights,
)


DEFAULT_EXPORT_URL = "https://atistik-backend.onrender.com/api/ml-export?labeled_only=true"


def safe_int(value, default=999):
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def safe_float(value, default=50.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def load_entries(url):
    with urllib.request.urlopen(url, timeout=45) as response:
        payload = json.load(response)
    if isinstance(payload, dict):
        return payload.get("entries") or payload.get("data") or []
    return payload or []


def build_metric_flags(entry):
    flags = entry.get("metric_source_flags") or {}
    features = entry.get("features") or {}

    def has_source(flag_key, feature_key):
        if flag_key in flags:
            return bool(flags.get(flag_key))
        return features.get(feature_key) is not None

    return {
        "_has_agf": has_source("hasAgf", "agf_score"),
        "_has_hp": has_source("hasHp", "hp_score"),
        "_has_weight": has_source("hasWeight", "weight_impact"),
        "_has_jockey": has_source("hasJockey", "jockey_score"),
        "_has_trainer": has_source("hasTrainer", "trainer_score"),
        "_has_training": has_source("hasTraining", "training_fitness"),
        "_has_training_times": has_source("hasTrainingTimes", "training_degree_score"),
        "_has_pedigree": has_source("hasPedigree", "pedigree"),
        "_has_age": has_source("hasAgeActionable", "age_score"),
        "_has_track_experience": has_source("hasTrackExperience", "track_experience_score"),
        "_has_surface_transition": has_source("hasSurfaceTransition", "surface_transition_score"),
        "_has_distance_transition": has_source("hasDistanceTransition", "distance_transition_score"),
        "_has_handicap_efficiency": has_source("hasHandicapEfficiency", "handicap_efficiency_score"),
        "_has_handicap_class_history": has_source("hasHandicapClassHistory", "handicap_class_transition_score"),
    }


def simulate_v418(entry):
    profile = extract_v4_race_profile(
        entry.get("race_type", ""),
        entry.get("distance", ""),
        entry.get("track", ""),
        safe_int(entry.get("field_size"), 0),
    )
    resolved = resolve_v4_profile_weights(profile)
    metrics = dict(entry.get("features") or {})
    metrics.update(build_metric_flags(entry))
    score = calculate_v4_shadow_score(metrics, resolved["weights"])
    return score, profile, resolved, metrics


def group_races(entries):
    races = defaultdict(list)
    for entry in entries:
        if safe_int(entry.get("finish_pos"), 0) <= 0:
            continue
        score, profile, resolved, metrics = simulate_v418(entry)
        cloned = dict(entry)
        cloned["_sim_v418_score"] = score
        cloned["_sim_v418_profile"] = profile
        cloned["_sim_v418_resolved"] = resolved
        cloned["_sim_v418_metrics"] = metrics
        races[str(entry.get("race_id"))].append(cloned)
    return [rows for rows in races.values() if len(rows) >= 2]


def ordered_rows(rows, mode):
    if mode == "visible":
        return sorted(rows, key=lambda row: safe_int(row.get("rank_pred"), 999))
    if mode == "v4_logged":
        return sorted(rows, key=lambda row: safe_int(row.get("v4_rank"), 999))
    if mode == "agf_only":
        return sorted(
            rows,
            key=lambda row: safe_float((row.get("features") or {}).get("agf_score"), -999.0),
            reverse=True,
        )
    if mode == "v418":
        return sorted(rows, key=lambda row: safe_float(row.get("_sim_v418_score"), -999.0), reverse=True)
    raise ValueError(f"unknown mode: {mode}")


def rank_map(rows, mode):
    return {id(row): index + 1 for index, row in enumerate(ordered_rows(rows, mode))}


def ordered_by_score(rows, score_by_id):
    return sorted(rows, key=lambda row: score_by_id.get(id(row), -999.0), reverse=True)


def score_row_with_weights(row, weights):
    return calculate_v4_shadow_score(row.get("_sim_v418_metrics") or {}, weights)


def spearman_rho(predicted_ranks, finish_ranks):
    n = len(predicted_ranks)
    if n < 2:
        return None
    diff_sq = sum((predicted_ranks[i] - finish_ranks[i]) ** 2 for i in range(n))
    return 1.0 - (6.0 * diff_sq) / (n * (n * n - 1))


def ndcg_at_k(ordered_rows_value, k=5):
    if not ordered_rows_value:
        return None
    field_size = len(ordered_rows_value)
    finish_ordered = sorted(ordered_rows_value, key=lambda row: safe_int(row.get("finish_pos"), 999))
    finish_ranks = {id(row): index + 1 for index, row in enumerate(finish_ordered)}

    def relevance(row):
        finish_rank = finish_ranks.get(id(row), field_size)
        return max(0.0, (field_size - finish_rank + 1) / field_size)

    def dcg(rows):
        total = 0.0
        for index, row in enumerate(rows[:k], start=1):
            total += relevance(row) / math.log2(index + 1)
        return total

    ideal = finish_ordered
    ideal_dcg = dcg(ideal)
    if ideal_dcg <= 0:
        return None
    return dcg(ordered_rows_value) / ideal_dcg


def full_order_guardrails(rows, ordered):
    predicted_ranks = {id(row): index + 1 for index, row in enumerate(ordered)}
    finish_ordered = sorted(rows, key=lambda row: safe_int(row.get("finish_pos"), 999))
    finish_ranks = {id(row): index + 1 for index, row in enumerate(finish_ordered)}
    shared = [row for row in rows if id(row) in predicted_ranks and id(row) in finish_ranks]
    if not shared:
        return None, None, None
    pred = [predicted_ranks[id(row)] for row in shared]
    finish = [finish_ranks[id(row)] for row in shared]
    mae = statistics.mean(abs(pred[index] - finish[index]) for index in range(len(shared)))
    rho = spearman_rho(pred, finish)
    ndcg5 = ndcg_at_k(ordered, 5)
    return mae, rho, ndcg5


def evaluate_custom(races, score_builder):
    top1 = top3 = top5 = 0
    winner_ranks = []
    pred1_finishes = []
    score_gaps = []
    mae_values = []
    rho_values = []
    ndcg5_values = []
    close_winner = 0
    surprise_top3 = surprise_top5 = surprise_total = 0

    for rows in races:
        score_by_id = {id(row): score_builder(row) for row in rows}
        ordered = ordered_by_score(rows, score_by_id)
        ranks = {id(row): index + 1 for index, row in enumerate(ordered)}
        winner = min(rows, key=lambda row: safe_int(row.get("finish_pos"), 999))
        winner_rank = ranks.get(id(winner), 999)
        winner_ranks.append(winner_rank)
        top1 += int(winner_rank == 1)
        top3 += int(winner_rank <= 3)
        top5 += int(winner_rank <= 5)
        pred1_finishes.append(safe_int(ordered[0].get("finish_pos"), 999))
        mae, rho, ndcg5 = full_order_guardrails(rows, ordered)
        if mae is not None:
            mae_values.append(mae)
        if rho is not None:
            rho_values.append(rho)
        if ndcg5 is not None:
            ndcg5_values.append(ndcg5)

        top_score = score_by_id.get(id(ordered[0]), 0.0)
        winner_score = score_by_id.get(id(winner), 0.0)
        gap = round(max(0.0, top_score - winner_score), 2)
        score_gaps.append(gap)
        if top_score <= 0 or gap <= top_score * 0.10:
            close_winner += 1

        agf_ranks = rank_map(rows, "agf_only")
        if agf_ranks.get(id(winner), 999) > 3:
            surprise_total += 1
            surprise_top3 += int(winner_rank <= 3)
            surprise_top5 += int(winner_rank <= 5)

    total = len(winner_ranks)
    return {
        "races": total,
        "top1": top1,
        "winner_top3": top3,
        "winner_top5": top5,
        "avg_winner_rank": round(statistics.mean(winner_ranks), 2) if winner_ranks else None,
        "avg_pred1_finish": round(statistics.mean(pred1_finishes), 2) if pred1_finishes else None,
        "avg_score_gap": round(statistics.mean(score_gaps), 2) if score_gaps else None,
        "mae": round(statistics.mean(mae_values), 2) if mae_values else None,
        "rho": round(statistics.mean(rho_values), 3) if rho_values else None,
        "ndcg5": round(statistics.mean(ndcg5_values), 3) if ndcg5_values else None,
        "winner_close_10pct": close_winner if score_gaps else None,
        "surprise_total": surprise_total,
        "surprise_top3": surprise_top3,
        "surprise_top5": surprise_top5,
    }


def evaluate(races, mode):
    top1 = top3 = top5 = 0
    winner_ranks = []
    pred1_finishes = []
    score_gaps = []
    mae_values = []
    rho_values = []
    ndcg5_values = []
    close_winner = 0
    surprise_top3 = surprise_top5 = surprise_total = 0

    for rows in races:
        ordered = ordered_rows(rows, mode)
        ranks = {id(row): index + 1 for index, row in enumerate(ordered)}
        winner = min(rows, key=lambda row: safe_int(row.get("finish_pos"), 999))
        winner_rank = ranks.get(id(winner), 999)
        winner_ranks.append(winner_rank)
        top1 += int(winner_rank == 1)
        top3 += int(winner_rank <= 3)
        top5 += int(winner_rank <= 5)
        pred1_finishes.append(safe_int(ordered[0].get("finish_pos"), 999))
        mae, rho, ndcg5 = full_order_guardrails(rows, ordered)
        if mae is not None:
            mae_values.append(mae)
        if rho is not None:
            rho_values.append(rho)
        if ndcg5 is not None:
            ndcg5_values.append(ndcg5)

        top_score = safe_float(ordered[0].get("_sim_v418_score"), 0.0) if mode == "v418" else None
        winner_score = safe_float(winner.get("_sim_v418_score"), 0.0) if mode == "v418" else None
        if top_score is not None and winner_score is not None:
            gap = round(max(0.0, top_score - winner_score), 2)
            score_gaps.append(gap)
            if top_score <= 0 or gap <= top_score * 0.10:
                close_winner += 1

        agf_ranks = rank_map(rows, "agf_only")
        if agf_ranks.get(id(winner), 999) > 3:
            surprise_total += 1
            surprise_top3 += int(winner_rank <= 3)
            surprise_top5 += int(winner_rank <= 5)

    total = len(winner_ranks)
    return {
        "races": total,
        "top1": top1,
        "winner_top3": top3,
        "winner_top5": top5,
        "avg_winner_rank": round(statistics.mean(winner_ranks), 2) if winner_ranks else None,
        "avg_pred1_finish": round(statistics.mean(pred1_finishes), 2) if pred1_finishes else None,
        "avg_score_gap": round(statistics.mean(score_gaps), 2) if score_gaps else None,
        "mae": round(statistics.mean(mae_values), 2) if mae_values else None,
        "rho": round(statistics.mean(rho_values), 3) if rho_values else None,
        "ndcg5": round(statistics.mean(ndcg5_values), 3) if ndcg5_values else None,
        "winner_close_10pct": close_winner if score_gaps else None,
        "surprise_total": surprise_total,
        "surprise_top3": surprise_top3,
        "surprise_top5": surprise_top5,
    }


def segment_key(rows):
    return rows[0].get("_sim_v418_profile", {}).get("category", "GLOBAL")


def fmt_metrics(label, metrics):
    races = metrics["races"] or 0
    close = "-"
    if metrics["winner_close_10pct"] is not None:
        close = f"{metrics['winner_close_10pct']}/{races}"
    surprise = "-"
    if metrics["surprise_total"]:
        surprise = f"{metrics['surprise_top3']}/{metrics['surprise_total']}"
    return (
        f"| {label} | {races} | {metrics['top1']} | {metrics['winner_top3']} | "
        f"{metrics['winner_top5']} | {metrics['avg_winner_rank']} | "
        f"{metrics['avg_pred1_finish']} | {metrics['avg_score_gap'] or '-'} | "
        f"{close} | {surprise} |"
    )


def fmt_guardrails(label, metrics):
    return (
        f"| {label} | {metrics['races']} | {metrics.get('mae')} | "
        f"{metrics.get('rho')} | {metrics.get('ndcg5')} |"
    )


def metric_row(label, metrics, baseline=None):
    delta_top3 = "-"
    delta_rank = "-"
    if baseline:
        delta_top3 = metrics["winner_top3"] - baseline["winner_top3"]
        if metrics["avg_winner_rank"] is not None and baseline["avg_winner_rank"] is not None:
            delta_rank = round(metrics["avg_winner_rank"] - baseline["avg_winner_rank"], 2)
    return (
        f"| {label} | {metrics['races']} | {metrics['top1']} | {metrics['winner_top3']} | "
        f"{metrics['winner_top5']} | {metrics['avg_winner_rank']} | "
        f"{metrics['avg_pred1_finish']} | {delta_top3} | {delta_rank} |"
    )


def row_weights(row):
    return (row.get("_sim_v418_resolved") or {}).get("weights") or {}


def handicap_profile_key(rows):
    profile = rows[0].get("_sim_v418_profile") or {}
    return profile.get("subtype") or "HANDIKAP"


def handicap_selected_key(rows):
    resolved = rows[0].get("_sim_v418_resolved") or {}
    return resolved.get("selectedKey") or "HANDIKAP"


def handicap_track_key(rows):
    profile = rows[0].get("_sim_v418_profile") or {}
    return profile.get("track") or "Unknown"


def handicap_distance_key(rows):
    profile = rows[0].get("_sim_v418_profile") or {}
    return profile.get("distanceBucket") or "unknown"


def handicap_field_key(rows):
    profile = rows[0].get("_sim_v418_profile") or {}
    return profile.get("fieldBucket") or "unknown"


def print_breakdown(title, races, key_fn):
    grouped = defaultdict(list)
    for rows in races:
        grouped[key_fn(rows)].append(rows)
    print()
    print(f"## {title}")
    print()
    print("| Segment | Races | Top1 | Winner Top3 | Winner Top5 | Avg Winner Rank | Avg Pred1 Finish | Avg Score Gap | Winner <=10% | Surprise WTop3 |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for key in sorted(grouped):
        print(fmt_metrics(str(key), evaluate(grouped[key], "v418")))


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
    "handicap_class_transition_score": "_has_handicap_class_history",
}


def active_weighted_metrics(segment_races):
    metrics = set()
    for race in segment_races:
        for row in race:
            for key, weight in row_weights(row).items():
                if safe_float(weight, 0.0) > 0:
                    metrics.add(key)
    return sorted(metrics)


def avg_weight_pct(rows, metric):
    if not rows:
        return 0.0
    weights = [safe_float(row_weights(row).get(metric), 0.0) * 100.0 for row in rows]
    return round(statistics.mean(weights), 1)


def print_metric_diagnostics(title, segment_races, active_metrics):
    rows = [row for race in segment_races for row in race]
    if not rows:
        return

    print()
    print(f"## {title} Metric Diagnostics")
    print()
    print("| Metric | Avg Weight % | Source Coverage | Non-neutral | Std Dev | Avg |")
    print("|---|---:|---:|---:|---:|---:|")

    for metric in active_metrics:
        values = [safe_float((row.get("_sim_v418_metrics") or {}).get(metric), 50.0) for row in rows]
        guard = SOURCE_GUARDS.get(metric)
        if guard:
            source_count = sum(1 for row in rows if (row.get("_sim_v418_metrics") or {}).get(guard))
        else:
            source_count = len(rows)
        non_neutral = sum(1 for value in values if abs(value - 50.0) >= 1.0)
        std_dev = round(statistics.pstdev(values), 2) if len(values) > 1 else 0.0
        avg = round(statistics.mean(values), 2) if values else 0.0
        weight_pct = avg_weight_pct(rows, metric)
        print(
            f"| {metric} | {weight_pct} | {source_count}/{len(rows)} | "
            f"{non_neutral}/{len(rows)} | {std_dev} | {avg} |"
        )


def print_metric_audit(title, segment_races):
    if not segment_races:
        return

    baseline = evaluate(segment_races, "v418")
    active_metrics = active_weighted_metrics(segment_races)
    print_metric_diagnostics(title, segment_races, active_metrics)

    print()
    print(f"## {title} Metric Ablation")
    print()
    print("| Removed Metric | Races | Top1 | Winner Top3 | Winner Top5 | Avg Winner Rank | Avg Pred1 Finish | Delta WTop3 | Delta Avg Winner Rank |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|")

    ablations = []
    for metric in active_metrics:
        def builder(row, removed_metric=metric):
            weights = dict(row_weights(row))
            weights[removed_metric] = 0.0
            return score_row_with_weights(row, weights)

        metrics = evaluate_custom(segment_races, builder)
        ablations.append((metric, metrics))

    ablations.sort(key=lambda item: (item[1]["winner_top3"] - baseline["winner_top3"], item[1]["avg_winner_rank"] or 999))
    for metric, metrics in ablations:
        print(metric_row(metric, metrics, baseline))

    print()
    print(f"## {title} Single-Metric Ranking")
    print()
    print("| Metric | Races | Top1 | Winner Top3 | Winner Top5 | Avg Winner Rank | Avg Pred1 Finish | Delta WTop3 | Delta Avg Winner Rank |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|")

    single_metrics = []
    for metric in active_metrics:
        def builder(row, only_metric=metric):
            return score_row_with_weights(row, {only_metric: 1.0})

        metrics = evaluate_custom(segment_races, builder)
        single_metrics.append((metric, metrics))

    single_metrics.sort(key=lambda item: (-item[1]["winner_top3"], item[1]["avg_winner_rank"] or 999, item[0]))
    for metric, metrics in single_metrics:
        print(metric_row(metric, metrics, baseline))


def collect_agf_weight_violations(races):
    violations = []
    for rows in races:
        for row in rows:
            profile = row.get("_sim_v418_profile") or {}
            resolved = row.get("_sim_v418_resolved") or {}
            agf_allowed = bool(resolved.get("agfAllowedForRanking"))
            agf_weight = safe_float((resolved.get("weights") or {}).get("agf_score"), 0.0)
            if not agf_allowed and agf_weight != 0:
                violations.append((row.get("race_id"), row.get("race_type"), profile, agf_weight))
    return violations


def print_full_order_guardrail_report(races, by_segment):
    print()
    print("## Full-Order Guardrails")
    print()
    print("| Model | Races | MAE | Rho | NDCG@5 |")
    print("|---|---:|---:|---:|---:|")
    for mode, label in [
        ("visible", "visible rank_pred"),
        ("v4_logged", "logged v4"),
        ("agf_only", "AGF only"),
        ("v418", f"v{_V4_VERSION} AGF-free"),
    ]:
        print(fmt_guardrails(label, evaluate(races, mode)))

    print()
    print("| Segment | Races | MAE | Rho | NDCG@5 | Sample Guard |")
    print("|---|---:|---:|---:|---:|---|")
    for segment in sorted(by_segment):
        metrics = evaluate(by_segment[segment], "v418")
        sample_guard = "ok" if metrics["races"] >= 30 else "low_sample"
        print(
            f"| {segment} | {metrics['races']} | {metrics.get('mae')} | "
            f"{metrics.get('rho')} | {metrics.get('ndcg5')} | {sample_guard} |"
        )


def build_acceptance_checks(races, by_segment, violations):
    visible = evaluate(races, "visible")
    logged = evaluate(races, "v4_logged")
    agf_only = evaluate(races, "agf_only")
    v418 = evaluate(races, "v418")

    checks = []
    checks.append((
        "AGF-free policy",
        "PASS" if not violations else "FAIL",
        f"{len(violations)} violations outside Maiden/Sartli 1",
    ))
    checks.append((
        "Overall Winner Top3 vs logged v4",
        "PASS" if v418["winner_top3"] >= logged["winner_top3"] else "FAIL",
        f"{v418['winner_top3']}/{v418['races']} vs {logged['winner_top3']}/{logged['races']}",
    ))
    checks.append((
        "Overall Winner Top3 vs AGF-only",
        "PASS" if v418["winner_top3"] >= agf_only["winner_top3"] else "WARN",
        f"{v418['winner_top3']}/{v418['races']} vs {agf_only['winner_top3']}/{agf_only['races']}",
    ))
    checks.append((
        "Full-order NDCG@5 vs logged v4",
        "PASS" if (v418.get("ndcg5") or 0) >= (logged.get("ndcg5") or 0) else "WARN",
        f"{v418.get('ndcg5')} vs {logged.get('ndcg5')}",
    ))
    checks.append((
        "Full-order MAE vs logged v4",
        "PASS" if (v418.get("mae") or 999) <= (logged.get("mae") or 999) else "WARN",
        f"{v418.get('mae')} vs {logged.get('mae')}",
    ))
    checks.append((
        "Top1 vs AGF-only",
        "WARN" if v418["top1"] < agf_only["top1"] else "PASS",
        f"{v418['top1']}/{v418['races']} vs {agf_only['top1']}/{agf_only['races']}",
    ))

    for segment in sorted(by_segment):
        segment_races = by_segment[segment]
        current = evaluate(segment_races, "v418")
        logged_segment = evaluate(segment_races, "v4_logged")
        if current["races"] < 30:
            status = "REVIEW"
            detail = f"{current['races']} races; sample below 30"
        elif current["winner_top3"] >= logged_segment["winner_top3"]:
            status = "PASS"
            detail = f"{current['winner_top3']}/{current['races']} vs logged {logged_segment['winner_top3']}/{logged_segment['races']}"
        else:
            status = "WARN"
            detail = f"{current['winner_top3']}/{current['races']} vs logged {logged_segment['winner_top3']}/{logged_segment['races']}"
        checks.append((f"{segment} segment gate", status, detail))

    return [
        {"check": check, "status": status, "evidence": detail}
        for check, status, detail in checks
    ]


def print_acceptance_gate(checks):
    print()
    print("## Acceptance Gate")
    print()
    print("| Check | Status | Evidence |")
    print("|---|---|---|")
    for item in checks:
        print(f"| {item['check']} | {item['status']} | {item['evidence']} |")


def gate_has_blocking_failure(checks, strict=False):
    blocking = {"FAIL", "WARN", "REVIEW"} if strict else {"FAIL"}
    return any(item.get("status") in blocking for item in checks)


def build_summary(races, by_segment, violations, checks):
    return {
        "version": _V4_VERSION,
        "models": {
            "visible": evaluate(races, "visible"),
            "logged_v4": evaluate(races, "v4_logged"),
            "agf_only": evaluate(races, "agf_only"),
            "v418_agf_free": evaluate(races, "v418"),
        },
        "segments": {
            segment: evaluate(segment_races, "v418")
            for segment, segment_races in sorted(by_segment.items())
        },
        "acceptance_gate": checks,
        "agf_weight_violations": len(violations),
    }


def print_report(races):
    print(f"# v{_V4_VERSION} AGF-Free Evaluation")
    print()
    print("| Model | Races | Top1 | Winner Top3 | Winner Top5 | Avg Winner Rank | Avg Pred1 Finish | Avg Score Gap | Winner <=10% | Surprise WTop3 |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for mode, label in [
        ("visible", "visible rank_pred"),
        ("v4_logged", "logged v4"),
        ("agf_only", "AGF only"),
        ("v418", f"v{_V4_VERSION} AGF-free"),
    ]:
        print(fmt_metrics(label, evaluate(races, mode)))

    by_segment = defaultdict(list)
    for rows in races:
        by_segment[segment_key(rows)].append(rows)

    print()
    print("| Segment | Races | Top1 | Winner Top3 | Winner Top5 | Avg Winner Rank | Avg Pred1 Finish | Avg Score Gap | Winner <=10% | Surprise WTop3 |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for segment in sorted(by_segment):
        print(fmt_metrics(segment, evaluate(by_segment[segment], "v418")))

    print_full_order_guardrail_report(races, by_segment)

    handicap_races = [rows for rows in races if segment_key(rows) == "HANDIKAP"]
    print_breakdown("Handikap By Subtype", handicap_races, handicap_profile_key)
    print_breakdown("Handikap By Selected Profile", handicap_races, handicap_selected_key)
    print_breakdown("Handikap By Track", handicap_races, handicap_track_key)
    print_breakdown("Handikap By Distance Bucket", handicap_races, handicap_distance_key)
    print_breakdown("Handikap By Field Bucket", handicap_races, handicap_field_key)
    print_metric_audit("Handikap", handicap_races)

    kv_races = [rows for rows in races if segment_key(rows) == "KV"]
    print_metric_audit("KV", kv_races)

    sartli_races = [rows for rows in races if segment_key(rows) == "SARTLI"]
    print_breakdown("Sartli By Selected Profile", sartli_races, handicap_selected_key)
    print_metric_audit("Sartli", sartli_races)

    grup_races = [rows for rows in races if segment_key(rows) == "GRUP"]
    print_metric_audit("Grup Low-Sample", grup_races)

    satis_races = [rows for rows in races if segment_key(rows) == "SATIS"]
    print_metric_audit("Satis Low-Sample", satis_races)

    violations = collect_agf_weight_violations(races)
    checks = build_acceptance_checks(races, by_segment, violations)
    print_acceptance_gate(checks)

    print()
    print(f"AGF weight violations outside allowed groups: {len(violations)}")
    if violations:
        for item in violations[:10]:
            print(f"- {item}")

    return build_summary(races, by_segment, violations, checks)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Evaluate local v4.18 AGF-free ranking from export data.")
    parser.add_argument("--url", default=DEFAULT_EXPORT_URL)
    parser.add_argument("--json-out", help="Write machine-readable summary JSON to this path.")
    parser.add_argument("--fail-on-gate", action="store_true", help="Exit 1 when any acceptance gate check is FAIL.")
    parser.add_argument(
        "--fail-on-warn",
        action="store_true",
        help="Exit 1 when any acceptance gate check is FAIL, WARN, or REVIEW.",
    )
    args = parser.parse_args(argv)
    entries = load_entries(args.url)
    races = group_races(entries)
    summary = print_report(races)
    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as handle:
            json.dump(summary, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
    if args.fail_on_warn and gate_has_blocking_failure(summary["acceptance_gate"], strict=True):
        return 1
    if args.fail_on_gate and gate_has_blocking_failure(summary["acceptance_gate"], strict=False):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
