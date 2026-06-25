"""Metric opportunity analysis for Atistik ranking.

This script is analysis-only. It reads labeled prediction exports and answers:
"Which metric percentages would have produced rankings closer to the actual
finish order for each race group/profile?"
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import evaluate_v418_agf_free as evaluator


DEFAULT_EXPORT_URL = "https://atistik-backend.onrender.com/api/ml-export?labeled_only=true"

METRIC_KEYS = [
    "degree_avg",
    "degree_trend",
    "degree_stability",
    "form_trend",
    "track_suit",
    "track_experience_score",
    "surface_transition_score",
    "distance_suit",
    "distance_transition_score",
    "training_fitness",
    "training_degree_score",
    "weight_impact",
    "handicap_efficiency_score",
    "handicap_class_transition_score",
    "running_style_proxy_score",
    "jockey_score",
    "bounce_score",
    "pace_score",
    "pedigree",
    "hp_score",
    "trainer_score",
    "agf_score",
    "age_score",
]

MIN_COVERAGE = 0.25
MIN_NON_NEUTRAL = 0.10
MAX_REFINED_METRICS = 12
MAX_REFINEMENT_PASSES_PER_STEP = 4


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def safe_int(value: Any, default: int = 999) -> int:
    try:
        if value is None or value == "":
            return int(default)
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def parse_date(value: Any) -> datetime:
    try:
        return datetime.strptime(str(value or ""), "%d.%m.%Y")
    except ValueError:
        return datetime.min


def normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    cleaned = {key: max(0.0, float(value or 0.0)) for key, value in weights.items()}
    total = sum(cleaned.values())
    if total <= 0:
        return {}
    return {key: value / total for key, value in sorted(cleaned.items()) if value > 0}


def weights_pct(weights: dict[str, float]) -> dict[str, float]:
    return {key: round(value * 100.0, 3) for key, value in sorted(weights.items()) if value > 0}


def segment_category(rows: list[dict[str, Any]]) -> str:
    return evaluator.segment_key(rows)


def selected_profile(rows: list[dict[str, Any]]) -> str:
    resolved = rows[0].get("_sim_v418_resolved") or {}
    profile = rows[0].get("_sim_v418_profile") or {}
    return resolved.get("selectedKey") or profile.get("subtype") or segment_category(rows)


def race_sort_key(rows: list[dict[str, Any]]) -> tuple[datetime, int, str]:
    row = rows[0]
    return (
        parse_date(row.get("race_date")),
        safe_int(row.get("race_no"), 999),
        str(row.get("race_id") or ""),
    )


def split_train_holdout(races: list[list[dict[str, Any]]], ratio: float = 0.8) -> tuple[list[list[dict[str, Any]]], list[list[dict[str, Any]]]]:
    ordered = sorted(races, key=race_sort_key)
    if len(ordered) < 2:
        return ordered, []
    split_at = max(1, min(len(ordered) - 1, int(len(ordered) * ratio)))
    return ordered[:split_at], ordered[split_at:]


def agf_allowed_for_segment(segment: str, races: list[list[dict[str, Any]]]) -> bool:
    if segment == "MAIDEN":
        return True
    if segment == "SART1":
        return True
    return False


def optimization_segment_for_race(rows: list[dict[str, Any]]) -> str:
    profile = selected_profile(rows)
    if profile == "SART1":
        return "SART1"
    return segment_category(rows)


def metric_value(row: dict[str, Any], metric: str) -> float:
    return safe_float((row.get("_sim_v418_metrics") or {}).get(metric), 50.0)


def metric_source(row: dict[str, Any], metric: str) -> bool:
    guard = evaluator.SOURCE_GUARDS.get(metric)
    metrics = row.get("_sim_v418_metrics") or {}
    if guard:
        return bool(metrics.get(guard))
    return metric in (row.get("features") or {})


def finish_relevance(rows: list[dict[str, Any]]) -> dict[int, float]:
    ordered = sorted(rows, key=lambda row: safe_int(row.get("finish_pos"), 999))
    count = len(ordered)
    if count <= 1:
        return {id(row): 100.0 for row in ordered}
    return {
        id(row): 100.0 * (count - 1 - index) / (count - 1)
        for index, row in enumerate(ordered)
    }


def pearson(xs: list[float], ys: list[float]) -> float:
    if len(xs) < 2:
        return 0.0
    mean_x = statistics.mean(xs)
    mean_y = statistics.mean(ys)
    var_x = sum((x - mean_x) ** 2 for x in xs)
    var_y = sum((y - mean_y) ** 2 for y in ys)
    if var_x <= 0 or var_y <= 0:
        return 0.0
    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    return cov / math.sqrt(var_x * var_y)


def metric_diagnostics(races: list[list[dict[str, Any]]], segment: str) -> list[dict[str, Any]]:
    rows = [row for race in races for row in race]
    total = len(rows)
    allow_agf = agf_allowed_for_segment(segment, races)
    diagnostics = []
    for metric in METRIC_KEYS:
        source_count = sum(1 for row in rows if metric_source(row, metric))
        values = [metric_value(row, metric) for row in rows]
        non_neutral = sum(1 for value in values if abs(value - 50.0) >= 1.0)
        coverage = source_count / total if total else 0.0
        non_neutral_ratio = non_neutral / total if total else 0.0
        if metric == "agf_score" and not allow_agf:
            eligible = False
            reason = "agf_disabled_for_segment"
        elif coverage < MIN_COVERAGE:
            eligible = False
            reason = "low_coverage"
        elif non_neutral_ratio < MIN_NON_NEUTRAL:
            eligible = False
            reason = "low_signal"
        else:
            eligible = True
            reason = "eligible"
        diagnostics.append({
            "metric": metric,
            "eligible": eligible,
            "reason": reason,
            "coverage": round(coverage, 4),
            "non_neutral_ratio": round(non_neutral_ratio, 4),
            "source_count": source_count,
            "row_count": total,
            "std_dev": round(statistics.pstdev(values), 4) if len(values) > 1 else 0.0,
            "avg": round(statistics.mean(values), 4) if values else 0.0,
        })
    return diagnostics


def eligible_metrics(races: list[list[dict[str, Any]]], segment: str) -> list[str]:
    return [item["metric"] for item in metric_diagnostics(races, segment) if item["eligible"]]


def race_optimal_weights(rows: list[dict[str, Any]], metrics: list[str]) -> dict[str, float]:
    relevance = finish_relevance(rows)
    raw: dict[str, float] = {}
    ys = [relevance[id(row)] for row in rows]
    for metric in metrics:
        xs = [metric_value(row, metric) for row in rows]
        rho = pearson(xs, ys)
        spread = statistics.pstdev(xs) if len(xs) > 1 else 0.0
        raw[metric] = max(0.0, rho) * max(0.0, spread)
    return normalize_weights(raw)


def average_race_weights(races: list[list[dict[str, Any]]], metrics: list[str]) -> dict[str, float]:
    collected = defaultdict(list)
    for rows in races:
        weights = race_optimal_weights(rows, metrics)
        for metric in metrics:
            collected[metric].append(weights.get(metric, 0.0))
    if not collected:
        return {}
    averaged = {}
    for metric, values in collected.items():
        if not values:
            continue
        ordered = sorted(values)
        trim = int(len(ordered) * 0.1)
        sample = ordered[trim:len(ordered) - trim] if len(ordered) - trim > trim else ordered
        averaged[metric] = statistics.mean(sample) if sample else 0.0
    return normalize_weights(averaged)


def refinement_metric_subset(metrics: list[str], seed: dict[str, float]) -> list[str]:
    if len(metrics) <= MAX_REFINED_METRICS:
        return metrics
    seeded = [metric for metric, _ in sorted(seed.items(), key=lambda item: -item[1]) if metric in metrics]
    remainder = [metric for metric in metrics if metric not in seeded]
    return (seeded + remainder)[:MAX_REFINED_METRICS]


def score_with_weights(row: dict[str, Any], weights: dict[str, float]) -> float:
    return sum(metric_value(row, metric) * weight for metric, weight in weights.items())


def evaluate_weights(races: list[list[dict[str, Any]]], weights: dict[str, float]) -> dict[str, Any]:
    if not races or not weights:
        return empty_metrics()
    return evaluator.evaluate_custom(races, lambda row: score_with_weights(row, weights))


def empty_metrics() -> dict[str, Any]:
    return {
        "races": 0,
        "top1": 0,
        "winner_top3": 0,
        "winner_top5": 0,
        "avg_winner_rank": None,
        "avg_pred1_finish": None,
        "avg_score_gap": None,
        "mae": None,
        "rho": None,
        "ndcg5": None,
        "winner_close_10pct": None,
        "surprise_total": 0,
        "surprise_top3": 0,
        "surprise_top5": 0,
    }


def avg_field_size(races: list[list[dict[str, Any]]]) -> float:
    if not races:
        return 1.0
    return max(1.0, statistics.mean(len(rows) for rows in races))


def objective_score(metrics: dict[str, Any], races: list[list[dict[str, Any]]]) -> float:
    if not metrics or not metrics.get("races"):
        return -999.0
    ndcg = safe_float(metrics.get("ndcg5"), 0.0)
    rho = (safe_float(metrics.get("rho"), -1.0) + 1.0) / 2.0
    mae = safe_float(metrics.get("mae"), avg_field_size(races))
    mae_component = max(0.0, 1.0 - mae / avg_field_size(races))
    return 0.45 * ndcg + 0.35 * rho + 0.20 * mae_component


def refine_weights(train_races: list[list[dict[str, Any]]], seed: dict[str, float], metrics: list[str]) -> dict[str, float]:
    current = normalize_weights({metric: seed.get(metric, 0.0) for metric in metrics})
    if not current:
        current = {metric: 1.0 / len(metrics) for metric in metrics} if metrics else {}
    best_score = objective_score(evaluate_weights(train_races, current), train_races)
    steps = [0.08, 0.04, 0.02, 0.01]
    for step in steps:
        improved = True
        passes = 0
        while improved and passes < MAX_REFINEMENT_PASSES_PER_STEP:
            passes += 1
            improved = False
            best_candidate = current
            for src in metrics:
                if current.get(src, 0.0) <= step:
                    continue
                for dst in metrics:
                    if src == dst:
                        continue
                    candidate = dict(current)
                    candidate[src] = max(0.0, candidate.get(src, 0.0) - step)
                    candidate[dst] = candidate.get(dst, 0.0) + step
                    candidate = normalize_weights(candidate)
                    score = objective_score(evaluate_weights(train_races, candidate), train_races)
                    if score > best_score + 1e-9:
                        best_score = score
                        best_candidate = candidate
                        improved = True
            current = best_candidate
    return normalize_weights(current)


def baseline_metrics(races: list[list[dict[str, Any]]]) -> dict[str, Any]:
    return evaluator.evaluate(races, "v418") if races else empty_metrics()


def current_weights_for_segment(races: list[list[dict[str, Any]]]) -> dict[str, float]:
    totals = defaultdict(list)
    for rows in races:
        for row in rows:
            for metric, value in (row.get("_sim_v418_resolved") or {}).get("weights", {}).items():
                if metric in METRIC_KEYS:
                    totals[metric].append(safe_float(value, 0.0))
    return normalize_weights({metric: statistics.mean(values) for metric, values in totals.items() if values})


def single_metric_metrics(races: list[list[dict[str, Any]]], metric: str) -> dict[str, Any]:
    return evaluate_weights(races, {metric: 1.0})


def recommendation_status(segment_level: str, race_count: int) -> str:
    threshold = 30 if segment_level == "group" else 12
    return "candidate" if race_count >= threshold else "diagnostic_only"


def holdout_verdict(
    status: str,
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    races: list[list[dict[str, Any]]],
) -> str:
    if status != "candidate":
        return "sample_too_low"
    if not races:
        return "no_holdout"
    objective_delta = objective_score(candidate, races) - objective_score(baseline, races)
    winner_top3_delta = safe_int(candidate.get("winner_top3"), 0) - safe_int(baseline.get("winner_top3"), 0)
    if objective_delta > 0.005 and winner_top3_delta >= 0:
        return "supported"
    if objective_delta > 0.005:
        return "risky_improvement"
    if abs(objective_delta) <= 0.005 and winner_top3_delta >= 0:
        return "mixed"
    return "not_supported"


def segment_rows(all_races: list[list[dict[str, Any]]]) -> dict[tuple[str, str], list[list[dict[str, Any]]]]:
    grouped = defaultdict(list)
    for rows in all_races:
        category = segment_category(rows)
        grouped[("group", category)].append(rows)
        profile = selected_profile(rows)
        if profile and profile != category:
            grouped[("profile", profile)].append(rows)
    return dict(grouped)


def analyze_segment(level: str, segment: str, races: list[list[dict[str, Any]]]) -> dict[str, Any]:
    train, holdout = split_train_holdout(races)
    diagnostics = metric_diagnostics(races, segment)
    metrics = [item["metric"] for item in diagnostics if item["eligible"]]
    seed = average_race_weights(train, metrics)
    refined_metrics = refinement_metric_subset(metrics, seed)
    refined = refine_weights(train, seed, refined_metrics)
    baseline_train = baseline_metrics(train)
    baseline_holdout = baseline_metrics(holdout)
    candidate_train = evaluate_weights(train, refined)
    candidate_holdout = evaluate_weights(holdout, refined)
    status = recommendation_status(level, len(races))
    winner_top3_delta = (candidate_holdout["winner_top3"] - baseline_holdout["winner_top3"]) if holdout else 0
    risk = "winner_top3_regression" if winner_top3_delta < 0 else "none"
    if not metrics:
        status = "insufficient_signal"
        risk = "insufficient_signal"
    verdict = holdout_verdict(status, baseline_holdout, candidate_holdout, holdout)
    return {
        "level": level,
        "segment": segment,
        "race_count": len(races),
        "train_races": len(train),
        "holdout_races": len(holdout),
        "status": status,
        "risk": risk,
        "holdout_verdict": verdict,
        "eligible_metrics": metrics,
        "candidate_weights": weights_pct(refined),
        "race_average_weights": weights_pct(seed),
        "current_avg_weights": weights_pct(current_weights_for_segment(races)),
        "baseline_train": baseline_train,
        "candidate_train": candidate_train,
        "baseline_holdout": baseline_holdout,
        "candidate_holdout": candidate_holdout,
        "objective_train": round(objective_score(candidate_train, train), 6),
        "baseline_objective_train": round(objective_score(baseline_train, train), 6),
        "objective_holdout": round(objective_score(candidate_holdout, holdout), 6) if holdout else None,
        "baseline_objective_holdout": round(objective_score(baseline_holdout, holdout), 6) if holdout else None,
        "diagnostics": diagnostics,
    }


def race_rows_for_csv(races: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    output = []
    for rows in sorted(races, key=race_sort_key):
        category = segment_category(rows)
        profile = selected_profile(rows)
        metrics = eligible_metrics([rows], optimization_segment_for_race(rows))
        weights = race_optimal_weights(rows, metrics)
        output.append({
            "race_id": rows[0].get("race_id"),
            "race_date": rows[0].get("race_date"),
            "race_no": rows[0].get("race_no"),
            "race_type": rows[0].get("race_type"),
            "category": category,
            "profile": profile,
            "horse_count": len(rows),
            "status": "ok" if weights else "insufficient_signal",
            "ideal_weight_json": json.dumps(weights_pct(weights), ensure_ascii=False, sort_keys=True),
        })
    return output


def format_metric_delta(candidate: dict[str, Any], baseline: dict[str, Any], metric: str) -> str:
    cand = candidate.get(metric)
    base = baseline.get(metric)
    if cand is None or base is None:
        return "-"
    if isinstance(cand, float) or isinstance(base, float):
        return f"{cand:.3f} vs {base:.3f}"
    return f"{cand} vs {base}"


def write_summary(path: Path, analyses: list[dict[str, Any]], export_count: int) -> None:
    lines = [
        "# Metric Opportunity Analysis",
        "",
        f"- Export entries used: `{export_count}` labeled rows before race filtering.",
        "- Objective: order closeness (`45% NDCG@5`, `35% normalized Spearman`, `20% MAE component`).",
        "- AGF policy: included only for `MAIDEN` and `SART1`; otherwise diagnostic only.",
        "- This report is analysis-only; live v4 weights are unchanged.",
        "",
        "## Candidate Summary",
        "",
        "| Segment | Level | Races | Status | Verdict | Risk | Holdout WTop3 | Holdout MAE | Holdout Rho | Candidate Weights |",
        "|---|---|---:|---|---|---|---|---|---|---|",
    ]
    for item in sorted(analyses, key=lambda x: (x["level"], x["segment"])):
        weights = item["candidate_weights"]
        top_weights = ", ".join(f"{k} {v:.1f}%" for k, v in sorted(weights.items(), key=lambda kv: -kv[1])[:6])
        lines.append(
            f"| {item['segment']} | {item['level']} | {item['race_count']} | {item['status']} | {item['holdout_verdict']} | {item['risk']} | "
            f"{format_metric_delta(item['candidate_holdout'], item['baseline_holdout'], 'winner_top3')} | "
            f"{format_metric_delta(item['candidate_holdout'], item['baseline_holdout'], 'mae')} | "
            f"{format_metric_delta(item['candidate_holdout'], item['baseline_holdout'], 'rho')} | {top_weights or '-'} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def build_outputs(analyses: list[dict[str, Any]], races: list[list[dict[str, Any]]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    group_rows = []
    diagnostic_rows = []
    for item in analyses:
        weights_json = json.dumps(item["candidate_weights"], ensure_ascii=False, sort_keys=True)
        avg_json = json.dumps(item["race_average_weights"], ensure_ascii=False, sort_keys=True)
        current_json = json.dumps(item["current_avg_weights"], ensure_ascii=False, sort_keys=True)
        group_rows.append({
            "level": item["level"],
            "segment": item["segment"],
            "race_count": item["race_count"],
            "train_races": item["train_races"],
            "holdout_races": item["holdout_races"],
            "status": item["status"],
            "holdout_verdict": item["holdout_verdict"],
            "risk": item["risk"],
            "candidate_weight_json": weights_json,
            "race_average_weight_json": avg_json,
            "current_avg_weight_json": current_json,
            "baseline_train_top1": item["baseline_train"]["top1"],
            "candidate_train_top1": item["candidate_train"]["top1"],
            "baseline_train_winner_top3": item["baseline_train"]["winner_top3"],
            "candidate_train_winner_top3": item["candidate_train"]["winner_top3"],
            "baseline_train_mae": item["baseline_train"]["mae"],
            "candidate_train_mae": item["candidate_train"]["mae"],
            "baseline_train_rho": item["baseline_train"]["rho"],
            "candidate_train_rho": item["candidate_train"]["rho"],
            "baseline_train_ndcg5": item["baseline_train"]["ndcg5"],
            "candidate_train_ndcg5": item["candidate_train"]["ndcg5"],
            "baseline_train_objective": item["baseline_objective_train"],
            "candidate_train_objective": item["objective_train"],
            "baseline_holdout_top1": item["baseline_holdout"]["top1"],
            "candidate_holdout_top1": item["candidate_holdout"]["top1"],
            "baseline_holdout_winner_top3": item["baseline_holdout"]["winner_top3"],
            "candidate_holdout_winner_top3": item["candidate_holdout"]["winner_top3"],
            "baseline_holdout_mae": item["baseline_holdout"]["mae"],
            "candidate_holdout_mae": item["candidate_holdout"]["mae"],
            "baseline_holdout_rho": item["baseline_holdout"]["rho"],
            "candidate_holdout_rho": item["candidate_holdout"]["rho"],
            "baseline_holdout_ndcg5": item["baseline_holdout"]["ndcg5"],
            "candidate_holdout_ndcg5": item["candidate_holdout"]["ndcg5"],
            "baseline_holdout_objective": item["baseline_objective_holdout"],
            "candidate_holdout_objective": item["objective_holdout"],
        })
        for diag in item["diagnostics"]:
            single = single_metric_metrics(
                [race for race in races if (item["level"], item["segment"]) in {
                    ("group", segment_category(race)),
                    ("profile", selected_profile(race)),
                }],
                diag["metric"],
            )
            diagnostic_rows.append({
                "level": item["level"],
                "segment": item["segment"],
                **diag,
                "single_metric_top1": single["top1"],
                "single_metric_winner_top3": single["winner_top3"],
                "single_metric_mae": single["mae"],
                "single_metric_rho": single["rho"],
                "single_metric_ndcg5": single["ndcg5"],
            })
    return group_rows, race_rows_for_csv(races), diagnostic_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze metric weight opportunities without changing live weights.")
    parser.add_argument("--export-url", default=DEFAULT_EXPORT_URL)
    parser.add_argument("--input", help="Optional local export JSON.")
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    if args.input:
        payload = json.loads(Path(args.input).read_text(encoding="utf-8"))
        entries = payload.get("entries", payload if isinstance(payload, list) else [])
    else:
        entries = evaluator.load_entries(args.export_url)
    races = evaluator.group_races(entries)
    segments = segment_rows(races)
    analyses = [
        analyze_segment(level, segment, segment_races)
        for (level, segment), segment_races in sorted(segments.items())
    ]

    output_dir = Path(args.output_dir) if args.output_dir else Path("reports") / f"metric-opportunity-{datetime.now().strftime('%Y%m%d')}"
    output_dir.mkdir(parents=True, exist_ok=True)

    group_rows, race_rows, diagnostic_rows = build_outputs(analyses, races)
    write_summary(output_dir / "summary.md", analyses, len(entries))
    write_csv(
        output_dir / "group_weight_candidates.csv",
        group_rows,
        [
            "level",
            "segment",
            "race_count",
            "train_races",
            "holdout_races",
            "status",
            "holdout_verdict",
            "risk",
            "candidate_weight_json",
            "race_average_weight_json",
            "current_avg_weight_json",
            "baseline_train_top1",
            "candidate_train_top1",
            "baseline_train_winner_top3",
            "candidate_train_winner_top3",
            "baseline_train_mae",
            "candidate_train_mae",
            "baseline_train_rho",
            "candidate_train_rho",
            "baseline_train_ndcg5",
            "candidate_train_ndcg5",
            "baseline_train_objective",
            "candidate_train_objective",
            "baseline_holdout_top1",
            "candidate_holdout_top1",
            "baseline_holdout_winner_top3",
            "candidate_holdout_winner_top3",
            "baseline_holdout_mae",
            "candidate_holdout_mae",
            "baseline_holdout_rho",
            "candidate_holdout_rho",
            "baseline_holdout_ndcg5",
            "candidate_holdout_ndcg5",
            "baseline_holdout_objective",
            "candidate_holdout_objective",
        ],
    )
    write_csv(
        output_dir / "race_optimal_weights.csv",
        race_rows,
        ["race_id", "race_date", "race_no", "race_type", "category", "profile", "horse_count", "status", "ideal_weight_json"],
    )
    write_csv(
        output_dir / "metric_diagnostics.csv",
        diagnostic_rows,
        [
            "level",
            "segment",
            "metric",
            "eligible",
            "reason",
            "coverage",
            "non_neutral_ratio",
            "source_count",
            "row_count",
            "std_dev",
            "avg",
            "single_metric_top1",
            "single_metric_winner_top3",
            "single_metric_mae",
            "single_metric_rho",
            "single_metric_ndcg5",
        ],
    )
    (output_dir / "metric_opportunity.json").write_text(
        json.dumps(
            {
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "export_count": len(entries),
                "race_count": len(races),
                "objective": {
                    "ndcg5": 0.45,
                    "spearman_rho_normalized": 0.35,
                    "mae_component": 0.20,
                },
                "agf_policy": "AGF is optimized only for MAIDEN and SART1.",
                "analyses": analyses,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(json.dumps({"output_dir": str(output_dir), "races": len(races), "segments": len(analyses)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
