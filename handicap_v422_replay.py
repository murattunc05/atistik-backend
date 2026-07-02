"""Replay HANDIKAP races with v4.22 candidate metrics.

The script is analysis-only. It does not change live weights or predictions.
It compares the logged v4 ranking with an AGF-free handicap candidate formula.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import urllib.request
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any


DEFAULT_EXPORT_URL = "https://atistik-backend.onrender.com/api/ml-export?labeled_only=true"

V422_HANDICAP_WEIGHTS = {
    "form_trend": 16.0,
    "recent_finish_position_score": 32.0,
    "distance_transition_score": 32.0,
    "surface_switch_safety_score": 14.0,
    "handicap_load_value_score": 12.0,
    "training_fitness": 8.0,
    "handicap_age_curve_score": 8.0,
    "hp_score": 4.0,
    "jockey_score": 2.0,
}

SINGLE_METRICS = [
    "form_trend",
    "hp_score",
    "distance_transition_score",
    "surface_transition_score",
    "surface_switch_safety_score",
    "handicap_efficiency_score",
    "handicap_load_value_score",
    "weight_impact",
    "weight_change_risk_score",
    "handicap_class_transition_score",
    "handicap_class_load_transition_score",
    "running_style_proxy_score",
    "recent_finish_position_score",
    "pace_score",
    "start_draw_score",
    "late_start_risk_score",
    "track_condition_suit_score",
    "age_score",
    "handicap_age_curve_score",
    "jockey_score",
    "training_fitness",
    "bounce_score",
]


def safe_float(value: Any, default: float = 50.0) -> float:
    try:
        if value is None or value == "":
            return float(default)
        parsed = float(value)
        if math.isnan(parsed):
            return float(default)
        return parsed
    except (TypeError, ValueError):
        return float(default)


def safe_int(value: Any, default: int = 999) -> int:
    try:
        if value is None or value == "":
            return int(default)
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def load_entries(export_url: str) -> list[dict[str, Any]]:
    with urllib.request.urlopen(export_url, timeout=120) as response:
        payload = json.loads(response.read().decode("utf-8"))
    entries = payload.get("entries", payload if isinstance(payload, list) else [])
    return [entry for entry in entries if safe_int(entry.get("finish_pos"), 0) > 0]


def is_handicap(row: dict[str, Any]) -> bool:
    profile = row.get("v4_profile") or {}
    race_type = str(row.get("race_type") or "").upper().replace("İ", "I")
    return profile.get("category") == "HANDIKAP" or "HANDIKAP" in race_type or "HNDIKAP" in race_type


def profile_key(rows: list[dict[str, Any]]) -> str:
    row = rows[0]
    profile = row.get("v4_profile") or {}
    subtype = str(profile.get("subtype") or profile.get("category") or "HANDIKAP")
    track = str(profile.get("track") or row.get("track") or "")
    return f"{subtype}|{track}" if track else subtype


def group_races(entries: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        if is_handicap(entry):
            grouped[str(entry.get("race_id"))].append(entry)
    races = [rows for rows in grouped.values() if len(rows) >= 2]
    return sorted(
        races,
        key=lambda rows: (
            datetime.strptime(str(rows[0].get("race_date") or "01.01.1900"), "%d.%m.%Y")
            if str(rows[0].get("race_date") or "")
            else datetime.min,
            safe_int(rows[0].get("race_no"), 999),
            str(rows[0].get("race_id") or ""),
        ),
    )


def feature(row: dict[str, Any], key: str) -> float:
    features = row.get("features") or {}
    if key in features:
        return safe_float(features.get(key), 50.0)
    if key == "handicap_load_value_score":
        return safe_float(features.get("handicap_efficiency_score"), 50.0)
    if key == "handicap_class_load_transition_score":
        return 50.0
    if key == "recent_finish_position_score":
        return safe_float(features.get("running_style_proxy_score"), 50.0)
    if key == "handicap_age_curve_score":
        age = safe_float(features.get("age_score"), 50.0)
        return 100.0 - age if abs(age - 50.0) > 0.01 else 50.0
    if key == "surface_switch_safety_score":
        return safe_float(features.get("surface_transition_score"), 50.0)
    if key in {"track_condition_suit_score", "start_draw_score", "late_start_risk_score", "weight_change_risk_score"}:
        return 50.0
    return safe_float(row.get(key), 50.0)


def score_candidate(row: dict[str, Any], weights: dict[str, float] = V422_HANDICAP_WEIGHTS) -> float:
    total = 0.0
    weighted = 0.0
    for key, weight in weights.items():
        value = feature(row, key)
        weighted += value * weight
        total += weight
    return round(weighted / total, 3) if total > 0 else 50.0


def rank_by_score(rows: list[dict[str, Any]], score_key: str | None = None, candidate: bool = False) -> dict[int, int]:
    scored = []
    for index, row in enumerate(rows):
        if candidate:
            score = score_candidate(row)
        elif score_key:
            score = safe_float(row.get(score_key), 0.0)
        else:
            score = -safe_int(row.get("v4_rank") or row.get("rank_pred"), 999)
        scored.append((index, score))
    scored.sort(key=lambda item: item[1], reverse=True)
    return {index: rank + 1 for rank, (index, _) in enumerate(scored)}


def spearman(predicted: list[int], actual: list[int]) -> float | None:
    if len(predicted) < 2:
        return None
    pred_mean = sum(predicted) / len(predicted)
    actual_mean = sum(actual) / len(actual)
    numerator = sum((p - pred_mean) * (a - actual_mean) for p, a in zip(predicted, actual))
    pred_den = math.sqrt(sum((p - pred_mean) ** 2 for p in predicted))
    actual_den = math.sqrt(sum((a - actual_mean) ** 2 for a in actual))
    if pred_den <= 0 or actual_den <= 0:
        return None
    return numerator / (pred_den * actual_den)


def ndcg_at_5(ranks: dict[int, int], finishes: list[int]) -> float:
    ordered = sorted(ranks.items(), key=lambda item: item[1])[:5]
    gains = []
    field_size = max(finishes) if finishes else 1
    for index, _rank in ordered:
        relevance = field_size - finishes[index] + 1
        gains.append((2 ** relevance - 1) / math.log2(len(gains) + 2))
    ideal_rels = sorted((field_size - finish + 1 for finish in finishes), reverse=True)[:5]
    ideal = [(2 ** rel - 1) / math.log2(pos + 2) for pos, rel in enumerate(ideal_rels)]
    denom = sum(ideal)
    return sum(gains) / denom if denom else 0.0


def evaluate(races: list[list[dict[str, Any]]], mode: str, metric: str | None = None) -> dict[str, Any]:
    top1 = winner_top3 = winner_top5 = 0
    rhos: list[float] = []
    maes: list[float] = []
    ndcgs: list[float] = []
    for rows in races:
        if mode == "candidate":
            ranks = rank_by_score(rows, candidate=True)
        elif mode == "metric":
            rows_with_signal = [row for row in rows if abs(feature(row, metric or "") - 50.0) > 0.01]
            if len(rows_with_signal) < 2:
                continue
            ranks = rank_by_score(rows, score_key=None, candidate=False)
            scored = sorted(
                [(idx, feature(row, metric or "")) for idx, row in enumerate(rows)],
                key=lambda item: item[1],
                reverse=True,
            )
            ranks = {idx: rank + 1 for rank, (idx, _score) in enumerate(scored)}
        else:
            ranks = {
                index: safe_int(row.get("v4_rank") or row.get("rank_pred"), 999)
                for index, row in enumerate(rows)
            }
            if all(rank >= 999 for rank in ranks.values()):
                ranks = rank_by_score(rows, "v4_score")

        finishes = [safe_int(row.get("finish_pos"), 999) for row in rows]
        winner_indexes = [idx for idx, finish in enumerate(finishes) if finish == 1]
        if winner_indexes:
            winner_rank = ranks[winner_indexes[0]]
            top1 += winner_rank == 1
            winner_top3 += winner_rank <= 3
            winner_top5 += winner_rank <= 5
        pred_list = [ranks[idx] for idx in range(len(rows))]
        rho = spearman(pred_list, finishes)
        if rho is not None:
            rhos.append(rho)
        maes.append(sum(abs(ranks[idx] - finishes[idx]) for idx in range(len(rows))) / len(rows))
        ndcgs.append(ndcg_at_5(ranks, finishes))

    count = len(races) if mode != "metric" else len(maes)
    return {
        "races": count,
        "top1": top1,
        "winner_top3": winner_top3,
        "winner_top5": winner_top5,
        "rho": round(sum(rhos) / len(rhos), 3) if rhos else 0.0,
        "mae": round(sum(maes) / len(maes), 3) if maes else 0.0,
        "ndcg5": round(sum(ndcgs) / len(ndcgs), 3) if ndcgs else 0.0,
    }


def write_report(races: list[list[dict[str, Any]]], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    baseline = evaluate(races, "baseline")
    candidate = evaluate(races, "candidate")

    rows = [
        ["Segment", "Races", "Top1", "WTop3", "WTop5", "Rho", "MAE", "NDCG@5"],
        [
            "v4.21 logged HANDIKAP",
            baseline["races"],
            f'{baseline["top1"]}/{baseline["races"]}',
            f'{baseline["winner_top3"]}/{baseline["races"]}',
            f'{baseline["winner_top5"]}/{baseline["races"]}',
            baseline["rho"],
            baseline["mae"],
            baseline["ndcg5"],
        ],
        [
            "v4.22 candidate HANDIKAP",
            candidate["races"],
            f'{candidate["top1"]}/{candidate["races"]}',
            f'{candidate["winner_top3"]}/{candidate["races"]}',
            f'{candidate["winner_top5"]}/{candidate["races"]}',
            candidate["rho"],
            candidate["mae"],
            candidate["ndcg5"],
        ],
    ]

    profile_lines = []
    by_profile: dict[str, list[list[dict[str, Any]]]] = defaultdict(list)
    for race in races:
        by_profile[profile_key(race)].append(race)
    for key, profile_races in sorted(by_profile.items(), key=lambda item: len(item[1]), reverse=True):
        if len(profile_races) < 5:
            continue
        old = evaluate(profile_races, "baseline")
        new = evaluate(profile_races, "candidate")
        profile_lines.append(
            [
                key,
                len(profile_races),
                f'{old["top1"]}->{new["top1"]}',
                f'{old["winner_top3"]}->{new["winner_top3"]}',
                f'{old["rho"]}->{new["rho"]}',
                f'{old["mae"]}->{new["mae"]}',
            ]
        )

    metric_rows = []
    for metric in SINGLE_METRICS:
        result = evaluate(races, "metric", metric)
        if result["races"] <= 0:
            continue
        metric_rows.append([
            metric,
            result["races"],
            result["top1"],
            result["winner_top3"],
            result["rho"],
            result["mae"],
        ])

    with (output_dir / "single_metric_diagnostics.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["metric", "races", "top1", "winner_top3", "rho", "mae"])
        writer.writerows(metric_rows)

    with (output_dir / "profile_comparison.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["profile", "races", "top1_old_new", "wtop3_old_new", "rho_old_new", "mae_old_new"])
        writer.writerows(profile_lines)

    def md_table(table_rows: list[list[Any]]) -> str:
        header = table_rows[0]
        body = table_rows[1:]
        lines = [
            "| " + " | ".join(str(cell) for cell in header) + " |",
            "| " + " | ".join("---" for _ in header) + " |",
        ]
        lines.extend("| " + " | ".join(str(cell) for cell in row) + " |" for row in body)
        return "\n".join(lines)

    top_metrics = sorted(metric_rows, key=lambda row: (row[4], -row[5]), reverse=True)[:12]
    summary = [
        f"# v4.22 HANDIKAP Replay Report ({datetime.now().strftime('%Y-%m-%d %H:%M')})",
        "",
        "- Analysis-only; live ranking weights are not changed by this report.",
        "- AGF is excluded from the v4.22 handicap candidate.",
        "- Old exports do not contain all raw start/condition fields, so missing new metrics stay neutral until newly logged analyses accumulate.",
        "",
        md_table(rows),
        "",
        "## Profile Comparison",
        "",
        md_table([["Profile", "Races", "Top1", "WTop3", "Rho", "MAE"], *profile_lines]) if profile_lines else "No profile with at least 5 races.",
        "",
        "## Strong Single Metrics",
        "",
        md_table([["Metric", "Races", "Top1", "WTop3", "Rho", "MAE"], *top_metrics]) if top_metrics else "No usable metric diagnostics.",
        "",
        "## Candidate Weights",
        "",
        md_table([["Metric", "Weight"], *[[key, value] for key, value in V422_HANDICAP_WEIGHTS.items()]]),
        "",
    ]
    (output_dir / "summary.md").write_text("\n".join(summary), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--export-url", default=DEFAULT_EXPORT_URL)
    parser.add_argument("--output-dir", default="")
    args = parser.parse_args()

    entries = load_entries(args.export_url)
    races = group_races(entries)
    stamp = datetime.now().strftime("%Y%m%d")
    output_dir = Path(args.output_dir or f"reports/handicap-v422-replay-{stamp}")
    write_report(races, output_dir)
    print(f"Wrote {output_dir / 'summary.md'} for {len(races)} HANDIKAP races")


if __name__ == "__main__":
    main()
