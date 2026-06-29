"""Generate an analysis-only v4.21 signal report from labeled export data."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import urllib.request
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from api_server import (
    _V4_VERSION,
    calculate_v4_shadow_score,
    extract_v4_race_profile,
    resolve_v4_profile_weights,
)


DEFAULT_EXPORT_URL = "https://atistik-backend.onrender.com/api/ml-export?labeled_only=true"


def safe_float(value: Any, default: float = 50.0) -> float:
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


def load_entries(export_url: str) -> list[dict[str, Any]]:
    with urllib.request.urlopen(export_url, timeout=120) as response:
        payload = json.loads(response.read().decode("utf-8"))
    entries = payload.get("entries", payload if isinstance(payload, list) else [])
    return [entry for entry in entries if safe_int(entry.get("finish_pos"), 0) > 0]


def group_races(entries: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        grouped[str(entry.get("race_id"))].append(entry)
    return [rows for rows in grouped.values() if len(rows) >= 2]


def category(row: dict[str, Any]) -> str:
    profile = row.get("v4_profile") or {}
    return profile.get("category") or "GLOBAL"


def relative_score(value: float, values: list[float], spread_floor: float = 4.0) -> float:
    if len(values) < 3:
        return 50.0
    spread = statistics.pstdev(values)
    if spread < spread_floor:
        return 50.0
    z = (value - statistics.mean(values)) / spread
    return round(max(0.0, min(100.0, 50.0 + z * 14.0)), 1)


def derive_v421_metrics(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    metric_keys = [
        "form_trend", "hp_score", "degree_avg", "distance_suit",
        "surface_transition_score", "weight_impact",
        "handicap_efficiency_score", "pace_score",
    ]
    field_values = {
        key: [safe_float((row.get("features") or {}).get(key)) for row in rows]
        for key in metric_keys
    }
    derived = []
    for row in rows:
        cloned = dict(row)
        metrics = dict(row.get("features") or {})
        flags = dict(row.get("metric_source_flags") or {})
        cat = category(row)
        if cat == "HANDIKAP":
            components = [
                relative_score(safe_float(metrics.get(key)), field_values[key])
                for key in metric_keys
            ]
            actionable = [score for score in components if abs(score - 50.0) >= 0.5]
            field_relative = round(sum(actionable) / len(actionable), 1) if actionable else 50.0
            pace = safe_float(metrics.get("pace_score"))
            style_proxy = safe_float(metrics.get("running_style_proxy_score"))
            pressure = safe_float(metrics.get("pace_pressure"), 0.0)
            pace_edge = pace
            if pressure >= 35 and style_proxy <= 40:
                pace_edge += 8
            elif pressure >= 35 and style_proxy >= 70:
                pace_edge -= 8
            elif pressure <= 15 and style_proxy >= 70:
                pace_edge += 8
            pace_edge = round(max(0.0, min(100.0, pace_edge)), 1)
            surface_safety = round(
                safe_float(metrics.get("surface_transition_score")) * 0.65
                + safe_float(metrics.get("track_experience_score")) * 0.35,
                1,
            )
            relief = safe_float(metrics.get("handicap_weight_relief_score"), safe_float(metrics.get("handicap_efficiency_score")))
            visibility = (
                safe_float(metrics.get("degree_avg")) * 0.25
                + safe_float(metrics.get("form_trend")) * 0.20
                + safe_float(metrics.get("hp_score")) * 0.20
                + safe_float(metrics.get("jockey_score")) * 0.15
                + safe_float(metrics.get("agf_score")) * 0.20
            )
            real_edge = (
                field_relative * 0.26
                + relief * 0.24
                + pace_edge * 0.20
                + surface_safety * 0.18
                + safe_float(metrics.get("distance_transition_score")) * 0.12
            )
            favorite_guard = round(max(0.0, min(100.0, 70.0 - max(0.0, visibility - real_edge) * 0.65)), 1)
            metrics.update({
                "field_relative_value_score": field_relative,
                "pace_map_edge_score": pace_edge,
                "surface_switch_safety_score": surface_safety,
                "favorite_risk_guard_score": favorite_guard,
                "handicap_weight_relief_score": relief,
                "_has_field_relative_value": bool(actionable),
                "_has_pace_map_edge": True,
                "_has_surface_switch_safety": flags.get("hasSurfaceTransition", False),
                "_has_favorite_risk_guard": True,
                "_has_handicap_weight_relief": flags.get("hasHandicapWeightRelief", flags.get("hasHandicapEfficiency", False)),
            })
        else:
            metrics.update({
                "field_relative_value_score": 50.0,
                "pace_map_edge_score": 50.0,
                "surface_switch_safety_score": 50.0,
                "favorite_risk_guard_score": 50.0,
                "_has_field_relative_value": False,
                "_has_pace_map_edge": False,
                "_has_surface_switch_safety": False,
                "_has_favorite_risk_guard": False,
                "_has_handicap_weight_relief": False,
            })
        if cat == "GRUP":
            elite = (
                safe_float(metrics.get("hp_score")) * 0.20
                + safe_float(metrics.get("form_trend")) * 0.18
                + safe_float(metrics.get("trainer_score")) * 0.16
                + safe_float(metrics.get("track_experience_score")) * 0.14
                + safe_float(metrics.get("training_degree_score")) * 0.12
                + safe_float(metrics.get("surface_transition_score")) * 0.10
                + safe_float(metrics.get("distance_suit")) * 0.10
            )
            metrics["elite_consensus_score"] = round(max(0.0, min(100.0, elite)), 1)
            metrics["class_peak_score"] = safe_float(metrics.get("class_peak_score"), 50.0)
            metrics["_has_elite_consensus"] = True
            metrics["_has_class_peak"] = False
        else:
            metrics.setdefault("elite_consensus_score", 50.0)
            metrics.setdefault("class_peak_score", 50.0)
            metrics.setdefault("_has_elite_consensus", False)
            metrics.setdefault("_has_class_peak", False)

        for key, flag in {
            "_has_agf": "hasAgf",
            "_has_hp": "hasHp",
            "_has_weight": "hasWeight",
            "_has_jockey": "hasJockey",
            "_has_trainer": "hasTrainer",
            "_has_training": "hasTraining",
            "_has_training_times": "hasTrainingTimes",
            "_has_pedigree": "hasPedigree",
            "_has_age": "hasAgeActionable",
            "_has_track_experience": "hasTrackExperience",
            "_has_surface_transition": "hasSurfaceTransition",
            "_has_distance_transition": "hasDistanceTransition",
            "_has_handicap_efficiency": "hasHandicapEfficiency",
            "_has_handicap_class_history": "hasHandicapClassHistory",
        }.items():
            metrics[key] = bool(flags.get(flag, (row.get("features") or {}).get(key[5:]) is not None))
        cloned["_v421_metrics"] = metrics
        derived.append(cloned)
    return derived


def score_v421(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    scored = []
    for row in derive_v421_metrics(rows):
        profile = extract_v4_race_profile(
            row.get("race_type", ""),
            row.get("distance", ""),
            row.get("track", ""),
            safe_int(row.get("field_size"), len(rows)),
        )
        resolved = resolve_v4_profile_weights(profile)
        score = calculate_v4_shadow_score(row["_v421_metrics"], resolved["weights"])
        cloned = dict(row)
        cloned["_v421_score"] = score
        cloned["_v421_profile"] = resolved.get("selectedKey")
        scored.append(cloned)
    return scored


def spearman(pred: list[int], actual: list[int]) -> float:
    if len(pred) < 2:
        return 0.0
    mean_p = statistics.mean(pred)
    mean_a = statistics.mean(actual)
    cov = sum((p - mean_p) * (a - mean_a) for p, a in zip(pred, actual))
    var_p = sum((p - mean_p) ** 2 for p in pred)
    var_a = sum((a - mean_a) ** 2 for a in actual)
    if var_p <= 0 or var_a <= 0:
        return 0.0
    return cov / math.sqrt(var_p * var_a)


def ndcg5(ordered: list[dict[str, Any]]) -> float:
    gains = []
    field = len(ordered)
    for row in ordered[:5]:
        finish = safe_int(row.get("finish_pos"), field)
        gains.append(max(0.0, field - finish + 1))
    dcg = sum(gain / math.log2(index + 2) for index, gain in enumerate(gains))
    ideal = sorted([max(0.0, field - safe_int(row.get("finish_pos"), field) + 1) for row in ordered], reverse=True)[:5]
    idcg = sum(gain / math.log2(index + 2) for index, gain in enumerate(ideal))
    return dcg / idcg if idcg else 0.0


def evaluate(races: list[list[dict[str, Any]]], mode: str) -> dict[str, Any]:
    totals = {"races": 0, "top1": 0, "winner_top3": 0, "winner_top5": 0}
    rho_values = []
    mae_values = []
    ndcg_values = []
    for rows in races:
        rows = score_v421(rows) if mode == "v421" else rows
        if mode == "v421":
            ordered = sorted(rows, key=lambda row: row.get("_v421_score", 0.0), reverse=True)
        else:
            ordered = sorted(rows, key=lambda row: safe_int(row.get("v4_rank"), 999))
        totals["races"] += 1
        winner_index = next((idx for idx, row in enumerate(ordered) if safe_int(row.get("finish_pos")) == 1), None)
        if winner_index == 0:
            totals["top1"] += 1
        if winner_index is not None and winner_index < 3:
            totals["winner_top3"] += 1
        if winner_index is not None and winner_index < 5:
            totals["winner_top5"] += 1
        pred_rank_by_id = {id(row): index + 1 for index, row in enumerate(ordered)}
        actual_order = sorted(rows, key=lambda row: safe_int(row.get("finish_pos")))
        pred_ranks = [pred_rank_by_id[id(row)] for row in actual_order]
        actual_ranks = [safe_int(row.get("finish_pos")) for row in actual_order]
        rho_values.append(spearman(pred_ranks, actual_ranks))
        mae_values.append(statistics.mean(abs(pred_rank_by_id[id(row)] - safe_int(row.get("finish_pos"))) for row in rows))
        ndcg_values.append(ndcg5(ordered))
    return {
        **totals,
        "rho": round(statistics.mean(rho_values), 3) if rho_values else 0.0,
        "mae": round(statistics.mean(mae_values), 3) if mae_values else 0.0,
        "ndcg5": round(statistics.mean(ndcg_values), 3) if ndcg_values else 0.0,
    }


def fmt_metrics(label: str, metrics: dict[str, Any]) -> str:
    races = metrics["races"]
    return (
        f"| {label} | {races} | {metrics['top1']}/{races} | "
        f"{metrics['winner_top3']}/{races} | {metrics['winner_top5']}/{races} | "
        f"{metrics['rho']:.3f} | {metrics['mae']:.3f} | {metrics['ndcg5']:.3f} |"
    )


def write_report(races: list[list[dict[str, Any]]], out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    groups: dict[str, list[list[dict[str, Any]]]] = defaultdict(list)
    for race in races:
        groups[category(race[0])].append(race)

    lines = [
        f"# v4.21 Signal Update Report ({datetime.now().strftime('%Y-%m-%d %H:%M')})",
        "",
        "- Analysis-only report from labeled export.",
        "- v4.21 candidate keeps AGF disabled outside MAIDEN + SART1.",
        "- Historical export cannot reconstruct full class_peak_score; GRUP class peak is therefore neutral in this report.",
        "",
        "| Segment | Races | Top1 | WTop3 | WTop5 | Rho | MAE | NDCG@5 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
        fmt_metrics("v4 logged overall", evaluate(races, "v4")),
        fmt_metrics("v4.21 candidate overall", evaluate(races, "v421")),
    ]
    for group in sorted(groups):
        if group not in {"HANDIKAP", "GRUP"}:
            continue
        lines.append(fmt_metrics(f"v4 logged {group}", evaluate(groups[group], "v4")))
        lines.append(fmt_metrics(f"v4.21 candidate {group}", evaluate(groups[group], "v421")))
    lines.extend([
        "",
        "## Notes",
        "- HANDIKAP visible profile stays conservative; new field-relative, pace-map, surface-safety and favorite-risk signals are logged for validation.",
        "- GRUP candidate uses AGF-free elite consensus; full live class history will be stronger than this export-only approximation.",
    ])
    report_path = out_dir / "summary.md"
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--export-url", default=DEFAULT_EXPORT_URL)
    parser.add_argument("--out-dir", default=None)
    args = parser.parse_args()
    out_dir = Path(args.out_dir or f"reports/v421-signal-update-{datetime.now().strftime('%Y%m%d')}")
    entries = load_entries(args.export_url)
    races = group_races(entries)
    report = write_report(races, out_dir)
    print(f"v{_V4_VERSION} signal report written: {report}")


if __name__ == "__main__":
    main()
