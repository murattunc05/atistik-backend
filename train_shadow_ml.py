import argparse
import json
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np
import requests
from sklearn.metrics import ndcg_score


FEATURE_COLS = [
    "degree_avg",
    "degree_trend",
    "degree_stability",
    "form_trend",
    "track_suit",
    "track_experience_score",
    "distance_suit",
    "training_fitness",
    "training_degree_score",
    "weight_impact",
    "handicap_efficiency_score",
    "jockey_score",
    "bounce_score",
    "pace_score",
    "pedigree",
    "hp_score",
    "trainer_score",
    "agf_score",
    "age_score",
    "v4_score",
    "v4_rank",
    "field_size",
    "distance_num",
    "is_handikap",
    "is_maiden",
    "is_sartli",
    "is_kv",
    "is_grup",
    "is_satis",
    "track_kum",
    "track_cim",
    "track_sentetik",
    "has_training",
    "has_agf",
    "has_hp",
    "has_pedigree",
    "has_trainer",
    "has_age_actionable",
    "has_track_experience",
    "has_handicap_efficiency",
    "top3_feature_avg",
    "feature_variance",
]

NO_AGF_FEATURE_COLS = [col for col in FEATURE_COLS if col not in {"agf_score", "has_agf"}]


def safe_float(value, default=0.0):
    try:
        if value is None:
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def fold_text(value):
    text = str(value or "").upper()
    replacements = {
        "İ": "I",
        "I": "I",
        "Ş": "S",
        "Ğ": "G",
        "Ü": "U",
        "Ö": "O",
        "Ç": "C",
        "ı": "I",
        "ş": "S",
        "ğ": "G",
        "ü": "U",
        "ö": "O",
        "ç": "C",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    return text


def track_bucket(track):
    folded = fold_text(track)
    if "SENTETIK" in folded:
        return "Sentetik"
    if "KUM" in folded:
        return "Kum"
    if "CIM" in folded:
        return "Cim"
    return "Unknown"


def category(entry):
    profile = entry.get("v4_profile") or {}
    if profile.get("category"):
        return profile["category"]
    folded = fold_text(entry.get("race_type"))
    if "HANDIKAP" in folded:
        return "HANDIKAP"
    if "MAIDEN" in folded:
        return "MAIDEN"
    if "SART" in folded:
        return "SARTLI"
    if "KV" in folded:
        return "KV"
    if "GRUP" in folded or " G1" in folded or " G2" in folded or " G3" in folded:
        return "GRUP"
    if "SATIS" in folded:
        return "SATIS"
    return "GLOBAL"


def handikap_profile(entry):
    profile = entry.get("v4_profile") or {}
    selected = profile.get("selectedKey")
    if selected and selected.startswith("HANDIKAP"):
        return selected
    folded = fold_text(entry.get("race_type"))
    import re

    match = re.search(r"HANDIKAP\s*(\d+)", folded)
    subtype = f"HANDIKAP{match.group(1)}" if match else "HANDIKAP"
    bucket = track_bucket(entry.get("track"))
    return f"{subtype}|{bucket}" if subtype != "HANDIKAP" else "HANDIKAP"


def feature_dict(entry):
    metrics = entry.get("features") or {}
    flags = entry.get("metric_source_flags") or {}
    race_type = entry.get("race_type") or ""
    folded_type = fold_text(race_type)
    track = track_bucket(entry.get("track"))
    try:
        distance_num = int("".join(ch for ch in str(entry.get("distance") or "") if ch.isdigit()) or 0)
    except ValueError:
        distance_num = 0

    metric_keys = [
        "degree_avg",
        "degree_trend",
        "degree_stability",
        "form_trend",
        "track_suit",
        "track_experience_score",
        "distance_suit",
        "training_fitness",
        "training_degree_score",
        "weight_impact",
        "handicap_efficiency_score",
        "jockey_score",
        "bounce_score",
        "pace_score",
        "pedigree",
        "hp_score",
        "trainer_score",
        "agf_score",
        "age_score",
    ]
    features = {key: safe_float(metrics.get(key), 50.0) for key in metric_keys}
    features.update(
        {
            "v4_score": safe_float(entry.get("v4_score"), safe_float(entry.get("ai_score"), 0.0)),
            "v4_rank": safe_float(entry.get("v4_rank"), safe_float(entry.get("rank_pred"), 0.0)),
            "field_size": safe_float(entry.get("field_size"), 0.0),
            "distance_num": safe_float(distance_num, 0.0),
            "is_handikap": 1.0 if "HANDIKAP" in folded_type else 0.0,
            "is_maiden": 1.0 if "MAIDEN" in folded_type or "MDN" in folded_type else 0.0,
            "is_sartli": 1.0 if "SART" in folded_type else 0.0,
            "is_kv": 1.0 if "KV" in folded_type else 0.0,
            "is_grup": 1.0 if "GRUP" in folded_type or " G1" in folded_type or " G2" in folded_type or " G3" in folded_type else 0.0,
            "is_satis": 1.0 if "SATIS" in folded_type else 0.0,
            "track_kum": 1.0 if track == "Kum" else 0.0,
            "track_cim": 1.0 if track == "Cim" else 0.0,
            "track_sentetik": 1.0 if track == "Sentetik" else 0.0,
            "has_training": 1.0 if flags.get("hasTraining") else 0.0,
            "has_agf": 1.0 if flags.get("hasAgf") else 0.0,
            "has_hp": 1.0 if flags.get("hasHp") else 0.0,
            "has_pedigree": 1.0 if flags.get("hasPedigree") else 0.0,
            "has_trainer": 1.0 if flags.get("hasTrainer") else 0.0,
            "has_age_actionable": 1.0 if flags.get("hasAgeActionable") else 0.0,
            "has_track_experience": 1.0 if flags.get("hasTrackExperience") else 0.0,
            "has_handicap_efficiency": 1.0 if flags.get("hasHandicapEfficiency") else 0.0,
        }
    )
    values = [features[key] for key in metric_keys]
    features["top3_feature_avg"] = float(np.mean(sorted(values)[-3:])) if values else 50.0
    features["feature_variance"] = float(np.var(values)) if values else 0.0
    return features


def load_entries(args):
    if args.input:
        with open(args.input, "r", encoding="utf-8") as f:
            payload = json.load(f)
    else:
        response = requests.get(args.export_url, timeout=120)
        response.raise_for_status()
        payload = response.json()
    entries = payload.get("entries", payload if isinstance(payload, list) else [])
    clean = []
    for entry in entries:
        if entry.get("finish_pos") is None:
            continue
        if not entry.get("features"):
            continue
        clean.append(entry)
    return clean


def race_sort_key(race_entries):
    entry = race_entries[0]
    raw_date = str(entry.get("race_date") or "")
    try:
        date_key = datetime.strptime(raw_date, "%d.%m.%Y")
    except ValueError:
        date_key = datetime.min
    return (date_key, str(entry.get("race_no") or ""), str(entry.get("race_id") or ""))


def split_races(entries, validation_ratio=0.2):
    races = defaultdict(list)
    for entry in entries:
        races[str(entry.get("race_id"))].append(entry)
    race_items = sorted(races.items(), key=lambda item: race_sort_key(item[1]))
    split_at = max(1, int(len(race_items) * (1.0 - validation_ratio)))
    return dict(race_items[:split_at]), dict(race_items[split_at:])


def matrix_from_races(races, feature_cols):
    X, y, groups, flat_entries = [], [], [], []
    for _, rows in races.items():
        valid_rows = [row for row in rows if row.get("finish_pos") is not None]
        valid_rows.sort(key=lambda row: safe_float(row.get("finish_pos"), 999.0))
        if len(valid_rows) < 2:
            continue
        field_size = max(int(safe_float(row.get("field_size"), len(valid_rows))) for row in valid_rows)
        groups.append(len(valid_rows))
        for row in valid_rows:
            values = feature_dict(row)
            X.append([safe_float(values.get(col), 0.0) for col in feature_cols])
            relevance = max(0.0, field_size - safe_float(row.get("finish_pos"), field_size) + 1.0)
            y.append(relevance)
            flat_entries.append(row)
    return np.array(X, dtype=np.float32), np.array(y, dtype=np.float32), groups, flat_entries


def train_ranker(train_races, feature_cols):
    import xgboost as xgb

    X, y, groups, _ = matrix_from_races(train_races, feature_cols)
    model = xgb.XGBRanker(
        objective="rank:ndcg",
        n_estimators=220,
        learning_rate=0.045,
        max_depth=4,
        subsample=0.9,
        colsample_bytree=0.9,
        reg_lambda=1.2,
        random_state=42,
        tree_method="hist",
    )
    model.fit(X, y, group=groups)
    return model


def rank_from_scores(rows, scores):
    order = sorted(range(len(rows)), key=lambda idx: scores[idx], reverse=True)
    ranks = {}
    for rank, idx in enumerate(order, start=1):
        ranks[id(rows[idx])] = rank
    return ranks


def rank_from_existing(rows, key, descending=False):
    if descending:
        ordered = sorted(rows, key=lambda row: safe_float((row.get("features") or {}).get(key), -999.0), reverse=True)
    else:
        ordered = sorted(rows, key=lambda row: safe_float(row.get(key), 999.0))
    return {id(row): rank for rank, row in enumerate(ordered, start=1)}


def spearman(pairs):
    if len(pairs) < 2:
        return None
    xs = [float(a) for a, _ in pairs]
    ys = [float(b) for _, b in pairs]
    mx, my = float(np.mean(xs)), float(np.mean(ys))
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = math.sqrt(sum((x - mx) ** 2 for x in xs) * sum((y - my) ** 2 for y in ys))
    return num / den if den else None


def evaluate_ranks(races, rank_builder):
    top1 = top3 = top5 = 0
    rhos, maes, ndcgs, winner_ranks, top_finishes = [], [], [], [], []
    race_count = 0
    for _, rows in races.items():
        rows = [row for row in rows if row.get("finish_pos") is not None]
        if len(rows) < 2:
            continue
        ranks = rank_builder(rows)
        if not ranks:
            continue
        race_count += 1
        winner = min(rows, key=lambda row: safe_float(row.get("finish_pos"), 999.0))
        top = min(rows, key=lambda row: ranks.get(id(row), 999))
        winner_rank = ranks.get(id(winner), 999)
        top1 += int(top.get("finish_pos") == 1)
        top3 += int(winner_rank <= 3)
        top5 += int(winner_rank <= 5)
        winner_ranks.append(winner_rank)
        top_finishes.append(safe_float(top.get("finish_pos"), 999.0))
        pairs = []
        y_true, y_score = [], []
        field_size = max(int(safe_float(row.get("field_size"), len(rows))) for row in rows)
        for row in rows:
            pred_rank = ranks.get(id(row), 999)
            finish_pos = safe_float(row.get("finish_pos"), 999.0)
            pairs.append((pred_rank, finish_pos))
            maes.append(abs(pred_rank - finish_pos))
            y_true.append(max(0.0, field_size - finish_pos + 1.0))
            y_score.append(-pred_rank)
        rho = spearman(pairs)
        if rho is not None:
            rhos.append(rho)
        try:
            ndcgs.append(float(ndcg_score([y_true], [y_score], k=min(5, len(rows)))))
        except Exception:
            pass
    return {
        "races": race_count,
        "top1": top1,
        "winner_top3": top3,
        "winner_top5": top5,
        "rho": float(np.mean(rhos)) if rhos else None,
        "mae": float(np.mean(maes)) if maes else None,
        "ndcg5": float(np.mean(ndcgs)) if ndcgs else None,
        "avg_winner_rank": float(np.mean(winner_ranks)) if winner_ranks else None,
        "avg_top_finish": float(np.mean(top_finishes)) if top_finishes else None,
    }


def evaluate_model(model, races, feature_cols):
    def builder(rows):
        X = np.array(
            [[safe_float(feature_dict(row).get(col), 0.0) for col in feature_cols] for row in rows],
            dtype=np.float32,
        )
        scores = model.predict(X)
        return rank_from_scores(rows, scores)

    return evaluate_ranks(races, builder)


def evaluate_existing(races, key):
    return evaluate_ranks(races, lambda rows: rank_from_existing(rows, key))


def evaluate_agf(races):
    return evaluate_ranks(races, lambda rows: rank_from_existing(rows, "agf_score", descending=True))


def fmt(value):
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def metrics_row(name, metrics):
    return (
        f"| {name} | {metrics['races']} | {metrics['top1']}/{metrics['races']} | "
        f"{metrics['winner_top3']}/{metrics['races']} | {metrics['winner_top5']}/{metrics['races']} | "
        f"{fmt(metrics['rho'])} | {fmt(metrics['mae'])} | {fmt(metrics['ndcg5'])} | "
        f"{fmt(metrics['avg_winner_rank'])} | {fmt(metrics['avg_top_finish'])} |"
    )


def subset_by_group(races, group_name):
    return {
        race_id: rows
        for race_id, rows in races.items()
        if rows and category(rows[0]) == group_name
    }


def subset_by_handikap_profile(races, profile_name):
    return {
        race_id: rows
        for race_id, rows in races.items()
        if rows and category(rows[0]) == "HANDIKAP" and handikap_profile(rows[0]) == profile_name
    }


def feature_stats(entries, feature_cols):
    values = defaultdict(list)
    for entry in entries:
        f = feature_dict(entry)
        for col in feature_cols:
            values[col].append(safe_float(f.get(col), 0.0))
    return {
        col: {
            "mean": float(np.mean(vals)) if vals else 0.0,
            "std": float(np.std(vals)) if vals else 0.0,
        }
        for col, vals in values.items()
    }


def write_report(path, metadata, validation_races, model_agf, model_no_agf):
    sections = []
    sections.append(f"# Shadow ML Training Report - {metadata['model_version']}\n")
    sections.append(
        f"- Train races: {metadata['train_races']}\n"
        f"- Validation races: {metadata['validation_races']}\n"
        f"- Labeled entries: {metadata['labeled_entries']}\n"
        f"- Split: chronological race-level 80/20\n"
        f"- Visible ranking impact: none\n"
    )
    header = "| Model | Races | Top1 | Winner Top3 | Winner Top5 | Rho | MAE | NDCG@5 | Avg winner rank | Avg top finish |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"

    def add_table(title, races):
        if not races:
            return
        sections.append(f"\n## {title}\n\n{header}")
        sections.append(metrics_row("v4.11", evaluate_existing(races, "v4_rank")))
        sections.append(metrics_row("ML + AGF", evaluate_model(model_agf, races, FEATURE_COLS)))
        sections.append(metrics_row("ML - AGF", evaluate_model(model_no_agf, races, NO_AGF_FEATURE_COLS)))
        sections.append(metrics_row("AGF only", evaluate_agf(races)))

    add_table("Validation Overall", validation_races)
    for group in ["HANDIKAP", "MAIDEN", "SARTLI", "KV", "GRUP", "SATIS"]:
        add_table(f"Validation {group}", subset_by_group(validation_races, group))
    for profile in ["HANDIKAP14|Kum", "HANDIKAP15|Kum", "HANDIKAP15|Cim", "HANDIKAP16|Kum", "HANDIKAP16|Cim"]:
        add_table(f"Validation {profile}", subset_by_handikap_profile(validation_races, profile))

    path.write_text("\n".join(sections) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Train Atistik shadow ML ranker from predictions.jsonl export.")
    parser.add_argument("--export-url", default="https://atistik-backend.onrender.com/api/ml-export?labeled_only=true")
    parser.add_argument("--input", help="Optional local JSON export file.")
    parser.add_argument("--output-dir", default=".")
    parser.add_argument("--validation-ratio", type=float, default=0.2)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    entries = load_entries(args)
    if len(entries) < 100:
        raise SystemExit(f"Not enough labeled entries for training: {len(entries)}")
    train_races, validation_races = split_races(entries, args.validation_ratio)
    if len(validation_races) < 10:
        raise SystemExit(f"Not enough validation races: {len(validation_races)}")

    model_agf = train_ranker(train_races, FEATURE_COLS)
    model_no_agf = train_ranker(train_races, NO_AGF_FEATURE_COLS)
    model_version = "shadow-" + datetime.now().strftime("%Y%m%d-%H%M")
    metadata = {
        "model_version": model_version,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "train_races": len(train_races),
        "validation_races": len(validation_races),
        "labeled_entries": len(entries),
        "includes_agf": True,
        "objective": "rank:ndcg",
        "retrain_rule": "+50 labeled races or weekly, whichever comes first",
        "activation_rule": "shadow only until 1000 overall races, 120 profile races, and 3 consecutive reports beat v4",
    }

    model_path = output_dir / "model_shadow_ranker.json"
    stats_path = output_dir / "feature_stats_shadow.json"
    report_path = output_dir / f"ml_training_report_{datetime.now().strftime('%Y%m%d')}.md"

    model_agf.save_model(str(model_path))
    stats_payload = {
        "feature_cols": FEATURE_COLS,
        "stats": feature_stats(entries, FEATURE_COLS),
        "metadata": metadata,
    }
    stats_path.write_text(json.dumps(stats_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    write_report(report_path, metadata, validation_races, model_agf, model_no_agf)

    print(json.dumps({
        "model": str(model_path),
        "stats": str(stats_path),
        "report": str(report_path),
        "metadata": metadata,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
