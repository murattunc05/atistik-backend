import argparse
import json
import math
import re
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np
from sklearn.metrics import ndcg_score


FEATURE_COLS = [
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
    "h16_training_degree_edge",
    "weight_impact",
    "handicap_efficiency_score",
    "handicap_weight_relief_score",
    "handicap_class_transition_score",
    "handicap_class_delta",
    "field_relative_value_score",
    "pace_map_edge_score",
    "surface_switch_safety_score",
    "favorite_risk_guard_score",
    "class_peak_score",
    "elite_consensus_score",
    "running_style_proxy_score",
    "pace_pressure",
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
    "is_handikap16",
    "is_maiden",
    "is_sartli",
    "is_sart1",
    "is_kv",
    "is_grup",
    "is_satis",
    "track_kum",
    "track_cim",
    "track_sentetik",
    "has_training",
    "has_training_times",
    "has_agf",
    "has_hp",
    "has_pedigree",
    "has_trainer",
    "has_age_actionable",
    "has_track_experience",
    "has_surface_transition",
    "has_distance_transition",
    "has_handicap_efficiency",
    "has_handicap_class_history",
    "days_since_last_race",
    "last_race_distance",
    "long_layoff_bucket",
    "recent_long_race_flag",
    "top3_feature_avg",
    "feature_variance",
]

AGF_INFLUENCED_FEATURE_COLS = {
    "agf_score",
    "has_agf",
    # v4 is allowed to consume AGF in MAIDEN/SART1, so its score and rank are
    # also downstream AGF signals and cannot enter a strict no-AGF artifact.
    "v4_score",
    "v4_rank",
    # These aggregates include agf_score in the exceptional profiles where
    # AGF is allowed, so a strictly no-AGF artifact must omit them as well.
    "top3_feature_avg",
    "feature_variance",
}
NO_AGF_FEATURE_COLS = [col for col in FEATURE_COLS if col not in AGF_INFLUENCED_FEATURE_COLS]

# These metrics were added after the currently deployed shadow model.  They
# must not enter a candidate merely because their neutral fallback is present;
# require enough races with a real upstream source in the *training* slice.
SOURCE_FLAG_BY_FEATURE = {
    "training_degree_score": "hasTrainingTimes",
    "h16_training_degree_edge": "hasTrainingTimes",
    "handicap_weight_relief_score": "hasHandicapWeightRelief",
    "field_relative_value_score": "hasFieldRelativeValue",
    "pace_map_edge_score": "hasPaceMapEdge",
    "surface_switch_safety_score": "hasSurfaceSwitchSafety",
    "favorite_risk_guard_score": "hasFavoriteRiskGuard",
    "class_peak_score": "hasClassPeak",
    "elite_consensus_score": "hasEliteConsensus",
}
SOURCE_PROFILE_BY_FEATURE = {
    "h16_training_degree_edge": "HANDIKAP16",
}
DEFAULT_MIN_SOURCE_RACES = 25
DEFAULT_MIN_SOURCE_RATIO = 0.05
TERMINAL_FINISH_POSITIONS = {99}


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
        "Ţ": "S",
        "ţ": "S",
        "Ț": "S",
        "ț": "S",
        "Þ": "S",
        "Åž": "S",
        "ÅŸ": "S",
        "Ä°": "I",
        "Ä±": "I",
        "Äž": "G",
        "ÄŸ": "G",
        "Ãœ": "U",
        "Ã¼": "U",
        "Ã–": "O",
        "Ã¶": "O",
        "Ã‡": "C",
        "Ã§": "C",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", text).strip()


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
    match = re.search(r"HANDIKAP\s*(\d+)", folded)
    subtype = f"HANDIKAP{match.group(1)}" if match else "HANDIKAP"
    bucket = track_bucket(entry.get("track"))
    return f"{subtype}|{bucket}" if subtype != "HANDIKAP" else "HANDIKAP"


def profile_subtype(entry):
    profile = entry.get("v4_profile") or {}
    subtype = str(profile.get("subtype") or "").upper()
    if subtype:
        return re.sub(r"^SARTLI(?=\d)", "SART", subtype)
    folded = fold_text(entry.get("race_type"))
    match = re.search(r"(HANDIKAP|SART(?:LI)?)\s*[-/]?\s*(\d+)", folded)
    if not match:
        return category(entry)
    prefix = "SART" if match.group(1).startswith("SART") else "HANDIKAP"
    return f"{prefix}{match.group(2)}"


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

    score_keys = [
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
        "handicap_weight_relief_score",
        "field_relative_value_score",
        "pace_map_edge_score",
        "surface_switch_safety_score",
        "favorite_risk_guard_score",
        "class_peak_score",
        "elite_consensus_score",
        "jockey_score",
        "bounce_score",
        "pace_score",
        "pedigree",
        "hp_score",
        "trainer_score",
        "agf_score",
        "age_score",
    ]
    is_maiden = "MAIDEN" in folded_type or "MDN" in folded_type
    is_handikap = "HANDIKAP" in folded_type
    is_handikap16 = profile_subtype(entry) == "HANDIKAP16"
    is_sartli = "SART" in folded_type
    is_sart1 = bool(is_sartli and re.search(r"\bSART(?:LI)?\s*[-/]?\s*1\b", folded_type))
    agf_allowed = bool(entry.get("agf_allowed_for_ranking", is_maiden or is_sart1))
    has_training_times = bool(flags.get("hasTrainingTimes"))

    features = {key: safe_float(metrics.get(key), 50.0) for key in score_keys}
    if not agf_allowed:
        features["agf_score"] = 50.0
    features["h16_training_degree_edge"] = (
        (features["training_degree_score"] - 50.0)
        if is_handikap16 and has_training_times
        else 0.0
    )
    features.update(
        {
            "handicap_class_transition_score": safe_float(metrics.get("handicap_class_transition_score"), 50.0),
            "handicap_class_delta": safe_float(metrics.get("handicap_class_delta"), 0.0),
            "handicap_weight_relief_score": safe_float(metrics.get("handicap_weight_relief_score"), 50.0),
            "field_relative_value_score": safe_float(metrics.get("field_relative_value_score"), 50.0),
            "pace_map_edge_score": safe_float(metrics.get("pace_map_edge_score"), 50.0),
            "surface_switch_safety_score": safe_float(metrics.get("surface_switch_safety_score"), 50.0),
            "favorite_risk_guard_score": safe_float(metrics.get("favorite_risk_guard_score"), 50.0),
            "class_peak_score": safe_float(metrics.get("class_peak_score"), 50.0),
            "elite_consensus_score": safe_float(metrics.get("elite_consensus_score"), 50.0),
            "running_style_proxy_score": safe_float(metrics.get("running_style_proxy_score"), 50.0),
            "pace_pressure": safe_float(metrics.get("pace_pressure"), 0.0),
        }
    )
    features.update(
        {
            "v4_score": safe_float(entry.get("v4_score"), safe_float(entry.get("ai_score"), 0.0)),
            "v4_rank": safe_float(entry.get("v4_rank"), safe_float(entry.get("rank_pred"), 0.0)),
            "field_size": safe_float(entry.get("field_size"), 0.0),
            "distance_num": safe_float(distance_num, 0.0),
            "is_handikap": 1.0 if is_handikap else 0.0,
            "is_handikap16": 1.0 if is_handikap16 else 0.0,
            "is_maiden": 1.0 if is_maiden else 0.0,
            "is_sartli": 1.0 if is_sartli else 0.0,
            "is_sart1": 1.0 if is_sart1 else 0.0,
            "is_kv": 1.0 if "KV" in folded_type else 0.0,
            "is_grup": 1.0 if "GRUP" in folded_type or " G1" in folded_type or " G2" in folded_type or " G3" in folded_type else 0.0,
            "is_satis": 1.0 if "SATIS" in folded_type else 0.0,
            "track_kum": 1.0 if track == "Kum" else 0.0,
            "track_cim": 1.0 if track == "Cim" else 0.0,
            "track_sentetik": 1.0 if track == "Sentetik" else 0.0,
            "has_training": 1.0 if flags.get("hasTraining") else 0.0,
            "has_training_times": 1.0 if has_training_times else 0.0,
            "has_agf": 1.0 if agf_allowed and flags.get("hasAgf") else 0.0,
            "has_hp": 1.0 if flags.get("hasHp") else 0.0,
            "has_pedigree": 1.0 if flags.get("hasPedigree") else 0.0,
            "has_trainer": 1.0 if flags.get("hasTrainer") else 0.0,
            "has_age_actionable": 1.0 if flags.get("hasAgeActionable") else 0.0,
            "has_track_experience": 1.0 if flags.get("hasTrackExperience") else 0.0,
            "has_surface_transition": 1.0 if flags.get("hasSurfaceTransition") else 0.0,
            "has_distance_transition": 1.0 if flags.get("hasDistanceTransition") else 0.0,
            "has_handicap_efficiency": 1.0 if flags.get("hasHandicapEfficiency") else 0.0,
            "has_handicap_weight_relief": 1.0 if flags.get("hasHandicapWeightRelief") else 0.0,
            "has_handicap_class_history": 1.0 if flags.get("hasHandicapClassHistory") else 0.0,
            "has_field_relative_value": 1.0 if flags.get("hasFieldRelativeValue") else 0.0,
            "has_pace_map_edge": 1.0 if flags.get("hasPaceMapEdge") else 0.0,
            "has_surface_switch_safety": 1.0 if flags.get("hasSurfaceSwitchSafety") else 0.0,
            "has_favorite_risk_guard": 1.0 if flags.get("hasFavoriteRiskGuard") else 0.0,
            "has_class_peak": 1.0 if flags.get("hasClassPeak") else 0.0,
            "has_elite_consensus": 1.0 if flags.get("hasEliteConsensus") else 0.0,
            "days_since_last_race": safe_float(entry.get("days_since_last_race"), -1.0),
            "last_race_distance": safe_float(entry.get("last_race_distance"), 0.0),
            "long_layoff_bucket": (
                3.0 if safe_float(entry.get("days_since_last_race"), -1.0) >= 91
                else 2.0 if safe_float(entry.get("days_since_last_race"), -1.0) >= 61
                else 1.0 if safe_float(entry.get("days_since_last_race"), -1.0) >= 40
                else 0.0
            ),
            "recent_long_race_flag": 1.0 if any(
                item.get("code") == "recent_long_race"
                for item in (entry.get("ranking_penalties") or [])
                if isinstance(item, dict)
            ) else 0.0,
        }
    )
    active_score_keys = [key for key in score_keys if agf_allowed or key != "agf_score"]
    values = [features[key] for key in active_score_keys]
    features["top3_feature_avg"] = float(np.mean(sorted(values)[-3:])) if values else 50.0
    features["feature_variance"] = float(np.var(values)) if values else 0.0
    return features


def race_key(entry):
    """Return a stable key even if upstream race ids are reused across dates."""
    raw_date = str(entry.get("race_date") or "unknown-date")
    race_id = str(entry.get("race_id") or "").strip()
    if race_id:
        return f"{raw_date}|{race_id}"
    city = str(entry.get("city") or entry.get("hippodrome") or "unknown-city")
    return f"{raw_date}|{city}|{entry.get('race_no') or 'unknown-race'}"


def valid_finish_position(value):
    try:
        finish = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(finish) and finish > 0


def finish_rank_integrity(rows):
    """Validate competition ranking while treating 99 as terminal status."""
    ranks = [int(float(row.get("finish_pos"))) for row in rows]
    terminal_status_count = sum(1 for rank in ranks if rank in TERMINAL_FINISH_POSITIONS)
    ranked_positions = [rank for rank in ranks if rank not in TERMINAL_FINISH_POSITIONS]
    rank_counts = Counter(ranked_positions)
    out_of_range_count = sum(1 for rank in ranked_positions if rank > len(rows))
    expected_rank = 1
    competition_pattern_valid = True
    for rank, tied_count in sorted(rank_counts.items()):
        if rank != expected_rank:
            competition_pattern_valid = False
            break
        expected_rank += tied_count
    return {
        "valid": not out_of_range_count and competition_pattern_valid,
        "terminal_status_count": terminal_status_count,
        "out_of_range_count": out_of_range_count,
        "competition_pattern_valid": competition_pattern_valid,
        "has_tie": any(count > 1 for count in rank_counts.values()),
    }


def _payload_entries(payload):
    if isinstance(payload, dict):
        entries = payload.get("entries", [])
    elif isinstance(payload, list):
        entries = payload
    else:
        entries = []
    return [entry for entry in entries if isinstance(entry, dict)]


def _read_local_entries(path):
    """Read either an API JSON export or the canonical predictions JSONL."""
    with open(path, "r", encoding="utf-8") as f:
        try:
            return _payload_entries(json.load(f))
        except json.JSONDecodeError:
            f.seek(0)
            entries = []
            for line_number, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"Invalid JSONL at {path}:{line_number}: {exc}") from exc
                if isinstance(item, dict):
                    entries.append(item)
            return entries


def filter_training_entries(entries, include_partial_races=False):
    """Select complete races by default so rank groups never contain partial fields."""
    races = defaultdict(list)
    for entry in entries:
        races[race_key(entry)].append(entry)

    clean = []
    summary = Counter()
    summary["raw_entries"] = len(entries)
    summary["raw_races"] = len(races)
    for key in [
        "integrity_clean_races",
        "integrity_invalid_races",
        "integrity_invalid_rows",
        "rank_out_of_range_races",
        "rank_out_of_range_rows",
        "competition_pattern_invalid_races",
        "valid_tie_races",
        "terminal_status_races",
        "terminal_status_rows",
    ]:
        summary[key] = 0
    for rows in races.values():
        labeled_rows = [row for row in rows if valid_finish_position(row.get("finish_pos"))]
        feature_rows = [row for row in rows if row.get("features")]
        if not labeled_rows:
            summary["unlabeled_races"] += 1
            continue
        if len(labeled_rows) != len(rows):
            summary["partial_races"] += 1
            if include_partial_races:
                clean.extend(row for row in labeled_rows if row.get("features"))
            continue
        integrity = finish_rank_integrity(rows)
        if integrity["terminal_status_count"]:
            summary["terminal_status_races"] += 1
            summary["terminal_status_rows"] += integrity["terminal_status_count"]
        if integrity["out_of_range_count"]:
            summary["rank_out_of_range_races"] += 1
            summary["rank_out_of_range_rows"] += integrity["out_of_range_count"]
        if not integrity["competition_pattern_valid"]:
            summary["competition_pattern_invalid_races"] += 1
        if not integrity["valid"]:
            summary["integrity_invalid_races"] += 1
            summary["integrity_invalid_rows"] += len(rows)
            continue
        summary["integrity_clean_races"] += 1
        if integrity["has_tie"]:
            summary["valid_tie_races"] += 1
        if len(feature_rows) != len(rows):
            summary["missing_feature_races"] += 1
            continue
        if len(rows) < 2:
            summary["single_runner_races"] += 1
            continue
        summary["complete_races"] += 1
        clean.extend(rows)

    summary["selected_entries"] = len(clean)
    summary["selected_races"] = len({race_key(entry) for entry in clean})
    return clean, dict(summary)


def load_entries(args, with_summary=False):
    if args.input:
        entries = _read_local_entries(args.input)
    else:
        import requests

        response = requests.get(args.export_url, timeout=120)
        response.raise_for_status()
        entries = _payload_entries(response.json())
    clean, summary = filter_training_entries(
        entries,
        include_partial_races=bool(getattr(args, "include_partial_races", False)),
    )
    return (clean, summary) if with_summary else clean


def race_sort_key(race_entries):
    entry = race_entries[0]
    raw_date = str(entry.get("race_date") or "")
    try:
        date_key = datetime.strptime(raw_date, "%d.%m.%Y")
    except ValueError:
        date_key = datetime.min
    race_no = safe_float(entry.get("race_no"), 999.0)
    return (date_key, race_no, str(entry.get("race_id") or ""))


def _race_items(entries):
    races = defaultdict(list)
    for entry in entries:
        races[race_key(entry)].append(entry)
    return sorted(races.items(), key=lambda item: race_sort_key(item[1]))


def _date_blocks(entries):
    blocks = []
    for race_id, rows in _race_items(entries):
        raw_date = str(rows[0].get("race_date") or "")
        if not blocks or blocks[-1][0] != raw_date:
            blocks.append((raw_date, []))
        blocks[-1][1].append((race_id, rows))
    return blocks


def _closest_date_boundary(blocks, target_races):
    if len(blocks) < 2:
        return 0
    cumulative = 0
    choices = []
    for index, (_, items) in enumerate(blocks[:-1], start=1):
        cumulative += len(items)
        choices.append((abs(cumulative - target_races), index))
    return min(choices)[1]


def _dict_from_blocks(blocks):
    return dict(item for _, block_items in blocks for item in block_items)


def split_races(entries, validation_ratio=0.2):
    blocks = _date_blocks(entries)
    race_count = sum(len(items) for _, items in blocks)
    target = max(1, int(race_count * (1.0 - validation_ratio)))
    split_at = _closest_date_boundary(blocks, target)
    if not split_at:
        return _dict_from_blocks(blocks), {}
    return _dict_from_blocks(blocks[:split_at]), _dict_from_blocks(blocks[split_at:])


def walk_forward_splits(entries, fold_count=3, initial_train_ratio=0.55):
    blocks = _date_blocks(entries)
    race_count = sum(len(items) for _, items in blocks)
    initial_boundary = _closest_date_boundary(blocks, max(1, int(race_count * initial_train_ratio)))
    remaining_blocks = blocks[initial_boundary:]
    if not initial_boundary or len(remaining_blocks) < fold_count:
        return []

    fold_size = max(1, math.ceil(len(remaining_blocks) / fold_count))
    splits = []
    for fold_index in range(fold_count):
        validation_start = initial_boundary + fold_index * fold_size
        validation_end = min(len(blocks), validation_start + fold_size)
        if validation_start >= validation_end:
            break
        splits.append((
            _dict_from_blocks(blocks[:validation_start]),
            _dict_from_blocks(blocks[validation_start:validation_end]),
        ))
    return splits


def select_feature_cols(
    train_races,
    base_cols,
    min_source_races=DEFAULT_MIN_SOURCE_RACES,
    min_source_ratio=DEFAULT_MIN_SOURCE_RATIO,
):
    race_count = len(train_races)
    selected = []
    coverage = {}
    for col in base_cols:
        source_flag = SOURCE_FLAG_BY_FEATURE.get(col)
        if not source_flag:
            selected.append(col)
            continue
        source_profile = SOURCE_PROFILE_BY_FEATURE.get(col)
        eligible_races = [
            rows for rows in train_races.values()
            if not source_profile or (rows and profile_subtype(rows[0]) == source_profile)
        ]
        source_races = sum(
            1
            for rows in eligible_races
            if any((row.get("metric_source_flags") or {}).get(source_flag) for row in rows)
        )
        eligible_race_count = len(eligible_races)
        ratio = source_races / eligible_race_count if eligible_race_count else 0.0
        accepted = source_races >= min_source_races and ratio >= min_source_ratio
        coverage[col] = {
            "source_flag": source_flag,
            "source_profile": source_profile,
            "source_races": source_races,
            "train_races": race_count,
            "eligible_races": eligible_race_count,
            "ratio": ratio,
            "accepted": accepted,
        }
        if accepted:
            selected.append(col)
    return selected, coverage


def without_agf_features(feature_cols):
    return [col for col in feature_cols if col not in AGF_INFLUENCED_FEATURE_COLS]


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


def rank_from_visible_v4(rows):
    v4_values = [safe_float(row.get("v4_rank"), 0.0) for row in rows]
    if sorted(int(value) for value in v4_values) == list(range(1, len(rows) + 1)):
        return rank_from_existing(rows, "v4_rank")
    return rank_from_existing(rows, "rank_pred")


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
    if key == "v4_rank":
        return evaluate_ranks(races, rank_from_visible_v4)
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


def detected_v4_label(races):
    versions = Counter(
        str(row.get("v4_version"))
        for rows in races.values()
        for row in rows
        if row.get("v4_version")
    )
    return f"v{versions.most_common(1)[0][0]}" if versions else "v4"


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


def subset_by_profile_subtype(races, subtype):
    return {
        race_id: rows
        for race_id, rows in races.items()
        if rows and profile_subtype(rows[0]) == subtype
    }


def normalized_boundary(scores):
    values = sorted((float(value) for value in scores), reverse=True)
    if len(values) < 4:
        return {"gap": None, "cutoff_crowd": len(values)}
    score_range = values[0] - values[-1]
    if score_range <= 1e-12:
        return {"gap": 0.0, "cutoff_crowd": len(values)}
    normalized = [(value - values[-1]) / score_range for value in values]
    cutoff = normalized[2]
    return {
        "gap": max(0.0, cutoff - normalized[3]),
        "cutoff_crowd": sum(abs(value - cutoff) <= 0.10 for value in normalized),
    }


def _median_or_none(values):
    return float(np.median(values)) if values else None


def compare_model_to_existing(model, races, feature_cols):
    rescues = damages = baseline_top3 = candidate_top3 = 0
    baseline_top1 = candidate_top1 = 0
    baseline_gaps, candidate_gaps = [], []
    baseline_crowds, candidate_crowds = [], []
    race_count = 0
    for _, raw_rows in races.items():
        rows = [row for row in raw_rows if row.get("finish_pos") is not None]
        if len(rows) < 2:
            continue
        X = np.array(
            [[safe_float(feature_dict(row).get(col), 0.0) for col in feature_cols] for row in rows],
            dtype=np.float32,
        )
        candidate_scores = [float(value) for value in model.predict(X)]
        candidate_ranks = rank_from_scores(rows, candidate_scores)
        baseline_ranks = rank_from_visible_v4(rows)
        winner = min(rows, key=lambda row: safe_float(row.get("finish_pos"), 999.0))
        baseline_rank = baseline_ranks.get(id(winner), 999)
        candidate_rank = candidate_ranks.get(id(winner), 999)
        baseline_hit = baseline_rank <= 3
        candidate_hit = candidate_rank <= 3
        baseline_top3 += int(baseline_hit)
        candidate_top3 += int(candidate_hit)
        baseline_top1 += int(baseline_rank == 1)
        candidate_top1 += int(candidate_rank == 1)
        rescues += int(not baseline_hit and candidate_hit)
        damages += int(baseline_hit and not candidate_hit)
        baseline_scores = [
            safe_float(row.get("v4_score"), safe_float(row.get("ai_score"), 0.0))
            for row in rows
        ]
        baseline_separation = normalized_boundary(baseline_scores)
        candidate_separation = normalized_boundary(candidate_scores)
        if baseline_separation["gap"] is not None:
            baseline_gaps.append(baseline_separation["gap"])
        if candidate_separation["gap"] is not None:
            candidate_gaps.append(candidate_separation["gap"])
        baseline_crowds.append(baseline_separation["cutoff_crowd"])
        candidate_crowds.append(candidate_separation["cutoff_crowd"])
        race_count += 1
    baseline_gap = _median_or_none(baseline_gaps)
    candidate_gap = _median_or_none(candidate_gaps)
    baseline_crowd = _median_or_none(baseline_crowds)
    candidate_crowd = _median_or_none(candidate_crowds)
    return {
        "races": race_count,
        "baselineWinnerTop3": baseline_top3,
        "candidateWinnerTop3": candidate_top3,
        "rescues": rescues,
        "damages": damages,
        "winnerTop3Net": rescues - damages,
        "baselineTop1": baseline_top1,
        "candidateTop1": candidate_top1,
        "top1Net": candidate_top1 - baseline_top1,
        "baselineBoundaryGapMedian": baseline_gap,
        "candidateBoundaryGapMedian": candidate_gap,
        "boundaryGapRatio": (
            candidate_gap / baseline_gap
            if baseline_gap is not None and baseline_gap > 1e-12 and candidate_gap is not None
            else None
        ),
        "baselineCutoffCrowdMedian": baseline_crowd,
        "candidateCutoffCrowdMedian": candidate_crowd,
    }


def build_retrain_gate(validation_races, model, feature_cols, walk_forward_results):
    overall_metrics = {
        "baseline": evaluate_existing(validation_races, "v4_rank"),
        "candidate": evaluate_model(model, validation_races, feature_cols),
        "comparison": compare_model_to_existing(model, validation_races, feature_cols),
    }
    h16_races = subset_by_profile_subtype(validation_races, "HANDIKAP16")
    h16_metrics = {
        "baseline": evaluate_existing(h16_races, "v4_rank"),
        "candidate": evaluate_model(model, h16_races, feature_cols),
        "comparison": compare_model_to_existing(model, h16_races, feature_cols),
    }
    overall = overall_metrics["comparison"]
    h16 = h16_metrics["comparison"]
    baseline_overall = overall_metrics["baseline"]
    candidate_overall = overall_metrics["candidate"]
    walk_forward_comparisons = [
        result.get("comparisons", {}).get("Overall", {})
        for result in walk_forward_results
        if result.get("comparisons", {}).get("Overall")
    ]

    def check(name, passed, detail):
        return {"name": name, "passed": bool(passed), "detail": detail}

    boundary_ok = (
        overall["boundaryGapRatio"] is not None and overall["boundaryGapRatio"] >= 0.90
    )
    crowd_ok = (
        overall["candidateCutoffCrowdMedian"] is not None
        and overall["baselineCutoffCrowdMedian"] is not None
        and overall["candidateCutoffCrowdMedian"] <= overall["baselineCutoffCrowdMedian"] + 1.0
    )
    h16_boundary_ok = (
        h16["boundaryGapRatio"] is not None and h16["boundaryGapRatio"] >= 0.90
    )
    checks = [
        check("overall_validation_minimum", overall["races"] >= 20, f'races={overall["races"]}/20'),
        check("overall_winner_top3_no_regression", overall["winnerTop3Net"] >= 0,
              f'net={overall["winnerTop3Net"]}'),
        check("overall_top1_loss_max_1", overall["top1Net"] >= -1, f'net={overall["top1Net"]}'),
        check("overall_ndcg_no_regression",
              candidate_overall["ndcg5"] is not None and baseline_overall["ndcg5"] is not None
              and candidate_overall["ndcg5"] >= baseline_overall["ndcg5"] - 0.005,
              f'baseline={fmt(baseline_overall["ndcg5"])},candidate={fmt(candidate_overall["ndcg5"])}'),
        check("overall_mae_no_regression",
              candidate_overall["mae"] is not None and baseline_overall["mae"] is not None
              and candidate_overall["mae"] <= baseline_overall["mae"] + 0.05,
              f'baseline={fmt(baseline_overall["mae"])},candidate={fmt(candidate_overall["mae"])}'),
        check("overall_boundary_not_compressed", boundary_ok,
              f'ratio={fmt(overall["boundaryGapRatio"])}'),
        check("overall_cutoff_crowd_not_worse", crowd_ok,
              f'baseline={fmt(overall["baselineCutoffCrowdMedian"])},candidate={fmt(overall["candidateCutoffCrowdMedian"])}'),
        check("h16_outer_minimum", h16["races"] >= 6, f'races={h16["races"]}/6'),
        check("h16_winner_top3_plus_1", h16["winnerTop3Net"] >= 1,
              f'net={h16["winnerTop3Net"]}'),
        check("h16_no_damage", h16["damages"] == 0, f'damages={h16["damages"]}'),
        check("h16_boundary_not_compressed", h16_boundary_ok,
              f'ratio={fmt(h16["boundaryGapRatio"])}'),
        check("walk_forward_no_winner_top3_regression",
              len(walk_forward_comparisons) >= 2
              and all(item.get("winnerTop3Net", -999) >= 0 for item in walk_forward_comparisons),
              f'nets={[item.get("winnerTop3Net") for item in walk_forward_comparisons]}'),
    ]
    return {
        "schemaVersion": "shadow-ml-retrain-gate-v1",
        "decision": "SHADOW_CANDIDATE" if all(item["passed"] for item in checks) else "REJECTED",
        "rankingImpact": False,
        "overallValidation": overall_metrics,
        "handikap16Validation": h16_metrics,
        "walkForwardOverall": walk_forward_comparisons,
        "checks": checks,
        "failedChecks": [item["name"] for item in checks if not item["passed"]],
        "policy": {
            "winnerTop3Primary": True,
            "h16OuterGainRequired": 1,
            "h16DamageAllowed": 0,
            "boundaryGapMinimumRatio": 0.90,
            "automaticDeployment": False,
            "mode": "shadow_only",
        },
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


def write_report(
    path,
    metadata,
    validation_races,
    model_agf,
    agf_feature_cols,
    model_no_agf,
    no_agf_feature_cols,
    walk_forward_results,
    retrain_gate,
):
    sections = []
    sections.append(f"# Shadow ML Training Report - {metadata['model_version']}\n")
    sections.append(
        f"- Train races: {metadata['train_races']}\n"
        f"- Validation races: {metadata['validation_races']}\n"
        f"- Labeled entries: {metadata['labeled_entries']}\n"
        f"- Split: chronological date-block {metadata['validation_ratio']:.0%} validation\n"
        f"- Saved variant: {metadata['model_variant']} ({metadata['feature_count']} features)\n"
        f"- Partial races excluded: {metadata['corpus_summary'].get('partial_races', 0)}\n"
        f"- Integrity-invalid races excluded: "
        f"{metadata['corpus_summary'].get('integrity_invalid_races', 0)} races / "
        f"{metadata['corpus_summary'].get('integrity_invalid_rows', 0)} rows\n"
        f"- Accepted finish patterns: {metadata['corpus_summary'].get('valid_tie_races', 0)} tied races / "
        f"{metadata['corpus_summary'].get('terminal_status_races', 0)} terminal-99 races\n"
        f"- Visible ranking impact: none\n"
    )
    header = "| Model | Races | Top1 | Winner Top3 | Winner Top5 | Rho | MAE | NDCG@5 | Avg winner rank | Avg top finish |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"

    def add_table(title, races):
        if not races:
            return
        sections.append(f"\n## {title}\n\n{header}")
        sections.append(metrics_row(detected_v4_label(races), evaluate_existing(races, "v4_rank")))
        sections.append(metrics_row("ML + AGF", evaluate_model(model_agf, races, agf_feature_cols)))
        sections.append(metrics_row("ML - AGF", evaluate_model(model_no_agf, races, no_agf_feature_cols)))
        sections.append(metrics_row("AGF only", evaluate_agf(races)))

    add_table("Validation Overall", validation_races)
    for group in ["HANDIKAP", "MAIDEN", "SARTLI", "KV", "GRUP", "SATIS"]:
        add_table(f"Validation {group}", subset_by_group(validation_races, group))
    for profile in ["HANDIKAP14|Kum", "HANDIKAP15|Kum", "HANDIKAP15|Cim", "HANDIKAP16|Kum", "HANDIKAP16|Cim"]:
        add_table(f"Validation {profile}", subset_by_handikap_profile(validation_races, profile))
    add_table("Validation HANDIKAP16 all tracks", subset_by_profile_subtype(validation_races, "HANDIKAP16"))

    if walk_forward_results:
        sections.append("\n## Walk-Forward Validation\n")
        sections.append(
            "| Fold | Train | Validation | Segment | Model | Top1 | Winner Top3 | Rho | MAE | NDCG@5 |\n"
            "|---:|---:|---:|---|---|---:|---:|---:|---:|---:|"
        )
        for result in walk_forward_results:
            for segment in ["Overall", "HANDIKAP"]:
                for model_name, metrics in result["segments"].get(segment, {}).items():
                    sections.append(
                        f"| {result['fold']} | {result['train_races']} | {result['validation_races']} | "
                        f"{segment} | {model_name} | {metrics['top1']}/{metrics['races']} | "
                        f"{metrics['winner_top3']}/{metrics['races']} | {fmt(metrics['rho'])} | "
                        f"{fmt(metrics['mae'])} | {fmt(metrics['ndcg5'])} |"
                    )

    sections.extend([
        "\n## Retrain Gate\n",
        f"- Decision: **{retrain_gate['decision']}**",
        "- Ranking impact: none (`shadow_only`)",
        f"- Failed checks: `{', '.join(retrain_gate['failedChecks']) or 'none'}`",
        "",
        "| Check | Passed | Detail |",
        "|---|---|---|",
    ])
    for item in retrain_gate["checks"]:
        sections.append(
            f"| {item['name']} | {'yes' if item['passed'] else 'no'} | {item['detail']} |"
        )

    path.write_text("\n".join(sections) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Train Atistik shadow ML ranker from predictions.jsonl export.")
    parser.add_argument("--export-url", default="https://atistik-backend.onrender.com/api/ml-export?labeled_only=true")
    parser.add_argument("--input", help="Optional local API export JSON or predictions JSONL file.")
    parser.add_argument("--output-dir", default=".")
    parser.add_argument("--validation-ratio", type=float, default=0.2)
    parser.add_argument(
        "--model-variant",
        choices=["no-agf", "agf"],
        default="no-agf",
        help="Model artifact to save. Both variants are still evaluated.",
    )
    parser.add_argument(
        "--include-partial-races",
        action="store_true",
        help="Legacy diagnostic only; default training excludes the whole partial race.",
    )
    parser.add_argument("--min-source-races", type=int, default=DEFAULT_MIN_SOURCE_RACES)
    parser.add_argument("--min-source-ratio", type=float, default=DEFAULT_MIN_SOURCE_RATIO)
    parser.add_argument("--walk-forward-folds", type=int, default=3)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    entries, corpus_summary = load_entries(args, with_summary=True)
    if len(entries) < 100:
        raise SystemExit(f"Not enough labeled entries for training: {len(entries)}")
    train_races, validation_races = split_races(entries, args.validation_ratio)
    if len(validation_races) < 10:
        raise SystemExit(f"Not enough validation races: {len(validation_races)}")

    agf_feature_cols, feature_coverage = select_feature_cols(
        train_races,
        FEATURE_COLS,
        min_source_races=args.min_source_races,
        min_source_ratio=args.min_source_ratio,
    )
    no_agf_feature_cols = without_agf_features(agf_feature_cols)
    model_agf = train_ranker(train_races, agf_feature_cols)
    model_no_agf = train_ranker(train_races, no_agf_feature_cols)
    walk_forward_results = []
    for fold_index, (fold_train, fold_validation) in enumerate(
        walk_forward_splits(entries, fold_count=args.walk_forward_folds),
        start=1,
    ):
        fold_agf_cols, _ = select_feature_cols(
            fold_train,
            FEATURE_COLS,
            min_source_races=args.min_source_races,
            min_source_ratio=args.min_source_ratio,
        )
        fold_no_agf_cols = without_agf_features(fold_agf_cols)
        fold_agf = train_ranker(fold_train, fold_agf_cols)
        fold_no_agf = train_ranker(fold_train, fold_no_agf_cols)
        segments = {}
        for segment_name, segment_races in [
            ("Overall", fold_validation),
            ("HANDIKAP", subset_by_group(fold_validation, "HANDIKAP")),
        ]:
            if not segment_races:
                continue
            segments[segment_name] = {
                detected_v4_label(segment_races): evaluate_existing(segment_races, "v4_rank"),
                "ML + AGF": evaluate_model(fold_agf, segment_races, fold_agf_cols),
                "ML - AGF": evaluate_model(fold_no_agf, segment_races, fold_no_agf_cols),
            }
        walk_forward_results.append({
            "fold": fold_index,
            "train_races": len(fold_train),
            "validation_races": len(fold_validation),
            "segments": segments,
            "comparisons": {
                "Overall": compare_model_to_existing(
                    fold_no_agf, fold_validation, fold_no_agf_cols
                ),
                "HANDIKAP16": compare_model_to_existing(
                    fold_no_agf,
                    subset_by_profile_subtype(fold_validation, "HANDIKAP16"),
                    fold_no_agf_cols,
                ),
            },
        })
    retrain_gate = build_retrain_gate(
        validation_races,
        model_no_agf,
        no_agf_feature_cols,
        walk_forward_results,
    )
    model_version = "shadow-" + datetime.now().strftime("%Y%m%d-%H%M")
    metadata = {
        "model_version": model_version,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "train_races": len(train_races),
        "validation_races": len(validation_races),
        "labeled_entries": len(entries),
        "validation_ratio": args.validation_ratio,
        "model_variant": args.model_variant,
        "includes_agf": args.model_variant == "agf",
        "feature_count": len(agf_feature_cols if args.model_variant == "agf" else no_agf_feature_cols),
        "agf_feature_count": len(agf_feature_cols),
        "no_agf_feature_count": len(no_agf_feature_cols),
        "corpus_summary": corpus_summary,
        "feature_source_gate": {
            "min_source_races": args.min_source_races,
            "min_source_ratio": args.min_source_ratio,
            "coverage": feature_coverage,
            "excluded_features": [col for col, item in feature_coverage.items() if not item["accepted"]],
        },
        "objective": "rank:ndcg",
        "walk_forward_folds": len(walk_forward_results),
        "retrain_rule": "+50 labeled races or weekly, whichever comes first",
        "activation_rule": "shadow only until 1000 overall races, 120 profile races, and 3 consecutive reports beat v4",
        "residual_signal": {
            "profile": "HANDIKAP16",
            "feature": "h16_training_degree_edge",
            "source_flag": "hasTrainingTimes",
            "evidence": "winner edge in 6/9 unrescued races; median +9.4",
        },
        "retrain_gate_decision": retrain_gate["decision"],
    }
    retrain_gate["modelVersion"] = model_version
    retrain_gate["createdAt"] = datetime.now().isoformat(timespec="seconds")

    model_path = output_dir / "model_shadow_ranker.json"
    stats_path = output_dir / "feature_stats_shadow.json"
    report_path = output_dir / f"ml_training_report_{datetime.now().strftime('%Y%m%d')}.md"
    gate_path = output_dir / f"ml_retrain_gate_{datetime.now().strftime('%Y%m%d')}.json"

    saved_model = model_agf if args.model_variant == "agf" else model_no_agf
    saved_feature_cols = agf_feature_cols if args.model_variant == "agf" else no_agf_feature_cols
    saved_model.save_model(str(model_path))
    train_entries = [row for rows in train_races.values() for row in rows]
    stats_payload = {
        "feature_cols": saved_feature_cols,
        "stats": feature_stats(train_entries, saved_feature_cols),
        "metadata": metadata,
    }
    stats_path.write_text(json.dumps(stats_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    gate_path.write_text(json.dumps(retrain_gate, ensure_ascii=False, indent=2), encoding="utf-8")
    write_report(
        report_path,
        metadata,
        validation_races,
        model_agf,
        agf_feature_cols,
        model_no_agf,
        no_agf_feature_cols,
        walk_forward_results,
        retrain_gate,
    )

    print(json.dumps({
        "model": str(model_path),
        "stats": str(stats_path),
        "report": str(report_path),
        "gate": str(gate_path),
        "gate_decision": retrain_gate["decision"],
        "metadata": metadata,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
