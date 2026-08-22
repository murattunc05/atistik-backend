"""Pure helpers for safely reconciling submitted race results.

The API layer owns file I/O and backup.  This module only resolves the target
race and stages label changes, which keeps the dangerous fallback rules
testable without importing the full Flask application.
"""

from __future__ import annotations

import math
import re
from typing import Any


RESULT_METADATA_FIELDS = (
    "result_status",
    "terminal_reason",
    "result_source",
    "official_degree_raw",
    "official_degree_seconds",
    "official_degree_source",
)
_BASE_RESULT_METADATA_FIELDS = ("result_status", "terminal_reason", "result_source")
_OFFICIAL_DEGREE_FIELDS = (
    "official_degree_raw",
    "official_degree_seconds",
    "official_degree_source",
)


def parse_official_degree_seconds(raw_degree: Any) -> float | None:
    """Parse only TJK's official ``ss.cc`` / ``m.ss.cc`` result formats."""
    text = str(raw_degree or "").strip()
    if not re.fullmatch(r"(?:\d{1,2}\.)?\d{1,2}\.\d{2}", text):
        return None
    parts = text.split(".")
    try:
        if len(parts) == 3:
            minutes, seconds, centiseconds = (int(part) for part in parts)
        else:
            minutes = 0
            seconds, centiseconds = (int(part) for part in parts)
    except (TypeError, ValueError):
        return None
    if seconds >= 60 or centiseconds >= 100:
        return None
    total = minutes * 60 + seconds + centiseconds / 100.0
    return round(total, 2) if 20.0 <= total <= 600.0 else None


def _validated_official_degree_metadata(item: dict[str, Any]) -> dict[str, Any]:
    """Return an all-or-nothing authoritative official-degree triad."""
    supplied = [item.get(field) not in (None, "") for field in _OFFICIAL_DEGREE_FIELDS]
    if not any(supplied):
        return {}
    if not all(supplied):
        return {}
    if (
        item.get("result_status") != "finished"
        or item.get("result_source") != "tjk_official_results"
        or item.get("official_degree_source") != "tjk_official_results"
        or item.get("terminal_reason") not in (None, "")
    ):
        return {}
    recomputed = parse_official_degree_seconds(item.get("official_degree_raw"))
    submitted = _safe_finite_float(item.get("official_degree_seconds"))
    if recomputed is None or submitted is None or abs(recomputed - submitted) > 0.01:
        return {}
    return {
        "official_degree_raw": str(item.get("official_degree_raw")).strip(),
        "official_degree_seconds": recomputed,
        "official_degree_source": "tjk_official_results",
    }


def _safe_finite_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def clean_result_name(value: Any) -> str:
    """Return the compact name key shared by result reconciliation."""
    text = str(value or "").split("\n")[0].strip().upper()
    text = re.sub(r"\s*\(\s*\d+\s*\)\s*$", "", text)
    return re.sub(r"[\W_]+", "", text, flags=re.UNICODE)


def _same_finish_position(left: Any, right: Any) -> bool:
    try:
        return int(left) == int(right)
    except (TypeError, ValueError):
        return str(left).strip() == str(right).strip()


def _positive_finish_position(value: Any) -> bool:
    """Accept only real, positive result labels from a submission."""
    if isinstance(value, bool):
        return False
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(numeric) and numeric > 0 and numeric.is_integer()


def _missing_finish_position(value: Any) -> bool:
    """Treat legacy zero/negative sentinels as unlabeled, like ``None``."""
    if value is None:
        return True
    if isinstance(value, bool):
        return value is False
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(numeric) and numeric <= 0


def _winner_flag(position: Any) -> int:
    try:
        return 1 if int(position) == 1 else 0
    except (TypeError, ValueError):
        return 0


def _race_groups(entries: list[Any], indices: list[int]) -> dict[str, dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for index in indices:
        entry = entries[index]
        if not isinstance(entry, dict):
            continue
        race_id = str(entry.get("race_id", "")).strip()
        if not race_id:
            continue
        group = groups.setdefault(race_id, {"indices": [], "names": set()})
        group["indices"].append(index)
        name = clean_result_name(entry.get("horse_name", ""))
        if name:
            group["names"].add(name)
    return groups


def _unique_best_group(
    groups: dict[str, dict[str, Any]],
    incoming_names: set[str],
    *,
    min_overlap: int = 1,
) -> tuple[str | None, list[int]]:
    scored: list[tuple[int, str, list[int]]] = []
    for race_id, group in groups.items():
        overlap = len(incoming_names & set(group["names"]))
        if overlap >= min_overlap:
            scored.append((overlap, race_id, list(group["indices"])))
    if not scored:
        return None, []
    scored.sort(key=lambda item: item[0], reverse=True)
    if len(scored) > 1 and scored[0][0] == scored[1][0]:
        return None, []
    return scored[0][1], scored[0][2]


def reconcile_result_submission(
    entries: list[Any],
    *,
    race_id: str,
    race_date: str,
    race_no: str,
    results: list[dict[str, Any]],
    allow_legacy_fallback: bool = False,
) -> dict[str, Any]:
    """Resolve one race and stage non-destructive label changes.

    Resolution order is numeric race id, then a unique date/race-number horse
    overlap, then a unique legacy group.  The legacy fallback is available only
    when the request supplies a date and only considers records whose own
    ``race_date`` is genuinely absent.  That last recovery path is disabled by
    default and must be explicitly enabled by an offline/manual repair tool.

    Existing identical labels are idempotent successes.  Existing different
    labels are conflicts and make the whole submission a no-op.
    """
    race_id = str(race_id or "").strip()
    race_date = str(race_date or "").strip()
    race_no = str(race_no or "").strip()
    incoming: dict[str, dict[str, Any]] = {}
    for item in results:
        if not isinstance(item, dict):
            continue
        name_key = clean_result_name(item.get("horse_name", ""))
        if name_key and _positive_finish_position(item.get("finish_pos")):
            metadata = {
                field: item.get(field)
                for field in _BASE_RESULT_METADATA_FIELDS
                if item.get(field) not in (None, "")
            }
            metadata.update(_validated_official_degree_metadata(item))
            incoming[name_key] = {
                "finish_pos": item.get("finish_pos"),
                "metadata": metadata,
            }
    incoming_names = set(incoming)

    race_id_indices = [
        index
        for index, entry in enumerate(entries)
        if isinstance(entry, dict) and str(entry.get("race_id", "")).strip() == race_id
    ]
    race_id_hits = len(race_id_indices)
    resolved_race_id: str | None = race_id if race_id_indices else None
    target_indices = race_id_indices
    resolution = "race_id" if target_indices else "none"

    if not target_indices and race_date:
        dated_indices = []
        for index, entry in enumerate(entries):
            if not isinstance(entry, dict):
                continue
            entry_date = str(entry.get("race_date", "")).strip()
            entry_no = str(entry.get("race_no", "")).strip()
            if entry_date != race_date:
                continue
            if race_no and entry_no and entry_no != race_no:
                continue
            dated_indices.append(index)
        resolved_race_id, target_indices = _unique_best_group(
            _race_groups(entries, dated_indices), incoming_names
        )
        if target_indices:
            resolution = "race_date"

    if not target_indices and allow_legacy_fallback and race_date and incoming_names:
        legacy_indices = [
            index
            for index, entry in enumerate(entries)
            if isinstance(entry, dict) and not str(entry.get("race_date", "")).strip()
        ]
        min_overlap = max(1, math.ceil(len(incoming_names) * 0.5))
        resolved_race_id, target_indices = _unique_best_group(
            _race_groups(entries, legacy_indices),
            incoming_names,
            min_overlap=min_overlap,
        )
        if target_indices:
            resolution = "legacy_missing_date"

    matched_names: set[str] = set()
    idempotent_names: set[str] = set()
    staged: list[tuple[int, Any, dict[str, Any]]] = []
    conflicts: list[dict[str, Any]] = []
    for index in target_indices:
        entry = entries[index]
        if not isinstance(entry, dict):
            continue
        name_key = clean_result_name(entry.get("horse_name", ""))
        if name_key not in incoming:
            continue
        matched_names.add(name_key)
        incoming_position = incoming[name_key]["finish_pos"]
        incoming_metadata = incoming[name_key]["metadata"]
        existing_position = entry.get("finish_pos")
        if _missing_finish_position(existing_position):
            staged.append((index, incoming_position, incoming_metadata))
        elif _same_finish_position(existing_position, incoming_position):
            metadata_conflicts = []
            metadata_backfill = {}
            for field, incoming_value in incoming_metadata.items():
                existing_value = entry.get(field)
                if existing_value in (None, ""):
                    metadata_backfill[field] = incoming_value
                elif (
                    field == "result_source"
                    and existing_value == "horse_history_fallback"
                    and incoming_value == "tjk_official_results"
                ):
                    metadata_backfill[field] = incoming_value
                elif existing_value != incoming_value:
                    metadata_conflicts.append({
                        "field": field,
                        "existing": existing_value,
                        "incoming": incoming_value,
                    })
            if metadata_conflicts:
                conflicts.append(
                    {
                        "race_id": str(entry.get("race_id", "")),
                        "horse_name": entry.get("horse_name", ""),
                        "existing_finish_pos": existing_position,
                        "incoming_finish_pos": incoming_position,
                        "conflict_type": "result_metadata",
                        "metadata_conflicts": metadata_conflicts,
                    }
                )
            elif metadata_backfill:
                staged.append((index, incoming_position, metadata_backfill))
            else:
                idempotent_names.add(name_key)
        else:
            conflicts.append(
                {
                    "race_id": str(entry.get("race_id", "")),
                    "horse_name": entry.get("horse_name", ""),
                    "existing_finish_pos": existing_position,
                    "incoming_finish_pos": incoming_position,
                }
            )

    output_entries = entries
    updated = 0
    if not conflicts and staged:
        output_entries = list(entries)
        for index, position, metadata in staged:
            updated_entry = dict(output_entries[index])
            updated_entry["finish_pos"] = position
            updated_entry["is_winner"] = _winner_flag(position)
            updated_entry.update(metadata)
            output_entries[index] = updated_entry
            updated += 1

    return {
        "entries": output_entries,
        "race_id_hits": race_id_hits,
        "resolved_race_id": resolved_race_id,
        "resolution": resolution,
        "legacy_fallback_used": resolution == "legacy_missing_date",
        "incoming": len(incoming_names),
        "matched": len(matched_names),
        "updated": updated,
        "would_update": len(staged),
        "idempotent": len(idempotent_names),
        "conflicts": conflicts,
    }
