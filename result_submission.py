"""Pure helpers for safely reconciling submitted race results.

The API layer owns file I/O and backup.  This module only resolves the target
race and stages label changes, which keeps the dangerous fallback rules
testable without importing the full Flask application.
"""

from __future__ import annotations

import math
import re
from typing import Any


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
    incoming: dict[str, Any] = {}
    for item in results:
        if not isinstance(item, dict):
            continue
        name_key = clean_result_name(item.get("horse_name", ""))
        if name_key and item.get("finish_pos") is not None:
            incoming[name_key] = item.get("finish_pos")
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
    staged: list[tuple[int, Any]] = []
    conflicts: list[dict[str, Any]] = []
    for index in target_indices:
        entry = entries[index]
        if not isinstance(entry, dict):
            continue
        name_key = clean_result_name(entry.get("horse_name", ""))
        if name_key not in incoming:
            continue
        matched_names.add(name_key)
        incoming_position = incoming[name_key]
        existing_position = entry.get("finish_pos")
        if existing_position is None:
            staged.append((index, incoming_position))
        elif _same_finish_position(existing_position, incoming_position):
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
        for index, position in staged:
            updated_entry = dict(output_entries[index])
            updated_entry["finish_pos"] = position
            updated_entry["is_winner"] = _winner_flag(position)
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
