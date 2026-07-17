"""Recover missing prediction rows from persisted daily analysis manifests.

The daily analysis manifest intentionally keeps only the visible ranking
summary, not the raw feature payload.  Recovered rows are therefore marked and
carry an empty ``features`` mapping so retraining can exclude them while rank
evaluation and official result labeling remain possible.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from result_submission import clean_result_name


def _profile(race_type: Any) -> str:
    text = str(race_type or "").upper().replace("İ", "I").replace("Ş", "S")
    if "MAIDEN" in text:
        return "MAIDEN"
    if "HANDIKAP" in text:
        return "HANDIKAP"
    if "SARTLI 1" in text or "SARTLI-1" in text:
        return "SART1"
    if "SARTLI" in text:
        return "SARTLI"
    if any(token in text for token in ("G 1", "G 2", "G 3", "KV-", "KV ")):
        return "ELITE"
    return "OTHER"


def _manifest_ts(manifest: dict[str, Any], path: Path) -> int:
    for field in ("finishedAt", "startedAt"):
        value = str(manifest.get(field) or "").strip()
        if not value:
            continue
        try:
            return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
        except ValueError:
            pass
    return int(path.stat().st_mtime)


def load_entries(path: Path) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, 1):
            if not line.strip():
                continue
            item = json.loads(line)
            if not isinstance(item, dict):
                raise ValueError(f"{path}:{line_no} JSON object degil")
            entries.append(item)
    return entries


def manifest_rows(path: Path) -> Iterable[dict[str, Any]]:
    manifest = json.loads(path.read_text(encoding="utf-8-sig"))
    race_date = str(manifest.get("raceDate") or "").strip()
    ts = _manifest_ts(manifest, path)
    if not race_date:
        raise ValueError(f"raceDate eksik: {path}")

    for city in manifest.get("cities", []) or []:
        for race in city.get("races", []) or []:
            if race.get("status") != "analyzed":
                continue
            race_id = str(race.get("raceId") or "").strip()
            race_no = str(race.get("raceNo") or "").strip()
            rankings = race.get("rankings", []) or []
            if not race_id or not rankings:
                continue
            seen_names: set[str] = set()
            seen_ranks: set[int] = set()
            for ranking in rankings:
                horse_name = str(ranking.get("horse") or "").strip()
                name_key = clean_result_name(horse_name)
                rank = int(ranking.get("rank") or 0)
                if not name_key or rank <= 0:
                    raise ValueError(f"gecersiz ranking: {path} race_id={race_id}")
                if name_key in seen_names or rank in seen_ranks:
                    raise ValueError(f"tekrarli ranking: {path} race_id={race_id}")
                seen_names.add(name_key)
                seen_ranks.add(rank)
                v4_score = ranking.get("v4Score", ranking.get("aiScore"))
                v4_rank = int(ranking.get("v4Rank") or rank)
                yield {
                    "ts": ts,
                    "race_id": race_id,
                    "race_date": race_date,
                    "race_no": race_no,
                    "race_type": race.get("raceType"),
                    "field_size": int(race.get("horseCount") or len(rankings)),
                    "distance": race.get("distance"),
                    "track": race.get("track"),
                    "horse_name": horse_name,
                    "ai_score": ranking.get("aiScore"),
                    "rank_pred": rank,
                    "finish_pos": None,
                    "is_winner": None,
                    "v4_score": v4_score,
                    "v4_rank": v4_rank,
                    "v4_profile": _profile(race.get("raceType")),
                    "v4_confidence": None,
                    "v4_decision_mode": "default_visible",
                    "v4_mode": "default_visible",
                    "v4_use_for_ranking": True,
                    "v4_weights": {},
                    "features": {},
                    "recovered_from": "automation_analysis_manifest",
                    "recovery_limitations": "rankings_only_no_features",
                }


def recover_entries(
    entries: list[dict[str, Any]], manifest_paths: Iterable[Path]
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    race_identity: dict[str, tuple[str, str]] = {}
    for entry in entries:
        race_id = str(entry.get("race_id") or "").strip()
        name_key = clean_result_name(entry.get("horse_name"))
        if race_id and name_key:
            by_key[(race_id, name_key)] = entry
        if race_id:
            identity = (
                str(entry.get("race_date") or "").strip(),
                str(entry.get("race_no") or "").strip(),
            )
            previous = race_identity.setdefault(race_id, identity)
            if previous != identity:
                raise ValueError(f"mevcut race_id kimligi celiskili: {race_id}")

    recovered: list[dict[str, Any]] = []
    skipped = 0
    manifests = 0
    races: set[str] = set()
    for path in sorted(manifest_paths):
        manifests += 1
        for row in manifest_rows(path):
            race_id = row["race_id"]
            identity = (row["race_date"], row["race_no"])
            previous = race_identity.setdefault(race_id, identity)
            if previous != identity:
                raise ValueError(
                    f"manifest race_id kimligi celisiyor: {race_id} {previous} != {identity}"
                )
            key = (race_id, clean_result_name(row["horse_name"]))
            if key in by_key:
                skipped += 1
                continue
            by_key[key] = row
            recovered.append(row)
            races.add(race_id)

    return entries + recovered, {
        "manifests": manifests,
        "recovered_rows": len(recovered),
        "recovered_races": len(races),
        "existing_rows_skipped": skipped,
        "final_rows": len(entries) + len(recovered),
    }


def write_atomic(path: Path, entries: list[dict[str, Any]]) -> None:
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            for entry in entries:
                handle.write(json.dumps(entry, ensure_ascii=False) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--predictions", type=Path, required=True)
    parser.add_argument("--runs-dir", type=Path, required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    start = datetime.fromisoformat(args.start_date).date()
    end = datetime.fromisoformat(args.end_date).date()
    if end < start:
        parser.error("end-date start-date'den once olamaz")
    manifests = []
    for path in sorted(args.runs_dir.glob("*/analysis.json")):
        try:
            day = datetime.fromisoformat(path.parent.name).date()
        except ValueError:
            continue
        if start <= day <= end:
            manifests.append(path)

    entries = load_entries(args.predictions)
    output, summary = recover_entries(entries, manifests)
    summary["applied"] = bool(args.apply)
    if args.apply and summary["recovered_rows"]:
        write_atomic(args.predictions, output)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
