#!/usr/bin/env python3
"""Immutably merge the daytime late-market ledger into the ML-data repo."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

try:
    from automation.late_market_agf_shadow import (
        VERSION,
        IntegrityError,
        atomic_write,
        canonical,
        clean_id,
        load_jsonl,
        sha256_payload,
    )
except ModuleNotFoundError as exc:
    if exc.name != "automation":
        raise
    from late_market_agf_shadow import (  # type: ignore[no-redef]
        VERSION,
        IntegrityError,
        atomic_write,
        canonical,
        clean_id,
        load_jsonl,
        sha256_payload,
    )


def valid_hash(item: dict) -> bool:
    expected = clean_id(item.get("snapshotSha256"))
    payload = dict(item)
    payload.pop("snapshotSha256", None)
    return len(expected) == 64 and expected == sha256_payload(payload)


def merge(state_path: Path, destination: Path) -> dict[str, int]:
    state = load_jsonl(state_path)
    existing = load_jsonl(destination) if destination.exists() else []
    merged: dict[str, dict] = {}
    for source, rows in (("destination", existing), ("state", state)):
        for item in rows:
            key = clean_id(item.get("snapshotKey"))
            if (
                not key
                or item.get("version") != VERSION
                or not valid_hash(item)
                or bool(item.get("usedForRanking"))
                or bool(item.get("rolloutEligible"))
                or bool(item.get("telegramVisible"))
            ):
                raise IntegrityError(f"{source}_snapshot_invalid:{key or 'missing-key'}")
            previous = merged.get(key)
            if previous is not None and canonical(previous) != canonical(item):
                raise IntegrityError(f"immutable_snapshot_conflict:{key}")
            merged[key] = item
    ordered = sorted(
        merged.values(),
        key=lambda item: (
            safe_number((item.get("identity") or {}).get("raceStartTs")),
            safe_number((item.get("identity") or {}).get("raceNo")),
            clean_id((item.get("identity") or {}).get("raceId")),
        ),
    )
    atomic_write(destination, "".join(canonical(item) + "\n" for item in ordered))
    return {"state": len(state), "before": len(existing), "after": len(ordered)}


def safe_number(value: object) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 999


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args()
    stats = merge(args.state, args.destination)
    print(json.dumps({"success": True, **stats}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
