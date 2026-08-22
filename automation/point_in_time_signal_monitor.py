#!/usr/bin/env python3
"""Coverage monitor for non-ranking point-in-time race telemetry.

The monitor measures collection integrity and the visible v4 Winner Top3
baseline only.  It deliberately reports candidate impact as unavailable until
a future, fully-labeled and chronologically evaluated candidate ranking exists.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any
from zoneinfo import ZoneInfo

try:
    from automation.future_signal_ledger import (
        point_in_time_snapshot_sha256,
        runner_identity_key,
        verify_point_in_time_snapshot,
    )
    from automation.metric_signal_registry import (
        classify_race,
        competitive_race_rows,
        fold_text,
        group_races,
        load_jsonl,
        profile_of,
        safe_int,
        track_bucket,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from future_signal_ledger import (
        point_in_time_snapshot_sha256,
        runner_identity_key,
        verify_point_in_time_snapshot,
    )
    from metric_signal_registry import (
        classify_race,
        competitive_race_rows,
        fold_text,
        group_races,
        load_jsonl,
        profile_of,
        safe_int,
        track_bucket,
    )


SCHEMA_VERSION = "point-in-time-signal-monitor-v1"
SCOPE_ORDER = ("ALL", "MAIDEN", "SART1", "SATIS", "BIG_FIELD")
ISTANBUL = ZoneInfo("Europe/Istanbul")


def _rate(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 4) if denominator else None


def _point_in_time(row: dict[str, Any]) -> dict[str, Any]:
    ledger = row.get("future_signal_ledger") or {}
    point = ledger.get("pointInTime") if isinstance(ledger, dict) else None
    return point if isinstance(point, dict) else {}


def _scope_names(rows: list[dict[str, Any]]) -> list[str]:
    profile = profile_of(rows)
    subtype = profile["subtype"].replace("SARTLI", "SART")
    names = ["ALL"]
    if profile["category"] == "MAIDEN":
        names.append("MAIDEN")
    if subtype == "SART1":
        names.append("SART1")
    if profile["category"] == "SATIS":
        names.append("SATIS")
    if len(rows) >= 12:
        names.append("BIG_FIELD")
    return names


def _strict_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return int(number) if number.is_integer() else None


def _distance_m(value: Any) -> int | None:
    match = re.search(r"(?<!\d)(\d{3,4})(?!\d)", str(value or ""))
    return int(match.group(1)) if match else None


def _derived_race_start_ts(race_date: Any, race_time: Any) -> int | None:
    parsed_date = None
    for pattern in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            parsed_date = datetime.strptime(str(race_date or "").strip(), pattern)
            break
        except ValueError:
            continue
    time_text = str(race_time or "").strip().replace(".", ":")
    if parsed_date is None or not re.fullmatch(r"\d{1,2}:\d{2}", time_text):
        return None
    try:
        hour, minute = (int(part) for part in time_text.split(":", 1))
        return int(
            parsed_date.replace(hour=hour, minute=minute, tzinfo=ISTANBUL).timestamp()
        )
    except ValueError:
        return None


def _all_rows_match(rows: list[dict[str, Any]], field: str, expected: Any) -> bool:
    return bool(rows) and all(
        str(row.get(field) or "").strip() == str(expected or "").strip()
        for row in rows
    )


def _point_wrapper_valid(point: dict[str, Any]) -> bool:
    return bool(
        point.get("schemaVersion") == "point-in-time-race-signal-v1"
        and point.get("mode") == "telemetry_only"
        and point.get("usedForRanking") is False
        and point.get("sentToTelegram") is False
        and point.get("rolloutEligible") is False
    )


def _official_label_contract(rows: list[dict[str, Any]]) -> bool:
    for row in rows:
        if str(row.get("result_source") or "") != "tjk_official_results":
            return False
        position = safe_int(row.get("finish_pos"), 0)
        status = str(row.get("result_status") or "")
        terminal_reason = str(row.get("terminal_reason") or "").strip()
        if position == 99:
            if status not in {"non_runner", "unranked_terminal"} or not terminal_reason:
                return False
        elif position > 0:
            if status != "finished" or terminal_reason:
                return False
        else:
            return False
    return bool(rows)


def _snapshot_contract(rows: list[dict[str, Any]], point: dict[str, Any]) -> dict[str, Any]:
    snapshot = point.get("preRaceSnapshot")
    if not isinstance(snapshot, dict):
        snapshot = {}
    identity = snapshot.get("identity")
    if not isinstance(identity, dict):
        identity = {}
    field = snapshot.get("field")
    if not isinstance(field, dict):
        field = {}
    pace = snapshot.get("pace")
    if not isinstance(pace, dict):
        pace = {}
    track_variant = snapshot.get("trackVariant")
    if not isinstance(track_variant, dict):
        track_variant = {}

    required_identity = (
        "raceId",
        "raceDate",
        "raceNo",
        "raceTime",
        "raceStartTs",
        "city",
        "cityId",
        "distanceM",
        "distanceBandM",
        "surface",
        "profile",
    )
    identity_present = all(identity.get(key) not in (None, "") for key in required_identity)
    expected_start = _derived_race_start_ts(identity.get("raceDate"), identity.get("raceTime"))
    identity_rows_match = bool(
        identity_present
        and _all_rows_match(rows, "race_id", identity.get("raceId"))
        and _all_rows_match(rows, "race_date", identity.get("raceDate"))
        and _all_rows_match(rows, "race_no", identity.get("raceNo"))
        and _all_rows_match(rows, "race_time", identity.get("raceTime"))
        and _all_rows_match(rows, "city", identity.get("city"))
        and _all_rows_match(rows, "city_id", identity.get("cityId"))
        and all(_distance_m(row.get("distance")) == _strict_int(identity.get("distanceM")) for row in rows)
        and all(track_bucket(row.get("track")) == track_bucket(identity.get("surface")) for row in rows)
        and all(fold_text(row.get("race_type")) == fold_text(identity.get("profile")) for row in rows)
        and expected_start is not None
        and expected_start == _strict_int(identity.get("raceStartTs"))
    )

    captured = _strict_int(snapshot.get("capturedTs"))
    start = _strict_int(identity.get("raceStartTs"))
    timing_valid = bool(
        snapshot.get("timingState") == "VALID_PRE_RACE"
        and captured is not None
        and start is not None
        and captured < start
    )
    declared = _strict_int(field.get("declaredRunnerCount"))
    threshold = _strict_int(field.get("largeFieldThreshold"))
    runner_snapshot_rows = field.get("runners")
    if not isinstance(runner_snapshot_rows, list):
        runner_snapshot_rows = []
    expected_runners: dict[str, dict[str, Any]] = {}
    runner_contract_valid = True
    for runner in runner_snapshot_rows:
        if not isinstance(runner, dict):
            runner_contract_valid = False
            continue
        key = str(runner.get("horseKey") or "")
        if not key or key != runner_identity_key(runner.get("horseName")) or key in expected_runners:
            runner_contract_valid = False
            continue
        if not re.fullmatch(r"[0-9a-f]{64}", str(runner.get("signalSha256") or "")):
            runner_contract_valid = False
        expected_runners[key] = runner

    actual_runners: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = runner_identity_key(row.get("horse_name"))
        if not key or key in actual_runners:
            runner_contract_valid = False
            continue
        actual_runners[key] = row
    if set(actual_runners) != set(expected_runners):
        runner_contract_valid = False
    for key in set(actual_runners) & set(expected_runners):
        row = actual_runners[key]
        expected = expected_runners[key]
        expected_no = str(expected.get("horseNo") or "")
        actual_no = str(row.get("horse_no") or "")
        if expected_no and expected_no != actual_no:
            runner_contract_valid = False
        ledger = row.get("future_signal_ledger")
        if not isinstance(ledger, dict):
            runner_contract_valid = False
            continue
        actual_signal = {
            "telemetry": ledger.get("telemetry") or {},
            "fieldDiagnosticScores": ledger.get("fieldDiagnosticScores") or {},
        }
        if point_in_time_snapshot_sha256(actual_signal) != expected.get("signalSha256"):
            runner_contract_valid = False

    field_valid = bool(
        declared == len(rows)
        and declared
        and threshold is not None
        and threshold > 0
        and field.get("isLargeField") is (declared >= threshold)
        and len(runner_snapshot_rows) == declared
        and runner_contract_valid
    )
    pace_source_count = _strict_int(pace.get("sourceProvenRunnerCount")) or 0
    pace_available = bool(
        pace.get("state") == "AVAILABLE"
        and _strict_int(pace.get("validStyleRunnerCount")) == len(rows)
        and 0 <= pace_source_count <= len(rows)
    )
    return {
        "wrapperValid": _point_wrapper_valid(point),
        "identityComplete": identity_rows_match,
        "snapshotTimingValid": timing_valid,
        "fieldSizeAvailable": field_valid,
        "paceAvailable": pace_available,
        "paceSourceProvenRunnerCount": pace_source_count if pace_available else 0,
        "trackConditionAvailable": identity.get("trackCondition") not in (None, ""),
        "priorTrackVariantAvailable": bool(
            track_variant.get("state") == "AVAILABLE_PRIOR_ONLY"
            and track_variant.get("usedForTelemetry") is True
            and track_variant.get("usedForRanking") is False
        ),
    }


def _race_observation(rows: list[dict[str, Any]]) -> dict[str, Any]:
    points = [_point_in_time(row) for row in rows]
    hashes = {
        str(point.get("preRaceSnapshotSha256") or "")
        for point in points
        if point
    }
    hash_valid = bool(
        len(points) == len(rows)
        and all(points)
        and len(hashes) == 1
        and all(verify_point_in_time_snapshot(point) for point in points)
    )
    first = points[0] if points else {}
    contract = _snapshot_contract(rows, first)
    immutable_valid = bool(
        hash_valid
        and all(_point_wrapper_valid(point) for point in points)
        and contract["wrapperValid"]
        and contract["identityComplete"]
        and contract["snapshotTimingValid"]
        and contract["fieldSizeAvailable"]
    )
    label_state = classify_race(rows)
    official_labels_valid = _official_label_contract(rows)
    evidence_eligible = bool(
        immutable_valid
        and label_state == "fully_labeled"
        and official_labels_valid
    )
    winner_top3: bool | None = None
    if evidence_eligible:
        competitive = competitive_race_rows(rows)
        winner = next(
            (row for row in competitive if safe_int(row.get("finish_pos"), 0) == 1),
            None,
        )
        if winner is not None:
            winner_top3 = safe_int(winner.get("rank_pred"), 999) <= 3
    return {
        "runnerCount": len(rows),
        "immutableValid": immutable_valid,
        "identityComplete": contract["identityComplete"],
        "snapshotTimingValid": contract["snapshotTimingValid"],
        "fieldSizeAvailable": contract["fieldSizeAvailable"],
        "paceAvailable": contract["paceAvailable"],
        "paceSourceProvenRunnerCount": contract["paceSourceProvenRunnerCount"],
        "trackConditionAvailable": contract["trackConditionAvailable"],
        "priorTrackVariantAvailable": contract["priorTrackVariantAvailable"],
        "labelState": label_state,
        "officialLabelsValid": official_labels_valid,
        "evidenceEligible": evidence_eligible,
        "winnerTop3": winner_top3,
    }


def _empty_scope() -> dict[str, int]:
    return {
        "races": 0,
        "runners": 0,
        "immutableValidRaces": 0,
        "identityCompleteRaces": 0,
        "preRaceTimingValidRaces": 0,
        "fieldSizeAvailableRaces": 0,
        "paceAvailableRaces": 0,
        "paceSourceProvenRunners": 0,
        "trackConditionAvailableRaces": 0,
        "priorTrackVariantAvailableRaces": 0,
        "fullyLabeledRaces": 0,
        "officialFullyLabeledRaces": 0,
        "evidenceFullyLabeledRaces": 0,
        "partialRaces": 0,
        "unlabeledRaces": 0,
        "integrityInvalidRaces": 0,
        "baselineWinnerTop3Hits": 0,
    }


def _finalize_scope(name: str, counts: dict[str, int]) -> dict[str, Any]:
    races = counts["races"]
    labeled = counts["evidenceFullyLabeledRaces"]
    runners = counts["runners"]
    return {
        "scope": name,
        **counts,
        "coverage": {
            "immutableValidRaceRate": _rate(counts["immutableValidRaces"], races),
            "identityCompleteRaceRate": _rate(counts["identityCompleteRaces"], races),
            "preRaceTimingValidRaceRate": _rate(counts["preRaceTimingValidRaces"], races),
            "fieldSizeAvailableRaceRate": _rate(counts["fieldSizeAvailableRaces"], races),
            "paceAvailableRaceRate": _rate(counts["paceAvailableRaces"], races),
            "paceSourceProvenRunnerRate": _rate(counts["paceSourceProvenRunners"], runners),
            "trackConditionAvailableRaceRate": _rate(
                counts["trackConditionAvailableRaces"], races
            ),
            "priorTrackVariantAvailableRaceRate": _rate(
                counts["priorTrackVariantAvailableRaces"], races
            ),
        },
        "winnerTop3": {
            "baselineEvaluableRaces": labeled,
            "baselineHits": counts["baselineWinnerTop3Hits"],
            "baselineHitRate": _rate(counts["baselineWinnerTop3Hits"], labeled),
            "candidateEvaluableRaces": 0,
            "candidateDeltaHits": None,
            "state": "WAITING_FOR_FUTURE_FULL_LABELS_AND_CANDIDATE_RANKING",
        },
    }


def build_report(
    entries: list[dict[str, Any]],
    *,
    invalid_json_lines: int = 0,
    generated_at: str | None = None,
    run_date: str | None = None,
) -> dict[str, Any]:
    counts = defaultdict(_empty_scope)
    observed_races = 0
    for rows in group_races(entries):
        if not rows or not any(_point_in_time(row) for row in rows):
            continue
        observed_races += 1
        observation = _race_observation(rows)
        for scope in _scope_names(rows):
            bucket = counts[scope]
            bucket["races"] += 1
            bucket["runners"] += observation["runnerCount"]
            bucket["immutableValidRaces"] += int(observation["immutableValid"])
            bucket["identityCompleteRaces"] += int(observation["identityComplete"])
            bucket["preRaceTimingValidRaces"] += int(observation["snapshotTimingValid"])
            bucket["fieldSizeAvailableRaces"] += int(observation["fieldSizeAvailable"])
            bucket["paceAvailableRaces"] += int(observation["paceAvailable"])
            bucket["paceSourceProvenRunners"] += observation["paceSourceProvenRunnerCount"]
            bucket["trackConditionAvailableRaces"] += int(
                observation["trackConditionAvailable"]
            )
            bucket["priorTrackVariantAvailableRaces"] += int(
                observation["priorTrackVariantAvailable"]
            )
            label_key = {
                "fully_labeled": "fullyLabeledRaces",
                "partial": "partialRaces",
                "unlabeled": "unlabeledRaces",
                "integrity_invalid": "integrityInvalidRaces",
            }.get(observation["labelState"], "integrityInvalidRaces")
            bucket[label_key] += 1
            bucket["officialFullyLabeledRaces"] += int(
                observation["labelState"] == "fully_labeled"
                and observation["officialLabelsValid"]
            )
            bucket["evidenceFullyLabeledRaces"] += int(observation["evidenceEligible"])
            if observation["winnerTop3"] is True:
                bucket["baselineWinnerTop3Hits"] += 1

    generated = generated_at or datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )
    return {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": generated,
        "runDate": run_date,
        "mode": "telemetry_only",
        "usedForRanking": False,
        "sentToTelegram": False,
        "inventory": {
            "predictionRows": len(entries),
            "invalidJsonLines": invalid_json_lines,
            "pointInTimeObservedRaces": observed_races,
        },
        "scopes": [
            _finalize_scope(scope, counts[scope])
            for scope in SCOPE_ORDER
        ],
        "policy": {
            "trackVariantCutoff": "source race start strictly earlier; source result known by immutable pre-race capture",
            "ownRaceResultForbidden": True,
            "winnerTop3Impact": "future fully-labeled chronological races only",
            "candidateImpactAvailable": False,
        },
    }


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    temporary.replace(path)


def persist(report: dict[str, Any], data_dir: Path, run_date: str) -> dict[str, str]:
    rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    paths = {
        "dailyJson": (
            data_dir
            / "automation"
            / "runs"
            / run_date
            / "point-in-time-signal-coverage.json"
        ),
        "latestJson": data_dir / "automation" / "point-in-time-signals" / "latest.json",
    }
    for path in paths.values():
        _atomic_write(path, rendered)
    return {key: str(path) for key, path in paths.items()}


def main() -> int:
    parser = argparse.ArgumentParser(description="Monitor point-in-time signal coverage.")
    parser.add_argument("--predictions", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--run-date", required=True)
    args = parser.parse_args()
    entries, invalid = load_jsonl(args.predictions)
    report = build_report(entries, invalid_json_lines=invalid, run_date=args.run_date)
    paths = persist(report, args.data_dir, args.run_date)
    print(json.dumps({
        "success": True,
        "runDate": args.run_date,
        "inventory": report["inventory"],
        "paths": paths,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
