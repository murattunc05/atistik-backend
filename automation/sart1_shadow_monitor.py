"""Prospective-only checkpoint report for the frozen SART1 Top3 candidate."""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


CHECKPOINT_RACES = 5
REVIEW_RACES = 15
PRODUCTION_PROFILE_RACES = 30
PRODUCTION_OUTER_RACES = 6
TERMINAL_FINISH_POSITIONS = {99}
ISTANBUL_TZ = ZoneInfo("Europe/Istanbul")


def safe_int(value: Any, default: int = 999) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def safe_float(value: Any, default: float = 50.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                rows.append(value)
    return rows


def parse_race_date(value: Any) -> datetime | None:
    try:
        return datetime.strptime(str(value or ""), "%d.%m.%Y")
    except ValueError:
        return None


def race_sort_key(rows: list[dict[str, Any]]) -> tuple[datetime, int, str]:
    first = rows[0]
    day = parse_race_date(first.get("race_date")) or datetime.min
    return day, safe_int(first.get("race_no"), 0), str(first.get("race_id") or "")


def candidate_identity_valid(rows: list[dict[str, Any]]) -> bool:
    identity_keys = (
        "race_date",
        "race_id",
        "race_no",
        "race_time",
        "city",
        "city_id",
        "race_type",
        "track",
        "distance",
        "field_size",
        "sart1_candidate_version",
        "sart1_candidate_observation_start",
        "sart1_candidate_created_ts",
        "sart1_candidate_baseline_version",
    )
    for key in identity_keys:
        values = {str(row.get(key) if row.get(key) is not None else "").strip() for row in rows}
        if len(values) != 1 or not next(iter(values)):
            return False

    horse_names = [str(row.get("horse_name") or "").strip().casefold() for row in rows]
    if any(not name for name in horse_names) or len(set(horse_names)) != len(horse_names):
        return False

    race_day = parse_race_date(rows[0].get("race_date"))
    race_time = str(rows[0].get("race_time") or "").strip().replace(".", ":")
    created_ts = safe_int(rows[0].get("sart1_candidate_created_ts"), 0)
    if race_day is None or created_ts <= 0:
        return False
    try:
        hour, minute = (int(part) for part in race_time.split(":", 1))
        race_start = race_day.replace(
            hour=hour,
            minute=minute,
            tzinfo=ISTANBUL_TZ,
        )
    except (TypeError, ValueError):
        return False
    return created_ts < int(race_start.timestamp())


def classify_race(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "unlabeled"
    expected = safe_int(rows[0].get("field_size"), len(rows))
    labels = [safe_int(row.get("finish_pos"), 0) for row in rows]
    labeled = sum(value > 0 for value in labels)
    if labeled == 0:
        return "unlabeled"
    if labeled != len(rows) or expected != len(rows):
        return "partial"
    if labels.count(1) != 1:
        return "integrity_invalid"

    ranked_positions = [
        value for value in labels
        if value not in TERMINAL_FINISH_POSITIONS
    ]
    if any(value < 1 or value > expected for value in ranked_positions):
        return "integrity_invalid"
    expected_rank = 1
    for rank, tied_count in sorted(Counter(ranked_positions).items()):
        if rank != expected_rank:
            return "integrity_invalid"
        expected_rank += tied_count

    visible = [safe_int(row.get("sart1_candidate_baseline_rank"), 0) for row in rows]
    candidate = [safe_int(row.get("sart1_candidate_rank"), 0) for row in rows]
    no_agf = [safe_int(row.get("sart1_candidate_no_agf_rank"), 0) for row in rows]
    valid_ranks = list(range(1, len(rows) + 1))
    if (
        sorted(visible) != valid_ranks
        or sorted(candidate) != valid_ranks
        or sorted(no_agf) != valid_ranks
    ):
        return "integrity_invalid"
    return "fully_labeled"


def ranking_guardrails(rows: list[dict[str, Any]], rank_key: str) -> dict[str, float | None]:
    ordered = sorted(rows, key=lambda row: safe_int(row.get(rank_key)))
    finish_ordered = sorted(rows, key=lambda row: safe_int(row.get("finish_pos")))
    predicted = {id(row): index + 1 for index, row in enumerate(ordered)}
    official = {id(row): index + 1 for index, row in enumerate(finish_ordered)}
    pred_ranks = [predicted[id(row)] for row in rows]
    finish_ranks = [official[id(row)] for row in rows]
    count = len(rows)
    if count < 2:
        rho = None
    else:
        diff_sq = sum(
            (pred_ranks[index] - finish_ranks[index]) ** 2
            for index in range(count)
        )
        rho = 1.0 - (6.0 * diff_sq) / (count * (count * count - 1))
    mae = statistics.mean(
        abs(pred_ranks[index] - finish_ranks[index])
        for index in range(count)
    ) if count else None

    def relevance(row: dict[str, Any]) -> float:
        finish_rank = official.get(id(row), count)
        return max(0.0, (count - finish_rank + 1) / count) if count else 0.0

    def dcg(sequence: list[dict[str, Any]], limit: int = 5) -> float:
        return sum(
            relevance(row) / math.log2(index + 1)
            for index, row in enumerate(sequence[:limit], start=1)
        )

    ideal_dcg = dcg(finish_ordered)
    ndcg5 = dcg(ordered) / ideal_dcg if ideal_dcg > 0 else None
    return {
        "mae": round(mae, 3) if mae is not None else None,
        "rho": round(rho, 4) if rho is not None else None,
        "ndcg5": round(ndcg5, 4) if ndcg5 is not None else None,
    }


def summarize_races(races: list[dict[str, Any]]) -> dict[str, Any]:
    visible_ranks = [race["visibleWinnerRank"] for race in races]
    candidate_ranks = [race["candidateWinnerRank"] for race in races]
    no_agf_ranks = [race["noAgfWinnerRank"] for race in races]

    def metrics(ranks: list[int], guardrail_key: str) -> dict[str, Any]:
        guardrails = [race[guardrail_key] for race in races]

        def average(key: str) -> float | None:
            values = [item[key] for item in guardrails if item.get(key) is not None]
            return round(statistics.mean(values), 4) if values else None

        mae = average("mae")
        rho = average("rho")
        ndcg5 = average("ndcg5")
        average_field = statistics.mean(race["fieldSize"] for race in races) if races else 1.0
        objective = None
        if mae is not None and rho is not None and ndcg5 is not None:
            objective = round(
                0.45 * ndcg5
                + 0.35 * ((rho + 1.0) / 2.0)
                + 0.20 * max(0.0, 1.0 - mae / max(average_field, 1.0)),
                4,
            )
        return {
            "top1": sum(rank == 1 for rank in ranks),
            "winnerTop3": sum(rank <= 3 for rank in ranks),
            "winnerTop5": sum(rank <= 5 for rank in ranks),
            "avgWinnerRank": (
                round(sum(ranks) / len(ranks), 3)
                if ranks
                else None
            ),
            "mae": mae,
            "rho": rho,
            "ndcg5": ndcg5,
            "objective": objective,
        }

    return {
        "races": len(races),
        "visible": metrics(visible_ranks, "visibleGuardrails"),
        "candidate": metrics(candidate_ranks, "candidateGuardrails"),
        "noAgf": metrics(no_agf_ranks, "noAgfGuardrails"),
    }


def checkpoint_pass(summary: dict[str, Any]) -> bool:
    visible = summary["visible"]
    candidate = summary["candidate"]
    return bool(
        candidate["winnerTop3"] >= visible["winnerTop3"]
        and candidate["winnerTop5"] >= visible["winnerTop5"]
        and candidate["top1"] >= visible["top1"] - 1
        and candidate["objective"] is not None
        and visible["objective"] is not None
        and candidate["objective"] >= visible["objective"]
    )


def build_report(entries: list[dict[str, Any]], run_date: str) -> dict[str, Any]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        version = str(entry.get("sart1_candidate_version") or "").strip()
        if not version:
            continue
        grouped[(
            str(entry.get("race_date") or ""),
            str(entry.get("race_id") or ""),
            version,
        )].append(entry)

    coverage = {
        "fullyLabeledRaces": 0,
        "partialRaces": 0,
        "unlabeledRaces": 0,
        "integrityInvalidRaces": 0,
        "preProspectiveExcludedRaces": 0,
    }
    complete: list[dict[str, Any]] = []
    versions: set[str] = set()

    for ((_, _, version), rows) in sorted(grouped.items(), key=lambda item: race_sort_key(item[1])):
        versions.add(version)
        race_day = parse_race_date(rows[0].get("race_date"))
        starts = {
            str(row.get("sart1_candidate_observation_start") or "").strip()
            for row in rows
        }
        start_day = parse_race_date(next(iter(starts))) if len(starts) == 1 else None
        if race_day is None or start_day is None:
            coverage["integrityInvalidRaces"] += 1
            continue
        if race_day < start_day:
            coverage["preProspectiveExcludedRaces"] += 1
            continue
        if not candidate_identity_valid(rows):
            coverage["integrityInvalidRaces"] += 1
            continue

        state = classify_race(rows)
        if state == "fully_labeled":
            coverage["fullyLabeledRaces"] += 1
        elif state == "partial":
            coverage["partialRaces"] += 1
            continue
        elif state == "unlabeled":
            coverage["unlabeledRaces"] += 1
            continue
        else:
            coverage["integrityInvalidRaces"] += 1
            continue

        winner = next(row for row in rows if safe_int(row.get("finish_pos"), 0) == 1)
        flags = [
            row.get("sart1_candidate_metric_source_flags")
            or row.get("metric_source_flags")
            or {}
            for row in rows
        ]
        feature_snapshots = [
            row.get("sart1_candidate_feature_snapshot") or {}
            for row in rows
        ]
        complete.append(
            {
                "raceId": str(winner.get("race_id") or ""),
                "raceDate": winner.get("race_date"),
                "raceNo": winner.get("race_no"),
                "raceTime": winner.get("race_time"),
                "city": winner.get("city"),
                "cityId": winner.get("city_id"),
                "raceType": winner.get("race_type"),
                "track": winner.get("track"),
                "distance": winner.get("distance"),
                "fieldSize": len(rows),
                "winner": winner.get("horse_name"),
                "candidateVersion": version,
                "candidateCreatedTs": safe_int(winner.get("sart1_candidate_created_ts"), 0),
                "visibleWinnerRank": safe_int(winner.get("sart1_candidate_baseline_rank")),
                "candidateWinnerRank": safe_int(winner.get("sart1_candidate_rank")),
                "noAgfWinnerRank": safe_int(winner.get("sart1_candidate_no_agf_rank")),
                "visibleGuardrails": ranking_guardrails(
                    rows,
                    "sart1_candidate_baseline_rank",
                ),
                "candidateGuardrails": ranking_guardrails(
                    rows,
                    "sart1_candidate_rank",
                ),
                "noAgfGuardrails": ranking_guardrails(
                    rows,
                    "sart1_candidate_no_agf_rank",
                ),
                "agfCoverage": winner.get("sart1_candidate_agf_coverage"),
                "agfApplied": bool(winner.get("sart1_candidate_agf_applied")),
                "sourceCoverage": {
                    "training": sum(bool(flag.get("hasTraining")) for flag in flags),
                    "trainingNonNeutral": sum(
                        bool(flag.get("hasTraining"))
                        and abs(safe_float(snapshot.get("training_fitness")) - 50.0) >= 1.0
                        for flag, snapshot in zip(flags, feature_snapshots)
                    ),
                    "trainer": sum(bool(flag.get("hasTrainer")) for flag in flags),
                    "trainerNonNeutral": sum(
                        bool(flag.get("hasTrainer"))
                        and abs(safe_float(snapshot.get("trainer_score")) - 50.0) >= 1.0
                        for flag, snapshot in zip(flags, feature_snapshots)
                    ),
                    "pedigree": sum(bool(flag.get("hasPedigree")) for flag in flags),
                    "pedigreeNonNeutral": sum(
                        bool(flag.get("hasPedigree"))
                        and abs(safe_float(snapshot.get("pedigree")) - 50.0) >= 1.0
                        for flag, snapshot in zip(flags, feature_snapshots)
                    ),
                    "agf": sum(bool(flag.get("hasAgf")) for flag in flags),
                    "agfNonNeutral": sum(
                        bool(flag.get("hasAgf"))
                        and abs(safe_float(snapshot.get("agf_score")) - 50.0) >= 1.0
                        for flag, snapshot in zip(flags, feature_snapshots)
                    ),
                    "runnerCount": len(rows),
                },
            }
        )

    complete.sort(key=lambda race: (
        parse_race_date(race["raceDate"]) or datetime.min,
        safe_int(race["raceNo"], 0),
        race["raceId"],
    ))
    cumulative = summarize_races(complete)
    agf_cohorts = {
        "applied": summarize_races([race for race in complete if race["agfApplied"]]),
        "notApplied": summarize_races([race for race in complete if not race["agfApplied"]]),
    }
    total_runners = sum(race["sourceCoverage"]["runnerCount"] for race in complete)
    source_coverage = {"runnerCount": total_runners}
    for source in ("training", "trainer", "pedigree", "agf"):
        count = sum(race["sourceCoverage"][source] for race in complete)
        non_neutral = sum(
            race["sourceCoverage"][f"{source}NonNeutral"]
            for race in complete
        )
        source_coverage[source] = {
            "count": count,
            "coverage": round(count / total_runners, 4) if total_runners else 0.0,
            "nonNeutralCount": non_neutral,
            "nonNeutralRatio": (
                round(non_neutral / total_runners, 4)
                if total_runners
                else 0.0
            ),
        }

    checkpoints: list[dict[str, Any]] = []
    for start in range(0, len(complete), CHECKPOINT_RACES):
        window = complete[start:start + CHECKPOINT_RACES]
        if len(window) < CHECKPOINT_RACES:
            break
        summary = summarize_races(window)
        checkpoints.append(
            {
                "index": len(checkpoints) + 1,
                "startRace": start + 1,
                "endRace": start + CHECKPOINT_RACES,
                **summary,
                "passed": checkpoint_pass(summary),
            }
        )

    last_three_pass = len(checkpoints) >= 3 and all(
        checkpoint["passed"] for checkpoint in checkpoints[-3:]
    )
    cumulative_improvement = (
        cumulative["candidate"]["winnerTop3"]
        - cumulative["visible"]["winnerTop3"]
    )
    objective_delta = (
        round(
            cumulative["candidate"]["objective"]
            - cumulative["visible"]["objective"],
            4,
        )
        if cumulative["candidate"]["objective"] is not None
        and cumulative["visible"]["objective"] is not None
        else None
    )
    source_gate_ready = bool(
        source_coverage["training"]["coverage"] >= 0.40
        and source_coverage["training"]["nonNeutralRatio"] >= 0.15
        and source_coverage["trainer"]["coverage"] >= 0.40
        and source_coverage["trainer"]["nonNeutralRatio"] >= 0.15
    )
    base_supported = bool(
        len(complete) >= REVIEW_RACES
        and last_three_pass
        and cumulative_improvement >= 2
        and cumulative["candidate"]["winnerTop5"]
        >= cumulative["visible"]["winnerTop5"]
        and objective_delta is not None
        and objective_delta >= 0.003
        and source_gate_ready
    )
    agf_applied_summary = agf_cohorts["applied"]
    agf_evidence_ready = agf_applied_summary["races"] >= 6
    agf_overlay_supported = bool(
        agf_evidence_ready
        and agf_applied_summary["candidate"]["winnerTop3"]
        >= agf_applied_summary["noAgf"]["winnerTop3"]
        and agf_applied_summary["candidate"]["winnerTop5"]
        >= agf_applied_summary["noAgf"]["winnerTop5"]
        and agf_applied_summary["candidate"]["objective"] is not None
        and agf_applied_summary["noAgf"]["objective"] is not None
        and agf_applied_summary["candidate"]["objective"]
        >= agf_applied_summary["noAgf"]["objective"]
    )
    research_supported = base_supported and agf_overlay_supported
    regression_signal = bool(
        checkpoints
        and (
            checkpoints[-1]["candidate"]["winnerTop3"]
            < checkpoints[-1]["visible"]["winnerTop3"]
            or checkpoints[-1]["candidate"]["winnerTop5"]
            < checkpoints[-1]["visible"]["winnerTop5"]
        )
    )

    if len(complete) < CHECKPOINT_RACES:
        status = "COLLECTING"
    elif regression_signal:
        status = "REGRESSION_SIGNAL"
    elif len(complete) < REVIEW_RACES:
        status = "EARLY_SIGNAL"
    elif research_supported:
        status = "SUPPORTED_FOR_FORMAL_REPLAY"
    elif base_supported and not agf_evidence_ready:
        status = "BASE_SUPPORTED_AWAITING_AGF"
    elif base_supported and not agf_overlay_supported:
        status = "AGF_OVERLAY_REJECTED"
    else:
        status = "REVIEW"

    return {
        "runDate": run_date,
        "dataAsOfTs": max(
            (race["candidateCreatedTs"] for race in complete),
            default=None,
        ),
        "mode": "prospective_shadow_only",
        "candidateVersions": sorted(versions),
        "status": status,
        "coverage": coverage,
        "cumulative": cumulative,
        "agfCohorts": agf_cohorts,
        "sourceCoverage": source_coverage,
        "checkpoints": checkpoints,
        "sourceGateReady": source_gate_ready,
        "baseSupported": base_supported,
        "agfEvidenceReady": agf_evidence_ready,
        "agfOverlaySupported": agf_overlay_supported,
        "researchSupported": research_supported,
        "regressionSignal": regression_signal,
        "prospectiveObjectiveDelta": objective_delta,
        "liveRolloutEligible": False,
        "liveRolloutReason": (
            "The bounded candidate cannot alter visible ranking before the "
            "prospective and formal replay gates pass. Formal review requires "
            f"{PRODUCTION_PROFILE_RACES} fully labeled profile races and at least "
            f"{PRODUCTION_OUTER_RACES} outer-holdout races."
        ),
        "nextCheckpointAt": (
            ((len(complete) // CHECKPOINT_RACES) + 1) * CHECKPOINT_RACES
        ),
        "races": complete,
    }


def markdown(report: dict[str, Any]) -> str:
    cumulative = report["cumulative"]
    visible = cumulative["visible"]
    candidate = cumulative["candidate"]
    no_agf = cumulative["noAgf"]
    coverage = report["coverage"]
    agf_applied = report["agfCohorts"]["applied"]
    agf_off = report["agfCohorts"]["notApplied"]
    sources = report["sourceCoverage"]
    lines = [
        f"# SART1 Prospective Shadow - {report['runDate']}",
        "",
        f"- Status: **{report['status']}**",
        f"- Fully labeled prospective races: {coverage['fullyLabeledRaces']}",
        f"- Partial / unlabeled / invalid: {coverage['partialRaces']} / "
        f"{coverage['unlabeledRaces']} / {coverage['integrityInvalidRaces']}",
        f"- Pre-prospective races excluded: {coverage['preProspectiveExcludedRaces']}",
        f"- Next checkpoint: {report['nextCheckpointAt']} races",
        f"- Prospective objective delta: {report['prospectiveObjectiveDelta']}",
        f"- Source gate ready: {report['sourceGateReady']}",
        f"- AGF evidence ready / overlay supported: "
        f"{report['agfEvidenceReady']} / {report['agfOverlaySupported']}",
        "- Live ranking and Telegram: unchanged",
        "",
        "| Ranking | Top1 | Winner Top3 | Winner Top5 | Avg winner rank |",
        "| --- | ---: | ---: | ---: | ---: |",
        f"| Visible v4 | {visible['top1']} | {visible['winnerTop3']} | "
        f"{visible['winnerTop5']} | {visible['avgWinnerRank']} |",
        f"| SART1 candidate | {candidate['top1']} | {candidate['winnerTop3']} | "
        f"{candidate['winnerTop5']} | {candidate['avgWinnerRank']} |",
        f"| SART1 no-AGF ablation | {no_agf['top1']} | {no_agf['winnerTop3']} | "
        f"{no_agf['winnerTop5']} | {no_agf['avgWinnerRank']} |",
        "",
        "| Ranking | Rho | MAE | NDCG@5 | Objective |",
        "| --- | ---: | ---: | ---: | ---: |",
        f"| Visible v4 | {visible['rho']} | {visible['mae']} | {visible['ndcg5']} | "
        f"{visible['objective']} |",
        f"| SART1 candidate | {candidate['rho']} | {candidate['mae']} | "
        f"{candidate['ndcg5']} | {candidate['objective']} |",
        f"| SART1 no-AGF ablation | {no_agf['rho']} | {no_agf['mae']} | "
        f"{no_agf['ndcg5']} | {no_agf['objective']} |",
        "",
        "## Source and AGF cohorts",
        "",
        f"- Runner source coverage/non-neutral: training "
        f"{sources['training']['count']}/{sources['training']['nonNeutralCount']}/"
        f"{sources['runnerCount']}, trainer {sources['trainer']['count']}/"
        f"{sources['trainer']['nonNeutralCount']}/{sources['runnerCount']}, pedigree "
        f"{sources['pedigree']['count']}/{sources['pedigree']['nonNeutralCount']}/"
        f"{sources['runnerCount']}, AGF {sources['agf']['count']}/"
        f"{sources['agf']['nonNeutralCount']}/{sources['runnerCount']}.",
        f"- AGF applied: {agf_applied['races']} races; visible/candidate/no-AGF "
        f"Winner Top3 {agf_applied['visible']['winnerTop3']}/"
        f"{agf_applied['candidate']['winnerTop3']}/"
        f"{agf_applied['noAgf']['winnerTop3']}.",
        f"- AGF not applied: {agf_off['races']} races; visible/candidate/no-AGF "
        f"Winner Top3 {agf_off['visible']['winnerTop3']}/"
        f"{agf_off['candidate']['winnerTop3']}/"
        f"{agf_off['noAgf']['winnerTop3']}.",
        "",
        "## Checkpoints",
        "",
        "| # | Races | Visible WTop3 | Candidate WTop3 | Passed |",
        "| ---: | ---: | ---: | ---: | --- |",
    ]
    for checkpoint in report["checkpoints"]:
        lines.append(
            f"| {checkpoint['index']} | {checkpoint['races']} | "
            f"{checkpoint['visible']['winnerTop3']} | "
            f"{checkpoint['candidate']['winnerTop3']} | "
            f"{'yes' if checkpoint['passed'] else 'no'} |"
        )
    if not report["checkpoints"]:
        lines.append("| - | 0 | 0 | 0 | collecting |")
    return "\n".join(lines) + "\n"


def atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(content, encoding="utf-8")
    os.replace(temp, path)


def persist(report: dict[str, Any], data_dir: Path) -> None:
    run_dir = data_dir / "automation" / "runs" / report["runDate"]
    latest_dir = data_dir / "automation" / "sart1-shadow"
    encoded = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    rendered = markdown(report)
    for path, content in (
        (run_dir / "sart1-shadow-checkpoint.json", encoded),
        (run_dir / "sart1-shadow-checkpoint.md", rendered),
        (latest_dir / "latest.json", encoded),
        (latest_dir / "latest.md", rendered),
    ):
        atomic_write(path, content)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--predictions", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--run-date", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(load_jsonl(args.predictions), args.run_date)
    persist(report, args.data_dir)
    print(json.dumps({
        "status": report["status"],
        "fullyLabeledRaces": report["coverage"]["fullyLabeledRaces"],
        "nextCheckpointAt": report["nextCheckpointAt"],
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
