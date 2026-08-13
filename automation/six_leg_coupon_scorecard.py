"""Analysis-only six-leg coupon scorecard for the active prediction log.

The scorecard deliberately does not model odds, stake, payout, substitutions,
or the official first/second six-leg pool start.  It answers a narrower and
repeatable question: for every structurally present six-consecutive-race
window on a city card, how many winning horses were inside visible Top-K?

Two evidence sets are always kept separate:

* ``winnerKnown`` accepts a race once one unambiguous winner is known, even if
  the remaining finish order is partial.
* ``cleanFull`` accepts only complete, integrity-safe finish orders.

Nothing in this module changes weights, ranking, predictions, or Telegram.
"""

from __future__ import annotations

import argparse
import json
import math
import tempfile
import unicodedata
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = "six-leg-coupon-scorecard-v1"
TOP_K_VALUES = (1, 2, 3, 4, 5)
HORIZON_DAYS = (7, 14, 30)
TERMINAL_FINISH_POSITIONS = {99}


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        numeric = float(value)
        return int(numeric) if math.isfinite(numeric) else default
    except (TypeError, ValueError):
        return default


def rounded(value: float | None, digits: int = 4) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, digits)


def rate(numerator: int, denominator: int) -> float | None:
    return rounded(numerator / denominator, 4) if denominator else None


def parse_date(value: Any) -> date | None:
    text = str(value or "").strip()
    for pattern in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, pattern).date()
        except ValueError:
            continue
    return None


def clean_text(value: Any) -> str:
    text = str(value or "").strip()
    return "" if text.casefold() in {"none", "null", "nan"} else text


def fold_text(value: Any) -> str:
    normalized = unicodedata.normalize("NFKD", clean_text(value))
    return "".join(char for char in normalized if not unicodedata.combining(char)).upper()


def load_jsonl(path: Path) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    invalid_lines = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                invalid_lines += 1
                continue
            if isinstance(value, dict):
                rows.append(value)
            else:
                invalid_lines += 1
    return rows, invalid_lines


def city_identity(rows: list[dict[str, Any]]) -> tuple[str, str]:
    city_ids = {clean_text(row.get("city_id")) for row in rows if clean_text(row.get("city_id"))}
    if len(city_ids) > 1:
        return "", ""
    city_names = {
        clean_text(row.get("city") or row.get("city_name"))
        for row in rows
        if clean_text(row.get("city") or row.get("city_name"))
    }
    if len(city_names) > 1:
        return "", ""
    city_name = next(iter(city_names), "")
    if city_ids:
        city_id = next(iter(city_ids))
        return f"id:{city_id}", city_name or city_id
    if city_name:
        return f"name:{fold_text(city_name)}", city_name
    return "", ""


def row_race_identity(row: dict[str, Any]) -> tuple[str, ...]:
    parsed = parse_date(row.get("race_date"))
    date_key = parsed.isoformat() if parsed else clean_text(row.get("race_date"))
    race_id = clean_text(row.get("race_id"))
    if race_id:
        return "race_id", date_key, race_id
    city_id = clean_text(row.get("city_id"))
    city = fold_text(row.get("city") or row.get("city_name"))
    return (
        "fallback",
        date_key,
        city_id or city,
        clean_text(row.get("race_no")),
        clean_text(row.get("race_time")),
    )


def group_races(entries: Iterable[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        grouped[row_race_identity(entry)].append(entry)
    return list(grouped.values())


def finish_order_is_clean(labels: list[int], field_size: int) -> bool:
    ranked = [value for value in labels if value not in TERMINAL_FINISH_POSITIONS]
    if any(value < 1 or value > field_size for value in ranked):
        return False
    expected_rank = 1
    for rank_value, tied_count in sorted(Counter(ranked).items()):
        if rank_value != expected_rank:
            return False
        expected_rank += tied_count
    return True


def classify_race(rows: list[dict[str, Any]]) -> tuple[str, dict[str, Any]]:
    """Classify race result quality and return card-ready metadata."""
    if not rows:
        return "integrity_invalid", {}

    parsed_dates = {parsed for row in rows if (parsed := parse_date(row.get("race_date")))}
    race_numbers = {safe_int(row.get("race_no"), 0) for row in rows}
    field_sizes = {safe_int(row.get("field_size"), 0) for row in rows}
    names = [fold_text(row.get("horse_name")) for row in rows]
    ranks = [safe_int(row.get("rank_pred"), 0) for row in rows]
    card_city_key, city_name = city_identity(rows)

    structural_ok = (
        len(parsed_dates) == 1
        and len(race_numbers) == 1
        and next(iter(race_numbers), 0) > 0
        and len(field_sizes) == 1
        and next(iter(field_sizes), 0) == len(rows)
        and all(names)
        and len(set(names)) == len(names)
        and sorted(ranks) == list(range(1, len(rows) + 1))
    )
    metadata = {
        "date": next(iter(parsed_dates), None),
        "raceNo": next(iter(race_numbers), 0),
        "raceId": clean_text(rows[0].get("race_id")),
        "cityKey": card_city_key,
        "city": city_name,
        "fieldSize": len(rows),
        "rankingVersion": clean_text(rows[0].get("v4_version")),
    }
    if not structural_ok:
        return "integrity_invalid", metadata

    labels = [safe_int(row.get("finish_pos"), 0) for row in rows]
    labeled = sum(value > 0 for value in labels)
    if any(
        value > 0 and value not in TERMINAL_FINISH_POSITIONS and value > len(rows)
        for value in labels
    ):
        return "integrity_invalid", metadata
    winners = [row for row, finish in zip(rows, labels) if finish == 1]
    if len(winners) > 1:
        return "integrity_invalid", metadata
    if not winners:
        return ("unlabeled" if labeled == 0 else "partial_winner_unknown"), metadata

    winner = winners[0]
    metadata.update({
        "winner": clean_text(winner.get("horse_name")),
        "winnerRank": safe_int(winner.get("rank_pred"), 0),
    })
    if labeled != len(rows):
        return "winner_known_partial", metadata
    if not finish_order_is_clean(labels, len(rows)):
        return "integrity_invalid", metadata
    return "fully_labeled", metadata


def build_race_records(entries: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    records = []
    for rows in group_races(entries):
        quality, metadata = classify_race(rows)
        records.append({"quality": quality, **metadata})
    return records


def build_cards(
    records: list[dict[str, Any]],
) -> tuple[dict[tuple[str, str], dict[int, dict[str, Any] | None]], dict[str, int]]:
    buckets: dict[tuple[str, str], dict[int, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    missing_card_identity = 0
    for record in records:
        race_date = record.get("date")
        city_key = clean_text(record.get("cityKey"))
        race_no = safe_int(record.get("raceNo"), 0)
        if not isinstance(race_date, date) or not city_key or race_no <= 0:
            missing_card_identity += 1
            continue
        buckets[(race_date.isoformat(), city_key)][race_no].append(record)

    cards: dict[tuple[str, str], dict[int, dict[str, Any] | None]] = {}
    collisions = 0
    for card_key, by_number in buckets.items():
        cards[card_key] = {}
        for race_no, candidates in by_number.items():
            if len(candidates) == 1:
                cards[card_key][race_no] = candidates[0]
            else:
                cards[card_key][race_no] = None
                collisions += 1
    return cards, {
        "cardIdentifiedRaces": len(records) - missing_card_identity,
        "missingCardIdentityRaces": missing_card_identity,
        "raceNumberCollisions": collisions,
    }


def window_detail(
    card_key: tuple[str, str],
    records: list[dict[str, Any]],
    evidence: str,
) -> dict[str, Any]:
    winner_ranks = [safe_int(record.get("winnerRank"), 999) for record in records]
    race_numbers = [safe_int(record.get("raceNo"), 0) for record in records]
    first = records[0]
    top_k = {}
    for k in TOP_K_VALUES:
        legs_hit = sum(rank <= k for rank in winner_ranks)
        top_k[str(k)] = {
            "legsHit": legs_hit,
            "fivePlus": legs_hit >= 5,
            "sixOfSix": legs_hit == 6,
        }
    return {
        "windowKey": f"{card_key[0]}|{card_key[1]}|{race_numbers[0]}-{race_numbers[-1]}",
        "raceDate": card_key[0],
        "cityKey": card_key[1],
        "city": first.get("city"),
        "startRaceNo": race_numbers[0],
        "endRaceNo": race_numbers[-1],
        "raceNos": race_numbers,
        "raceIds": [record.get("raceId") for record in records],
        "winnerRanks": winner_ranks,
        "evidence": evidence,
        "containsPartialRace": any(record.get("quality") != "fully_labeled" for record in records),
        "topK": top_k,
    }


def build_windows(
    cards: dict[tuple[str, str], dict[int, dict[str, Any] | None]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    winner_known: list[dict[str, Any]] = []
    clean_full: list[dict[str, Any]] = []
    structural_windows = 0
    for card_key, by_number in sorted(cards.items()):
        if not by_number:
            continue
        first_no = min(by_number)
        last_no = max(by_number)
        for start in range(first_no, last_no - 4):
            records = [by_number.get(race_no) for race_no in range(start, start + 6)]
            if any(record is None for record in records):
                continue
            structural_windows += 1
            typed_records = [record for record in records if isinstance(record, dict)]
            qualities = {record.get("quality") for record in typed_records}
            if qualities <= {"fully_labeled", "winner_known_partial"}:
                winner_known.append(window_detail(card_key, typed_records, "winnerKnown"))
            if qualities == {"fully_labeled"}:
                clean_full.append(window_detail(card_key, typed_records, "cleanFull"))
    return winner_known, clean_full, structural_windows


def summarize_windows(windows: list[dict[str, Any]]) -> dict[str, Any]:
    window_count = len(windows)
    unique_race_ranks: dict[tuple[str, str, int], int] = {}
    for window in windows:
        for race_no, winner_rank in zip(window["raceNos"], window["winnerRanks"]):
            unique_race_ranks[(window["raceDate"], window["cityKey"], race_no)] = winner_rank
    top_k = {}
    for k in TOP_K_VALUES:
        key = str(k)
        hits_per_window = [safe_int(window["topK"][key]["legsHit"], 0) for window in windows]
        window_leg_hits = sum(hits_per_window)
        unique_leg_hits = sum(rank <= k for rank in unique_race_ranks.values())
        five_plus = sum(hits >= 5 for hits in hits_per_window)
        six_of_six = sum(hits == 6 for hits in hits_per_window)
        distribution = Counter(hits_per_window)
        top_k[key] = {
            "legHits": unique_leg_hits,
            "legOpportunities": len(unique_race_ranks),
            "legHitRate": rate(unique_leg_hits, len(unique_race_ranks)),
            "windowLegHits": window_leg_hits,
            "windowLegOpportunities": window_count * 6,
            "windowLegHitRate": rate(window_leg_hits, window_count * 6),
            "averageLegsHitPerWindow": rounded(window_leg_hits / window_count, 3) if window_count else None,
            "fivePlusWindows": five_plus,
            "fivePlusWindowRate": rate(five_plus, window_count),
            "sixOfSixWindows": six_of_six,
            "sixOfSixWindowRate": rate(six_of_six, window_count),
            "legsHitDistribution": {str(value): distribution.get(value, 0) for value in range(7)},
        }
    return {
        "windows": window_count,
        "cards": len({(window["raceDate"], window["cityKey"]) for window in windows}),
        "topK": top_k,
    }


def windows_in_horizon(
    windows: list[dict[str, Any]],
    end_date: date,
    days: int,
) -> list[dict[str, Any]]:
    start_date = end_date - timedelta(days=days - 1)
    return [
        window
        for window in windows
        if (parsed := parse_date(window.get("raceDate"))) is not None
        and start_date <= parsed <= end_date
    ]


def build_report(
    entries: list[dict[str, Any]],
    run_date: str,
    *,
    invalid_json_lines: int = 0,
) -> dict[str, Any]:
    as_of = parse_date(run_date)
    if as_of is None or len(run_date) != 10 or run_date[4] != "-":
        raise ValueError("run_date must be YYYY-MM-DD")

    records = build_race_records(entries)
    cards, card_inventory = build_cards(records)
    winner_windows, clean_windows, structural_windows = build_windows(cards)
    quality_counts = Counter(record.get("quality") for record in records)
    horizons = {}
    for days in HORIZON_DAYS:
        winner_subset = windows_in_horizon(winner_windows, as_of, days)
        clean_subset = windows_in_horizon(clean_windows, as_of, days)
        horizons[f"last{days}Days"] = {
            "startDate": (as_of - timedelta(days=days - 1)).isoformat(),
            "endDate": as_of.isoformat(),
            "winnerKnown": summarize_windows(winner_subset),
            "cleanFull": summarize_windows(clean_subset),
        }

    source_timestamp = max((safe_int(row.get("ts"), 0) for row in entries), default=0)
    last30_winner = windows_in_horizon(winner_windows, as_of, 30)
    last30_clean = windows_in_horizon(clean_windows, as_of, 30)
    return {
        "schemaVersion": SCHEMA_VERSION,
        "runDate": as_of.isoformat(),
        "sourceSnapshotAt": (
            datetime.fromtimestamp(source_timestamp, tz=timezone.utc).isoformat(timespec="seconds")
            if source_timestamp > 0
            else None
        ),
        "input": {
            "validJsonRows": len(entries),
            "invalidJsonLines": invalid_json_lines,
            "raceGroups": len(records),
            "raceQualityCounts": dict(sorted(quality_counts.items())),
            **card_inventory,
            "cards": len(cards),
            "structuralSixLegWindows": structural_windows,
            "winnerKnownSixLegWindows": len(winner_windows),
            "cleanFullSixLegWindows": len(clean_windows),
        },
        "horizons": horizons,
        "windowDetailsLast30Days": {
            "winnerKnown": last30_winner,
            "cleanFull": last30_clean,
        },
        "policy": {
            "rankingField": "rank_pred",
            "topK": list(TOP_K_VALUES),
            "windowLength": 6,
            "windowDefinition": "all_contiguous_six_race_number_windows_per_city_day",
            "overlappingWindowsPossible": True,
            "winnerKnownDefinition": "one_integrity_safe_winner; remaining order may be partial",
            "cleanFullDefinition": "complete_integrity_safe_finish_order",
            "officialPoolScheduleModeled": False,
            "oddsStakePayoutModeled": False,
            "automaticWeightChange": False,
            "usedForRanking": False,
        },
    }


def format_rate(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:.1f}%"


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Altılı Kupon Scorecard",
        "",
        f'- Run date: `{report["runDate"]}`',
        f'- Race groups: `{report["input"]["raceGroups"]}`',
        f'- Structural six-leg windows: `{report["input"]["structuralSixLegWindows"]}`',
        f'- Winner-known windows: `{report["input"]["winnerKnownSixLegWindows"]}`',
        f'- Clean-full windows: `{report["input"]["cleanFullSixLegWindows"]}`',
        "- Ranking impact: `false`",
        "",
    ]
    for days in HORIZON_DAYS:
        horizon = report["horizons"][f"last{days}Days"]
        lines.extend([
            f"## Son {days} gün",
            "",
            f'Period: `{horizon["startDate"]}` - `{horizon["endDate"]}`',
            "",
        ])
        for evidence_key, evidence_label in (
            ("winnerKnown", "Winner-known"),
            ("cleanFull", "Clean-full"),
        ):
            summary = horizon[evidence_key]
            lines.extend([
                f"### {evidence_label}",
                "",
                f'Windows: `{summary["windows"]}`; cards: `{summary["cards"]}`.',
                "",
                "| Selection depth | Unique race hit | Window leg slots | 5+/6 windows | 6/6 windows |",
                "|---:|---:|---:|---:|---:|",
            ])
            for k in TOP_K_VALUES:
                item = summary["topK"][str(k)]
                lines.append(
                    f'| Top{k} | {item["legHits"]}/{item["legOpportunities"]} '
                    f'({format_rate(item["legHitRate"])}) | '
                    f'{item["windowLegHits"]}/{item["windowLegOpportunities"]} '
                    f'({format_rate(item["windowLegHitRate"])}) | '
                    f'{item["fivePlusWindows"]}/{summary["windows"]} '
                    f'({format_rate(item["fivePlusWindowRate"])}) | '
                    f'{item["sixOfSixWindows"]}/{summary["windows"]} '
                    f'({format_rate(item["sixOfSixWindowRate"])}) |'
                )
            lines.append("")
    lines.extend([
        "## Interpretation",
        "",
        "- Every city/day sequence with six consecutive race numbers is one window.",
        "- Winner-known can include partial result orders; clean-full never does.",
        "- This is a ranking coverage scorecard, not an official pool, cost, odds, payout, or substitution simulator.",
        "- The scorecard is analysis-only and never changes visible rankings or weights.",
        "",
    ])
    return "\n".join(lines)


def atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        handle.write(content)
        temporary = Path(handle.name)
    temporary.replace(path)


def persist(report: dict[str, Any], data_dir: Path) -> dict[str, str]:
    daily_dir = data_dir / "automation" / "runs" / report["runDate"]
    latest_dir = data_dir / "automation" / "coupon-scorecard"
    json_text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    markdown = render_markdown(report)
    paths = {
        "dailyJson": daily_dir / "six-leg-coupon-scorecard.json",
        "dailyMarkdown": daily_dir / "six-leg-coupon-scorecard.md",
        "latestJson": latest_dir / "latest.json",
        "latestMarkdown": latest_dir / "latest.md",
    }
    for path in paths.values():
        atomic_write(path, json_text if path.suffix == ".json" else markdown)
    return {key: str(path) for key, path in paths.items()}


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the analysis-only six-leg coupon scorecard.")
    parser.add_argument("--predictions", required=True, type=Path)
    parser.add_argument("--data-dir", required=True, type=Path)
    parser.add_argument("--run-date", default=datetime.now().astimezone().strftime("%Y-%m-%d"))
    arguments = parser.parse_args()

    entries, invalid_lines = load_jsonl(arguments.predictions)
    report = build_report(entries, arguments.run_date, invalid_json_lines=invalid_lines)
    paths = persist(report, arguments.data_dir)
    print(json.dumps({
        "success": True,
        "runDate": report["runDate"],
        "input": report["input"],
        "horizons": report["horizons"],
        "paths": paths,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
