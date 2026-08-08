import re


def score_text(value):
    try:
        return f"{float(value):.1f}"
    except (TypeError, ValueError):
        return "-"


def rank_key(item):
    try:
        return int(item.get("v4Rank", 9999))
    except (TypeError, ValueError):
        return 9999


def _race_confidence(race, rankings):
    decision = race.get("decisionConfidence") or {}
    breakdown = race.get("confidenceBreakdown") or {}
    if not decision and isinstance(breakdown, dict):
        decision = breakdown.get("overall") or {}
    if rankings and not decision:
        decision = rankings[0].get("v4DecisionConfidence") or {}
    if rankings and not breakdown:
        breakdown = rankings[0].get("v4ConfidenceBreakdown") or {}
    return decision if isinstance(decision, dict) else {}, breakdown if isinstance(breakdown, dict) else {}


def _confidence_lines(race, rankings):
    decision, breakdown = _race_confidence(race, rankings)
    is_low = bool(
        decision.get("lowConfidence")
        or decision.get("openRace")
        or decision.get("label") == "LOW"
    )
    if not is_low:
        return []

    lines = ["⚠ DÜŞÜK GÜVEN / AÇIK YARIŞ"]
    separation = breakdown.get("separation") or {}
    data = breakdown.get("data") or {}
    details = []
    gap = separation.get("top3Top4Gap")
    crowd = separation.get("cutoffCrowd2pt")
    if decision.get("openRace") and gap is not None:
        details.append(f"Top3 sınırı {score_text(gap)} puan")
    if decision.get("openRace") and crowd is not None:
        details.append(f"±2 puanda {crowd} at")
    coverage = data.get("weightedRealCoverage")
    try:
        if coverage is not None and float(coverage) < 0.85:
            details.append(f"gerçek kaynak %{float(coverage) * 100:.0f}")
    except (TypeError, ValueError):
        pass
    if details:
        lines.append(" | ".join(details))
    return lines


def format_analysis_message(run_date, analysis, run_url=None):
    totals = analysis.get("totals", {})
    lines = [
        "Atistik gunluk analiz tamamlandi",
        f"Tarih: {run_date}",
        f"Analiz edilen kosu: {totals.get('analyzed', 0)} / {totals.get('racesFound', 0)}",
        f"Hata: {totals.get('failed', 0)}",
        "",
    ]

    for city in analysis.get("cities", []) or []:
        city_name = city.get("city", "")
        for race in city.get("races", []) or []:
            rankings = race.get("rankings", []) or []
            if not rankings:
                continue

            lines.append(f"{city_name} {race.get('raceNo', '')}. Kosu")
            race_type = race.get("raceType") or ""
            horse_count = race.get("horseCount") or len(rankings)
            if race_type or horse_count:
                lines.append(f"{race_type} | {horse_count} at")
            lines.extend(_confidence_lines(race, rankings))

            for item in sorted(rankings, key=rank_key):
                rank = item.get("v4Rank", "")
                horse = str(item.get("horse") or "").strip()
                horse_no = str(item.get("no") or "").strip()
                has_number_suffix = bool(re.search(r"\(\d+\)$", horse))
                display_name = horse if has_number_suffix or not horse_no else f"{horse} ({horse_no})"
                score = score_text(item.get("v4Score"))
                lines.append(f"{rank}. {display_name} - v4 puan: {score}")
            lines.append("")

    return "\n".join(lines).strip()
