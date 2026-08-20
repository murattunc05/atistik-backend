#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-analyze}"
RUN_DATE="${2:-$(TZ=Europe/Istanbul date +%Y-%m-%d)}"
ROOT_DIR="${ATISTIK_ROOT:-/opt/atistik/backend}"
DATA_DIR="${ATISTIK_ML_DATA_DIR:-${ROOT_DIR}/ml-data}"

case "$MODE" in
  analyze)
    REPORT_NAME="analysis.json"
    ;;
  results)
    REPORT_NAME="results.json"
    ;;
  *)
    echo "Desteklenmeyen guard modu: $MODE" >&2
    exit 2
    ;;
esac

REPORT_PATH="${DATA_DIR}/automation/runs/${RUN_DATE}/${REPORT_NAME}"
ANALYSIS_PATH="${DATA_DIR}/automation/runs/${RUN_DATE}/analysis.json"

report_ok() {
  python3 - "$MODE" "$REPORT_PATH" "$ANALYSIS_PATH" <<'PY'
import json
import sys
from pathlib import Path

mode = sys.argv[1]
path = Path(sys.argv[2])
analysis_path = Path(sys.argv[3])
try:
    data = json.loads(path.read_text(encoding="utf-8-sig"))
except Exception:
    raise SystemExit(1)

totals = data.get("totals") or {}

def analysis_ok(analysis):
    if not isinstance(analysis, dict):
        return False
    analysis_totals = analysis.get("totals") or {}
    cities = analysis.get("cities") or []
    if not cities:
        return False
    try:
        unresolved_total = any(
            int(analysis_totals.get(field, 0) or 0) > 0
            for field in ("failed", "failedCities", "unresolvedRaces", "unresolved")
        )
    except (TypeError, ValueError):
        return False
    requested = [str(city).strip().casefold() for city in (analysis.get("citiesRequested") or []) if str(city).strip()]
    reported = [str(city.get("city") or "").strip().casefold() for city in cities if str(city.get("city") or "").strip()]
    structure_ok = bool(requested) and sorted(requested) == sorted(reported)
    derived_races = 0
    derived_analyzed = 0
    for city in cities:
        city_status = str(city.get("status") or "").strip()
        races = city.get("races") or []
        if city_status not in {"ok", "no_races"}:
            structure_ok = False
            break
        if city_status == "no_races" and races:
            structure_ok = False
            break
        if city_status == "ok" and not races:
            structure_ok = False
            break
        if any(str(race.get("status") or "").strip() != "analyzed" for race in races):
            structure_ok = False
            break
        derived_races += len(races)
        derived_analyzed += len(races)
    for field, derived in (("cities", len(cities)), ("racesFound", derived_races), ("analyzed", derived_analyzed)):
        if field in analysis_totals:
            try:
                if int(analysis_totals.get(field, 0) or 0) != derived:
                    structure_ok = False
            except (TypeError, ValueError):
                structure_ok = False
    analyzed = int(analysis_totals.get("analyzed", 0) or 0)
    all_no_races = bool(cities) and all(
        str(city.get("status") or "").strip() == "no_races" for city in cities
    )
    return (
        analysis.get("mode") == "analyze"
        and analysis.get("status") == "completed"
        and not unresolved_total
        and structure_ok
        and (analyzed > 0 or all_no_races)
    )

if mode == "analyze":
    ok = analysis_ok(data)
else:
    try:
        analysis = json.loads(analysis_path.read_text(encoding="utf-8-sig"))
    except Exception:
        analysis = None
    checked = int(totals.get("checked", 0) or 0)
    submitted = int(totals.get("submitted", 0) or 0)
    no_races = (
        data.get("reason") == "analysis_manifest_no_races"
        and checked == 0
        and submitted == 0
        and isinstance(analysis, dict)
        and analysis_ok(analysis)
        and all(
            str(city.get("status") or "").strip() == "no_races"
            for city in (analysis.get("cities") or [])
        )
    )
    ok = (
        data.get("mode") == "results"
        and data.get("status") == "completed"
        and data.get("reason") != "analysis_manifest_incomplete"
        and data.get("analysisManifestComplete") is not False
        and analysis_ok(analysis)
        and (checked > 0 or no_races)
        and submitted == checked
        and int(totals.get("partialLabels", 0) or 0) == 0
        and int(totals.get("pending", 0) or 0) == 0
        and int(totals.get("failed", 0) or 0) == 0
    )
raise SystemExit(0 if ok else 1)
PY
}

cd "$ROOT_DIR"
mkdir -p "$DATA_DIR"

if report_ok; then
  echo "[ATISTIK] ${MODE} raporu zaten basarili: ${REPORT_PATH}"
  exit 0
fi

echo "[ATISTIK] ${MODE} raporu eksik veya basarisiz; Pi lokal retry basliyor: ${RUN_DATE}"
exec "${ROOT_DIR}/scripts/raspberry/run-automation.sh" "$MODE" "$RUN_DATE"
