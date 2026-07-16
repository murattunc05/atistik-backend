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

report_ok() {
  python3 - "$MODE" "$REPORT_PATH" <<'PY'
import json
import sys
from pathlib import Path

mode = sys.argv[1]
path = Path(sys.argv[2])
try:
    data = json.loads(path.read_text(encoding="utf-8-sig"))
except Exception:
    raise SystemExit(1)

totals = data.get("totals") or {}
if mode == "analyze":
    ok = (
        data.get("mode") == "analyze"
        and int(totals.get("analyzed", 0) or 0) > 0
        and int(totals.get("failed", 0) or 0) == 0
    )
else:
    checked = int(totals.get("checked", 0) or 0)
    submitted = int(totals.get("submitted", 0) or 0)
    ok = (
        data.get("mode") == "results"
        and data.get("status") == "completed"
        and checked > 0
        and submitted == checked
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
