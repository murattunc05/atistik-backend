#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${ATISTIK_ROOT:-/opt/atistik/backend}"
DATA_DIR="${ATISTIK_ML_DATA_DIR:-${ROOT_DIR}/ml-data}"
STATE_DIR="${ATISTIK_STATE_DIR:-${ROOT_DIR}/state}"
STATE_PREDICTIONS="${ATISTIK_PREDICTIONS_HOST_PATH:-${STATE_DIR}/predictions.jsonl}"
BACKEND_URL="${ATISTIK_HOST_BACKEND_URL:-http://127.0.0.1:5000}"
RUN_DATE="${1:-$(TZ=Europe/Istanbul date +%Y-%m-%d)}"
ANALYSIS="${DATA_DIR}/automation/runs/${RUN_DATE}/analysis.json"
SHADOW_DIR="${STATE_DIR}/late-market-agf"
TEMP_DATA_DIR="${SHADOW_DIR}/probe-output"

if [[ ! -s "$ANALYSIS" ]]; then
  echo "[LATE MARKET AGF] Sabah analysis manifesti yok: $ANALYSIS" >&2
  exit 4
fi
if [[ ! -s "$STATE_PREDICTIONS" ]]; then
  echo "[LATE MARKET AGF] State predictions yok: $STATE_PREDICTIONS" >&2
  exit 5
fi

mkdir -p "$SHADOW_DIR" "$TEMP_DATA_DIR"

# Collector is read-only with respect to production predictions and writes its
# append-only evidence only under /opt/atistik/backend/state.
exec /usr/bin/python3 "${ROOT_DIR}/automation/late_market_agf_shadow.py" \
  --analysis "$ANALYSIS" \
  --predictions "$STATE_PREDICTIONS" \
  --data-dir "$TEMP_DATA_DIR" \
  --backend-url "$BACKEND_URL" \
  --run-date "$RUN_DATE"
