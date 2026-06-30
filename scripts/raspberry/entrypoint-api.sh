#!/usr/bin/env bash
set -euo pipefail

STATE_PREDICTIONS="${ATISTIK_PREDICTIONS_PATH:-/state/predictions.jsonl}"
APP_PREDICTIONS="/app/predictions.jsonl"

mkdir -p "$(dirname "$STATE_PREDICTIONS")"
touch "$STATE_PREDICTIONS"

if [[ -e "$APP_PREDICTIONS" && ! -L "$APP_PREDICTIONS" ]]; then
  cp "$APP_PREDICTIONS" "${STATE_PREDICTIONS}.image-seed" || true
  rm -f "$APP_PREDICTIONS"
fi

ln -sfn "$STATE_PREDICTIONS" "$APP_PREDICTIONS"

exec "$@"
