#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-analyze}"
RUN_DATE="${2:-}"
ROOT_DIR="${ATISTIK_ROOT:-/opt/atistik/backend}"
DATA_DIR="${ATISTIK_ML_DATA_DIR:-${ROOT_DIR}/ml-data}"
LOG_DIR="${ATISTIK_LOG_DIR:-${ROOT_DIR}/logs}"
COMPOSE_FILE="${ATISTIK_COMPOSE_FILE:-docker-compose.raspberry.yml}"
BACKEND_URL="${ATISTIK_BACKEND_URL:-http://atistik-api:5000}"

mkdir -p "$DATA_DIR" "$LOG_DIR"
cd "$ROOT_DIR"

if [[ ! -f ".env.raspberry" ]]; then
  echo ".env.raspberry bulunamadi: $ROOT_DIR/.env.raspberry" >&2
  exit 2
fi

set -a
# shellcheck disable=SC1091
source .env.raspberry
set +a

if [[ -z "${ML_DATA_REPO:-}" || -z "${ML_DATA_TOKEN:-}" ]]; then
  echo "ML_DATA_REPO ve ML_DATA_TOKEN .env.raspberry icinde tanimli olmali." >&2
  exit 2
fi

GIT_AUTH_HEADER="AUTHORIZATION: basic $(printf 'x-access-token:%s' "$ML_DATA_TOKEN" | base64 | tr -d '\n')"
git_ml() {
  git -c "http.https://github.com/.extraheader=$GIT_AUTH_HEADER" "$@"
}

fix_data_permissions() {
  if [[ -d "$DATA_DIR/automation" ]]; then
    docker compose -f "$COMPOSE_FILE" run --rm atistik-worker \
      sh -c "chown -R $(id -u):$(id -g) /ml-data/automation" >/dev/null 2>&1 || true
  fi
}

trap fix_data_permissions EXIT

if [[ ! -d "$DATA_DIR/.git" ]]; then
  rm -rf "$DATA_DIR"
  git_ml clone "https://github.com/${ML_DATA_REPO}.git" "$DATA_DIR"
else
  fix_data_permissions
  git_ml -C "$DATA_DIR" fetch origin main
  git -C "$DATA_DIR" checkout main
  git_ml -C "$DATA_DIR" pull --rebase origin main
fi

docker compose -f "$COMPOSE_FILE" up -d atistik-api

echo "[ATISTIK] Syncing predictions.jsonl from GitHub backup before ${MODE}..."
docker compose -f "$COMPOSE_FILE" run --rm atistik-worker \
  curl -fsS -X POST "${BACKEND_URL}/api/ml-restore?force=true" >/dev/null

DATE_ARGS=()
if [[ -n "$RUN_DATE" ]]; then
  DATE_ARGS=(--date "$RUN_DATE")
fi

LOG_FILE="${LOG_DIR}/automation-${MODE}-$(TZ=Europe/Istanbul date +%Y%m%d-%H%M%S).log"

docker compose -f "$COMPOSE_FILE" run --rm atistik-worker \
  python automation/atistik_daily_job.py \
    --mode "$MODE" \
    "${DATE_ARGS[@]}" \
    --backend-url "$BACKEND_URL" \
    --data-dir /ml-data \
  | tee "$LOG_FILE"

fix_data_permissions

git -C "$DATA_DIR" config user.name "atistik-raspberry"
git -C "$DATA_DIR" config user.email "atistik-raspberry@users.noreply.github.com"

if ! git -C "$DATA_DIR" diff --quiet -- automation || [[ -n "$(git -C "$DATA_DIR" ls-files --others --exclude-standard automation)" ]]; then
  git -C "$DATA_DIR" add automation
  git -C "$DATA_DIR" commit -m "Atistik raspberry ${MODE} $(TZ=Europe/Istanbul date +%Y-%m-%d)"
  for attempt in 1 2 3 4 5; do
    if git_ml -C "$DATA_DIR" push; then
      exit 0
    fi
    if [[ "$attempt" == "5" ]]; then
      echo "ML-data push 5 denemeden sonra basarisiz." >&2
      exit 1
    fi
    git_ml -C "$DATA_DIR" pull --rebase origin main
    sleep 5
  done
else
  echo "ML-data automation degisikligi yok."
fi
