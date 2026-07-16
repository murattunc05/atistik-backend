#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-analyze}"
RUN_DATE="${2:-}"
ROOT_DIR="${ATISTIK_ROOT:-/opt/atistik/backend}"
DATA_DIR="${ATISTIK_ML_DATA_DIR:-${ROOT_DIR}/ml-data}"
LOG_DIR="${ATISTIK_LOG_DIR:-${ROOT_DIR}/logs}"
STATE_PREDICTIONS="${ATISTIK_PREDICTIONS_HOST_PATH:-${ROOT_DIR}/state/predictions.jsonl}"
COMPOSE_FILE="${ATISTIK_COMPOSE_FILE:-docker-compose.raspberry.yml}"
BACKEND_URL="${ATISTIK_BACKEND_URL:-http://atistik-api:5000}"
IMAGE_NAME="${ATISTIK_IMAGE_NAME:-atistik-api:raspberry}"
RENDER_BACKEND_URL="${ATISTIK_RENDER_BACKEND_URL:-https://atistik-backend.onrender.com}"
RENDER_RESTORE_MAX_ATTEMPTS="${ATISTIK_RENDER_RESTORE_MAX_ATTEMPTS:-6}"
RENDER_RESTORE_SLEEP_SECONDS="${ATISTIK_RENDER_RESTORE_SLEEP_SECONDS:-10}"

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

current_git_revision() {
  git rev-parse --short=12 HEAD 2>/dev/null || echo "unknown"
}

image_revision() {
  docker image inspect "$IMAGE_NAME" \
    --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' 2>/dev/null || true
}

backend_prediction_stats() {
  local base_url="$1"
  local status_json
  status_json="$(curl -fsS "${base_url%/}/api/ml-status" 2>/dev/null)" || return 1
  python3 -c 'import json, sys
try:
    predictions = json.load(sys.stdin).get("predictions") or {}
    total = predictions.get("valid_json_lines", predictions.get("lines", 0))
    labeled = predictions.get("labeled_lines", 0)
    print(f"{int(total)}\t{int(labeled)}")
except Exception:
    print("")' \
    <<<"$status_json" 2>/dev/null
}

prediction_file_lines() {
  local path="$1"
  python3 -c 'import json, sys
count = 0
with open(sys.argv[1], "r", encoding="utf-8") as handle:
    for line in handle:
        if not line.strip():
            continue
        json.loads(line)
        count += 1
print(count)' "$path"
}

persist_state_predictions() {
  if [[ ! -s "$STATE_PREDICTIONS" ]]; then
    echo "[ATISTIK] State predictions bulunamadi veya bos: $STATE_PREDICTIONS" >&2
    return 1
  fi

  local state_lines
  local repo_lines=0
  state_lines="$(prediction_file_lines "$STATE_PREDICTIONS")" || {
    echo "[ATISTIK] State predictions JSONL dogrulamasi basarisiz." >&2
    return 1
  }
  if [[ -f "$DATA_DIR/predictions.jsonl" ]]; then
    repo_lines="$(prediction_file_lines "$DATA_DIR/predictions.jsonl")" || {
      echo "[ATISTIK] Repo predictions JSONL dogrulamasi basarisiz." >&2
      return 1
    }
  fi
  if ((state_lines < repo_lines)); then
    echo "[ATISTIK] State predictions gerilemis: ${state_lines}/${repo_lines}; backup yapilmadi." >&2
    return 1
  fi

  cp "$STATE_PREDICTIONS" "$DATA_DIR/predictions.jsonl"
  echo "[ATISTIK] State predictions ML-data repo'ya alindi: ${state_lines} satir."
}

restore_render_from_backup() {
  if [[ -z "$RENDER_BACKEND_URL" ]]; then
    echo "[ATISTIK] Render restore atlandi: ATISTIK_RENDER_BACKEND_URL bos."
    return 0
  fi

  local expected_stats
  local expected_total
  local expected_labeled
  expected_stats="$(backend_prediction_stats "$BACKEND_URL" || true)"
  read -r expected_total expected_labeled <<<"$expected_stats"
  if [[ -z "$expected_total" || -z "$expected_labeled" ]]; then
    echo "[ATISTIK] Pi local prediction sayaclari okunamadi; Render restore dogrulanamiyor." >&2
    return 1
  fi

  echo "[ATISTIK] Render predictions restore basliyor; hedef total=${expected_total}, labeled=${expected_labeled}."

  local attempt
  local actual_stats
  local actual_total
  local actual_labeled
  for ((attempt = 1; attempt <= RENDER_RESTORE_MAX_ATTEMPTS; attempt++)); do
    if curl -fsS -X POST "${RENDER_BACKEND_URL%/}/api/ml-restore?force=true" >/dev/null; then
      actual_stats="$(backend_prediction_stats "$RENDER_BACKEND_URL" || true)"
      read -r actual_total actual_labeled <<<"$actual_stats"
      if [[ -n "$actual_total" && -n "$actual_labeled" && "$actual_total" -ge "$expected_total" && "$actual_labeled" -ge "$expected_labeled" ]]; then
        echo "[ATISTIK] Render restore tamam: total=${actual_total}, labeled=${actual_labeled}."
        return 0
      fi
      echo "[ATISTIK] Render restore bekleniyor: total=${actual_total:-unknown}/${expected_total}, labeled=${actual_labeled:-unknown}/${expected_labeled} (attempt ${attempt}/${RENDER_RESTORE_MAX_ATTEMPTS})."
    else
      echo "[ATISTIK] Render restore istegi basarisiz (attempt ${attempt}/${RENDER_RESTORE_MAX_ATTEMPTS})." >&2
    fi

    if [[ "$attempt" -lt "$RENDER_RESTORE_MAX_ATTEMPTS" ]]; then
      sleep "$RENDER_RESTORE_SLEEP_SECONDS"
    fi
  done

  echo "[ATISTIK] Render restore dogrulanamadi; canli backend GitHub backup gerisinde kalabilir." >&2
  return 1
}

ensure_image_current() {
  local current_rev
  local built_rev
  current_rev="$(current_git_revision)"
  built_rev="$(image_revision)"

  if [[ "$current_rev" == "unknown" ]]; then
    echo "[ATISTIK] Git revision okunamadi; mevcut Docker image kullanilacak."
    return 0
  fi

  if [[ "$built_rev" != "$current_rev" ]]; then
    echo "[ATISTIK] Docker image eski (${built_rev:-none}); ${current_rev} icin rebuild ediliyor..."
    docker compose -f "$COMPOSE_FILE" build \
      --build-arg "ATISTIK_IMAGE_REVISION=$current_rev" \
      atistik-api
  fi
}

fix_data_permissions() {
  if [[ -d "$DATA_DIR/automation" ]]; then
    docker compose -f "$COMPOSE_FILE" run --rm atistik-worker \
      sh -c "chown -R $(id -u):$(id -g) /ml-data/automation" >/dev/null 2>&1 || true
  fi
}

sync_ml_data() {
  fix_data_permissions
  git_ml -C "$DATA_DIR" fetch origin main
  git -C "$DATA_DIR" checkout main
  if git_ml -C "$DATA_DIR" pull --rebase origin main; then
    return 0
  fi

  echo "[ATISTIK] ML-data rebase conflict; local state backup branch'e alinip origin/main'e hizalaniyor..." >&2
  git -C "$DATA_DIR" rebase --abort >/dev/null 2>&1 || true
  local backup_branch="backup/raspberry-sync-$(TZ=Europe/Istanbul date +%Y%m%d-%H%M%S)"
  git -C "$DATA_DIR" branch "$backup_branch" HEAD >/dev/null 2>&1 || true
  git -C "$DATA_DIR" reset --hard origin/main
}

trap fix_data_permissions EXIT

if [[ ! -d "$DATA_DIR/.git" ]]; then
  rm -rf "$DATA_DIR"
  git_ml clone "https://github.com/${ML_DATA_REPO}.git" "$DATA_DIR"
else
  sync_ml_data
fi

ensure_image_current
docker compose -f "$COMPOSE_FILE" up -d atistik-api

echo "[ATISTIK] Syncing predictions.jsonl from GitHub backup before ${MODE}..."
docker compose -f "$COMPOSE_FILE" run --rm atistik-worker \
  curl -fsS -X POST "${BACKEND_URL}/api/ml-restore?force=true" >/dev/null

DATE_ARGS=()
if [[ -n "$RUN_DATE" ]]; then
  DATE_ARGS=(--date "$RUN_DATE")
fi

LOG_FILE="${LOG_DIR}/automation-${MODE}-$(TZ=Europe/Istanbul date +%Y%m%d-%H%M%S).log"

automation_status=0
set +e
docker compose -f "$COMPOSE_FILE" run --rm atistik-worker \
  python automation/atistik_daily_job.py \
    --mode "$MODE" \
    "${DATE_ARGS[@]}" \
    --backend-url "$BACKEND_URL" \
    --data-dir /ml-data \
  | tee "$LOG_FILE"
automation_status=${PIPESTATUS[0]}
set -e

fix_data_permissions

persist_state_predictions

git -C "$DATA_DIR" config user.name "atistik-raspberry"
git -C "$DATA_DIR" config user.email "atistik-raspberry@users.noreply.github.com"

if ! git -C "$DATA_DIR" diff --quiet -- automation predictions.jsonl || [[ -n "$(git -C "$DATA_DIR" ls-files --others --exclude-standard automation)" ]]; then
  git -C "$DATA_DIR" add automation predictions.jsonl
  git -C "$DATA_DIR" commit -m "Atistik raspberry ${MODE} $(TZ=Europe/Istanbul date +%Y-%m-%d)"
  pushed=0
  for attempt in 1 2 3 4 5; do
    if git_ml -C "$DATA_DIR" push; then
      pushed=1
      break
    fi
    if [[ "$attempt" == "5" ]]; then
      echo "ML-data push 5 denemeden sonra basarisiz." >&2
      exit 1
    fi
    git_ml -C "$DATA_DIR" pull --rebase origin main
    sleep 5
  done
  if [[ "$pushed" != "1" ]]; then
    exit 1
  fi
else
  echo "ML-data automation degisikligi yok."
fi

restore_render_from_backup

if [[ "$automation_status" -ne 0 ]]; then
  echo "[ATISTIK] Automation ${MODE} hata kodu ile bitti: ${automation_status}" >&2
  exit "$automation_status"
fi
