#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE="${WORKSPACE:-$(cd "$SCRIPT_DIR/.." && pwd)}"

log() {
  echo "[fresh_install] $*"
}

warn() {
  echo "[fresh_install][WARN] $*" >&2
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "[fresh_install][ERROR] Missing required command: $1" >&2
    exit 1
  fi
}

infer_repo_url() {
  local repo_name="$1"
  local origin
  origin="$(git -C "$SCRIPT_DIR" remote get-url origin 2>/dev/null || true)"
  if [[ -z "$origin" ]]; then
    echo ""
    return
  fi
  if [[ "$origin" =~ ^https?://github.com/([^/]+)/[^/]+(\.git)?$ ]]; then
    echo "https://github.com/${BASH_REMATCH[1]}/${repo_name}.git"
    return
  fi
  if [[ "$origin" =~ ^git@github.com:([^/]+)/[^/]+(\.git)?$ ]]; then
    echo "git@github.com:${BASH_REMATCH[1]}/${repo_name}.git"
    return
  fi
  echo ""
}

ensure_git_config() {
  local name="${GIT_NAME:-}"
  local email="${GIT_EMAIL:-}"
  if [[ -n "$name" ]]; then
    git config --global user.name "$name"
  fi
  if [[ -n "$email" ]]; then
    git config --global user.email "$email"
  fi
  if ! git config --global user.name >/dev/null; then
    warn "GIT_NAME not set; git user.name not configured."
  fi
  if ! git config --global user.email >/dev/null; then
    warn "GIT_EMAIL not set; git user.email not configured."
  fi
}

clone_or_update() {
  local repo_url="$1"
  local dest="$2"
  if [[ -d "$dest/.git" ]]; then
    log "Updating $dest"
    git -C "$dest" pull --ff-only || true
    return
  fi
  if [[ -z "$repo_url" ]]; then
    echo "[fresh_install][ERROR] Missing repo URL for $dest" >&2
    exit 1
  fi
  log "Cloning $repo_url -> $dest"
  git clone "$repo_url" "$dest"
}

write_r2_config() {
  local target="$1"
  local scope="${2:-AF}"
  local include_secrets="${3:-0}"
  local account_id=""
  local bucket=""
  local endpoint=""
  local access_key=""
  local secret_key=""
  local token=""
  local prefix_raw_csv=""
  local prefix_raw_parquet=""
  local prefix_morph_csv=""
  local prefix_morph_parquet=""
  local prefix_predictions=""
  local prefix_predictions_meta=""
  local parquet_compression=""

  if [[ "$scope" == "AM" ]]; then
    account_id="${AM_R2_ACCOUNT_ID:-${AF_R2_ACCOUNT_ID:-${R2_ACCOUNT_ID:-}}}"
    bucket="${AM_R2_BUCKET:-${AF_R2_BUCKET:-${R2_BUCKET:-}}}"
    endpoint="${AM_R2_ENDPOINT:-${AF_R2_ENDPOINT:-${R2_ENDPOINT:-}}}"
    access_key="${AM_R2_ACCESS_KEY:-${AF_R2_ACCESS_KEY:-${R2_ACCESS_KEY:-}}}"
    secret_key="${AM_R2_SECRET_KEY:-${AF_R2_SECRET_KEY:-${R2_SECRET_KEY:-}}}"
    token="${AM_R2_TOKEN:-${AF_R2_TOKEN:-${R2_TOKEN:-}}}"
    prefix_raw_csv="${AM_R2_PREFIX_RAW_CSV:-${AF_R2_PREFIX_RAW_CSV:-${R2_PREFIX_RAW_CSV:-}}}"
    prefix_raw_parquet="${AM_R2_PREFIX_RAW_PARQUET:-${AF_R2_PREFIX_RAW_PARQUET:-${R2_PREFIX_RAW_PARQUET:-}}}"
    prefix_morph_csv="${AM_R2_PREFIX_MORPH_CSV:-${AF_R2_PREFIX_MORPH_CSV:-${R2_PREFIX_MORPH_CSV:-}}}"
    prefix_morph_parquet="${AM_R2_PREFIX_MORPH_PARQUET:-${AF_R2_PREFIX_MORPH_PARQUET:-${R2_PREFIX_MORPH_PARQUET:-}}}"
    prefix_predictions="${AM_R2_PREFIX_PREDICTIONS:-${AF_R2_PREFIX_PREDICTIONS:-${R2_PREFIX_PREDICTIONS:-}}}"
    prefix_predictions_meta="${AM_R2_PREFIX_PREDICTIONS_META:-${AF_R2_PREFIX_PREDICTIONS_META:-${R2_PREFIX_PREDICTIONS_META:-}}}"
    parquet_compression="${AM_R2_PARQUET_COMPRESSION:-${AF_R2_PARQUET_COMPRESSION:-${R2_PARQUET_COMPRESSION:-}}}"
  else
    account_id="${AF_R2_ACCOUNT_ID:-${R2_ACCOUNT_ID:-}}"
    bucket="${AF_R2_BUCKET:-${R2_BUCKET:-}}"
    endpoint="${AF_R2_ENDPOINT:-${R2_ENDPOINT:-}}"
    access_key="${AF_R2_ACCESS_KEY:-${R2_ACCESS_KEY:-}}"
    secret_key="${AF_R2_SECRET_KEY:-${R2_SECRET_KEY:-}}"
    token="${AF_R2_TOKEN:-${R2_TOKEN:-}}"
    prefix_raw_csv="${AF_R2_PREFIX_RAW_CSV:-${R2_PREFIX_RAW_CSV:-}}"
    prefix_raw_parquet="${AF_R2_PREFIX_RAW_PARQUET:-${R2_PREFIX_RAW_PARQUET:-}}"
    prefix_morph_csv="${AF_R2_PREFIX_MORPH_CSV:-${R2_PREFIX_MORPH_CSV:-}}"
    prefix_morph_parquet="${AF_R2_PREFIX_MORPH_PARQUET:-${R2_PREFIX_MORPH_PARQUET:-}}"
    prefix_predictions="${AF_R2_PREFIX_PREDICTIONS:-${R2_PREFIX_PREDICTIONS:-}}"
    prefix_predictions_meta="${AF_R2_PREFIX_PREDICTIONS_META:-${R2_PREFIX_PREDICTIONS_META:-}}"
    parquet_compression="${AF_R2_PARQUET_COMPRESSION:-${R2_PARQUET_COMPRESSION:-}}"
  fi

  if [[ -z "$account_id" || -z "$bucket" ]]; then
    warn "R2 identifiers missing for scope ${scope}; set ${scope}_R2_ACCOUNT_ID/${scope}_R2_BUCKET."
    return
  fi
  if [[ "$include_secrets" == "1" || "$include_secrets" == "true" ]]; then
    if [[ -z "$access_key" || -z "$secret_key" ]]; then
      warn "R2 secrets missing for scope ${scope}; set ${scope}_R2_ACCESS_KEY/${scope}_R2_SECRET_KEY."
      return
    fi
  fi

  python3 - <<PY
import json
from pathlib import Path

include_secrets = "${include_secrets}".lower() in {"1", "true", "yes", "on"}
payload = {
  "account_id": "${account_id}",
  "bucket": "${bucket}",
  "endpoint": "${endpoint}",
  "prefix_raw_csv": "${prefix_raw_csv:-datasets/raw/csv}",
  "prefix_raw_parquet": "${prefix_raw_parquet:-datasets/raw/parquet}",
  "prefix_morph_csv": "${prefix_morph_csv:-datasets/morphed/csv}",
  "prefix_morph_parquet": "${prefix_morph_parquet:-datasets/morphed/parquet}",
  "prefix_predictions": "${prefix_predictions:-predictions}",
  "prefix_predictions_meta": "${prefix_predictions_meta:-}",
  "parquet_compression": "${parquet_compression:-zstd}",
}
if include_secrets:
  payload["access_key"] = "${access_key}"
  payload["secret_key"] = "${secret_key}"
  payload["token"] = "${token}"
path = Path("${target}")
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(payload, indent=2))
print(f"Wrote R2 config to {path}")
PY
}

write_db_config() {
  local target="$1"
  local db_url="${AF_DB_URL:-${POSTGRES_URL:-}}"
  if [[ -z "$db_url" ]]; then
    warn "AF_DB_URL not set; skipping DB config."
    return
  fi
  python3 - <<PY
import json
from pathlib import Path

path = Path("${target}")
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps({"db_url": "${db_url}"}, indent=2))
print(f"Wrote DB config to {path}")
PY
}

ensure_venv() {
  local repo="$1"
  local venv_path="$repo/.venv"
  if [[ ! -d "$venv_path" ]]; then
    log "Creating venv in $repo"
    python3 -m venv "$venv_path"
  fi
  log "Installing requirements for $repo"
  mkdir -p "$WORKSPACE/.cache/pip"
  PIP_CACHE_DIR="$WORKSPACE/.cache/pip" "$venv_path/bin/pip3" install -r "$repo/requirements.txt"
}

configure_watchdog() {
  local cfg_path="$WORKSPACE/runpod_tricks/runpod_config.json"
  local api_key="${RUNPOD_API_KEY:-${RunPodAPIKEY:-}}"
  if [[ -z "$api_key" ]]; then
    warn "RUNPOD_API_KEY not set; watchdog will not be enabled."
    return
  fi
  python3 - <<PY
import json
from pathlib import Path

path = Path("${cfg_path}")
data = {}
if path.exists():
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError:
        data = {}

data["api_key"] = "${api_key}"
data["enabled"] = "${WATCHDOG_ENABLED:-true}".lower() in {"1","true","yes","on"}
data["idle_enabled"] = "${WATCHDOG_IDLE_ENABLED:-true}".lower() in {"1","true","yes","on"}
data["queue_empty_enabled"] = "${WATCHDOG_QUEUE_EMPTY_ENABLED:-true}".lower() in {"1","true","yes","on"}
data["terminate_on_empty_queue"] = "${WATCHDOG_TERMINATE_ON_EMPTY_QUEUE:-true}".lower() in {"1","true","yes","on"}
data.setdefault("idle_seconds", int("${WATCHDOG_IDLE_SECONDS:-600}"))
data.setdefault("poll_seconds", int("${WATCHDOG_POLL_SECONDS:-60}"))
data.setdefault("gpu_util_threshold", int("${WATCHDOG_GPU_UTIL_THRESHOLD:-5}"))
data.setdefault("gpu_mem_fraction_threshold", float("${WATCHDOG_GPU_MEM_THRESHOLD:-0.05}"))
data.setdefault("terminate_mode", "${WATCHDOG_TERMINATE_MODE:-terminate}")

path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(data, indent=2, sort_keys=True))
print(f"Wrote watchdog config to {path}")
PY

  local py_bin="$WORKSPACE/AlphaForecasting/.venv/bin/python"
  if [[ ! -x "$py_bin" ]]; then
    py_bin="python3"
  fi
  if ! pgrep -f "pod_watchdog.py" >/dev/null 2>&1; then
    log "Starting watchdog process"
    nohup "$py_bin" "$WORKSPACE/runpod_tricks/pod_watchdog.py" > "$WORKSPACE/runpod_tricks/runpod_watchdog.log" 2>&1 &
  fi
}

require_cmd git
require_cmd python3

log "Configuring git"
ensure_git_config

AF_REPO_URL="${AF_REPO_URL:-$(infer_repo_url AlphaForecasting)}"
AM_REPO_URL="${AM_REPO_URL:-$(infer_repo_url AlphaMorphing)}"

clone_or_update "$AF_REPO_URL" "$WORKSPACE/AlphaForecasting"
clone_or_update "$AM_REPO_URL" "$WORKSPACE/AlphaMorphing"

write_r2_config "$WORKSPACE/AlphaForecasting/config/r2_public.json" "AF" "0"
write_r2_config "$WORKSPACE/AlphaMorphing/config/r2_public.json" "AM" "0"
if [[ "${WRITE_R2_SECRET_CONFIG:-}" == "1" || "${AF_WRITE_R2_SECRET_CONFIG:-}" == "1" ]]; then
  write_r2_config "$WORKSPACE/AlphaForecasting/.secrets/r2.json" "AF" "1"
  write_r2_config "$WORKSPACE/AlphaMorphing/.secrets/r2.json" "AM" "1"
else
  warn "Skipping R2 secrets file write; set WRITE_R2_SECRET_CONFIG=1 to persist secrets."
fi

if [[ "${WRITE_DB_CONFIG:-}" == "1" || "${AF_WRITE_DB_CONFIG:-}" == "1" ]]; then
  write_db_config "$WORKSPACE/AlphaForecasting/runs/db_config.json"
else
  warn "Skipping DB config file write; set WRITE_DB_CONFIG=1 to persist AF_DB_URL."
fi

ensure_venv "$WORKSPACE/AlphaForecasting"
ensure_venv "$WORKSPACE/AlphaMorphing"

configure_watchdog

log "Fresh install complete."
