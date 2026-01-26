#!/usr/bin/env bash
set -e

# Persistent Codex home
export CODEX_HOME=/workspace/.codex
mkdir -p "$CODEX_HOME"

# Make sure Codex still lands there even if CODEX_HOME isn't seen
rm -rf "$HOME/.codex" 2>/dev/null || true
ln -sfn "$CODEX_HOME" "$HOME/.codex" || true

# Cache dirs live on the persistent workspace volume
export XDG_CACHE_HOME=/workspace/.cache
export HF_HOME=/workspace/.cache/huggingface
export HUGGINGFACE_HUB_CACHE="$HF_HOME/hub"
export HF_HUB_CACHE="$HF_HOME/hub"
export HF_DATASETS_CACHE="$HF_HOME/datasets"
export TRANSFORMERS_CACHE="$HF_HOME/transformers"
export TORCH_HOME=/workspace/.cache/torch

mkdir -p "$XDG_CACHE_HOME" "$HF_HOME" "$HUGGINGFACE_HUB_CACHE" "$HF_DATASETS_CACHE" "$TRANSFORMERS_CACHE" "$TORCH_HOME"
mkdir -p "$HOME/.cache"
rm -rf "$HOME/.cache/huggingface" 2>/dev/null || true
ln -sfn "$HF_HOME" "$HOME/.cache/huggingface" || true

git config --global user.name "Renan"
git config --global user.email "renandeluca01@gmail.com"

if ! command -v nvtop >/dev/null 2>&1; then
  apt-get update
  DEBIAN_FRONTEND=noninteractive apt-get install -y nvtop
fi

WATCHDOG="/workspace/runpod_tricks/pod_watchdog.py"
WATCHDOG_CONFIG="/workspace/runpod_tricks/runpod_config.json"
WATCHDOG_LOG="/workspace/runpod_tricks/runpod_watchdog.log"
if [ -f "$WATCHDOG" ] && [ -f "$WATCHDOG_CONFIG" ]; then
  python3 - <<'PY'
import json
from pathlib import Path

path = Path("/workspace/runpod_tricks/runpod_config.json")
try:
    data = json.loads(path.read_text())
except (FileNotFoundError, json.JSONDecodeError):
    data = {}

data["enabled"] = False
data.setdefault("idle_enabled", False)
data.setdefault("queue_empty_enabled", False)
path.write_text(json.dumps(data, indent=2, sort_keys=True))
PY
  if ! pgrep -f "pod_watchdog.py" >/dev/null 2>&1; then
    PY_BIN="/workspace/AlphaForecasting/.venv/bin/python"
    if [ ! -x "$PY_BIN" ]; then
      PY_BIN="python3"
    fi
    nohup "$PY_BIN" "$WATCHDOG" > "$WATCHDOG_LOG" 2>&1 &
  fi
fi

exit 0
