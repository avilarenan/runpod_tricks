#!/usr/bin/env bash
set -e

# Persistent Codex home
export CODEX_HOME=/workspace/.codex
mkdir -p "$CODEX_HOME"

# Make sure Codex still lands there even if CODEX_HOME isn't seen
rm -rf "$HOME/.codex" 2>/dev/null || true
ln -sfn "$CODEX_HOME" "$HOME/.codex" || true

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
  if ! pgrep -f "pod_watchdog.py" >/dev/null 2>&1; then
    PY_BIN="/workspace/AlphaForecasting/.venv/bin/python"
    if [ ! -x "$PY_BIN" ]; then
      PY_BIN="python3"
    fi
    nohup "$PY_BIN" "$WATCHDOG" > "$WATCHDOG_LOG" 2>&1 &
  fi
fi

exit 0
