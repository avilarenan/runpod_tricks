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

exit 0
