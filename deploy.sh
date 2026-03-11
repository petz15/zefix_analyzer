#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/zefix_analyzer"
BRANCH="main"

cd "$APP_DIR"

echo "==> Updating repo"
git fetch origin
git checkout "$BRANCH"
git pull --ff-only origin "$BRANCH"

echo "==> Building and restarting containers"
docker compose up -d --build --remove-orphans

echo "==> Current status"
docker compose ps
