#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "[data] Stopping existing containers..."
docker compose down

echo "[data] Rebuilding images..."
docker compose build

echo "[data] Starting containers in detached mode..."
docker compose up -d
