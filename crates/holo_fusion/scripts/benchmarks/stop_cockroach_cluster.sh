#!/usr/bin/env bash
set -euo pipefail

SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPTS_DIR/../../../../" && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-"$SCRIPTS_DIR/docker-compose.cockroach.yml"}"
COCKROACH_DATA_DIR="${COCKROACH_DATA_DIR:-"$ROOT_DIR/.cluster/holo_fusion/cockroach"}"

if ! command -v docker >/dev/null 2>&1; then
  echo "error: docker is required" >&2
  exit 1
fi

if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  echo "error: docker compose is required (docker compose or docker-compose)" >&2
  exit 1
fi

COCKROACH_DATA_DIR="$COCKROACH_DATA_DIR" "${COMPOSE[@]}" -f "$COMPOSE_FILE" down -v --remove-orphans
rm -rf "$COCKROACH_DATA_DIR"
echo "cockroach cluster stopped and data dir removed: $COCKROACH_DATA_DIR"
