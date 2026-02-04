#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_DIR="${CLUSTER_DIR:-"$ROOT_DIR/.cluster"}"
PIDS_FILE="$CLUSTER_DIR/pids"

# Also kill anything still listening on the default ports. This protects against
# stale clusters when the pids file was truncated/overwritten.
REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
GRPC_HOST="${GRPC_HOST:-127.0.0.1}"

NODE1_REDIS_PORT="${NODE1_REDIS_PORT:-16379}"
NODE2_REDIS_PORT="${NODE2_REDIS_PORT:-16380}"
NODE3_REDIS_PORT="${NODE3_REDIS_PORT:-16381}"

NODE1_GRPC_PORT="${NODE1_GRPC_PORT:-15051}"
NODE2_GRPC_PORT="${NODE2_GRPC_PORT:-15052}"
NODE3_GRPC_PORT="${NODE3_GRPC_PORT:-15053}"

if [[ -f "$PIDS_FILE" ]]; then
  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    kill "$pid" 2>/dev/null || true
  done <"$PIDS_FILE"
fi

kill_listeners_on_port() {
  local port="$1"

  if ! command -v lsof >/dev/null 2>&1; then
    return 0
  fi

  local pids
  pids="$(lsof -nP -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -z "$pids" ]]; then
    return 0
  fi

  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    kill "$pid" 2>/dev/null || true
  done <<<"$pids"
}

# Redis listeners
kill_listeners_on_port "$NODE1_REDIS_PORT"
kill_listeners_on_port "$NODE2_REDIS_PORT"
kill_listeners_on_port "$NODE3_REDIS_PORT"

# gRPC listeners
kill_listeners_on_port "$NODE1_GRPC_PORT"
kill_listeners_on_port "$NODE2_GRPC_PORT"
kill_listeners_on_port "$NODE3_GRPC_PORT"

rm -rf "$CLUSTER_DIR"
echo "cluster cleaned up ($CLUSTER_DIR removed)"
