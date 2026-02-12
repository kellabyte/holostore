#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CLUSTER_DIR="${CLUSTER_DIR:-"$ROOT_DIR/.cluster"}"
HF_CLUSTER_DIR="${CLUSTER_DIR}/holo_fusion"
PIDS_FILE="${HF_CLUSTER_DIR}/pids"

NODE1_PG_PORT="${NODE1_PG_PORT:-55432}"
NODE2_PG_PORT="${NODE2_PG_PORT:-55433}"
NODE3_PG_PORT="${NODE3_PG_PORT:-55434}"

NODE1_HEALTH_PORT="${NODE1_HEALTH_PORT:-18081}"
NODE2_HEALTH_PORT="${NODE2_HEALTH_PORT:-18082}"
NODE3_HEALTH_PORT="${NODE3_HEALTH_PORT:-18083}"

NODE1_REDIS_PORT="${NODE1_REDIS_PORT:-16379}"
NODE2_REDIS_PORT="${NODE2_REDIS_PORT:-16380}"
NODE3_REDIS_PORT="${NODE3_REDIS_PORT:-16381}"

NODE1_GRPC_PORT="${NODE1_GRPC_PORT:-15051}"
NODE2_GRPC_PORT="${NODE2_GRPC_PORT:-15052}"
NODE3_GRPC_PORT="${NODE3_GRPC_PORT:-15053}"

kill_pid_file_processes() {
  [[ -f "$PIDS_FILE" ]] || return 0

  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    kill "$pid" >/dev/null 2>&1 || true
  done <"$PIDS_FILE"

  sleep 1

  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
  done <"$PIDS_FILE"
}

kill_named_holo_fusion_processes() {
  if ! command -v pgrep >/dev/null 2>&1; then
    return 0
  fi

  local pids
  pids="$(pgrep -f '(^|/)(holo-fusion)( |$)' || true)"
  [[ -z "$pids" ]] && return 0

  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    kill "$pid" >/dev/null 2>&1 || true
  done <<<"$pids"

  sleep 1

  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
  done <<<"$pids"
}

kill_listeners_on_port() {
  local port="$1"

  if ! command -v lsof >/dev/null 2>&1; then
    return 0
  fi

  local pids
  pids="$(lsof -nP -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
  [[ -z "$pids" ]] && return 0

  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    kill "$pid" >/dev/null 2>&1 || true
  done <<<"$pids"
}

kill_pid_file_processes
kill_named_holo_fusion_processes

# Best-effort listener cleanup on the default 3-node ports.
kill_listeners_on_port "$NODE1_PG_PORT"
kill_listeners_on_port "$NODE2_PG_PORT"
kill_listeners_on_port "$NODE3_PG_PORT"

kill_listeners_on_port "$NODE1_HEALTH_PORT"
kill_listeners_on_port "$NODE2_HEALTH_PORT"
kill_listeners_on_port "$NODE3_HEALTH_PORT"

kill_listeners_on_port "$NODE1_REDIS_PORT"
kill_listeners_on_port "$NODE2_REDIS_PORT"
kill_listeners_on_port "$NODE3_REDIS_PORT"

kill_listeners_on_port "$NODE1_GRPC_PORT"
kill_listeners_on_port "$NODE2_GRPC_PORT"
kill_listeners_on_port "$NODE3_GRPC_PORT"

rm -rf "$HF_CLUSTER_DIR"
echo "holo_fusion cluster cleaned up ($HF_CLUSTER_DIR removed)"
