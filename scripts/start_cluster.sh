#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_DIR="${CLUSTER_DIR:-"$ROOT_DIR/.cluster"}"

REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
GRPC_HOST="${GRPC_HOST:-127.0.0.1}"

# Default to non-standard ports to avoid conflicting with a locally running Redis.
NODE1_REDIS_PORT="${NODE1_REDIS_PORT:-16379}"
NODE2_REDIS_PORT="${NODE2_REDIS_PORT:-16380}"
NODE3_REDIS_PORT="${NODE3_REDIS_PORT:-16381}"

NODE1_GRPC_PORT="${NODE1_GRPC_PORT:-15051}"
NODE2_GRPC_PORT="${NODE2_GRPC_PORT:-15052}"
NODE3_GRPC_PORT="${NODE3_GRPC_PORT:-15053}"

NODE1_ID="${NODE1_ID:-1}"
NODE2_ID="${NODE2_ID:-2}"
NODE3_ID="${NODE3_ID:-3}"

# Defaults for cleanup/build (override via env).
CLUSTER_CLEANUP="${HOLO_CLUSTER_CLEANUP:-1}"
CLUSTER_BUILD="${HOLO_BUILD:-1}"
BUILD_PROFILE="${HOLO_BUILD_PROFILE:-release}"

declare -a FEATURES=()
add_feature() {
  local feat="$1"
  [[ -z "$feat" ]] && return
  if [[ " ${FEATURES[*]-} " == *" ${feat} "* ]]; then
    return
  fi
  FEATURES+=("$feat")
}

add_features_list() {
  local list="$1"
  [[ -z "$list" ]] && return
  IFS=',' read -r -a parts <<<"$list"
  for part in "${parts[@]}"; do
    add_feature "$part"
  done
}

add_features_list "${HOLO_BUILD_FEATURES:-}"
if [[ "${HOLO_WAL_ENGINE:-}" == "raft-engine" ]]; then
  add_feature "raft-engine"
fi

# Swap node IDs (node2 <-> node3) to test ID-based skew.
if [[ "${SWAP_NODE_IDS:-0}" == "1" ]]; then
  tmp="$NODE2_ID"
  NODE2_ID="$NODE3_ID"
  NODE3_ID="$tmp"
fi

# Swap node ports (node2 <-> node3) to test port-based skew.
if [[ "${SWAP_NODE_PORTS:-0}" == "1" ]]; then
  tmp="$NODE2_REDIS_PORT"
  NODE2_REDIS_PORT="$NODE3_REDIS_PORT"
  NODE3_REDIS_PORT="$tmp"

  tmp="$NODE2_GRPC_PORT"
  NODE2_GRPC_PORT="$NODE3_GRPC_PORT"
  NODE3_GRPC_PORT="$tmp"
fi

# Expected node binary interface (to be implemented as part of Milestone 1):
#   holo-store node \
#     --node-id <id> \
#     --listen-redis <host:port> \
#     --listen-grpc <host:port> \
#     --bootstrap | --join <seed-host:port> \
#     --initial-members <id@host:port,...>
#
# Override with HOLO_BIN to point at a built binary:
#   HOLO_BIN=./target/release/holo-store ./scripts/start_cluster.sh
if [[ -z "${HOLO_BIN:-}" ]]; then
  if [[ "$BUILD_PROFILE" == "release" ]]; then
    HOLO_BIN="$ROOT_DIR/target/release/holo-store"
  else
    HOLO_BIN="$ROOT_DIR/target/debug/holo-store"
  fi
fi

if [[ "$CLUSTER_CLEANUP" != "0" ]]; then
  "$ROOT_DIR/scripts/cleanup_cluster.sh" >/dev/null 2>&1 || true
fi

if [[ "$CLUSTER_BUILD" != "0" ]]; then
  build_args=(-p holo_store)
  if [[ "$BUILD_PROFILE" == "release" ]]; then
    build_args+=(--release)
  fi
  if [[ ${#FEATURES[@]} -gt 0 ]]; then
    build_args+=(--features "$(IFS=,; echo "${FEATURES[*]}")")
  fi
  cargo build "${build_args[@]}"
fi

if [[ ! -x "$HOLO_BIN" ]]; then
  cat <<EOF
error: node binary not found/executable at: $HOLO_BIN

Build it first (once implemented), or override HOLO_BIN:
  HOLO_BIN=./path/to/holo-store ./scripts/start_cluster.sh
EOF
  exit 1
fi

# Default RPC inflight tuning (override via env).
export HOLO_RPC_INFLIGHT_LIMIT="${HOLO_RPC_INFLIGHT_LIMIT:-128}"
export HOLO_RPC_INFLIGHT_MIN="${HOLO_RPC_INFLIGHT_MIN:-32}"
export HOLO_RPC_INFLIGHT_MAX="${HOLO_RPC_INFLIGHT_MAX:-128}"
export HOLO_RPC_INFLIGHT_HIGH_WAIT_MS="${HOLO_RPC_INFLIGHT_HIGH_WAIT_MS:-50}"
export HOLO_RPC_INFLIGHT_LOW_WAIT_MS="${HOLO_RPC_INFLIGHT_LOW_WAIT_MS:-5}"
export HOLO_RPC_INFLIGHT_HIGH_QUEUE="${HOLO_RPC_INFLIGHT_HIGH_QUEUE:-2048}"
export HOLO_RPC_INFLIGHT_LOW_QUEUE="${HOLO_RPC_INFLIGHT_LOW_QUEUE:-128}"

# Default RPC batching (override via env).
export HOLO_RPC_BATCH_MAX="${HOLO_RPC_BATCH_MAX:-128}"
export HOLO_RPC_BATCH_WAIT_US="${HOLO_RPC_BATCH_WAIT_US:-200}"
# Recovery pacing controls: keep stall recovery responsive but bounded.
export HOLO_RECOVERY_MIN_DELAY_MS="${HOLO_RECOVERY_MIN_DELAY_MS:-200}"
export HOLO_STALL_RECOVER_INTERVAL_MS="${HOLO_STALL_RECOVER_INTERVAL_MS:-100}"

# Default RPC handler delay injection (override via env).
# Keep this explicit so stale shell exports are visible and easy to diagnose.
export HOLO_RPC_HANDLER_DELAY_MS="${HOLO_RPC_HANDLER_DELAY_MS:-0}"
NODE1_RPC_DELAY_MS="${HOLO_NODE1_RPC_DELAY_MS:-$HOLO_RPC_HANDLER_DELAY_MS}"
NODE2_RPC_DELAY_MS="${HOLO_NODE2_RPC_DELAY_MS:-$HOLO_RPC_HANDLER_DELAY_MS}"
NODE3_RPC_DELAY_MS="${HOLO_NODE3_RPC_DELAY_MS:-$HOLO_RPC_HANDLER_DELAY_MS}"

# Default max shard slots (override via env).
# `HOLO_DATA_SHARDS` is still accepted as a legacy alias for compatibility.
export HOLO_MAX_SHARDS="${HOLO_MAX_SHARDS:-${HOLO_DATA_SHARDS:-4}}"
export HOLO_ROUTING_MODE="${HOLO_ROUTING_MODE:-range}"

# Default client batching (override via env).
export HOLO_CLIENT_SET_BATCH_MAX="${HOLO_CLIENT_SET_BATCH_MAX:-256}"
export HOLO_CLIENT_SET_BATCH_TARGET="${HOLO_CLIENT_SET_BATCH_TARGET:-128}"
# Proposal pipelining controls for direct SET path.
export HOLO_CLIENT_SET_PROPOSAL_PIPELINE_DEPTH="${HOLO_CLIENT_SET_PROPOSAL_PIPELINE_DEPTH:-8}"
export HOLO_RANGE_WRITE_BATCH_TARGET="${HOLO_RANGE_WRITE_BATCH_TARGET:-${HOLO_SQL_WRITE_BATCH_TARGET:-1024}}"
export HOLO_RANGE_WRITE_BATCH_MAX_BYTES="${HOLO_RANGE_WRITE_BATCH_MAX_BYTES:-${HOLO_SQL_WRITE_BATCH_MAX_BYTES:-1048576}}"
# Proposal pipelining controls for replicated range-write path.
export HOLO_RANGE_WRITE_PROPOSAL_PIPELINE_DEPTH="${HOLO_RANGE_WRITE_PROPOSAL_PIPELINE_DEPTH:-8}"
export HOLO_CLIENT_GET_BATCH_MAX="${HOLO_CLIENT_GET_BATCH_MAX:-256}"
export HOLO_CLIENT_BATCH_WAIT_US="${HOLO_CLIENT_BATCH_WAIT_US:-200}"
export HOLO_CLIENT_BATCH_INFLIGHT="${HOLO_CLIENT_BATCH_INFLIGHT:-4}"
export HOLO_CLIENT_BATCH_QUEUE="${HOLO_CLIENT_BATCH_QUEUE:-4096}"

# Default Redis connection batching (override via env).
export HOLO_REDIS_SET_BATCH_MAX="${HOLO_REDIS_SET_BATCH_MAX:-128}"
export HOLO_REDIS_GET_BATCH_MAX="${HOLO_REDIS_GET_BATCH_MAX:-256}"

# Default WAL persistence tuning (override via env).
export HOLO_WAL_PERSIST_MODE="${HOLO_WAL_PERSIST_MODE:-sync_data}"
export HOLO_WAL_PERSIST_EVERY="${HOLO_WAL_PERSIST_EVERY:-1024}"
export HOLO_WAL_PERSIST_INTERVAL_US="${HOLO_WAL_PERSIST_INTERVAL_US:-10000}"
export HOLO_WAL_PERSIST_ASYNC="${HOLO_WAL_PERSIST_ASYNC:-false}"

check_port_free() {
  local port="$1"

  if ! command -v lsof >/dev/null 2>&1; then
    return 0
  fi

  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "error: port already in use: $port" >&2
    lsof -nP -iTCP:"$port" -sTCP:LISTEN >&2 || true
    return 1
  fi
}

# Fail fast if a previous cluster is still running.
check_port_free "$NODE1_REDIS_PORT" || exit 1
check_port_free "$NODE2_REDIS_PORT" || exit 1
check_port_free "$NODE3_REDIS_PORT" || exit 1
check_port_free "$NODE1_GRPC_PORT" || exit 1
check_port_free "$NODE2_GRPC_PORT" || exit 1
check_port_free "$NODE3_GRPC_PORT" || exit 1

mkdir -p "$CLUSTER_DIR"/{logs,data}

PIDS_FILE="$CLUSTER_DIR/pids"
: >"$PIDS_FILE"

members="${NODE1_ID}@${GRPC_HOST}:${NODE1_GRPC_PORT},${NODE2_ID}@${GRPC_HOST}:${NODE2_GRPC_PORT},${NODE3_ID}@${GRPC_HOST}:${NODE3_GRPC_PORT}"

start_node() {
  local node_id="$1"
  local redis_addr="$2"
  local grpc_addr="$3"
  local mode="$4"
  local arg="$5"
  local delay_ms=""
  local log_file="$CLUSTER_DIR/logs/node${node_id}.log"
  local data_dir="$CLUSTER_DIR/data/node${node_id}"
  mkdir -p "$data_dir"

  case "$node_id" in
    "$NODE1_ID") delay_ms="$NODE1_RPC_DELAY_MS" ;;
    "$NODE2_ID") delay_ms="$NODE2_RPC_DELAY_MS" ;;
    "$NODE3_ID") delay_ms="$NODE3_RPC_DELAY_MS" ;;
  esac

  local -a cmd=(
    "$HOLO_BIN" node
    --node-id "$node_id"
    --listen-redis "$redis_addr"
    --listen-grpc "$grpc_addr"
    --initial-members "$members"
    --data-dir "$data_dir"
    --max-shards "$HOLO_MAX_SHARDS"
  )

  if [[ -n "${HOLO_READ_MODE:-}" ]]; then
    cmd+=(--read-mode "$HOLO_READ_MODE")
  fi

  if [[ "$mode" == "bootstrap" ]]; then
    cmd+=(--bootstrap)
  else
    cmd+=(--join "$arg")
  fi

  if [[ -n "$delay_ms" && "$delay_ms" != "0" ]]; then
    HOLO_RPC_HANDLER_DELAY_MS="$delay_ms" "${cmd[@]}" >"$log_file" 2>&1 &
  else
    "${cmd[@]}" >"$log_file" 2>&1 &
  fi

  echo "$!" >>"$PIDS_FILE"
}

start_node "$NODE1_ID" "${REDIS_HOST}:${NODE1_REDIS_PORT}" "${GRPC_HOST}:${NODE1_GRPC_PORT}" bootstrap ""
start_node "$NODE2_ID" "${REDIS_HOST}:${NODE2_REDIS_PORT}" "${GRPC_HOST}:${NODE2_GRPC_PORT}" join "${GRPC_HOST}:${NODE1_GRPC_PORT}"
start_node "$NODE3_ID" "${REDIS_HOST}:${NODE3_REDIS_PORT}" "${GRPC_HOST}:${NODE3_GRPC_PORT}" join "${GRPC_HOST}:${NODE1_GRPC_PORT}"

cat <<EOF
cluster started (pids in $PIDS_FILE)

rpc handler delay injection (ms):
  node1: ${NODE1_RPC_DELAY_MS}
  node2: ${NODE2_RPC_DELAY_MS}
  node3: ${NODE3_RPC_DELAY_MS}

runtime tuning:
  HOLO_MAX_SHARDS=${HOLO_MAX_SHARDS}
  HOLO_CLIENT_SET_BATCH_MAX=${HOLO_CLIENT_SET_BATCH_MAX}
  HOLO_CLIENT_SET_BATCH_TARGET=${HOLO_CLIENT_SET_BATCH_TARGET}
  HOLO_CLIENT_SET_PROPOSAL_PIPELINE_DEPTH=${HOLO_CLIENT_SET_PROPOSAL_PIPELINE_DEPTH}
  HOLO_RANGE_WRITE_BATCH_TARGET=${HOLO_RANGE_WRITE_BATCH_TARGET}
  HOLO_RANGE_WRITE_BATCH_MAX_BYTES=${HOLO_RANGE_WRITE_BATCH_MAX_BYTES}
  HOLO_RANGE_WRITE_PROPOSAL_PIPELINE_DEPTH=${HOLO_RANGE_WRITE_PROPOSAL_PIPELINE_DEPTH}
  HOLO_RECOVERY_MIN_DELAY_MS=${HOLO_RECOVERY_MIN_DELAY_MS}
  HOLO_STALL_RECOVER_INTERVAL_MS=${HOLO_STALL_RECOVER_INTERVAL_MS}
  HOLO_RPC_INFLIGHT_LIMIT=${HOLO_RPC_INFLIGHT_LIMIT}
  HOLO_RPC_INFLIGHT_MIN=${HOLO_RPC_INFLIGHT_MIN}
  HOLO_RPC_INFLIGHT_MAX=${HOLO_RPC_INFLIGHT_MAX}

redis endpoints:
  node1: ${REDIS_HOST}:${NODE1_REDIS_PORT}
  node2: ${REDIS_HOST}:${NODE2_REDIS_PORT}
  node3: ${REDIS_HOST}:${NODE3_REDIS_PORT}

grpc endpoints:
  node1: ${GRPC_HOST}:${NODE1_GRPC_PORT}
  node2: ${GRPC_HOST}:${NODE2_GRPC_PORT}
  node3: ${GRPC_HOST}:${NODE3_GRPC_PORT}

logs:
  $CLUSTER_DIR/logs/node1.log
  $CLUSTER_DIR/logs/node2.log
  $CLUSTER_DIR/logs/node3.log
EOF
