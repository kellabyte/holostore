#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

NODES="${NODES:-127.0.0.1:16379,127.0.0.1:16380,127.0.0.1:16381}"
READ_NODES="${READ_NODES:-}"
WRITE_NODES="${WRITE_NODES:-}"
CLIENTS="${CLIENTS:-3}"
KEYS="${KEYS:-5}"
SET_PCT="${SET_PCT:-50}"
DURATION="${DURATION:-10s}"
OP_TIMEOUT="${OP_TIMEOUT:-10s}"
FAIL_FAST="${FAIL_FAST:-true}"
FAULT_DISCONNECT_PCT="${FAULT_DISCONNECT_PCT:-0}"
ALLOW_ERRORS="${ALLOW_ERRORS:-0}"
FAIL_INJECT="${FAIL_INJECT:-0}"
FAIL_KILL_INTERVAL="${FAIL_KILL_INTERVAL:-2s}"
FAIL_KILL_SIGNAL="${FAIL_KILL_SIGNAL:-KILL}"
FAIL_KILL_RESTART="${FAIL_KILL_RESTART:-1}"

OUT="${OUT:-$ROOT_DIR/.tmp/porcupine/history.json}"

PYTHON_BIN="${PYTHON_BIN:-python3}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  PYTHON_BIN="python"
fi

OUT_ABS="$("$PYTHON_BIN" - "$OUT" <<'PY'
import os
import sys
print(os.path.abspath(sys.argv[1]))
PY
)"

cleanup() {
  "$ROOT_DIR/scripts/cleanup_cluster.sh" >/dev/null 2>&1 || true
}
trap cleanup EXIT

inject_failures() {
  local pids_file="$CLUSTER_DIR/pids"
  if [[ ! -f "$pids_file" ]]; then
    echo "failure injector: pids file not found: $pids_file" >&2
    return 1
  fi

  echo "failure injector: interval=$FAIL_KILL_INTERVAL signal=$FAIL_KILL_SIGNAL restart=$FAIL_KILL_RESTART"
  while true; do
    sleep "$FAIL_KILL_INTERVAL"

    local pid
    pid=$("$PYTHON_BIN" - <<'PY' "$pids_file"
import random
import sys
path = sys.argv[1]
with open(path) as f:
    pids = [line.strip() for line in f if line.strip()]
if not pids:
    sys.exit(1)
print(random.choice(pids))
PY
)
    if [[ -z "$pid" ]]; then
      continue
    fi

    local cmd
    cmd=$(ps -p "$pid" -o command= || true)
    if [[ -z "$cmd" ]]; then
      continue
    fi

    echo "failure injector: killing pid $pid"
    kill "-$FAIL_KILL_SIGNAL" "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true

    if [[ "$FAIL_KILL_RESTART" == "1" ]]; then
      local ts
      ts=$(date +"%Y%m%d_%H%M%S")
      local log_file="$CLUSTER_DIR/logs/injector_${pid}_${ts}.log"
      echo "failure injector: restarting pid $pid"
      bash -c "$cmd" >"$log_file" 2>&1 &
      echo "$!" >>"$pids_file"
    fi
  done
}

echo "==> cleaning cluster"
"$ROOT_DIR/scripts/cleanup_cluster.sh"

echo "==> building holo-store (release)"
make -C "$ROOT_DIR" build-release

echo "==> starting cluster"
HOLO_BIN="${HOLO_BIN:-$ROOT_DIR/target/release/holo-store}" \
  "$ROOT_DIR/scripts/start_cluster.sh"

wait_port() {
  local host="$1"
  local port="$2"
  local attempts="${3:-50}"
  local delay="${4:-0.1}"

  if ! command -v nc >/dev/null 2>&1; then
    sleep 1
    return 0
  fi

  for _ in $(seq 1 "$attempts"); do
    if nc -z "$host" "$port" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$delay"
  done
  return 1
}

echo "==> waiting for redis port 16379"
wait_port 127.0.0.1 16379 || { echo "error: redis port 16379 not ready" >&2; exit 1; }

echo "==> building holo-workload (release)"
cargo build -p holo_workload --release

injector_pid=""
if [[ "$FAIL_INJECT" == "1" ]]; then
  inject_failures &
  injector_pid="$!"
fi

echo "==> running workload (history: $OUT_ABS)"
workload_args=(
  --nodes "$NODES"
  --clients "$CLIENTS"
  --keys "$KEYS"
  --set-pct "$SET_PCT"
  --duration "$DURATION"
  --op-timeout "$OP_TIMEOUT"
  --fail-fast="$FAIL_FAST"
  --out "$OUT_ABS"
)

if [[ -n "$READ_NODES" ]]; then
  workload_args+=(--read-nodes "$READ_NODES")
fi
if [[ -n "$WRITE_NODES" ]]; then
  workload_args+=(--write-nodes "$WRITE_NODES")
fi
if [[ "$FAULT_DISCONNECT_PCT" != "0" ]]; then
  workload_args+=(--fault-disconnect-pct "$FAULT_DISCONNECT_PCT")
fi

"$ROOT_DIR/target/release/holo-workload" run "${workload_args[@]}"

if [[ -n "$injector_pid" ]]; then
  kill "$injector_pid" 2>/dev/null || true
  wait "$injector_pid" 2>/dev/null || true
fi

echo "==> checking linearizability (porcupine)"
(
  cd "$ROOT_DIR/tools/porcupine_check"
  if [[ "$ALLOW_ERRORS" == "1" ]]; then
    go run . --history "$OUT_ABS" --allow-errors
  else
    go run . --history "$OUT_ABS"
  fi
)

echo "OK"
