#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/.tmp/porcupine}"
mkdir -p "$OUT_DIR"

TS="$(date +"%Y%m%d_%H%M%S")"
SUMMARY="$OUT_DIR/suite_${TS}.summary"

run_case() {
  local name="$1"
  shift
  local out="$OUT_DIR/${name}_${TS}.out"
  echo "==> ${name}" | tee -a "$SUMMARY"
  if env "$@" "$ROOT_DIR/scripts/porcupine.sh" >"$out" 2>&1; then
    echo "PASS ${name}" | tee -a "$SUMMARY"
  else
    echo "FAIL ${name} (see $out)" | tee -a "$SUMMARY"
  fi
  echo | tee -a "$SUMMARY"
}

echo "Linearizability suite started at ${TS}" >"$SUMMARY"

# Baseline: standard workload, no injected failures.
# Verifies: basic linearizability under steady-state conditions.
run_case "baseline" \
  FAIL_INJECT=0 \
  FAULT_DISCONNECT_PCT=0

# Slow replica:
# Verifies: linearizability under an artificially delayed replica.
run_case "slow_replica" \
  HOLO_NODE3_RPC_DELAY_MS=200 \
  DURATION=20s \
  FAIL_INJECT=0 \
  FAULT_DISCONNECT_PCT=0 \
  FAILURE_GRACE=2s

# Hot-key contention:
# Verifies: ordering/visibility under heavy write contention on a single key.
run_case "hot_key_contention" \
  KEYS=1 \
  SET_PCT=90 \
  DURATION=20s \
  FAIL_INJECT=0 \
  FAULT_DISCONNECT_PCT=0

# Mixed read/write nodes: single read node, all writes.
# Verifies: correctness when GETs are routed to a subset of nodes.
run_case "mixed_read_write_nodes" \
  READ_NODES="127.0.0.1:16379" \
  WRITE_NODES="127.0.0.1:16379,127.0.0.1:16380,127.0.0.1:16381" \
  FAIL_INJECT=0 \
  FAULT_DISCONNECT_PCT=0

# Client-side disconnect injection.
# Verifies: protocol/driver resilience to connection churn without server failures.
run_case "client_disconnects" \
  DURATION=20s \
  FAULT_DISCONNECT_PCT=5 \
  FAIL_INJECT=0

# Crash during write:
# Verifies: linearizability + durability while nodes crash/restart during heavy SET load.
run_case "crash_during_write" \
  DURATION=25s \
  SET_PCT=100 \
  FAIL_FAST=false \
  ALLOW_ERRORS=1 \
  FAIL_INJECT=1 \
  FAIL_KILL_INTERVAL=3s \
  FAIL_KILL_SIGNAL=KILL \
  FAIL_KILL_RESTART=1 \
  FAILURE_GRACE=2s

# Server-side kill/restart injection.
# Verifies: linearizability and recovery under node crashes/restarts.
run_case "server_kill_restart" \
  DURATION=30s \
  FAIL_FAST=false \
  ALLOW_ERRORS=1 \
  FAIL_INJECT=1 \
  FAIL_KILL_INTERVAL=5s \
  FAIL_KILL_SIGNAL=KILL \
  FAIL_KILL_RESTART=1 \
  FAILURE_GRACE=2s

echo "Summary written to $SUMMARY"
