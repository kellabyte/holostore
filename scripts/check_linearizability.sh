#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

NODES="${NODES:-127.0.0.1:16379,127.0.0.1:16380,127.0.0.1:16381}"
CLIENTS="${CLIENTS:-3}"
KEYS="${KEYS:-5}"
SET_PCT="${SET_PCT:-50}"
DURATION="${DURATION:-10s}"
OP_TIMEOUT="${OP_TIMEOUT:-10s}"
FAIL_FAST="${FAIL_FAST:-true}"

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

echo "==> running workload (history: $OUT_ABS)"
"$ROOT_DIR/target/release/holo-workload" run \
  --nodes "$NODES" \
  --clients "$CLIENTS" \
  --keys "$KEYS" \
  --set-pct "$SET_PCT" \
  --duration "$DURATION" \
  --op-timeout "$OP_TIMEOUT" \
  --fail-fast="$FAIL_FAST" \
  --out "$OUT_ABS"

echo "==> checking linearizability (porcupine)"
(
  cd "$ROOT_DIR/tools/porcupine_check"
  go run . --history "$OUT_ABS"
)

echo "OK"
