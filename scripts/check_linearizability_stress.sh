#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/.tmp/porcupine}"
mkdir -p "$OUT_DIR"

TS="$(date +"%Y%m%d_%H%M%S")"
SUMMARY="$OUT_DIR/stress_suite_${TS}.summary"

STRESS_RUNS="${STRESS_RUNS:-5}"
STRICT="${STRICT:-0}"
STRESS_CLIENTS="${STRESS_CLIENTS:-3}"
STRESS_DURATION="${STRESS_DURATION:-20s}"
MERGE_STRESS_DURATION="${MERGE_STRESS_DURATION:-60s}"

run_case() {
  local name="$1"
  shift
  local out="$OUT_DIR/${name}_${TS}.out"

  echo "==> ${name}" | tee -a "$SUMMARY"
  if env "$@" "$ROOT_DIR/scripts/porcupine.sh" >"$out" 2>&1; then
    echo "PASS ${name}" | tee -a "$SUMMARY"
    return 0
  fi

  echo "FAIL ${name} (see $out)" | tee -a "$SUMMARY"
  return 1
}

echo "Linearizability stress suite started at ${TS}" >"$SUMMARY"
echo "STRESS_RUNS=${STRESS_RUNS} STRICT=${STRICT} STRESS_CLIENTS=${STRESS_CLIENTS} STRESS_DURATION=${STRESS_DURATION}" | tee -a "$SUMMARY"
echo | tee -a "$SUMMARY"

failures=0
for i in $(seq 1 "$STRESS_RUNS"); do
  # Non-gating stress scenario:
  # - Exercises live autosplit + migration with higher concurrency than the
  #   main suite, which can expose timing-sensitive races.
  # - Intended for soak/regression detection, not CI gating by default.
  if ! run_case "range_autosplit_stress_run_${i}" \
    HOLO_INITIAL_RANGES=1 \
    HOLO_RANGE_SPLIT_MIN_KEYS=2 \
    HOLO_RANGE_SPLIT_MIN_QPS=0 \
    CLIENTS="$STRESS_CLIENTS" \
    DURATION="$STRESS_DURATION" \
    FAIL_INJECT=0 \
    FAULT_DISCONNECT_PCT=0; then
    failures=$((failures + 1))
  fi
  echo | tee -a "$SUMMARY"
done

# Non-gating long run with autosplit + automerge enabled together.
# This exercises descriptor churn from both directions over time.
if ! run_case "autosplit_automerge_long" \
  HOLO_INITIAL_RANGES=1 \
  HOLO_RANGE_SPLIT_MIN_KEYS=2 \
  HOLO_RANGE_SPLIT_MIN_QPS=0 \
  HOLO_RANGE_MERGE_MAX_KEYS=8 \
  HOLO_RANGE_MERGE_MAX_QPS=1000000 \
  HOLO_RANGE_MERGE_QPS_SUSTAIN=1 \
  HOLO_RANGE_MGR_INTERVAL_MS=50 \
  CLIENTS="$STRESS_CLIENTS" \
  DURATION="$MERGE_STRESS_DURATION" \
  FAIL_INJECT=0 \
  FAULT_DISCONNECT_PCT=0; then
  failures=$((failures + 1))
fi
echo | tee -a "$SUMMARY"

total_cases=$((STRESS_RUNS + 1))
echo "Summary written to $SUMMARY"
echo "Failures: $failures/$total_cases"

if [[ "$STRICT" == "1" && "$failures" -gt 0 ]]; then
  exit 1
fi

exit 0
