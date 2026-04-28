#!/usr/bin/env bash
set -euo pipefail

# Run the local Antithesis correctness suite end-to-end. By default this starts
# from clean .tmp/antithesis state, builds images, runs every installed command,
# stops Compose, and preserves history/checker artifacts for inspection.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/antithesis_common.sh"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
RESULTS_TSV=""
REPORT_PATH=""
REPORT_LOG_DIR=""
REPORT_GENERATED=0
SUITE_STATUS=0

is_truthy() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|y|Y) return 0 ;;
    *) return 1 ;;
  esac
}

init_report() {
  REPORT_LOG_DIR="$ANTITHESIS_TMP_DIR/history/suite-report-$RUN_ID"
  RESULTS_TSV="$REPORT_LOG_DIR/results.tsv"
  REPORT_PATH="$ANTITHESIS_TMP_DIR/history/antithesis-full-report-$RUN_ID.md"
  mkdir -p "$REPORT_LOG_DIR"
  printf 'name\tstatus\texit_code\tstarted_at\tended_at\tcommand\tstdout\tstderr\n' > "$RESULTS_TSV"
}

report_text() {
  local value="$1"
  local root_prefix="$ROOT_DIR/"
  value="${value//$root_prefix/}"
  if [[ "$value" == "$ROOT_DIR" ]]; then
    value="."
  fi
  printf '%s' "$value"
}

record_step() {
  local name="$1"
  local status="$2"
  local exit_code="$3"
  local started_at="$4"
  local ended_at="$5"
  local command="$6"
  local stdout_log="$7"
  local stderr_log="$8"

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$name" "$status" "$exit_code" "$started_at" "$ended_at" \
    "$(report_text "$command")" "$(report_text "$stdout_log")" "$(report_text "$stderr_log")" >> "$RESULTS_TSV"
}

run_step() {
  local name="$1"
  local display_command="$2"
  shift 2

  local stdout_log="$REPORT_LOG_DIR/${name}-${RUN_ID}.stdout"
  local stderr_log="$REPORT_LOG_DIR/${name}-${RUN_ID}.stderr"
  local started_at
  local ended_at
  local exit_code
  local status

  started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo "==> ${display_command}"

  set +e
  "$@" > >(tee "$stdout_log") 2> >(tee "$stderr_log" >&2)
  exit_code=$?
  set -e

  ended_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if [[ "$exit_code" -eq 0 ]]; then
    status="PASS"
  else
    status="FAIL"
    SUITE_STATUS=1
  fi

  record_step "$name" "$status" "$exit_code" "$started_at" "$ended_at" "$display_command" "$stdout_log" "$stderr_log"
  return 0
}

record_skipped_step() {
  local name="$1"
  local command="$2"
  local now
  now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  record_step "$name" "SKIP" "0" "$now" "$now" "$command" "" ""
}

run_client_command() {
  local command="$1"
  shift
  local name
  local env_args=(-e ANTITHESIS_LOCAL_ASSERTS=1)
  local display_command="$command"

  for assignment in "$@"; do
    env_args+=(-e "$assignment")
  done

  if [[ "$#" -gt 0 ]]; then
    display_command="$(printf '%s ' "$@")$command"
  fi

  name="$(basename "$command" .py)"
  run_step "$name" "$display_command" compose_in_antithesis exec -T "${env_args[@]}" client "$command"
}

generate_report() {
  if [[ "$REPORT_GENERATED" -eq 1 || -z "$RESULTS_TSV" || ! -f "$RESULTS_TSV" ]]; then
    return 0
  fi

  if command -v python3 >/dev/null 2>&1; then
    python3 "$ROOT_DIR/scripts/antithesis_generate_report.py" \
      --root-dir "$ROOT_DIR" \
      --history-dir "$ANTITHESIS_TMP_DIR/history" \
      --results "$RESULTS_TSV" \
      --out "$REPORT_PATH" || true
    if [[ -f "${REPORT_PATH}.status" ]] && grep -q '^FAIL$' "${REPORT_PATH}.status"; then
      SUITE_STATUS=1
    fi
  else
    {
      echo "# Antithesis Full Suite Report"
      echo
      echo "Report generation was limited because python3 is not available."
      echo
      echo "Raw results: $(report_text "$RESULTS_TSV")"
    } > "$REPORT_PATH"
  fi

  REPORT_GENERATED=1
}

if is_truthy "${ANTITHESIS_CLEAN_BEFORE:-1}"; then
  compose_in_antithesis down -v >/dev/null 2>&1 || true
  rm -rf "$ANTITHESIS_TMP_DIR"
fi

prepare_antithesis_tmp
init_report

if ! is_truthy "${ANTITHESIS_SKIP_BUILD:-0}"; then
  run_step "build-images" "$ROOT_DIR/scripts/antithesis_local_build.sh" "$ROOT_DIR/scripts/antithesis_local_build.sh"
else
  record_skipped_step "build-images" "ANTITHESIS_SKIP_BUILD=1"
fi

if [[ "$SUITE_STATUS" -ne 0 ]]; then
  exit "$SUITE_STATUS"
fi

# Full-suite defaults exercise split/range churn. Callers can still override
# these before invoking the script.
export HOLO_INITIAL_RANGES="${HOLO_INITIAL_RANGES:-1}"
export HOLO_MAX_SHARDS="${HOLO_MAX_SHARDS:-8}"
export HOLO_RANGE_SPLIT_MIN_KEYS="${HOLO_RANGE_SPLIT_MIN_KEYS:-2}"
export HOLO_RANGE_SPLIT_MIN_QPS="${HOLO_RANGE_SPLIT_MIN_QPS:-0}"

cleanup() {
  local status=$?

  if [[ "$status" -ne 0 ]]; then
    SUITE_STATUS=1
  fi

  generate_report

  if is_truthy "${ANTITHESIS_KEEP_STACK:-0}"; then
    echo
    echo "Leaving local Antithesis stack running because ANTITHESIS_KEEP_STACK=1"
  else
    compose_in_antithesis down -v >/dev/null 2>&1 || true
  fi

  if [[ "$SUITE_STATUS" -eq 0 ]] && is_truthy "${ANTITHESIS_CLEAN_AFTER:-0}"; then
    rm -rf \
      "$ANTITHESIS_BUILD_DIR" \
      "$ANTITHESIS_TMP_DIR/node1-data" \
      "$ANTITHESIS_TMP_DIR/node2-data" \
      "$ANTITHESIS_TMP_DIR/node3-data"
    echo
    echo "Cleaned build and node state; retained history/report artifacts."
  else
    echo
    echo "Antithesis artifacts: $(report_text "$ANTITHESIS_TMP_DIR/history")"
  fi

  if [[ -f "$REPORT_PATH" ]]; then
    echo "Antithesis report: $(report_text "$REPORT_PATH")"
  fi

  exit "$SUITE_STATUS"
}
trap cleanup EXIT

run_step "start-stack" "docker compose up -d" compose_in_antithesis up -d
if [[ "$SUITE_STATUS" -ne 0 ]]; then
  exit "$SUITE_STATUS"
fi

run_step "wait-for-setup" "wait for setup-complete.json" wait_for_antithesis_setup "${ANTITHESIS_SETUP_TIMEOUT_S:-180}"
if [[ "$SUITE_STATUS" -ne 0 ]]; then
  exit "$SUITE_STATUS"
fi

run_client_command /opt/antithesis/test/v1/main/first_prepare.py
run_client_command /opt/antithesis/test/v1/main/singleton_driver_linearizability.py KEY_PREFIX=antithesis_full_singleton_
run_client_command /opt/antithesis/test/v1/main/parallel_driver_baseline_registers.py KEY_PREFIX=antithesis_full_baseline_
run_client_command /opt/antithesis/test/v1/main/parallel_driver_hot_key.py KEY_PREFIX=antithesis_full_hot_
run_client_command /opt/antithesis/test/v1/main/parallel_driver_range_churn.py KEY_PREFIX=antithesis_full_range_
run_client_command /opt/antithesis/test/v1/main/parallel_driver_disconnects.py KEY_PREFIX=antithesis_full_disconnects_
run_client_command /opt/antithesis/test/v1/main/anytime_health.py
run_client_command /opt/antithesis/test/v1/main/anytime_metrics_sanity.py
run_client_command /opt/antithesis/test/v1/main/eventually_recovery.py
run_client_command /opt/antithesis/test/v1/recovery/singleton_driver_crash_recovery.py
run_client_command /opt/antithesis/test/v1/recovery/eventually_recovery_check.py

run_client_command /opt/antithesis/test/v1/main/finally_check_linearizability.py

echo
if [[ "$SUITE_STATUS" -eq 0 ]]; then
  echo "Local Antithesis full suite passed."
else
  echo "Local Antithesis full suite failed."
fi

exit "$SUITE_STATUS"
