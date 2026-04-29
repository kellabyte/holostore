#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/benchmark_common.sh"

SCRIPT_STARTED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
SCRIPT_STARTED_SECONDS="$(date +%s)"
BUILD_SECONDS=0
BENCHMARK_SECONDS=0
TOTAL_SECONDS=0

TARGET="${TARGET:-both}"
SCENARIO="${SCENARIO:-mixed-uniform}"
DURATION="${DURATION:-30s}"
RATE="${RATE:-1000}"
WORKERS="${WORKERS:-}"
CONNECTIONS="${CONNECTIONS:-}"
WORKER_HEADROOM="${WORKER_HEADROOM:-1}"
QUEUE_CAP="${QUEUE_CAP:-0}"
WRITE_PCT="${WRITE_PCT:-50}"
CONTENTION="${CONTENTION:-uniform}"
KEYS="${KEYS:-10000}"
HOT_KEYS="${HOT_KEYS:-1}"
HOT_PCT="${HOT_PCT:-90}"
VALUE_BYTES="${VALUE_BYTES:-128}"
TIMEOUT="${TIMEOUT:-5s}"
SEED="${SEED:-1}"
PRELOAD="${PRELOAD:-1}"
PRELOAD_WORKERS="${PRELOAD_WORKERS:-0}"
PRELOAD_TIMEOUT="${PRELOAD_TIMEOUT:-30s}"
PRELOAD_RETRIES="${PRELOAD_RETRIES:-3}"
KEEP_CLUSTER="${KEEP_CLUSTER:-0}"
BUILD="${BUILD:-1}"
RESULTS_ROOT="${RESULTS_ROOT:-$BENCHMARK_TMP_DIR/results}"

usage() {
  cat <<EOF
usage: $0 [options]

Runs HoloStore and/or etcd in Docker, then runs the benchmark runner in Docker.

Options:
  --target holostore|etcd|both
  --scenario NAME
  --duration 30s
  --rate OPS_PER_SEC
  --workers N         Required. Number of concurrent scheduled operation workers.
  --connections N     Target client/connection pool size (default: --workers)
  --worker-headroom N Multiplier used for worker recommendation warnings
  --queue-cap N       Scheduled request queue capacity; 0 uses workers*2
  --write-pct 0..100
  --contention uniform|single-key|hotspot|zipf
  --keys N
  --hot-keys N
  --hot-pct 0..100
  --value-bytes N
  --timeout 5s
  --seed N
  --results-root PATH  Directory where timestamped results are written (default: .tmp/benchmarks/results)
  --platform PLATFORM  Docker platform for benchmark containers. Use
                       linux/arm64 for native Apple Silicon, or native to
                       match the Docker server architecture (default: native).
  --preload-workers N  Preload worker clients; 0 picks a conservative default
  --preload-timeout 30s
  --preload-retries N
  --holostore-build-mode host|docker
                      Build HoloStore on the host for Linux, or use the slower
                      Docker builder fallback
  --holostore-target TARGET
  --no-preload
  --keep
  --no-build          Reuse existing benchmark images instead of rebuilding them
EOF
}

now_seconds() {
  date +%s
}

elapsed_seconds_since() {
  local started="$1"
  local finished
  finished="$(now_seconds)"
  printf '%s\n' "$((finished - started))"
}

format_duration() {
  local seconds="$1"
  local hours minutes remainder
  hours=$((seconds / 3600))
  minutes=$(((seconds % 3600) / 60))
  remainder=$((seconds % 60))
  if [[ "$hours" -gt 0 ]]; then
    printf '%dh %dm %ds\n' "$hours" "$minutes" "$remainder"
  elif [[ "$minutes" -gt 0 ]]; then
    printf '%dm %ds\n' "$minutes" "$remainder"
  else
    printf '%ds\n' "$remainder"
  fi
}

write_timing_json() {
  local path="$1"
  local completed_at
  completed_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  python3 - \
    "$path" \
    "$SCRIPT_STARTED_AT" \
    "$completed_at" \
    "$TOTAL_SECONDS" \
    "$BUILD_SECONDS" \
    "$BENCHMARK_SECONDS" \
    "${BENCHMARK_PLATFORM:-}" \
    "$TARGET" \
    "$SCENARIO" \
    "$DURATION" \
    "$RATE" \
    "$WORKERS" \
    "$CONNECTIONS" \
    "$WORKER_HEADROOM" \
    "$QUEUE_CAP" \
    "$WRITE_PCT" \
    "$CONTENTION" \
    "$KEYS" \
    "$HOT_KEYS" \
    "$HOT_PCT" \
    "$VALUE_BYTES" \
    "$TIMEOUT" \
    "$SEED" \
    "$PRELOAD" \
    "$PRELOAD_WORKERS" \
    "$PRELOAD_TIMEOUT" \
    "$PRELOAD_RETRIES" \
    "$BUILD" \
    "$KEEP_CLUSTER" \
    "$BENCHMARK_HOLOSTORE_BUILD_MODE" \
    "$BENCHMARK_HOLOSTORE_TARGET" <<'PY'
import json
import sys


def fmt(seconds: int) -> str:
    hours, rem = divmod(seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


path, started_at, completed_at, total, build, benchmark, platform = sys.argv[1:8]
arg_names = [
    "--target",
    "--scenario",
    "--duration",
    "--rate",
    "--workers",
    "--connections",
    "--worker-headroom",
    "--queue-cap",
    "--write-pct",
    "--contention",
    "--keys",
    "--hot-keys",
    "--hot-pct",
    "--value-bytes",
    "--timeout",
    "--seed",
    "--preload",
    "--preload-workers",
    "--preload-timeout",
    "--preload-retries",
    "--build",
    "--keep",
    "--holostore-build-mode",
    "--holostore-target",
]
benchmark_args = {
    name: value for name, value in zip(arg_names, sys.argv[8:]) if value != ""
}
payload = {
    "started_at": started_at,
    "completed_at": completed_at,
    "platform": platform or "docker default",
    "benchmark_args": benchmark_args,
    "total_seconds": int(total),
    "build_seconds": int(build),
    "benchmark_seconds": int(benchmark),
    "total": fmt(int(total)),
    "build": fmt(int(build)),
    "benchmark": fmt(int(benchmark)),
}
with open(path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
    handle.write("\n")
PY
}

write_failure_json() {
  local path="$1"
  local target="$2"
  local phase="$3"
  local exit_code="$4"
  local message="$5"
  local failed_at
  failed_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  mkdir -p "$(dirname "$path")"
  python3 - "$path" "$target" "$SCENARIO" "$phase" "$exit_code" "$message" "$failed_at" <<'PY'
import json
import sys


path, target, scenario, phase, exit_code, message, failed_at = sys.argv[1:8]
try:
    exit_code_int = int(exit_code)
except ValueError:
    exit_code_int = None

if exit_code_int == 137:
    reason = (
        f"{phase} exited 137; the container was killed, commonly because "
        "Docker or Colima ran out of memory or the process received SIGKILL"
    )
elif exit_code_int is None:
    reason = f"{phase} failed: {message}"
else:
    reason = f"{phase} exited {exit_code_int}: {message}"

payload = {
    "target": target,
    "scenario": scenario,
    "phase": phase,
    "exit_code": exit_code_int if exit_code_int is not None else exit_code,
    "message": message,
    "reason": reason,
    "failed_at": failed_at,
}
with open(path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
    handle.write("\n")
PY
}

annotate_report_with_timing() {
  local report="$1"
  local timing_json="$2"
  [[ -f "$report" ]] || return 0
  python3 - "$report" "$timing_json" <<'PY'
import json
import sys
from pathlib import Path


report = Path(sys.argv[1])
timing_path = Path(sys.argv[2])
timing = json.loads(timing_path.read_text(encoding="utf-8"))
start_marker = "<!-- benchmark-timing:start -->"
end_marker = "<!-- benchmark-timing:end -->"


def cell(value: object) -> str:
    text = str(value).replace("|", "\\|")
    return f"`{text}`"


environment_rows = [
    f"| Docker platform | {cell(timing.get('platform', 'docker default'))} |",
]
for name, value in timing.get("benchmark_args", {}).items():
    environment_rows.append(f"| {cell(name)} | {cell(value)} |")

block = "\n".join(
    [
        start_marker,
        "",
        "## Run Environment",
        "",
        "| setting | value |",
        "| --- | --- |",
        *environment_rows,
        "",
        "## Run Timing",
        "",
        "| metric | duration | seconds |",
        "| --- | --- | --- |",
        f"| total | {timing['total']} | {timing['total_seconds']} |",
        f"| build | {timing['build']} | {timing['build_seconds']} |",
        f"| benchmark run | {timing['benchmark']} | {timing['benchmark_seconds']} |",
        "",
        end_marker,
        "",
    ]
)
text = report.read_text(encoding="utf-8")
if start_marker in text and end_marker in text:
    before, rest = text.split(start_marker, 1)
    _, after = rest.split(end_marker, 1)
    text = before.rstrip() + "\n\n" + block + "\n" + after.lstrip()
else:
    lines = text.splitlines()
    if lines and lines[0].startswith("# "):
        text = lines[0] + "\n\n" + block + "\n" + "\n".join(lines[1:]).lstrip()
    else:
        text = block + text
report.write_text(text.rstrip() + "\n", encoding="utf-8")
PY
}

annotate_reports_with_timing() {
  local timing_json="$1"
  local report
  if [[ -f "$RUN_ROOT/report.md" ]]; then
    annotate_report_with_timing "$RUN_ROOT/report.md" "$timing_json"
  fi
  for target in "${targets[@]}"; do
    report="$RUN_ROOT/$target/report.md"
    annotate_report_with_timing "$report" "$timing_json"
  done
}

print_timing_summary() {
  echo "timing:"
  echo "  platform: ${BENCHMARK_PLATFORM:-docker default}"
  echo "  total: $(format_duration "$TOTAL_SECONDS") (${TOTAL_SECONDS}s)"
  echo "  build: $(format_duration "$BUILD_SECONDS") (${BUILD_SECONDS}s)"
  echo "  benchmark run: $(format_duration "$BENCHMARK_SECONDS") (${BENCHMARK_SECONDS}s)"
}

final_report_path() {
  if [[ -f "$RUN_ROOT/report.md" ]]; then
    printf '%s\n' "$RUN_ROOT/report.md"
    return 0
  fi
  if [[ "${#targets[@]}" == "1" ]]; then
    printf '%s\n' "$RUN_ROOT/${targets[0]}/report.md"
    return 0
  fi
  printf '%s\n' "$RUN_ROOT/report.md"
}

print_final_report_path() {
  local report_path
  report_path="$(final_report_path)"
  if [[ ! -f "$report_path" ]]; then
    echo "report missing: $report_path"
  fi
  echo "$report_path"
}

require_positive_int() {
  local name="$1"
  local value="$2"
  if [[ -z "$value" ]]; then
    echo "$name is required" >&2
    usage >&2
    exit 2
  fi
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "$name must be a positive integer" >&2
    exit 2
  fi
}

require_nonnegative_int() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    echo "$name must be a non-negative integer" >&2
    exit 2
  fi
}

write_platform_compose_override() {
  local target="$1"
  local path="$2"
  local -a platform_services
  local service

  resolve_benchmark_platform
  if [[ -z "$BENCHMARK_PLATFORM" ]]; then
    return 1
  fi

  case "$target" in
    holostore) platform_services=(holostore1 holostore2 holostore3 bench-runner) ;;
    etcd) platform_services=(etcd1 etcd2 etcd3 bench-runner) ;;
    *) echo "target must be holostore or etcd" >&2; exit 2 ;;
  esac

  {
    echo "services:"
    for service in "${platform_services[@]}"; do
      printf '  %s:\n' "$service"
      printf '    platform: %s\n' "$BENCHMARK_PLATFORM"
    done
  } >"$path"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target) TARGET="$2"; shift 2 ;;
    --platform) BENCHMARK_PLATFORM="$2"; BENCHMARK_PLATFORM_RESOLVED=0; shift 2 ;;
    --scenario) SCENARIO="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --rate) RATE="$2"; shift 2 ;;
    --workers) WORKERS="$2"; shift 2 ;;
    --connections) CONNECTIONS="$2"; shift 2 ;;
    --worker-headroom) WORKER_HEADROOM="$2"; shift 2 ;;
    --queue-cap) QUEUE_CAP="$2"; shift 2 ;;
    --write-pct) WRITE_PCT="$2"; shift 2 ;;
    --contention) CONTENTION="$2"; shift 2 ;;
    --keys) KEYS="$2"; shift 2 ;;
    --hot-keys) HOT_KEYS="$2"; shift 2 ;;
    --hot-pct) HOT_PCT="$2"; shift 2 ;;
    --value-bytes) VALUE_BYTES="$2"; shift 2 ;;
    --timeout) TIMEOUT="$2"; shift 2 ;;
    --seed) SEED="$2"; shift 2 ;;
    --results-root) RESULTS_ROOT="$2"; shift 2 ;;
    --preload-workers) PRELOAD_WORKERS="$2"; shift 2 ;;
    --preload-timeout) PRELOAD_TIMEOUT="$2"; shift 2 ;;
    --preload-retries) PRELOAD_RETRIES="$2"; shift 2 ;;
    --holostore-build-mode) BENCHMARK_HOLOSTORE_BUILD_MODE="$2"; shift 2 ;;
    --holostore-target) BENCHMARK_HOLOSTORE_TARGET="$2"; shift 2 ;;
    --no-preload) PRELOAD="0"; shift ;;
    --keep) KEEP_CLUSTER="1"; shift ;;
    --no-build) BUILD="0"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

case "$TARGET" in
  holostore|etcd|both) ;;
  *) echo "--target must be holostore, etcd, or both" >&2; exit 2 ;;
esac

require_positive_int "--workers" "$WORKERS"
if [[ -z "$CONNECTIONS" ]]; then
  CONNECTIONS="$WORKERS"
else
  require_positive_int "--connections" "$CONNECTIONS"
fi
require_nonnegative_int "--queue-cap" "$QUEUE_CAP"

resolve_compose
resolve_benchmark_platform
warn_if_benchmark_platform_is_not_native
echo "benchmark platform: ${BENCHMARK_PLATFORM:-docker default}"
echo "benchmark clients: workers=$WORKERS connections=$CONNECTIONS queue_cap=$QUEUE_CAP"

mkdir -p "$RESULTS_ROOT"
RESULTS_ROOT="$(cd "$RESULTS_ROOT" && pwd)"
STAMP="$(date +%Y%m%d-%H%M%S)"
RUN_ROOT="$RESULTS_ROOT/${STAMP}-${SCENARIO}"
mkdir -p "$RUN_ROOT"

target_endpoints() {
  case "$1" in
    holostore) printf '%s\n' holostore1:6379,holostore2:6379,holostore3:6379 ;;
    etcd) printf '%s\n' etcd1:2379,etcd2:2379,etcd3:2379 ;;
  esac
}

run_one() {
  local target="$1"
  local run_dir="$RUN_ROOT/$target"
  local project="holobench_${target}_${STAMP}"
  local endpoints
  local platform_override
  local status=0
  local failed_phase=""
  local failure_message=""
  local report_status=0
  local -a compose services up_args bench_args report_args

  mkdir -p "$run_dir"
  endpoints="$(target_endpoints "$target")"
  compose=(-f "$SCRIPT_DIR/docker-compose.${target}.yml" -f "$SCRIPT_DIR/docker-compose.runner.yml")
  platform_override="$RUN_ROOT/compose-platform-${target}.yml"
  if write_platform_compose_override "$target" "$platform_override"; then
    compose+=(-f "$platform_override")
  fi
  case "$target" in
    holostore) services=(holostore1 holostore2 holostore3) ;;
    etcd) services=(etcd1 etcd2 etcd3) ;;
  esac

  export BENCH_RESULTS_DIR="$run_dir"

  if [[ "$status" == "0" ]]; then
    up_args=(up -d --wait --no-build)
    "${COMPOSE[@]}" -p "$project" "${compose[@]}" "${up_args[@]}" "${services[@]}" || {
      status=$?
      failed_phase="compose-up"
      failure_message="Docker Compose did not start healthy target services"
    }
  fi

  if [[ "$status" == "0" ]]; then
    bench_args=(
      benchtool run
      --target "$target"
      --endpoints "$endpoints"
      --scenario "$SCENARIO"
      --duration "$DURATION"
      --rate "$RATE"
      --workers "$WORKERS"
      --connections "$CONNECTIONS"
      --worker-headroom "$WORKER_HEADROOM"
      --queue-cap "$QUEUE_CAP"
      --write-pct "$WRITE_PCT"
      --contention "$CONTENTION"
      --keys "$KEYS"
      --hot-keys "$HOT_KEYS"
      --hot-pct "$HOT_PCT"
      --value-bytes "$VALUE_BYTES"
      --timeout "$TIMEOUT"
      --seed "$SEED"
      --preload-workers "$PRELOAD_WORKERS"
      --preload-timeout "$PRELOAD_TIMEOUT"
      --preload-retries "$PRELOAD_RETRIES"
      --preload="$([[ "$PRELOAD" == "1" ]] && echo true || echo false)"
      --out-dir /results
    )
    "${COMPOSE[@]}" -p "$project" "${compose[@]}" run --rm bench-runner "${bench_args[@]}" || {
      status=$?
      failed_phase="benchmark"
      failure_message="benchmark runner failed before producing a complete run"
    }
  fi

  if [[ "$status" != "0" ]]; then
    if ! write_failure_json "$run_dir/failure.json" "$target" "$failed_phase" "$status" "$failure_message"; then
      echo "warning: failed to write failure metadata: $run_dir/failure.json" >&2
    fi
  fi

  if [[ "$status" == "0" ]]; then
    report_args=(
      python /usr/local/bin/bench_report.py
      --run-dir /results
      --out /results/report.md
      --title "${target} Benchmark Report"
    )
    "${COMPOSE[@]}" -p "$project" "${compose[@]}" run --rm bench-runner "${report_args[@]}" || {
      report_status=$?
      if python3 "$SCRIPT_DIR/report.py" --run-dir "$run_dir" --out "$run_dir/report.md" --title "${target} Benchmark Report"; then
        :
      else
        status=$report_status
        failed_phase="target-report"
        failure_message="target Markdown report generation failed"
        if ! write_failure_json "$run_dir/failure.json" "$target" "$failed_phase" "$status" "$failure_message"; then
          echo "warning: failed to write failure metadata: $run_dir/failure.json" >&2
        fi
      fi
    }
  else
    report_args=(
      python /usr/local/bin/bench_report.py
      --run-dir /results
      --out /results/report.md
      --title "${target} Benchmark Report"
    )
    "${COMPOSE[@]}" -p "$project" "${compose[@]}" run --rm bench-runner "${report_args[@]}" \
      || python3 "$SCRIPT_DIR/report.py" --run-dir "$run_dir" --out "$run_dir/report.md" --title "${target} Benchmark Report" \
      || true
  fi

  if [[ "$KEEP_CLUSTER" != "1" ]]; then
    "${COMPOSE[@]}" -p "$project" "${compose[@]}" down -v >/dev/null 2>&1 || true
  else
    echo "kept Docker Compose project: $project"
  fi

  return "$status"
}

targets=()
if [[ "$TARGET" == "both" ]]; then
  targets=(holostore etcd)
else
  targets=("$TARGET")
fi

if [[ "$BUILD" == "1" ]]; then
  build_started="$(now_seconds)"
  build_benchmark_images "$TARGET"
  BUILD_SECONDS="$(elapsed_seconds_since "$build_started")"
fi

benchmark_started="$(now_seconds)"
RUN_STATUS=0
for target in "${targets[@]}"; do
  echo "==> running $target benchmark"
  if run_one "$target"; then
    :
  else
    status=$?
    echo "benchmark target failed: $target exit=$status" >&2
    RUN_STATUS="$status"
  fi
done

if [[ "${#targets[@]}" -gt 1 ]]; then
  report_cmd=(docker run --rm)
  if [[ -n "$BENCHMARK_PLATFORM" ]]; then
    report_cmd+=(--platform "$BENCHMARK_PLATFORM")
  fi
  report_cmd+=(
    -v "$RUN_ROOT:/results"
    "$BENCH_RUNNER_IMAGE"
    python /usr/local/bin/bench_report.py
      --run-dir /results/holostore
      --run-dir /results/etcd
      --out /results/report.md
      --title "HoloStore vs etcd Benchmark Report"
  )
  if "${report_cmd[@]}"; then
    :
  else
    report_status="$?"
    if python3 "$SCRIPT_DIR/report.py" \
      --run-dir "$RUN_ROOT/holostore" \
      --run-dir "$RUN_ROOT/etcd" \
      --out "$RUN_ROOT/report.md" \
      --title "HoloStore vs etcd Benchmark Report"; then
      :
    else
      if ! write_failure_json "$RUN_ROOT/failure.json" "$TARGET" "combined-report" "$report_status" "combined Markdown report generation failed"; then
        echo "warning: failed to write failure metadata: $RUN_ROOT/failure.json" >&2
      fi
      if [[ "$RUN_STATUS" == "0" ]]; then
        RUN_STATUS="$report_status"
      fi
    fi
  fi
fi
BENCHMARK_SECONDS="$(elapsed_seconds_since "$benchmark_started")"
TOTAL_SECONDS="$(elapsed_seconds_since "$SCRIPT_STARTED_SECONDS")"

TIMING_JSON="$RUN_ROOT/timing.json"
write_timing_json "$TIMING_JSON"
annotate_reports_with_timing "$TIMING_JSON"
print_timing_summary
echo "results: $RUN_ROOT"
print_final_report_path
exit "$RUN_STATUS"
