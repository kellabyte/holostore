#!/usr/bin/env bash
set -euo pipefail

# Purpose:
# - Measure merge-time availability and latency stability under sustained write
#   load, then compare baseline (`HEAD`) against the current working tree.
#
# Design:
# - Runs two scenarios with identical load and cluster config:
#   1) baseline in a temporary detached worktree at `HEAD`
#   2) current workspace (including local changes)
# - For each scenario, it starts a 3-node cluster, waits for write quorum,
#   starts `redis-benchmark`, triggers a merge mid-run, and extracts:
#   - overall throughput/latency summary
#   - merge-window min RPS and max avg latency
#   - benchmark-side error count
#
# Inputs:
# - Environment variables to tune load and output:
#   OUT_DIR, BENCH_CLIENTS, BENCH_PIPELINE, BENCH_REQUESTS, BENCH_KEYSPACE,
#   BENCH_VALUE_SIZE, BENCH_WARMUP_SEC, BENCH_WINDOW_PRE, BENCH_WINDOW_POST.
#
# Outputs:
# - Per-scenario raw logs and parsed metrics under `$OUT_DIR/<timestamp>/`
# - A comparison report at `$OUT_DIR/<timestamp>/comparison.txt`

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/.tmp/merge_cutover_bench}"
TS="$(date +"%Y%m%d_%H%M%S")"
RUN_DIR="$OUT_DIR/$TS"
mkdir -p "$RUN_DIR"

BENCH_HOST="${BENCH_HOST:-127.0.0.1}"
BENCH_PORT="${BENCH_PORT:-16379}"
BENCH_CLIENTS="${BENCH_CLIENTS:-50}"
BENCH_PIPELINE="${BENCH_PIPELINE:-100}"
BENCH_REQUESTS="${BENCH_REQUESTS:-1000000}"
BENCH_KEYSPACE="${BENCH_KEYSPACE:-1000000}"
BENCH_VALUE_SIZE="${BENCH_VALUE_SIZE:-8}"
BENCH_WARMUP_SEC="${BENCH_WARMUP_SEC:-5}"
BENCH_WINDOW_PRE="${BENCH_WINDOW_PRE:-2}"
BENCH_WINDOW_POST="${BENCH_WINDOW_POST:-8}"
SCENARIO_BUILD="${SCENARIO_BUILD:-1}"

BASE_GRPC_TARGET="${BASE_GRPC_TARGET:-127.0.0.1:15051}"

if ! command -v redis-benchmark >/dev/null 2>&1; then
  echo "error: redis-benchmark not found in PATH" >&2
  exit 1
fi
if ! command -v redis-cli >/dev/null 2>&1; then
  echo "error: redis-cli not found in PATH" >&2
  exit 1
fi

log() {
  printf '[%s] %s\n' "$(date +"%H:%M:%S")" "$*"
}

wait_for_write_quorum() {
  local host="$1"
  local port="$2"
  local timeout_s="${3:-60}"
  local deadline
  deadline=$((SECONDS + timeout_s))
  while (( SECONDS < deadline )); do
    # Write-path readiness matters for this benchmark, not only TCP readiness.
    local resp
    resp="$(redis-cli -h "$host" -p "$port" SET __merge_bench_ready__ 1 2>/dev/null || true)"
    if [[ "$resp" == "OK" ]]; then
      return 0
    fi
    sleep 0.25
  done
  return 1
}

sample_count() {
  local bench_log="$1"
  if [[ ! -f "$bench_log" ]]; then
    echo 0
    return 0
  fi
  tr '\r' '\n' <"$bench_log" | awk '/rps=/{c+=1} END{print c+0}'
}

parse_bench_metrics() {
  local bench_log="$1"
  local merge_sample="$2"
  local out_file="$3"
  local pre_window="$4"
  local post_window="$5"

  local intervals="$out_file.intervals"
  tr '\r' '\n' <"$bench_log" \
    | awk '
      /rps=/ {
        idx += 1;
        rps = "";
        avg = "";
        n = split($0, parts_rps, "rps=");
        if (n >= 2) {
          split(parts_rps[2], tail_rps, " ");
          rps = tail_rps[1];
        }
        m = split($0, parts_avg, "avg_msec=");
        if (m >= 2) {
          split(parts_avg[2], tail_avg, " ");
          avg = tail_avg[1];
        }
        if (rps != "" && avg != "") {
          gsub(/[^0-9.]/, "", rps);
          gsub(/[^0-9.]/, "", avg);
          if (rps != "" && avg != "") {
            printf "%d %.6f %.6f\n", idx, rps + 0.0, avg + 0.0;
          }
        }
      }
    ' >"$intervals"

  local summary throughput avg min p50 p95 p99 max
  summary="$(awk '/Summary:/{flag=1} flag{print}' "$bench_log")"
  throughput="$(printf '%s\n' "$summary" | awk '/throughput summary:/{print $3; exit}')"
  avg="$(printf '%s\n' "$summary" | awk '/^[[:space:]]*[0-9.]+/{print $1; exit}')"
  min="$(printf '%s\n' "$summary" | awk '/^[[:space:]]*[0-9.]+/{print $2; exit}')"
  p50="$(printf '%s\n' "$summary" | awk '/^[[:space:]]*[0-9.]+/{print $3; exit}')"
  p95="$(printf '%s\n' "$summary" | awk '/^[[:space:]]*[0-9.]+/{print $4; exit}')"
  p99="$(printf '%s\n' "$summary" | awk '/^[[:space:]]*[0-9.]+/{print $5; exit}')"
  max="$(printf '%s\n' "$summary" | awk '/^[[:space:]]*[0-9.]+/{print $6; exit}')"

  local merge_start merge_end
  merge_start=$((merge_sample - pre_window))
  if (( merge_start < 1 )); then
    merge_start=1
  fi
  merge_end=$((merge_sample + post_window))

  # Compute windowed stats around merge trigger sample.
  local merge_min_rps merge_max_avg merge_avg_rps
  merge_min_rps="$(awk -v s="$merge_start" -v e="$merge_end" '
    NR>=s && NR<=e {
      if (min=="" || $2<min) min=$2;
    }
    END { if (min=="") print "nan"; else printf "%.6f", min; }
  ' "$intervals")"
  merge_max_avg="$(awk -v s="$merge_start" -v e="$merge_end" '
    NR>=s && NR<=e {
      if (max=="" || $3>max) max=$3;
    }
    END { if (max=="") print "nan"; else printf "%.6f", max; }
  ' "$intervals")"
  merge_avg_rps="$(awk -v s="$merge_start" -v e="$merge_end" '
    NR>=s && NR<=e { sum+=$2; n+=1; }
    END { if (n==0) print "nan"; else printf "%.6f", (sum/n); }
  ' "$intervals")"

  local err_count
  err_count="$(tr '\r' '\n' <"$bench_log" | rg -N "ERR |Error from server|failed" -c || true)"
  if [[ -z "$err_count" ]]; then
    err_count=0
  fi

  {
    echo "throughput_rps=$throughput"
    echo "lat_avg_ms=$avg"
    echo "lat_min_ms=$min"
    echo "lat_p50_ms=$p50"
    echo "lat_p95_ms=$p95"
    echo "lat_p99_ms=$p99"
    echo "lat_max_ms=$max"
    echo "merge_sample=$merge_sample"
    echo "merge_window_start=$merge_start"
    echo "merge_window_end=$merge_end"
    echo "merge_window_min_rps=$merge_min_rps"
    echo "merge_window_avg_rps=$merge_avg_rps"
    echo "merge_window_max_avg_ms=$merge_max_avg"
    echo "bench_error_count=$err_count"
  } >"$out_file"
}

run_scenario() {
  local label="$1"
  local workdir="$2"
  local scenario_dir="$RUN_DIR/$label"
  local bench_log="$scenario_dir/redis-benchmark.log"
  local start_log="$scenario_dir/start_cluster.log"
  local merge_log="$scenario_dir/merge.log"
  local metrics_file="$scenario_dir/metrics.env"

  mkdir -p "$scenario_dir"
  log "[$label] starting scenario in $workdir"

  pushd "$workdir" >/dev/null

  # Deterministic cluster shape for merge benchmark.
  export HOLO_INITIAL_RANGES=2
  export HOLO_ROUTING_MODE=range
  export HOLO_RANGE_SPLIT_MIN_KEYS=1000000
  export HOLO_RANGE_SPLIT_MIN_QPS=0
  export HOLO_RANGE_MERGE_MAX_KEYS=0
  export HOLO_REBALANCE_ENABLED=false
  export HOLO_BUILD="$SCENARIO_BUILD"

  ./scripts/cleanup_cluster.sh >/dev/null 2>&1 || true
  ./scripts/start_cluster.sh >"$start_log" 2>&1

  if ! wait_for_write_quorum "$BENCH_HOST" "$BENCH_PORT" 90; then
    echo "error: [$label] write quorum was not ready" >&2
    ./scripts/cleanup_cluster.sh >/dev/null 2>&1 || true
    popd >/dev/null
    return 1
  fi

  # Seed both keyspaces so both initial ranges have live data.
  redis-benchmark -h "$BENCH_HOST" -p "$BENCH_PORT" \
    -c 20 -n 50000 -r 50000 -P 16 -d "$BENCH_VALUE_SIZE" \
    set 0seed:__rand_int__ x >/dev/null 2>&1 || true
  redis-benchmark -h "$BENCH_HOST" -p "$BENCH_PORT" \
    -c 20 -n 50000 -r 50000 -P 16 -d "$BENCH_VALUE_SIZE" \
    set kseed:__rand_int__ x >/dev/null 2>&1 || true

  log "[$label] running redis-benchmark"
  redis-benchmark -h "$BENCH_HOST" -p "$BENCH_PORT" \
    -c "$BENCH_CLIENTS" -n "$BENCH_REQUESTS" -r "$BENCH_KEYSPACE" -P "$BENCH_PIPELINE" \
    -d "$BENCH_VALUE_SIZE" set bench:__rand_int__ x >"$bench_log" 2>&1 &
  local bench_pid=$!

  sleep "$BENCH_WARMUP_SEC"
  local merge_sample
  merge_sample="$(sample_count "$bench_log")"

  local holoctl_bin
  holoctl_bin="$workdir/target/release/holoctl"
  if [[ ! -x "$holoctl_bin" ]]; then
    # Fallback to debug if release holoctl is unavailable.
    holoctl_bin="$workdir/target/debug/holoctl"
  fi

  log "[$label] triggering merge at sample $merge_sample"
  local merge_rc=0
  "$holoctl_bin" --target "$BASE_GRPC_TARGET" merge --left-shard-id 1 >"$merge_log" 2>&1 || merge_rc=$?

  local bench_rc=0
  wait "$bench_pid" || bench_rc=$?

  parse_bench_metrics "$bench_log" "$merge_sample" "$metrics_file" "$BENCH_WINDOW_PRE" "$BENCH_WINDOW_POST"
  {
    echo "merge_rc=$merge_rc"
    echo "bench_rc=$bench_rc"
  } >>"$metrics_file"

  ./scripts/cleanup_cluster.sh >/dev/null 2>&1 || true
  popd >/dev/null
  log "[$label] scenario complete"
}

write_comparison() {
  local baseline_file="$1"
  local current_file="$2"
  local out="$3"
  # shellcheck disable=SC1090
  source "$baseline_file"
  local b_throughput="$throughput_rps"
  local b_lat_p95="$lat_p95_ms"
  local b_lat_p99="$lat_p99_ms"
  local b_merge_min_rps="$merge_window_min_rps"
  local b_merge_max_avg="$merge_window_max_avg_ms"
  local b_err="$bench_error_count"
  local b_merge_rc="$merge_rc"
  local b_bench_rc="$bench_rc"

  # shellcheck disable=SC1090
  source "$current_file"
  local c_throughput="$throughput_rps"
  local c_lat_p95="$lat_p95_ms"
  local c_lat_p99="$lat_p99_ms"
  local c_merge_min_rps="$merge_window_min_rps"
  local c_merge_max_avg="$merge_window_max_avg_ms"
  local c_err="$bench_error_count"
  local c_merge_rc="$merge_rc"
  local c_bench_rc="$bench_rc"

  {
    echo "Merge Cutover Benchmark Comparison"
    echo "timestamp=$TS"
    echo
    echo "load: redis-benchmark -h $BENCH_HOST -p $BENCH_PORT -c $BENCH_CLIENTS -n $BENCH_REQUESTS -r $BENCH_KEYSPACE -P $BENCH_PIPELINE -d $BENCH_VALUE_SIZE set bench:__rand_int__ x"
    echo "merge trigger: holoctl --target $BASE_GRPC_TARGET merge --left-shard-id 1"
    echo "merge window: [sample-$BENCH_WINDOW_PRE, sample+$BENCH_WINDOW_POST]"
    echo
    echo "baseline throughput_rps=$b_throughput"
    echo "current  throughput_rps=$c_throughput"
    echo
    echo "baseline lat_p95_ms=$b_lat_p95 lat_p99_ms=$b_lat_p99"
    echo "current  lat_p95_ms=$c_lat_p95 lat_p99_ms=$c_lat_p99"
    echo
    echo "baseline merge_window_min_rps=$b_merge_min_rps merge_window_max_avg_ms=$b_merge_max_avg"
    echo "current  merge_window_min_rps=$c_merge_min_rps merge_window_max_avg_ms=$c_merge_max_avg"
    echo
    echo "baseline bench_error_count=$b_err merge_rc=$b_merge_rc bench_rc=$b_bench_rc"
    echo "current  bench_error_count=$c_err merge_rc=$c_merge_rc bench_rc=$c_bench_rc"
  } >"$out"
}

cleanup_worktree() {
  local worktree="$1"
  if [[ -n "$worktree" && -d "$worktree" ]]; then
    git -C "$ROOT_DIR" worktree remove "$worktree" --force >/dev/null 2>&1 || true
    rm -rf "$worktree"
  fi
}

main() {
  local baseline_dir="$ROOT_DIR/.tmp/merge-bench-baseline-$TS"
  local comparison_file="$RUN_DIR/comparison.txt"

  log "creating detached baseline worktree"
  mkdir -p "$ROOT_DIR/.tmp"
  git -C "$ROOT_DIR" worktree add --detach "$baseline_dir" HEAD >/dev/null

  run_scenario "baseline_head" "$baseline_dir"
  run_scenario "current_worktree" "$ROOT_DIR"

  write_comparison \
    "$RUN_DIR/baseline_head/metrics.env" \
    "$RUN_DIR/current_worktree/metrics.env" \
    "$comparison_file"

  cleanup_worktree "$baseline_dir"

  log "comparison report: $comparison_file"
  cat "$comparison_file"
}

main "$@"
