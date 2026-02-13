#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
BENCH_DIR="$ROOT_DIR/crates/holo_fusion/scripts/benchmarks"
RESULTS_ROOT="$BENCH_DIR/results"
mkdir -p "$RESULTS_ROOT"

if ! command -v psql >/dev/null 2>&1; then
  echo "error: psql is required" >&2
  exit 1
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "error: curl is required" >&2
  exit 1
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is required" >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "error: python3 is required" >&2
  exit 1
fi

PG_URL="${PG_URL:-postgresql://postgres@127.0.0.1:55432/datafusion?sslmode=disable}"
AUTO_CLUSTER="${AUTO_CLUSTER:-1}"
STOP_CLUSTER_ON_EXIT="${STOP_CLUSTER_ON_EXIT:-1}"
TOPOLOGY_TARGET="${TOPOLOGY_TARGET:-127.0.0.1:15051}"
HOLOCTL_BIN="${HOLOCTL_BIN:-$ROOT_DIR/target/debug/holoctl}"
METRICS_PORTS="${METRICS_PORTS:-18081,18082,18083}"
METRICS_HOST="${METRICS_HOST:-127.0.0.1}"

BATCH_20K_ROWS="${BATCH_20K_ROWS:-20000}"
BATCH_50K_ROWS="${BATCH_50K_ROWS:-50000}"
BATCH_20K_RUNS="${BATCH_20K_RUNS:-3}"
BATCH_50K_RUNS="${BATCH_50K_RUNS:-3}"

SPLIT_CHURN_ENABLED="${SPLIT_CHURN_ENABLED:-1}"
SPLIT_AFTER_BATCH_INDEX="${SPLIT_AFTER_BATCH_INDEX:-2}"
SPLIT_ORDERS_TABLE_ID="${SPLIT_ORDERS_TABLE_ID:-100}"
SPLIT_ORDERS_PRIMARY_KEY="${SPLIT_ORDERS_PRIMARY_KEY:-50000}"
AUTO_CLUSTER_MAX_SHARDS="${AUTO_CLUSTER_MAX_SHARDS:-8}"

P95_SLO_20K_MS="${P95_SLO_20K_MS:-8000}"
P95_SLO_50K_MS="${P95_SLO_50K_MS:-20000}"
ENFORCE_SLO="${ENFORCE_SLO:-1}"

RUN_ID="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="$RESULTS_ROOT/phase9_sales_facts_canary_${RUN_ID}"
SNAPSHOT_DIR="$RUN_DIR/snapshots"
mkdir -p "$SNAPSHOT_DIR"
LOG_FILE="$RUN_DIR/run.log"
SUMMARY_FILE="$RUN_DIR/summary.txt"
STATS_CSV="$RUN_DIR/batch_stats.csv"

touch "$LOG_FILE"
exec > >(tee -a "$LOG_FILE") 2>&1

cleanup() {
  if [[ "$AUTO_CLUSTER" == "1" && "$STOP_CLUSTER_ON_EXIT" == "1" ]]; then
    echo "==> stopping dev cluster"
    "$ROOT_DIR/crates/holo_fusion/scripts/stop_cluster.sh" || true
  fi
}
trap cleanup EXIT

run_holoctl() {
  "$HOLOCTL_BIN" --target "$TOPOLOGY_TARGET" "$@"
}

trigger_split_churn() {
  local batch_index="$1"
  local out_file="$SNAPSHOT_DIR/split_event_batch_${batch_index}.txt"
  split_hex="$(split_key_hex "$SPLIT_ORDERS_TABLE_ID" "$SPLIT_ORDERS_PRIMARY_KEY")"
  if run_holoctl split --hex --split-key "$split_hex" >"$out_file" 2>&1; then
    split_done=1
    return 0
  fi
  echo "warning: split-churn request failed; see $out_file"
  return 1
}

ensure_holoctl_bin() {
  if [[ -x "$HOLOCTL_BIN" ]]; then
    return
  fi
  echo "==> building holoctl binary"
  cargo build -q -p holo_store --bin holoctl
}

now_ns() {
  python3 - <<'PY'
import time
print(time.time_ns())
PY
}

wait_for_pg() {
  local attempts=0
  local max_attempts=120
  until psql "$PG_URL" -v ON_ERROR_STOP=1 -Atqc "SELECT 1" >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    if [[ "$attempts" -ge "$max_attempts" ]]; then
      echo "error: timed out waiting for postgres endpoint at $PG_URL" >&2
      return 1
    fi
    sleep 1
  done
}

split_key_hex() {
  local table_id="$1"
  local primary_key="$2"
  python3 - "$table_id" "$primary_key" <<'PY'
import sys
table_id = int(sys.argv[1])
primary_key = int(sys.argv[2])
ordered = (primary_key & ((1 << 64) - 1)) ^ (1 << 63)
out = (
    bytes([0x20])
    + table_id.to_bytes(8, "big", signed=False)
    + bytes([0x02, 0x00])
    + ordered.to_bytes(8, "big", signed=False)
)
print(out.hex())
PY
}

capture_snapshot() {
  local tag="$1"
  local topology_file="$SNAPSHOT_DIR/${tag}_topology.txt"
  echo "==> snapshot topology: $topology_file"
  run_holoctl topology >"$topology_file"

  IFS=',' read -r -a ports <<<"$METRICS_PORTS"
  local idx=0
  for port in "${ports[@]}"; do
    idx=$((idx + 1))
    local metrics_file="$SNAPSHOT_DIR/${tag}_metrics_node${idx}.txt"
    echo "==> snapshot metrics node${idx}: $metrics_file"
    curl -fsS "http://${METRICS_HOST}:${port}/metrics" >"$metrics_file"
  done
}

run_sql() {
  local sql="$1"
  psql "$PG_URL" -v ON_ERROR_STOP=1 -Atqc "$sql"
}

current_count() {
  run_sql "SELECT COUNT(order_id)::BIGINT FROM sales_facts;"
}

run_batch() {
  local label="$1"
  local rows="$2"
  local index="$3"
  local before_count="$4"

  local before_tag
  before_tag="$(printf "batch_%02d_%s_before" "$index" "$label")"
  capture_snapshot "$before_tag"

  local start_ns
  start_ns="$(now_ns)"
  run_sql "INSERT INTO sales_facts (
    order_id, customer_id, merchant_id, region_id, event_day, status, amount_cents
  )
  WITH base AS (
    SELECT COALESCE(MAX(order_id), 0) AS max_id
    FROM sales_facts
  )
  SELECT
    base.max_id + i AS order_id,
    100000 + (i % 500000) AS customer_id,
    1 + (i % 20000) AS merchant_id,
    1 + (i % 20) AS region_id,
    1 + ((i - 1) / 86400) AS event_day,
    CASE
      WHEN i % 10 < 6 THEN 'paid'
      WHEN i % 10 < 8 THEN 'shipped'
      WHEN i % 10 = 8 THEN 'pending'
      ELSE 'cancelled'
    END AS status,
    100 + ((i * 37) % 20000) AS amount_cents
  FROM base
  CROSS JOIN generate_series(1, ${rows}) AS g(i);"
  local end_ns
  end_ns="$(now_ns)"
  local elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))

  local after_count
  after_count="$(current_count)"
  local delta=$((after_count - before_count))
  local pass="ok"
  if [[ "$delta" -ne "$rows" ]]; then
    pass="fail"
  fi

  local after_tag
  after_tag="$(printf "batch_%02d_%s_after" "$index" "$label")"
  capture_snapshot "$after_tag"

  printf "%s,%s,%s,%s,%s,%s\n" \
    "$index" "$label" "$rows" "$elapsed_ms" "$delta" "$pass" >>"$STATS_CSV"
  echo "batch index=$index label=$label rows=$rows elapsed_ms=$elapsed_ms delta=$delta status=$pass"
}

p95_from_csv() {
  local label="$1"
  local count
  count="$(awk -F',' -v label="$label" 'NR > 1 && $2 == label { c += 1 } END { print c + 0 }' "$STATS_CSV")"
  if [[ "$count" -eq 0 ]]; then
    echo 0
    return
  fi
  local idx=$(( (count * 95 + 99) / 100 ))
  awk -F',' -v label="$label" 'NR > 1 && $2 == label { print $4 }' "$STATS_CSV" \
    | sort -n \
    | sed -n "${idx}p"
}

batch_failures() {
  awk -F',' 'NR > 1 && $6 != "ok" { c += 1 } END { print c + 0 }' "$STATS_CSV"
}

if [[ "$AUTO_CLUSTER" == "1" ]]; then
  echo "==> starting 3-node dev cluster"
  "$ROOT_DIR/crates/holo_fusion/scripts/stop_cluster.sh" || true
  HOLO_FUSION_CLEANUP=1 \
  HOLO_FUSION_BUILD=1 \
  HOLO_FUSION_HOLOSTORE_MAX_SHARDS="$AUTO_CLUSTER_MAX_SHARDS" \
  HOLO_FUSION_HOLOSTORE_INITIAL_RANGES=1 \
  HOLO_FUSION_HOLOSTORE_ROUTING_MODE=range \
  "$ROOT_DIR/crates/holo_fusion/scripts/start_cluster.sh"
fi

echo "==> waiting for SQL endpoint readiness"
wait_for_pg
ensure_holoctl_bin

echo "==> preparing sales_facts table"
run_sql "CREATE TABLE IF NOT EXISTS sales_facts (
  order_id BIGINT NOT NULL,
  customer_id BIGINT NOT NULL,
  merchant_id BIGINT NOT NULL,
  region_id BIGINT NOT NULL,
  event_day BIGINT NOT NULL,
  status VARCHAR NOT NULL,
  amount_cents BIGINT NOT NULL,
  PRIMARY KEY (order_id)
);"
run_sql "DELETE FROM sales_facts
WHERE order_id >= 0 AND order_id <= 9223372036854775807;"

baseline_count="$(current_count)"

echo "index,label,rows,elapsed_ms,row_delta,status" >"$STATS_CSV"
capture_snapshot "run_start"

total_batches=$((BATCH_20K_RUNS + BATCH_50K_RUNS))
split_done=0
batch_index=0

for ((i = 1; i <= BATCH_20K_RUNS; i++)); do
  batch_index=$((batch_index + 1))
  before_count="$(current_count)"
  run_batch "20k" "$BATCH_20K_ROWS" "$batch_index" "$before_count"
  if [[ "$SPLIT_CHURN_ENABLED" == "1" && "$split_done" -eq 0 && "$batch_index" -eq "$SPLIT_AFTER_BATCH_INDEX" ]]; then
    echo "==> triggering split-churn after batch $batch_index"
    trigger_split_churn "$batch_index" || true
  fi
done

for ((i = 1; i <= BATCH_50K_RUNS; i++)); do
  batch_index=$((batch_index + 1))
  before_count="$(current_count)"
  run_batch "50k" "$BATCH_50K_ROWS" "$batch_index" "$before_count"
  if [[ "$SPLIT_CHURN_ENABLED" == "1" && "$split_done" -eq 0 && "$batch_index" -eq "$SPLIT_AFTER_BATCH_INDEX" ]]; then
    echo "==> triggering split-churn after batch $batch_index"
    trigger_split_churn "$batch_index" || true
  fi
done

capture_snapshot "run_end"

p95_20k="$(p95_from_csv "20k")"
p95_50k="$(p95_from_csv "50k")"
failed_batches="$(batch_failures)"
final_count="$(current_count)"
expected_count=$((baseline_count + BATCH_20K_RUNS * BATCH_20K_ROWS + BATCH_50K_RUNS * BATCH_50K_ROWS))

slo_status="PASS"
if [[ "$failed_batches" -ne 0 ]]; then
  slo_status="FAIL"
fi
if [[ "$final_count" -ne "$expected_count" ]]; then
  slo_status="FAIL"
fi
if [[ "$p95_20k" -gt "$P95_SLO_20K_MS" ]]; then
  slo_status="FAIL"
fi
if [[ "$p95_50k" -gt "$P95_SLO_50K_MS" ]]; then
  slo_status="FAIL"
fi
if [[ "$SPLIT_CHURN_ENABLED" == "1" && "$split_done" -eq 0 ]]; then
  slo_status="FAIL"
fi

cat >"$SUMMARY_FILE" <<EOF
run_id=$RUN_ID
pg_url=$PG_URL
topology_target=$TOPOLOGY_TARGET
auto_cluster_max_shards=$AUTO_CLUSTER_MAX_SHARDS
total_batches=$total_batches
baseline_row_count=$baseline_count
batch_20k_runs=$BATCH_20K_RUNS
batch_20k_rows=$BATCH_20K_ROWS
batch_50k_runs=$BATCH_50K_RUNS
batch_50k_rows=$BATCH_50K_ROWS
split_churn_enabled=$SPLIT_CHURN_ENABLED
split_triggered=$split_done
split_after_batch_index=$SPLIT_AFTER_BATCH_INDEX
slo_p95_20k_ms=$P95_SLO_20K_MS
slo_p95_50k_ms=$P95_SLO_50K_MS
observed_p95_20k_ms=$p95_20k
observed_p95_50k_ms=$p95_50k
failed_batches=$failed_batches
expected_final_row_count=$expected_count
observed_final_row_count=$final_count
slo_status=$slo_status
stats_csv=$STATS_CSV
snapshots_dir=$SNAPSHOT_DIR
log_file=$LOG_FILE
EOF

echo
echo "==> Phase 9 sales_facts canary summary"
cat "$SUMMARY_FILE"
echo
echo "results_dir=$RUN_DIR"

if [[ "$ENFORCE_SLO" == "1" && "$slo_status" != "PASS" ]]; then
  echo "error: Phase 9 canary SLO gate failed" >&2
  exit 1
fi
