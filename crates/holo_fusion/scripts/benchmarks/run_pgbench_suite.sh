#!/usr/bin/env bash
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$BENCH_DIR/results"
mkdir -p "$RESULTS_DIR"

TARGET="${TARGET:-}"
PG_URL="${PG_URL:-}"
CONCURRENCY="${CONCURRENCY:-50}"
JOBS="${JOBS:-8}"
DURATION_SEC="${DURATION_SEC:-60}"
SEED_ROWS="${SEED_ROWS:-1000000}"
AUTO_SETUP="${AUTO_SETUP:-1}"

if ! command -v pgbench >/dev/null 2>&1; then
  echo "error: pgbench is required" >&2
  exit 1
fi

if ! command -v psql >/dev/null 2>&1; then
  echo "error: psql is required" >&2
  exit 1
fi

if [[ -z "$PG_URL" ]]; then
  case "$TARGET" in
    cockroach)
      PG_URL="postgresql://root@127.0.0.1:26257/holostore_bench?sslmode=disable"
      ;;
    holofusion)
      PG_URL="postgresql://datafusion@127.0.0.1:55432/datafusion?sslmode=disable"
      ;;
    *)
      cat <<EOF >&2
usage:
  TARGET=cockroach ./crates/holo_fusion/scripts/benchmarks/run_pgbench_suite.sh
  TARGET=holofusion ./crates/holo_fusion/scripts/benchmarks/run_pgbench_suite.sh
  PG_URL='postgresql://user@host:port/db?sslmode=disable' ./crates/holo_fusion/scripts/benchmarks/run_pgbench_suite.sh
EOF
      exit 1
      ;;
  esac
fi

if [[ "$AUTO_SETUP" == "1" ]]; then
  echo "==> applying schema"
  psql "$PG_URL" -v ON_ERROR_STOP=1 -f "$BENCH_DIR/schema.sql" >/dev/null

  echo "==> seeding baseline rows (SEED_ROWS=$SEED_ROWS)"
  psql "$PG_URL" -v ON_ERROR_STOP=1 -c "
    INSERT INTO orders (order_id, customer_id, status, total_cents, created_at)
    SELECT
      g,
      2000 + (g % 1000),
      CASE
        WHEN g % 4 = 0 THEN 'pending'
        WHEN g % 4 = 1 THEN 'paid'
        WHEN g % 4 = 2 THEN 'shipped'
        ELSE 'cancelled'
      END,
      1000 + (g % 50000),
      now() - (g || ' seconds')::interval
    FROM generate_series(1, ${SEED_ROWS}) AS gs(g)
    ON CONFLICT (order_id) DO NOTHING;
  " >/dev/null
fi

ts="$(date +%Y%m%d_%H%M%S)"
out="$RESULTS_DIR/pgbench_${ts}.out"

run_case() {
  local name="$1"
  local file="$2"
  echo "==> $name" | tee -a "$out"
  pgbench -n \
    -M prepared \
    -c "$CONCURRENCY" \
    -j "$JOBS" \
    -T "$DURATION_SEC" \
    -r \
    -f "$file" \
    "$PG_URL" | tee -a "$out"
  echo | tee -a "$out"
}

{
  echo "target=${TARGET:-custom}"
  echo "pg_url=$PG_URL"
  echo "concurrency=$CONCURRENCY jobs=$JOBS duration_sec=$DURATION_SEC seed_rows=$SEED_ROWS auto_setup=$AUTO_SETUP"
  echo
} | tee -a "$out"

run_case "insert workload" "$BENCH_DIR/insert.sql"
run_case "select workload" "$BENCH_DIR/select.sql"
run_case "update workload" "$BENCH_DIR/update.sql"

echo "results written to $out"
