# Phase 9 Sales Facts Canary and SLO Gate

This package closes out Phase 9 with a reproducible ingest canary and explicit pass/fail gates.

## Scope

Workload:
- `sales_facts` bulk ingest batches at `20k` and `50k` rows.
- Insert shape: `INSERT ... SELECT ... generate_series(...)` with `MAX(order_id)` offset.
- Split-churn fault: force a range split during ingest.

Artifacts:
- `/metrics` snapshots before and after every batch.
- `holoctl topology` snapshots before and after every batch.
- Per-batch latency + row growth stats.

Script:
- `crates/holo_fusion/scripts/benchmarks/run_phase9_sales_facts_canary.sh`

## Pass/Fail Contract

The canary declares `PASS` only when all checks hold:

1. No partial apply:
- Every batch reports row delta equal to requested batch size.

2. Correct row growth:
- Final `COUNT(order_id)` equals:
  - `BATCH_20K_RUNS * BATCH_20K_ROWS + BATCH_50K_RUNS * BATCH_50K_ROWS`.

3. Split-churn exercised:
- If `SPLIT_CHURN_ENABLED=1`, at least one split must be triggered during the run.

4. Latency SLO:
- `p95(20k)` <= `P95_SLO_20K_MS`.
- `p95(50k)` <= `P95_SLO_50K_MS`.

When `ENFORCE_SLO=1` (default), any failure returns non-zero exit status.

## Default Gate Values

- `P95_SLO_20K_MS=8000`
- `P95_SLO_50K_MS=20000`
- `BATCH_20K_RUNS=3`
- `BATCH_50K_RUNS=3`
- `SPLIT_CHURN_ENABLED=1`
- `SPLIT_AFTER_BATCH_INDEX=2`
- `AUTO_CLUSTER_MAX_SHARDS=8`

## Run

Automatic 3-node cluster (recommended):

```bash
./crates/holo_fusion/scripts/benchmarks/run_phase9_sales_facts_canary.sh
```

Against an already running cluster:

```bash
AUTO_CLUSTER=0 \
PG_URL='postgresql://postgres@127.0.0.1:55432/datafusion?sslmode=disable' \
TOPOLOGY_TARGET='127.0.0.1:15051' \
./crates/holo_fusion/scripts/benchmarks/run_phase9_sales_facts_canary.sh
```

## Output Layout

Results are written under:
- `crates/holo_fusion/scripts/benchmarks/results/phase9_sales_facts_canary_<timestamp>/`

Key files:
- `run.log`: full execution log.
- `summary.txt`: gate inputs + observed outputs + final PASS/FAIL.
- `batch_stats.csv`: per-batch latency and row-delta checks.
- `snapshots/*_topology.txt`: topology before/after each batch.
- `snapshots/*_metrics_node{1,2,3}.txt`: metrics snapshots before/after each batch.
