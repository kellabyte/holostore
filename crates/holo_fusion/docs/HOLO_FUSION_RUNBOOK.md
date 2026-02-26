# HoloFusion Runbook and Rollout Guide

This document covers production operations through Phase 10 for `holo_fusion`.

## Scope

This runbook covers:

- Incident triage for SQL-path production issues.
- Capacity planning and runtime tuning.
- Upgrade and rollback checklists.

It assumes the current HoloFusion architecture:

- One embedded HoloStore node per `holo-fusion` process.
- Optional Ballista SQL mode enabled per node.
- PostgreSQL wire protocol for clients.

## Service Endpoints

Default per-node endpoints:

| Surface | Default |
| --- | --- |
| PostgreSQL wire | `127.0.0.1:55432` |
| Health server | `127.0.0.1:18081` |
| Embedded HoloStore gRPC | `127.0.0.1:15051` |

Health paths:

- `/live`: process liveness.
- `/ready`: readiness gate (`200` only when `HealthState::Ready`).
- `/state`: coarse state + detail (`Ready`, `Degraded`, `Bootstrapping`, `NotReady`).
- `/metrics`: text counters used for triage.

Local 3-node developer cluster scripts:

- Start: `./crates/holo_fusion/scripts/start_cluster.sh`
- Stop/cleanup: `./crates/holo_fusion/scripts/stop_cluster.sh`

## Quick Triage Checklist (First 5 Minutes)

1. Confirm process and readiness:

```bash
curl -sS http://127.0.0.1:18081/live
curl -i -sS http://127.0.0.1:18081/ready
curl -sS http://127.0.0.1:18081/state
```

2. Capture point-in-time metrics:

```bash
curl -sS http://127.0.0.1:18081/metrics > /tmp/hf.metrics.$(date +%s).txt
```

3. Check cluster topology and local-record convergence:

```bash
cargo run -q -p holo_store --bin holoctl -- --target 127.0.0.1:15051 topology
```

4. Validate SQL control plane:

```bash
psql "host=127.0.0.1 port=55432 user=datafusion dbname=datafusion sslmode=disable" -c \
"SELECT pg_catalog.pg_postmaster_start_time(), pg_catalog.pg_is_in_recovery(), pg_catalog.txid_current();"
```

5. Inspect node logs:

- `.cluster/holo_fusion/logs/node1.log`
- `.cluster/holo_fusion/logs/node2.log`
- `.cluster/holo_fusion/logs/node3.log`

## Key Metrics and What They Mean

High-signal counters from `/metrics`:

- `tx_conflict_count`: explicit transaction commit conflicts.
- `distributed_write_conflicts`: storage-layer write conflicts across shards.
- `statement_timeout_count`: timed-out statements (`SQLSTATE 57014`).
- `admission_reject_count`: overload rejections (`SQLSTATE 53300`).
- `scan_row_limit_reject_count`: scan guardrail trips (`SQLSTATE 54000`).
- `txn_stage_limit_reject_count`: transactional staging limit trips (`SQLSTATE 54000`).
- `distributed_write_shard_<n>_conflicts`: per-shard conflict hotspot signal.
- `query_execution_started` / `query_execution_completed` / `query_execution_failed`: end-to-end SQL execution lifecycle counters.
- `stage_events`: total query/stage timeline events emitted across planning/scan execution.
- `scan_retry_count` / `scan_reroute_count`: scan-path resiliency activity under failures/topology churn.
- `scan_chunk_count` / `scan_duplicate_rows_skipped`: storage scan chunk flow and idempotent merge behavior.
- `admission_<class>_queue_depth` / `_queue_depth_peak` / `_wait_ns_total`: per-class queue pressure and latency.
- `distributed_write_target_<target>_circuit_state`: per-target breaker state (`0=closed,1=open,2=half_open`).
- `distributed_write_target_<target>_circuit_open_count` / `_circuit_reject_count`: breaker trips and shed traffic.
- `distributed_write_target_<target>_retryable_failures`: retry-governance signal by endpoint.
- `ingest_rows_ingested_total`, `ingest_jobs_started/completed/failed`: bulk ingest throughput and stability.
- `ingest_job_<n>_rows_per_second`, `_queue_depth`, `_inflight_bytes`, `_status`: job-level ingest progress.

Use deltas/rates, not absolute values, during incident windows.

## Incident Playbooks

### 1) Conflict Spike (`40001`)

Symptoms:

- Client retries increase with `SQLSTATE 40001`.
- `tx_conflict_count` and `distributed_write_conflicts` rise rapidly.

Actions:

1. Identify hotspot shards:

```bash
curl -sS http://127.0.0.1:18081/metrics | rg "distributed_write_shard_.*_conflicts"
```

2. Confirm distribution skew:

```bash
cargo run -q -p holo_store --bin holoctl -- --target 127.0.0.1:15051 topology
```

3. Immediate mitigation:
- Reduce client-side write concurrency.
- Reduce conflicting key contention (application partitioning/key-space spread).
- If range-routing hotspot is obvious, split/rebalance with `holoctl` before restoring full load.

4. Verify recovery:
- Conflict counter rate returns to baseline.
- p95 write latency and retry depth normalize.

### 2) Degraded or Not Ready

Symptoms:

- `/ready` returns `503`.
- `/state` shows `Degraded` or `NotReady`.

Actions:

1. Capture state detail:

```bash
curl -sS http://127.0.0.1:18081/state
```

2. Classify:
- `Degraded` with Ballista fallback detail: SQL remains available with local DataFusion.
- `NotReady`: treat as traffic-blocking; remove node from traffic until recovered.

3. Validate cluster substrate:

```bash
cargo run -q -p holo_store --bin holoctl -- --target 127.0.0.1:15051 state
```

4. Recover:
- Restart failed node process.
- Re-check `/ready` and `topology` convergence before reintroducing traffic.

### 3) Overload, Timeouts, Guardrail Rejections

Symptoms:

- `SQLSTATE 57014`, `53300`, `54000`.
- Rising `statement_timeout_count`, `admission_reject_count`, or guardrail counters.

Actions:

1. Identify dominant failure mode using `/metrics`.
2. Apply focused tuning:
- Timeouts: increase `HOLO_FUSION_DML_STATEMENT_TIMEOUT_MS` only if workload justifies longer statements.
- Admission: raise `HOLO_FUSION_DML_MAX_INFLIGHT_STATEMENTS` cautiously.
- Class budgets: tune `HOLO_FUSION_DML_MAX_INFLIGHT_READS`, `...WRITES`, `...TXNS`, `...BACKGROUND`.
- Queue policy: tune `HOLO_FUSION_DML_ADMISSION_QUEUE_LIMIT` and `...ADMISSION_WAIT_TIMEOUT_MS`.
- Scan pressure: tune `HOLO_FUSION_DML_MAX_SCAN_ROWS`.
- Transaction staging pressure: tune `HOLO_FUSION_DML_MAX_TXN_STAGED_ROWS`.
- Write RPC sizing: tune `HOLO_FUSION_DML_WRITE_MAX_BATCH_ENTRIES` and `HOLO_FUSION_DML_WRITE_MAX_BATCH_BYTES`.
- Retry governance: tune `HOLO_FUSION_DML_RETRY_MAX_ATTEMPTS`, `...RETRY_BASE_DELAY_MS`, `...RETRY_MAX_DELAY_MS`, `...RETRY_BUDGET_MS`.
- Circuit breaker: tune `HOLO_FUSION_DML_CIRCUIT_BREAKER_FAILURE_THRESHOLD` and `...CIRCUIT_BREAKER_OPEN_MS`.
- In-flight write budgets: tune `HOLO_FUSION_DML_WRITE_MAX_INFLIGHT_ROWS`, `...BYTES`, `...RPCS`.
- Bulk ingest controls: tune `HOLO_FUSION_BULK_CHUNK_ROWS_{INITIAL,MIN,MAX}` and latency thresholds.
3. Prefer load-shedding and workload shaping over unlimited knob increases.

### 4) Shard Imbalance / Replica Convergence Issues

Symptoms:

- One node records significantly outpace peers for same shard/range.
- Query latency or conflicts correlate with specific shard leaders.

Actions:

1. Inspect topology and per-range record counts:

```bash
cargo run -q -p holo_store --bin holoctl -- --target 127.0.0.1:15051 topology
```

2. If needed, use `holoctl` split/rebalance flows to redistribute load.
3. Re-check convergence after stabilization and verify no continuous drift.

## Capacity Planning and Tuning Knobs

Primary runtime knobs:

| Knob | Default | Use |
| --- | --- | --- |
| `HOLO_FUSION_DML_STATEMENT_TIMEOUT_MS` | `0` (disabled) | Upper bound for statement duration. |
| `HOLO_FUSION_DML_MAX_INFLIGHT_STATEMENTS` | `1024` | Admission-control concurrency limit. |
| `HOLO_FUSION_DML_MAX_INFLIGHT_READS` | `1024` | Read admission budget. |
| `HOLO_FUSION_DML_MAX_INFLIGHT_WRITES` | `1024` | Write admission budget. |
| `HOLO_FUSION_DML_MAX_INFLIGHT_TXNS` | `512` | Explicit transaction admission budget. |
| `HOLO_FUSION_DML_MAX_INFLIGHT_BACKGROUND` | `256` | Background/elastic admission budget. |
| `HOLO_FUSION_DML_ADMISSION_QUEUE_LIMIT` | `4096` | Max queue depth before `53300` shed. |
| `HOLO_FUSION_DML_ADMISSION_WAIT_TIMEOUT_MS` | `0` | Queue wait timeout (`53300` on expiry). |
| `HOLO_FUSION_DML_MAX_SCAN_ROWS` | `100000` | Caps per-mutation/snapshot scan fan-out. |
| `HOLO_FUSION_DML_MAX_TXN_STAGED_ROWS` | `100000` | Caps staged writes in explicit tx. |
| `HOLO_FUSION_DML_WRITE_MAX_BATCH_ENTRIES` | `1024` | Max rows per distributed write RPC chunk. |
| `HOLO_FUSION_DML_WRITE_MAX_BATCH_BYTES` | `1048576` | Approximate payload budget per write RPC chunk. |
| `HOLO_RANGE_WRITE_BATCH_TARGET` | `1024` | Embedded HoloStore range-write proposal target (decoupled from Redis `SET` batching). |
| `HOLO_RANGE_WRITE_BATCH_MAX_BYTES` | `1048576` | Embedded HoloStore range-write proposal byte cap (decoupled from Redis `SET` batching). |
| `HOLO_FUSION_DML_RETRY_MAX_ATTEMPTS` | `5` | Max attempts for retry-governed distributed writes. |
| `HOLO_FUSION_DML_RETRY_BASE_DELAY_MS` | `50` | Base backoff for jittered retry policy. |
| `HOLO_FUSION_DML_RETRY_MAX_DELAY_MS` | `1000` | Ceiling for retry backoff delay. |
| `HOLO_FUSION_DML_RETRY_BUDGET_MS` | `10000` | Total retry time budget per statement. |
| `HOLO_FUSION_DML_CIRCUIT_BREAKER_FAILURE_THRESHOLD` | `4` | Consecutive retryable failures before open. |
| `HOLO_FUSION_DML_CIRCUIT_BREAKER_OPEN_MS` | `2000` | Open-state cooldown before half-open probe. |
| `HOLO_FUSION_DML_WRITE_MAX_INFLIGHT_ROWS` | `32768` | In-flight row budget guardrail. |
| `HOLO_FUSION_DML_WRITE_MAX_INFLIGHT_BYTES` | `33554432` | In-flight byte budget guardrail. |
| `HOLO_FUSION_DML_WRITE_MAX_INFLIGHT_RPCS` | `32` | In-flight RPC budget guardrail. |
| `HOLO_FUSION_DML_WRITE_PIPELINE_DEPTH` | `4` | Max in-flight write batches per target before awaiting completions. |
| `HOLO_FUSION_BULK_CHUNK_ROWS_INITIAL` | `1024` | Initial adaptive chunk target for bulk ingest. |
| `HOLO_FUSION_BULK_CHUNK_ROWS_MIN` | `128` | Lower bound for adaptive bulk chunking. |
| `HOLO_FUSION_BULK_CHUNK_ROWS_MAX` | `8192` | Upper bound for adaptive bulk chunking. |
| `HOLO_FUSION_BULK_CHUNK_LOW_LATENCY_MS` | `40` | Grow chunk when latency stays below threshold. |
| `HOLO_FUSION_BULK_CHUNK_HIGH_LATENCY_MS` | `150` | Shrink chunk when latency exceeds threshold. |
| `HOLO_FUSION_HOLOSTORE_MAX_SHARDS` | `0` | Shard slot cap (`0` = auto mode with no explicit cap). |
| `HOLO_FUSION_HOLOSTORE_INITIAL_RANGES` | `1` | Starting split count before growth. |
| `HOLO_FUSION_HOLOSTORE_ROUTING_MODE` | `range` | Key routing mode (`range`/`hash`). |
| `HOLO_FUSION_PHASE10_HASH_PK_DDL_ENABLED` | `true` | Kill switch for new `USING HASH` DDL adoption. |
| `HOLO_FUSION_PHASE10_HASH_PK_MIGRATION_ENABLED` | `true` | Kill switch for hash-PK migration control path. |
| `HOLO_FUSION_PHASE10_RETRY_GOVERNANCE_ENABLED` | `true` | Kill switch for retry-governance policy. |
| `HOLO_FUSION_PHASE10_CIRCUIT_BREAKER_ENABLED` | `true` | Kill switch for per-target circuit breakers. |
| `HOLO_FUSION_PHASE10_BULK_INGEST_ENABLED` | `true` | Kill switch for streaming bulk ingest path. |

Tuning method:

1. Change one knob at a time.
2. Run representative benchmark workload.
3. Compare p50/p95/p99 and error counters.
4. Keep a rollback value for each changed knob.

For benchmark SLO targets and regression gates, use:

- `crates/holo_fusion/docs/PHASE7_BENCHMARK_SLO.md`
- `crates/holo_fusion/docs/PHASE9_SALES_FACTS_CANARY_SLO.md`

## Release Checklist (Safe Upgrade)

Pre-release gates:

1. `cargo test -p holo_fusion -- --nocapture`
2. Run benchmark gate:

```bash
HOLO_FUSION_ENFORCE_SLO=1 cargo test -p holo_fusion phase7_benchmark_and_slo_package -- --ignored --nocapture
```

3. Run Phase 9 ingest canary gate:

```bash
./crates/holo_fusion/scripts/benchmarks/run_phase9_sales_facts_canary.sh
```

4. Validate SQL client compatibility probes:
- `pg_postmaster_start_time()`
- `pg_is_in_recovery()`
- `txid_current()`
- `pg_catalog.pg_locks`
5. Validate Phase 10 stability gate:
- run `phase10_hash_primary_key_create_insert_and_scan`
- run `phase10_hash_primary_key_migration_backfills_online`
- run `phase10_metrics_expose_circuit_and_ingest_status`

Rolling upgrade guidance:

1. Upgrade one node at a time.
2. Keep quorum healthy throughout (never take down a majority).
3. After each node:
- Wait for `/ready` = `200`.
- Validate `holoctl topology` convergence.
- Run a write + read smoke check via `psql`.
- Confirm metadata migration state reached current schema version (`v2`) with no pending checkpoint.
4. Continue to next node only after stabilization.

For metadata migration state model and rollout details, see:
- `crates/holo_fusion/docs/METADATA_MIGRATION.md`

## Rollback Checklist

1. Stop rollout immediately on regression (availability, correctness, or SLO breach).
2. Disable Phase 10 controls first when impact is isolated:
- `HOLO_FUSION_PHASE10_CIRCUIT_BREAKER_ENABLED=false`
- `HOLO_FUSION_PHASE10_RETRY_GOVERNANCE_ENABLED=false`
- `HOLO_FUSION_PHASE10_BULK_INGEST_ENABLED=false`
- `HOLO_FUSION_PHASE10_HASH_PK_DDL_ENABLED=false`
- `HOLO_FUSION_PHASE10_HASH_PK_MIGRATION_ENABLED=false`
3. Re-deploy previous known-good binary and previous knob set on affected node.
4. Verify:
- `/ready` healthy
- topology convergence
- conflict/timeout/admission rates return to baseline
5. If schema or metadata migration was included, run the documented reverse migration before resuming traffic.

## Post-Incident Checklist

1. Save `/metrics` snapshots and relevant log slices.
2. Record exact SQLSTATE mix seen by clients.
3. Add a regression test or benchmark profile reproducing the issue.
4. Update this runbook when mitigation steps change.
