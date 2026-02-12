# HoloFusion Runbook and Rollout Guide

This document closes Phase 7 operational guidance for `holo_fusion`.

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
- Scan pressure: tune `HOLO_FUSION_DML_MAX_SCAN_ROWS`.
- Transaction staging pressure: tune `HOLO_FUSION_DML_MAX_TXN_STAGED_ROWS`.
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
| `HOLO_FUSION_DML_MAX_SCAN_ROWS` | `100000` | Caps per-mutation/snapshot scan fan-out. |
| `HOLO_FUSION_DML_MAX_TXN_STAGED_ROWS` | `100000` | Caps staged writes in explicit tx. |
| `HOLO_FUSION_HOLOSTORE_MAX_SHARDS` | `1` | Total shards available for spread. |
| `HOLO_FUSION_HOLOSTORE_INITIAL_RANGES` | `1` | Starting split count before growth. |
| `HOLO_FUSION_HOLOSTORE_ROUTING_MODE` | `range` | Key routing mode (`range`/`hash`). |

Tuning method:

1. Change one knob at a time.
2. Run representative benchmark workload.
3. Compare p50/p95/p99 and error counters.
4. Keep a rollback value for each changed knob.

For benchmark SLO targets and regression gates, use:

- `crates/holo_fusion/docs/PHASE7_BENCHMARK_SLO.md`

## Release Checklist (Safe Upgrade)

Pre-release gates:

1. `cargo test -p holo_fusion -- --nocapture`
2. Run benchmark gate:

```bash
HOLO_FUSION_ENFORCE_SLO=1 cargo test -p holo_fusion phase7_benchmark_and_slo_package -- --ignored --nocapture
```

3. Validate SQL client compatibility probes:
- `pg_postmaster_start_time()`
- `pg_is_in_recovery()`
- `txid_current()`
- `pg_catalog.pg_locks`

Rolling upgrade guidance:

1. Upgrade one node at a time.
2. Keep quorum healthy throughout (never take down a majority).
3. After each node:
- Wait for `/ready` = `200`.
- Validate `holoctl topology` convergence.
- Run a write + read smoke check via `psql`.
4. Continue to next node only after stabilization.

## Rollback Checklist

1. Stop rollout immediately on regression (availability, correctness, or SLO breach).
2. Re-deploy previous known-good binary and previous knob set on affected node.
3. Verify:
- `/ready` healthy
- topology convergence
- conflict/timeout/admission rates return to baseline
4. If schema or metadata migration was included, run the documented reverse migration before resuming traffic.

## Post-Incident Checklist

1. Save `/metrics` snapshots and relevant log slices.
2. Record exact SQLSTATE mix seen by clients.
3. Add a regression test or benchmark profile reproducing the issue.
4. Update this runbook when mitigation steps change.
