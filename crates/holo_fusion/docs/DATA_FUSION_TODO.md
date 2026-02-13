# Data Fusion TODO

1. [x] Lock architecture details in `DATA_FUSION.md`.
- Finalize embedded HoloStore + Ballista lifecycle.
- Finalize scheduler topology decision and failure handling.
- Finalize no-Redis SQL path constraints.

2. [x] Create storage-model specification in `HOLO_FUSION_STORAGE_MODEL.md`.
- Define database/schema/table/row/column/index key encoding.
- Include at least one worked DDL-to-keys/value example.

3. [x] Run dependency compatibility spike by scaffolding `crates/holo_fusion`.
- Pin `datafusion`, `ballista`, and `datafusion-postgres`.
- Get `cargo check` green with the pinned set.

4. [x] Define MVP SQL support matrix in `HOLO_FUSION_SQL_SCOPE.md`.
- List supported statements for MVP.
- List explicitly rejected/deferred statements and expected error behavior.

5. [x] Complete Phase 5 conflict-safe DML implementation.
- Add storage-level conditional apply RPC for expected-version checks.
- Use optimistic concurrency control for `UPDATE` / `DELETE`.
- Return SQLSTATE `40001` on write-write conflict.
- Add shard rollback behavior so failed multi-shard DML does not leave partial visible state.

6. [x] Add Phase 5 correctness tests.
- Concurrent `UPDATE` conflict test (one winner, one `40001` loser).
- Distributed cross-shard conflict test with rollback verification.

7. [x] Complete Phase 6 SQL transaction semantics.
- [x] Return deterministic SQLSTATE `0A000` for explicit `BEGIN`/`COMMIT`/`ROLLBACK` until session tx manager lands.
- [x] Define session state model for `BEGIN` / `COMMIT` / `ROLLBACK`.
- [x] Define isolation target and statement-to-transaction mapping.
- [x] Add multi-statement transaction integration tests.

8. [x] Complete Phase 7 hardening and operability.
- [x] Close transaction-control protocol gap:
  - Ensure explicit transaction semantics are consistent for simple-query and extended-query paths.
  - Add integration tests covering both protocol paths for `BEGIN` / `COMMIT` / `ROLLBACK`.
- [x] Add production observability:
  - Emit transaction metrics (begin/commit/rollback counts, conflict counts, commit latency).
  - Emit distributed write metrics (per-shard apply latency, rollback count, conflict hotspots).
  - Add tracing spans across SQL statement routing, transaction staging, commit, and rollback.
- [x] Add guardrails and backpressure:
  - Statement timeout enforcement and SQLSTATE mapping for timeout/cancel.
  - Memory/row limits for scans and transactional staging to prevent unbounded growth.
  - Admission control behavior under overload with deterministic error semantics.
- [x] Build benchmark and SLO package:
  - Read-heavy, mixed, and write-heavy benchmark scenarios.
  - Baseline and target SLOs for p50/p95/p99 latency and throughput.
  - Regression gate criteria for CI/perf test runs.
- [x] Add fault-injection and resiliency tests:
  - Node loss during transactional commit.
  - Partial shard availability and retry behavior.
  - Restart/rejoin semantics with correctness checks for visibility and rollback.
- [x] Publish runbook and rollout guidance:
  - Operational runbook for incident triage (conflict spikes, degraded mode, shard imbalance).
  - Capacity planning notes and tuning knobs.
  - Release checklist for safe upgrades and rollback steps.

9. [x] Complete Phase 8 generic storage model expansion.
- [x] #1 Generic CREATE TABLE + schema-driven provider:
  - Persist table columns and primary-key metadata in HoloStore (`row_v1`).
  - Remove CREATE TABLE hard requirement on the fixed `orders_v1` column set.
  - Add schema-driven scan/insert codec path for non-`orders` tables.
  - Keep `orders_v1` codec path for backward compatibility and existing transactional DML semantics.
- [x] #2 Generic UPDATE/DELETE across row_v1 tables.
  - Add schema-driven `row_v1` mutation parsing and typed assignment coercion for `UPDATE`.
  - Add schema-driven `row_v1` delete execution with PK-bounded scans and conditional writes.
  - Preserve distributed optimistic-concurrency semantics (`40001` on write-write conflict) with rollback payloads.
  - Return deterministic `0A000` for explicit-transaction `row_v1` UPDATE/DELETE until generic transactional DML staging lands.
- [x] #3 Explicit metadata migration/backfill plan for existing clusters.
  - Add persisted metadata schema-state record with resumable backfill checkpointing.
  - Add startup migration runner that upgrades legacy `orders_v1` metadata rows in-place.
  - Add idempotent conflict-safe conditional writes for concurrent migrators across nodes.
  - Add migration tests covering legacy decode, canonical backfill, and idempotent reruns.
- [x] #4 Extended type coverage and SQL defaults/check constraints roadmap.
  - Add schema-persisted column defaults and CHECK constraints with metadata validation at DDL time.
  - Enforce defaults/checks/not-null/type coercion on row_v1 INSERT/UPDATE execution paths.
  - Strengthen numeric CHECK correctness for large integer domains (including uint64 precision-sensitive comparisons).
  - Expand assignment/type coercion coverage for unsigned integer columns (`UInt8/16/32/64`), including proper range enforcement.
  - Document supported DEFAULT/CHECK SQL scope and deterministic deferred forms in `HOLO_FUSION_SQL_SCOPE.md`.

10. [ ] Complete Phase 9 distributed SQL execution evolution.
- [x] Publish design baseline in `DISTRIBUTED_SQL_EXECUTION_DESIGN.md`.
- [x] Phase A: instrumentation and plan placement introspection.
  - query execution IDs + stage IDs emitted across SQL hook and storage scan stages.
  - `/metrics` includes query/stage lifecycle counters and active-query snapshots.
  - `EXPLAIN` now returns distributed stage placement classification (`scan`, `aggregate_partial`, `aggregate_final`, `join`, `exchange`, `topk`).
- [x] Phase B: leaseholder filter/projection pushdown via typed scan contract.
  - typed `ScanSpec` / `ScanChunk` / `ScanStats` contract added for storage-facing scan execution.
  - explicit pushdown fallback stage events emitted when predicates are not storage-pushdown-safe.
  - projection is applied at provider scan output to reduce downstream batch width.
- [x] Phase C: distributed partial aggregation/top-k.
  - distributed optimizer defaults force repartitioned aggregates/sorts/windows with multi-partition execution.
  - explain placement surfaces partial/final aggregate and top-k stage placement.
- [x] Phase D: distributed joins with bounded-memory exchanges.
  - repartitioned distributed join planning enabled by default.
  - exchange/join placement surfaced in explain output for operability.
- [x] Phase E: failure semantics, resume, and topology-churn safety.
  - scan retry with bounded backoff + topology reroute on transient/leaseholder movement errors.
  - idempotent scan merge skips duplicate keys after retry/reroute chunk replays.
- [x] Phase F: production SLO gates and regression automation.
  - runtime spill controls and sort spill reservation wired through environment controls.
  - smoke coverage extended for phase-9 metrics and explain-placement behavior.
  - reproducible `sales_facts` ingest canary (`20k` / `50k` batches + split-churn) with hard pass/fail gates.
  - canary snapshots capture `/metrics` and `holoctl topology` before/after each batch for regression triage.

11. [ ] Complete Phase 10 workload management and transaction throughput control.
- [ ] Phase A: adaptive admission control and overload semantics.
  - shard-aware admission budgets for read/write/transaction classes with deterministic fairness.
  - explicit queue-time limits and deterministic SQLSTATE mapping for overload rejection paths.
  - add shard/replica token-based pacing and deterministic overload semantics (`53300`).
  - separate regular traffic vs elastic/background traffic with independent budgets.
  - separate guardrails for foreground SQL workload vs background maintenance tasks.
- [ ] Phase B: distributed flow control, in-flight replication budgets, and hotspot distribution controls.
  - hotspot fix first (highest ROI): add hash distribution for sequential keys (table-level hash-sharded PK/routing).
  - add `PRIMARY KEY (...) USING HASH` DDL support (with optional shard/bucket count).
  - persist metadata for hash-sharded key layout and placement configuration.
  - add hash-based write routing and scan planning behavior.
  - pre-split and rebalance ranges before large ingest jobs; treat this as required for sustained linear scale.
  - per-shard and per-target in-flight limits (rows, bytes, and RPC count) on write and rollback paths.
  - leaseholder/replica backpressure signaling surfaced to SQL execution before RPC timeout boundaries.
  - dynamic write-batch sizing policy driven by observed apply latency and timeout/error feedback.
- [ ] Phase C: retry governance and circuit breakers.
  - bounded retry budgets per statement and per transaction with jittered exponential backoff.
  - explicit retryable/non-retryable error classification across HoloStore RPC and SQL hook layers.
  - per-target circuit breakers with half-open probing to prevent thundering-herd retries.
- [ ] Phase D: transaction pipelining, commit-path optimization, and bulk ingest execution.
  - add dedicated bulk path for `INSERT ... SELECT` / `COPY` (separate from normal OLTP path).
  - stream batches directly through the sink path (no end-to-end buffering before writes start).
  - add per-shard writer workers for bulk ingest.
  - enforce bounded in-flight windows for bulk ingest (rows/bytes/RPC).
  - use adaptive chunk sizing from observed latency/error feedback.
  - add idempotent chunk IDs for safe retry/resume semantics.
  - replace global `sql_write_lock` with per-shard concurrency control.
  - avoid per-chunk polling loops; return commit/apply acknowledgments from storage and barrier once per statement phase.
  - pipeline conditional writes safely within one transaction while preserving conflict semantics.
  - reduce commit critical path via parallel shard commit where correctness constraints permit.
  - add online migration/backfill path from non-hash PK to hash PK.
  - durable coordinator recovery semantics for partially-pipelined transactions and rollback intents.
- [ ] Phase E: observability and SLO-driven control loops.
  - emit queue-depth, admission wait-time, drop/reject, and circuit-state metrics per shard/target.
  - add ingest progress metrics (`rows_ingested`, `rows_per_second`, `queue_depth`, `inflight_bytes`, per-shard lag) and job-level status.
  - publish saturation diagnostics and recommended tuning bands in runbook and metrics output.
  - add hotspot metrics and correctness coverage for `USING HASH` routing and rebalance behavior.
  - add CI/perf regression gates for sustained p95/p99 overload behavior regressions.
- [ ] Phase F: failure-injection validation and rollout safety.
  - overload chaos suite for burst writes, hot shards, partial partitions, and slow follower scenarios.
  - canary rollout plan with abort thresholds tied to latency/error/admission metrics.
  - add rollout/canary gates for hash-distributed PK adoption and bulk ingest controls.
  - feature-flag kill switches and rollback playbooks for each Phase 10 control-plane capability.
