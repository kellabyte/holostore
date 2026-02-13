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
- [ ] Phase A: instrumentation and plan placement introspection.
- [ ] Phase B: leaseholder filter/projection pushdown via typed scan contract.
- [ ] Phase C: distributed partial aggregation/top-k.
- [ ] Phase D: distributed joins with bounded-memory exchanges.
- [ ] Phase E: failure semantics, resume, and topology-churn safety.
- [ ] Phase F: production SLO gates and regression automation.
