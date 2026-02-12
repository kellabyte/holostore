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
