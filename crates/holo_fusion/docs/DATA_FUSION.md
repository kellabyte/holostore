# HoloFusion Project Plan (HoloStore + DataFusion + Ballista + Postgres Wire)

## Objective

Build a new crate, `crates/holo_fusion`, that exposes a SQL server over the PostgreSQL wire protocol, backed by embedded HoloStore key/value data and embedded Ballista/DataFusion execution in every process.

Primary target:
- SQL clients (psql, BI tools, JDBC/ODBC via PG protocol) can connect and run queries.
- HoloStore remains the source of truth for data and transaction behavior.
- Every HoloFusion node embeds both a HoloStore node and a Ballista node.
- SQL reads and writes use in-process HoloStore integration for local data (no Redis hop on SQL path).
- DataFusion/Ballista provide SQL planning and vectorized/distributed query execution.

## Scope

In scope:
- New `holo_fusion` crate in this workspace.
- Postgres protocol server built with [`datafusion-postgres`](https://github.com/datafusion-contrib/datafusion-postgres).
- Embedded-node runtime: each `holo_fusion` process hosts SQL server + HoloStore node + Ballista node.
- TableProvider-based integration from SQL tables to HoloStore shards/ranges.
- Incremental DML support (`INSERT` first, then `UPDATE`/`DELETE`).
- Ballista integration for distributed query execution across embedded nodes.
- No Redis protocol dependency on internal SQL execution paths.

Out of scope for initial milestones:
- Full PostgreSQL feature parity.
- Full ANSI SQL coverage.
- Automatic cost-based cross-system optimizer tuned for all workloads.

## Design Principles

- Keep OLTP correctness in HoloStore; do not assume DataFusion/Ballista provide ACID.
- Start with narrow SQL surface area and expand with tests.
- Make write paths idempotent and retry-safe (Ballista task/stage retries).
- Do not use Redis protocol between HoloFusion nodes for SQL execution.
- Prefer local-shard in-memory reads through embedded HoloStore before remote execution.
- Build clear fallback paths:
  - Local DataFusion execution for simple/small queries.
  - Ballista for distributed scans/joins/aggregations.

## Target Architecture

`holo_fusion` will contain the following modules:

1. `server`
- Bootstraps PG wire listener.
- Auth, session lifecycle, query submission, result streaming.

2. `session`
- SQL session state.
- Transaction state (`autocommit`, `BEGIN/COMMIT/ROLLBACK` handling).
- Maps SQL statements to execution contexts.

3. `catalog`
- Logical DB/schema/table metadata.
- Resolves table names to `TableProvider` instances.
- Tracks table-to-range/shard mapping metadata.

4. `provider`
- HoloStore-backed `TableProvider` and related scan/write logic.
- Implements `scan`, `insert_into`, and later `update`/`delete_from`.

5. `planner`
- DataFusion `SessionContext` configuration.
- Rule toggles, pushdown settings, extension points.

6. `ballista`
- Embedded distributed planner/executor path.
- Query routing policy (local vs distributed).

7. `txn`
- Integration boundary to HoloStore transaction semantics.
- Snapshot selection, write intents, conflict checks, commit/rollback behavior.

8. `metrics`
- Query latency, scanned rows/bytes, pushdown ratio, retries, commit outcomes.

9. `embedded_node`
- Boots HoloStore node and Ballista node inside each HoloFusion process.
- Owns component lifecycle (startup ordering, readiness, shutdown).
- Exposes direct in-process adapters so SQL paths do not go through Redis.

## Embedded Runtime Contract

This section is the concrete lifecycle contract for every HoloFusion process.

### Per-process role model

| Component | Required on every node | Notes |
|---|---|---|
| PostgreSQL wire server (`datafusion-postgres`) | Yes | Client entrypoint for SQL traffic. |
| HoloStore node | Yes | Source of truth for storage + transactional correctness. |
| Ballista executor | Yes | Executes distributed tasks close to local shard ownership. |
| Ballista scheduler | No (single active for MVP) | One active scheduler in MVP, others reconnect as clients/executors. |

MVP topology decision:
- Single active Ballista scheduler.
- Every HoloFusion node runs: PG server + HoloStore + Ballista executor.

### Startup sequence (strict order)

1. `BOOTSTRAP_CONFIG`
- Load node config, cluster addresses, scheduler target, and feature flags.
- Initialize structured logging and metrics registry.

2. `START_HOLOSTORE`
- Start embedded HoloStore services (storage engine, consensus, RPC transport).
- Join or bootstrap HoloStore cluster membership.

3. `WAIT_HOLOSTORE_READY`
- Gate until:
  - local keyspace opened,
  - metadata/range routing state loaded,
  - cluster RPC health is acceptable.

4. `START_BALLISTA`
- Start Ballista executor locally.
- Start Ballista scheduler only if this node is designated active scheduler.
- If not scheduler host, connect to configured scheduler endpoint.

5. `WAIT_BALLISTA_READY`
- Gate until:
  - scheduler endpoint is reachable,
  - local executor registration is visible to scheduler,
  - query planner can build distributed plan context.

6. `INIT_SQL_LAYER`
- Create DataFusion session state and hooks.
- Register catalogs, pg_catalog support, and HoloStore-backed providers.
- Enforce no-Redis SQL path adapters.

7. `START_PG_SERVER`
- Bind Postgres wire listener only after steps 1-6 pass.
- Move node state to `READY`.

### Readiness and liveness contract

Expose three health states:

| State | Meaning | SQL behavior |
|---|---|---|
| `LIVE` | Process is running main event loop. | Not sufficient for traffic. |
| `READY` | HoloStore + Ballista + SQL layer gates all passed. | Accept read/write SQL normally. |
| `DEGRADED` | Process is alive but one non-fatal dependency is impaired. | Accept only operations allowed by degradation policy. |

Readiness gates:
- HoloStore local readiness is mandatory.
- Ballista scheduler/executor readiness is mandatory for distributed mode.
- SQL layer is ready only when catalog/provider init succeeds.

MVP degradation policy:
- If HoloStore local readiness fails: mark `NOT READY`, reject all SQL.
- If Ballista scheduler unavailable but HoloStore healthy:
  - remain `DEGRADED`,
  - permit local-only SQL plans,
  - reject distributed-required plans with deterministic error.

### Failure handling policy

| Failure event | Detection | Action | Recovery |
|---|---|---|---|
| HoloStore local engine unavailable | Health gate or RPC/storage error threshold | Mark `NOT READY`, reject new SQL, cancel write execution path. | Restart HoloStore subsystem; re-enter startup gates before ready. |
| Scheduler connection lost | Ballista client heartbeat/connect failures | Switch to `DEGRADED`; disable distributed plan dispatch. | Exponential reconnect with jitter; restore distributed mode when healthy. |
| Local executor registration lost | Scheduler registration/heartbeat mismatch | Keep `DEGRADED`; continue local SQL where valid. | Re-register executor; return to `READY` after success. |
| PG listener bind failure | Startup bind error | Treat as fatal startup failure. | Exit process for supervisor restart. |
| In-process adapter invariant violated (Redis hop on SQL path) | Runtime guard/metric assertion | Fail request and emit high-priority error metric/log. | Block release until invariant fixed; tests must catch regression. |

### Reconnect, retry, and fencing rules

- Scheduler reconnect backoff:
  - initial `250ms`, exponential up to `5s`, with jitter.
- Distributed write tasks must include idempotency keys:
  - `(tx_id, shard_id, partition_id, attempt_id)`.
- On scheduler failover, new scheduler epoch must fence stale task ownership
  before accepting task-status updates from previous epoch workers.

### No-Redis SQL path enforcement

Enforcement requirements:

1. Architecture:
- SQL `TableProvider` read/write paths call embedded HoloStore interfaces
  directly (or native cluster RPC), not Redis protocol handlers.

2. Validation:
- Integration test suite must assert no Redis protocol dependency on SQL path.
- Metrics must expose and assert `sql_redis_hop_count == 0` in MVP runs.

3. Operational guard:
- Any non-zero Redis-hop metric on SQL traffic is a release blocker.

### Shutdown sequence

1. Mark node `NOT READY` and stop accepting new SQL sessions.
2. Drain in-flight SQL queries with bounded timeout.
3. Flush/commit or abort active SQL transactions per policy.
4. Deregister Ballista executor from scheduler.
5. Stop Ballista services.
6. Stop HoloStore services cleanly (WAL/storage flush according to durability mode).
7. Exit process.

## Implementation Phases

Current status snapshot (updated 2026-02-12):
- Phase 0: Complete.
- Phase 1: Complete.
- Phase 2: Complete.
- Phase 3: Complete.
- Phase 4: Complete.
- Phase 5: Complete (conflict-safe `UPDATE`/`DELETE`, SQLSTATE `40001`, rollback-on-conflict tests).
- Phase 6: Complete (session-managed `BEGIN` / `COMMIT` / `ROLLBACK` semantics landed across protocol paths).
- Phase 7: Complete (operability hardening, benchmark/SLO package, resiliency tests, runbook/rollout guidance).

## Phase 0: Foundation and Compatibility

Tasks:
- Pick and pin compatible versions of `datafusion`, `ballista`, `datafusion-postgres`.
- Lock the deployment model: every `holo_fusion` node embeds both HoloStore and Ballista.
- Define embedded Ballista topology:
  - executor role on every node,
  - scheduler strategy and discovery/registration model.
- Define in-process integration boundaries between SQL layer and embedded HoloStore APIs (no Redis hop).
- Finalize embedded runtime contract:
  - startup ordering,
  - readiness/degraded/liveness states,
  - failure handling and shutdown behavior.
- Define the storage model for SQL primitives on HoloStore:
  - Database/schema/table identifiers and metadata keys.
  - Row key encoding (primary key layout, shard/range routing fields).
  - Column value encoding (types, nullability, version/tombstone representation).
  - Secondary index key layout and lookup strategy.
  - Mapping rules from SQL table definitions to HoloStore keyspace.
- Define first SQL subset and error contract.

Deliverables:
- Dependency matrix in crate README.
- RFC-style architecture note for embedded process model and transaction boundary.
- Storage-model spec document that defines key scheme and encoding for
  database/schema/table/row/column/index data.
- Embedded runtime lifecycle spec (startup/readiness/degraded/shutdown) with
  failure-policy matrix.

Acceptance criteria:
- `cargo check` passes with pinned dependency set.
- Finalized MVP statement list (supported + explicitly unsupported).
- Storage-model spec is reviewed and includes worked examples for at least one
  table (DDL, encoded keys, and encoded row/column values).
- Embedded-node architecture doc defines startup/readiness/shutdown ordering for
  SQL server, HoloStore, and Ballista in one process.
- Tabletop failure exercises are documented for:
  - HoloStore local outage,
  - scheduler loss,
  - executor deregistration,
  - process restart and rejoin.

Phase 0 Ballista scheduler decision table:

| Option | Description | Pros | Cons | Best fit | Required Phase 0 outputs |
|---|---|---|---|---|---|
| Single active scheduler | Run one Ballista scheduler service for the cluster; every HoloFusion node runs an executor + HoloStore node. | Simplest operations model, easiest debugging, fastest path to first distributed queries. | Scheduler is a control-plane SPOF unless fronted by failover automation. | MVP and early production with modest cluster size. | Scheduler placement and failover runbook, reconnect behavior spec, health-check and fencing rules. |
| Co-located HA scheduler set | Run scheduler replicas (typically on dedicated subset of HoloFusion nodes) with external consensus/state backend for failover. | Better control-plane availability and maintenance flexibility. | Higher complexity: leader election/state coordination, stricter versioning/rollout discipline. | Larger clusters or stricter availability targets. | HA scheduler architecture spec, consensus/state backend choice, failover SLOs, rolling upgrade procedure. |

Phase 0 default decision:
- Start with `Single active scheduler` for MVP.
- Gate move to `Co-located HA scheduler set` on defined thresholds:
  - sustained scheduler CPU/queue pressure,
  - failover SLO requirements,
  - cluster/node count growth.

## Phase 1: Crate Skeleton and Server Bootstrap

Tasks:
- Add `crates/holo_fusion` to workspace members.
- Build minimal server that listens on PG wire and accepts connections.
- Bootstrap embedded HoloStore and Ballista components in the same process.
- Wire simple SQL execution path (`SELECT 1` smoke test).

Deliverables:
- `holo_fusion` crate compiles and starts with embedded HoloStore + Ballista.
- Basic integration test: connect via PG client and run health query.

Acceptance criteria:
- `psql` can connect and run `SELECT 1`.
- Structured logs show session open/close and statement execution timing.
- Startup logs/health checks show all three subsystems healthy:
  PG server, HoloStore node, Ballista node.

## Phase 2: Read-Only MVP (DataFusion + HoloStore TableProvider)

Tasks:
- Implement HoloStore `TableProvider` for table scans.
- Implement direct in-process read path from `TableProvider` to embedded HoloStore
  engine for local shard/range data.
- Implement filter/projection pushdown where possible.
- Add catalog registration for at least one SQL table mapped to HoloStore keyspace.

Deliverables:
- `SELECT ... FROM table WHERE ...` operational for single-table scans.
- Unit tests for provider scan and pushdown behavior.

Acceptance criteria:
- Correct row results on deterministic fixture data.
- Pushdown metrics emitted and validated in tests.
- Local-shard SQL reads execute without Redis protocol calls.

## Phase 3: Distributed Read Path (Ballista)

Tasks:
- Enable Ballista-backed distributed execution across embedded-node cluster.
- Implement planner policy for local vs distributed execution.
- Ensure each HoloFusion node participates as a Ballista node and can execute
  tasks near local shard ownership.
- Validate distributed scans/joins on multi-shard data.

Deliverables:
- Cluster bootstrap/config for embedded Ballista + HoloStore nodes.
- Integration tests covering local and distributed Ballista paths.

Acceptance criteria:
- Same query results between local and distributed modes.
- Ballista mode shows improved throughput for large scans/aggregations.
- Multi-node tests verify each HoloFusion process runs as both Ballista node and
  HoloStore node.

## Phase 4: Write MVP (`INSERT`)

Tasks:
- Implement `insert_into` in HoloStore `TableProvider`.
- Route writes by shard/range and execute as batch writes through embedded
  HoloStore integration (no Redis protocol on SQL write path).
- Ensure idempotency keys for retry safety.

Deliverables:
- `INSERT INTO ... VALUES` and `INSERT INTO ... SELECT`.
- Multi-shard insert integration tests.

Acceptance criteria:
- Correct inserted row counts and values.
- Retry/failure tests do not create duplicate committed rows.
- Local-shard SQL inserts execute via in-process HoloStore APIs.

## Phase 5: Transactional DML (`UPDATE` / `DELETE`)

Status:
- Complete.

Tasks:
- Implement `update` and `delete_from` provider hooks.
- Add predicate-to-key/range planning to avoid full scans when possible.
- Integrate with HoloStore conflict detection and commit protocol.
- Keep DML execution on embedded HoloStore APIs and native cluster RPCs, not Redis.

Deliverables:
- Controlled support for `UPDATE` and `DELETE` with documented limits.
- Correctness tests for write-write conflicts and rollback behavior.

Acceptance criteria:
- No lost updates under concurrent write tests.
- Rollback leaves no visible partial state.

Implemented checkpoints:
- `UPDATE`/`DELETE` now execute with optimistic expected-version checks at the storage boundary.
- Conflicts are returned as SQLSTATE `40001` (serialization failure style retry signal).
- Conditional multi-shard apply performs compensating rollback on prior shards when a later shard conflicts.
- Integration coverage includes:
  - concurrent write-write conflict test (`40001`),
  - distributed multi-shard conflict test validating rollback of earlier-shard changes.

## Phase 6: SQL Transaction Semantics

Status:
- Complete.

Implemented checkpoints:
- Session-level transaction manager for explicit transactions:
  - `BEGIN`, `COMMIT`, `ROLLBACK`
  - session-scoped active/aborted transaction states
- Snapshot reads with read-your-writes semantics for explicit transaction scope.
- Transactional `INSERT` / `UPDATE` / `DELETE` are staged in-memory and committed through
  storage-level conditional writes.
- SQLSTATE mapping:
  - `BEGIN` while already active returns a no-op `BEGIN` completion,
  - `COMMIT`/`ROLLBACK` without active transaction return no-op command completion,
  - `25P02` for statements while transaction is aborted (until `ROLLBACK`),
  - `40001` for commit-time write conflicts.
- Cross-shard commit conflict handling reuses compensating rollback behavior so failed commits
  do not leave partial visible state.

Tasks:
- Session-level transaction manager behavior:
  - `BEGIN`, `COMMIT`, `ROLLBACK`
  - autocommit mode
- Snapshot read behavior for transaction isolation target.
- Error mapping to SQLSTATE where practical.

Deliverables:
- Transaction behavior spec.
- Integration tests for multi-statement transactions.

Acceptance criteria:
- Multi-shard transaction tests satisfy declared isolation guarantees.
- Clear, stable error behavior for conflict/timeout/retry conditions.

Protocol note:
- Explicit transaction semantics are validated on both simple-query and
  extended-query paths, including no-op behavior parity for transaction-control
  statements outside active transaction scope.

## Phase 7: Hardening and Operability

Status:
- Complete.

Tasks:
- Performance profiling and tuning.
- Observability dashboards and tracing spans.
- Backpressure, limits, and guardrails (max rows, memory, timeout).

Deliverables:
- Benchmark suite (read-heavy, mixed, write-heavy):
  - `crates/holo_fusion/docs/PHASE7_BENCHMARK_SLO.md`
- Runbook and rollout guidance:
  - `crates/holo_fusion/docs/HOLO_FUSION_RUNBOOK.md`

Acceptance criteria:
- SLO targets met for defined benchmark workloads.
- Failure injection tests pass (node loss, retries, partial shard failures).

## Testing Strategy

- Unit tests:
  - Catalog resolution
  - TableProvider scan/write behavior
  - Predicate/range planning
- Integration tests:
  - PG wire protocol end-to-end
  - Ballista distributed queries
  - Embedded-node startup (PG + Ballista + HoloStore) and readiness checks
  - Local read path validation with no Redis protocol dependency
  - Multi-shard insert/update/delete correctness
  - Transaction commit/rollback and conflict behavior
- Fault tests:
  - Task retries
  - Executor failures
  - Partial shard availability

## Key Risks and Mitigations

1. Distributed write retries cause duplicate effects.
- Mitigation: idempotency keys and deduplicating commit records in HoloStore.

2. SQL feature expectations exceed MVP surface.
- Mitigation: explicit compatibility matrix and precise error messages.

3. Cross-shard transaction latency is too high.
- Mitigation: optimize for single-shard fast path; reserve distributed commit for necessary cases.

4. Planner/execution behavior changes across upstream versions.
- Mitigation: pinned versions, compatibility tests, staged upgrades.

## Immediate Next Steps (Execution Order)

1. Create `crates/holo_fusion` skeleton and add to workspace.
2. Pin dependency set (`datafusion`, `ballista`, `datafusion-postgres`) and get `cargo check` green.
3. Implement embedded process bootstrap for PG server + HoloStore + Ballista in one node.
4. Implement PG wire bootstrap + `SELECT 1` connectivity test.
5. Add first HoloStore-backed read-only table provider with direct in-process local reads and end-to-end `SELECT` test.
