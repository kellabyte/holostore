# HoloFusion Distributed SQL Execution Design

## Purpose

Define the production-grade path from the current coordinator-centric SQL execution model to a distributed SQL execution model that is closer to CockroachDB-style DistSQL behavior:

- push compute to data whenever possible,
- keep correctness anchored in HoloStore/Accord semantics,
- and enforce hard quality/performance gates at every phase.

This document is intentionally strict about quality gates and failure behavior. No phase is "done" unless it meets correctness, resiliency, and performance criteria.

## Architectural Decision: Ballista + Custom HoloStore Integration

Decision:

- We **will use Ballista** for distributed SQL compute.
- We **will not use Ballista as the correctness authority** for storage semantics.

This is a hybrid model, not an either/or choice.

Ballista responsibilities:

- distributed physical execution,
- exchanges/repartitioning,
- distributed joins,
- partial/final aggregation,
- parallel scan pipelines.

HoloFusion/HoloStore responsibilities:

- shard/range routing and leaseholder targeting,
- snapshot/read-barrier semantics aligned with Accord behavior,
- write correctness and conflict behavior,
- split/merge/rebalance safety semantics.

Why this split is required:

- Ballista does not natively understand HoloStore shard topology, leaseholder movement, or Accord visibility semantics.
- HoloStore/Accord semantics are correctness-critical and must stay in the storage/control plane.
- Rebuilding distributed SQL compute ourselves would duplicate Ballista and increase risk/time substantially.

Operational query policy:

- OLTP-style point reads/writes: prefer direct, low-latency HoloFusion/HoloStore execution paths.
- Scan/aggregation/join-heavy queries: prefer Ballista distributed execution with HoloStore-aware pushdown and routing.

## Current State (What We Have Today)

Current read path (simplified):

```text
SQL client
   |
   v
Gateway node (DataFusion)
   |
   +--> fetch topology
   +--> route range segments to shard leaseholders
   +--> range_snapshot_latest RPC per target
   +--> materialize rows into local MemTable
   +--> execute remaining SQL operators locally
```

Key properties of the current path:

- Routing is shard-aware and leaseholder-oriented.
- Only narrow PK-bounds predicates are pushed down today.
- Most filtering/projection/aggregation/join work happens on one node.
- For broad scans, significant row volume can be shipped to a single coordinator.

Current implementation split (today):

- Ballista/DataFusion:
  - SQL parsing/planning and local execution pipeline.
  - Optional Ballista mode exists but is not yet the primary end-to-end distributed execution path for storage-aware scans.
- HoloFusion/HoloStore:
  - shard leaseholder routing,
  - `range_snapshot_latest` data fetch,
  - storage correctness semantics and write-path correctness.
- Interaction today:
  - coordinator fetches shard data via HoloStore-aware RPCs, then executes most remaining operators locally.

## Problem Statement

The current model is correct enough for functional use, but naive for large/parallel workloads:

- High network fan-in to one gateway.
- Under-utilization of cluster CPU/memory.
- Poor scaling for large scans, joins, and aggregations.
- Tail-latency amplification at the coordinator.

To be production-grade at scale, execution must become distributed, data-local, and backpressure-aware.

## Design Goals

1. Preserve correctness:
- SQL visibility and write semantics must remain consistent with HoloStore/Accord ordering.
- Split/merge/rebalance/replica movement must not produce stale or duplicate-visible rows.

2. Scale query performance:
- Reduce bytes shipped to gateways.
- Increase parallelism across shard leaseholders/executors.
- Improve p95/p99 latency for large scans and analytical queries.

3. Operational safety:
- Deterministic retry/cancel behavior.
- Clear progress/health observability.
- Strong failure recovery guarantees.

## Non-Goals (For This Design)

- Full PostgreSQL optimizer parity.
- Full ANSI SQL engine redesign.
- Replacing DataFusion/Ballista internals; we integrate with and extend them.

## Target Architecture

### High-Level Pipeline

```text
                    +------------------------------------------+
Client SQL -------> | Gateway Planner/Coordinator (Ballista)   |
                    +-------------------+----------------------+
                                        |
                     plan fragments + pushdown traits
                                        |
             +---------------------+---------------------+
             |                     |                     |
             v                     v                     v
 +-----------------------+ +-----------------------+ +-----------------------+
 | Executor Node         | | Executor Node         | | Executor Node         |
 | Ballista Worker       | | Ballista Worker       | | Ballista Worker       |
 | + HoloStore Leaseholder| | + HoloStore Leaseholder| | + HoloStore Leaseholder|
 +-----------+-----------+ +-----------+-----------+ +-----------+-----------+
             |                     |                     |
             v                     v                     v
      local shard scan      local shard scan      local shard scan
      (HoloStore)           (HoloStore)           (HoloStore)
      + local filter/proj   + local filter/proj   + local filter/proj
      + partial agg         + partial agg         + partial agg
      (Ballista stage)      (Ballista stage)      (Ballista stage)
             \                    |                    /
              \                   |                   /
               +-------- exchange streams -----------+
                         (Ballista)
                                   |
                                   v
                         final merge/materialize
                              (Ballista)
```

Component ownership legend:

- Ballista/DataFusion: distributed SQL planning and execution operators.
- HoloFusion/HoloStore: shard routing, leaseholder reads, snapshot/barrier semantics, write correctness.

## Ballista/HoloStore Interaction Contract

This is the required interaction model for every distributed SQL statement.

1. Gateway planning:
- Ballista/DataFusion builds a physical plan and identifies pushdown-eligible operators.

2. Routing and bounds:
- HoloFusion/HoloStore topology routing resolves shard/range targets and leaseholder endpoints.

3. Pushdown handoff:
- Ballista stage sends a typed `ScanSpec` to HoloStore-facing scan RPCs (not raw SQL text).

4. Local storage execution:
- HoloStore executes bounded shard scans under statement snapshot semantics and returns `ScanChunk`s.

5. Distributed compute:
- Ballista workers consume chunks and run distributed operators (filter/projection/aggregate/join/exchange).

6. Finalization:
- Ballista merges final results and streams to client.

7. Writes and transaction correctness:
- HoloStore/Accord remains authoritative for write visibility, conflict handling, and commit semantics.
- Ballista is not the source of truth for write correctness.

### Execution Principles

1. Data-local first:
- Scan/filter/projection should run on nodes that host leaseholder data.

2. Partial-before-global:
- Partial aggregate/sort/top-k should happen near data before global merge.

3. Typed pushdown contract:
- Pushdown operations use a stable typed IR, not ad-hoc SQL strings.

4. Deterministic semantics:
- Every distributed stage has explicit snapshot/version context and retry rules.

## Query Classes and Desired Placement

| Query Class | Current | Target |
|---|---|---|
| PK point lookups | mostly efficient | stay local/targeted |
| PK range scans | routed, but row-heavy return | filter/project pushdown at leaseholder |
| Non-PK filters | often coordinator-heavy | index + predicate pushdown where possible |
| Aggregations | mostly coordinator | partial agg on executors + final merge |
| Joins | mostly coordinator | distributed joins with locality-aware strategy |
| ORDER BY/LIMIT | coordinator-heavy | distributed top-k + bounded merge |

## Correctness Model Requirements

All distributed execution phases must preserve:

1. Snapshot consistency per statement:
- Every distributed read stage binds to a statement snapshot token.
- Retries must reuse or explicitly re-acquire snapshot according to policy.

2. Replica/range movement safety:
- Stage tasks must tolerate split/merge/rebalance by re-routing with idempotent resume.
- No duplicate-visible rows after task retry or movement.

3. Transaction integration:
- Explicit SQL transaction paths keep current transaction-state and conflict semantics.
- Retryable conflicts remain deterministic (`40001` class where applicable).

## Pushdown Contract (Required)

Introduce a typed pushdown contract for storage-facing scans:

- `ScanSpec`
  - table id, projected columns, filter expression IR, limit, ordering hints
  - snapshot token / read barrier context
  - shard/range bounds

- `ScanChunk`
  - encoded arrow-compatible batch payload
  - resume cursor
  - stats (rows, bytes, scan latency, filtered rows)

- `ScanStats`
  - rows scanned vs returned
  - bytes read vs returned
  - local CPU time
  - wait/backpressure counters

No "best effort" silent downgrade: if pushdown is unsupported for an operator, planner must explicitly choose fallback and emit observability markers.

## Phased Delivery Plan

### Phase Ownership Matrix

| Phase | Ballista/DataFusion Implementation | HoloFusion/HoloStore Implementation | Key Interaction |
|---|---|---|---|
| A | plan/stage observability, explain placement | query-id propagation, routing-stage metrics | unified query/stage IDs across compute + storage |
| B | pushdown planner rules, expression lowering | typed scan RPC + leaseholder snapshot scan | `ScanSpec`/`ScanChunk` protocol |
| C | partial/final aggregate operators, distributed top-k | stats/hints for shard-local reduction | partial results flow from HoloStore-backed scans into Ballista merges |
| D | distributed join strategies + exchanges | topology/stats surfaces for locality | join strategy selection informed by shard/layout metadata |
| E | retry/resume/cancel behavior for stages | re-route + snapshot safety under movement | deterministic resume across compute/storage boundaries |
| F | SLO instrumentation for distributed plans | storage-path SLO instrumentation | end-to-end gates that include both planes |

## Phase A: Instrumentation and Plan Introspection Baseline

Scope:
- Add distributed-plan/execution traceability before major behavior changes.

Implementation split:
- Ballista/DataFusion:
  - emit operator placement and stage-level execution traces.
- HoloFusion/HoloStore:
  - expose per-shard scan RPC metrics and routing decisions with same query ID.
- Interaction:
  - a single query/stage identity must correlate gateway plan, executor stages, and storage scans.

Deliverables:
- Explain output showing operator placement (gateway vs executor).
- Per-stage metrics (rows/bytes in/out, latency, retries, spills).
- Query execution IDs correlated across nodes/logs/traces.

Quality gate:
- 100% of SQL requests have a query execution ID and stage timeline.
- No regression in current correctness suites.

## Phase B: Leaseholder Filter/Projection Pushdown

Scope:
- Push projection and supported predicates into leaseholder-local scan stage.

Implementation split:
- Ballista/DataFusion:
  - pushdown eligibility rules and expression-to-IR lowering.
- HoloFusion/HoloStore:
  - implement typed scan RPC execution on leaseholders with snapshot semantics.
- Interaction:
  - `ScanSpec` carries predicates/projection from Ballista to HoloStore; `ScanChunk` returns Arrow-compatible batches back to Ballista.

Deliverables:
- `ScanSpec` + `ScanChunk` RPC path.
- Typed predicate IR lowering from DataFusion expressions.
- Executor-side evaluation for supported expressions.

Quality gate:
- Functional parity with existing scan semantics.
- Predicate correctness tests across types/null semantics.
- p95 bytes returned reduced by >=40% on selective scan benchmark.

## Phase C: Distributed Partial Aggregation and Top-K

Scope:
- Add distributed partial aggregate and top-k operators.

Implementation split:
- Ballista/DataFusion:
  - partial aggregate/top-k stages and final merge operators.
- HoloFusion/HoloStore:
  - surface scan stats and locality hints that improve partial-stage placement.
- Interaction:
  - HoloStore-backed scan chunks feed Ballista partial stages; only reduced data flows to final merge.

Deliverables:
- Partial aggregate on executors.
- Final aggregate merge on gateway or merge executor.
- Distributed top-k with bounded memory merge.

Quality gate:
- Aggregate correctness under split/rebalance/failover tests.
- p95 latency reduction >=30% on aggregation benchmark at 3+ nodes.
- No unbounded memory growth under stress.

## Phase D: Distributed Join Execution

Scope:
- Introduce distributed join planning/execution with clear strategy selection.

Implementation split:
- Ballista/DataFusion:
  - colocated/broadcast/repartition join operators and exchange planning.
- HoloFusion/HoloStore:
  - provide layout, shard, and key-distribution stats used in strategy choice.
- Interaction:
  - join strategy selection must consume storage topology metadata to avoid unnecessary reshuffles.

Deliverables:
- Strategy set: colocated join, broadcast hash join, repartition hash join.
- Cost heuristics using row-count/size stats.
- Exchange operator with backpressure and bounded buffering.

Quality gate:
- Join correctness matrix (inner/left/basic predicates/null handling).
- Spill behavior validated when memory thresholds exceeded.
- Retry safety verified on executor restarts.

## Phase E: Failure Semantics, Recovery, and Resume

Scope:
- Harden distributed execution against node loss and topology churn.

Implementation split:
- Ballista/DataFusion:
  - stage retry/resume, cancellation propagation, and deterministic failure states.
- HoloFusion/HoloStore:
  - safe re-routing on leaseholder/range movement with snapshot compatibility checks.
- Interaction:
  - retries must preserve read correctness when tasks move between executors or leaseholders.

Deliverables:
- Stage-level retry/resume semantics with idempotent chunk delivery.
- Deterministic cancellation propagation.
- Re-route on leaseholder change with snapshot safety checks.

Quality gate:
- Fault injection suite:
  - executor crash during scan/agg/join,
  - gateway fail/retry,
  - split/merge/rebalance during query.
- Zero correctness regressions in linearizability/correctness suites.

## Phase F: Production Performance and SLO Gates

Scope:
- Lock production SLOs and make them CI/perf-gate enforced.

Implementation split:
- Ballista/DataFusion:
  - distributed compute SLO metrics and regression gates.
- HoloFusion/HoloStore:
  - storage-path latency/bytes/scans SLO metrics and regression gates.
- Interaction:
  - release gate is end-to-end; both compute-plane and storage-plane SLOs must pass together.

Deliverables:
- Benchmark package:
  - point read,
  - selective scan,
  - full scan,
  - agg-heavy,
  - join-heavy,
  - mixed OLTP/OLAP.
- Capacity model and tuning guide.
- Regression gates in CI/perf pipelines.

Quality gate:
- Meets agreed SLO targets for p50/p95/p99 and throughput.
- Stable under sustained load + failure scenarios.

## Production Quality Bar (Mandatory Across All Phases)

A phase is not complete unless all are true:

1. Correctness:
- Existing SQL/HoloStore correctness suites pass.
- New phase-specific correctness/fault tests pass.

2. Reliability:
- Restart/retry semantics validated.
- Backpressure and timeout paths tested.

3. Observability:
- Metrics, traces, and explain output cover new behavior.
- Stuck-stage detection and alert signals exist.

4. Performance:
- Baseline comparison published.
- Phase-specific performance goals met without hidden regressions.

## CockroachDB-Inspired Alignment (Without Copying Internals)

Target behavior we should converge toward:

- Logical planning at gateway, distributed physical operators across nodes.
- Early data reduction near ranges (filter/project/partial agg).
- Explicit exchange operators with flow control.
- Deterministic distributed execution metadata for diagnostics.

Where we deliberately differ:

- Correctness boundary remains HoloStore/Accord-native.
- Storage and metadata contracts remain HoloFusion-specific.

## Operational and Safety Controls

Required controls:

- Per-query admission control and stage concurrency limits.
- Memory quotas per stage; spill policy where needed.
- Query timeout + cancel propagation end-to-end.
- Circuit-breaker behavior for overloaded executors.

Required operator visibility:

- `EXPLAIN DIST`-style placement output.
- per-node active stages and queue depth.
- bytes sent/received by exchange.
- skew indicators (largest shard contribution, straggler stage).

## Risk Register

1. Risk: pushdown semantic mismatch with DataFusion expression behavior.
- Mitigation: typed IR + exhaustive expression equivalence tests.

2. Risk: retries duplicate chunk delivery.
- Mitigation: chunk sequence IDs + idempotent merge contracts.

3. Risk: topology churn invalidates running plans.
- Mitigation: re-route checkpoints + bounded restart at stage boundaries.

4. Risk: distributed joins cause memory pressure.
- Mitigation: spill path + strict quotas + skew detection.

## Acceptance Criteria for "Cockroach-Like Distributed SQL Baseline"

We can claim baseline completion when:

- Selective scans and aggregations are primarily data-local.
- Coordinator is no longer a single-row-fan-in bottleneck for standard analytical queries.
- Distributed joins run with bounded memory and deterministic retries.
- Fault injection demonstrates safe execution during node loss and topology changes.
- SLO gates are enforced in automation and routinely passing.

## Immediate Next Steps

1. Implement Phase A instrumentation and explain placement output.
2. Implement `ScanSpec`/`ScanChunk` pushdown contract.
3. Land Phase B filter/projection pushdown with benchmark gates.
