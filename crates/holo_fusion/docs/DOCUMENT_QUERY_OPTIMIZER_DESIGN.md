# HoloFusion Document Query Optimizer Design

## Purpose

Define a production-grade document query optimizer for HoloFusion that:

- uses HoloStore/HoloFusion as the correctness authority,
- uses secondary indexes for primary acceleration,
- uses `rsonpath-lib` as a fast residual path evaluator where applicable,
- stays compatible with future document-as-row/column storage models.

This design is intentionally explicit about planner boundaries: `rsonpath-lib`
is an execution primitive, not the global optimizer.

## Architectural Decision: Hybrid Optimizer + Residual Engine

Decision:

- We **will build a HoloFusion-native document optimizer** with a typed logical IR.
- We **will use secondary indexes first** for selectivity and pruning.
- We **will use `rsonpath-lib` only for residual path evaluation** where semantics
  and feature support are compatible.
- We **will not delegate index choice, join/order/aggregate planning, or global cost
  decisions to `rsonpath-lib`**.

Why this split is required:

- `rsonpath-lib` is a high-performance JSONPath engine, not a distributed planner.
- Indexing, routing, snapshot semantics, retry/churn behavior, and write correctness
  are HoloFusion/HoloStore responsibilities.
- A typed optimizer boundary keeps compatibility when storage evolves from JSON blobs
  to materialized row/column layouts.

## Current State (Relevant to This Design)

- HoloFusion already has:
  - typed scan contracts and pushdown boundaries (`ScanSpec`/`ScanChunk`/`ScanStats`);
  - shard/range routing and retry/reroute behavior;
  - distributed plan placement and stage observability;
  - secondary index metadata/lifecycle defined in `HOLO_FUSION_INDEXING.md`.
- Current pushdown behavior is SQL-centric and not yet document-IR aware.
- No end-to-end document optimizer currently exists.

## Problem Statement

If we attach JSONPath evaluation directly to scans without a planner:

- broad scans become the default path;
- secondary indexes are underused;
- distributed resources are wasted on non-selective predicates;
- future row/column materialization requires rewriting user-facing semantics.

We need one optimizer that can:

1. classify predicates into indexable vs residual forms,
2. choose scan/index plans by cost,
3. preserve correctness under topology churn,
4. execute residual checks efficiently (including `rsonpath-lib` fast paths).

## Design Goals

1. Correctness first:
- statement-level snapshot consistency with existing HoloStore semantics;
- deterministic retry/reroute and duplicate suppression.

2. Performance:
- index-first candidate pruning;
- data-local execution;
- bounded residual evaluation.

3. Surface independence:
- support multiple frontends (SQL+JSON, API DSL, optional GraphQL) by compiling
  all of them into one document IR.

4. Forward compatibility:
- preserve query semantics when physical storage moves from document blobs to
  row/column materialization.

## Non-Goals

- Full MongoDB API or wire-protocol compatibility.
- Replacing DataFusion/Ballista internals.
- Treating `rsonpath-lib` as a complete replacement for filtering, sorting,
  grouping, and update planning.

## Query Model

All document queries compile into a shared logical request:

```text
DocumentQueryRequest {
  namespace, collection,
  projection,
  filter (DocPredicateExpr),
  sort,
  page_args,
  group_by,
  aggregates,
  having,
  consistency/snapshot options
}
```

## Core IR

### Predicate IR

```text
DocPredicateExpr =
  And(Vec<DocPredicateExpr>)
  Or(Vec<DocPredicateExpr>)
  Not(Box<DocPredicateExpr>)
  Compare { path, op, value }
  Exists { path }
  In { path, values }
  JsonPathRaw { query, params }
```

### Planning Classification

Each predicate is classified into one of:

- `Indexable`: can be satisfied by one or more secondary indexes.
- `StoragePushdown`: can be evaluated by storage scan operators without full doc decode.
- `Residual`: must be evaluated after candidate fetch.

The optimizer tracks this classification explicitly in the plan.

## `rsonpath-lib` Integration Boundary

`rsonpath-lib` is used in the residual stage when:

- the residual expression can be represented by supported JSONPath selectors;
- input is available as valid UTF-8 JSON bytes;
- query has been compiled/cached successfully.

If any condition fails, fallback residual evaluators are used.

Important constraint:

- `rsonpath-lib` currently exposes a subset-oriented API (pre-`1.0` stability).
  The optimizer must maintain a capability matrix and never assume full JSONPath
  feature parity.

## Secondary Index Leverage

The optimizer does not ask `rsonpath-lib` to use indexes. Instead it does:

1. Predicate extraction:
- extract indexable atoms from `DocPredicateExpr`.

2. Candidate plan enumeration:
- primary scan;
- single-index scan;
- multi-index intersection/union;
- covering index plan when projection is fully index-covered.

3. Cost selection:
- choose lowest estimated cost plan using selectivity/cardinality statistics.

4. Residual execution:
- only evaluate non-indexable predicates on the reduced candidate set.

## Physical Plan Shape

Canonical physical plan:

```text
DocIndexScan/DocPrimaryScan
   -> CandidatePkSet
   -> DocFetch (only required fields)
   -> DocResidualFilter (rsonpath-capable subset + fallback evaluator)
   -> Sort/Page or Group/Aggregate/Having
   -> Project/Encode result
```

## Cost Model (Initial)

Initial model (simple, deterministic):

- `cost(scan) ~= rows_in_range * row_decode_cost`
- `cost(index) ~= index_entries_read + candidate_fetch_cost + residual_cost`
- `cost(intersection) ~= sum(index_reads) + intersect_cost + fetch_cost`

Selection inputs:

- index cardinality / NDV,
- histogram-ish range selectivity,
- candidate fetch fanout,
- residual selectivity estimate.

Phase 1 can use heuristic selectivity defaults when statistics are sparse.

## Distributed Execution and Routing

Document plans reuse existing HoloFusion routing principles:

- route index and primary key ranges to leaseholders;
- execute shard-local candidate generation;
- stream candidate PKs/chunks with resume cursors;
- perform dedup/idempotent merge across retries/reroutes;
- push partial aggregation where safe.

Candidate and result streams must carry query execution id + stage id for traceability.

## Correctness and Failure Semantics

The document optimizer must preserve:

1. Statement snapshot consistency:
- index reads and primary fetches bind to one statement snapshot policy.

2. Retry safety:
- reroute and retry must be idempotent; duplicate candidate PKs are suppressed.

3. Index/data consistency:
- write path remains authoritative through existing conditional replicated writes
  and rollback semantics.

## Storage Evolution: JSON Blob -> Row/Column

This design stays valid if documents are later materialized into columns:

- same `DocPredicateIR` at the API boundary;
- planner first maps paths to materialized columns/indexes;
- unmapped paths stay residual;
- residual evaluator can still use JSON reconstruction or sidecar JSON payload.

Result: no user-facing query rewrite required during storage migration.

## Aggregation / GroupBy / Having Strategy

- Aggregation is planned by HoloFusion/DataFusion operators, not `rsonpath-lib`.
- `group_by` and aggregate projection are pushdown-eligible when index/column paths
  are materialized and distributable.
- `having` is applied post-aggregate and can only be index-assisted when it can be
  rewritten to pre-aggregate predicate forms.

## Updates and Patch Planning

Document updates are planned as:

1. candidate selection (`indexable + residual`),
2. conflict-safe fetch/version check,
3. patch application,
4. secondary index delta planning,
5. conditional write + rollback payload generation.

Patch semantics remain engine-owned; JSONPath is only a selector aid.

## Observability Requirements

Add document planner metrics and traces:

- `doc_query_plans_total{kind=index|scan|covering|intersection}`
- `doc_query_residual_rows_in_total`
- `doc_query_residual_rows_out_total`
- `doc_query_index_candidates_total`
- `doc_query_fallback_eval_total{reason=unsupported|compile_error|invalid_json}`
- `doc_rsonpath_compile_cache_hit_total`
- `doc_rsonpath_compile_cache_miss_total`
- stage timeline events for classify/enumerate/select/execute phases.

## Rollout Plan

### Phase A: IR + Classification

- Add `DocPredicateIR`, classifier, and explain output.
- No behavior change; shadow-plan only.

### Phase B: Index-Aware Read Plans

- Enable index/single-intersection plans for equality/range predicates.
- Residual evaluated with existing safe fallback evaluator.

### Phase C: `rsonpath-lib` Residual Fast Path

- Add capability-gated residual operator using compiled query cache.
- Enforce deterministic fallback on unsupported constructs.

### Phase D: Distributed Candidate Planning

- Push candidate generation to leaseholder-local execution.
- Add idempotent merge and retry/reroute invariants.

### Phase E: Aggregate/GroupBy Integration

- Add distributed aggregate planning for document query forms.
- Add covering-index and partial-aggregate placement rules.

### Phase F: Storage Evolution Compatibility

- Add materialized-path mapping layer for row/column storage.
- Keep the same user-facing query semantics and IR.

## Testing and Quality Gates

Required gates per phase:

- Correctness:
  - deterministic results under split/merge/rebalance churn;
  - conflict-safe write/update semantics with index maintenance.
- Resiliency:
  - retry/reroute with no duplicate-visible result rows.
- Performance:
  - measurable reduction in fetched rows vs primary scan baseline for
    indexable predicates;
  - bounded p95/p99 regression limits in canary suites.

## Open Risks and Mitigations

1. Risk: `rsonpath-lib` feature/stability gaps (pre-`1.0` API).
- Mitigation: strict capability matrix + mandatory fallback evaluator.

2. Risk: poor selectivity estimates causing wrong plan choice.
- Mitigation: conservative heuristics first; add stats collection and adaptive
  re-planning thresholds.

3. Risk: candidate explosion with weak predicates.
- Mitigation: hard candidate caps, spill-safe pipelines, and deterministic
  fallback to bounded primary scans.

## Summary

The document optimizer should treat `rsonpath-lib` as a high-performance residual
operator inside an index-aware, cost-based, distributed plan architecture.

This gives immediate performance wins for document workloads while keeping a clean
path to future row/column materialization and advanced indexing.
