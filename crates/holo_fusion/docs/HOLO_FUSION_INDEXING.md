# Holo Fusion Indexing

This document defines HoloFusion secondary-index support, index key/value layout,
and core operational semantics.

## Goals

- PostgreSQL-compatible DDL entry points for secondary indexes.
- Production-safe uniqueness enforcement using conditional replicated writes.
- Deterministic index write ordering and rollback behavior under failures.
- Hotspot-aware hash distribution option aligned with existing hash-PK strategy.
- Index lifecycle support for online create/backfill/public transitions.

## Supported SQL Scope

- `CREATE INDEX [IF NOT EXISTS] <name> ON <table> (<columns...>)`
- `CREATE UNIQUE INDEX ...`
- `CREATE INDEX ... USING HASH (...)`
- `CREATE INDEX ... INCLUDE (<columns...>)`
- `CREATE INDEX ... WITH (hash_buckets=<n>)` for hash-distributed indexes
- `DROP INDEX [IF EXISTS] <name>`

### Current limitations

- `CREATE INDEX CONCURRENTLY` is not supported.
- Partial indexes (`WHERE ...`) are not supported.
- Descending key columns are not supported.
- `NULLS NOT DISTINCT` is not supported.
- Secondary indexes are currently enabled for `row_v1` tables.

## Metadata Model

Secondary-index metadata is persisted separately from table metadata.

- Metadata key prefix: `0x04`
- Metadata key shape: `{prefix}{db_id}{schema_id}{table_id}{index_id}`
- Value: JSON-encoded `SecondaryIndexRecord`

Key fields:

- `index_name`
- `unique`
- `key_columns`
- `include_columns`
- `distribution` (`range` or `hash`)
- `hash_bucket_count` (for hash distribution)
- `state` (`write_only`, `public`)

## Data-Key Layout

### Secondary index row entries

- Data key prefix: `0x21`
- Key shape:
  - `{0x21}{table_id}{index_id}`
  - Optional hash segment `{0x68}{bucket_u16}` for hash indexes
  - Encoded key-column tuple bytes
  - Primary-key suffix `{pk_tag}{not_null}{ordered_i64_pk}`

The primary-key suffix guarantees deterministic uniqueness per row.

### Unique reservation entries

- Data key prefix: `0x22`
- Key shape mirrors row-entry prefix + encoded index-key tuple
- Value stores the owning primary key

Unique reservation entries are used for conflict detection (`expected_version=0`) and
therefore enforce uniqueness without relying on read-after-write races.

### Tombstones

Index row and reservation deletions use SQL tombstone payloads compatible with
existing storage conflict semantics.

## Write Semantics

Primary-row writes and index writes are planned and applied in one conditional
batch transaction per statement-phase chunk.

For each row mutation:

- `INSERT`: add row-index entry + optional unique reservation.
- `UPDATE`: remove stale index keys, update covering payload as needed,
  add new keys/reservations when key changes.
- `DELETE`: remove row-index entry + optional unique reservation.

Rollback metadata is captured for all index keys in the same write pipeline,
so failures revert both primary and index effects together.

## Online Create / Backfill Lifecycle

`CREATE INDEX` lifecycle:

1. Persist index metadata in `write_only` state.
2. Refresh provider registration so new writes maintain the index.
3. Backfill existing rows in bounded page chunks.
4. Promote metadata state to `public`.
5. Re-register provider to expose final state.

If backfill fails, metadata is cleaned up and the statement returns an error.

## Uniqueness and NULL Semantics

Unique indexes follow PostgreSQL default null behavior (`NULLS DISTINCT`):
rows with any `NULL` key component do not conflict with each other.

## Distribution Strategy

Secondary indexes support:

- `range` distribution for ordered access patterns.
- `hash` distribution with configurable buckets for hotspot mitigation on
  sequential/skewed keys.

This aligns with HoloFusion’s hash-PK and flow-control architecture.

## Operational Notes

- Index creation performs online backfill; plan capacity for initial build.
- Hash bucket count should be selected with expected write concurrency and shard
  count in mind.
- Drop index removes metadata and tombstones underlying index keys lazily via
  normal compaction/retention behavior.

## Cost-Based Access Path Selection

Secondary indexes are now integrated with a modular optimizer stack:

- Predicate extraction (`optimizer/predicates.rs`) normalizes conjunctions, `IN`
  lists, and range bounds.
- Statistics (`optimizer/stats.rs`) maintain sampled row-count/NDV/null-fraction,
  histogram, MCV, and multi-column prefix summaries.
- Cost model (`optimizer/cost.rs`) is pluggable and currently uses a
  distributed-aware baseline that prices scan bytes, CPU, fanout RPCs, probe
  count, and lookup penalties.
- Access-path planner (`optimizer/planner.rs`) enumerates table scan and index
  probe candidates (including covering/index-only eligibility where possible)
  and chooses the lowest-cost path with uncertainty guardrails.
- Runtime feedback (`optimizer/feedback.rs`) tracks estimate-vs-actual error and
  adapts cardinality multipliers per query signature.
- Learned shadow model (`optimizer/learned.rs`) records online corrections in
  shadow mode without taking over plan decisions.

Planner behavior remains conservative: when uncertainty is high and index gain
is marginal, table scan is preferred for stability.
