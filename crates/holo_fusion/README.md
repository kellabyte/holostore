# HoloFusion

`holo_fusion` is the SQL layer for HoloStore:

- PostgreSQL wire protocol for clients (`psql`, BI tools, drivers)
- DataFusion for SQL planning/execution
- Optional Ballista standalone execution mode
- Embedded HoloStore node in each process as the storage/consensus backend

The goal is to provide a practical distributed SQL surface while preserving
HoloStore correctness and routing semantics.

## What Runs In One `holo-fusion` Process

```text
                    +-------------------------+
SQL client -------->| Postgres wire endpoint  |
                    +------------+------------+
                                 |
                                 v
                    +-------------------------+
                    | DataFusion SQL runtime  |
                    +------------+------------+
                                 |
                    +------------+------------+
                    |                         |
                    v                         v
      +-------------------------+   +-----------------------+
      | HoloStore TableProvider |   | DML hook / txn layer  |
      +------------+------------+   +-----------+-----------+
                   \                         /
                    \                       /
                     v                     v
                    +-------------------------+
                    | Embedded HoloStore node |
                    | (routing + KV + Accord) |
                    +-------------------------+
```

Optional: Ballista standalone mode can be enabled for distributed planning.

## Main Components

- `src/main.rs`
  - binary entrypoint (`holo-fusion`)
  - loads `HoloFusionConfig` from env
  - starts full runtime

- `src/lib.rs`
  - runtime orchestration
  - health lifecycle (`Bootstrapping`, `Ready`, `Degraded`, `NotReady`)
  - embedded HoloStore startup and SQL server wiring

- `src/provider.rs`
  - `TableProvider` implementation backed by HoloStore keys
  - scan/pushdown behavior and row encoding

- `src/mutation.rs`
  - DML query hook (`INSERT`/`UPDATE`/`DELETE`)
  - transaction/session handling over SQL workflow

- `src/metadata.rs`
  - SQL table metadata records persisted in HoloStore
  - create/list/find metadata APIs

- `src/catalog.rs`
  - bootstraps and syncs DataFusion catalog from stored metadata

- `src/topology.rs`
  - fetches cluster state
  - routes scans/keys to shard leaseholders/ranges

- `src/metrics.rs`
  - pushdown and request execution metrics used for observability

- `src/pg_compat.rs`
  - PostgreSQL compatibility helpers/UDFs used by SQL clients

- `src/pg_catalog_ext.rs`
  - dynamic `pg_catalog` extensions layered over `datafusion-pg-catalog`
  - live `pg_catalog.pg_locks` backed by SQL transaction state

- `src/ballista_codec.rs`
  - logical extension codec used when Ballista SQL is enabled

## Design Notes

- HoloStore is the source of truth for data and transactional ordering.
- SQL paths use direct embedded HoloStore integration, not Redis protocol hops.
- Metadata and data live in separate logical prefixes (see storage model doc).
- Routing supports range mode and hash mode through HoloStore topology state.

## Current Scope

What is implemented today:

- Postgres wire server lifecycle
- Metadata-backed table registration
- HoloStore-backed reads and writes via provider + hooks
- Cluster scripts for local multi-node testing
- Health endpoint and degraded mode behavior
- Phase 9 distributed SQL baseline:
  - query/stage execution IDs and stage timeline metrics
  - typed storage scan contract with retry/reroute resume behavior
  - distributed explain placement classification (`EXPLAIN`)
  - distributed planner defaults for agg/top-k/join repartitioning

What is intentionally not full-featured yet:

- Full PostgreSQL feature parity
- Full SQL grammar/optimizer surface
- Long-tail distributed SQL semantics found in mature systems

## Quick Start

Run a local 3-node `holo_fusion` cluster:

```bash
./crates/holo_fusion/scripts/start_cluster.sh
```

Stop it:

```bash
./crates/holo_fusion/scripts/stop_cluster.sh
```

Connect with `psql` to node1 default port:

```bash
psql "host=127.0.0.1 port=55432 user=datafusion dbname=datafusion sslmode=disable"
```

Run single process directly:

```bash
cargo run -p holo_fusion --bin holo-fusion
```

## Useful Environment Variables

- `HOLO_FUSION_PG_HOST`, `HOLO_FUSION_PG_PORT`
- `HOLO_FUSION_HEALTH_ADDR`
- `HOLO_FUSION_ENABLE_BALLISTA_SQL`
- `HOLO_FUSION_DML_PREWRITE_DELAY_MS`
- `HOLO_FUSION_DML_STATEMENT_TIMEOUT_MS`
- `HOLO_FUSION_DML_MAX_INFLIGHT_STATEMENTS`
- `HOLO_FUSION_DML_MAX_SCAN_ROWS`
- `HOLO_FUSION_DML_MAX_TXN_STAGED_ROWS`
- `HOLO_FUSION_DML_WRITE_MAX_BATCH_ENTRIES`
- `HOLO_FUSION_DML_WRITE_MAX_BATCH_BYTES`
- `HOLO_FUSION_SCAN_PARALLELISM`
- `HOLO_FUSION_INDEX_BACKFILL_PAGE_SIZE`
- `HOLO_FUSION_INDEX_BACKFILL_WRITE_MAX_BATCH_ENTRIES`
- `HOLO_FUSION_INDEX_BACKFILL_WRITE_MAX_BATCH_BYTES`
- `HOLO_FUSION_INDEX_BACKFILL_WRITE_PIPELINE_DEPTH`
- `HOLO_FUSION_INDEX_BACKFILL_TARGET_PARALLELISM`
- `HOLO_RANGE_WRITE_BATCH_TARGET` (embedded HoloStore range-write proposal chunk target; default `1024`)
- `HOLO_RANGE_WRITE_BATCH_MAX_BYTES` (embedded HoloStore range-write proposal byte cap; default `1048576`)
- `HOLO_FUSION_SCAN_MAX_ROWS` (`0` disables scan-row rejection; default `0`)
- `HOLO_FUSION_SQL_TARGET_PARTITIONS`
- `HOLO_FUSION_SQL_SORT_SPILL_RESERVATION_BYTES`
- `HOLO_FUSION_SQL_SPILL_COMPRESSION` (`lz4_frame`, `zstd`, `uncompressed`)
- `HOLO_FUSION_NODE_ID`
- `HOLO_FUSION_HOLOSTORE_REDIS_ADDR`
- `HOLO_FUSION_HOLOSTORE_GRPC_ADDR`
- `HOLO_FUSION_BOOTSTRAP`
- `HOLO_FUSION_JOIN_ADDR`
- `HOLO_FUSION_INITIAL_MEMBERS`
- `HOLO_FUSION_HOLOSTORE_DATA_DIR` (default `./tmp/cluster/holo_fusion/node-<node_id>`)
- `HOLO_FUSION_HOLOSTORE_MAX_SHARDS`
- `HOLO_FUSION_HOLOSTORE_INITIAL_RANGES`
- `HOLO_FUSION_HOLOSTORE_ROUTING_MODE` (`range` or `hash`)

## Tests

Run crate tests:

```bash
cargo test -p holo_fusion
```

Key test files:

- `crates/holo_fusion/tests/smoke.rs`
- `crates/holo_fusion/tests/distributed.rs`
- `crates/holo_fusion/tests/bench_slo.rs` (ignored; run explicitly)

## Detailed Docs

- `crates/holo_fusion/docs/DATA_FUSION.md`
- `crates/holo_fusion/docs/HOLO_FUSION_STORAGE_MODEL.md`
- `crates/holo_fusion/docs/HOLO_FUSION_SQL_SCOPE.md`
- `crates/holo_fusion/docs/HOLO_FUSION_TODO.md`
- `crates/holo_fusion/docs/HOLO_FUSION_INDEXING.md`
- `crates/holo_fusion/docs/DISTRIBUTED_SQL_EXECUTION_DESIGN.md`
- `crates/holo_fusion/docs/METADATA_MIGRATION.md`
- `crates/holo_fusion/docs/PHASE7_BENCHMARK_SLO.md`
- `crates/holo_fusion/docs/HOLO_FUSION_RUNBOOK.md`
