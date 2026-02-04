# Raft-Engine WAL Integration

This document describes how HoloStore integrates `raft-engine` as the default WAL backend, and how `raft-engine` features are mapped to HoloStore’s existing WAL semantics.

## Enablement

`raft-engine` is **optional** and gated behind a Cargo feature.

Build:
```bash
cargo build -p holo_store --features raft-engine
```

Run (CLI or env):
```bash
HOLO_WAL_ENGINE=raft-engine ./scripts/start_cluster.sh
```

If `HOLO_WAL_ENGINE=raft-engine` is set without the feature enabled (e.g., built with `--no-default-features`), startup fails with a clear error.

## Architecture Overview

The integration lives in `crates/holo_store/src/wal.rs` as an additional `CommitLog` implementation:
- `FileWal`: existing DIY file-backed WAL (legacy option).
- `RaftEngineWal`: new optional backend using `raft-engine`.

At runtime, the node chooses the WAL with:
- `--wal-engine file|raft-engine`
- or `HOLO_WAL_ENGINE=raft-engine|file`

Both backends implement the same `CommitLog` trait and are used through a `dyn CommitLog` handle in `main.rs`.

## Mapping Accord Concepts to Raft-Engine

### Group ID (region)
- Accord’s group is derived from `TxnId` using `txn_group_id`.
- That group id is used as the `raft-engine` **region id**.
- This matches `raft-engine`’s multi-group model (one log stream per region).

### Log Index
`raft-engine` requires **consecutive indices** per region. We maintain a per-group `last_index` counter and assign:
```
log_index = last_index + 1
```

We intentionally keep this distinct from `CommitLogEntry.seq`, which is an Accord sequence for conflict resolution, not a storage index.

### Entry Encoding
We reuse the existing compact binary encoding from `FileWal`:
- `txn_id` (node_id + counter)
- `seq`
- `deps[]`
- `command` bytes

The encoded bytes are stored as the value in `raft-engine` without using protobuf. This keeps the WAL format consistent across both backends.

### Key Layout
Within each `raft-engine` region:
- Entry keys are `b'e' + log_index` (big-endian u64).
- The entry payload is the encoded `CommitLogEntry`.

Metadata:
- Region `0` contains a list of active groups in `meta/groups`.
- Each group stores its `last_index` in `meta/last_index`.

This lets replay discover all regions without external config.

## Write Path Mapping

HoloStore batches commits at the Accord group level, then forwards batches into the WAL.

In `RaftEngineWal::append_commits`:
1. Compute per-group next indices.
2. Add entries to a single `LogBatch`.
3. Update `meta/last_index` and `meta/groups` in the same batch.
4. Call `engine.write(batch, sync)`.

### Sync/Flush Behavior
We map HoloStore’s existing persistence knobs to `raft-engine`’s `sync` flag:
- `HOLO_WAL_PERSIST_EVERY`
- `HOLO_WAL_PERSIST_INTERVAL_US`
- `HOLO_WAL_PERSIST_MODE` (`none`, `buffer`, `sync_data`, `sync_all`)

When a sync threshold is hit, we call `engine.write(..., sync = true)`; otherwise `sync = false`.

Note: `raft-engine` exposes a single sync boolean per write. We map `sync_data` and `sync_all` to `sync = true` (conservative choice).

## Replay Path

`CommitLog::load()`:
1. Read `meta/groups` to discover groups.
2. For each group, read `meta/last_index`.
3. Scan from index `1..=last_index`, decoding each present entry.
4. Return all entries (caller filters by group).

This keeps recovery logic identical to the file WAL: Accord replays and executes committed entries to rebuild in-memory state.

## Compaction/GC

HoloStore already tracks `mark_executed(txn_id)` and periodically calls `compact(max_delete)`.

In the `raft-engine` WAL:
- `mark_executed` records the txn in an in-memory set.
- `compact(max_delete)`:
  - maps executed txn ids to their log indices,
  - sorts by index,
  - deletes up to `max_delete` entries via a `LogBatch`.

This mimics the file WAL’s “drop executed commits” behavior.

## Tradeoffs and Known Limitations

- **Executed tracking is in-memory**: on crash/restart, executed markers are lost and entries may be retained longer until replay re-executes and re-marks them.
- **Sequential index allocation**: indices are per-group and strictly monotonic, which is required by `raft-engine`.
- **No protobuf entry**: we store encoded bytes directly (simpler and consistent with the file WAL).
- **Sync mapping is conservative**: we map both `sync_data` and `sync_all` to `sync=true` in `raft-engine`.

## Summary

`raft-engine` provides the multi-group log storage and batch/GC mechanics HoloStore needs. The integration keeps the external WAL behavior stable:
- same `CommitLog` trait,
- same entry encoding,
- same replay/compaction workflow,
- runtime selection for A/B comparison against the existing file WAL.
