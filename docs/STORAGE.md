# Storage and WAL

This document describes how HoloStore persists data and how durability/recovery work today.

## WAL (Commit Log)

HoloStore uses a **custom commit log** implementation (`crates/holo_store/src/wal.rs`).
It acts as the authoritative durability source for committed commands.

Key points:
- Commits are appended before execution.
- Appends are batched to amortize syscalls and fsync.
- Each record includes a CRC32 checksum for corruption detection.
- Persistence mode is configurable:
  - `sync_data`: fsync data only (durable data)
  - `sync_all`: fsync data + metadata
  - `buffer`: no fsync (non‑durable, for perf testing)

Tuning:
- `HOLO_WAL_COMMIT_BATCH_MAX`
- `HOLO_WAL_COMMIT_BATCH_WAIT_US`
- `HOLO_WAL_PERSIST_EVERY`
- `HOLO_WAL_PERSIST_INTERVAL_US`
- `HOLO_WAL_PERSIST_MODE`

## Storage Engine (Fjall)

HoloStore uses **Fjall** (an LSM‑tree KV engine) as the backing store.
We open a Fjall keyspace and use partitions for versions and latest values
(`crates/holo_store/src/kv.rs`).

Fjall durability settings:
- `manual_journal_persist` can be enabled to decouple persistence from every write.
- `fsync_ms` can be used to control periodic fsync.

Relevant flags:
- `HOLO_FJALL_MANUAL_JOURNAL_PERSIST`
- `HOLO_FJALL_FSYNC_MS`

## Durability Model

The WAL is the **source of truth** for durability. Fjall is treated as the
execution state that can be rebuilt from the WAL on restart.

This means:
- If WAL persistence is enabled, committed data survives crashes.
- If WAL persistence is disabled (`buffer`), durability is not guaranteed.
- Fjall may lag behind WAL but can be replayed.

## Recovery Flow

On restart:
1. WAL is replayed to recover committed commands.
2. Commands are re‑applied to Fjall.
3. Any missing command bytes can be fetched from peers during recovery.

The system also maintains an in‑memory executed log for lagging replicas. GC
is driven by executed‑prefix gossip from peers.

## Tuning Knobs

WAL:
- `HOLO_WAL_PERSIST_MODE=sync_data|sync_all|buffer`
- `HOLO_WAL_PERSIST_EVERY` / `HOLO_WAL_PERSIST_INTERVAL_US`
- `HOLO_WAL_COMMIT_BATCH_MAX` / `HOLO_WAL_COMMIT_BATCH_WAIT_US`

Fjall:
- `HOLO_FJALL_MANUAL_JOURNAL_PERSIST=true|false`
- `HOLO_FJALL_FSYNC_MS=<ms>`

Use these to balance throughput, latency, and durability.
