# HoloStore — Design Notes

This document reflects the current implementation (Accord consensus, batched WAL, async apply, and fjall-backed storage). It focuses on the high-level design and the concurrency/durability tradeoffs that exist today.

## Overview

HoloStore is a distributed, strongly-consistent key/value store built on the Accord consensus algorithm. Each data shard maps to an Accord group. Clients talk to a Redis-compatible interface; commands are routed into the consensus engine, committed by quorum, and then executed against the storage engine.

## Accord Groups

Accord is leaderless. Any node can coordinate proposals.

Writes (`SET`):
- Proposals go through pre-accept → accept → commit.
- Dependencies between transactions are tracked per key.
- A fast path is possible when dependencies converge.
- Committed transactions are executed asynchronously by the executor loop.

Reads (`GET`):
- Reads can go through the protocol or other modes (see `docs/READ_MODES.md`).

## Concurrency Model

The system deliberately separates consensus, durability, and execution:
- Consensus runs on the async runtime (RPC handling + proposal coordination).
- Commit log appends are batched on a dedicated commit-log thread.
- Storage applies are executed on a dedicated apply worker thread to keep the async runtime responsive.

This separation reduces head-of-line blocking and allows batching to amortize IO.

## Execution Scheduling

Committed transactions are tracked in two queues:
- `committed_queue`: all committed txns ordered by sequence.
- `committed_ready`: committed txns whose dependencies are already executed.

When a transaction executes, its dependents are decremented in a pending-deps map and moved into `committed_ready` when unblocked. This avoids repeated scans of the entire committed queue under load.

If cycles exist or ready items cannot be found, the executor falls back to a dependency SCC scan to resolve a consistent execution order.

## Batching

Batching exists at multiple layers:
- Client batching coalesces multiple SETs into a single proposal.
- RPC batching coalesces pre-accept/accept/commit messages per peer.
- Commit-log batching writes multiple commits per WAL append.
- Apply batching writes multiple commands to the state machine at once.

Batch sizes are tunable via environment variables and are critical to throughput/latency tradeoffs.

## Commit Log (WAL)

The commit log is the durability source of truth.

Design:
- Commits are appended to the WAL before they are applied.
- Appends are batched to amortize syscalls and fsync.
- The WAL can run with different persistence modes:
  - `sync_data` (durable data), `sync_all`, or `buffer` (non-durable).
- A background compaction path removes executed entries once safe.

Key idea: the WAL retains committed commands, so a node can recover even if the storage engine lags behind execution.

## Storage Engine (Fjall)

The state machine writes to fjall. Durability depends on fjall’s own fsync policy plus the WAL.

Current behavior:
- WAL is the authoritative durability source.
- Fjall can be configured to fsync periodically (`HOLO_FJALL_FSYNC_MS`), or disabled.
- On restart, WAL replay repopulates fjall state.

## Recovery

Recovery uses three sources of truth:
- WAL replay for committed commands on startup.
- Executed-log values to satisfy lagging replicas that need command bytes.
- Peer recovery RPCs when a node lacks command bytes locally.

Executed-log GC:
- Nodes gossip executed prefixes per origin.
- A global minimum executed prefix is computed.
- Executed-log entries at or below that prefix are eligible for GC.

## Observability

`HOLOSTATS` exposes internal counters and timings via Redis:
- Consensus progress counters and execution latency.
- WAL batch and fsync metrics.
- Queue lengths and executed-log sizes.

These metrics are the primary tool for diagnosing stalls, batching behavior, and IO pressure.

## Key Tradeoffs

- Larger batches increase throughput but raise tail latency.
- More shards reduce per-group contention but increase parallelism and memory.
- WAL persistence settings directly affect durability and performance.
- Read mode choices affect consistency (see `docs/READ_MODES.md`).

## Running Linearizability Checks

Porcupine tests live under `scripts/check_linearizability.sh` and are wired to `make check-linearizability`.

## Partitioning

HoloStore partitions data by **Accord group**. Each partition is a separate consensus group with its
own log, execution queue, and state-machine application. At startup the node creates
`HOLO_DATA_SHARDS` data groups (IDs starting at a fixed base), and the Redis front-end
routes each key to a shard.

Current characteristics:
- Shard count is static per node at startup (`HOLO_DATA_SHARDS`).
- Each shard has its own commit log and executor.
- Sharding reduces contention per group and improves parallelism.

**Future direction:**
The long-term goal is **dynamic sharding**, similar to CockroachDB’s range model. That means:
- Smaller logical ranges that can split/merge based on load.
- Rebalancing ranges across nodes without downtime.
- Independent consensus groups per range with automated movement.

This will allow the system to scale with workload hotspots and keep per-group queues small,
while retaining Accord’s correctness and concurrency benefits.
