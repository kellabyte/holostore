# Cluster Membership and Metadata (Control Plane)

This document tracks the plan and implementation status for dynamic cluster
membership and Cockroach-style control-plane metadata.

## Goals

- A **global control-plane consensus group** (meta group) that stores:
  - cluster membership
  - shard descriptors and placements
  - replication and read settings
  - cluster epoch/config versioning
- Dynamic membership (add/remove nodes) through the meta group.
- Dynamic sharding (split/merge) and rebalancing driven by meta state.

## Design Overview

### Meta Group (control plane)

- Single, fixed Accord group (meta group) replicated across all nodes.
- Stores `ClusterState` as a replicated log:
  - `members`: node_id → {grpc_addr, redis_addr, status}
  - `shards`: range descriptors and replica placements
  - `epoch`: monotonically increasing config version
  - `replication_factor`: default replication

### Data Groups (shards)

- One Accord group per shard/range.
- Shard descriptors live in the meta group; data groups are created/retired as
  descriptors change.

### Routing

- Nodes maintain a cached view of `ClusterState` (applied from meta group).
- Requests route to shard groups based on descriptors.

## Implementation Checklist

### Phase 1: Control Plane (Meta Group)

- [x] Document control-plane design and checklist.
- [x] Add `ClusterState` and `ClusterCommand` types.
- [x] Add meta-group state machine to apply cluster commands.
- [x] Persist `ClusterState` snapshot to disk for observability.
- [x] Update Join RPC to propose membership changes to meta group.
- [x] Add watch/refresh loop to apply membership updates to transport layer.
- [x] Expose cluster admin RPCs (`ClusterState`, add/remove nodes, range ops).

### Phase 2: Dynamic Shards

- [x] Shard descriptors (key ranges, replicas, leaseholder).
  - [x] Static lexicographic key ranges to replace hardcoded shard mapping.
- [x] Range split command + metadata update path.
- [x] Range merge command + metadata update path.
- [x] Range merge orchestration safety:
  - [x] range-scoped fencing on participating ranges for cutover
  - [x] local quiesce checks before cutover
  - [x] explicit replica catch-up barrier checks for merged ranges
  - [x] timeout-safe unfreeze/abort-before-cutover behavior
  - [x] pause/resume/cancel controls for in-flight merges
- [x] Range split policy (key-count heuristic via range manager).
- [x] Range merge policy (automatic).
- [x] Bootstrap from a single range and split dynamically.
- [x] Merge-under-load integration test (`tests/range_merge.rs`).
- [x] Merge crash/restart and merge+rebalance integration tests
  (`tests/range_merge_recovery.rs`, `tests/range_merge_controls.rs`).

### Phase 3: Rebalancing

- [x] Replica add/remove workflow with validation against active membership.
- [x] Lease transfer metadata updates.
- [x] Rebalancer loop (simple even-distribution heuristic).
- [x] Lease transfer and decommissioning workflow.
- [x] Staged replica move workflow:
  - [x] add learner
  - [x] catch-up gate (`last_executed_prefix`)
  - [x] promote to joint-config metadata phase
  - [x] transfer lease
  - [x] finalize cutover and remove outgoing replica
  - [x] timeout safety: abort stalled pre-cutover moves, force-finalize stalled post-lease moves
  - [x] rollback safety: explicit `AbortReplicaMove` restores pre-move placement/roles
- [x] Manual `RangeRebalance` now uses staged workflow (single replica replacement)
  instead of immediate `SetReplicas` cutover.
- [x] Data-plane runtime reconfiguration of per-shard Accord membership:
  - [x] runtime member set follows shard replicas
  - [x] runtime voter set follows `shard_replica_roles` (`Learner` excluded from quorum)
  - [x] commit fanout includes learners for catch-up while quorum only counts voters

### Phase 4: Meta Resilience (Cockroach-style)

- [ ] Meta range split for large cluster metadata.
- [ ] Multiple meta ranges with well-known descriptors.

## Notes / Gaps

- Split includes local key movement (`FjallRangeMigrator`) before descriptor update.
- Merge now performs local key movement (right range -> left shard partition)
  under split lock before descriptor cutover.
- Merge currently requires adjacent ranges to have matching replica set and
  leaseholder, and rejects merges with in-flight replica moves.
- Merge now runs as a persisted, resumable state machine in control-plane metadata:
  - `Preparing -> Catchup -> Copying -> Cutover -> Finalizing -> Complete`
  - supports `pause/resume/cancel` (`holoctl merge --pause|--resume|--cancel`)
  - restarts resume from persisted phase/progress
  - cutover fencing is range-scoped (no cluster-wide freeze required)
  - cutover requires executed-prefix convergence and per-replica row-count convergence
- Rebalancing now has a background loop:
  - drains `Decommissioning` nodes shard-by-shard
  - finalizes node removal once drained
  - balances replica and leaseholder counts across active nodes
- `ClusterRemoveNode` now means "begin decommissioning", not immediate hard removal.
- Replica configuration is validated against active members and target RF.
- Per-shard Accord groups now refresh membership/voters from meta state on every
  cluster epoch change (instead of static-at-startup members).
- In-flight replica moves now persist timing metadata (`started_unix_ms`,
  `last_progress_unix_ms`) in control-plane state. The rebalance manager uses
  these to recover stalled workflows automatically.
- Retired-range GC is now deferred until control-plane convergence checks pass
  on all non-removed replicas (epoch and descriptor convergence), reducing
  premature cleanup risk during recovery.
- Remaining major production gaps:
  - staged replica reconfiguration is now persisted and replayed through per-shard
    Accord log commands (`CMD_MEMBERSHIP_RECONFIG`), but orchestration is still
    controller-driven (not a shard-local autonomous joint-consensus workflow)
  - meta-plane still runs as a single range (no meta split / multi-meta-range)
  - move-controller HA is still single-writer (node 1); metadata is durable, but
    orchestration leader election is not implemented yet

- The node defaults to `--routing-mode range` (lexicographic key ranges).
  Prefix-heavy workloads can concentrate traffic into a single range until
  the keyspace is split. The range manager can split automatically based on:
  - growth: `HOLO_RANGE_SPLIT_MIN_KEYS` (approximate; uses SET ops since last split; 0 disables)
  - sustained load: `HOLO_RANGE_SPLIT_MIN_QPS` (SET QPS; disabled when 0; default 100k)
  Load-based splitting is further tuned by:
  - `HOLO_RANGE_SPLIT_QPS_SUSTAIN` (consecutive intervals above threshold)
  - `HOLO_RANGE_SPLIT_COOLDOWN_MS` (per-shard cooldown)
  You can also use manual pre-splits (Cockroach-style) to avoid initial hotspots.

## Admin Demo

Run a cluster (for example with `scripts/start_cluster.sh`), then exercise the
admin RPCs with:

```bash
./scripts/cluster_admin_demo.sh
```

This script prints the cluster state, splits the first range at key `m`, shows
the updated state, merges the range back, applies a replica/leaseholder update,
and prints the final state.

For staged replica moves, `ClusterState` now exposes:
- `shard_rebalances`: in-flight move metadata (`from_node`, `to_node`, phase)
- `shard_replica_roles`: per-shard role map (`Learner`, `Voter`, `Outgoing`)

`RangeRebalance` accepts only one replica replacement at a time (plus optional
lease change). Multi-node replacement must be issued as sequential moves.

`holoctl` also provides a topology table that shows each node, the ranges it
serves, and local record counts per range:

```bash
cargo run -q -p holo_store --bin holoctl -- --target 127.0.0.1:15051 topology
```

For in-flight merge progress details:

```bash
cargo run -q -p holo_store --bin holoctl -- --target 127.0.0.1:15051 merge-status
```

## Redis Benchmark Tip

`redis-benchmark` generates keys like `key:000000000123`. If you start with a
single full-keyspace range, all traffic will hit that one range until you split.

To pre-split into ~4 ranges before running the benchmark:

```bash
./scripts/presplit_redis_benchmark.sh
```

By default this script assumes padded keys (`key:000000012345`), which matches
`redis-benchmark -r` behavior. If your workload uses plain keys, run:

```bash
REDIS_BENCH_KEY_STYLE=plain ./scripts/presplit_redis_benchmark.sh
```

For stable benchmark numbers (without split churn during the run), disable
auto-splitting while measuring:

```bash
HOLO_RANGE_SPLIT_MIN_KEYS=0 HOLO_RANGE_SPLIT_MIN_QPS=0 ./scripts/start_cluster.sh

# Optional: move timeout safety for staged reconfiguration (default 180000 ms).
HOLO_REBALANCE_MOVE_TIMEOUT_MS=180000 ./scripts/start_cluster.sh
```
