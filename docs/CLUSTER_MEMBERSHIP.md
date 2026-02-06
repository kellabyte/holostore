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
- [x] Range split policy (key-count heuristic via range manager).
- [ ] Range merge policy (automatic).
- [x] Bootstrap from a single range and split dynamically.

### Phase 3: Rebalancing

- [x] Replica add/remove workflow (metadata-only).
- [x] Lease transfer metadata updates.
- [ ] Rebalancer loop (simple even-distribution heuristic).
- [ ] Lease transfer and decommissioning workflow.

### Phase 4: Meta Resilience (Cockroach-style)

- [ ] Meta range split for large cluster metadata.
- [ ] Multiple meta ranges with well-known descriptors.

## Notes / Gaps

- Range operations are metadata-only. They do not yet trigger data movement or
  dynamic Accord group creation.
- Rebalancing is explicit via admin RPCs; there is no background rebalancer yet.
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
```
