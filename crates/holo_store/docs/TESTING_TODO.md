# Testing TODO

This document lists the test work we should add for `holo_accord` and the storage/WAL stack.
Prioritize in this order: correctness → durability → integration → performance regression.

## 1) Accord Core (unit + property tests)

1. **Dependency tracking + ready-queue correctness**
   - Commit A, then commit B that depends on A.
   - Assert B does not execute before A.
   - Assert `committed_ready` only contains B after A executes.

2. **Batch execution ordering**
   - Commit A,B with satisfied deps.
   - Assert both execute in the same batch (or a single executor pass).

3. **Cycle resolution (SCC fallback)**
   - Two or more transactions with cyclic deps.
   - Assert executor makes progress and executes all.

4. **Recovery path correctness**
   - Simulate missing command bytes on a replica.
   - Force `fetch_command` and ensure recovered command executes.

5. **Executed-prefix + GC safety**
   - Gossip executed prefixes across replicas.
   - Assert GC only removes entries safe for all peers.

6. **Fast-path vs slow-path accounting**
   - Workload with no conflicts → fast-path dominant.
   - Workload with conflicts → slow-path increments.

## 2) WAL / Durability Tests

1. **Append + replay**
   - Append commits; restart; replay; ensure state matches.

2. **Batch append ordering**
   - Append a batch of commits and verify replay ordering.

3. **Persist mode matrix**
   - `sync_data`, `sync_all`, `buffer`.
   - Validate that `buffer` can lose in-flight data, others should not.

4. **Compaction correctness**
   - Mark executed, compact, reload.
   - Verify only safe entries are removed.

## 3) Integration Tests (multi-node)

1. **Porcupine linearizability (short)**
   - Run `holo-workload` + Porcupine for a small duration.

2. **Partitioning correctness**
   - Keys mapping to different shards should not interfere.

3. **Crash + restart recovery**
   - Kill one node mid-test; restart; ensure it replays WAL and catches up.
   - Add explicit multi-node crash/restart integration test.

4. **Leaderless availability**
   - Ensure proposals can be made from any node after startup.

5. **Snapshot/compaction boundary**
   - Add recovery test once snapshotting/compaction is implemented.

## 4) Performance Regression Tests

1. **Throughput baseline**
   - Short redis-benchmark (1–5s) to catch 10x regressions.

2. **Latency sanity**
   - Track p50/p95 in a light load test to catch tail regressions.

## Suggested Harnesses

- **Unit tests**: in-process transport + dummy state machine under
  `crates/holo_accord/tests/`.
- **Integration tests**: scripts or a `tests/` harness that spawns
  `holo-store` nodes and runs workloads.
- **Perf tests**: simple CI job or local script that records baseline metrics.
