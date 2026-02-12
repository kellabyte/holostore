# Linearizability testing

This repo uses the Porcupine linearizability checker to validate client-visible behavior. The harness runs a short workload against a local cluster, records a history of operations, and then checks that history for linearizability.

## Tooling

- Checker: [Porcupine](https://github.com/anishathalye/porcupine) (Go)
- Harness: `scripts/porcupine.sh` (single scenario) and `scripts/check_linearizability.sh` (suite, wired to `make check-linearizability`)
- History format: JSON written by `holo-workload`

## What we test

The workload issues operations against the key/value API and writes a history file (default `/.tmp/porcupine/history.json` in the repo root). The checker verifies linearizability per key using a simple register model:

- `set(value) -> ok` writes the register.
- `get() -> nil` is allowed only if no successful `set` has happened yet.
- `get() -> value` must return the most recent linearized `set` value.

Additionally, there is an out-of-thin-air sanity check: any observed `get` value must have been attempted by some `set` for that key.

## Operations and measurements

The recorded operations include:

- `set` calls (with the value written)
- `get` calls (returning `nil` or a concrete value)
- timestamps for call/return times (used by Porcupine)
- results (`ok`, `value`, `nil`, `err`)

By default, if any operation errored, the checker fails early; you can allow errors with `--allow-errors` (or `ALLOW_ERRORS=1` when running `scripts/porcupine.sh`).

## Running

- `make check-linearizability`
- `make check-linearizability-stress` (non-gating stress loop)
- `./scripts/porcupine.sh` (single scenario)
- `./scripts/check_linearizability.sh` (suite)
- `./scripts/check_linearizability_stress.sh` (non-gating autosplit + autosplit/automerge stress suite)
- `./scripts/check_read_minority.sh` (manual: read-only minority routing)

## Suite scenarios

The suite (`scripts/check_linearizability.sh`) runs multiple scenarios. Each scenario issues the same
GET/SET workload, but changes routing or failure injection to stress different correctness paths.

### Baseline

- **Operations:** mixed GET/SET against the cluster (`SET` returns `OK`, `GET` returns a value or `nil`).
- **Read mode:** uses the server’s configured read mode (default `accord`).
- **Failures injected:** none.
- **Expected result:** linearizability should hold with no errors or lost writes.

### Range autosplit

- **Operations:** mixed GET/SET as usual.
- **Read mode:** uses the server’s configured read mode (default `accord`).
- **Failures injected:** none, but the node's background range manager will split the keyspace
  and migrate keys between shards during the run.
- **Expected result:** linearizability should hold across splits (no lost or duplicated committed writes).

### Range autosplit stress (non-gating)

- **Operations:** same mixed GET/SET workload as autosplit.
- **Read mode:** uses the server’s configured read mode (default `accord`).
- **Failures injected:** none.
- **Stress shape:** repeated runs with higher client concurrency (`CLIENTS=3`) while live split/migration is active.
- **Expected result:** same correctness target as autosplit, but this is intentionally **non-gating** by default.
  Use it for race detection and soak testing (`STRICT=1` makes failures exit non-zero).

### Autosplit + automerge long run (non-gating)

- **Operations:** mixed GET/SET under longer run time (`MERGE_STRESS_DURATION`, default `60s`).
- **Read mode:** uses the server’s configured read mode (default `accord`).
- **Failures injected:** none.
- **Stress shape:** runs with autosplit and automerge both enabled so descriptors churn in both directions.
- **Expected result:** successful operations remain linearizable while ranges split/merge over time.
  This scenario is non-gating by default and intended for soak/race detection.

### Slow replica

- **Operations:** mixed GET/SET against the cluster.
- **Read mode:** uses the server’s configured read mode (default `accord`).
- **Failures injected:** artificial RPC handler delay on one node (e.g. node3).
- **Expected result:** linearizability should hold for successful operations despite a slow replica.

### Hot-key contention

- **Operations:** single key (`KEYS=1`) with a high write rate (`SET_PCT=90`).
- **Read mode:** uses the server’s configured read mode (default `accord`).
- **Failures injected:** none.
- **Expected result:** linearizability should hold under heavy contention and ordering pressure.

### Mixed read/write nodes

- **Operations:** GETs are routed to a single read node; SETs are sent to all nodes.
- **Read mode:** uses the server’s configured read mode on the chosen read node (default `accord`).
- **Failures injected:** none.
- **Expected result:** linearizability should still hold even when reads are constrained to a subset of nodes.

### Client disconnects

- **Operations:** mixed GET/SET as usual.
- **Read mode:** uses the server’s configured read mode (default `accord`).
- **Failures injected:** client-side disconnect/reconnect before a percentage of operations
  (`FAULT_DISCONNECT_PCT`, defaults to `5` in the suite).
- **Expected result:** the workload may record transient errors, but successful operations must still
  be linearizable and values must never appear out of thin air.

### Crash during write

- **Operations:** SET-heavy workload (`SET_PCT=100`).
- **Read mode:** uses the server’s configured read mode (default `accord`).
- **Failures injected:** aggressive kill/restart during writes (`FAIL_KILL_INTERVAL=1s`).
- **Expected result:** no lost committed writes after restarts; linearizability should hold for
  successful operations despite transient errors (suite enables `ALLOW_ERRORS=1`).

### Server kill/restart

- **Operations:** mixed GET/SET as usual.
- **Read mode:** uses the server’s configured read mode (default `accord`).
- **Failures injected:** random node process kill/restart during the workload
  (`FAIL_INJECT=1`, `FAIL_KILL_INTERVAL=3s`, `FAIL_KILL_SIGNAL=KILL`, `FAIL_KILL_RESTART=1` in the suite).
- **Expected result:** the system should recover after crashes and still satisfy linearizability for
  successful operations. Failures are acceptable while nodes are down, but recovered nodes must replay
  committed writes without data loss (suite enables `ALLOW_ERRORS=1`).

You can control the workload with environment variables (examples: `CLIENTS`, `KEYS`, `SET_PCT`, `DURATION`, `OP_TIMEOUT`, `FAIL_FAST`, `OUT`, `READ_NODES`, `WRITE_NODES`, `FAULT_DISCONNECT_PCT`, `FAIL_INJECT`, `FAIL_KILL_INTERVAL`, `FAIL_KILL_SIGNAL`, `FAIL_KILL_RESTART`, `ALLOW_ERRORS`, `FAILURE_GRACE`, `HOLO_NODE1_RPC_DELAY_MS`, `HOLO_NODE2_RPC_DELAY_MS`, `HOLO_NODE3_RPC_DELAY_MS`).

## Manual scenarios

### Read-only minority routing

`scripts/check_read_minority.sh` routes GETs to a single node and writes to the other nodes. This
can surface stale reads and is **not expected to be linearizable**; it is intended as a characterization
test rather than a suite gate.
