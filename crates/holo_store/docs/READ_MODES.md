# Read Modes and Guarantees

HoloStore supports multiple read modes that trade consistency for latency. Writes always go
through Accord; only reads vary by mode.

Select a mode:
- CLI: `--read-mode accord|quorum|local`
- Env: `HOLO_READ_MODE=accord|quorum|local`

## Modes

### Accord (default)
**What it does**
- Reads execute through Accord as a read transaction (`BATCH_GET`) rather than a local KV read.
- Before proposing the read, the node performs a **read barrier**:
  - Query peers for `last_committed` per key.
  - Compute the max committed txn per key.
  - Wait until those txn IDs are executed locally.
  - Use those txn IDs as dependencies for the read proposal.

**Guarantee**
- Linearizable reads with respect to committed writes in the same group, assuming the
  Accord implementation is correct and the read barrier completes successfully.

**Tradeoffs**
- Highest latency due to quorum RPCs + consensus proposal.

**Related settings**
- `HOLO_READ_BARRIER_TIMEOUT_MS` (default 2000)
- `HOLO_READ_BARRIER_FALLBACK_QUORUM` (default false)
- `HOLO_READ_BARRIER_ALL_PEERS` (default true)

If the read barrier times out and `HOLO_READ_BARRIER_FALLBACK_QUORUM=true`, the request
falls back to quorum reads (see below).

### Quorum
**What it does**
- Reads bypass Accord and query a quorum of replicas.
- Each replica returns its latest executed MVCC version; the client selects the highest.

**Guarantee**
- Not linearizable. Reads can be stale relative to the latest committed write.
- Not monotonic across calls (different quorums can return older values).

**Tradeoffs**
- Lower latency than Accord in exchange for staleness risk.

### Local
**What it does**
- Reads only the local node’s executed MVCC state; no network calls.

**Guarantee**
- Not linearizable and can be arbitrarily stale.

**Tradeoffs**
- Lowest latency and lowest overhead.

## Practical guidance

- Use `accord` for correctness tests (Porcupine, invariants, etc.).
- Use `quorum` for latency-sensitive reads where occasional staleness is acceptable.
- Use `local` for debugging or when you explicitly accept staleness.

## Current limitations

- The read barrier is implemented via `last_committed` + `wait_executed`. If peer responses
  are slow or missing, the read can time out.
- `quorum` and `local` modes are intentionally weaker and should not be used when
  linearizability is required.
