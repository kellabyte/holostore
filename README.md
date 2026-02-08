# HoloStore

```
 _     _  _____          _____  _______ _______  _____   ______ _______
 |_____| |     | |      |     | |______    |    |     | |_____/ |______
 |     | |_____| |_____ |_____| ______|    |    |_____| |    \_ |______
                                                                            
```

**HoloStore** is a distributed, strongly-consistent key/value store built on the
**Accord** consensus algorithm. It exposes a Redis-compatible interface for
simple benchmarking and integrates a durable commit log (WAL) + storage engine
to make writes correct and crash-safe.

**What is Accord?**
- Accord is a leaderless consensus protocol.
- It uses **pre-accept → accept → commit** phases with dependency tracking.
- A **fast path** is possible when dependencies converge without conflicts.
- Transactions are ordered per key via dependency sets, so unrelated writes can
  proceed in parallel without a single global leader.
- [Accord consensus protocol paper (CEP-15 draft whitepaper)](https://github.com/eatonphil/accord-protocol).

In this repository we implement:
- A per-partition Accord group with batched consensus RPCs.
- A commit log (WAL) that persists committed entries and replays on restart.
- An execution loop that applies committed commands to a state machine.
- A Redis protocol surface for easy benchmarking.

**Documentation**
- [Design overview](docs/DESIGN.md)
- [Storage engine](docs/STORAGE.md)
- [Read modes](docs/READ_MODES.md)
- [Linearizability testing](docs/LINEARIZABILITY.md)
- [Cluster membership](docs/CLUSTER_MEMBERSHIP.md)
- [Testing TODOs](docs/TESTING_TODO.md)

**Build**
```bash
make build-release
```

**Run a local 3-node cluster**
```bash
./scripts/cleanup_cluster.sh
./scripts/start_cluster.sh
```

**Benchmark with redis-benchmark**
```bash
redis-benchmark -h localhost -p 16379 -c 50 -n 1000000 -r 100000 -P 100 -t get

====== GET ======
  1000000 requests completed in 1.76 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

Summary:
  throughput summary: 569,152.00 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        8.721     4.504     7.271    13.479    39.615    52.735
```

**Porcupine linearizability check**
```bash
make check-linearizability
```

You can customize the workload via env vars:
```bash
NODES="127.0.0.1:16379,127.0.0.1:16380,127.0.0.1:16381" \
CLIENTS=3 KEYS=5 SET_PCT=50 DURATION=10s OP_TIMEOUT=10s FAIL_FAST=true \
./scripts/check_linearizability.sh
```

**Notes**
- By default, the cluster starts on non-standard Redis ports (16379–16381).
- The start script prefers `target/release/holo-store` if it exists.
- You can override defaults via environment variables (batch sizes, shards, WAL
  persistence, etc.).
