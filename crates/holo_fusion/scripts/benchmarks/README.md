# Benchmarks

This directory contains quick SQL benchmark tooling for:

- HoloFusion (Postgres wire protocol)
- CockroachDB (3-node Docker cluster)

## Files

- `docker-compose.cockroach.yml`: 3-node CockroachDB cluster
- `start_cockroach_cluster.sh`: start and initialize cluster
- `stop_cockroach_cluster.sh`: stop cluster and remove containers/volumes
- `schema.sql`: benchmark table schema (`orders`)
- `insert.sql`: pgbench insert/upsert workload
- `select.sql`: pgbench point-select workload
- `update.sql`: pgbench point-update workload
- `run_pgbench_suite.sh`: runs insert/select/update workloads sequentially
- `run_phase9_sales_facts_canary.sh`: Phase 9 ingest canary with split-churn + SLO gating + metrics/topology snapshots

CockroachDB data is stored in:

- `.cluster/holo_fusion/cockroach/node1`
- `.cluster/holo_fusion/cockroach/node2`
- `.cluster/holo_fusion/cockroach/node3`

via Docker bind mounts (not Docker named volumes).

## Quick Start

Start CockroachDB:

```bash
./crates/holo_fusion/scripts/benchmarks/start_cockroach_cluster.sh
```

Run suite against CockroachDB:

```bash
TARGET=cockroach ./crates/holo_fusion/scripts/benchmarks/run_pgbench_suite.sh
```

Run suite against HoloFusion:

```bash
TARGET=holofusion ./crates/holo_fusion/scripts/benchmarks/run_pgbench_suite.sh
```

Run Phase 9 canary (3-node HoloFusion dev cluster + `sales_facts` 20k/50k ingest gate):

```bash
./crates/holo_fusion/scripts/benchmarks/run_phase9_sales_facts_canary.sh
```

Stop CockroachDB cluster:

```bash
./crates/holo_fusion/scripts/benchmarks/stop_cockroach_cluster.sh
```

## Useful knobs

```bash
CONCURRENCY=50 JOBS=8 DURATION_SEC=60 SEED_ROWS=1000000 TARGET=cockroach ./crates/holo_fusion/scripts/benchmarks/run_pgbench_suite.sh
```

To use a custom DSN:

```bash
PG_URL='postgresql://user@host:port/db?sslmode=disable' ./crates/holo_fusion/scripts/benchmarks/run_pgbench_suite.sh
```

Results are written under `crates/holo_fusion/scripts/benchmarks/results/`.
