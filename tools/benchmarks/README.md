# HoloStore / etcd Benchmarks

This directory contains Docker-based benchmarks for comparing HoloStore and
etcd with the same runner and the same three-node cluster shape.

The benchmark runner emits a constant offered load, like wrk2. It accounts for
coordinated omission by measuring corrected latency from each operation's
scheduled start time to completion time. The raw service time from actual
worker start to completion is also recorded in `metrics.csv`.

## Quick Start

Run the default mixed workload against both stores:

```bash
./tools/benchmarks/run_benchmark.sh \
  --target both \
  --duration 60s \
  --rate 5000 \
  --workers 256 \
  --connections 128
```

Results are written under `.tmp/benchmarks/results/<timestamp>-<scenario>/`.
For `--target both`, the top-level `report.md` compares both stores and embeds
PNG graphs for throughput and corrected latency over time.

Before starting a run, the wrapper removes stale Docker Compose benchmark
projects named `holobench_holostore_*` or `holobench_etcd_*`, including their
containers, networks, and volumes. This prevents kept or interrupted clusters
from consuming memory, ports, disk, or Docker DNS state during a later run. Use
`--no-clean-stale` only when intentionally preserving an older benchmark
cluster for debugging.

On Apple Silicon, use a Docker server that reports `arm64` and force the
benchmark platform to `linux/arm64` if you want to avoid stale amd64 images:

```bash
docker version --format '{{.Server.Arch}}'  # should print arm64

./tools/benchmarks/run_benchmark.sh \
  --platform linux/arm64 \
  --target both \
  --duration 60s \
  --rate 5000 \
  --workers 256 \
  --connections 128
```

## Image Builds

The wrapper builds benchmark images from staged contexts under
`.tmp/benchmarks/build`. The Go load generator is compiled in a builder
container. HoloStore is compiled on the host for the Linux target used by
Docker, then the runtime image is assembled from only the binary and benchmark
support files it needs. Docker does not use the repo root as a build context.

On macOS, the host Linux build needs a C cross linker for native dependencies
such as `lz4-sys`. The benchmark scripts use an installed musl cross compiler
when present, or `zig cc` wrappers when Zig is installed:

```bash
brew install zig
```

The default benchmark platform is `native`, which resolves to the Docker server
architecture and is then passed to Docker builds and Compose. An arm64 Docker
server resolves to `linux/arm64`; an amd64 Docker server resolves to
`linux/amd64`. Override it with `--platform` or `BENCHMARK_PLATFORM`. Use
`--platform linux/arm64` on an M-series Mac to force arm64 runner, etcd, and
HoloStore containers.

The default HoloStore target follows the benchmark platform:
`x86_64-unknown-linux-musl` for `linux/amd64`, or
`aarch64-unknown-linux-musl` for `linux/arm64`. Override it with
`--holostore-target` or `BENCHMARK_HOLOSTORE_TARGET`. If the host cross
toolchain is unavailable, use the slower fallback with
`--holostore-build-mode docker`.

To build the images without running a benchmark:

```bash
./tools/benchmarks/build_images.sh --platform linux/arm64 --target both
```

## Workload Shapes

The main knobs are:

- `--write-pct`: percentage of writes. Use `0` for read-only and `100` for write-only.
- `--contention`: `uniform`, `single-key`, `hotspot`, or `zipf`.
- `--keys`: total keyspace size.
- `--hot-keys` and `--hot-pct`: hot-set size and traffic share for `hotspot`.
- `--value-bytes`: write payload size.
- `--rate`: constant offered load in operations per second.
- `--workers`: required number of concurrent scheduled operation workers. The
  runner uses one outstanding operation per worker, so too few workers can make
  corrected latency mostly measure client queueing.
- `--connections`: target client/connection pool size. It defaults to
  `--workers`, but setting it lower keeps the client footprint bounded. This is
  especially important for etcd because each pooled client is a `clientv3`
  client rather than a raw TCP socket.
- `--worker-headroom`: multiplier used for worker recommendation warnings.
- `--queue-cap`: scheduled request queue capacity. The default `0` uses a
  bounded `workers * 2` queue. The runner keeps absolute scheduled timestamps
  when this queue backs up, so corrected latency includes scheduler lag instead
  of silently lowering the offered rate.
- `--preload-workers`, `--preload-timeout`, and `--preload-retries`: controls
  for loading the keyspace before read or mixed workloads. Preload uses a
  conservative worker default and a separate timeout from the timed run. It is
  skipped automatically for `--write-pct 100` because set-only tests do not
  need existing values.

Examples:

```bash
# Read-heavy uniform workload.
./tools/benchmarks/run_benchmark.sh \
  --target both \
  --scenario read-heavy-uniform \
  --write-pct 5 \
  --contention uniform \
  --keys 100000 \
  --duration 2m \
  --rate 10000 \
  --workers 512 \
  --connections 128

# Write-only single-key contention.
./tools/benchmarks/run_benchmark.sh \
  --target both \
  --scenario write-single-key \
  --write-pct 100 \
  --contention single-key \
  --keys 1 \
  --duration 60s \
  --rate 2000 \
  --workers 128 \
  --connections 64

# Hotspot contention.
./tools/benchmarks/run_benchmark.sh \
  --target both \
  --scenario mixed-hotspot \
  --write-pct 50 \
  --contention hotspot \
  --keys 100000 \
  --hot-keys 10 \
  --hot-pct 95 \
  --duration 60s \
  --rate 5000 \
  --workers 256 \
  --connections 128
```

## Direct Runner Use

The Go runner can be used directly inside the benchmark runner image. The
wrapper script auto-detects `docker compose` vs `docker-compose`; if you run
Compose manually, use whichever binary is installed on your machine.

```bash
./tools/benchmarks/build_images.sh --target etcd

docker compose \
  -f tools/benchmarks/docker-compose.etcd.yml \
  -f tools/benchmarks/docker-compose.runner.yml \
  up -d --no-build --wait etcd1 etcd2 etcd3

BENCH_RESULTS_DIR="$(pwd)/.tmp/benchmarks/results/manual-etcd" \
docker compose \
  -f tools/benchmarks/docker-compose.etcd.yml \
  -f tools/benchmarks/docker-compose.runner.yml \
  run --rm bench-runner \
  benchtool run \
    --target etcd \
    --endpoints etcd1:2379,etcd2:2379,etcd3:2379 \
    --scenario manual-etcd \
    --rate 1000 \
    --duration 30s \
    --workers 64 \
    --connections 32 \
    --preload-workers 16 \
    --out-dir /results
```

## Outputs

Each run directory contains:

- `config.json`: benchmark configuration.
- `metrics.csv`: one row per completion second with throughput, error count,
  corrected latency percentiles (`p50`, `p75`, `p90`, `p95`, `p99`, `p99.9`,
  `max`), service-latency percentiles, and start-lag percentiles. Start lag is
  the time from a request's scheduled start to the moment a worker actually
  begins it; high start lag means the load generator is client-queue limited.
  The runner rewrites this file during the run so interrupted benchmarks keep
  the latest completed one-second rows.
- `errors.csv`: exact per-second error category counts with a representative
  sample string for each category. This stays compact during failure storms
  because the runner aggregates errors instead of logging one line per failed
  operation.
- `summary.json`: aggregate throughput and latency summary.
  Latency summaries are backed by HDR histograms rather than raw retained
  samples. The `runner_runtime` section records queue capacities, peak queue
  depths, scheduler backpressure, result queue backpressure, Go heap size, and
  GC count so long runs can be diagnosed without guessing. The
  `error_categories` and `top_error_messages` sections summarize operation
  failures by type and sampled message.
- `failure.json`: written when a target phase fails before the normal artifacts
  are complete. It records the phase, exit code, timestamp, and a short reason.
- `events-node*.csv`: optional HoloStore timeline events emitted by benchmark
  nodes, such as range split start/end markers. Reports convert these wall-clock
  timestamps to benchmark-relative seconds and overlay them on timeline graphs.
- `timing.json`: wrapper wall-clock timing with total, build, and benchmark
  run durations.
- `report.md`: Markdown report with embedded PNG graphs.
  The report header includes total time, build time, benchmark run time, and an
  environment table with the Docker platform and benchmark arguments used for
  the run. It also includes an overload status table that calls out targets
  whose completed throughput, drain time, or error count make corrected-latency
  results unsafe to compare as steady-state service latency. If a target fails,
  the wrapper still writes the target report and, for `--target both`, the
  top-level comparison report with a failed-target section and any missing
  artifacts called out explicitly.
- `graphs/*.png`: generated charts.

No HoloStore or etcd process is started on the host. The HoloStore cluster,
etcd cluster, and load generator all run as Docker containers on the Compose
network.
