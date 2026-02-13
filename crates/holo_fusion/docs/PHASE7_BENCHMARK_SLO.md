# Phase 7 Benchmark and SLO Package

This package defines the Phase 7 performance scenarios and regression gate.

## Scenarios

The benchmark harness lives in:
- `crates/holo_fusion/tests/bench_slo.rs`

It runs three workloads against a single-node embedded HoloFusion runtime:

1. Read-heavy
- Point reads by primary key.
- Measures end-to-end SQL latency distribution.

2. Mixed
- Blend of point reads, point updates, and aggregate queries.
- Approximates HTAP-ish user traffic.

3. Write-heavy
- Primary-key updates at sustained rate.
- Focuses on mutation path latency.

## Reported percentiles

For each workload:
- `p50`
- `p95`
- `p99`

Output is printed to stdout by the benchmark test.

## Regression gate

The benchmark supports optional SLO enforcement:

- Set `HOLO_FUSION_ENFORCE_SLO=1` to enable failure-on-regression.
- Override targets via:
  - `HOLO_FUSION_SLO_READ_P95_MS`
  - `HOLO_FUSION_SLO_MIXED_P95_MS`
  - `HOLO_FUSION_SLO_WRITE_P95_MS`

Default p95 targets:
- read-heavy: `50ms`
- mixed: `80ms`
- write-heavy: `100ms`

## How to run

Run benchmark only:

```bash
cargo test -p holo_fusion phase7_benchmark_and_slo_package -- --ignored --nocapture
```

Run with SLO gate enabled:

```bash
HOLO_FUSION_ENFORCE_SLO=1 \
HOLO_FUSION_SLO_READ_P95_MS=40 \
HOLO_FUSION_SLO_MIXED_P95_MS=70 \
HOLO_FUSION_SLO_WRITE_P95_MS=90 \
cargo test -p holo_fusion phase7_benchmark_and_slo_package -- --ignored --nocapture
```
