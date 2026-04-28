# Antithesis correctness harness

This directory packages a local Docker Compose harness and Antithesis-ready test commands for HoloStore correctness testing.

The harness reuses:

- `holo-store` for node processes
- `holo-workload` for Redis-protocol histories
- `porcupine-check` for per-key register linearizability

Local state and artifacts live under the repo's `.tmp/antithesis/` directory so cleanup matches the rest of the repo's cluster scripts.

## Local workflow

Build the fast local images:

```bash
make antithesis-build
```

`make antithesis-build` compiles linux/amd64 `holo-store`, `holo-workload`, and `porcupine-check` artifacts inside builder containers and then assembles thin local runtime images from generated contexts under `.tmp/antithesis/build/`. The clean-room packaging Dockerfiles for tenant/registry builds remain:

- `tools/antithesis/Dockerfile.node`
- `tools/antithesis/Dockerfile.client`

Start the local stack:

```bash
make antithesis-up
```

Run the local smoke path:

```bash
make antithesis-smoke
```

Run the full local correctness suite:

```bash
make antithesis-full
```

`make antithesis-full` starts from a clean `.tmp/antithesis` directory, builds the local images, starts the three-node stack, runs all main and recovery test commands, runs the final merged-history checker, stops Compose, and preserves artifacts under `.tmp/antithesis/history`.

At the end of every full-suite run, the script writes a Markdown report and prints its path:

```text
Antithesis report: .tmp/antithesis/history/antithesis-full-report-<timestamp>.md
```

The report lists every test command, whether it passed or failed, the anomaly classes it targets, summary counters from workload/checker artifacts, and stdout/stderr tails for failures.
Paths written into the Markdown report are rendered relative to the repo root so shared reports do not expose local usernames or home directories.

The local full suite runs workload commands with local-failing assertions and per-command key prefixes so each command's immediate Porcupine check is independent. Antithesis can still invoke the same commands directly with their default shared-key settings.

For faster iteration when images are already current:

```bash
ANTITHESIS_SKIP_BUILD=1 make antithesis-full
```

Useful full-suite options:

- `ANTITHESIS_CLEAN_BEFORE=0` reuses existing `.tmp/antithesis` state.
- `ANTITHESIS_CLEAN_AFTER=1` removes build and node state after a passing run while keeping history/report artifacts.
- `ANTITHESIS_KEEP_STACK=1` leaves the Compose stack running after the suite exits.
- `ANTITHESIS_SETUP_TIMEOUT_S=300` changes the setup wait timeout.

Clean everything, including bind-mounted node data and histories:

```bash
make antithesis-clean
```

The helper scripts resolve either `docker compose` or `docker-compose`. The Compose file bind-mounts:

- `../../../.tmp/antithesis/node1-data` -> `/data`
- `../../../.tmp/antithesis/node2-data` -> `/data`
- `../../../.tmp/antithesis/node3-data` -> `/data`
- `../../../.tmp/antithesis/history` -> `/history`

## Test commands

The client image installs the test commands at:

- `/opt/antithesis/test/v1/main/`
- `/opt/antithesis/test/v1/recovery/`

Examples:

```bash
docker compose --env-file tools/antithesis/config/.env.example -f tools/antithesis/config/docker-compose.yaml exec client \
  /opt/antithesis/test/v1/main/singleton_driver_linearizability.py

docker compose --env-file tools/antithesis/config/.env.example -f tools/antithesis/config/docker-compose.yaml exec client \
  /opt/antithesis/test/v1/main/parallel_driver_hot_key.py

docker compose --env-file tools/antithesis/config/.env.example -f tools/antithesis/config/docker-compose.yaml exec client \
  /opt/antithesis/test/v1/main/finally_check_linearizability.py
```

The primary local smoke path runs `main/singleton_driver_linearizability.py`. The full local suite runs all installed main and recovery commands, then ends with the final merged-history checker.

## Correctness tests

Most workload commands use the same safety oracle:

- `holo-workload` issues Redis-compatible `GET` and `SET` operations and records a Porcupine-compatible history.
- `porcupine-check` checks all successful operations for per-key register linearizability.
- errored operations may be allowed in fault-oriented scenarios, but successful operations are still checked.
- checksum mode binds returned values to the requested key and catches malformed or corrupted values.
- out-of-thin-air checking rejects any `GET` value that was never attempted by a `SET` for that key.

The `main` template contains broad safety and recovery-adjacent tests:

- `first_prepare.py` waits for the health-checker sentinel and verifies `/history` is writable. This catches early harness/setup bugs, premature `setup_complete`, and missing shared artifact storage.
- `singleton_driver_linearizability.py` runs the shortest useful mixed `GET`/`SET` workload and checks it immediately. This catches basic per-key linearizability violations, stale reads, lost writes, wrong-value reads, out-of-thin-air reads, and checksum/key-binding corruption.
- `parallel_driver_baseline_registers.py` runs moderate multi-key concurrency over a shared keyspace. This catches ordinary register safety regressions, cross-client ordering bugs, accidental keyspace collisions, and read freshness problems across nodes.
- `parallel_driver_hot_key.py` drives one key with high write contention. This targets Accord dependency ordering anomalies: missed conflicts, reordered committed writes, stale reads after completed writes, duplicate application, and contention-specific visibility bugs.
- `parallel_driver_range_churn.py` drives many keys while the cluster can be configured for aggressive range splitting. This targets range-manager anomalies: missing range ownership, duplicate active owners, wrong-range reads, split/migration lost writes, stale reads during descriptor churn, and key/value mixups across ranges.
- `parallel_driver_disconnects.py` injects client reconnects while preserving the linearizability check for successful operations. This catches Redis protocol/session bugs, ambiguous retry handling, server-side panics under valid requests, and incorrect treatment of disconnected or timed-out operations.
- `anytime_health.py` probes Redis reachability during active faults without requiring every node to be reachable. This catches harness crashes and gives coverage that at least one node is reachable on some timelines.
- `anytime_metrics_sanity.py` fetches `HOLOMETRICS` from reachable nodes and checks parseability, finite non-negative values, and local counter monotonicity. This catches malformed metrics, negative counters, counter regressions within one probe, and metrics-path crashes.
- `eventually_recovery.py` waits for the cluster to become reachable after faults stop, then runs a small fresh linearizability workload. This catches failure-to-recover, stuck membership/routing, post-fault stale reads, and recovery paths that can serve traffic but violate safety.
- `finally_check_linearizability.py` merges all completed `history-*.json` files, offsets client IDs, aligns histories by absolute start time, and runs Porcupine on the merged history. This catches anomalies that only appear across concurrent drivers, including shared-key ordering bugs and history-collision mistakes.

The `recovery` template focuses on acknowledged writes, WAL replay, and post-crash visibility:

- `recovery/singleton_driver_crash_recovery.py` runs a set-heavy workload and records acknowledged successful writes. This creates evidence for crash-safety checks and targets durability regressions around acknowledged commits.
- `recovery/eventually_recovery_check.py` waits for reachable nodes, runs a fresh mixed workload, and spot-checks acknowledged values when available. This catches acknowledged-write loss, replay ordering bugs, duplicate/idempotency mistakes, and recovered nodes serving values that were not acknowledged for that key.

Safety assertions should fail on correctness anomalies. Coverage assertions such as `sometimes()` are used for liveness or path coverage, for example observing successful writes, successful reads, recovery, metrics, split/merge metrics, or transient errors in fault scenarios.

## Artifacts

The client and health-checker write artifacts to `/history`, which maps to `.tmp/antithesis/history` locally.

Expected artifacts include:

- `history-*.json`
- `history-*.summary.json`
- `*.checker-summary.json`
- `checker-*.stdout`
- `checker-*.stderr`
- `workload-*.stdout`
- `workload-*.stderr`
- `failure-*.html`
- `merged-history.json`
- `setup-complete.json`
- `antithesis-full-report-*.md`
- `antithesis-full-report-*.md.status`
- `suite-report-*/`

`history-*.json` files now include an absolute `start_unix_us` timestamp so final merge checks can align operations across multiple drivers before running Porcupine.

## Range churn

The default `.env.example` keeps auto-splitting mostly disabled for a conservative smoke path. To exercise split churn locally, override settings before `make antithesis-up`, for example:

```bash
HOLO_INITIAL_RANGES=1 \
HOLO_MAX_SHARDS=8 \
HOLO_RANGE_SPLIT_MIN_KEYS=2 \
HOLO_RANGE_SPLIT_MIN_QPS=0 \
make antithesis-up
```

## Antithesis packaging

Build clean-room images that are suitable for Antithesis upload:

```bash
ANTITHESIS_REGISTRY=<ANTITHESIS_REGISTRY> IMAGE_TAG=<IMAGE_TAG> make antithesis-package-build
```

`make antithesis-package-build` stages minimal Docker contexts under `.tmp/antithesis/build/package/` before invoking `docker build`. The node and client contexts include only the Rust workspace sources they need plus the relevant Antithesis files; the health-checker and config images get their own small contexts. This keeps the Antithesis packaging path from depending on repo-root Docker context rules.

Push those images with your normal registry workflow, then reference them from the Antithesis deployment using placeholders only:

- `<TENANT_NAME>`
- `<ANTITHESIS_REGISTRY>`
- `<IMAGE_TAG>`
- `<WEBHOOK_URL>`

Do not commit tenant names, credentials, registry auth, or webhook secrets into this repository.

## Passing and failing runs

A passing run means:

- the workload completed
- the checker validated every successful operation history
- no out-of-thin-air value appeared
- checksum-mode values, when enabled, matched both their embedded key and checksum

When a linearizability check fails:

1. Inspect `failure-*.html`.
2. Open the matching `history-*.json` or `merged-history.json`.
3. Check the corresponding `checker-*.stderr` and `workload-*.stderr`.
4. Re-run the failing scenario with the same `SEED` when available.
