# Linearizability testing

This repo uses the Porcupine linearizability checker to validate client-visible behavior. The harness runs a short workload against a local cluster, records a history of operations, and then checks that history for linearizability.

## Tooling

- Checker: [Porcupine](https://github.com/anishathalye/porcupine) (Go)
- Harness: `scripts/check_linearizability.sh` (wired to `make check-linearizability`)
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

By default, if any operation errored, the checker fails early; you can allow errors with `--allow-errors` when running the checker directly.

## Running

- `make check-linearizability`
- `./scripts/check_linearizability.sh`

You can control the workload with environment variables (examples: `CLIENTS`, `KEYS`, `SET_PCT`, `DURATION`, `OP_TIMEOUT`, `FAIL_FAST`, `OUT`).
