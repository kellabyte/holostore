#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Read routing test.
#
# What it does:
# - Routes GETs to a single node (node3).
# - Routes SETs to the other nodes (node1+node2).
#
# What it verifies (default behavior):
# - By default nodes run in `accord` read mode. In that mode, a GET sent to node3
#   is still a cluster-wide read: node3 acts as the coordinator and consults peers
#   before replying.
# - This test therefore verifies that "reads via a different node than writes" still
#   produce globally consistent results when using accord reads.
#
# Read mode semantics:
# - This script does NOT force a read mode. The node uses whatever read mode it was
#   started with (default is `accord`).
# - To force a mode explicitly, run with `HOLO_READ_MODE=local|quorum|accord`.
#
# Expected outcome:
# - With `accord` (default), the history should be linearizable.
# - With `local`, the same routing turns into true minority-local reads and can be
#   stale; Porcupine may flag non-linearizability.
READ_NODES="${READ_NODES:-127.0.0.1:16381}"
WRITE_NODES="${WRITE_NODES:-127.0.0.1:16379,127.0.0.1:16380}"

export READ_NODES
export WRITE_NODES

exec "$ROOT_DIR/scripts/porcupine.sh" "$@"
