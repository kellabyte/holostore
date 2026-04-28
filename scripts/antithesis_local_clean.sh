#!/usr/bin/env bash
set -euo pipefail

# Tear down the local Antithesis Compose stack and remove bind-mounted state.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/antithesis_common.sh"

compose_in_antithesis down -v "$@" || true
rm -rf "$ANTITHESIS_TMP_DIR"

