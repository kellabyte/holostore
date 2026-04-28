#!/usr/bin/env bash
set -euo pipefail

# Build nothing implicitly: assume the caller already built images, bring the
# stack up detached, wait for setup completion, then run the singleton driver.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/antithesis_common.sh"

prepare_antithesis_tmp
compose_in_antithesis up -d

cleanup() {
  compose_in_antithesis down -v >/dev/null 2>&1 || true
  rm -rf "$ANTITHESIS_TMP_DIR"
}
trap cleanup EXIT

wait_for_antithesis_setup 120

compose_in_antithesis exec -T -e ANTITHESIS_LOCAL_ASSERTS=1 client \
  /opt/antithesis/test/v1/main/singleton_driver_linearizability.py
