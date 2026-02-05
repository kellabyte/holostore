#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOT_DIR}/.tmp/tests"
mkdir -p "${OUT_DIR}"

TS="$(date +"%Y%m%d_%H%M%S")"
OUT_FILE="${OUT_DIR}/wal_and_read_tests_${TS}.out"

strip_colors() {
  sed -E 's/\x1b\[[0-9;]*m//g'
}

{
  echo "Running WAL crash test..."
  CARGO_TERM_COLOR=always cargo test --color=always -p holo_store --test wal_crash
  echo
  echo "Running accord read barrier test..."
  CARGO_TERM_COLOR=always cargo test --color=always -p holo_store --test accord_read_barrier
} 2>&1 | tee >(strip_colors > "${OUT_FILE}")

echo "Saved output to ${OUT_FILE}"
