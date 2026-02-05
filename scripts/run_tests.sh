#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOT_DIR}/.tmp/tests"
mkdir -p "${OUT_DIR}"

TS="$(date +"%Y%m%d_%H%M%S")"
OUT_FILE="${OUT_DIR}/integration_tests_${TS}.out"

strip_colors() {
  sed -E 's/\x1b\[[0-9;]*m//g'
}

{
  echo "Running holo_store integration tests..."
  CARGO_TERM_COLOR=always cargo test --color=always -p holo_store --tests
} 2>&1 | tee >(strip_colors > "${OUT_FILE}")

echo "Saved output to ${OUT_FILE}"
