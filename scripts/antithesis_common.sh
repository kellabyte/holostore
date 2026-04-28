#!/usr/bin/env bash
set -euo pipefail

# Shared helpers for the local Antithesis Docker Compose workflow.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ANTITHESIS_TMP_DIR="${ANTITHESIS_TMP_DIR:-"$ROOT_DIR/.tmp/antithesis"}"
ANTITHESIS_BUILD_DIR="${ANTITHESIS_BUILD_DIR:-"$ANTITHESIS_TMP_DIR/build"}"

resolve_compose() {
  if docker compose version >/dev/null 2>&1; then
    COMPOSE=(docker compose)
    return 0
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    COMPOSE=(docker-compose)
    return 0
  fi
  echo "error: neither 'docker compose' nor 'docker-compose' is available" >&2
  exit 1
}

prepare_antithesis_tmp() {
  mkdir -p \
    "$ANTITHESIS_BUILD_DIR" \
    "$ANTITHESIS_TMP_DIR/node1-data" \
    "$ANTITHESIS_TMP_DIR/node2-data" \
    "$ANTITHESIS_TMP_DIR/node3-data" \
    "$ANTITHESIS_TMP_DIR/history"
}

# The Antithesis Dockerfiles intentionally build from staged contexts instead
# of the repo root, so Docker never needs a root-level .dockerignore here.
reset_antithesis_context() {
  local context_dir="$1"
  rm -rf "$context_dir"
  mkdir -p "$context_dir"
}

prune_antithesis_context() {
  local context_dir="$1"
  find "$context_dir" -type d -name __pycache__ -prune -exec rm -rf {} +
  find "$context_dir" -type f \( -name '*.pyc' -o -name '.DS_Store' \) -delete
}

copy_antithesis_workspace_sources() {
  local context_dir="$1"
  cp "$ROOT_DIR/Cargo.toml" "$ROOT_DIR/Cargo.lock" "$context_dir/"
  cp -R "$ROOT_DIR/crates" "$context_dir/crates"
}

stage_antithesis_health_context() {
  local context_dir="$1"
  reset_antithesis_context "$context_dir"
  mkdir -p "$context_dir/health-checker"
  cp "$ROOT_DIR/tools/antithesis/health-checker/entrypoint.py" "$context_dir/health-checker/entrypoint.py"
  prune_antithesis_context "$context_dir"
}

stage_antithesis_config_context() {
  local context_dir="$1"
  reset_antithesis_context "$context_dir"
  mkdir -p "$context_dir/config"
  cp "$ROOT_DIR/tools/antithesis/config/docker-compose.yaml" "$context_dir/config/docker-compose.yaml"
  cp "$ROOT_DIR/tools/antithesis/config/.env.example" "$context_dir/config/.env.example"
  prune_antithesis_context "$context_dir"
}

stage_antithesis_node_package_context() {
  local context_dir="$1"
  reset_antithesis_context "$context_dir"
  copy_antithesis_workspace_sources "$context_dir"
  mkdir -p "$context_dir/node"
  cp "$ROOT_DIR/tools/antithesis/node/entrypoint.sh" "$context_dir/node/entrypoint.sh"
  prune_antithesis_context "$context_dir"
}

stage_antithesis_client_package_context() {
  local context_dir="$1"
  reset_antithesis_context "$context_dir"
  copy_antithesis_workspace_sources "$context_dir"
  mkdir -p "$context_dir/tools"
  cp -R "$ROOT_DIR/tools/porcupine_check" "$context_dir/tools/porcupine_check"
  cp -R "$ROOT_DIR/tools/antithesis/client" "$context_dir/client"
  prune_antithesis_context "$context_dir"
}

compose_in_antithesis() {
  resolve_compose
  (
    cd "$ROOT_DIR/tools/antithesis/config"
    "${COMPOSE[@]}" --env-file .env.example "$@"
  )
}

wait_for_antithesis_setup() {
  local timeout_s="${1:-120}"
  local setup_file="$ANTITHESIS_TMP_DIR/history/setup-complete.json"

  for _ in $(seq 1 "$timeout_s"); do
    if [[ -f "$setup_file" ]]; then
      return 0
    fi
    sleep 1
  done

  echo "error: cluster did not become ready (missing $setup_file)" >&2
  compose_in_antithesis logs --no-color >&2 || true
  return 1
}
