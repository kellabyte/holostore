#!/usr/bin/env bash
set -euo pipefail

SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPTS_DIR/../../../../" && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-"$SCRIPTS_DIR/docker-compose.cockroach.yml"}"
DB_NAME="${DB_NAME:-holostore_bench}"
COCKROACH_DATA_DIR="${COCKROACH_DATA_DIR:-"$ROOT_DIR/.cluster/holo_fusion/cockroach"}"

if ! command -v docker >/dev/null 2>&1; then
  echo "error: docker is required" >&2
  exit 1
fi

if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  echo "error: docker compose is required (docker compose or docker-compose)" >&2
  exit 1
fi

mkdir -p "$COCKROACH_DATA_DIR/node1" "$COCKROACH_DATA_DIR/node2" "$COCKROACH_DATA_DIR/node3"

COCKROACH_DATA_DIR="$COCKROACH_DATA_DIR" "${COMPOSE[@]}" -f "$COMPOSE_FILE" up -d cockroach1 cockroach2 cockroach3

echo "waiting for cockroach cluster init readiness..."
ready=0
init_output=""
for _ in $(seq 1 60); do
  if [[ "$(docker inspect -f '{{.State.Running}}' cockroach1 2>/dev/null || true)" != "true" ]]; then
    break
  fi
  set +e
  init_output="$(COCKROACH_DATA_DIR="$COCKROACH_DATA_DIR" "${COMPOSE[@]}" -f "$COMPOSE_FILE" exec -T cockroach1 ./cockroach init \
    --insecure --host=cockroach1:26257 2>&1)"
  rc=$?
  set -e
  if [[ $rc -eq 0 ]] || grep -qi "already been initialized" <<<"$init_output"; then
    ready=1
    break
  fi
  sleep 1
done

if [[ "$ready" -ne 1 ]]; then
  echo "error: cockroach init readiness timed out" >&2
  if [[ -n "$init_output" ]]; then
    echo "last init output: $init_output" >&2
  fi
  COCKROACH_DATA_DIR="$COCKROACH_DATA_DIR" "${COMPOSE[@]}" -f "$COMPOSE_FILE" ps >&2 || true
  for node in cockroach1 cockroach2 cockroach3; do
    echo "--- $node logs ---" >&2
    docker logs --tail 60 "$node" 2>&1 >&2 || true
  done
  exit 1
fi

db_ready=0
for _ in $(seq 1 20); do
  if COCKROACH_DATA_DIR="$COCKROACH_DATA_DIR" "${COMPOSE[@]}" -f "$COMPOSE_FILE" exec -T cockroach1 ./cockroach sql \
    --insecure --host=cockroach1:26257 \
    -e "CREATE DATABASE IF NOT EXISTS ${DB_NAME};" >/dev/null 2>&1; then
    db_ready=1
    break
  fi
  sleep 1
done

if [[ "$db_ready" -ne 1 ]]; then
  echo "error: unable to create database ${DB_NAME}" >&2
  COCKROACH_DATA_DIR="$COCKROACH_DATA_DIR" "${COMPOSE[@]}" -f "$COMPOSE_FILE" ps >&2 || true
  exit 1
fi

cat <<EOF
cockroach cluster started

sql endpoints:
  node1: postgresql://root@127.0.0.1:26257/${DB_NAME}?sslmode=disable
  node2: postgresql://root@127.0.0.1:26258/${DB_NAME}?sslmode=disable
  node3: postgresql://root@127.0.0.1:26259/${DB_NAME}?sslmode=disable

ui endpoints:
  node1: http://127.0.0.1:8080
  node2: http://127.0.0.1:8081
  node3: http://127.0.0.1:8082

data dir:
  $COCKROACH_DATA_DIR
EOF
