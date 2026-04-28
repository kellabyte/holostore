#!/usr/bin/env bash
set -euo pipefail

# Build fast local runtime images from linux/amd64 binaries produced inside
# builder containers while preserving the clean-room Dockerfiles in tools/antithesis/
# for tenant packaging.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/antithesis_common.sh"

prepare_antithesis_tmp

LINUX_TARGET_DIR="$ANTITHESIS_BUILD_DIR/linux-target"
CARGO_HOME_DIR="$ANTITHESIS_BUILD_DIR/cargo-home"
mkdir -p "$LINUX_TARGET_DIR" "$CARGO_HOME_DIR"

docker run --rm --platform linux/amd64 \
  -e CARGO_HOME=/cargo \
  -e CARGO_TARGET_DIR=/src/.tmp/antithesis/build/linux-target \
  -v "$ROOT_DIR":/src \
  -v "$CARGO_HOME_DIR":/cargo \
  -w /src \
  rust:1-bookworm \
  bash -lc '
    set -euo pipefail
    export PATH=/usr/local/cargo/bin:$PATH
    rustup component add rustfmt
    cargo build -p holo_store --release --bin holo-store
    cargo build -p holo_workload --release --bin holo-workload
  '

docker run --rm --platform linux/amd64 \
  -v "$ROOT_DIR":/src \
  -w /src \
  golang:1-bookworm \
  bash -lc '
    set -euo pipefail
    export PATH=/usr/local/go/bin:$PATH
    cd tools/porcupine_check
    GOCACHE=/tmp/go-build go build -buildvcs=false -o /src/.tmp/antithesis/build/porcupine-check .
  '

NODE_CONTEXT="$ANTITHESIS_BUILD_DIR/node-context"
CLIENT_CONTEXT="$ANTITHESIS_BUILD_DIR/client-context"
HEALTH_CONTEXT="$ANTITHESIS_BUILD_DIR/health-checker-context"
CONFIG_CONTEXT="$ANTITHESIS_BUILD_DIR/config-context"
reset_antithesis_context "$NODE_CONTEXT"
reset_antithesis_context "$CLIENT_CONTEXT"
stage_antithesis_health_context "$HEALTH_CONTEXT"
stage_antithesis_config_context "$CONFIG_CONTEXT"

cp "$LINUX_TARGET_DIR/release/holo-store" "$NODE_CONTEXT/holo-store"
cp "$ROOT_DIR/tools/antithesis/node/entrypoint.sh" "$NODE_CONTEXT/entrypoint.sh"

cp "$LINUX_TARGET_DIR/release/holo-workload" "$CLIENT_CONTEXT/holo-workload"
cp "$ANTITHESIS_BUILD_DIR/porcupine-check" "$CLIENT_CONTEXT/porcupine-check"
cp -R "$ROOT_DIR/tools/antithesis/client" "$CLIENT_CONTEXT/client"

docker build --platform linux/amd64 -f "$ROOT_DIR/tools/antithesis/Dockerfile.node.local" -t holostore-node:antithesis "$NODE_CONTEXT"
docker build --platform linux/amd64 -f "$ROOT_DIR/tools/antithesis/Dockerfile.client.local" -t holostore-client:antithesis "$CLIENT_CONTEXT"
docker build --platform linux/amd64 -f "$ROOT_DIR/tools/antithesis/Dockerfile.health-checker" -t holostore-health-checker:antithesis "$HEALTH_CONTEXT"
docker build --platform linux/amd64 -f "$ROOT_DIR/tools/antithesis/Dockerfile.config" -t holostore-config:antithesis "$CONFIG_CONTEXT"
