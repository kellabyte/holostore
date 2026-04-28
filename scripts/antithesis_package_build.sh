#!/usr/bin/env bash
set -euo pipefail

# Build uploadable Antithesis images from generated, minimal Docker contexts.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/antithesis_common.sh"

prepare_antithesis_tmp

IMAGE_TAG="${IMAGE_TAG:-antithesis}"
ANTITHESIS_REGISTRY="${ANTITHESIS_REGISTRY:-}"
ANTITHESIS_REGISTRY="${ANTITHESIS_REGISTRY%/}"
PACKAGE_BUILD_DIR="$ANTITHESIS_BUILD_DIR/package"

image_ref() {
  local name="$1"
  if [[ -n "$ANTITHESIS_REGISTRY" ]]; then
    printf '%s/%s:%s\n' "$ANTITHESIS_REGISTRY" "$name" "$IMAGE_TAG"
  else
    printf '%s:%s\n' "$name" "$IMAGE_TAG"
  fi
}

NODE_IMAGE="${HOLOSTORE_NODE_IMAGE:-$(image_ref holostore-node)}"
CLIENT_IMAGE="${HOLOSTORE_CLIENT_IMAGE:-$(image_ref holostore-client)}"
HEALTHCHECK_IMAGE="${HOLOSTORE_HEALTHCHECK_IMAGE:-$(image_ref holostore-health-checker)}"
CONFIG_IMAGE="${HOLOSTORE_CONFIG_IMAGE:-$(image_ref holostore-config)}"

NODE_CONTEXT="$PACKAGE_BUILD_DIR/node-context"
CLIENT_CONTEXT="$PACKAGE_BUILD_DIR/client-context"
HEALTH_CONTEXT="$PACKAGE_BUILD_DIR/health-checker-context"
CONFIG_CONTEXT="$PACKAGE_BUILD_DIR/config-context"

stage_antithesis_node_package_context "$NODE_CONTEXT"
stage_antithesis_client_package_context "$CLIENT_CONTEXT"
stage_antithesis_health_context "$HEALTH_CONTEXT"
stage_antithesis_config_context "$CONFIG_CONTEXT"

docker build --platform linux/amd64 -f "$ROOT_DIR/tools/antithesis/Dockerfile.node" -t "$NODE_IMAGE" "$NODE_CONTEXT"
docker build --platform linux/amd64 -f "$ROOT_DIR/tools/antithesis/Dockerfile.client" -t "$CLIENT_IMAGE" "$CLIENT_CONTEXT"
docker build --platform linux/amd64 -f "$ROOT_DIR/tools/antithesis/Dockerfile.health-checker" -t "$HEALTHCHECK_IMAGE" "$HEALTH_CONTEXT"
docker build --platform linux/amd64 -f "$ROOT_DIR/tools/antithesis/Dockerfile.config" -t "$CONFIG_IMAGE" "$CONFIG_CONTEXT"

cat <<EOF
Built Antithesis package images:
  node:           $NODE_IMAGE
  client:         $CLIENT_IMAGE
  health-checker: $HEALTHCHECK_IMAGE
  config:         $CONFIG_IMAGE

Staged contexts:
  $NODE_CONTEXT
  $CLIENT_CONTEXT
  $HEALTH_CONTEXT
  $CONFIG_CONTEXT
EOF
