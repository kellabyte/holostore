#!/usr/bin/env bash
set -euo pipefail

# Resolve Docker service names to IPv4 addresses because the node CLI expects
# concrete socket addresses for initial membership and join targets.
lookup_ip() {
  local name="$1"
  local ip=""
  while [[ -z "$ip" ]]; do
    ip="$(getent hosts "$name" | awk '{print $1; exit}')"
    if [[ -n "$ip" ]]; then
      printf '%s\n' "$ip"
      return 0
    fi
    sleep 0.2
  done
}

NODE_ID="${NODE_ID:?NODE_ID is required}"
HOLO_MAX_SHARDS="${HOLO_MAX_SHARDS:-4}"

n1="$(lookup_ip holo-node1)"
n2="$(lookup_ip holo-node2)"
n3="$(lookup_ip holo-node3)"
members="1@${n1}:15051,2@${n2}:15051,3@${n3}:15051"

mode_args=(--join "${n1}:15051")
if [[ "$NODE_ID" == "1" ]]; then
  mode_args=(--bootstrap)
else
  until nc -z "$n1" 15051 >/dev/null 2>&1; do
    sleep 0.2
  done
fi

mkdir -p /data

exec /usr/local/bin/holo-store node \
  --node-id "$NODE_ID" \
  --listen-redis "0.0.0.0:6379" \
  --listen-grpc "0.0.0.0:15051" \
  --initial-members "$members" \
  --data-dir "/data" \
  --max-shards "$HOLO_MAX_SHARDS" \
  "${mode_args[@]}"

