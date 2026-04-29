#!/usr/bin/env bash
set -euo pipefail

lookup_ip() {
  local name="$1"
  local ip=""
  until [[ -n "$ip" ]]; do
    ip="$(getent hosts "$name" | awk '{print $1; exit}')"
    if [[ -z "$ip" ]]; then
      sleep 0.2
    fi
  done
  printf '%s\n' "$ip"
}

NODE_ID="${NODE_ID:?NODE_ID is required}"
HOLO_NODE_HOSTS="${HOLO_NODE_HOSTS:-holostore1,holostore2,holostore3}"
HOLO_REDIS_PORT="${HOLO_REDIS_PORT:-6379}"
HOLO_GRPC_PORT="${HOLO_GRPC_PORT:-15051}"
HOLO_MAX_SHARDS="${HOLO_MAX_SHARDS:-4}"

IFS=',' read -r -a hosts <<<"$HOLO_NODE_HOSTS"
if [[ "${#hosts[@]}" -ne 3 ]]; then
  echo "HOLO_NODE_HOSTS must contain exactly 3 comma-separated hosts" >&2
  exit 2
fi

members=""
for idx in "${!hosts[@]}"; do
  member_id="$((idx + 1))"
  member_ip="$(lookup_ip "${hosts[$idx]}")"
  if [[ -n "$members" ]]; then
    members+=","
  fi
  members+="${member_id}@${member_ip}:${HOLO_GRPC_PORT}"
done

seed_ip="$(lookup_ip "${hosts[0]}")"
mode_args=(--join "${seed_ip}:${HOLO_GRPC_PORT}")
if [[ "$NODE_ID" == "1" ]]; then
  mode_args=(--bootstrap)
else
  until nc -z "$seed_ip" "$HOLO_GRPC_PORT" >/dev/null 2>&1; do
    sleep 0.2
  done
fi

mkdir -p /data

exec /usr/local/bin/holo-store node \
  --node-id "$NODE_ID" \
  --listen-redis "0.0.0.0:${HOLO_REDIS_PORT}" \
  --listen-grpc "0.0.0.0:${HOLO_GRPC_PORT}" \
  --initial-members "$members" \
  --data-dir "/data" \
  --max-shards "$HOLO_MAX_SHARDS" \
  "${mode_args[@]}"
