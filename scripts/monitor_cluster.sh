#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_DIR="${CLUSTER_DIR:-"$ROOT_DIR/.cluster"}"
PIDS_FILE="$CLUSTER_DIR/pids"

INTERVAL="${INTERVAL:-1}"
DURATION="${DURATION:-""}"
OUT="${OUT:-""}"

if [[ ! -f "$PIDS_FILE" ]]; then
  echo "error: pids file not found: $PIDS_FILE" >&2
  exit 1
fi

PIDS=()
while IFS= read -r pid; do
  [[ -z "$pid" ]] && continue
  PIDS+=("$pid")
done < "$PIDS_FILE"

if [[ ${#PIDS[@]} -eq 0 ]]; then
  echo "error: no pids found in $PIDS_FILE" >&2
  exit 1
fi

NODE_INFO=()
for pid in "${PIDS[@]}"; do
  cmd=$(ps -p "$pid" -o command= || true)
  node_id="?"
  if [[ "$cmd" =~ --node-id[[:space:]]+([0-9]+) ]]; then
    node_id="${BASH_REMATCH[1]}"
  fi
  NODE_INFO+=("$pid $node_id")
done

header="timestamp,node_id,pid,cpu_pct,rss_kb,etime"
if [[ -n "$OUT" ]]; then
  echo "$header" > "$OUT"
else
  echo "$header"
fi

start_ts=$(date +%s)

while true; do
  now=$(date +%s)
  ts=$(date -Iseconds)

  for entry in "${NODE_INFO[@]}"; do
    pid=$(awk '{print $1}' <<<"$entry")
    node_id=$(awk '{print $2}' <<<"$entry")
    line=$(ps -p "$pid" -o %cpu=,rss=,etime= || true)
    if [[ -z "$line" ]]; then
      continue
    fi
    cpu=$(awk '{print $1}' <<<"$line")
    rss=$(awk '{print $2}' <<<"$line")
    etime=$(awk '{print $3}' <<<"$line")
    row="$ts,$node_id,$pid,$cpu,$rss,$etime"
    if [[ -n "$OUT" ]]; then
      echo "$row" >> "$OUT"
    else
      echo "$row"
    fi
  done

  if [[ -n "$DURATION" ]]; then
    if (( now - start_ts >= DURATION )); then
      break
    fi
  fi

  sleep "$INTERVAL"
done
