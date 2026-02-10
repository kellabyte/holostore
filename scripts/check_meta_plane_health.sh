#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="${TARGET:-127.0.0.1:15051}"
HOLOCTL_BIN="${HOLOCTL_BIN:-$ROOT_DIR/target/release/holoctl}"
MAX_STUCK="${HOLO_META_MAX_STUCK_REBALANCES:-0}"
MAX_LAG_OPS="${HOLO_META_MAX_LAG_OPS:-256}"
MAX_ERROR_RATE="${HOLO_META_MAX_PROPOSAL_ERROR_RATE:-0.01}"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required (brew install jq)" >&2
  exit 2
fi

if [[ ! -x "$HOLOCTL_BIN" ]]; then
  echo "holoctl not found at $HOLOCTL_BIN; run: make build-release" >&2
  exit 2
fi

STATE_JSON="$("$HOLOCTL_BIN" --target "$TARGET" state)"

inflight="$(jq -r '.meta_health.rebalances_inflight // 0' <<<"$STATE_JSON")"
stuck="$(jq -r '.meta_health.rebalances_stuck // 0' <<<"$STATE_JSON")"
stuck_threshold_ms="$(jq -r '.meta_health.stuck_threshold_ms // 0' <<<"$STATE_JSON")"
max_lag_observed="$(jq -r '[.meta_health.lag_by_index[]?] | max // 0' <<<"$STATE_JSON")"
prop_count="$(jq -r '.meta_health.proposal_total.count // 0' <<<"$STATE_JSON")"
prop_errors="$(jq -r '.meta_health.proposal_total.errors // 0' <<<"$STATE_JSON")"
meta_lease_until="$(jq -r '.controller_leases.Meta.lease_until_ms // 0' <<<"$STATE_JSON")"
now_ms="$(($(date +%s) * 1000))"

prop_error_rate="$(awk -v count="$prop_count" -v err="$prop_errors" 'BEGIN { if (count == 0) print 0; else print err / count }')"

echo "meta health target=$TARGET"
echo "  rebalances_inflight=$inflight"
echo "  rebalances_stuck=$stuck (threshold_ms=$stuck_threshold_ms max_allowed=$MAX_STUCK)"
echo "  max_lag_ops=$max_lag_observed (max_allowed=$MAX_LAG_OPS)"
echo "  proposal_count=$prop_count proposal_errors=$prop_errors error_rate=$prop_error_rate (max_allowed=$MAX_ERROR_RATE)"
echo "  meta_controller_lease_until_ms=$meta_lease_until now_ms=$now_ms"

fail=0
if (( stuck > MAX_STUCK )); then
  echo "FAIL: stuck meta rebalances $stuck > $MAX_STUCK" >&2
  fail=1
fi
if (( max_lag_observed > MAX_LAG_OPS )); then
  echo "FAIL: meta lag $max_lag_observed > $MAX_LAG_OPS" >&2
  fail=1
fi
if ! awk -v v="$prop_error_rate" -v max="$MAX_ERROR_RATE" 'BEGIN { exit(v <= max ? 0 : 1) }'; then
  echo "FAIL: meta proposal error rate $prop_error_rate > $MAX_ERROR_RATE" >&2
  fail=1
fi
if (( meta_lease_until <= now_ms )); then
  echo "FAIL: meta controller lease is expired (or missing)" >&2
  fail=1
fi

if (( fail == 0 )); then
  echo "OK: meta-plane health within thresholds"
else
  exit 1
fi
