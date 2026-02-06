//! Background range management: splitting and (future) rebalancing.
//!
//! This is a simplified Cockroach-style range manager. Today it:
//! - monitors range load (SET QPS) using node-local counters
//! - approximates range growth using SET ops since a per-range baseline
//! - proposes a safe split when a range is too big or too hot and there is a free shard index
//!
//! Safety: range splits are coordinated with a cluster-wide "freeze" flag stored
//! in the meta group. While frozen, nodes block client traffic so there are no
//! in-flight writes during migration + descriptor updates.

use std::sync::Arc;
use std::time::Duration;

use fjall::{Keyspace, PartitionCreateOptions};

use crate::cluster::{ClusterCommand, ShardDesc};
use crate::load::ShardLoadSnapshot;
use crate::NodeState;

/// Configuration for the range manager.
#[derive(Clone, Copy, Debug)]
pub struct RangeManagerConfig {
    /// Evaluate split candidates at this interval.
    pub interval: Duration,
    /// Split when a range has at least this many "new" keys since its last split.
    ///
    /// Today this is implemented as an approximation: the range manager tracks
    /// SET ops per shard index and uses "SET ops since baseline" as a proxy for
    /// unique key growth. This avoids expensive keyspace scans on the write path.
    ///
    /// Set to 0 to disable size/growth-based splitting.
    pub split_min_keys: usize,
    /// Split when a range's sustained SET QPS exceeds this threshold.
    ///
    /// Set to 0 to disable load-based splitting (size-based splitting may still apply).
    pub split_min_qps: u64,
    /// Require QPS threshold for this many consecutive evaluations.
    pub split_qps_sustain: u8,
    /// Cooldown between splits for the same shard index.
    pub split_cooldown: Duration,
}

pub fn spawn(
    state: Arc<NodeState>,
    keyspace: Arc<Keyspace>,
    cfg: RangeManagerConfig,
) {
    // Keep it simple: only one node proposes management operations.
    if state.node_id != 1 {
        return;
    }
    if state.data_shards <= 1 {
        return;
    }
    if !matches!(state.routing_mode, crate::RoutingMode::Range) {
        return;
    }

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(cfg.interval);
        let mut last = state.shard_load.snapshot();
        // Approximate "range size" using set-op deltas since baseline. This is
        // not unique keys, but it's cheap and good enough to trigger splits for
        // write-heavy workloads.
        let mut baseline_set_ops = last.set_ops.clone();
        let mut sustained: Vec<u8> = vec![0; state.shard_load.shards()];
        let mut cooldown_until: Vec<std::time::Instant> =
            vec![std::time::Instant::now(); state.shard_load.shards()];
        loop {
            ticker.tick().await;
            let now = std::time::Instant::now();
            if let Err(err) = maybe_split_once(
                state.clone(),
                keyspace.clone(),
                cfg,
                &mut last,
                &mut baseline_set_ops,
                &mut sustained,
                &mut cooldown_until,
                now,
            )
            .await
            {
                tracing::warn!(error = ?err, "range manager split attempt failed");
            }
        }
    });
}

async fn maybe_split_once(
    state: Arc<NodeState>,
    keyspace: Arc<Keyspace>,
    cfg: RangeManagerConfig,
    last: &mut ShardLoadSnapshot,
    baseline_set_ops: &mut [u64],
    sustained: &mut [u8],
    cooldown_until: &mut [std::time::Instant],
    now: std::time::Instant,
) -> anyhow::Result<()> {
    if state.cluster_store.frozen() {
        return Ok(());
    }

    let shard_limit = state.data_shards.max(1);
    let Some(target_idx) = state.cluster_store.first_free_shard_index(shard_limit) else {
        return Ok(());
    };

    let cluster_state = state.cluster_store.state();
    let Some((reason, shard)) = pick_split_candidate(
        state.clone(),
        &cluster_state.shards,
        cfg,
        last,
        baseline_set_ops,
        sustained,
        cooldown_until,
        now,
    )? else {
        return Ok(());
    };

    // Only read the keyspace once we've decided to split.
    let Some(split_key) = pick_split_key_from_range(keyspace.as_ref(), &shard)? else {
        // No keys in this range; reset streaks and wait.
        if shard.shard_index < sustained.len() {
            sustained[shard.shard_index] = 0;
        }
        return Ok(());
    };

    tracing::info!(
        shard_id = shard.shard_id,
        shard_index = shard.shard_index,
        reason = reason,
        target_shard_index = target_idx,
        split_key = %String::from_utf8_lossy(&split_key),
        "range manager proposing split"
    );

    // Freeze traffic to avoid in-flight proposals during key migration + reroute.
    propose_meta(state.clone(), ClusterCommand::SetFrozen { frozen: true }).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    propose_meta(
        state.clone(),
        ClusterCommand::SplitRange {
            split_key,
            target_shard_index: target_idx,
        },
    )
    .await?;

    propose_meta(state, ClusterCommand::SetFrozen { frozen: false }).await?;

    // Apply cooldown to the *source* shard index to avoid rapid re-splitting.
    if let Some(slot) = cooldown_until.get_mut(shard.shard_index) {
        *slot = now + cfg.split_cooldown;
    }
    // Reset "size" baselines for both source and target shard indices.
    if shard.shard_index < baseline_set_ops.len() {
        baseline_set_ops[shard.shard_index] = last
            .set_ops
            .get(shard.shard_index)
            .copied()
            .unwrap_or(baseline_set_ops[shard.shard_index]);
    }
    if target_idx < baseline_set_ops.len() {
        baseline_set_ops[target_idx] = last.set_ops.get(target_idx).copied().unwrap_or(0);
    }
    if shard.shard_index < sustained.len() {
        sustained[shard.shard_index] = 0;
    }
    Ok(())
}

async fn propose_meta(state: Arc<NodeState>, cmd: ClusterCommand) -> anyhow::Result<()> {
    let payload = crate::cluster::ClusterStateMachine::encode_command(&cmd)?;
    let _ = state.meta_handle.propose(payload).await?;
    Ok(())
}

fn pick_split_candidate(
    state: Arc<NodeState>,
    shards: &[ShardDesc],
    cfg: RangeManagerConfig,
    last: &mut ShardLoadSnapshot,
    baseline_set_ops: &mut [u64],
    sustained: &mut [u8],
    cooldown_until: &mut [std::time::Instant],
    now: std::time::Instant,
) -> anyhow::Result<Option<(&'static str, ShardDesc)>> {
    let cur = state.shard_load.snapshot();
    let secs = cfg.interval.as_secs_f64().max(0.001);

    // Compute SET QPS deltas per shard index.
    let mut qps_by_idx = vec![0u64; cur.set_ops.len()];
    for (idx, (cur_v, last_v)) in cur
        .set_ops
        .iter()
        .copied()
        .zip(last.set_ops.iter().copied())
        .enumerate()
    {
        let delta = cur_v.saturating_sub(last_v);
        let qps = ((delta as f64) / secs) as u64;
        qps_by_idx[idx] = qps;
    }

    // Update last snapshot.
    *last = cur;

    // Track sustained-above-threshold streaks.
    for (idx, qps) in qps_by_idx.iter().copied().enumerate() {
        let above = cfg.split_min_qps > 0 && qps >= cfg.split_min_qps;
        let cooldown_active = cooldown_until
            .get(idx)
            .map(|until| *until > now)
            .unwrap_or(false);

        if above && !cooldown_active {
            if let Some(s) = sustained.get_mut(idx) {
                *s = s.saturating_add(1);
            }
        } else if let Some(s) = sustained.get_mut(idx) {
            *s = 0;
        }
    }

    // 1) Prefer load-based splitting (hottest shard that is sustained above threshold).
    let mut best_load: Option<(u64, ShardDesc)> = None;
    for shard in shards {
        let idx = shard.shard_index;
        if idx >= qps_by_idx.len() || idx >= sustained.len() {
            continue;
        }
        if sustained[idx] < cfg.split_qps_sustain {
            continue;
        }
        let qps = qps_by_idx[idx];
        match &best_load {
            Some((best_qps, _)) if *best_qps >= qps => {}
            _ => best_load = Some((qps, shard.clone())),
        }
    }
    if let Some((_qps, shard)) = best_load {
        return Ok(Some(("load", shard)));
    }

    // 2) Size-based splitting (approximate by SET ops since baseline).
    if cfg.split_min_keys == 0 {
        return Ok(None);
    }
    let mut best_size: Option<(u64, ShardDesc)> = None;
    for shard in shards {
        let idx = shard.shard_index;
        if idx >= last.set_ops.len() || idx >= baseline_set_ops.len() {
            continue;
        }
        let cooldown_active = cooldown_until
            .get(idx)
            .map(|until| *until > now)
            .unwrap_or(false);
        if cooldown_active {
            continue;
        }
        let since = last.set_ops[idx].saturating_sub(baseline_set_ops[idx]);
        if since < cfg.split_min_keys as u64 {
            continue;
        }
        match &best_size {
            Some((best_since, _)) if *best_since >= since => {}
            _ => best_size = Some((since, shard.clone())),
        }
    }
    if let Some((_since, shard)) = best_size {
        return Ok(Some(("size", shard)));
    }

    Ok(None)
}

fn pick_split_key_from_range(keyspace: &Keyspace, shard: &ShardDesc) -> anyhow::Result<Option<Vec<u8>>> {
    let latest_name = format!("kv_latest_{}", shard.shard_index);
    let latest = keyspace.open_partition(&latest_name, PartitionCreateOptions::default())?;

    // Pick a split key by median key position in this range.
    // This avoids pathological splits from min/max midpoint when key prefixes
    // are long and only a suffix differs (e.g. key:000...).
    let mut total = 0usize;
    for item in latest_range(&latest, &shard.start_key, &shard.end_key) {
        let _ = item?;
        total += 1;
    }
    if total < 2 {
        return Ok(None);
    }
    let mid = total / 2;

    let mut prev_key: Option<Vec<u8>> = None;
    for (idx, item) in latest_range(&latest, &shard.start_key, &shard.end_key).enumerate() {
        let (key, _) = item?;
        let key_vec = key.to_vec();
        if idx == mid {
            // Ensure the split point is strictly between neighbors/bounds.
            if let Some(prev) = prev_key.as_ref() {
                if prev >= &key_vec {
                    return Ok(None);
                }
            }
            if !shard.start_key.is_empty() && key_vec <= shard.start_key {
                return Ok(None);
            }
            if !shard.end_key.is_empty() && key_vec >= shard.end_key {
                return Ok(None);
            }
            return Ok(Some(key_vec));
        }
        prev_key = Some(key_vec);
    }

    Ok(None)
}

fn latest_range(
    latest: &fjall::PartitionHandle,
    start: &[u8],
    end: &[u8],
) -> Box<dyn DoubleEndedIterator<Item = fjall::Result<fjall::KvPair>>> {
    let start = start.to_vec();
    if end.is_empty() {
        Box::new(latest.range(start..))
    } else {
        Box::new(latest.range(start..end.to_vec()))
    }
}
