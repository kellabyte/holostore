//! Background range management: splitting and (future) rebalancing.
//!
//! This is a simplified Cockroach-style range manager. Today it:
//! - monitors range load (SET QPS) using node-local counters
//! - approximates range growth using SET ops since a per-range baseline
//! - proposes a safe split when a range is too big or too hot and there is a free shard index
//! - proposes a safe merge when adjacent ranges become too small
//!
//! Safety: range splits are coordinated with a cluster-wide "freeze" flag stored
//! in the meta group. While frozen, nodes block client traffic so there are no
//! in-flight writes during migration + descriptor updates.

use std::sync::Arc;
use std::time::{Duration, Instant};

use fjall::{Keyspace, PartitionCreateOptions};
use serde::Deserialize;
use volo::net::Address;

use crate::cluster::{ClusterCommand, MemberState, ShardDesc};
use crate::load::ShardLoadSnapshot;
use crate::volo_gen::holo_store::rpc;
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
    /// Merge adjacent ranges when their combined key count is below this threshold.
    ///
    /// Set to 0 to disable automatic merge proposals.
    pub merge_max_keys: usize,
    /// Cooldown after merge proposals to avoid split/merge oscillation.
    pub merge_cooldown: Duration,
    /// Maximum combined sustained SET QPS for an adjacent pair to be eligible
    /// for automatic merge. Set to 0 to disable QPS-based cold gating.
    pub merge_max_qps: u64,
    /// Require low combined QPS for this many consecutive evaluations before
    /// proposing a merge.
    pub merge_qps_sustain: u8,
    /// Hysteresis for merge key threshold as a percentage of `merge_max_keys`
    /// (1-100). Lower values reduce split/merge oscillation.
    pub merge_key_hysteresis_pct: u8,
}

pub fn spawn(state: Arc<NodeState>, keyspace: Arc<Keyspace>, cfg: RangeManagerConfig) {
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
        let mut merge_cooldown_until = std::collections::BTreeMap::<u64, std::time::Instant>::new();
        let mut merge_sustained = std::collections::BTreeMap::<u64, u8>::new();
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
                &mut merge_cooldown_until,
                &mut merge_sustained,
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
    merge_cooldown_until: &mut std::collections::BTreeMap<u64, std::time::Instant>,
    merge_sustained: &mut std::collections::BTreeMap<u64, u8>,
    now: std::time::Instant,
) -> anyhow::Result<()> {
    if state.cluster_store.frozen() {
        return Ok(());
    }

    let cluster_state = state.cluster_store.state();
    // Keep range-split freeze windows out of the way while replica moves are
    // in progress; rebalancing has higher correctness priority.
    if !cluster_state.shard_rebalances.is_empty() || !cluster_state.shard_merges.is_empty() {
        return Ok(());
    }

    let shard_limit = state.data_shards.max(1);
    let Some(target_idx) = state.cluster_store.first_free_shard_index(shard_limit) else {
        return Ok(());
    };

    let mut qps_by_idx = Vec::new();
    if let Some((reason, shard)) = pick_split_candidate(
        state.clone(),
        &cluster_state.shards,
        cfg,
        last,
        baseline_set_ops,
        sustained,
        cooldown_until,
        &mut qps_by_idx,
        now,
    )? {
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
        let freeze_before_epoch = state.cluster_store.epoch();
        propose_meta(state.clone(), ClusterCommand::SetFrozen { frozen: true }).await?;
        // Any error after freeze must still unfreeze before returning.
        let split_res = async {
            let freeze_epoch = wait_for_local_state(
                state.clone(),
                freeze_before_epoch,
                true,
                Duration::from_secs(5),
            )
            .await?;
            wait_for_cluster_converged(
                state.clone(),
                freeze_epoch,
                true,
                None,
                Duration::from_secs(5),
            )
            .await?;

            // While frozen, wait until the source group has drained all in-flight
            // consensus work. This avoids migrating from a state that is still
            // catching up committed writes.
            // Drain any client operations that already passed the freeze check.
            state
                .wait_for_client_ops_drained(Duration::from_secs(5))
                .await?;
            wait_for_shard_quiesced(state.clone(), shard.shard_index, Duration::from_secs(5))
                .await?;
            let split_before_epoch = state.cluster_store.epoch();
            propose_meta(
                state.clone(),
                ClusterCommand::SplitRange {
                    split_key,
                    target_shard_index: target_idx,
                },
            )
            .await?;
            let split_epoch =
                wait_for_local_epoch(state.clone(), split_before_epoch, Duration::from_secs(5))
                    .await?;
            let shard_count = state.cluster_store.state().shards.len();
            wait_for_cluster_converged(
                state.clone(),
                split_epoch,
                true,
                Some(shard_count),
                Duration::from_secs(8),
            )
            .await?;
            Ok::<(), anyhow::Error>(())
        }
        .await;

        // Always unfreeze, even if split failed.
        let unfreeze_before_epoch = state.cluster_store.epoch();
        let unfreeze_res =
            propose_meta(state.clone(), ClusterCommand::SetFrozen { frozen: false }).await;
        if let Err(err) = split_res {
            if let Err(unfreeze_err) = unfreeze_res {
                tracing::warn!(error = ?unfreeze_err, "failed to unfreeze after split failure");
            }
            return Err(err);
        }
        unfreeze_res?;
        let unfreeze_epoch = wait_for_local_state(
            state.clone(),
            unfreeze_before_epoch,
            false,
            Duration::from_secs(5),
        )
        .await?;
        let shard_count = state.cluster_store.state().shards.len();
        wait_for_cluster_converged(
            state.clone(),
            unfreeze_epoch,
            false,
            Some(shard_count),
            Duration::from_secs(5),
        )
        .await?;

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
        return Ok(());
    }

    if let Some((left, right, key_total)) = pick_merge_candidate(
        &cluster_state,
        keyspace.as_ref(),
        cfg,
        &qps_by_idx,
        merge_cooldown_until,
        merge_sustained,
        now,
    )? {
        tracing::info!(
            left_shard_id = left.shard_id,
            right_shard_id = right.shard_id,
            left_index = left.shard_index,
            right_index = right.shard_index,
            key_total,
            "range manager proposing merge"
        );
        propose_meta(
            state.clone(),
            ClusterCommand::BeginRangeMerge {
                left_shard_id: left.shard_id,
                right_shard_id: right.shard_id,
            },
        )
        .await?;
        let cooldown_deadline = now + cfg.merge_cooldown;
        merge_cooldown_until.insert(left.shard_id, cooldown_deadline);
        merge_cooldown_until.insert(right.shard_id, cooldown_deadline);
        merge_sustained.remove(&left.shard_id);
        merge_sustained.remove(&right.shard_id);
    }
    Ok(())
}

async fn wait_for_shard_quiesced(
    state: Arc<NodeState>,
    shard_index: usize,
    timeout: Duration,
) -> anyhow::Result<()> {
    let group_id = crate::GROUP_DATA_BASE + shard_index as u64;
    let group = state
        .group(group_id)
        .ok_or_else(|| anyhow::anyhow!("missing group for shard index {}", shard_index))?;
    let deadline = Instant::now() + timeout;

    loop {
        let stats = group.debug_stats().await;
        let pending = stats.records_status_preaccepted_len
            + stats.records_status_accepted_len
            + stats.records_status_committed_len
            + stats.records_status_executing_len
            + stats.committed_queue_len
            + stats.read_waiters_len;
        if pending == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for shard {} to quiesce (preaccepted={}, accepted={}, committed={}, executing={}, committed_queue={}, read_waiters={})",
                shard_index,
                stats.records_status_preaccepted_len,
                stats.records_status_accepted_len,
                stats.records_status_committed_len,
                stats.records_status_executing_len,
                stats.committed_queue_len,
                stats.read_waiters_len
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[derive(Debug, Deserialize)]
struct ClusterProbe {
    epoch: u64,
    frozen: bool,
    shards: Vec<serde_json::Value>,
}

async fn wait_for_local_epoch(
    state: Arc<NodeState>,
    min_epoch: u64,
    timeout: Duration,
) -> anyhow::Result<u64> {
    let deadline = Instant::now() + timeout;
    loop {
        let local_epoch = state.cluster_store.epoch();
        if local_epoch > min_epoch {
            return Ok(local_epoch);
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for local cluster epoch to advance beyond {min_epoch} (now={local_epoch})"
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_local_state(
    state: Arc<NodeState>,
    min_epoch: u64,
    frozen: bool,
    timeout: Duration,
) -> anyhow::Result<u64> {
    let deadline = Instant::now() + timeout;
    loop {
        let snapshot = state.cluster_store.state();
        if snapshot.epoch > min_epoch && snapshot.frozen == frozen {
            return Ok(snapshot.epoch);
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for local state (epoch>{min_epoch}, frozen={frozen}) (now_epoch={}, now_frozen={})",
                snapshot.epoch,
                snapshot.frozen
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_cluster_converged(
    state: Arc<NodeState>,
    min_epoch: u64,
    frozen: bool,
    min_shards: Option<usize>,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let members = state
            .cluster_store
            .state()
            .members
            .values()
            .filter(|m| m.state != MemberState::Removed)
            .map(|m| (m.node_id, m.grpc_addr.clone()))
            .collect::<Vec<_>>();

        let local = state.cluster_store.state();
        let mut all_ok = true;
        let mut last_err = String::new();
        for (node_id, grpc_addr) in members {
            if node_id == state.node_id {
                let shard_ok = min_shards.map(|n| local.shards.len() >= n).unwrap_or(true);
                if !(local.epoch >= min_epoch && local.frozen == frozen && shard_ok) {
                    all_ok = false;
                    last_err = format!(
                        "local node {} not converged (epoch={}, frozen={}, shards={})",
                        node_id,
                        local.epoch,
                        local.frozen,
                        local.shards.len()
                    );
                    break;
                }
                continue;
            }

            match fetch_remote_probe(&grpc_addr, Duration::from_secs(1)).await {
                Ok(remote) => {
                    let shard_ok = min_shards.map(|n| remote.shards.len() >= n).unwrap_or(true);
                    if !(remote.epoch >= min_epoch && remote.frozen == frozen && shard_ok) {
                        all_ok = false;
                        last_err = format!(
                            "node {} not converged (epoch={}, frozen={}, shards={})",
                            node_id,
                            remote.epoch,
                            remote.frozen,
                            remote.shards.len()
                        );
                        break;
                    }
                }
                Err(err) => {
                    all_ok = false;
                    last_err = format!("node {} probe failed: {err}", node_id);
                    break;
                }
            }
        }

        if all_ok {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for cluster convergence (min_epoch={}, frozen={}, min_shards={:?}): {}",
                min_epoch,
                frozen,
                min_shards,
                last_err
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn fetch_remote_probe(addr: &str, timeout: Duration) -> anyhow::Result<ClusterProbe> {
    let socket_addr: std::net::SocketAddr = addr.parse()?;
    let client = rpc::HoloRpcClientBuilder::new("holo_store.rpc.HoloRpc")
        .address(Address::from(socket_addr))
        .build();
    let resp = tokio::time::timeout(timeout, client.cluster_state(rpc::ClusterStateRequest {}))
        .await
        .map_err(|_| anyhow::anyhow!("timeout calling cluster_state"))??;
    let json = resp.into_inner().json.to_string();
    Ok(serde_json::from_str::<ClusterProbe>(&json)?)
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
    qps_by_idx_out: &mut Vec<u64>,
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
    *qps_by_idx_out = qps_by_idx.clone();

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

fn pick_merge_candidate(
    cluster_state: &crate::cluster::ClusterState,
    keyspace: &Keyspace,
    cfg: RangeManagerConfig,
    qps_by_idx: &[u64],
    merge_cooldown_until: &mut std::collections::BTreeMap<u64, std::time::Instant>,
    merge_sustained: &mut std::collections::BTreeMap<u64, u8>,
    now: std::time::Instant,
) -> anyhow::Result<Option<(ShardDesc, ShardDesc, usize)>> {
    if cfg.merge_max_keys == 0 || cluster_state.shards.len() < 2 {
        return Ok(None);
    }
    if !cluster_state.shard_rebalances.is_empty() || !cluster_state.shard_merges.is_empty() {
        return Ok(None);
    }

    merge_cooldown_until.retain(|_, until| *until > now);
    let adjacent_left_ids = cluster_state
        .shards
        .windows(2)
        .map(|pair| pair[0].shard_id)
        .collect::<std::collections::BTreeSet<_>>();
    merge_sustained.retain(|left_id, _| adjacent_left_ids.contains(left_id));

    let key_threshold = if cfg.merge_key_hysteresis_pct == 0 {
        cfg.merge_max_keys
    } else {
        ((cfg.merge_max_keys as u128).saturating_mul(cfg.merge_key_hysteresis_pct as u128) / 100)
            .max(1) as usize
    };
    let merge_qps_sustain = cfg.merge_qps_sustain.max(1);

    let mut best: Option<(usize, ShardDesc, ShardDesc)> = None;
    for pair in cluster_state.shards.windows(2) {
        let left = &pair[0];
        let right = &pair[1];
        if !left.end_key.is_empty() && left.end_key != right.start_key {
            continue;
        }
        if left.replicas != right.replicas || left.leaseholder != right.leaseholder {
            continue;
        }
        if merge_cooldown_until
            .get(&left.shard_id)
            .map(|until| *until > now)
            .unwrap_or(false)
            || merge_cooldown_until
                .get(&right.shard_id)
                .map(|until| *until > now)
                .unwrap_or(false)
        {
            continue;
        }

        let left_count =
            count_keys_in_range(keyspace, left.shard_index, &left.start_key, &left.end_key)?;
        let right_count = count_keys_in_range(
            keyspace,
            right.shard_index,
            &right.start_key,
            &right.end_key,
        )?;
        let total = left_count.saturating_add(right_count);
        if total > key_threshold {
            merge_sustained.remove(&left.shard_id);
            continue;
        }
        let combined_qps = qps_by_idx.get(left.shard_index).copied().unwrap_or(0)
            + qps_by_idx.get(right.shard_index).copied().unwrap_or(0);
        if cfg.merge_max_qps > 0 && combined_qps > cfg.merge_max_qps {
            merge_sustained.remove(&left.shard_id);
            continue;
        }
        let streak = merge_sustained.entry(left.shard_id).or_insert(0);
        *streak = streak.saturating_add(1);
        if *streak < merge_qps_sustain {
            continue;
        }
        match &best {
            Some((best_total, _, _)) if *best_total <= total => {}
            _ => best = Some((total, left.clone(), right.clone())),
        }
    }

    Ok(best.map(|(total, left, right)| (left, right, total)))
}

fn count_keys_in_range(
    keyspace: &Keyspace,
    shard_index: usize,
    start_key: &[u8],
    end_key: &[u8],
) -> anyhow::Result<usize> {
    let latest_name = format!("kv_latest_{shard_index}");
    let latest = keyspace.open_partition(&latest_name, PartitionCreateOptions::default())?;
    let mut count = 0usize;
    for item in latest_range(&latest, start_key, end_key) {
        let _ = item?;
        count = count.saturating_add(1);
    }
    Ok(count)
}

fn pick_split_key_from_range(
    keyspace: &Keyspace,
    shard: &ShardDesc,
) -> anyhow::Result<Option<Vec<u8>>> {
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
