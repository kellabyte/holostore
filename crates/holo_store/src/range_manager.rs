//! Background range management: splitting and (future) rebalancing.
//!
//! This is a simplified Cockroach-style range manager. Today it:
//! - monitors range load (SET QPS) using node-local counters
//! - approximates range growth using SET ops since a per-range baseline
//! - proposes a safe split when a range is too big or too hot and there is a free shard index
//! - proposes a safe merge when adjacent ranges become too small
//!
//! Safety: range splits are coordinated with shard-scoped fences in the meta
//! group. Only the split source/target shards are paused while migration +
//! descriptor updates run.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use fjall::{Keyspace, PartitionCreateOptions};
use serde::Deserialize;
use volo::net::Address;

use crate::cluster::{ClusterCommand, ControllerDomain, MemberState, ShardDesc};
use crate::load::ShardLoadSnapshot;
use crate::volo_gen::holo_store::rpc;
use crate::NodeState;

const SPLIT_FENCE_PREFIX: &str = "split-op";
const SPLIT_FENCE_STALE_TIMEOUT: Duration = Duration::from_secs(20);
const SPLIT_SHARD_DRAIN_LOW_WATERMARK: u64 = 8;
const SPLIT_SHARD_DRAIN_STABLE_FOR: Duration = Duration::from_millis(100);
const SPLIT_SHARD_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const SPLIT_FENCE_SYNC_TIMEOUT: Duration = Duration::from_secs(15);
const SPLIT_CLUSTER_SYNC_TIMEOUT: Duration = Duration::from_secs(15);
const SPLIT_FAILURE_BACKOFF_BASE: Duration = Duration::from_millis(250);
const SPLIT_FAILURE_BACKOFF_MAX: Duration = Duration::from_secs(15);
const SPLIT_FAILURE_BACKOFF_MAX_SHIFT: u32 = 6;
const SPLIT_KEY_SAMPLE_SIZE: usize = 256;
static SPLIT_TOKEN_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy)]
struct SplitFailureBackoff {
    failures: u32,
    retry_after: Instant,
}

#[derive(Debug, Clone)]
struct SplitFenceOp {
    token: String,
    started_ms: u64,
    reason: String,
    source_shard_id: u64,
}

impl SplitFenceOp {
    fn new(node_id: u64, source_shard_id: u64) -> Self {
        let started_ms = now_unix_ms();
        let seq = SPLIT_TOKEN_SEQ.fetch_add(1, Ordering::Relaxed);
        let token = format!("n{node_id}-{started_ms}-{seq}");
        let reason = encode_split_fence_reason(&token, started_ms);
        Self {
            token,
            started_ms,
            reason,
            source_shard_id,
        }
    }
}

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
        let mut split_failure_backoff =
            std::collections::BTreeMap::<u64, SplitFailureBackoff>::new();
        let mut merge_cooldown_until = std::collections::BTreeMap::<u64, std::time::Instant>::new();
        let mut merge_sustained = std::collections::BTreeMap::<u64, u8>::new();
        loop {
            ticker.tick().await;
            if !state
                .ensure_controller_leader(ControllerDomain::Range)
                .await
            {
                continue;
            }
            if let Err(err) = recover_stale_split_fences(state.clone()).await {
                tracing::warn!(error = ?err, "range manager stale split-fence recovery failed");
            }
            let now = std::time::Instant::now();
            if let Err(err) = maybe_split_once(
                state.clone(),
                keyspace.clone(),
                cfg,
                &mut last,
                &mut baseline_set_ops,
                &mut sustained,
                &mut cooldown_until,
                &mut split_failure_backoff,
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
    split_failure_backoff: &mut std::collections::BTreeMap<u64, SplitFailureBackoff>,
    merge_cooldown_until: &mut std::collections::BTreeMap<u64, std::time::Instant>,
    merge_sustained: &mut std::collections::BTreeMap<u64, u8>,
    now: std::time::Instant,
) -> anyhow::Result<()> {
    if state.cluster_store.frozen() {
        return Ok(());
    }

    let cluster_state = state.cluster_store.state();
    // Keep split operations out of the way while replica moves are in
    // progress; rebalancing has higher correctness priority.
    if !cluster_state.shard_rebalances.is_empty() || !cluster_state.shard_merges.is_empty() {
        return Ok(());
    }
    let live_shards = cluster_state
        .shards
        .iter()
        .map(|s| s.shard_id)
        .collect::<std::collections::BTreeSet<_>>();
    split_failure_backoff
        .retain(|shard_id, entry| live_shards.contains(shard_id) && entry.retry_after > now);
    state.set_split_backoff_active(split_failure_backoff.len() as u64);

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
        split_failure_backoff,
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
        let split_fence_op = SplitFenceOp::new(state.node_id, shard.shard_id);
        let fenced_shards = vec![split_fence_op.source_shard_id];

        tracing::info!(
            shard_id = shard.shard_id,
            shard_index = shard.shard_index,
            reason = reason,
            target_shard_index = target_idx,
            split_token = %split_fence_op.token,
            split_started_ms = split_fence_op.started_ms,
            split_key = %String::from_utf8_lossy(&split_key),
            "range manager proposing split"
        );
        state.record_split_attempt(reason);

        let split_res = async {
            ensure_range_controller_leader(state.clone()).await?;
            let fence_before_epoch = state.cluster_store.epoch();
            for shard_id in &fenced_shards {
                propose_meta(
                    state.clone(),
                    ClusterCommand::SetShardFence {
                        shard_id: *shard_id,
                        fenced: true,
                        reason: split_fence_op.reason.clone(),
                    },
                )
                .await?;
            }
            let required_fences =
                fenced_shards_to_expectations(&fenced_shards, &split_fence_op.reason);
            let fence_epoch = wait_for_local_fences(
                state.clone(),
                fence_before_epoch,
                &required_fences,
                &[],
                SPLIT_FENCE_SYNC_TIMEOUT,
            )
            .await?;
            wait_for_cluster_converged(
                state.clone(),
                fence_epoch,
                None,
                &required_fences,
                &[],
                SPLIT_CLUSTER_SYNC_TIMEOUT,
            )
            .await?;

            // Drain only the source shard to a low watermark; global client
            // drains under sustained ingest can starve split progress.
            wait_for_shard_low_watermark(
                state.clone(),
                shard.shard_index,
                SPLIT_SHARD_DRAIN_LOW_WATERMARK,
                SPLIT_SHARD_DRAIN_STABLE_FOR,
                SPLIT_SHARD_DRAIN_TIMEOUT,
            )
            .await?;

            ensure_range_controller_leader(state.clone()).await?;
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
                wait_for_local_epoch(state.clone(), split_before_epoch, SPLIT_FENCE_SYNC_TIMEOUT)
                    .await?;
            state
                .cluster_store
                .state()
                .shards
                .iter()
                .find(|s| s.shard_index == target_idx)
                .ok_or_else(|| {
                    anyhow::anyhow!("split finished without target shard index {target_idx}")
                })?;
            let shard_count = state.cluster_store.state().shards.len();
            wait_for_cluster_converged(
                state.clone(),
                split_epoch,
                Some(shard_count),
                &required_fences,
                &[],
                SPLIT_CLUSTER_SYNC_TIMEOUT,
            )
            .await?;
            Ok::<(), anyhow::Error>(())
        }
        .await;

        let clear_res = clear_split_fences(
            state.clone(),
            &fenced_shards,
            &split_fence_op.reason,
            SPLIT_FENCE_SYNC_TIMEOUT,
        )
        .await;
        if let Err(err) = split_res {
            let reason = classify_split_failure(&err);
            let (delay, failures) =
                schedule_split_failure_backoff(split_failure_backoff, shard.shard_id, now);
            state.record_split_failure(reason, &err.to_string());
            state.set_split_backoff_active(split_failure_backoff.len() as u64);
            tracing::warn!(
                shard_id = shard.shard_id,
                reason = reason,
                failures,
                retry_after_ms = delay.as_millis(),
                "range split failed; backoff scheduled"
            );
            if let Err(clear_err) = clear_res {
                tracing::warn!(error = ?clear_err, "failed to clear split fences after split failure");
            }
            return Err(err);
        }
        if let Err(err) = clear_res {
            let reason = classify_split_failure(&err);
            let (delay, failures) =
                schedule_split_failure_backoff(split_failure_backoff, shard.shard_id, now);
            state.record_split_failure(reason, &err.to_string());
            state.set_split_backoff_active(split_failure_backoff.len() as u64);
            tracing::warn!(
                shard_id = shard.shard_id,
                reason = reason,
                failures,
                retry_after_ms = delay.as_millis(),
                "range split fence cleanup failed; backoff scheduled"
            );
            return Err(err);
        }
        split_failure_backoff.remove(&shard.shard_id);
        state.set_split_backoff_active(split_failure_backoff.len() as u64);
        state.record_split_success();

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

async fn wait_for_shard_low_watermark(
    state: Arc<NodeState>,
    shard_index: usize,
    max_pending: u64,
    stable_for: Duration,
    timeout: Duration,
) -> anyhow::Result<()> {
    let group_id = crate::GROUP_DATA_BASE + shard_index as u64;
    let group = state
        .group(group_id)
        .ok_or_else(|| anyhow::anyhow!("missing group for shard index {}", shard_index))?;
    let deadline = Instant::now() + timeout;

    let mut stable_since: Option<Instant> = None;
    loop {
        ensure_range_controller_leader(state.clone()).await?;
        let stats = group.debug_stats().await;
        let pending = (stats.records_status_preaccepted_len
            + stats.records_status_accepted_len
            + stats.records_status_committed_len
            + stats.records_status_executing_len
            + stats.committed_queue_len
            + stats.read_waiters_len) as u64;
        if pending <= max_pending {
            let now = Instant::now();
            if let Some(since) = stable_since {
                if now.saturating_duration_since(since) >= stable_for {
                    return Ok(());
                }
            } else {
                stable_since = Some(now);
            }
        } else {
            stable_since = None;
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for shard {} low-watermark drain (pending={}, watermark={}, stable_for_ms={}, preaccepted={}, accepted={}, committed={}, executing={}, committed_queue={}, read_waiters={})",
                shard_index,
                pending,
                max_pending,
                stable_for.as_millis(),
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
    shards: Vec<serde_json::Value>,
    #[serde(default)]
    shard_fences: std::collections::BTreeMap<String, String>,
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

fn encode_split_fence_reason(token: &str, started_ms: u64) -> String {
    format!("{SPLIT_FENCE_PREFIX}:{token}:{started_ms}")
}

fn parse_split_fence_reason(reason: &str) -> Option<(String, u64)> {
    let mut parts = reason.split(':');
    let prefix = parts.next()?;
    if prefix != SPLIT_FENCE_PREFIX {
        return None;
    }
    let token = parts.next()?.to_string();
    let started_ms = parts.next()?.parse::<u64>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((token, started_ms))
}

fn fenced_shards_to_expectations(shard_ids: &[u64], reason: &str) -> Vec<(u64, String)> {
    shard_ids
        .iter()
        .copied()
        .map(|shard_id| (shard_id, reason.to_string()))
        .collect()
}

fn local_has_required_fences(
    snapshot: &crate::cluster::ClusterState,
    required_fences: &[(u64, String)],
) -> bool {
    required_fences.iter().all(|(shard_id, reason)| {
        snapshot
            .shard_fences
            .get(shard_id)
            .map(|current| current == reason)
            .unwrap_or(false)
    })
}

fn local_has_cleared_fences(
    snapshot: &crate::cluster::ClusterState,
    cleared_fences: &[u64],
) -> bool {
    cleared_fences
        .iter()
        .all(|shard_id| !snapshot.shard_fences.contains_key(shard_id))
}

fn probe_has_required_fences(probe: &ClusterProbe, required_fences: &[(u64, String)]) -> bool {
    required_fences.iter().all(|(shard_id, reason)| {
        probe
            .shard_fences
            .get(&shard_id.to_string())
            .map(|current| current == reason)
            .unwrap_or(false)
    })
}

fn probe_has_cleared_fences(probe: &ClusterProbe, cleared_fences: &[u64]) -> bool {
    cleared_fences
        .iter()
        .all(|shard_id| !probe.shard_fences.contains_key(&shard_id.to_string()))
}

async fn ensure_range_controller_leader(state: Arc<NodeState>) -> anyhow::Result<()> {
    if state
        .ensure_controller_leader(ControllerDomain::Range)
        .await
    {
        Ok(())
    } else {
        anyhow::bail!("lost range controller lease during split operation")
    }
}

async fn recover_stale_split_fences(state: Arc<NodeState>) -> anyhow::Result<()> {
    ensure_range_controller_leader(state.clone()).await?;
    let snapshot = state.cluster_store.state();
    if snapshot.shard_fences.is_empty() {
        return Ok(());
    }

    let now_ms = now_unix_ms();
    let stale_after_ms = SPLIT_FENCE_STALE_TIMEOUT
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    let mut stale_by_reason = std::collections::BTreeMap::<String, Vec<u64>>::new();
    for (shard_id, reason) in &snapshot.shard_fences {
        let Some((_token, started_ms)) = parse_split_fence_reason(reason) else {
            continue;
        };
        if now_ms.saturating_sub(started_ms) >= stale_after_ms {
            stale_by_reason
                .entry(reason.clone())
                .or_default()
                .push(*shard_id);
        }
    }

    for (reason, shard_ids) in stale_by_reason {
        let (token, started_ms) = parse_split_fence_reason(&reason)
            .ok_or_else(|| anyhow::anyhow!("invalid split fence reason format: {reason}"))?;
        tracing::warn!(
            split_token = %token,
            started_ms,
            stale_for_ms = now_ms.saturating_sub(started_ms),
            shard_ids = ?shard_ids,
            "clearing stale split fences"
        );

        ensure_range_controller_leader(state.clone()).await?;
        let clear_before_epoch = state.cluster_store.epoch();
        for shard_id in &shard_ids {
            propose_meta(
                state.clone(),
                ClusterCommand::SetShardFence {
                    shard_id: *shard_id,
                    fenced: false,
                    reason: reason.clone(),
                },
            )
            .await?;
        }
        let local_epoch = wait_for_local_fences(
            state.clone(),
            clear_before_epoch,
            &[],
            &shard_ids,
            Duration::from_secs(5),
        )
        .await?;
        wait_for_cluster_converged(
            state.clone(),
            local_epoch,
            None,
            &[],
            &shard_ids,
            Duration::from_secs(5),
        )
        .await?;
    }
    Ok(())
}

async fn clear_split_fences(
    state: Arc<NodeState>,
    fenced_shards: &[u64],
    reason: &str,
    timeout: Duration,
) -> anyhow::Result<()> {
    if fenced_shards.is_empty() {
        return Ok(());
    }
    ensure_range_controller_leader(state.clone()).await?;
    let clear_before_epoch = state.cluster_store.epoch();
    for shard_id in fenced_shards {
        propose_meta(
            state.clone(),
            ClusterCommand::SetShardFence {
                shard_id: *shard_id,
                fenced: false,
                reason: reason.to_string(),
            },
        )
        .await?;
    }
    let local_epoch = wait_for_local_fences(
        state.clone(),
        clear_before_epoch,
        &[],
        fenced_shards,
        timeout,
    )
    .await?;
    wait_for_cluster_converged(
        state.clone(),
        local_epoch,
        None,
        &[],
        fenced_shards,
        timeout,
    )
    .await
}

async fn wait_for_local_epoch(
    state: Arc<NodeState>,
    min_epoch: u64,
    timeout: Duration,
) -> anyhow::Result<u64> {
    let deadline = Instant::now() + timeout;
    loop {
        ensure_range_controller_leader(state.clone()).await?;
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

async fn wait_for_local_fences(
    state: Arc<NodeState>,
    min_epoch: u64,
    required_fences: &[(u64, String)],
    cleared_fences: &[u64],
    timeout: Duration,
) -> anyhow::Result<u64> {
    let deadline = Instant::now() + timeout;
    loop {
        ensure_range_controller_leader(state.clone()).await?;
        let snapshot = state.cluster_store.state();
        if snapshot.epoch >= min_epoch
            && local_has_required_fences(&snapshot, required_fences)
            && local_has_cleared_fences(&snapshot, cleared_fences)
        {
            return Ok(snapshot.epoch);
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for local fence state (epoch>={min_epoch}, required_fences={:?}, cleared_fences={:?}) (now_epoch={}, now_fences={:?})",
                required_fences,
                cleared_fences,
                snapshot.epoch,
                snapshot.shard_fences
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_cluster_converged(
    state: Arc<NodeState>,
    min_epoch: u64,
    min_shards: Option<usize>,
    required_fences: &[(u64, String)],
    cleared_fences: &[u64],
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        ensure_range_controller_leader(state.clone()).await?;
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
                let fences_ok = local_has_required_fences(&local, required_fences)
                    && local_has_cleared_fences(&local, cleared_fences);
                if !(local.epoch >= min_epoch && shard_ok && fences_ok) {
                    all_ok = false;
                    last_err = format!(
                        "local node {} not converged (epoch={}, shards={}, fences={:?})",
                        node_id,
                        local.epoch,
                        local.shards.len(),
                        local.shard_fences
                    );
                    break;
                }
                continue;
            }

            match fetch_remote_probe(&grpc_addr, Duration::from_secs(1)).await {
                Ok(remote) => {
                    let shard_ok = min_shards.map(|n| remote.shards.len() >= n).unwrap_or(true);
                    let fences_ok = probe_has_required_fences(&remote, required_fences)
                        && probe_has_cleared_fences(&remote, cleared_fences);
                    if !(remote.epoch >= min_epoch && shard_ok && fences_ok) {
                        all_ok = false;
                        last_err = format!(
                            "node {} not converged (epoch={}, shards={}, fences={:?})",
                            node_id,
                            remote.epoch,
                            remote.shards.len(),
                            remote.shard_fences
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
                "timed out waiting for cluster convergence (min_epoch={}, min_shards={:?}, required_fences={:?}, cleared_fences={:?}): {}",
                min_epoch,
                min_shards,
                required_fences,
                cleared_fences,
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
    ensure_range_controller_leader(state.clone()).await?;
    state
        .propose_meta_command_guarded(ControllerDomain::Range, cmd)
        .await
}

fn pick_split_candidate(
    state: Arc<NodeState>,
    shards: &[ShardDesc],
    cfg: RangeManagerConfig,
    last: &mut ShardLoadSnapshot,
    baseline_set_ops: &mut [u64],
    sustained: &mut [u8],
    cooldown_until: &mut [std::time::Instant],
    split_failure_backoff: &std::collections::BTreeMap<u64, SplitFailureBackoff>,
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
        if split_failure_backoff
            .get(&shard.shard_id)
            .map(|entry| entry.retry_after > now)
            .unwrap_or(false)
        {
            continue;
        }
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
        if split_failure_backoff
            .get(&shard.shard_id)
            .map(|entry| entry.retry_after > now)
            .unwrap_or(false)
        {
            continue;
        }
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

    // Approximate median with one-pass reservoir sampling. This avoids a full
    // two-pass scan of the range while still spreading split points.
    let mut seen = 0usize;
    let mut sample = Vec::<Vec<u8>>::with_capacity(SPLIT_KEY_SAMPLE_SIZE);
    for item in latest_range(&latest, &shard.start_key, &shard.end_key) {
        let (key, _) = item?;
        let key_vec = key.to_vec();
        seen = seen.saturating_add(1);
        if sample.len() < SPLIT_KEY_SAMPLE_SIZE {
            sample.push(key_vec);
            continue;
        }
        let replace = reservoir_replace_index(shard.shard_id, seen, &key_vec);
        if replace < SPLIT_KEY_SAMPLE_SIZE {
            sample[replace] = key_vec;
        }
    }
    if seen < 2 || sample.len() < 2 {
        return Ok(None);
    }
    sample.sort();
    let mid = sample.len() / 2;
    let in_bounds = |key: &Vec<u8>| {
        (shard.start_key.is_empty() || key.as_slice() > shard.start_key.as_slice())
            && (shard.end_key.is_empty() || key.as_slice() < shard.end_key.as_slice())
    };
    if let Some(candidate) = sample[mid..].iter().find(|k| in_bounds(k)) {
        return Ok(Some(candidate.clone()));
    }
    if let Some(candidate) = sample[..mid].iter().rev().find(|k| in_bounds(k)) {
        return Ok(Some(candidate.clone()));
    }
    Ok(None)
}

fn reservoir_replace_index(shard_id: u64, seen: usize, key: &[u8]) -> usize {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    shard_id.hash(&mut hasher);
    seen.hash(&mut hasher);
    key.hash(&mut hasher);
    (hasher.finish() as usize) % seen.max(1)
}

fn classify_split_failure(err: &anyhow::Error) -> &'static str {
    let text = err.to_string().to_lowercase();
    if text.contains("controller lease") {
        "lease_lost"
    } else if text.contains("cluster convergence") {
        "cluster_convergence_timeout"
    } else if text.contains("fence") {
        "fence_sync_failed"
    } else if text.contains("low-watermark drain")
        || text.contains("timed out waiting for shard")
        || text.contains("quiesce")
    {
        "shard_drain_timeout"
    } else if text.contains("split finished without target shard") || text.contains("split key") {
        "split_apply_failed"
    } else {
        "other"
    }
}

fn schedule_split_failure_backoff(
    split_failure_backoff: &mut std::collections::BTreeMap<u64, SplitFailureBackoff>,
    shard_id: u64,
    now: Instant,
) -> (Duration, u32) {
    let entry = split_failure_backoff
        .entry(shard_id)
        .or_insert(SplitFailureBackoff {
            failures: 0,
            retry_after: now,
        });
    entry.failures = entry.failures.saturating_add(1);
    let delay = split_failure_backoff_delay(shard_id, entry.failures);
    entry.retry_after = now + delay;
    (delay, entry.failures)
}

fn split_failure_backoff_delay(shard_id: u64, failures: u32) -> Duration {
    use std::hash::{Hash, Hasher};

    let shift = failures
        .saturating_sub(1)
        .min(SPLIT_FAILURE_BACKOFF_MAX_SHIFT);
    let multiplier = 1u64 << shift;
    let base_ms = SPLIT_FAILURE_BACKOFF_BASE
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    let raw_ms = base_ms.saturating_mul(multiplier);
    let capped_ms = raw_ms.min(
        SPLIT_FAILURE_BACKOFF_MAX
            .as_millis()
            .min(u128::from(u64::MAX)) as u64,
    );

    // Deterministic jitter in [80%, 120%] to avoid synchronized retries.
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    shard_id.hash(&mut hasher);
    failures.hash(&mut hasher);
    now_unix_ms().hash(&mut hasher);
    let jitter_percent = 80u64 + (hasher.finish() % 41);
    let jittered_ms = capped_ms.saturating_mul(jitter_percent) / 100;
    Duration::from_millis(jittered_ms.max(1))
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
