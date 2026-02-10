//! Background metadata-range management.
//!
//! This manager handles three responsibilities:
//! - conservative meta-range autosplitting
//! - staged meta replica reconfiguration progress
//! - conservative meta replica/lease balancing

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

use holo_accord::accord::{ExecutedPrefix, NodeId};

use crate::cluster::{ClusterCommand, ClusterState, MemberState, MetaRangeDesc, ReplicaMove, ReplicaMovePhase};
use crate::NodeState;

/// Configuration for metadata-range autosplitting and balancing.
#[derive(Clone, Copy, Debug)]
pub struct MetaManagerConfig {
    /// Enable/disable automatic meta-range splitting.
    pub enable_split: bool,
    /// Enable/disable automatic meta-range balancing.
    pub enable_balance: bool,
    /// Evaluation interval.
    pub interval: Duration,
    /// Minimum sustained routed meta-ops QPS to consider splitting.
    pub split_min_qps: u64,
    /// Minimum cumulative routed meta-ops before splitting.
    pub split_min_ops: u64,
    /// Consecutive intervals above threshold required to split.
    pub split_qps_sustain: u8,
    /// Cooldown between splits for the same meta index.
    pub split_cooldown: Duration,
    /// Upper bound on available meta groups.
    pub max_meta_groups: usize,
    /// Replica skew required to trigger a move.
    pub replica_skew_threshold: usize,
    /// Leaseholder skew required to trigger lease balancing.
    pub lease_skew_threshold: usize,
    /// Cooldown between automatic balancing actions on the same meta range.
    pub balance_cooldown: Duration,
    /// Warn when an in-flight meta move is idle longer than this.
    pub move_stuck_warn: Duration,
    /// Warn when per-meta-group executed-prefix lag exceeds this.
    pub lag_warn_ops: u64,
    /// Abort/force-finalize an in-flight meta move that has stalled this long.
    pub move_timeout: Duration,
    /// Admission control: skip meta manager proposals above this client inflight count.
    pub max_client_inflight: u64,
}

pub fn spawn(state: Arc<NodeState>, cfg: MetaManagerConfig) {
    if (!cfg.enable_split && !cfg.enable_balance) || cfg.max_meta_groups == 0 {
        return;
    }

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(cfg.interval);
        let mut last_totals: BTreeMap<usize, u64> = BTreeMap::new();
        let mut sustained: BTreeMap<usize, u8> = BTreeMap::new();
        let mut split_cooldown_until: BTreeMap<usize, Instant> = BTreeMap::new();
        let mut balance_cooldown_until: BTreeMap<u64, Instant> = BTreeMap::new();
        let mut last_warn: BTreeMap<String, Instant> = BTreeMap::new();

        loop {
            ticker.tick().await;

            if state.cluster_store.frozen() {
                continue;
            }
            if state.client_inflight_ops() > cfg.max_client_inflight {
                tracing::debug!(
                    inflight = state.client_inflight_ops(),
                    max_inflight = cfg.max_client_inflight,
                    "meta manager skipping reconcile under high client load"
                );
                continue;
            }

            if let Err(err) = maybe_reconcile_once(
                state.clone(),
                cfg,
                &mut last_totals,
                &mut sustained,
                &mut split_cooldown_until,
                &mut balance_cooldown_until,
                &mut last_warn,
            )
            .await
            {
                tracing::warn!(error = ?err, "meta manager reconcile failed");
            }
        }
    });
}

#[allow(clippy::too_many_arguments)]
async fn maybe_reconcile_once(
    state: Arc<NodeState>,
    cfg: MetaManagerConfig,
    last_totals: &mut BTreeMap<usize, u64>,
    sustained: &mut BTreeMap<usize, u8>,
    split_cooldown_until: &mut BTreeMap<usize, Instant>,
    balance_cooldown_until: &mut BTreeMap<u64, Instant>,
    last_warn: &mut BTreeMap<String, Instant>,
) -> anyhow::Result<()> {
    maybe_warn_meta_slo(state.clone(), cfg, last_warn).await?;

    if let Some(cmd) = plan_stalled_meta_move_step(
        &state.cluster_store.state(),
        state.node_id,
        cfg.move_timeout,
    ) {
        state.propose_meta_command(cmd).await?;
        return Ok(());
    }

    if let Some(cmd) = plan_inflight_meta_move_step(state.clone()).await? {
        state.propose_meta_command(cmd).await?;
        return Ok(());
    }

    if cfg.enable_split {
        if let Some(cmd) = plan_meta_split(
            state.clone(),
            cfg,
            last_totals,
            sustained,
            split_cooldown_until,
        ) {
            state.propose_meta_command(cmd).await?;
            return Ok(());
        }
    }

    if cfg.enable_balance {
        if let Some(cmd) = plan_meta_balance_step(state, cfg, balance_cooldown_until).await? {
            // Don't await here from moved state in the callee path.
            return Ok(cmd);
        }
    }

    Ok(())
}

fn plan_meta_split(
    state: Arc<NodeState>,
    cfg: MetaManagerConfig,
    last_totals: &mut BTreeMap<usize, u64>,
    sustained: &mut BTreeMap<usize, u8>,
    cooldown_until: &mut BTreeMap<usize, Instant>,
) -> Option<ClusterCommand> {
    let snapshot = state.cluster_store.state();
    // Keep meta splits conservative while data-plane move/merge work is active.
    if !snapshot.shard_rebalances.is_empty() || !snapshot.shard_merges.is_empty() {
        return None;
    }
    if !snapshot.meta_rebalances.is_empty() {
        return None;
    }
    let target_meta_index = state
        .cluster_store
        .first_free_meta_index(cfg.max_meta_groups)?;

    let totals = state.cluster_store.meta_ops_total_by_index();
    let secs = cfg.interval.as_secs_f64().max(0.001);
    let now = Instant::now();

    let mut qps_by_index = BTreeMap::<usize, u64>::new();
    for (idx, cur) in &totals {
        let prev = last_totals.get(idx).copied().unwrap_or(0);
        let delta = cur.saturating_sub(prev);
        let qps = ((delta as f64) / secs) as u64;
        qps_by_index.insert(*idx, qps);
    }
    *last_totals = totals.clone();

    cooldown_until.retain(|_, until| *until > now);

    let mut best: Option<(u64, usize)> = None;
    for range in &snapshot.meta_ranges {
        // Decentralized orchestration: each meta range is managed by its
        // current leaseholder instead of a single global controller.
        if range.leaseholder != state.node_id {
            continue;
        }
        let idx = range.meta_index;
        let qps = qps_by_index.get(&idx).copied().unwrap_or(0);
        let total = totals.get(&idx).copied().unwrap_or(0);
        let split_possible = range.start_hash < range.end_hash;
        let cooling = cooldown_until.get(&idx).map(|d| *d > now).unwrap_or(false);

        if split_possible && !cooling && qps >= cfg.split_min_qps && total >= cfg.split_min_ops {
            let streak = sustained.entry(idx).or_insert(0);
            *streak = streak.saturating_add(1);
        } else {
            sustained.remove(&idx);
            continue;
        }

        if sustained.get(&idx).copied().unwrap_or(0) < cfg.split_qps_sustain.max(1) {
            continue;
        }

        match best {
            Some((best_qps, _)) if best_qps >= qps => {}
            _ => best = Some((qps, idx)),
        }
    }

    let (_, source_idx) = best?;
    let source = snapshot
        .meta_ranges
        .iter()
        .find(|r| r.meta_index == source_idx)?
        .clone();
    if source.start_hash >= source.end_hash {
        return None;
    }
    let split_hash = source.start_hash + ((source.end_hash - source.start_hash) / 2) + 1;

    let deadline = now + cfg.split_cooldown;
    cooldown_until.insert(source_idx, deadline);
    cooldown_until.insert(target_meta_index, deadline);
    sustained.remove(&source_idx);

    tracing::info!(
        source_meta_index = source_idx,
        target_meta_index,
        split_hash,
        "meta manager proposing meta-range split"
    );

    Some(ClusterCommand::SplitMetaRange {
        split_hash,
        target_meta_index,
    })
}

async fn plan_inflight_meta_move_step(state: Arc<NodeState>) -> anyhow::Result<Option<ClusterCommand>> {
    let snapshot = state.cluster_store.state();
    for (&meta_range_id, mv) in &snapshot.meta_rebalances {
        let Some(range) = snapshot
            .meta_ranges
            .iter()
            .find(|r| r.meta_range_id == meta_range_id)
            .cloned()
        else {
            return Ok(Some(ClusterCommand::AbortMetaReplicaMove { meta_range_id }));
        };

        // Let only the range leaseholder drive this move's phase transitions.
        if range.leaseholder != state.node_id {
            continue;
        }

        match mv.phase {
            ReplicaMovePhase::LearnerSync => {
                if !meta_membership_matches(state.clone(), &snapshot, &range) {
                    continue;
                }
                if !seed_meta_learner_prefixes_if_needed(
                    state.clone(),
                    &range,
                    mv.from_node,
                    mv.to_node,
                )
                .await?
                {
                    continue;
                }
                if !meta_prefixes_converged(state.clone(), &range).await? {
                    continue;
                }
                return Ok(Some(ClusterCommand::PromoteMetaReplicaLearner { meta_range_id }));
            }
            ReplicaMovePhase::JointConfig => {
                let target = resolve_target_meta_leaseholder(&range, mv).unwrap_or(mv.to_node);
                return Ok(Some(ClusterCommand::TransferMetaRangeLease {
                    meta_range_id,
                    leaseholder: target,
                }));
            }
            ReplicaMovePhase::LeaseTransferred => {
                return Ok(Some(ClusterCommand::FinalizeMetaReplicaMove { meta_range_id }));
            }
        }
    }
    Ok(None)
}

async fn plan_meta_balance_step(
    state: Arc<NodeState>,
    cfg: MetaManagerConfig,
    cooldown_until: &mut BTreeMap<u64, Instant>,
) -> anyhow::Result<Option<()>> {
    let snapshot = state.cluster_store.state();
    if !snapshot.meta_rebalances.is_empty() {
        return Ok(None);
    }
    if snapshot.meta_ranges.is_empty() {
        return Ok(None);
    }
    // Keep balancing conservative while data-range workflows are active.
    if !snapshot.shard_rebalances.is_empty() || !snapshot.shard_merges.is_empty() {
        return Ok(None);
    }

    let mut active = snapshot
        .members
        .iter()
        .filter_map(|(id, m)| (m.state == MemberState::Active).then_some(*id))
        .collect::<Vec<_>>();
    if active.len() < 2 {
        return Ok(None);
    }
    active.sort_unstable();
    cooldown_until.retain(|_, until| *until > Instant::now());

    let mut replica_counts = BTreeMap::<NodeId, usize>::new();
    let mut lease_counts = BTreeMap::<NodeId, usize>::new();
    for id in &active {
        replica_counts.insert(*id, 0);
        lease_counts.insert(*id, 0);
    }
    for range in &snapshot.meta_ranges {
        for node in &range.replicas {
            if let Some(v) = replica_counts.get_mut(node) {
                *v += 1;
            }
        }
        if let Some(v) = lease_counts.get_mut(&range.leaseholder) {
            *v += 1;
        }
    }

    // First priority: drain metadata replicas from non-active nodes
    // (Decommissioning/Removed) with staged replacement, mirroring data ranges.
    for range in &snapshot.meta_ranges {
        if range.leaseholder != state.node_id {
            continue;
        }
        if cooldown_until
            .get(&range.meta_range_id)
            .map(|d| *d > Instant::now())
            .unwrap_or(false)
        {
            continue;
        }
        let mut donor = None;
        for replica in &range.replicas {
            let state = snapshot
                .members
                .get(replica)
                .map(|m| m.state)
                .unwrap_or(MemberState::Removed);
            if state != MemberState::Active {
                donor = Some(*replica);
                break;
            }
        }
        let Some(donor) = donor else {
            continue;
        };
        let Some((&receiver, _)) = replica_counts
            .iter()
            .filter(|(node, _)| !range.replicas.contains(node))
            .min_by_key(|(_, count)| **count)
        else {
            continue;
        };

        let mut desired = range.replicas.clone();
        desired.retain(|id| *id != donor);
        desired.push(receiver);
        let target_leaseholder = if range.leaseholder == donor {
            Some(receiver)
        } else {
            None
        };
        let planned = state.cluster_store.plan_meta_rebalance_command(
            range.meta_range_id,
            desired,
            target_leaseholder,
        )?;
        if let Some(cmd) = planned {
            cooldown_until.insert(range.meta_range_id, Instant::now() + cfg.balance_cooldown);
            state.propose_meta_command(cmd).await?;
            return Ok(Some(()));
        }
    }

    if let Some((donor, receiver)) =
        skew_pair(&replica_counts, cfg.replica_skew_threshold.max(1))
    {
        for range in &snapshot.meta_ranges {
            if range.leaseholder != state.node_id {
                continue;
            }
            if cooldown_until
                .get(&range.meta_range_id)
                .map(|d| *d > Instant::now())
                .unwrap_or(false)
            {
                continue;
            }
            if !range.replicas.contains(&donor) || range.replicas.contains(&receiver) {
                continue;
            }
            let mut desired = range.replicas.clone();
            desired.retain(|id| *id != donor);
            desired.push(receiver);
            let target_leaseholder = if range.leaseholder == donor {
                Some(receiver)
            } else {
                None
            };
            let planned = state
                .cluster_store
                .plan_meta_rebalance_command(range.meta_range_id, desired, target_leaseholder)?;
            if let Some(cmd) = planned {
                cooldown_until.insert(range.meta_range_id, Instant::now() + cfg.balance_cooldown);
                state.propose_meta_command(cmd).await?;
                return Ok(Some(()));
            }
        }
    }

    if let Some((donor, receiver)) = skew_pair(&lease_counts, cfg.lease_skew_threshold.max(1)) {
        for range in &snapshot.meta_ranges {
            if range.leaseholder != state.node_id {
                continue;
            }
            if cooldown_until
                .get(&range.meta_range_id)
                .map(|d| *d > Instant::now())
                .unwrap_or(false)
            {
                continue;
            }
            if range.leaseholder != donor || !range.replicas.contains(&receiver) {
                continue;
            }
            cooldown_until.insert(range.meta_range_id, Instant::now() + cfg.balance_cooldown);
            state
                .propose_meta_command(ClusterCommand::TransferMetaRangeLease {
                    meta_range_id: range.meta_range_id,
                    leaseholder: receiver,
                })
                .await?;
            return Ok(Some(()));
        }
    }

    Ok(None)
}

async fn maybe_warn_meta_slo(
    state: Arc<NodeState>,
    cfg: MetaManagerConfig,
    last_warn: &mut BTreeMap<String, Instant>,
) -> anyhow::Result<()> {
    let snapshot = state.cluster_store.state();
    let now = crate::unix_time_ms();
    let warn_interval = cfg.interval.max(Duration::from_secs(5));

    for (meta_range_id, mv) in &snapshot.meta_rebalances {
        let last_progress = mv.last_progress_unix_ms.max(mv.started_unix_ms);
        if last_progress == 0 {
            continue;
        }
        let idle_ms = now.saturating_sub(last_progress);
        let threshold_ms = cfg
            .move_stuck_warn
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        if idle_ms >= threshold_ms {
            let key = format!("stuck-meta-move-{meta_range_id}");
            if should_warn(last_warn, &key, warn_interval) {
                tracing::warn!(
                    meta_range_id,
                    from = mv.from_node,
                    to = mv.to_node,
                    phase = ?mv.phase,
                    idle_ms,
                    threshold_ms,
                    "meta move appears stuck"
                );
            }
        }
    }

    for range in &snapshot.meta_ranges {
        let lag = meta_group_prefix_lag(state.clone(), range).await?;
        if lag >= cfg.lag_warn_ops {
            let key = format!("meta-lag-{}", range.meta_index);
            if should_warn(last_warn, &key, warn_interval) {
                tracing::warn!(
                    meta_range_id = range.meta_range_id,
                    meta_index = range.meta_index,
                    lag,
                    threshold = cfg.lag_warn_ops,
                    "meta group executed-prefix lag above threshold"
                );
            }
        }
    }

    Ok(())
}

fn should_warn(last_warn: &mut BTreeMap<String, Instant>, key: &str, interval: Duration) -> bool {
    let now = Instant::now();
    let should = last_warn
        .get(key)
        .map(|ts| now.duration_since(*ts) >= interval)
        .unwrap_or(true);
    if should {
        last_warn.insert(key.to_string(), now);
    }
    should
}

fn plan_stalled_meta_move_step(
    snapshot: &ClusterState,
    node_id: NodeId,
    move_timeout: Duration,
) -> Option<ClusterCommand> {
    if move_timeout.is_zero() {
        return None;
    }
    let timeout_ms = move_timeout.as_millis().min(u128::from(u64::MAX)) as u64;
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0);
    let mut stalled: Option<(u64, u64, ReplicaMove)> = None;
    for (&meta_range_id, mv) in &snapshot.meta_rebalances {
        let Some(range) = snapshot
            .meta_ranges
            .iter()
            .find(|r| r.meta_range_id == meta_range_id)
        else {
            continue;
        };
        if range.leaseholder != node_id {
            continue;
        }
        let last_progress = mv.last_progress_unix_ms.max(mv.started_unix_ms);
        if last_progress == 0 {
            continue;
        }
        let idle_ms = now_ms.saturating_sub(last_progress);
        if idle_ms < timeout_ms {
            continue;
        }
        let should_replace = stalled
            .as_ref()
            .map(|(_, best_idle_ms, _)| idle_ms > *best_idle_ms)
            .unwrap_or(true);
        if should_replace {
            stalled = Some((meta_range_id, idle_ms, mv.clone()));
        }
    }
    let (meta_range_id, idle_ms, mv) = stalled?;

    match mv.phase {
        ReplicaMovePhase::LeaseTransferred => {
            tracing::warn!(
                meta_range_id,
                from = mv.from_node,
                to = mv.to_node,
                phase = ?mv.phase,
                idle_ms,
                timeout_ms,
                "forcing finalize for stalled meta replica move"
            );
            Some(ClusterCommand::FinalizeMetaReplicaMove { meta_range_id })
        }
        ReplicaMovePhase::LearnerSync | ReplicaMovePhase::JointConfig => {
            tracing::warn!(
                meta_range_id,
                from = mv.from_node,
                to = mv.to_node,
                phase = ?mv.phase,
                idle_ms,
                timeout_ms,
                "aborting stalled meta replica move"
            );
            Some(ClusterCommand::AbortMetaReplicaMove { meta_range_id })
        }
    }
}

fn skew_pair(counts: &BTreeMap<NodeId, usize>, threshold: usize) -> Option<(NodeId, NodeId)> {
    let (&max_node, &max_count) = counts.iter().max_by_key(|(_, c)| *c)?;
    let (&min_node, &min_count) = counts.iter().min_by_key(|(_, c)| *c)?;
    (max_count > min_count.saturating_add(threshold)).then_some((max_node, min_node))
}

fn resolve_target_meta_leaseholder(range: &MetaRangeDesc, mv: &ReplicaMove) -> Option<NodeId> {
    if let Some(target) = mv.target_leaseholder {
        return Some(target);
    }
    if range.leaseholder != mv.from_node {
        return Some(range.leaseholder);
    }
    range
        .replicas
        .iter()
        .copied()
        .find(|id| *id != mv.from_node)
}

async fn seed_meta_learner_prefixes_if_needed(
    state: Arc<NodeState>,
    range: &MetaRangeDesc,
    source: NodeId,
    target: NodeId,
) -> anyhow::Result<bool> {
    let gid = crate::meta_group_id_for_index(range.meta_index);
    let source_prefixes = state.transport.last_executed_prefix(source, gid).await?;
    if source_prefixes.is_empty() {
        return Ok(true);
    }

    let target_prefixes = state.transport.last_executed_prefix(target, gid).await?;
    let needs_seed = source_prefixes.iter().any(|src| {
        prefix_counter(&target_prefixes, src.node_id) < src.counter
    });
    if !needs_seed {
        return Ok(true);
    }

    state
        .transport
        .seed_executed_prefix(target, gid, &source_prefixes)
        .await?;
    tracing::info!(
        meta_range_id = range.meta_range_id,
        meta_index = range.meta_index,
        source,
        target,
        seeded_prefixes = source_prefixes.len(),
        "seeded meta learner executed prefixes"
    );
    Ok(true)
}

fn meta_membership_matches(state: Arc<NodeState>, snapshot: &ClusterState, range: &MetaRangeDesc) -> bool {
    let gid = crate::meta_group_id_for_index(range.meta_index);
    let Some(group) = state.group(gid) else {
        return false;
    };
    let (members, voters) = crate::meta_membership_sets(snapshot, range);

    let mut current_members = group.members().into_iter().map(|m| m.id).collect::<Vec<_>>();
    current_members.sort_unstable();
    current_members.dedup();
    let mut current_voters = group.voters();
    current_voters.sort_unstable();
    current_voters.dedup();

    current_members == members && current_voters == voters
}

async fn meta_prefixes_converged(state: Arc<NodeState>, range: &MetaRangeDesc) -> anyhow::Result<bool> {
    let gid = crate::meta_group_id_for_index(range.meta_index);
    let mut snapshots = Vec::new();
    for node in &range.replicas {
        let prefixes = match state.transport.last_executed_prefix(*node, gid).await {
            Ok(p) => p,
            Err(err) => {
                tracing::debug!(
                    node,
                    group_id = gid,
                    error = ?err,
                    "failed to read meta executed prefixes for convergence check"
                );
                return Ok(false);
            }
        };
        snapshots.push(normalize_prefixes(prefixes));
    }
    if snapshots.is_empty() {
        return Ok(false);
    }
    let first = snapshots[0].clone();
    Ok(snapshots.into_iter().all(|s| s == first))
}

async fn meta_group_prefix_lag(state: Arc<NodeState>, range: &MetaRangeDesc) -> anyhow::Result<u64> {
    let gid = crate::meta_group_id_for_index(range.meta_index);
    let mut by_prefix = BTreeMap::<NodeId, (u64, u64)>::new();
    for node in &range.replicas {
        let prefixes = match state.transport.last_executed_prefix(*node, gid).await {
            Ok(p) => p,
            Err(_) => return Ok(0),
        };
        for p in prefixes {
            let entry = by_prefix.entry(p.node_id).or_insert((p.counter, p.counter));
            entry.0 = entry.0.min(p.counter);
            entry.1 = entry.1.max(p.counter);
        }
    }
    let mut lag = 0u64;
    for (_, (min_c, max_c)) in by_prefix {
        lag = lag.saturating_add(max_c.saturating_sub(min_c));
    }
    Ok(lag)
}

fn normalize_prefixes(prefixes: Vec<ExecutedPrefix>) -> Vec<(NodeId, u64)> {
    let mut out = prefixes
        .into_iter()
        .map(|p| (p.node_id, p.counter))
        .collect::<Vec<_>>();
    out.sort_unstable_by_key(|(node_id, counter)| (*node_id, *counter));
    out
}

fn prefix_counter(prefixes: &[ExecutedPrefix], node_id: NodeId) -> u64 {
    prefixes
        .iter()
        .find(|p| p.node_id == node_id)
        .map(|p| p.counter)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{
        ClusterCommand, ClusterState, MemberInfo, MemberState, MetaRangeDesc, ReplicaMove,
        ReplicaMovePhase,
    };
    use std::collections::BTreeMap;

    fn member(node_id: NodeId) -> MemberInfo {
        MemberInfo {
            node_id,
            grpc_addr: format!("127.0.0.1:{}", 15050 + node_id),
            redis_addr: format!("127.0.0.1:{}", 16378 + node_id),
            state: MemberState::Active,
        }
    }

    fn base_state_with_meta_move(phase: ReplicaMovePhase, idle_ms: u64) -> ClusterState {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
            .unwrap_or(0);
        let mut members = BTreeMap::new();
        members.insert(1, member(1));
        members.insert(2, member(2));
        members.insert(3, member(3));
        ClusterState {
            epoch: 1,
            frozen: false,
            replication_factor: 3,
            members,
            shards: Vec::new(),
            shard_replica_roles: BTreeMap::new(),
            shard_rebalances: BTreeMap::new(),
            shard_merges: BTreeMap::new(),
            shard_fences: BTreeMap::new(),
            retired_ranges: BTreeMap::new(),
            meta_ranges: vec![MetaRangeDesc {
                meta_range_id: 1,
                meta_index: 0,
                start_hash: 0,
                end_hash: u64::MAX,
                replicas: vec![1, 2, 3],
                leaseholder: 1,
            }],
            meta_replica_roles: BTreeMap::new(),
            meta_rebalances: BTreeMap::from([(
                1,
                ReplicaMove {
                    from_node: 1,
                    to_node: 3,
                    phase,
                    backfill_done: false,
                    target_leaseholder: Some(3),
                    started_unix_ms: now.saturating_sub(idle_ms.saturating_add(1)),
                    last_progress_unix_ms: now.saturating_sub(idle_ms),
                },
            )]),
            controller_leases: BTreeMap::new(),
            meta_controller_lease: None,
        }
    }

    #[test]
    fn stalled_meta_move_aborts_before_lease_transfer() {
        let state = base_state_with_meta_move(ReplicaMovePhase::LearnerSync, 120_000);
        let cmd = plan_stalled_meta_move_step(&state, 1, Duration::from_secs(30))
            .expect("expected stalled command");
        match cmd {
            ClusterCommand::AbortMetaReplicaMove { meta_range_id } => assert_eq!(meta_range_id, 1),
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn stalled_meta_move_force_finalizes_after_lease_transfer() {
        let state = base_state_with_meta_move(ReplicaMovePhase::LeaseTransferred, 120_000);
        let cmd = plan_stalled_meta_move_step(&state, 1, Duration::from_secs(30))
            .expect("expected stalled command");
        match cmd {
            ClusterCommand::FinalizeMetaReplicaMove { meta_range_id } => assert_eq!(meta_range_id, 1),
            other => panic!("unexpected command: {other:?}"),
        }
    }
}
