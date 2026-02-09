//! Background replica rebalancing and node decommission orchestration.
//!
//! Reconfiguration is staged:
//! 1. add target as learner (`BeginReplicaMove`)
//! 2. copy latest-visible KV rows from source to learner (`RangeSnapshotLatest` / `RangeApplyLatest`)
//! 3. wait for learner catch-up (`last_executed_prefix`)
//! 4. promote learner to joint config (`PromoteReplicaLearner`)
//! 5. transfer lease away from outgoing replica (`TransferShardLease`)
//! 6. cut over and remove outgoing (`FinalizeReplicaMove`)

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use holo_accord::accord::{ExecutedPrefix, NodeId};

use crate::cluster::{
    ClusterCommand, ClusterState, MemberState, ReplicaMove, ReplicaMovePhase, ReplicaRole, ShardDesc,
};
use crate::NodeState;

/// Configuration for the background rebalancer.
#[derive(Clone, Copy, Debug)]
pub struct RebalanceManagerConfig {
    /// Evaluate and apply at most one metadata change at this interval.
    pub interval: Duration,
    /// Abort/force-complete an in-flight move that has made no persisted
    /// progress for longer than this duration. Set to 0 to disable.
    pub move_timeout: Duration,
}

/// Spawn the background rebalancer.
pub fn spawn(state: Arc<NodeState>, cfg: RebalanceManagerConfig) {
    // Keep a single controller to avoid competing proposals.
    if state.node_id != 1 {
        return;
    }

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(cfg.interval);
        loop {
            ticker.tick().await;
            if state.cluster_store.frozen() {
                continue;
            }
            if let Err(err) = reconcile_once(state.clone(), cfg).await {
                tracing::warn!(error = ?err, "rebalance manager reconcile failed");
            }
        }
    });
}

async fn reconcile_once(state: Arc<NodeState>, cfg: RebalanceManagerConfig) -> anyhow::Result<()> {
    let snapshot = state.cluster_store.state();

    if let Some(cmd) = plan_stalled_move_step(&snapshot, cfg.move_timeout) {
        propose_meta(state, cmd).await?;
        return Ok(());
    }

    if let Some(cmd) = plan_inflight_move_step(state.clone(), &snapshot).await? {
        propose_meta(state, cmd).await?;
        return Ok(());
    }

    if let Some(cmd) = plan_decommission_step(&snapshot) {
        propose_meta(state, cmd).await?;
        return Ok(());
    }

    if let Some(cmd) = plan_replica_balance_step(&snapshot) {
        propose_meta(state, cmd).await?;
        return Ok(());
    }

    if let Some(cmd) = plan_lease_balance_step(&snapshot) {
        propose_meta(state, cmd).await?;
    }

    Ok(())
}

fn plan_stalled_move_step(state: &ClusterState, move_timeout: Duration) -> Option<ClusterCommand> {
    if move_timeout.is_zero() {
        return None;
    }
    let timeout_ms = move_timeout.as_millis().min(u128::from(u64::MAX)) as u64;
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0);
    let (&shard_id, mv) = state.shard_rebalances.iter().next()?;
    let last_progress = mv.last_progress_unix_ms.max(mv.started_unix_ms);
    if last_progress == 0 {
        return None;
    }
    let idle_ms = now_ms.saturating_sub(last_progress);
    if idle_ms < timeout_ms {
        return None;
    }

    match mv.phase {
        // Once lease moved, force completion rather than rollback.
        ReplicaMovePhase::LeaseTransferred => {
            tracing::warn!(
                shard_id,
                from = mv.from_node,
                to = mv.to_node,
                phase = ?mv.phase,
                idle_ms,
                timeout_ms,
                "forcing finalize for stalled replica move"
            );
            Some(ClusterCommand::FinalizeReplicaMove { shard_id })
        }
        ReplicaMovePhase::LearnerSync | ReplicaMovePhase::JointConfig => {
            tracing::warn!(
                shard_id,
                from = mv.from_node,
                to = mv.to_node,
                phase = ?mv.phase,
                idle_ms,
                timeout_ms,
                "aborting stalled replica move"
            );
            Some(ClusterCommand::AbortReplicaMove { shard_id })
        }
    }
}

async fn propose_meta(state: Arc<NodeState>, cmd: ClusterCommand) -> anyhow::Result<()> {
    let payload = crate::cluster::ClusterStateMachine::encode_command(&cmd)?;
    let _ = state.meta_handle.propose(payload).await?;
    Ok(())
}

async fn plan_inflight_move_step(
    state: Arc<NodeState>,
    snapshot: &ClusterState,
) -> anyhow::Result<Option<ClusterCommand>> {
    let Some((&shard_id, mv)) = snapshot.shard_rebalances.iter().next() else {
        return Ok(None);
    };
    let Some(shard) = snapshot.shards.iter().find(|s| s.shard_id == shard_id) else {
        return Ok(None);
    };

    match mv.phase {
        ReplicaMovePhase::LearnerSync => {
            let learner_members = shard.replicas.clone();
            let learner_voters = voters_for_shard(snapshot, shard.shard_id, &learner_members);
            if !ensure_data_group_membership(
                state.clone(),
                shard,
                learner_members,
                learner_voters,
                "learner-sync",
            )
            .await?
            {
                tracing::debug!(
                    shard_id = shard.shard_id,
                    shard_index = shard.shard_index,
                    "waiting for learner-sync membership convergence"
                );
                return Ok(None);
            }
            if !target_has_learner_sync_state(
                state.clone(),
                snapshot.epoch,
                shard.shard_id,
                mv,
            )
            .await
            {
                tracing::debug!(
                    shard_id = shard.shard_id,
                    target = mv.to_node,
                    min_epoch = snapshot.epoch,
                    "waiting for target learner-sync metadata state"
                );
                return Ok(None);
            }
            if !mv.backfill_done {
                if backfill_learner_replica(state.clone(), shard, mv.from_node, mv.to_node).await? {
                    return Ok(Some(ClusterCommand::MarkReplicaMoveBackfilled { shard_id }));
                }
                return Ok(None);
            }
            let caught_up =
                learner_caught_up(state.clone(), shard, mv.from_node, mv.to_node).await?;
            if caught_up {
                let mut joint_voters = shard.replicas.clone();
                joint_voters.sort_unstable();
                joint_voters.dedup();
                if !ensure_data_group_membership(
                    state.clone(),
                    shard,
                    shard.replicas.clone(),
                    joint_voters,
                    "joint-config",
                )
                .await?
                {
                    return Ok(None);
                }
                Ok(Some(ClusterCommand::PromoteReplicaLearner { shard_id }))
            } else {
                tracing::debug!(
                    shard_id = shard.shard_id,
                    source = mv.from_node,
                    target = mv.to_node,
                    "waiting for learner catch-up"
                );
                Ok(None)
            }
        }
        ReplicaMovePhase::JointConfig => {
            let joint_members = shard.replicas.clone();
            let joint_voters = voters_for_shard(snapshot, shard.shard_id, &joint_members);
            if !ensure_data_group_membership(
                state.clone(),
                shard,
                joint_members,
                joint_voters,
                "joint-config",
            )
            .await?
            {
                return Ok(None);
            }
            if !target_has_joint_config_state(
                state.clone(),
                snapshot.epoch,
                shard.shard_id,
                mv,
            )
            .await
            {
                return Ok(None);
            }
            let requested = mv
                .target_leaseholder
                .filter(|id| *id != mv.from_node && shard.replicas.contains(id));
            let desired_leaseholder = requested.unwrap_or(mv.to_node);
            if shard.leaseholder != desired_leaseholder {
                Ok(Some(ClusterCommand::TransferShardLease {
                    shard_id,
                    leaseholder: desired_leaseholder,
                }))
            } else {
                if !target_has_lease_state(
                    state.clone(),
                    snapshot.epoch,
                    shard.shard_id,
                    mv,
                    desired_leaseholder,
                    false,
                )
                .await
                {
                    return Ok(None);
                }
                let mut stable_members = shard.replicas.clone();
                stable_members.retain(|id| *id != mv.from_node);
                stable_members.sort_unstable();
                stable_members.dedup();
                if stable_members.is_empty() {
                    return Ok(None);
                }
                if !ensure_data_group_membership(
                    state.clone(),
                    shard,
                    stable_members.clone(),
                    stable_members,
                    "finalize-cutover",
                )
                .await?
                {
                    return Ok(None);
                }
                Ok(Some(ClusterCommand::FinalizeReplicaMove { shard_id }))
            }
        }
        ReplicaMovePhase::LeaseTransferred => {
            let requested = mv
                .target_leaseholder
                .filter(|id| *id != mv.from_node && shard.replicas.contains(id));
            let desired_leaseholder = requested.unwrap_or(mv.to_node);
            if !target_has_lease_state(
                state.clone(),
                snapshot.epoch,
                shard.shard_id,
                mv,
                desired_leaseholder,
                true,
            )
            .await
            {
                return Ok(None);
            }
            let mut stable_members = shard.replicas.clone();
            stable_members.retain(|id| *id != mv.from_node);
            stable_members.sort_unstable();
            stable_members.dedup();
            if stable_members.is_empty() {
                return Ok(None);
            }
            if !ensure_data_group_membership(
                state.clone(),
                shard,
                stable_members.clone(),
                stable_members,
                "finalize-cutover",
            )
            .await?
            {
                return Ok(None);
            }
            Ok(Some(ClusterCommand::FinalizeReplicaMove { shard_id }))
        }
    }
}

fn voters_for_shard(state: &ClusterState, shard_id: u64, members: &[NodeId]) -> Vec<NodeId> {
    let mut voters = if let Some(roles) = state.shard_replica_roles.get(&shard_id) {
        members
            .iter()
            .copied()
            .filter(|id| {
                roles.get(id).copied().unwrap_or(ReplicaRole::Voter) != ReplicaRole::Learner
            })
            .collect::<Vec<_>>()
    } else {
        members.to_vec()
    };
    if voters.is_empty() {
        voters = members.to_vec();
    }
    voters.sort_unstable();
    voters.dedup();
    voters
}

async fn ensure_data_group_membership(
    state: Arc<NodeState>,
    shard: &ShardDesc,
    mut members: Vec<NodeId>,
    mut voters: Vec<NodeId>,
    stage: &'static str,
) -> anyhow::Result<bool> {
    members.sort_unstable();
    members.dedup();
    voters.sort_unstable();
    voters.dedup();
    if voters.is_empty() {
        anyhow::bail!("cannot apply empty voter set for shard {}", shard.shard_id);
    }
    if voters.iter().any(|id| !members.contains(id)) {
        anyhow::bail!(
            "voter set for shard {} must be subset of members",
            shard.shard_id
        );
    }

    if state.shard_membership_matches(shard.shard_index, &members, &voters) {
        return Ok(true);
    }

    if let Err(err) = state
        .propose_shard_membership_reconfig(shard.shard_index, &members, &voters)
        .await
    {
        tracing::debug!(
            shard_id = shard.shard_id,
            shard_index = shard.shard_index,
            stage,
            error = ?err,
            "data-plane shard membership proposal did not commit yet"
        );
        return Ok(false);
    }

    let matched = state.shard_membership_matches(shard.shard_index, &members, &voters);
    if !matched {
        tracing::debug!(
            shard_id = shard.shard_id,
            shard_index = shard.shard_index,
            stage,
            "waiting for local shard membership view to converge"
        );
    }
    Ok(matched)
}

async fn backfill_learner_replica(
    state: Arc<NodeState>,
    shard: &ShardDesc,
    source: NodeId,
    target: NodeId,
) -> anyhow::Result<bool> {
    const PAGE_LIMIT: usize = 2_000;
    const MAX_PAGES: usize = 1_000_000;

    let mut cursor = Vec::new();
    let mut pages = 0usize;
    let mut applied_total = 0u64;

    loop {
        if pages >= MAX_PAGES {
            anyhow::bail!(
                "backfill exceeded page limit for shard {} (possible cursor stall)",
                shard.shard_id
            );
        }
        pages += 1;
        let (entries, next_cursor, done) = state
            .transport
            .range_snapshot_latest(
                source,
                shard.shard_index,
                &shard.start_key,
                &shard.end_key,
                &cursor,
                PAGE_LIMIT,
            )
            .await?;
        if !entries.is_empty() {
            let applied = state
                .transport
                .range_apply_latest(
                    target,
                    shard.shard_index,
                    &shard.start_key,
                    &shard.end_key,
                    entries,
                )
                .await?;
            applied_total = applied_total.saturating_add(applied);
        }
        if done {
            // Backfill copies historical KV state out-of-band; seed executed-prefix
            // floors so future per-origin counters can advance contiguously on the
            // learner without replaying all historical commands.
            let group_id = crate::GROUP_DATA_BASE + shard.shard_index as u64;
            let source_prefixes = state
                .transport
                .last_executed_prefix(source, group_id)
                .await?;
            state
                .transport
                .seed_executed_prefix(target, group_id, &source_prefixes)
                .await?;
            tracing::info!(
                shard_id = shard.shard_id,
                shard_index = shard.shard_index,
                source,
                target,
                pages,
                applied = applied_total,
                seeded_prefixes = source_prefixes.len(),
                "learner backfill complete"
            );
            return Ok(true);
        }
        if next_cursor == cursor {
            anyhow::bail!(
                "backfill cursor stalled for shard {} (source={}, target={})",
                shard.shard_id,
                source,
                target
            );
        }
        cursor = next_cursor;
    }
}

async fn fetch_target_snapshot(state: Arc<NodeState>, target: NodeId) -> Option<ClusterState> {
    match state.transport.cluster_state(target).await {
        Ok(snapshot) => Some(snapshot),
        Err(err) => {
            tracing::debug!(target, error = ?err, "failed to fetch target cluster_state");
            None
        }
    }
}

fn shard_role(state: &ClusterState, shard_id: u64, node_id: NodeId) -> Option<ReplicaRole> {
    state
        .shard_replica_roles
        .get(&shard_id)
        .and_then(|roles| roles.get(&node_id).copied())
}

async fn target_has_learner_sync_state(
    state: Arc<NodeState>,
    min_epoch: u64,
    shard_id: u64,
    mv: &ReplicaMove,
) -> bool {
    let Some(target_state) = fetch_target_snapshot(state, mv.to_node).await else {
        return false;
    };
    if target_state.epoch < min_epoch {
        return false;
    }
    let Some(target_mv) = target_state.shard_rebalances.get(&shard_id) else {
        return false;
    };
    if target_mv.phase != ReplicaMovePhase::LearnerSync
        || target_mv.from_node != mv.from_node
        || target_mv.to_node != mv.to_node
    {
        return false;
    }
    match shard_role(&target_state, shard_id, mv.to_node) {
        Some(ReplicaRole::Learner) => {}
        _ => return false,
    }
    target_state
        .shards
        .iter()
        .find(|s| s.shard_id == shard_id)
        .map(|s| s.replicas.contains(&mv.to_node))
        .unwrap_or(false)
}

async fn target_has_joint_config_state(
    state: Arc<NodeState>,
    min_epoch: u64,
    shard_id: u64,
    mv: &ReplicaMove,
) -> bool {
    let Some(target_state) = fetch_target_snapshot(state, mv.to_node).await else {
        return false;
    };
    if target_state.epoch < min_epoch {
        return false;
    }
    let Some(target_mv) = target_state.shard_rebalances.get(&shard_id) else {
        return false;
    };
    match target_mv.phase {
        ReplicaMovePhase::JointConfig | ReplicaMovePhase::LeaseTransferred => {}
        ReplicaMovePhase::LearnerSync => return false,
    }
    match shard_role(&target_state, shard_id, mv.to_node) {
        Some(ReplicaRole::Voter) => {}
        _ => return false,
    }
    match shard_role(&target_state, shard_id, mv.from_node) {
        Some(ReplicaRole::Outgoing) => {}
        _ => return false,
    }
    true
}

async fn target_has_lease_state(
    state: Arc<NodeState>,
    min_epoch: u64,
    shard_id: u64,
    mv: &ReplicaMove,
    expected_leaseholder: NodeId,
    require_lease_transferred_phase: bool,
) -> bool {
    let Some(target_state) = fetch_target_snapshot(state, mv.to_node).await else {
        return false;
    };
    if target_state.epoch < min_epoch {
        return false;
    }
    let Some(target_mv) = target_state.shard_rebalances.get(&shard_id) else {
        return false;
    };
    if require_lease_transferred_phase {
        if target_mv.phase != ReplicaMovePhase::LeaseTransferred {
            return false;
        }
    } else if !matches!(
        target_mv.phase,
        ReplicaMovePhase::JointConfig | ReplicaMovePhase::LeaseTransferred
    ) {
        return false;
    }

    let Some(target_shard) = target_state.shards.iter().find(|s| s.shard_id == shard_id) else {
        return false;
    };
    if target_shard.leaseholder != expected_leaseholder {
        return false;
    }
    match shard_role(&target_state, shard_id, mv.to_node) {
        Some(ReplicaRole::Voter) => {}
        _ => return false,
    }
    true
}

async fn learner_caught_up(
    state: Arc<NodeState>,
    shard: &ShardDesc,
    source: NodeId,
    target: NodeId,
) -> anyhow::Result<bool> {
    let group_id = crate::GROUP_DATA_BASE + shard.shard_index as u64;
    let source_prefixes = state.transport.last_executed_prefix(source, group_id).await?;
    let target_prefixes = state.transport.last_executed_prefix(target, group_id).await?;
    // Learner is considered caught up when it has executed at least everything
    // the source has executed for every origin node in this group.
    for src in &source_prefixes {
        let target_counter = prefix_counter(&target_prefixes, src.node_id);
        if target_counter < src.counter {
            tracing::debug!(
                shard_id = shard.shard_id,
                shard_index = shard.shard_index,
                source,
                target,
                origin = src.node_id,
                source_counter = src.counter,
                target_counter,
                "learner catch-up lagging for origin"
            );
            return Ok(false);
        }
    }
    // Also keep the explicit source-origin check for clearer intent.
    let source_counter = prefix_counter(&source_prefixes, source);
    let target_seen_source = prefix_counter(&target_prefixes, source);
    if target_seen_source < source_counter {
        tracing::debug!(
            shard_id = shard.shard_id,
            shard_index = shard.shard_index,
            source,
            target,
            source_counter,
            target_counter = target_seen_source,
            "learner catch-up lagging on source-origin prefix"
        );
    }
    Ok(target_seen_source >= source_counter)
}

fn prefix_counter(prefixes: &[ExecutedPrefix], node_id: NodeId) -> u64 {
    prefixes
        .iter()
        .find(|p| p.node_id == node_id)
        .map(|p| p.counter)
        .unwrap_or(0)
}

fn plan_decommission_step(state: &ClusterState) -> Option<ClusterCommand> {
    let active = active_member_ids(state);
    if active.is_empty() {
        return None;
    }

    for (node_id, member) in &state.members {
        if member.state != MemberState::Decommissioning {
            continue;
        }
        // Move one shard at a time off the decommissioning node.
        if let Some(shard) = state
            .shards
            .iter()
            .find(|s| s.replicas.iter().any(|id| *id == *node_id))
        {
            // If this shard already has an in-flight move, wait for the phase machine.
            if state.shard_rebalances.contains_key(&shard.shard_id) {
                return None;
            }
            let Some(target) = active.iter().copied().find(|id| !shard.replicas.contains(id))
            else {
                // No destination available yet.
                return None;
            };
            return Some(ClusterCommand::BeginReplicaMove {
                shard_id: shard.shard_id,
                from_node: *node_id,
                to_node: target,
                target_leaseholder: None,
            });
        }

        // Drained from every shard: finalize removal.
        return Some(ClusterCommand::FinalizeNodeRemoval { node_id: *node_id });
    }
    None
}

fn plan_replica_balance_step(state: &ClusterState) -> Option<ClusterCommand> {
    if !state.shard_rebalances.is_empty() {
        return None;
    }
    let active = active_member_ids(state);
    if active.is_empty() {
        return None;
    }
    let desired = desired_replica_count(state, active.len());
    if desired <= 1 || state.shards.is_empty() {
        return None;
    }

    let counts = replica_counts(state, &active);
    let (most_loaded, most) = max_count(&counts)?;
    let (least_loaded, least) = min_count(&counts)?;
    if most <= least + 1 {
        return None;
    }

    for shard in &state.shards {
        if !shard.replicas.contains(&most_loaded) || shard.replicas.contains(&least_loaded) {
            continue;
        }
        return Some(ClusterCommand::BeginReplicaMove {
            shard_id: shard.shard_id,
            from_node: most_loaded,
            to_node: least_loaded,
            target_leaseholder: None,
        });
    }
    None
}

fn plan_lease_balance_step(state: &ClusterState) -> Option<ClusterCommand> {
    if !state.shard_rebalances.is_empty() {
        return None;
    }
    let active = active_member_ids(state);
    if active.is_empty() || state.shards.is_empty() {
        return None;
    }
    let mut lease_counts = BTreeMap::new();
    for id in &active {
        lease_counts.insert(*id, 0usize);
    }
    for shard in &state.shards {
        if let Some(v) = lease_counts.get_mut(&shard.leaseholder) {
            *v += 1;
        }
    }
    let (most_loaded, most) = max_count(&lease_counts)?;
    let (least_loaded, least) = min_count(&lease_counts)?;
    if most <= least + 1 {
        return None;
    }

    for shard in &state.shards {
        if shard.leaseholder != most_loaded {
            continue;
        }
        if !shard.replicas.contains(&least_loaded) {
            continue;
        }
        return Some(ClusterCommand::TransferShardLease {
            shard_id: shard.shard_id,
            leaseholder: least_loaded,
        });
    }
    None
}

fn active_member_ids(state: &ClusterState) -> Vec<NodeId> {
    state
        .members
        .iter()
        .filter_map(|(id, member)| (member.state == MemberState::Active).then_some(*id))
        .collect()
}

fn desired_replica_count(state: &ClusterState, active_members: usize) -> usize {
    state.replication_factor.min(active_members).max(1)
}

fn replica_counts(state: &ClusterState, active: &[NodeId]) -> BTreeMap<NodeId, usize> {
    let mut counts = BTreeMap::new();
    for id in active {
        counts.insert(*id, 0usize);
    }
    for shard in &state.shards {
        for id in &shard.replicas {
            if let Some(v) = counts.get_mut(id) {
                *v += 1;
            }
        }
    }
    counts
}

fn max_count(counts: &BTreeMap<NodeId, usize>) -> Option<(NodeId, usize)> {
    counts
        .iter()
        .max_by_key(|(_, count)| **count)
        .map(|(id, count)| (*id, *count))
}

fn min_count(counts: &BTreeMap<NodeId, usize>) -> Option<(NodeId, usize)> {
    counts
        .iter()
        .min_by_key(|(_, count)| **count)
        .map(|(id, count)| (*id, *count))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::cluster::{ClusterState, MemberInfo, MemberState, ShardDesc};

    fn member(node_id: NodeId, state: MemberState) -> MemberInfo {
        MemberInfo {
            node_id,
            grpc_addr: format!("127.0.0.1:{}", 15050 + node_id),
            redis_addr: format!("127.0.0.1:{}", 16378 + node_id),
            state,
        }
    }

    fn shard(shard_id: u64, replicas: Vec<NodeId>, leaseholder: NodeId) -> ShardDesc {
        ShardDesc {
            shard_id,
            shard_index: (shard_id - 1) as usize,
            start_hash: 0,
            end_hash: 0,
            start_key: vec![],
            end_key: vec![],
            replicas,
            leaseholder,
        }
    }

    fn empty_control_plane() -> (
        BTreeMap<u64, BTreeMap<NodeId, crate::cluster::ReplicaRole>>,
        BTreeMap<u64, crate::cluster::ReplicaMove>,
    ) {
        (BTreeMap::new(), BTreeMap::new())
    }

    fn now_unix_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
            .unwrap_or(0)
    }

    #[test]
    fn decommission_plans_begin_move_then_finalize_when_drained() {
        let mut members = BTreeMap::new();
        members.insert(1, member(1, MemberState::Active));
        members.insert(2, member(2, MemberState::Active));
        members.insert(3, member(3, MemberState::Decommissioning));
        members.insert(4, member(4, MemberState::Active));
        let (roles, moves) = empty_control_plane();
        let state = ClusterState {
            epoch: 1,
            frozen: false,
            replication_factor: 3,
            members,
            shards: vec![shard(10, vec![1, 2, 3], 3)],
            shard_replica_roles: roles,
            shard_rebalances: moves,
        };

        let cmd = plan_decommission_step(&state).expect("expected decommission step");
        match cmd {
            ClusterCommand::BeginReplicaMove {
                shard_id,
                from_node,
                to_node,
                ..
            } => {
                assert_eq!(shard_id, 10);
                assert_eq!(from_node, 3);
                assert_eq!(to_node, 4);
            }
            other => panic!("unexpected command: {other:?}"),
        }

        let mut drained = state.clone();
        drained.shards[0].replicas = vec![1, 2, 4];
        let finalize = plan_decommission_step(&drained).expect("expected finalize");
        match finalize {
            ClusterCommand::FinalizeNodeRemoval { node_id } => assert_eq!(node_id, 3),
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn replica_balancer_plans_begin_move_from_hot_to_cold_node() {
        let mut members = BTreeMap::new();
        members.insert(1, member(1, MemberState::Active));
        members.insert(2, member(2, MemberState::Active));
        members.insert(3, member(3, MemberState::Active));
        let (roles, moves) = empty_control_plane();
        let state = ClusterState {
            epoch: 1,
            frozen: false,
            replication_factor: 2,
            members,
            shards: vec![
                shard(1, vec![1, 2], 1),
                shard(2, vec![1, 2], 1),
                shard(3, vec![1, 2], 2),
            ],
            shard_replica_roles: roles,
            shard_rebalances: moves,
        };

        let cmd = plan_replica_balance_step(&state).expect("expected rebalance");
        match cmd {
            ClusterCommand::BeginReplicaMove {
                from_node,
                to_node,
                ..
            } => {
                assert!(from_node == 1 || from_node == 2);
                assert_eq!(to_node, 3);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn lease_balancer_moves_lease_to_underloaded_replica() {
        let mut members = BTreeMap::new();
        members.insert(1, member(1, MemberState::Active));
        members.insert(2, member(2, MemberState::Active));
        members.insert(3, member(3, MemberState::Active));
        let (roles, moves) = empty_control_plane();
        let state = ClusterState {
            epoch: 1,
            frozen: false,
            replication_factor: 2,
            members,
            shards: vec![
                shard(1, vec![1, 2], 1),
                shard(2, vec![1, 3], 1),
                shard(3, vec![2, 3], 2),
            ],
            shard_replica_roles: roles,
            shard_rebalances: moves,
        };

        let cmd = plan_lease_balance_step(&state).expect("expected lease rebalance");
        match cmd {
            ClusterCommand::TransferShardLease { leaseholder, .. } => {
                assert_eq!(leaseholder, 3);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn stalled_learner_sync_move_is_aborted() {
        let mut members = BTreeMap::new();
        members.insert(1, member(1, MemberState::Active));
        members.insert(2, member(2, MemberState::Active));
        members.insert(3, member(3, MemberState::Active));
        members.insert(4, member(4, MemberState::Active));

        let mut moves = BTreeMap::new();
        let now = now_unix_ms();
        moves.insert(
            1,
            crate::cluster::ReplicaMove {
                from_node: 1,
                to_node: 4,
                phase: crate::cluster::ReplicaMovePhase::LearnerSync,
                backfill_done: true,
                target_leaseholder: None,
                started_unix_ms: now.saturating_sub(120_000),
                last_progress_unix_ms: now.saturating_sub(120_000),
            },
        );

        let mut roles = BTreeMap::new();
        roles.insert(
            1,
            BTreeMap::from([
                (1, crate::cluster::ReplicaRole::Outgoing),
                (2, crate::cluster::ReplicaRole::Voter),
                (3, crate::cluster::ReplicaRole::Voter),
                (4, crate::cluster::ReplicaRole::Learner),
            ]),
        );

        let state = ClusterState {
            epoch: 1,
            frozen: false,
            replication_factor: 3,
            members,
            shards: vec![shard(1, vec![1, 2, 3, 4], 1)],
            shard_replica_roles: roles,
            shard_rebalances: moves,
        };

        let cmd = plan_stalled_move_step(&state, Duration::from_millis(1_000))
            .expect("expected stalled move command");
        match cmd {
            ClusterCommand::AbortReplicaMove { shard_id } => assert_eq!(shard_id, 1),
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn stalled_lease_transferred_move_force_finalizes() {
        let mut members = BTreeMap::new();
        members.insert(1, member(1, MemberState::Active));
        members.insert(2, member(2, MemberState::Active));
        members.insert(3, member(3, MemberState::Active));
        members.insert(4, member(4, MemberState::Active));

        let mut moves = BTreeMap::new();
        let now = now_unix_ms();
        moves.insert(
            1,
            crate::cluster::ReplicaMove {
                from_node: 1,
                to_node: 4,
                phase: crate::cluster::ReplicaMovePhase::LeaseTransferred,
                backfill_done: true,
                target_leaseholder: Some(4),
                started_unix_ms: now.saturating_sub(120_000),
                last_progress_unix_ms: now.saturating_sub(120_000),
            },
        );

        let state = ClusterState {
            epoch: 1,
            frozen: false,
            replication_factor: 3,
            members,
            shards: vec![shard(1, vec![1, 2, 3, 4], 4)],
            shard_replica_roles: BTreeMap::new(),
            shard_rebalances: moves,
        };

        let cmd = plan_stalled_move_step(&state, Duration::from_millis(1_000))
            .expect("expected stalled move command");
        match cmd {
            ClusterCommand::FinalizeReplicaMove { shard_id } => assert_eq!(shard_id, 1),
            other => panic!("unexpected command: {other:?}"),
        }
    }
}
