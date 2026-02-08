//! Background replica rebalancing and node decommission orchestration.
//!
//! Reconfiguration is staged:
//! 1. add target as learner (`BeginReplicaMove`)
//! 2. wait for learner catch-up (`last_executed_prefix`)
//! 3. promote learner to joint config (`PromoteReplicaLearner`)
//! 4. transfer lease away from outgoing replica (`TransferShardLease`)
//! 5. cut over and remove outgoing (`FinalizeReplicaMove`)

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use holo_accord::accord::{ExecutedPrefix, NodeId};

use crate::cluster::{ClusterCommand, ClusterState, MemberState, ReplicaMovePhase, ShardDesc};
use crate::NodeState;

/// Configuration for the background rebalancer.
#[derive(Clone, Copy, Debug)]
pub struct RebalanceManagerConfig {
    /// Evaluate and apply at most one metadata change at this interval.
    pub interval: Duration,
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
            if let Err(err) = reconcile_once(state.clone()).await {
                tracing::warn!(error = ?err, "rebalance manager reconcile failed");
            }
        }
    });
}

async fn reconcile_once(state: Arc<NodeState>) -> anyhow::Result<()> {
    let snapshot = state.cluster_store.state();

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
            let caught_up = learner_caught_up(state, shard, mv.from_node, mv.to_node).await?;
            if caught_up {
                Ok(Some(ClusterCommand::PromoteReplicaLearner { shard_id }))
            } else {
                Ok(None)
            }
        }
        ReplicaMovePhase::JointConfig => {
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
                Ok(Some(ClusterCommand::FinalizeReplicaMove { shard_id }))
            }
        }
        ReplicaMovePhase::LeaseTransferred => {
            Ok(Some(ClusterCommand::FinalizeReplicaMove { shard_id }))
        }
    }
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
            return Ok(false);
        }
    }
    // Also keep the explicit source-origin check for clearer intent.
    let source_counter = prefix_counter(&source_prefixes, source);
    let target_seen_source = prefix_counter(&target_prefixes, source);
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
}
