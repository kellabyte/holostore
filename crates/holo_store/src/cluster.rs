//! Control-plane cluster metadata and membership state machine.

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::Context;
use holo_accord::accord::{CommandKeys, ExecMeta, NodeId, StateMachine};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

use crate::kv::encode_key_prefix;
use crate::kv::ShardRouter;
use fjall::{Keyspace, PartitionCreateOptions};
/// Cluster member state.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MemberState {
    Active,
    Decommissioning,
    Removed,
}

/// Cluster member descriptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberInfo {
    pub node_id: NodeId,
    pub grpc_addr: String,
    pub redis_addr: String,
    pub state: MemberState,
}

/// Shard (range) descriptor. Key ranges are lexicographic and end-exclusive.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardDesc {
    pub shard_id: u64,
    pub shard_index: usize,
    pub start_hash: u64,
    pub end_hash: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub replicas: Vec<NodeId>,
    pub leaseholder: NodeId,
}

/// Role of a node for a specific shard during replica reconfiguration.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReplicaRole {
    Learner,
    Voter,
    Outgoing,
}

/// Reconfiguration phase for a shard replica move.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReplicaMovePhase {
    LearnerSync,
    JointConfig,
    LeaseTransferred,
}

/// In-flight replica move tracked in the control-plane metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaMove {
    pub from_node: NodeId,
    pub to_node: NodeId,
    pub phase: ReplicaMovePhase,
    #[serde(default)]
    pub target_leaseholder: Option<NodeId>,
}

/// Cluster-wide metadata stored in the meta group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterState {
    pub epoch: u64,
    pub frozen: bool,
    pub replication_factor: usize,
    pub members: BTreeMap<NodeId, MemberInfo>,
    pub shards: Vec<ShardDesc>,
    #[serde(default)]
    pub shard_replica_roles: BTreeMap<u64, BTreeMap<NodeId, ReplicaRole>>,
    #[serde(default)]
    pub shard_rebalances: BTreeMap<u64, ReplicaMove>,
}

/// Commands applied to the meta group state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterCommand {
    AddNode {
        node_id: NodeId,
        grpc_addr: String,
        redis_addr: String,
    },
    RemoveNode {
        node_id: NodeId,
    },
    FinalizeNodeRemoval {
        node_id: NodeId,
    },
    BeginReplicaMove {
        shard_id: u64,
        from_node: NodeId,
        to_node: NodeId,
        target_leaseholder: Option<NodeId>,
    },
    PromoteReplicaLearner {
        shard_id: u64,
    },
    TransferShardLease {
        shard_id: u64,
        leaseholder: NodeId,
    },
    FinalizeReplicaMove {
        shard_id: u64,
    },
    SetFrozen {
        frozen: bool,
    },
    SplitRange {
        split_key: Vec<u8>,
        target_shard_index: usize,
    },
    MergeRange {
        left_shard_id: u64,
    },
    SetReplicas {
        shard_id: u64,
        replicas: Vec<NodeId>,
        leaseholder: NodeId,
    },
}

/// Coordinates local on-disk key movement for metadata-only range operations.
pub trait RangeMigrator: Send + Sync + 'static {
    fn migrate(
        &self,
        from_shard: usize,
        to_shard: usize,
        start_key: &[u8],
        end_key: &[u8],
    ) -> anyhow::Result<()>;
}

/// Fjall-backed range migrator that moves keys between shard partitions.
pub struct FjallRangeMigrator {
    keyspace: Arc<Keyspace>,
}

impl FjallRangeMigrator {
    pub fn new(keyspace: Arc<Keyspace>) -> Self {
        Self { keyspace }
    }

    fn open_shard_partitions(
        &self,
        shard: usize,
    ) -> anyhow::Result<(fjall::PartitionHandle, fjall::PartitionHandle)> {
        let versions_name = format!("kv_versions_{shard}");
        let latest_name = format!("kv_latest_{shard}");
        let versions = self
            .keyspace
            .open_partition(&versions_name, PartitionCreateOptions::default())?;
        let latest = self
            .keyspace
            .open_partition(&latest_name, PartitionCreateOptions::default())?;
        Ok((versions, latest))
    }
}

impl RangeMigrator for FjallRangeMigrator {
    fn migrate(
        &self,
        from_shard: usize,
        to_shard: usize,
        start_key: &[u8],
        end_key: &[u8],
    ) -> anyhow::Result<()> {
        if from_shard == to_shard {
            return Ok(());
        }

        let (from_versions, from_latest) = self.open_shard_partitions(from_shard)?;
        let (to_versions, to_latest) = self.open_shard_partitions(to_shard)?;

        let start = start_key.to_vec();
        let mut latest_entries = Vec::<(Vec<u8>, Vec<u8>)>::new();
        let mut latest_iter: Box<dyn DoubleEndedIterator<Item = fjall::Result<fjall::KvPair>>> =
            if end_key.is_empty() {
                Box::new(from_latest.range(start..))
            } else {
                Box::new(from_latest.range(start..end_key.to_vec()))
            };

        // Snapshot the source range before mutating it. Deleting from a
        // partition while iterating it can skip entries and cause key loss.
        while let Some(item) = latest_iter.next() {
            let (key, latest_val) = item?;
            latest_entries.push((key.to_vec(), latest_val.to_vec()));
        }

        // Commit in chunks to keep batch sizes bounded.
        const CHUNK_ITEMS: usize = 10_000;
        let mut batch = self.keyspace.batch();
        let mut queued = 0usize;

        for (key_bytes, latest_val) in latest_entries {
            // Snapshot all version rows for this key before mutating
            // `from_versions` to avoid iterator invalidation.
            let prefix = encode_key_prefix(&key_bytes);
            let mut version_entries = Vec::<(Vec<u8>, Vec<u8>)>::new();
            for ver_item in from_versions.prefix(prefix) {
                let (ver_key, ver_val) = ver_item?;
                version_entries.push((ver_key.to_vec(), ver_val.to_vec()));
            }

            // Copy latest index entry.
            batch.insert(&to_latest, key_bytes.clone(), latest_val);
            batch.remove(&from_latest, key_bytes.clone());
            queued += 2;

            // Copy all version entries for this key.
            for (ver_key, ver_val) in version_entries {
                batch.insert(&to_versions, ver_key.clone(), ver_val);
                batch.remove(&from_versions, ver_key);
                queued += 2;
                if queued >= CHUNK_ITEMS {
                    batch.commit()?;
                    batch = self.keyspace.batch();
                    queued = 0;
                }
            }

            if queued >= CHUNK_ITEMS {
                batch.commit()?;
                batch = self.keyspace.batch();
                queued = 0;
            }
        }

        if queued > 0 {
            batch.commit()?;
        }

        Ok(())
    }
}

/// Shared state + persistence wrapper.
#[derive(Clone)]
pub struct ClusterStateStore {
    state: Arc<RwLock<ClusterState>>,
    path: PathBuf,
    split_lock: Arc<std::sync::RwLock<()>>,
}

impl ClusterStateStore {
    pub fn load_or_init(
        path: impl AsRef<Path>,
        members: BTreeMap<NodeId, MemberInfo>,
        replication_factor: usize,
        initial_ranges: usize,
    ) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Ok(data) = fs::read(&path) {
            if let Ok(mut state) = serde_json::from_slice::<ClusterState>(&data) {
                normalize_replica_metadata(&mut state);
                return Ok(Self {
                    state: Arc::new(RwLock::new(state)),
                    path,
                    split_lock: Arc::new(std::sync::RwLock::new(())),
                });
            }
        }

        let effective_rf = replication_factor.max(1).min(members.len().max(1));
        let mut state = ClusterState {
            epoch: 1,
            frozen: false,
            replication_factor: effective_rf,
            members,
            shards: Vec::new(),
            shard_replica_roles: BTreeMap::new(),
            shard_rebalances: BTreeMap::new(),
        };
        // Start with a single full-keyspace range (Cockroach-style bootstrap).
        let count = initial_ranges.max(1).min(256);
        let replicas: Vec<NodeId> = state.members.keys().copied().collect();
        if let Some(leaseholder) = state.members.keys().copied().next() {
            for idx in 0..count {
                // When bootstrapping with a single range, use unbounded start/end.
                let start_key = if count == 1 {
                    Vec::new()
                } else {
                    vec![(idx * 256 / count) as u8]
                };
                let end_key = if idx + 1 == count {
                    Vec::new()
                } else {
                    vec![((idx + 1) * 256 / count) as u8]
                };
                state.shards.push(ShardDesc {
                    shard_id: (idx as u64) + 1,
                    shard_index: idx,
                    start_hash: 0,
                    end_hash: 0,
                    start_key,
                    end_key,
                    replicas: replicas.clone(),
                    leaseholder,
                });
            }
        }
        normalize_replica_metadata(&mut state);

        let store = Self {
            state: Arc::new(RwLock::new(state)),
            path,
            split_lock: Arc::new(std::sync::RwLock::new(())),
        };
        store.persist()?;
        Ok(store)
    }

    pub fn state(&self) -> ClusterState {
        self.state.read().unwrap().clone()
    }

    /// Return just the shard descriptors for routing snapshots.
    pub fn shards_snapshot(&self) -> Vec<ShardDesc> {
        self.state.read().unwrap().shards.clone()
    }

    pub fn members_string(&self) -> String {
        let state = self.state.read().unwrap();
        state
            .members
            .values()
            .filter(|m| m.state != MemberState::Removed)
            .map(|m| format!("{}@{}", m.node_id, m.grpc_addr))
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn epoch(&self) -> u64 {
        self.state.read().unwrap().epoch
    }

    pub fn frozen(&self) -> bool {
        self.state.read().unwrap().frozen
    }

    pub fn members_map(&self) -> anyhow::Result<std::collections::HashMap<NodeId, SocketAddr>> {
        let state = self.state.read().unwrap();
        let mut out = std::collections::HashMap::new();
        for (id, member) in &state.members {
            if member.state == MemberState::Removed {
                continue;
            }
            let addr: SocketAddr = member.grpc_addr.parse().with_context(|| {
                format!("invalid grpc addr for node {id}: {}", member.grpc_addr)
            })?;
            out.insert(*id, addr);
        }
        Ok(out)
    }

    pub fn shard_for_key(&self, key: &[u8]) -> Option<ShardDesc> {
        let state = self.state.read().unwrap();
        if state.shards.is_empty() {
            return None;
        }
        if state.shards.len() == 1 {
            return Some(state.shards[0].clone());
        }
        let has_key_ranges = state
            .shards
            .iter()
            .any(|s| !s.start_key.is_empty() || !s.end_key.is_empty());
        if has_key_ranges {
            for shard in &state.shards {
                if key_in_range(key, &shard.start_key, &shard.end_key) {
                    return Some(shard.clone());
                }
            }
            return Some(state.shards[0].clone());
        }
        // Hash-routing compatibility path.
        let idx = self.shard_index_for_hash(hash_key(key));
        state.shards.iter().find(|s| s.shard_index == idx).cloned()
    }

    pub fn shard_index_for_hash(&self, hash: u64) -> usize {
        let state = self.state.read().unwrap();
        for shard in &state.shards {
            if hash >= shard.start_hash && hash <= shard.end_hash {
                return shard.shard_index;
            }
        }
        0
    }

    pub fn shard_index_for_key(&self, key: &[u8]) -> usize {
        let state = self.state.read().unwrap();
        if state.shards.len() == 1 {
            return state.shards[0].shard_index;
        }
        let has_key_ranges = state
            .shards
            .iter()
            .any(|s| !s.start_key.is_empty() || !s.end_key.is_empty());
        if has_key_ranges {
            for shard in &state.shards {
                if key_in_range(key, &shard.start_key, &shard.end_key) {
                    return shard.shard_index;
                }
            }
            return 0;
        }
        self.shard_index_for_hash(hash_key(key))
    }

    pub fn split_lock(&self) -> Arc<std::sync::RwLock<()>> {
        self.split_lock.clone()
    }

    pub fn first_free_shard_index(&self, limit: usize) -> Option<usize> {
        let state = self.state.read().unwrap();
        let mut used = vec![false; limit];
        for shard in &state.shards {
            if shard.shard_index < limit {
                used[shard.shard_index] = true;
            }
        }
        used.iter().position(|u| !*u)
    }

    /// Plan a staged rebalance command for a shard.
    ///
    /// This accepts only safe staged changes:
    /// - no-op or lease-only change
    /// - single replica replacement (`from -> to`) with optional target leaseholder
    pub fn plan_rebalance_command(
        &self,
        shard_id: u64,
        replicas: Vec<NodeId>,
        leaseholder: Option<NodeId>,
    ) -> anyhow::Result<Option<ClusterCommand>> {
        let state = self.state.read().unwrap();
        let shard = state
            .shards
            .iter()
            .find(|s| s.shard_id == shard_id)
            .ok_or_else(|| anyhow::anyhow!("unknown shard id {shard_id}"))?;

        if state.shard_rebalances.contains_key(&shard_id) {
            anyhow::bail!("shard {shard_id} already has an in-flight move");
        }

        let active_members = active_member_ids_from_state(&state);
        if active_members.is_empty() {
            anyhow::bail!("no active members available");
        }
        let expected_replicas = state.replication_factor.min(active_members.len()).max(1);

        let mut desired = replicas;
        dedupe_nodes_in_place(&mut desired);
        if desired.is_empty() {
            anyhow::bail!("replicas cannot be empty");
        }
        if desired.len() != expected_replicas {
            anyhow::bail!(
                "invalid replica count for shard {shard_id}: got {}, expected {} (rf={}, active={})",
                desired.len(),
                expected_replicas,
                state.replication_factor,
                active_members.len()
            );
        }
        for id in &desired {
            let Some(member) = state.members.get(id) else {
                anyhow::bail!("replica node {id} does not exist");
            };
            if member.state != MemberState::Active {
                anyhow::bail!("replica node {id} is not Active");
            }
        }

        if let Some(target_leaseholder) = leaseholder {
            if !desired.contains(&target_leaseholder) {
                anyhow::bail!(
                    "leaseholder {} must be in requested replicas {:?}",
                    target_leaseholder,
                    desired
                );
            }
        }

        let mut current = shard.replicas.clone();
        dedupe_nodes_in_place(&mut current);
        let added = desired
            .iter()
            .copied()
            .filter(|id| !current.contains(id))
            .collect::<Vec<_>>();
        let removed = current
            .iter()
            .copied()
            .filter(|id| !desired.contains(id))
            .collect::<Vec<_>>();

        if added.is_empty() && removed.is_empty() {
            if let Some(target_leaseholder) = leaseholder {
                if target_leaseholder != shard.leaseholder {
                    return Ok(Some(ClusterCommand::TransferShardLease {
                        shard_id,
                        leaseholder: target_leaseholder,
                    }));
                }
            }
            return Ok(None);
        }

        if added.len() == 1 && removed.len() == 1 && desired.len() == current.len() {
            let from_node = removed[0];
            let to_node = added[0];
            if let Some(target_leaseholder) = leaseholder {
                if target_leaseholder == from_node {
                    anyhow::bail!(
                        "leaseholder {} cannot be source node for move {} -> {}",
                        target_leaseholder,
                        from_node,
                        to_node
                    );
                }
            }
            return Ok(Some(ClusterCommand::BeginReplicaMove {
                shard_id,
                from_node,
                to_node,
                target_leaseholder: leaseholder,
            }));
        }

        anyhow::bail!(
            "staged rebalance supports replacing exactly one replica at a time (current={:?}, requested={:?})",
            current,
            desired
        );
    }

    pub fn apply_split_range(
        &self,
        split_key: Vec<u8>,
        target_shard_index: usize,
        migrator: Option<&dyn RangeMigrator>,
        shard_limit: usize,
    ) -> anyhow::Result<()> {
        if target_shard_index >= shard_limit {
            anyhow::bail!(
                "target_shard_index {target_shard_index} exceeds shard limit {shard_limit}"
            );
        }

        // Block client request routing while we move keys and update descriptors.
        let _split_guard = self.split_lock.write().unwrap();

        let mut state = self.state.write().unwrap();
        let idx = state
            .shards
            .iter()
            .position(|s| key_in_range(&split_key, &s.start_key, &s.end_key))
            .ok_or_else(|| anyhow::anyhow!("split key does not map to any shard"))?;

        // Ensure we are not reusing an active shard index.
        if state
            .shards
            .iter()
            .any(|s| s.shard_index == target_shard_index)
        {
            anyhow::bail!("target shard index {target_shard_index} already in use");
        }

        let shard_snapshot = state.shards[idx].clone();
        if !shard_snapshot.start_key.is_empty() && split_key <= shard_snapshot.start_key {
            anyhow::bail!("split key must be greater than shard start");
        }
        if !shard_snapshot.end_key.is_empty() && split_key >= shard_snapshot.end_key {
            anyhow::bail!("split key must be less than shard end");
        }

        let from_shard = shard_snapshot.shard_index;
        let start = split_key.clone();
        let end = shard_snapshot.end_key.clone();

        // Move keys for the right-hand side before changing routing.
        if let Some(migrator) = migrator {
            migrator.migrate(from_shard, target_shard_index, &start, &end)?;
        }

        let right_id = next_shard_id(&state.shards);
        let right = ShardDesc {
            shard_id: right_id,
            shard_index: target_shard_index,
            start_hash: 0,
            end_hash: 0,
            start_key: start,
            end_key: end.clone(),
            replicas: shard_snapshot.replicas.clone(),
            leaseholder: shard_snapshot.leaseholder,
        };
        state.shards[idx].end_key = right.start_key.clone();
        state.shards.insert(idx + 1, right);
        let left_id = shard_snapshot.shard_id;
        let left_roles = state
            .shard_replica_roles
            .entry(left_id)
            .or_insert_with(|| default_roles_for_replicas(&shard_snapshot.replicas))
            .clone();
        state.shard_replica_roles.insert(right_id, left_roles);
        state.shard_rebalances.remove(&right_id);
        state.epoch = state.epoch.saturating_add(1);

        drop(state);
        self.persist()
    }

    pub fn apply_command(&self, cmd: ClusterCommand) -> anyhow::Result<()> {
        let mut state = self.state.write().unwrap();
        normalize_replica_metadata(&mut state);
        match cmd {
            ClusterCommand::AddNode {
                node_id,
                grpc_addr,
                redis_addr,
            } => {
                state
                    .members
                    .entry(node_id)
                    .and_modify(|m| {
                        m.grpc_addr = grpc_addr.clone();
                        m.redis_addr = redis_addr.clone();
                        m.state = MemberState::Active;
                    })
                    .or_insert(MemberInfo {
                        node_id,
                        grpc_addr,
                        redis_addr,
                        state: MemberState::Active,
                    });
                state.epoch = state.epoch.saturating_add(1);
                // Shard placement reconfiguration is handled by the rebalancer loop.
            }
            ClusterCommand::RemoveNode { node_id } => {
                let current = state
                    .members
                    .get(&node_id)
                    .ok_or_else(|| anyhow::anyhow!("unknown node id {node_id}"))?;
                if current.state == MemberState::Removed {
                    anyhow::bail!("node {node_id} is already removed");
                }
                if state
                    .shard_rebalances
                    .values()
                    .any(|m| m.from_node == node_id)
                {
                    anyhow::bail!("node {node_id} has in-flight replica moves");
                }
                if let Some(member) = state.members.get_mut(&node_id) {
                    member.state = MemberState::Decommissioning;
                }
                let active_members = active_member_ids_from_state(&state);
                // If the removed node currently holds leases, move leases to any
                // active replica immediately to reduce impact while draining.
                for shard in &mut state.shards {
                    if shard.leaseholder != node_id {
                        continue;
                    }
                    if let Some(next) = shard
                        .replicas
                        .iter()
                        .copied()
                        .find(|id| active_members.contains(id))
                    {
                        shard.leaseholder = next;
                    }
                }
                state.epoch = state.epoch.saturating_add(1);
            }
            ClusterCommand::FinalizeNodeRemoval { node_id } => {
                let current = state
                    .members
                    .get(&node_id)
                    .ok_or_else(|| anyhow::anyhow!("unknown node id {node_id}"))?;
                if current.state == MemberState::Removed {
                    // Idempotent finalize.
                    return Ok(());
                }
                if current.state != MemberState::Decommissioning {
                    anyhow::bail!("node {node_id} must be decommissioning before finalization");
                }
                if state
                    .shards
                    .iter()
                    .any(|s| s.replicas.iter().any(|id| *id == node_id))
                {
                    anyhow::bail!("node {node_id} still owns shard replicas");
                }
                if state
                    .shard_rebalances
                    .values()
                    .any(|m| m.from_node == node_id || m.to_node == node_id)
                {
                    anyhow::bail!("node {node_id} still has in-flight replica moves");
                }
                if let Some(member) = state.members.get_mut(&node_id) {
                    member.state = MemberState::Removed;
                }
                state.epoch = state.epoch.saturating_add(1);
            }
            ClusterCommand::BeginReplicaMove {
                shard_id,
                from_node,
                to_node,
                target_leaseholder,
            } => {
                if from_node == to_node {
                    anyhow::bail!("from_node and to_node cannot be equal");
                }
                if state.shard_rebalances.contains_key(&shard_id) {
                    anyhow::bail!("shard {shard_id} already has an in-flight move");
                }
                let active_members = active_member_ids_from_state(&state);
                if !active_members.contains(&to_node) {
                    anyhow::bail!("target node {to_node} is not Active");
                }
                if let Some(leaseholder) = target_leaseholder {
                    if leaseholder == from_node {
                        anyhow::bail!("target leaseholder cannot be source node {from_node}");
                    }
                    if !active_members.contains(&leaseholder) {
                        anyhow::bail!("target leaseholder {leaseholder} is not Active");
                    }
                }
                let shard_idx = state
                    .shards
                    .iter()
                    .position(|s| s.shard_id == shard_id)
                    .ok_or_else(|| anyhow::anyhow!("unknown shard id {shard_id}"))?;
                let replicas_after_move = {
                    let shard = &mut state.shards[shard_idx];
                    if !shard.replicas.contains(&from_node) {
                        anyhow::bail!(
                            "source node {from_node} is not a replica of shard {shard_id}"
                        );
                    }
                    if shard.replicas.contains(&to_node) {
                        anyhow::bail!(
                            "target node {to_node} is already a replica of shard {shard_id}"
                        );
                    }
                    shard.replicas.push(to_node);
                    if let Some(leaseholder) = target_leaseholder {
                        if !shard.replicas.contains(&leaseholder) {
                            anyhow::bail!(
                                "target leaseholder {leaseholder} must be in resulting replicas"
                            );
                        }
                    }
                    shard.replicas.clone()
                };
                let roles = state
                    .shard_replica_roles
                    .entry(shard_id)
                    .or_insert_with(|| default_roles_for_replicas(&replicas_after_move));
                roles.insert(to_node, ReplicaRole::Learner);
                roles.insert(from_node, ReplicaRole::Voter);
                state.shard_rebalances.insert(
                    shard_id,
                    ReplicaMove {
                        from_node,
                        to_node,
                        phase: ReplicaMovePhase::LearnerSync,
                        target_leaseholder,
                    },
                );
                state.epoch = state.epoch.saturating_add(1);
            }
            ClusterCommand::PromoteReplicaLearner { shard_id } => {
                let (to_node, from_node) = {
                    let mv = state
                        .shard_rebalances
                        .get_mut(&shard_id)
                        .ok_or_else(|| anyhow::anyhow!("shard {shard_id} has no in-flight move"))?;
                    if mv.phase != ReplicaMovePhase::LearnerSync {
                        anyhow::bail!(
                            "shard {shard_id} learner can only be promoted from LearnerSync"
                        );
                    }
                    mv.phase = ReplicaMovePhase::JointConfig;
                    (mv.to_node, mv.from_node)
                };
                let roles = state
                    .shard_replica_roles
                    .entry(shard_id)
                    .or_insert_with(BTreeMap::new);
                roles.insert(to_node, ReplicaRole::Voter);
                roles.insert(from_node, ReplicaRole::Outgoing);
                state.epoch = state.epoch.saturating_add(1);
            }
            ClusterCommand::TransferShardLease {
                shard_id,
                leaseholder,
            } => {
                let shard_idx = state
                    .shards
                    .iter()
                    .position(|s| s.shard_id == shard_id)
                    .ok_or_else(|| anyhow::anyhow!("unknown shard id {shard_id}"))?;
                let replicas_snapshot = {
                    let shard = &state.shards[shard_idx];
                    if !shard.replicas.contains(&leaseholder) {
                        anyhow::bail!("leaseholder {leaseholder} must be in shard replicas");
                    }
                    shard.replicas.clone()
                };
                let roles = state
                    .shard_replica_roles
                    .entry(shard_id)
                    .or_insert_with(|| default_roles_for_replicas(&replicas_snapshot));
                let role = roles
                    .get(&leaseholder)
                    .copied()
                    .unwrap_or(ReplicaRole::Voter);
                if role == ReplicaRole::Learner {
                    anyhow::bail!("leaseholder cannot be a Learner");
                }
                state.shards[shard_idx].leaseholder = leaseholder;
                if let Some(mv) = state.shard_rebalances.get_mut(&shard_id) {
                    if mv.phase == ReplicaMovePhase::JointConfig {
                        mv.phase = ReplicaMovePhase::LeaseTransferred;
                    }
                }
                state.epoch = state.epoch.saturating_add(1);
            }
            ClusterCommand::FinalizeReplicaMove { shard_id } => {
                let mv = state
                    .shard_rebalances
                    .get(&shard_id)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("shard {shard_id} has no in-flight move"))?;
                if mv.phase == ReplicaMovePhase::LearnerSync {
                    anyhow::bail!("cannot finalize shard {shard_id} before learner promotion");
                }
                let shard_idx = state
                    .shards
                    .iter()
                    .position(|s| s.shard_id == shard_id)
                    .ok_or_else(|| anyhow::anyhow!("unknown shard id {shard_id}"))?;
                let replicas_snapshot = {
                    let shard = &mut state.shards[shard_idx];
                    if shard.leaseholder == mv.from_node {
                        anyhow::bail!(
                            "cannot finalize shard {shard_id} while leaseholder is source node"
                        );
                    }
                    shard.replicas.retain(|id| *id != mv.from_node);
                    shard.replicas.clone()
                };
                let roles = state
                    .shard_replica_roles
                    .entry(shard_id)
                    .or_insert_with(|| default_roles_for_replicas(&replicas_snapshot));
                roles.remove(&mv.from_node);
                roles.insert(mv.to_node, ReplicaRole::Voter);
                state.shard_rebalances.remove(&shard_id);
                state.epoch = state.epoch.saturating_add(1);
            }
            ClusterCommand::SetFrozen { frozen } => {
                state.frozen = frozen;
                state.epoch = state.epoch.saturating_add(1);
            }
            ClusterCommand::SplitRange { .. } => {
                // Split operations must run through `apply_split_range` so keys
                // are moved before routing changes.
                anyhow::bail!("SplitRange must be applied via apply_split_range");
            }
            ClusterCommand::MergeRange { left_shard_id } => {
                let idx = match state
                    .shards
                    .iter()
                    .position(|s| s.shard_id == left_shard_id)
                {
                    Some(idx) => idx,
                    None => anyhow::bail!("unknown shard id {left_shard_id}"),
                };
                if idx + 1 >= state.shards.len() {
                    anyhow::bail!("merge requires a right-hand neighbor");
                }
                let right = state.shards.remove(idx + 1);
                let left = &mut state.shards[idx];
                // Metadata-only merges are only safe when both ranges map to the
                // same underlying data shard (no data movement yet).
                if left.shard_index != right.shard_index {
                    anyhow::bail!(
                        "cannot merge shards with different shard_index (left={}, right={})",
                        left.shard_index,
                        right.shard_index
                    );
                }
                if left.end_key.is_empty() {
                    anyhow::bail!("cannot merge an unbounded range");
                }
                if !left.end_key.is_empty() && left.end_key != right.start_key {
                    anyhow::bail!("shards are not adjacent");
                }
                left.end_key = right.end_key;
                state.shard_replica_roles.remove(&right.shard_id);
                state.shard_rebalances.remove(&right.shard_id);
                state.epoch = state.epoch.saturating_add(1);
            }
            ClusterCommand::SetReplicas {
                shard_id,
                replicas,
                leaseholder,
            } => {
                let shard_idx = state
                    .shards
                    .iter()
                    .position(|s| s.shard_id == shard_id)
                    .ok_or_else(|| anyhow::anyhow!("unknown shard id {shard_id}"))?;
                let mut deduped = Vec::with_capacity(replicas.len());
                for id in replicas {
                    if !deduped.contains(&id) {
                        deduped.push(id);
                    }
                }
                if deduped.is_empty() {
                    anyhow::bail!("replicas cannot be empty");
                }
                let active_members = active_member_ids_from_state(&state);
                if active_members.is_empty() {
                    anyhow::bail!("no active members available");
                }
                let desired = state.replication_factor.min(active_members.len()).max(1);
                if deduped.len() != desired {
                    anyhow::bail!(
                        "invalid replica count for shard {shard_id}: got {}, expected {} (rf={}, active={})",
                        deduped.len(),
                        desired,
                        state.replication_factor,
                        active_members.len()
                    );
                }
                for id in &deduped {
                    let Some(member) = state.members.get(id) else {
                        anyhow::bail!("replica node {id} does not exist");
                    };
                    if member.state != MemberState::Active {
                        anyhow::bail!("replica node {id} is not Active");
                    }
                }
                let resolved_leaseholder = if leaseholder == 0 {
                    deduped[0]
                } else {
                    leaseholder
                };
                if !deduped.contains(&resolved_leaseholder) {
                    anyhow::bail!(
                        "leaseholder {} must be a member of replicas {:?}",
                        resolved_leaseholder,
                        deduped
                    );
                }
                let replicas_snapshot = {
                    let shard = &mut state.shards[shard_idx];
                    shard.replicas = deduped;
                    shard.leaseholder = resolved_leaseholder;
                    shard.replicas.clone()
                };
                state
                    .shard_replica_roles
                    .insert(shard_id, default_roles_for_replicas(&replicas_snapshot));
                state.shard_rebalances.remove(&shard_id);
                state.epoch = state.epoch.saturating_add(1);
            }
        }
        normalize_replica_metadata(&mut state);
        drop(state);
        self.persist()
    }

    fn persist(&self) -> anyhow::Result<()> {
        let state = self.state.read().unwrap();
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).context("create cluster state dir")?;
        }
        let data = serde_json::to_vec_pretty(&*state).context("serialize cluster state")?;
        fs::write(&self.path, data).context("write cluster state")?;
        Ok(())
    }
}

/// Meta group state machine that applies `ClusterCommand`s.
pub struct ClusterStateMachine {
    store: ClusterStateStore,
    migrator: Option<Arc<dyn RangeMigrator>>,
    shard_limit: usize,
}

impl ClusterStateMachine {
    pub fn new(
        store: ClusterStateStore,
        migrator: Option<Arc<dyn RangeMigrator>>,
        shard_limit: usize,
    ) -> Self {
        Self {
            store,
            migrator,
            shard_limit: shard_limit.max(1),
        }
    }

    pub fn store(&self) -> ClusterStateStore {
        self.store.clone()
    }

    pub fn encode_command(cmd: &ClusterCommand) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(cmd)?)
    }

    pub fn decode_command(data: &[u8]) -> anyhow::Result<ClusterCommand> {
        Ok(serde_json::from_slice(data)?)
    }
}

fn active_member_ids_from_state(state: &ClusterState) -> Vec<NodeId> {
    state
        .members
        .iter()
        .filter_map(|(id, m)| (m.state == MemberState::Active).then_some(*id))
        .collect()
}

fn default_roles_for_replicas(replicas: &[NodeId]) -> BTreeMap<NodeId, ReplicaRole> {
    replicas
        .iter()
        .copied()
        .map(|id| (id, ReplicaRole::Voter))
        .collect()
}

fn normalize_replica_metadata(state: &mut ClusterState) {
    state
        .shard_replica_roles
        .retain(|shard_id, _| state.shards.iter().any(|s| s.shard_id == *shard_id));
    state
        .shard_rebalances
        .retain(|shard_id, mv| {
            let Some(shard) = state.shards.iter().find(|s| s.shard_id == *shard_id) else {
                return false;
            };
            shard.replicas.contains(&mv.from_node) && shard.replicas.contains(&mv.to_node)
        });

    for shard in &mut state.shards {
        dedupe_nodes_in_place(&mut shard.replicas);
        if shard.replicas.is_empty() {
            continue;
        }
        if !shard.replicas.contains(&shard.leaseholder) {
            shard.leaseholder = shard.replicas[0];
        }
        let roles = state
            .shard_replica_roles
            .entry(shard.shard_id)
            .or_insert_with(|| default_roles_for_replicas(&shard.replicas));
        roles.retain(|id, _| shard.replicas.contains(id));
        for id in &shard.replicas {
            roles.entry(*id).or_insert(ReplicaRole::Voter);
        }
        if let Some(mv) = state.shard_rebalances.get_mut(&shard.shard_id) {
            if mv.target_leaseholder == Some(mv.from_node) {
                mv.target_leaseholder = None;
            }
            if let Some(target) = mv.target_leaseholder {
                if !shard.replicas.contains(&target) {
                    mv.target_leaseholder = None;
                }
            }
        }
    }
}

fn dedupe_nodes_in_place(nodes: &mut Vec<NodeId>) {
    let mut out = Vec::with_capacity(nodes.len());
    for id in nodes.drain(..) {
        if !out.contains(&id) {
            out.push(id);
        }
    }
    *nodes = out;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn member(node_id: NodeId) -> MemberInfo {
        MemberInfo {
            node_id,
            grpc_addr: format!("127.0.0.1:{}", 15050 + node_id),
            redis_addr: format!("127.0.0.1:{}", 16378 + node_id),
            state: MemberState::Active,
        }
    }

    fn test_store(state: ClusterState) -> ClusterStateStore {
        ClusterStateStore {
            path: PathBuf::from("/tmp/cluster_state_test.json"),
            state: Arc::new(RwLock::new(state)),
            split_lock: Arc::new(RwLock::new(())),
        }
    }

    fn base_state() -> ClusterState {
        let mut members = BTreeMap::new();
        members.insert(1, member(1));
        members.insert(2, member(2));
        members.insert(3, member(3));
        members.insert(4, member(4));
        members.insert(5, member(5));
        ClusterState {
            epoch: 1,
            frozen: false,
            replication_factor: 3,
            members,
            shards: vec![ShardDesc {
                shard_id: 10,
                shard_index: 0,
                start_hash: 0,
                end_hash: 0,
                start_key: vec![],
                end_key: vec![],
                replicas: vec![1, 2, 3],
                leaseholder: 1,
            }],
            shard_replica_roles: BTreeMap::new(),
            shard_rebalances: BTreeMap::new(),
        }
    }

    #[test]
    fn plan_rebalance_replacement_returns_begin_move() {
        let store = test_store(base_state());
        let cmd = store
            .plan_rebalance_command(10, vec![2, 3, 4], Some(4))
            .expect("plan should succeed")
            .expect("command should be present");
        match cmd {
            ClusterCommand::BeginReplicaMove {
                shard_id,
                from_node,
                to_node,
                target_leaseholder,
            } => {
                assert_eq!(shard_id, 10);
                assert_eq!(from_node, 1);
                assert_eq!(to_node, 4);
                assert_eq!(target_leaseholder, Some(4));
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn plan_rebalance_lease_only_returns_transfer() {
        let store = test_store(base_state());
        let cmd = store
            .plan_rebalance_command(10, vec![1, 2, 3], Some(2))
            .expect("plan should succeed")
            .expect("command should be present");
        match cmd {
            ClusterCommand::TransferShardLease {
                shard_id,
                leaseholder,
            } => {
                assert_eq!(shard_id, 10);
                assert_eq!(leaseholder, 2);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn plan_rebalance_rejects_multi_node_swap() {
        let store = test_store(base_state());
        let err = store
            .plan_rebalance_command(10, vec![2, 4, 5], Some(4))
            .expect_err("multi-node swap should fail");
        assert!(
            err.to_string()
                .contains("replacing exactly one replica at a time")
        );
    }

    #[test]
    fn migrator_moves_all_keys_without_loss() {
        fn temp_dir(name: &str) -> PathBuf {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            std::env::temp_dir().join(format!(
                "holo_store_{name}_{}_{}",
                std::process::id(),
                nanos
            ))
        }

        let dir = temp_dir("migrator_move_all");
        std::fs::create_dir_all(&dir).expect("create temp dir");

        let keyspace = Arc::new(
            fjall::Config::new(&dir)
                .open()
                .expect("open temporary keyspace"),
        );
        let from_versions = keyspace
            .open_partition("kv_versions_0", PartitionCreateOptions::default())
            .expect("open from versions");
        let from_latest = keyspace
            .open_partition("kv_latest_0", PartitionCreateOptions::default())
            .expect("open from latest");
        let to_versions = keyspace
            .open_partition("kv_versions_1", PartitionCreateOptions::default())
            .expect("open to versions");
        let to_latest = keyspace
            .open_partition("kv_latest_1", PartitionCreateOptions::default())
            .expect("open to latest");

        // Keep enough keys to force multiple internal migration commits.
        const KEY_COUNT: usize = 6_000;
        for i in 0..KEY_COUNT {
            let key = format!("k{i:05}").into_bytes();
            let val = format!("v{i:05}").into_bytes();
            from_latest
                .insert(key.clone(), val.clone())
                .expect("insert latest");

            // Match the production version-key layout:
            // [u32 key_len][key bytes][u64 seq][u64 node_id][u64 counter]
            let mut version_key = Vec::with_capacity(4 + key.len() + 8 + 8 + 8);
            version_key.extend_from_slice(&(key.len() as u32).to_be_bytes());
            version_key.extend_from_slice(&key);
            version_key.extend_from_slice(&(i as u64).to_be_bytes());
            version_key.extend_from_slice(&1u64.to_be_bytes());
            version_key.extend_from_slice(&(i as u64).to_be_bytes());
            from_versions
                .insert(version_key, val)
                .expect("insert version");
        }

        let start_key = b"k01000".to_vec();
        let end_key = b"k05000".to_vec();
        let migrator = FjallRangeMigrator::new(keyspace.clone());
        migrator
            .migrate(0, 1, &start_key, &end_key)
            .expect("migrate range");

        for i in 0..KEY_COUNT {
            let key = format!("k{i:05}").into_bytes();
            let moved = key.as_slice() >= start_key.as_slice() && key.as_slice() < end_key.as_slice();

            let in_from_latest = from_latest.get(&key).expect("read from latest").is_some();
            let in_to_latest = to_latest.get(&key).expect("read to latest").is_some();
            assert_eq!(in_to_latest, moved, "latest to-shard mismatch for key {}", i);
            assert_eq!(in_from_latest, !moved, "latest from-shard mismatch for key {}", i);

            let mut version_key = Vec::with_capacity(4 + key.len() + 8 + 8 + 8);
            version_key.extend_from_slice(&(key.len() as u32).to_be_bytes());
            version_key.extend_from_slice(&key);
            version_key.extend_from_slice(&(i as u64).to_be_bytes());
            version_key.extend_from_slice(&1u64.to_be_bytes());
            version_key.extend_from_slice(&(i as u64).to_be_bytes());
            let in_from_versions = from_versions
                .get(&version_key)
                .expect("read from versions")
                .is_some();
            let in_to_versions = to_versions
                .get(&version_key)
                .expect("read to versions")
                .is_some();
            assert_eq!(in_to_versions, moved, "versions to-shard mismatch for key {}", i);
            assert_eq!(
                in_from_versions,
                !moved,
                "versions from-shard mismatch for key {}",
                i
            );
        }

        drop(to_latest);
        drop(to_versions);
        drop(from_latest);
        drop(from_versions);
        drop(keyspace);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

fn hash_key(bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

fn key_in_range(key: &[u8], start: &[u8], end: &[u8]) -> bool {
    let lower_ok = start.is_empty() || key >= start;
    let upper_ok = end.is_empty() || key < end;
    lower_ok && upper_ok
}

impl ShardRouter for ClusterStateStore {
    fn shard_for_key(&self, key: &[u8]) -> usize {
        self.shard_index_for_key(key)
    }
}

fn next_shard_id(shards: &[ShardDesc]) -> u64 {
    shards.iter().map(|s| s.shard_id).max().unwrap_or(0) + 1
}

// Note: shard_index values are stable across splits/merges for now; new ranges
// inherit their parent's shard_index until we support creating/moving data
// groups for new ranges.

impl StateMachine for ClusterStateMachine {
    fn command_keys(&self, _data: &[u8]) -> anyhow::Result<CommandKeys> {
        Ok(CommandKeys {
            reads: Vec::new(),
            writes: vec![b"cluster".to_vec()],
        })
    }

    fn apply(&self, data: &[u8], _meta: ExecMeta) {
        if let Ok(cmd) = Self::decode_command(data) {
            let res = match cmd {
                ClusterCommand::SplitRange {
                    split_key,
                    target_shard_index,
                } => self.store.apply_split_range(
                    split_key,
                    target_shard_index,
                    self.migrator.as_deref(),
                    self.shard_limit,
                ),
                other => self.store.apply_command(other),
            };
            if let Err(err) = res {
                tracing::error!(error = ?err, "cluster state apply failed");
            };
        } else {
            tracing::error!("cluster command decode failed");
        }
    }
}
