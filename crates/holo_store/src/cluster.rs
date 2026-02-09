//! Control-plane cluster metadata and membership state machine.

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

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

/// Reconfiguration phase for a range-merge workflow.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RangeMergePhase {
    Preparing,
    Copying,
    Catchup,
    Cutover,
    Finalizing,
}

/// In-flight range merge tracked in control-plane metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeMerge {
    pub left_shard_id: u64,
    pub right_shard_id: u64,
    pub phase: RangeMergePhase,
    #[serde(default)]
    pub copied_rows: u64,
    #[serde(default)]
    pub copied_bytes: u64,
    #[serde(default)]
    pub lag_ops: u64,
    #[serde(default)]
    pub retry_count: u32,
    #[serde(default)]
    pub eta_seconds: u64,
    #[serde(default)]
    pub last_error: String,
    #[serde(default)]
    pub paused: bool,
    #[serde(default)]
    pub started_unix_ms: u64,
    #[serde(default)]
    pub last_progress_unix_ms: u64,
    #[serde(default)]
    pub cutover_epoch: Option<u64>,
}

/// Retired right-hand range metadata kept briefly before GC cleanup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetiredRange {
    pub shard_id: u64,
    pub shard_index: usize,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub retired_epoch: u64,
    pub gc_after_epoch: u64,
}

/// In-flight replica move tracked in the control-plane metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaMove {
    pub from_node: NodeId,
    pub to_node: NodeId,
    pub phase: ReplicaMovePhase,
    #[serde(default)]
    pub backfill_done: bool,
    #[serde(default)]
    pub target_leaseholder: Option<NodeId>,
    /// Wall-clock timestamp when this move was created.
    #[serde(default)]
    pub started_unix_ms: u64,
    /// Wall-clock timestamp when this move last advanced phase/progress.
    #[serde(default)]
    pub last_progress_unix_ms: u64,
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
    #[serde(default)]
    pub shard_merges: BTreeMap<u64, RangeMerge>,
    #[serde(default)]
    pub shard_fences: BTreeMap<u64, String>,
    #[serde(default)]
    pub retired_ranges: BTreeMap<u64, RetiredRange>,
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
    MarkReplicaMoveBackfilled {
        shard_id: u64,
    },
    AbortReplicaMove {
        shard_id: u64,
    },
    BeginRangeMerge {
        left_shard_id: u64,
        right_shard_id: u64,
    },
    AdvanceRangeMerge {
        left_shard_id: u64,
        phase: RangeMergePhase,
        copied_rows: u64,
        copied_bytes: u64,
        lag_ops: u64,
        retry_count: u32,
        eta_seconds: u64,
        last_error: String,
    },
    PauseRangeMerge {
        left_shard_id: u64,
        paused: bool,
    },
    AbortRangeMerge {
        left_shard_id: u64,
    },
    CompleteRangeMerge {
        left_shard_id: u64,
    },
    SetShardFence {
        shard_id: u64,
        fenced: bool,
        reason: String,
    },
    GcRetiredRange {
        shard_id: u64,
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
            shard_merges: BTreeMap::new(),
            shard_fences: BTreeMap::new(),
            retired_ranges: BTreeMap::new(),
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

    /// Install a full control-plane snapshot from a remote seed.
    ///
    /// The snapshot is only applied when it is at least as new as local state.
    pub fn install_snapshot(&self, mut snapshot: ClusterState) -> anyhow::Result<()> {
        normalize_replica_metadata(&mut snapshot);
        let mut state = self.state.write().unwrap();
        if snapshot.epoch < state.epoch {
            return Ok(());
        }
        *state = snapshot;
        drop(state);
        self.persist()
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

    pub fn shard_fence_reason(&self, shard_id: u64) -> Option<String> {
        self.state
            .read()
            .unwrap()
            .shard_fences
            .get(&shard_id)
            .cloned()
    }

    pub fn is_shard_fenced(&self, shard_id: u64) -> bool {
        self.state
            .read()
            .unwrap()
            .shard_fences
            .contains_key(&shard_id)
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
        if state
            .shard_merges
            .values()
            .any(|m| m.left_shard_id == shard_id || m.right_shard_id == shard_id)
        {
            anyhow::bail!("shard {shard_id} already has an in-flight merge");
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

        // Idempotent replay: a late-joining node may re-apply an already committed
        // split command from the shard log. Treat the exact same split boundary as
        // a no-op so metadata catch-up can continue.
        if let Some(existing_idx) = state
            .shards
            .iter()
            .position(|s| s.shard_index == target_shard_index)
        {
            let existing = &state.shards[existing_idx];
            let has_left_boundary = existing_idx > 0
                && state
                    .shards
                    .get(existing_idx.saturating_sub(1))
                    .map(|left| left.end_key == split_key)
                    .unwrap_or(false);
            if existing.start_key == split_key && has_left_boundary {
                return Ok(());
            }
            anyhow::bail!("target shard index {target_shard_index} already in use");
        }

        let idx = state
            .shards
            .iter()
            .position(|s| key_in_range(&split_key, &s.start_key, &s.end_key))
            .ok_or_else(|| anyhow::anyhow!("split key does not map to any shard"))?;

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

    pub fn apply_merge_range(
        &self,
        left_shard_id: u64,
        migrator: Option<&dyn RangeMigrator>,
    ) -> anyhow::Result<()> {
        // Block client request routing while we move keys and update descriptors.
        let _split_guard = self.split_lock.write().unwrap();

        let mut state = self.state.write().unwrap();
        normalize_replica_metadata(&mut state);

        let idx = match state
            .shards
            .iter()
            .position(|s| s.shard_id == left_shard_id)
        {
            Some(idx) => idx,
            None => anyhow::bail!("unknown shard id {left_shard_id}"),
        };
        let left_snapshot = state.shards[idx].clone();
        let right_snapshot = if idx + 1 < state.shards.len() {
            Some(state.shards[idx + 1].clone())
        } else {
            None
        };

        let Some(right_snapshot) = right_snapshot else {
            // Idempotent replay after cutover: shard already has no right neighbor.
            let epoch_now = state.epoch;
            if let Some(merge) = state.shard_merges.get_mut(&left_shard_id) {
                merge.phase = RangeMergePhase::Finalizing;
                merge.last_progress_unix_ms = unix_time_ms();
                if merge.cutover_epoch.is_none() {
                    merge.cutover_epoch = Some(epoch_now);
                }
            }
            return Ok(());
        };

        if !left_snapshot.end_key.is_empty() && left_snapshot.end_key != right_snapshot.start_key {
            anyhow::bail!("shards are not adjacent");
        }
        if left_snapshot.replicas != right_snapshot.replicas {
            anyhow::bail!(
                "cannot merge shards with different replica sets (left={:?}, right={:?})",
                left_snapshot.replicas,
                right_snapshot.replicas
            );
        }
        if left_snapshot.leaseholder != right_snapshot.leaseholder {
            anyhow::bail!(
                "cannot merge shards with different leaseholders (left={}, right={})",
                left_snapshot.leaseholder,
                right_snapshot.leaseholder
            );
        }
        if state.shard_rebalances.contains_key(&left_snapshot.shard_id)
            || state
                .shard_rebalances
                .contains_key(&right_snapshot.shard_id)
        {
            anyhow::bail!("cannot merge while either shard has in-flight rebalancing");
        }
        if state.shard_merges.iter().any(|(left, m)| {
            *left != left_shard_id
                && (m.left_shard_id == left_shard_id
                    || m.right_shard_id == left_shard_id
                    || m.left_shard_id == right_snapshot.shard_id
                    || m.right_shard_id == right_snapshot.shard_id)
        }) {
            anyhow::bail!("cannot merge while either shard has an in-flight merge");
        }

        // When data lives in different physical shard partitions, merge requires
        // moving right-hand keys into the left shard before descriptor cutover.
        if left_snapshot.shard_index != right_snapshot.shard_index {
            let Some(migrator) = migrator else {
                anyhow::bail!(
                    "cannot merge shards with different shard_index (left={}, right={}) without a range migrator",
                    left_snapshot.shard_index,
                    right_snapshot.shard_index
                );
            };
            migrator.migrate(
                right_snapshot.shard_index,
                left_snapshot.shard_index,
                &right_snapshot.start_key,
                &right_snapshot.end_key,
            )?;
        }

        let right = state.shards.remove(idx + 1);
        let left = &mut state.shards[idx];
        left.end_key = right.end_key.clone();
        state.shard_replica_roles.remove(&right.shard_id);
        state.shard_rebalances.remove(&right.shard_id);
        state.shard_fences.remove(&right.shard_id);
        let cutover_epoch = state.epoch.saturating_add(1);
        if let Some(merge) = state.shard_merges.get_mut(&left_shard_id) {
            merge.phase = RangeMergePhase::Finalizing;
            merge.last_progress_unix_ms = unix_time_ms();
            merge.cutover_epoch = Some(cutover_epoch);
        }
        state.epoch = cutover_epoch;
        let retired_epoch = state.epoch;
        let gc_after_epoch = retired_epoch.saturating_add(10);
        state.retired_ranges.insert(
            right.shard_id,
            RetiredRange {
                shard_id: right.shard_id,
                shard_index: right.shard_index,
                start_key: right.start_key.clone(),
                end_key: right.end_key.clone(),
                retired_epoch,
                gc_after_epoch,
            },
        );

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
                if let Some(existing) = state.shard_rebalances.get(&shard_id) {
                    if existing.from_node == from_node
                        && existing.to_node == to_node
                        && existing.target_leaseholder == target_leaseholder
                    {
                        // Idempotent replay of the same move intent.
                        return Ok(());
                    }
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
                        backfill_done: false,
                        target_leaseholder,
                        started_unix_ms: unix_time_ms(),
                        last_progress_unix_ms: unix_time_ms(),
                    },
                );
                state.epoch = state.epoch.saturating_add(1);
            }
            ClusterCommand::MarkReplicaMoveBackfilled { shard_id } => {
                let Some(mv) = state.shard_rebalances.get_mut(&shard_id) else {
                    // Idempotent replay after the move already finalized/aborted.
                    return Ok(());
                };
                if mv.backfill_done {
                    // Already marked done.
                    return Ok(());
                }
                if mv.phase != ReplicaMovePhase::LearnerSync {
                    // Move has already advanced beyond LearnerSync on this node.
                    return Ok(());
                }
                mv.backfill_done = true;
                mv.last_progress_unix_ms = unix_time_ms();
                state.epoch = state.epoch.saturating_add(1);
            }
            ClusterCommand::AbortReplicaMove { shard_id } => {
                let mv = state
                    .shard_rebalances
                    .get(&shard_id)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("shard {shard_id} has no in-flight move"))?;
                let shard_idx = state
                    .shards
                    .iter()
                    .position(|s| s.shard_id == shard_id)
                    .ok_or_else(|| anyhow::anyhow!("unknown shard id {shard_id}"))?;

                let replicas_snapshot = {
                    let shard = &mut state.shards[shard_idx];
                    if !shard.replicas.contains(&mv.from_node) {
                        shard.replicas.push(mv.from_node);
                    }
                    shard.replicas.retain(|id| *id != mv.to_node);
                    dedupe_nodes_in_place(&mut shard.replicas);
                    if shard.replicas.is_empty() {
                        anyhow::bail!("cannot abort move for shard {shard_id}: no replicas remain");
                    }

                    if shard.leaseholder == mv.to_node
                        || !shard.replicas.contains(&shard.leaseholder)
                    {
                        if shard.replicas.contains(&mv.from_node) {
                            shard.leaseholder = mv.from_node;
                        } else {
                            shard.leaseholder = shard.replicas[0];
                        }
                    }
                    shard.replicas.clone()
                };

                state
                    .shard_replica_roles
                    .insert(shard_id, default_roles_for_replicas(&replicas_snapshot));
                state.shard_rebalances.remove(&shard_id);
                state.epoch = state.epoch.saturating_add(1);
            }
            ClusterCommand::PromoteReplicaLearner { shard_id } => {
                let (to_node, from_node) = match state.shard_rebalances.get_mut(&shard_id) {
                    None => {
                        // Idempotent replay after completion.
                        return Ok(());
                    }
                    Some(mv) => {
                        if mv.phase != ReplicaMovePhase::LearnerSync {
                            // Already promoted or finalized.
                            return Ok(());
                        }
                        if !mv.backfill_done {
                            anyhow::bail!(
                                "shard {shard_id} learner cannot be promoted before backfill completes"
                            );
                        }
                        mv.phase = ReplicaMovePhase::JointConfig;
                        mv.last_progress_unix_ms = unix_time_ms();
                        (mv.to_node, mv.from_node)
                    }
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
                        mv.last_progress_unix_ms = unix_time_ms();
                    }
                }
                state.epoch = state.epoch.saturating_add(1);
            }
            ClusterCommand::FinalizeReplicaMove { shard_id } => {
                let Some(mv) = state.shard_rebalances.get(&shard_id).cloned() else {
                    // Idempotent replay after completion.
                    return Ok(());
                };
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
            ClusterCommand::BeginRangeMerge {
                left_shard_id,
                right_shard_id,
            } => {
                if state.shard_merges.contains_key(&left_shard_id) {
                    // Idempotent replay of already-started merge.
                    return Ok(());
                }
                let left_idx = state
                    .shards
                    .iter()
                    .position(|s| s.shard_id == left_shard_id)
                    .ok_or_else(|| anyhow::anyhow!("unknown left shard id {left_shard_id}"))?;
                if left_idx + 1 >= state.shards.len() {
                    anyhow::bail!("left shard {left_shard_id} has no right neighbor");
                }
                let right = &state.shards[left_idx + 1];
                if right.shard_id != right_shard_id {
                    anyhow::bail!(
                        "right neighbor mismatch for left shard {left_shard_id} (expected {right_shard_id}, got {})",
                        right.shard_id
                    );
                }
                if !state.shards[left_idx].end_key.is_empty()
                    && state.shards[left_idx].end_key != right.start_key
                {
                    anyhow::bail!("shards are not adjacent");
                }
                if state.shard_rebalances.contains_key(&left_shard_id)
                    || state.shard_rebalances.contains_key(&right_shard_id)
                {
                    anyhow::bail!(
                        "cannot start merge while either shard has in-flight rebalancing"
                    );
                }
                if state.shard_merges.values().any(|m| {
                    m.left_shard_id == left_shard_id
                        || m.right_shard_id == left_shard_id
                        || m.left_shard_id == right_shard_id
                        || m.right_shard_id == right_shard_id
                }) {
                    anyhow::bail!("cannot start merge while either shard has in-flight merge");
                }
                let now = unix_time_ms();
                state.shard_merges.insert(
                    left_shard_id,
                    RangeMerge {
                        left_shard_id,
                        right_shard_id,
                        phase: RangeMergePhase::Preparing,
                        copied_rows: 0,
                        copied_bytes: 0,
                        lag_ops: 0,
                        retry_count: 0,
                        eta_seconds: 0,
                        last_error: String::new(),
                        paused: false,
                        started_unix_ms: now,
                        last_progress_unix_ms: now,
                        cutover_epoch: None,
                    },
                );
                state.epoch = state.epoch.saturating_add(1);
            }
            ClusterCommand::AdvanceRangeMerge {
                left_shard_id,
                phase,
                copied_rows,
                copied_bytes,
                lag_ops,
                retry_count,
                eta_seconds,
                last_error,
            } => {
                let Some(merge) = state.shard_merges.get_mut(&left_shard_id) else {
                    // Merge may already be finalized.
                    return Ok(());
                };
                merge.phase = phase;
                merge.copied_rows = copied_rows;
                merge.copied_bytes = copied_bytes;
                merge.lag_ops = lag_ops;
                merge.retry_count = retry_count;
                merge.eta_seconds = eta_seconds;
                merge.last_error = last_error;
                merge.last_progress_unix_ms = unix_time_ms();
                state.epoch = state.epoch.saturating_add(1);
            }
            ClusterCommand::PauseRangeMerge {
                left_shard_id,
                paused,
            } => {
                let Some(merge) = state.shard_merges.get_mut(&left_shard_id) else {
                    return Ok(());
                };
                if merge.paused != paused {
                    merge.paused = paused;
                    merge.last_progress_unix_ms = unix_time_ms();
                    state.epoch = state.epoch.saturating_add(1);
                }
            }
            ClusterCommand::AbortRangeMerge { left_shard_id } => {
                if let Some(existing) = state.shard_merges.get(&left_shard_id) {
                    if existing.cutover_epoch.is_some()
                        || matches!(existing.phase, RangeMergePhase::Finalizing)
                    {
                        anyhow::bail!(
                            "cannot abort merge for shard {left_shard_id} after cutover; finalize instead"
                        );
                    }
                }
                if let Some(merge) = state.shard_merges.remove(&left_shard_id) {
                    state.shard_fences.remove(&merge.left_shard_id);
                    state.shard_fences.remove(&merge.right_shard_id);
                    state.epoch = state.epoch.saturating_add(1);
                }
            }
            ClusterCommand::CompleteRangeMerge { left_shard_id } => {
                if let Some(merge) = state.shard_merges.remove(&left_shard_id) {
                    state.shard_fences.remove(&merge.left_shard_id);
                    state.shard_fences.remove(&merge.right_shard_id);
                    state.epoch = state.epoch.saturating_add(1);
                }
            }
            ClusterCommand::SetShardFence {
                shard_id,
                fenced,
                reason,
            } => {
                let changed = if fenced {
                    let prev = state.shard_fences.insert(shard_id, reason.clone());
                    prev.as_deref() != Some(reason.as_str())
                } else {
                    state.shard_fences.remove(&shard_id).is_some()
                };
                if changed {
                    state.epoch = state.epoch.saturating_add(1);
                }
            }
            ClusterCommand::GcRetiredRange { shard_id } => {
                if state.retired_ranges.remove(&shard_id).is_some() {
                    state.epoch = state.epoch.saturating_add(1);
                }
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
            ClusterCommand::MergeRange { .. } => {
                // Merge operations must run through `apply_merge_range` so keys
                // are moved before descriptor cutover.
                anyhow::bail!("MergeRange must be applied via apply_merge_range");
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
    let active_shards = state
        .shards
        .iter()
        .map(|s| s.shard_id)
        .collect::<std::collections::BTreeSet<_>>();

    state
        .shard_replica_roles
        .retain(|shard_id, _| state.shards.iter().any(|s| s.shard_id == *shard_id));
    state.shard_rebalances.retain(|shard_id, mv| {
        let Some(shard) = state.shards.iter().find(|s| s.shard_id == *shard_id) else {
            return false;
        };
        shard.replicas.contains(&mv.from_node) && shard.replicas.contains(&mv.to_node)
    });
    state.shard_merges.retain(|left_shard_id, merge| {
        if *left_shard_id != merge.left_shard_id {
            return false;
        }
        if merge.started_unix_ms == 0 {
            merge.started_unix_ms = unix_time_ms();
        }
        if merge.last_progress_unix_ms == 0 {
            merge.last_progress_unix_ms = merge.started_unix_ms;
        }
        let left_exists = active_shards.contains(&merge.left_shard_id);
        let right_exists = active_shards.contains(&merge.right_shard_id);
        // Keep finalizing merges after right shard cutover until explicit completion.
        if merge.phase == RangeMergePhase::Finalizing {
            return left_exists;
        }
        left_exists && right_exists
    });
    state
        .shard_fences
        .retain(|shard_id, _| active_shards.contains(shard_id));
    state.retired_ranges.retain(|shard_id, retired| {
        // Retired ranges should no longer be active shards.
        !active_shards.contains(shard_id) && retired.shard_id == *shard_id
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
            if mv.started_unix_ms == 0 {
                mv.started_unix_ms = unix_time_ms();
            }
            if mv.last_progress_unix_ms == 0 {
                mv.last_progress_unix_ms = mv.started_unix_ms;
            }
        }
    }
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
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
    use std::sync::Mutex;
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
            shard_merges: BTreeMap::new(),
            shard_fences: BTreeMap::new(),
            retired_ranges: BTreeMap::new(),
        }
    }

    #[derive(Default)]
    struct MockMigrator {
        calls: Mutex<Vec<(usize, usize, Vec<u8>, Vec<u8>)>>,
    }

    impl RangeMigrator for MockMigrator {
        fn migrate(
            &self,
            from_shard: usize,
            to_shard: usize,
            start_key: &[u8],
            end_key: &[u8],
        ) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push((
                from_shard,
                to_shard,
                start_key.to_vec(),
                end_key.to_vec(),
            ));
            Ok(())
        }
    }

    fn two_shard_state() -> ClusterState {
        let mut state = base_state();
        state.shards = vec![
            ShardDesc {
                shard_id: 10,
                shard_index: 0,
                start_hash: 0,
                end_hash: 0,
                start_key: vec![],
                end_key: b"m".to_vec(),
                replicas: vec![1, 2, 3],
                leaseholder: 1,
            },
            ShardDesc {
                shard_id: 11,
                shard_index: 1,
                start_hash: 0,
                end_hash: 0,
                start_key: b"m".to_vec(),
                end_key: vec![],
                replicas: vec![1, 2, 3],
                leaseholder: 1,
            },
        ];
        state.shard_replica_roles.insert(
            10,
            BTreeMap::from([
                (1, ReplicaRole::Voter),
                (2, ReplicaRole::Voter),
                (3, ReplicaRole::Voter),
            ]),
        );
        state.shard_replica_roles.insert(
            11,
            BTreeMap::from([
                (1, ReplicaRole::Voter),
                (2, ReplicaRole::Voter),
                (3, ReplicaRole::Voter),
            ]),
        );
        state
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
        assert!(err
            .to_string()
            .contains("replacing exactly one replica at a time"));
    }

    #[test]
    fn split_range_is_idempotent_on_replay() {
        let store = test_store(base_state());
        let split_key = b"key:050000".to_vec();

        store
            .apply_split_range(split_key.clone(), 1, None, 8)
            .expect("first split should apply");
        let epoch_after_first = store.state().epoch;

        store
            .apply_split_range(split_key.clone(), 1, None, 8)
            .expect("replayed split should be a no-op");
        let state = store.state();
        assert_eq!(
            state.epoch, epoch_after_first,
            "idempotent replay should not bump epoch"
        );
        assert_eq!(
            state.shards.len(),
            2,
            "replayed split must not duplicate ranges"
        );
        assert_eq!(state.shards[1].shard_index, 1);
        assert_eq!(state.shards[1].start_key, split_key);
    }

    #[test]
    fn merge_range_moves_data_and_updates_descriptors() {
        let store = test_store(two_shard_state());
        let migrator = MockMigrator::default();

        let before_epoch = store.state().epoch;
        store
            .apply_merge_range(10, Some(&migrator))
            .expect("merge should apply");

        let state = store.state();
        assert_eq!(state.epoch, before_epoch + 1);
        assert_eq!(state.shards.len(), 1);
        assert_eq!(state.shards[0].shard_id, 10);
        assert_eq!(state.shards[0].shard_index, 0);
        assert_eq!(state.shards[0].start_key, Vec::<u8>::new());
        assert_eq!(state.shards[0].end_key, Vec::<u8>::new());
        assert!(
            !state.shard_replica_roles.contains_key(&11),
            "right shard role metadata must be removed"
        );

        let calls = migrator.calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "migrator should be invoked exactly once");
        let (from_shard, to_shard, start_key, end_key) = &calls[0];
        assert_eq!((*from_shard, *to_shard), (1, 0));
        assert_eq!(start_key.as_slice(), b"m");
        assert!(end_key.is_empty());
    }

    #[test]
    fn merge_range_requires_migrator_for_cross_shard_merge() {
        let store = test_store(two_shard_state());
        let err = store
            .apply_merge_range(10, None)
            .expect_err("merge without migrator should fail");
        assert!(
            err.to_string().contains("without a range migrator"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn abort_replica_move_restores_original_layout() {
        let store = test_store(base_state());
        store
            .apply_command(ClusterCommand::BeginReplicaMove {
                shard_id: 10,
                from_node: 1,
                to_node: 4,
                target_leaseholder: None,
            })
            .expect("begin move");
        store
            .apply_command(ClusterCommand::AbortReplicaMove { shard_id: 10 })
            .expect("abort move");

        let state = store.state();
        let shard = state
            .shards
            .iter()
            .find(|s| s.shard_id == 10)
            .expect("shard");
        assert_eq!(shard.replicas, vec![1, 2, 3]);
        assert_eq!(shard.leaseholder, 1);
        assert!(state.shard_rebalances.is_empty());
        let roles = state.shard_replica_roles.get(&10).expect("shard roles");
        assert_eq!(roles.get(&1), Some(&ReplicaRole::Voter));
        assert_eq!(roles.get(&2), Some(&ReplicaRole::Voter));
        assert_eq!(roles.get(&3), Some(&ReplicaRole::Voter));
        assert!(!roles.contains_key(&4));
    }

    #[test]
    fn abort_replica_move_after_lease_transfer_restores_source_leaseholder() {
        let store = test_store(base_state());
        store
            .apply_command(ClusterCommand::BeginReplicaMove {
                shard_id: 10,
                from_node: 1,
                to_node: 4,
                target_leaseholder: Some(4),
            })
            .expect("begin move");
        store
            .apply_command(ClusterCommand::MarkReplicaMoveBackfilled { shard_id: 10 })
            .expect("mark backfilled");
        store
            .apply_command(ClusterCommand::PromoteReplicaLearner { shard_id: 10 })
            .expect("promote learner");
        store
            .apply_command(ClusterCommand::TransferShardLease {
                shard_id: 10,
                leaseholder: 4,
            })
            .expect("transfer lease");

        store
            .apply_command(ClusterCommand::AbortReplicaMove { shard_id: 10 })
            .expect("abort move");

        let state = store.state();
        let shard = state
            .shards
            .iter()
            .find(|s| s.shard_id == 10)
            .expect("shard");
        assert_eq!(shard.replicas, vec![1, 2, 3]);
        assert_eq!(shard.leaseholder, 1);
        assert!(state.shard_rebalances.is_empty());
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
            let moved =
                key.as_slice() >= start_key.as_slice() && key.as_slice() < end_key.as_slice();

            let in_from_latest = from_latest.get(&key).expect("read from latest").is_some();
            let in_to_latest = to_latest.get(&key).expect("read to latest").is_some();
            assert_eq!(
                in_to_latest, moved,
                "latest to-shard mismatch for key {}",
                i
            );
            assert_eq!(
                in_from_latest, !moved,
                "latest from-shard mismatch for key {}",
                i
            );

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
            assert_eq!(
                in_to_versions, moved,
                "versions to-shard mismatch for key {}",
                i
            );
            assert_eq!(
                in_from_versions, !moved,
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
                ClusterCommand::MergeRange { left_shard_id } => self
                    .store
                    .apply_merge_range(left_shard_id, self.migrator.as_deref()),
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
