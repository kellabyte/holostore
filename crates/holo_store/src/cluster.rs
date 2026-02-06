//! Control-plane cluster metadata and membership state machine.

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::Context;
use holo_accord::accord::{CommandKeys, ExecMeta, NodeId, StateMachine};
use std::net::SocketAddr;
use serde::{Deserialize, Serialize};

use fjall::{Keyspace, PartitionCreateOptions};
use crate::kv::ShardRouter;
use crate::kv::encode_key_prefix;
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

/// Cluster-wide metadata stored in the meta group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterState {
    pub epoch: u64,
    pub frozen: bool,
    pub replication_factor: usize,
    pub members: BTreeMap<NodeId, MemberInfo>,
    pub shards: Vec<ShardDesc>,
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
    fn migrate(&self, from_shard: usize, to_shard: usize, start_key: &[u8], end_key: &[u8])
        -> anyhow::Result<()>;
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
        let versions =
            self.keyspace
                .open_partition(&versions_name, PartitionCreateOptions::default())?;
        let latest =
            self.keyspace
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
        let mut iter: Box<dyn DoubleEndedIterator<Item = fjall::Result<fjall::KvPair>>> =
            if end_key.is_empty() {
                Box::new(from_latest.range(start..))
            } else {
                Box::new(from_latest.range(start..end_key.to_vec()))
            };

        // Commit in chunks to keep memory bounded.
        const CHUNK_ITEMS: usize = 10_000;
        let mut batch = self.keyspace.batch();
        let mut queued = 0usize;

        while let Some(item) = iter.next() {
            let (key, latest_val) = item?;
            let key_bytes = key.to_vec();

            // Copy latest index entry.
            batch.insert(&to_latest, key_bytes.clone(), latest_val.to_vec());
            batch.remove(&from_latest, key_bytes.clone());
            queued += 2;

            // Copy all version entries for this key (same prefix).
            let prefix = encode_key_prefix(&key_bytes);
            for ver_item in from_versions.prefix(prefix) {
                let (ver_key, ver_val) = ver_item?;
                batch.insert(&to_versions, ver_key.to_vec(), ver_val.to_vec());
                batch.remove(&from_versions, ver_key.to_vec());
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
            if let Ok(state) = serde_json::from_slice::<ClusterState>(&data) {
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
            let addr: SocketAddr = member
                .grpc_addr
                .parse()
                .with_context(|| format!("invalid grpc addr for node {id}: {}", member.grpc_addr))?;
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
        state.epoch = state.epoch.saturating_add(1);

        drop(state);
        self.persist()
    }

    pub fn apply_command(&self, cmd: ClusterCommand) -> anyhow::Result<()> {
        let mut state = self.state.write().unwrap();
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
                // TODO: update shard placements when dynamic sharding is implemented.
            }
            ClusterCommand::RemoveNode { node_id } => {
                if let Some(member) = state.members.get_mut(&node_id) {
                    member.state = MemberState::Removed;
                    state.epoch = state.epoch.saturating_add(1);
                }
                for shard in &mut state.shards {
                    shard.replicas.retain(|id| *id != node_id);
                    if shard.leaseholder == node_id {
                        shard.leaseholder = shard.replicas.first().copied().unwrap_or(0);
                    }
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
                state.epoch = state.epoch.saturating_add(1);
            }
            ClusterCommand::SetReplicas {
                shard_id,
                replicas,
                leaseholder,
            } => {
                let shard = state
                    .shards
                    .iter_mut()
                    .find(|s| s.shard_id == shard_id)
                    .ok_or_else(|| anyhow::anyhow!("unknown shard id {shard_id}"))?;
                if replicas.is_empty() {
                    anyhow::bail!("replicas cannot be empty");
                }
                shard.replicas = replicas.clone();
                shard.leaseholder = if leaseholder == 0 {
                    replicas[0]
                } else {
                    leaseholder
                };
                state.epoch = state.epoch.saturating_add(1);
            }
        }
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
