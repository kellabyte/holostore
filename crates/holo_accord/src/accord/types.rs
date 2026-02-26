//! Shared types for the Accord consensus engine.
//!
//! These types are kept in a small, dependency-light module because they are
//! used by both the consensus engine and the transport/state-machine layers.

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;

/// Logical identifier for an Accord group (shard).
pub type GroupId = u64;
/// Logical node identifier within a group.
pub type NodeId = u64;
/// Bit shift used to encode shard/group id in transaction counters.
pub const TXN_COUNTER_SHARD_SHIFT: u32 = 48;

/// Unique transaction identifier scoped by node and a monotonically increasing counter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TxnId {
    pub node_id: NodeId,
    pub counter: u64,
}

/// Derive the Accord group id from a transaction id.
pub fn txn_group_id(txn_id: TxnId) -> GroupId {
    if TXN_COUNTER_SHARD_SHIFT >= 64 {
        0
    } else {
        txn_id.counter >> TXN_COUNTER_SHARD_SHIFT
    }
}

/// Ballot used to resolve conflicts between competing proposals.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Ballot {
    pub counter: u64,
    pub node_id: NodeId,
}

impl Ballot {
    pub const fn zero() -> Self {
        Self {
            counter: 0,
            node_id: 0,
        }
    }

    pub const fn initial(node_id: NodeId) -> Self {
        Self {
            counter: 0,
            node_id,
        }
    }
}

impl Ord for Ballot {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.counter, self.node_id).cmp(&(other.counter, other.node_id))
    }
}

impl PartialOrd for Ballot {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug)]
pub struct Member {
    pub id: NodeId,
}

/// Per-group configuration and operational tuning.
///
/// The `*timeout` values guard against slow/failed peers. Batch settings are
/// used to amortize network and log overhead without sacrificing correctness.
#[derive(Clone, Debug)]
pub struct Config {
    pub group_id: GroupId,
    pub node_id: NodeId,
    pub members: Vec<Member>,

    /// Upper bound for point-to-point RPC waits used by protocol steps.
    pub rpc_timeout: Duration,
    /// End-to-end timeout for one propose attempt (pre-accept/accept/commit path).
    pub propose_timeout: Duration,
    /// Lower bound before triggering recovery for a blocked transaction.
    ///
    /// Design: avoids aggressive recovery churn on short-lived dependency waits.
    pub recovery_min_delay: Duration,
    /// Minimum spacing between executor-driven stall recovery probes.
    ///
    /// Design: keeps recovery bounded under heavy contention so execution work
    /// is not starved by repeated probe attempts.
    pub stall_recover_interval: Duration,
    pub preaccept_stall_hits: u32,
    pub execute_batch_max: usize,
    pub inline_command_in_accept_commit: bool,
    pub commit_log_batch_max: usize,
    pub commit_log_batch_wait: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecMeta {
    pub seq: u64,
    pub txn_id: TxnId,
}

impl Config {
    pub fn quorum(&self) -> usize {
        (self.members.len() / 2) + 1
    }

    pub fn fast_quorum(&self) -> usize {
        // For now we use an EPaxos-style fast quorum (N - f). With N=2f+1, this is a majority.
        self.quorum()
    }

    pub fn peers(&self) -> Vec<NodeId> {
        self.members
            .iter()
            .map(|m| m.id)
            .filter(|id| *id != self.node_id)
            .collect()
    }
}

#[derive(Clone, Debug, Default)]
pub struct CommandKeys {
    pub reads: Vec<Vec<u8>>,
    pub writes: Vec<Vec<u8>>,
}

impl CommandKeys {
    pub fn is_write(&self) -> bool {
        !self.writes.is_empty()
    }

    pub fn keys(&self) -> impl Iterator<Item = &Vec<u8>> {
        self.reads.iter().chain(self.writes.iter())
    }
}

/// Application-specific state machine driven by the consensus executor.
///
/// The consensus layer treats commands as opaque bytes; `command_keys` is used
/// for dependency tracking, while `apply`/`read`/`mark_visible` implement the
/// storage semantics.
pub trait StateMachine: Send + Sync + 'static {
    fn command_keys(&self, data: &[u8]) -> anyhow::Result<CommandKeys>;
    fn apply(&self, data: &[u8], meta: ExecMeta);
    fn apply_batch(&self, items: &[(Vec<u8>, ExecMeta)]) {
        for (data, meta) in items {
            self.apply(data, *meta);
        }
    }

    fn read(&self, _data: &[u8], _meta: ExecMeta) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn mark_visible(&self, _data: &[u8], _meta: ExecMeta) {}
}

#[derive(Clone, Debug)]
pub struct CommitLogEntry {
    pub txn_id: TxnId,
    pub seq: u64,
    pub deps: Vec<TxnId>,
    pub command: Vec<u8>,
}

/// WAL floor snapshot used for checkpoint/compaction observability.
#[derive(Clone, Debug, Default)]
pub struct CommitLogCheckpointStatus {
    /// Highest index that a durable storage checkpoint guarantees.
    pub durable_floor_by_group: BTreeMap<GroupId, u64>,
    /// Highest contiguous executed index tracked by the WAL.
    pub executed_floor_by_group: BTreeMap<GroupId, u64>,
    /// Highest index already removed from WAL storage.
    pub compacted_floor_by_group: BTreeMap<GroupId, u64>,
}

/// Durable commit log interface (WAL).
///
/// Implementations are responsible for persisting committed entries and
/// returning them on startup for replay.
pub trait CommitLog: Send + Sync + 'static {
    fn append_commit(&self, entry: CommitLogEntry) -> anyhow::Result<()>;
    fn append_commits(&self, entries: Vec<CommitLogEntry>) -> anyhow::Result<()> {
        for entry in entries {
            self.append_commit(entry)?;
        }
        Ok(())
    }
    fn mark_executed(&self, txn_id: TxnId) -> anyhow::Result<()>;
    fn load(&self) -> anyhow::Result<Vec<CommitLogEntry>>;
    fn compact(&self, max_delete: usize) -> anyhow::Result<usize>;
    /// Mark that KV/meta state has been durably checkpointed.
    ///
    /// WAL implementations can use this to safely gate log compaction.
    fn mark_durable_checkpoint(&self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Return checkpoint/compaction floor state for monitoring.
    fn checkpoint_status(&self) -> CommitLogCheckpointStatus {
        CommitLogCheckpointStatus::default()
    }
}

#[derive(Clone, Debug)]
pub struct PreAcceptRequest {
    pub group_id: GroupId,
    pub txn_id: TxnId,
    pub ballot: Ballot,
    pub command: Vec<u8>,
    pub seq: u64,
    pub deps: Vec<TxnId>,
}

#[derive(Clone, Debug)]
pub struct PreAcceptResponse {
    pub ok: bool,
    pub promised: Ballot,
    pub seq: u64,
    pub deps: Vec<TxnId>,
}

#[derive(Clone, Debug)]
pub struct AcceptRequest {
    pub group_id: GroupId,
    pub txn_id: TxnId,
    pub ballot: Ballot,
    pub command: Vec<u8>,
    pub command_digest: [u8; 32],
    pub has_command: bool,
    pub seq: u64,
    pub deps: Vec<TxnId>,
}

#[derive(Clone, Debug)]
pub struct AcceptResponse {
    pub ok: bool,
    pub promised: Ballot,
}

#[derive(Clone, Debug)]
pub struct CommitRequest {
    pub group_id: GroupId,
    pub txn_id: TxnId,
    pub ballot: Ballot,
    pub command: Vec<u8>,
    pub command_digest: [u8; 32],
    pub has_command: bool,
    pub seq: u64,
    pub deps: Vec<TxnId>,
}

#[derive(Clone, Debug)]
pub struct CommitResponse {
    pub ok: bool,
}

#[derive(Clone, Debug)]
pub struct RecoverRequest {
    pub group_id: GroupId,
    pub txn_id: TxnId,
    pub ballot: Ballot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnStatus {
    Unknown,
    PreAccepted,
    Accepted,
    Committed,
    Executed,
}

#[derive(Clone, Debug)]
pub struct RecoverResponse {
    pub ok: bool,
    pub promised: Ballot,
    pub status: TxnStatus,
    pub accepted_ballot: Option<Ballot>,
    pub command: Vec<u8>,
    pub seq: u64,
    pub deps: Vec<TxnId>,
}

/// Monotonic prefix of executed transactions per node.
#[derive(Clone, Debug)]
pub struct ExecutedPrefix {
    pub node_id: NodeId,
    pub counter: u64,
}

/// Gossip message sharing executed prefixes with peers.
#[derive(Clone, Debug)]
pub struct ReportExecutedRequest {
    pub group_id: GroupId,
    pub from_node_id: NodeId,
    pub prefixes: Vec<ExecutedPrefix>,
}

/// Ack for executed-prefix gossip.
#[derive(Clone, Debug)]
pub struct ReportExecutedResponse {
    pub ok: bool,
}

/// Transport interface for consensus RPCs.
///
/// The consensus engine is transport-agnostic; concrete implementations can
/// use gRPC, in-memory channels, or test harnesses.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn pre_accept(
        &self,
        target: NodeId,
        req: PreAcceptRequest,
    ) -> anyhow::Result<PreAcceptResponse>;

    async fn accept(&self, target: NodeId, req: AcceptRequest) -> anyhow::Result<AcceptResponse>;

    async fn commit(&self, target: NodeId, req: CommitRequest) -> anyhow::Result<CommitResponse>;

    async fn recover(&self, target: NodeId, req: RecoverRequest)
        -> anyhow::Result<RecoverResponse>;

    async fn fetch_command(
        &self,
        target: NodeId,
        group_id: GroupId,
        txn_id: TxnId,
    ) -> anyhow::Result<Option<Vec<u8>>>;

    async fn report_executed(
        &self,
        target: NodeId,
        req: ReportExecutedRequest,
    ) -> anyhow::Result<ReportExecutedResponse>;

    async fn last_executed_prefix(
        &self,
        target: NodeId,
        group_id: GroupId,
    ) -> anyhow::Result<Vec<ExecutedPrefix>>;

    async fn executed(
        &self,
        target: NodeId,
        group_id: GroupId,
        txn_id: TxnId,
    ) -> anyhow::Result<bool>;

    async fn mark_visible(
        &self,
        target: NodeId,
        group_id: GroupId,
        txn_id: TxnId,
    ) -> anyhow::Result<bool>;
}
