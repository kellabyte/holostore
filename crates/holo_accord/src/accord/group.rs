//! Consensus engine and executor for a single Accord group.
//!
//! Purpose:
//! - Drive proposal, recovery, and execution for one replicated Accord group.
//!
//! Design:
//! - Implements quorum rounds (PreAccept/Accept/Commit), dependency tracking,
//!   and executor progression with bounded background workers.
//! - Keeps critical proposal paths allocation-light and cancellation-friendly so
//!   quorum completion does not wait on the slowest replica.
//!
//! Inputs:
//! - Client commands, peer RPC responses, runtime membership/voter updates, and
//!   durability configuration.
//!
//! Outputs:
//! - Linearizable replicated decisions, state-machine apply/read results,
//!   commit-log progress, and runtime metrics/debug counters.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::RwLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use bytes::Bytes;
use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::sync::{mpsc, oneshot, Mutex, Notify};
use tokio::time;

use super::state::{
    is_monotonic_update, status_to_txn_status, ExecutedLogEntry, Record, State, Status,
};
use super::types::{
    txn_group_id, AcceptRequest, AcceptResponse, Ballot, CommandKeys, CommitLog,
    CommitLogAppendOptions, CommitLogEntry, CommitRequest, CommitResponse, Config, ExecMeta,
    ExecutedPrefix, Member, NodeId, PreAcceptRequest, PreAcceptResponse, RecoverRequest,
    RecoverResponse, ReportExecutedRequest, ReportExecutedResponse, StateMachine, Transport, TxnId,
    TxnStatus, TXN_COUNTER_SHARD_SHIFT,
};

const COMPACT_EVERY_APPLIED: u64 = 1024;
const COMPACT_MAX_DELETE: usize = 4096;

fn txn_local_mask() -> u64 {
    if TXN_COUNTER_SHARD_SHIFT >= 64 {
        u64::MAX
    } else {
        (1u64 << TXN_COUNTER_SHARD_SHIFT) - 1
    }
}

fn txn_local_counter(counter: u64) -> u64 {
    counter & txn_local_mask()
}

fn initial_txn_counter_seed() -> u64 {
    let now_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(u128::from(u64::MAX)) as u64;
    txn_local_counter(now_us)
}

fn command_digest(command: &[u8]) -> [u8; 32] {
    *blake3::hash(command).as_bytes()
}

/// Force command bytes into an owned buffer so long-lived state does not keep
/// references to larger transient RPC decode buffers.
fn detach_command_bytes(command: &Bytes) -> Bytes {
    if command.is_empty() {
        Bytes::new()
    } else {
        Bytes::copy_from_slice(command.as_ref())
    }
}

/// Build recover-response command fields from optional bytes/digest.
///
/// Purpose:
/// - Encode command presence explicitly so recovery can differentiate a
///   committed NOOP from a missing non-empty command.
///
/// Design:
/// - `has_command` is `true` only when command bytes are present.
/// - `command_digest` is populated from bytes when present, otherwise from the
///   provided digest hint.
///
/// Inputs:
/// - `command`: optional recovered command bytes.
/// - `digest_hint`: optional digest sourced from record/executed metadata.
///
/// Outputs:
/// - Tuple `(command_bytes, command_digest, has_command)` used by
///   `RecoverResponse`.
fn recover_response_payload(
    command: Option<Bytes>,
    digest_hint: Option<[u8; 32]>,
) -> (Bytes, Option<[u8; 32]>, bool) {
    if let Some(command) = command {
        let digest = command_digest(&command);
        return (command, Some(digest), true);
    }
    (Bytes::new(), digest_hint, false)
}

/// Lightweight handle used by callers to submit proposals.
#[derive(Clone)]
pub struct Handle {
    group: Arc<Group>,
}

/// Result of a proposal: either applied (write) or a read response.
#[derive(Clone, Debug)]
pub enum ProposalResult {
    Applied,
    Read(Option<Vec<u8>>),
}

#[derive(Clone, Copy, Debug)]
/// Coordinator-observed timing breakdown for one commit round.
///
/// Purpose:
/// - Split commit critical-path latency into local handler work and remote
///   quorum wait so benchmarks can identify the dominant safe optimization.
///
/// Design:
/// - `local_state_update_us` covers coordinator-local `rpc_commit` work other
///   than synchronous durable WAL waiting.
/// - `local_durable_wait_us` isolates fsync/WAL wait when `SyncCommit` is
///   enabled.
/// - `local_log_queue_wait_us` / `local_log_append_us` split durable wait into
///   group-local queueing and append execution.
/// - `local_post_durable_state_update_us` captures the final state mutation
///   after durable WAL completion.
/// - `remote_quorum_wait_us` measures the extra wall time needed to collect
///   remote voter quorum ACKs after the local member commits.
///
/// Inputs:
/// - Populated inside `run_commit_round`.
///
/// Outputs:
/// - One passive timing sample recorded into `GroupMetrics`.
struct CommitRoundTimings {
    local_state_update_us: u64,
    local_durable_wait_us: u64,
    local_log_queue_wait_us: u64,
    local_log_append_us: u64,
    local_post_durable_state_update_us: u64,
    remote_quorum_wait_us: u64,
}

#[derive(Clone, Copy, Debug, Default)]
/// Timing breakdown for one local `rpc_commit` execution.
///
/// Purpose:
/// - Let the coordinator distinguish durable WAL wait from other local commit
///   bookkeeping without changing RPC semantics or wire format.
///
/// Design:
/// - `total_us` is the full local handler wall time.
/// - `durable_wait_us` covers synchronous commit-log queue wait plus append
///   execution while waiting for durable ACK.
/// - `durable_queue_wait_us` isolates time spent waiting in the group-local
///   commit-log batcher queue.
/// - `durable_append_us` isolates the storage append call itself.
/// - `post_durable_state_update_us` captures the final committed-state updates
///   that run after durable WAL completion.
/// - `state_update_us` is derived as `total_us - durable_wait_us`.
///
/// Inputs:
/// - Measured inside `rpc_commit_with_timings`.
///
/// Outputs:
/// - Returned only to local callers such as `run_commit_round`; network callers
///   still receive plain `CommitResponse`.
struct CommitRpcTimings {
    total_us: u64,
    durable_wait_us: u64,
    durable_queue_wait_us: u64,
    durable_append_us: u64,
    state_update_us: u64,
    post_durable_state_update_us: u64,
}

#[derive(Clone, Copy, Debug)]
struct PhaseTimings {
    pre_accept_us: u64,
    accept_us: u64,
    commit_us: u64,
    commit_local_state_update_us: u64,
    commit_local_durable_wait_us: u64,
    commit_local_log_queue_wait_us: u64,
    commit_local_log_append_us: u64,
    commit_local_post_durable_state_update_us: u64,
    commit_remote_quorum_wait_us: u64,
    execute_us: u64,
    visible_us: u64,
}

/// One synchronous commit-log append completion observed by the coordinator.
///
/// Purpose:
/// - Attribute sync-commit latency into queueing inside the group-local
///   commit-log worker and actual append execution time.
///
/// Design:
/// - Produced by the group-local commit-log batcher, not by the storage engine.
/// - `queue_wait_us` is measured from enqueue to batch-start.
/// - `append_us` is the wall time spent inside `append_commits_with_options`.
///
/// Inputs:
/// - Filled when the commit-log worker completes one queued append request.
///
/// Outputs:
/// - Returned to `rpc_commit_with_timings` through the existing completion
///   channel without changing protocol semantics.
#[derive(Clone, Copy, Debug, Default)]
struct CommitLogAppendCompletion {
    queue_wait_us: u64,
    append_us: u64,
}

/// Aggregated count/latency for the peer that most often closes commit quorum.
///
/// Purpose:
/// - Surface which replica tends to be the quorum-closing responder on commit
///   rounds so follow-up work can focus on that follower path.
///
/// Design:
/// - Stored as raw counters so shard/group snapshots can merge losslessly.
/// - `total_us` / `max_us` are measured at the moment quorum is satisfied.
///
/// Inputs:
/// - Recorded only when one remote voter response increases `ok` to quorum.
///
/// Outputs:
/// - Exported through `DebugStats` for `HOLOSTATS` and periodic logs.
#[derive(Clone, Debug, Default)]
pub struct CommitQuorumCloserStat {
    pub node_id: NodeId,
    pub count: u64,
    pub total_us: u64,
    pub max_us: u64,
}

/// Snapshot of group internals for debugging / metrics.
#[derive(Clone, Debug)]
pub struct DebugStats {
    pub records_len: usize,
    pub records_capacity: usize,
    pub records_status_none_len: usize,
    pub records_status_preaccepted_len: usize,
    pub records_status_accepted_len: usize,
    pub records_status_committed_len: usize,
    pub records_status_executing_len: usize,
    pub records_status_executed_len: usize,
    pub records_missing_command_len: usize,
    pub records_missing_keys_len: usize,
    pub records_committed_missing_command_len: usize,
    pub records_committed_missing_keys_len: usize,
    pub committed_queue_len: usize,
    pub committed_queue_ghost_len: usize,
    pub frontier_keys_len: usize,
    pub frontier_entries_len: usize,
    pub executed_prefix_nodes: usize,
    pub executed_out_of_order_len: usize,
    pub executed_log_len: usize,
    pub executed_log_capacity: usize,
    pub executed_log_order_capacity: usize,
    pub executed_log_command_bytes: usize,
    pub executed_log_max_command_bytes: usize,
    pub executed_log_deps_total: usize,
    pub executed_log_max_deps_len: usize,
    pub reported_executed_peers: usize,
    pub recovering_len: usize,
    pub read_waiters_len: usize,
    pub proposal_timeouts: u64,
    pub execute_timeouts: u64,
    pub recovery_attempts: u64,
    pub recovery_successes: u64,
    pub recovery_failures: u64,
    pub recovery_timeouts: u64,
    pub recovery_noops: u64,
    pub recovery_last_ms: u64,
    pub exec_progress_count: u64,
    pub exec_progress_total_us: u64,
    pub exec_progress_max_us: u64,
    pub exec_progress_true: u64,
    pub exec_progress_false: u64,
    pub exec_progress_errors: u64,
    pub exec_recover_count: u64,
    pub exec_recover_total_us: u64,
    pub exec_recover_max_us: u64,
    pub exec_recover_true: u64,
    pub exec_recover_false: u64,
    pub exec_recover_errors: u64,
    pub apply_write_count: u64,
    pub apply_write_total_us: u64,
    pub apply_write_max_us: u64,
    pub apply_read_count: u64,
    pub apply_read_total_us: u64,
    pub apply_read_max_us: u64,
    pub apply_batch_count: u64,
    pub apply_batch_total_us: u64,
    pub apply_batch_max_us: u64,
    pub mark_visible_count: u64,
    pub mark_visible_total_us: u64,
    pub mark_visible_max_us: u64,
    pub state_update_count: u64,
    pub state_update_total_us: u64,
    pub state_update_max_us: u64,
    pub commit_local_state_count: u64,
    pub commit_local_state_total_us: u64,
    pub commit_local_state_max_us: u64,
    pub commit_local_durable_count: u64,
    pub commit_local_durable_total_us: u64,
    pub commit_local_durable_max_us: u64,
    pub commit_local_log_queue_count: u64,
    pub commit_local_log_queue_total_us: u64,
    pub commit_local_log_queue_max_us: u64,
    pub commit_local_log_append_count: u64,
    pub commit_local_log_append_total_us: u64,
    pub commit_local_log_append_max_us: u64,
    pub commit_local_post_durable_state_count: u64,
    pub commit_local_post_durable_state_total_us: u64,
    pub commit_local_post_durable_state_max_us: u64,
    pub commit_remote_quorum_count: u64,
    pub commit_remote_quorum_total_us: u64,
    pub commit_remote_quorum_max_us: u64,
    pub commit_tail_count: u64,
    pub commit_tail_total_us: u64,
    pub commit_tail_max_us: u64,
    pub commit_quorum_closer_top: Vec<CommitQuorumCloserStat>,
    pub fast_path_count: u64,
    pub slow_path_count: u64,
}

impl Handle {
    pub async fn propose(&self, command: impl Into<Bytes>) -> anyhow::Result<ProposalResult> {
        self.group.propose(command.into()).await
    }

    pub async fn propose_read_with_deps(
        &self,
        command: impl Into<Bytes>,
        deps: Vec<(TxnId, u64)>,
    ) -> anyhow::Result<ProposalResult> {
        self.group
            .propose_read_with_deps(command.into(), deps)
            .await
    }
}

/// The core consensus group: owns transport, state, and executor machinery.
///
/// Design notes:
/// - Proposals are processed on the async executor.
/// - Commit log appends are batched on a dedicated thread.
/// - State-machine application is offloaded to a worker to avoid blocking the
///   async runtime during heavy write batches.
pub struct Group {
    config: Config,
    members: RwLock<Vec<Member>>,
    voters: RwLock<Vec<NodeId>>,
    transport: Arc<dyn Transport>,
    sm: Arc<dyn StateMachine>,
    commit_log: Option<Arc<dyn CommitLog>>,
    commit_log_tx: Option<std_mpsc::Sender<CommitLogWork>>,
    apply_tx: Option<std_mpsc::Sender<ApplyWork>>,
    state: Mutex<State>,
    execute_lock: Mutex<()>,
    executor_notify: Notify,
    executor_started: AtomicBool,
    metrics: Arc<GroupMetrics>,
    compact_counter: AtomicU64,
    peer_rr: AtomicU64,
    start_at: time::Instant,
}

/// Internal execution plan item (one committed txn).
#[derive(Clone, Debug)]
struct ApplyItem {
    id: TxnId,
    command: Bytes,
    keys: CommandKeys,
    seq: u64,
}

/// Work item sent to the apply worker (write batch + response channel).
struct ApplyWork {
    batch: Vec<(Bytes, ExecMeta)>,
    tx: oneshot::Sender<ApplyResult>,
}

/// Work item sent to the commit-log batcher.
///
/// Inputs:
/// - `entry`: commit record to append to the WAL.
/// - `require_durable`: whether this caller requires fsync-on-ack semantics.
/// - `done_tx`: completion channel that receives the final append result plus
///   passive queue/append timing details.
/// - `enqueued_at`: time when the request entered the group-local batcher.
///
/// Design:
/// - Multiple items can be batched together; if any item requires durable
///   persistence, the whole batch is appended with `require_durable=true`.
struct CommitLogWork {
    entry: CommitLogEntry,
    require_durable: bool,
    done_tx: std_mpsc::Sender<anyhow::Result<CommitLogAppendCompletion>>,
    enqueued_at: std::time::Instant,
}

struct ApplyResult {
    apply_us: u64,
    visible_us: u64,
    result: anyhow::Result<()>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecoveryKind {
    Value,
    Noop,
}

#[derive(Debug)]
struct BlockedStep {
    id: TxnId,
    status: Status,
    blocking_dep: Option<TxnId>,
    blocking_status: Option<Status>,
    blocking_missing: bool,
}

/// Atomically updated counters for `DebugStats`.
#[derive(Default)]
struct GroupMetrics {
    exec_progress_count: AtomicU64,
    exec_progress_total_us: AtomicU64,
    exec_progress_max_us: AtomicU64,
    exec_progress_true: AtomicU64,
    exec_progress_false: AtomicU64,
    exec_progress_errors: AtomicU64,
    exec_recover_count: AtomicU64,
    exec_recover_total_us: AtomicU64,
    exec_recover_max_us: AtomicU64,
    exec_recover_true: AtomicU64,
    exec_recover_false: AtomicU64,
    exec_recover_errors: AtomicU64,
    exec_progress_false_streak: AtomicU64,
    apply_write_count: AtomicU64,
    apply_write_total_us: AtomicU64,
    apply_write_max_us: AtomicU64,
    apply_read_count: AtomicU64,
    apply_read_total_us: AtomicU64,
    apply_read_max_us: AtomicU64,
    apply_batch_count: AtomicU64,
    apply_batch_total_us: AtomicU64,
    apply_batch_max_us: AtomicU64,
    mark_visible_count: AtomicU64,
    mark_visible_total_us: AtomicU64,
    mark_visible_max_us: AtomicU64,
    state_update_count: AtomicU64,
    state_update_total_us: AtomicU64,
    state_update_max_us: AtomicU64,
    commit_local_state_count: AtomicU64,
    commit_local_state_total_us: AtomicU64,
    commit_local_state_max_us: AtomicU64,
    commit_local_durable_count: AtomicU64,
    commit_local_durable_total_us: AtomicU64,
    commit_local_durable_max_us: AtomicU64,
    commit_local_log_queue_count: AtomicU64,
    commit_local_log_queue_total_us: AtomicU64,
    commit_local_log_queue_max_us: AtomicU64,
    commit_local_log_append_count: AtomicU64,
    commit_local_log_append_total_us: AtomicU64,
    commit_local_log_append_max_us: AtomicU64,
    commit_local_post_durable_state_count: AtomicU64,
    commit_local_post_durable_state_total_us: AtomicU64,
    commit_local_post_durable_state_max_us: AtomicU64,
    commit_remote_quorum_count: AtomicU64,
    commit_remote_quorum_total_us: AtomicU64,
    commit_remote_quorum_max_us: AtomicU64,
    commit_tail_count: AtomicU64,
    commit_tail_total_us: AtomicU64,
    commit_tail_max_us: AtomicU64,
    commit_quorum_closer: StdMutex<HashMap<NodeId, CommitQuorumCloserStat>>,
    exec_stall_log_at_us: AtomicU64,
    exec_stall_recover_at_us: AtomicU64,
    fast_path_count: AtomicU64,
    slow_path_count: AtomicU64,
}

#[derive(Default, Clone, Debug)]
struct MetricsSnapshot {
    exec_progress_count: u64,
    exec_progress_total_us: u64,
    exec_progress_max_us: u64,
    exec_progress_true: u64,
    exec_progress_false: u64,
    exec_progress_errors: u64,
    exec_recover_count: u64,
    exec_recover_total_us: u64,
    exec_recover_max_us: u64,
    exec_recover_true: u64,
    exec_recover_false: u64,
    exec_recover_errors: u64,
    apply_write_count: u64,
    apply_write_total_us: u64,
    apply_write_max_us: u64,
    apply_read_count: u64,
    apply_read_total_us: u64,
    apply_read_max_us: u64,
    apply_batch_count: u64,
    apply_batch_total_us: u64,
    apply_batch_max_us: u64,
    mark_visible_count: u64,
    mark_visible_total_us: u64,
    mark_visible_max_us: u64,
    state_update_count: u64,
    state_update_total_us: u64,
    state_update_max_us: u64,
    commit_local_state_count: u64,
    commit_local_state_total_us: u64,
    commit_local_state_max_us: u64,
    commit_local_durable_count: u64,
    commit_local_durable_total_us: u64,
    commit_local_durable_max_us: u64,
    commit_local_log_queue_count: u64,
    commit_local_log_queue_total_us: u64,
    commit_local_log_queue_max_us: u64,
    commit_local_log_append_count: u64,
    commit_local_log_append_total_us: u64,
    commit_local_log_append_max_us: u64,
    commit_local_post_durable_state_count: u64,
    commit_local_post_durable_state_total_us: u64,
    commit_local_post_durable_state_max_us: u64,
    commit_remote_quorum_count: u64,
    commit_remote_quorum_total_us: u64,
    commit_remote_quorum_max_us: u64,
    commit_tail_count: u64,
    commit_tail_total_us: u64,
    commit_tail_max_us: u64,
    fast_path_count: u64,
    slow_path_count: u64,
}

impl GroupMetrics {
    fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            exec_progress_count: self.exec_progress_count.load(Ordering::Relaxed),
            exec_progress_total_us: self.exec_progress_total_us.load(Ordering::Relaxed),
            exec_progress_max_us: self.exec_progress_max_us.load(Ordering::Relaxed),
            exec_progress_true: self.exec_progress_true.load(Ordering::Relaxed),
            exec_progress_false: self.exec_progress_false.load(Ordering::Relaxed),
            exec_progress_errors: self.exec_progress_errors.load(Ordering::Relaxed),
            exec_recover_count: self.exec_recover_count.load(Ordering::Relaxed),
            exec_recover_total_us: self.exec_recover_total_us.load(Ordering::Relaxed),
            exec_recover_max_us: self.exec_recover_max_us.load(Ordering::Relaxed),
            exec_recover_true: self.exec_recover_true.load(Ordering::Relaxed),
            exec_recover_false: self.exec_recover_false.load(Ordering::Relaxed),
            exec_recover_errors: self.exec_recover_errors.load(Ordering::Relaxed),
            apply_write_count: self.apply_write_count.load(Ordering::Relaxed),
            apply_write_total_us: self.apply_write_total_us.load(Ordering::Relaxed),
            apply_write_max_us: self.apply_write_max_us.load(Ordering::Relaxed),
            apply_read_count: self.apply_read_count.load(Ordering::Relaxed),
            apply_read_total_us: self.apply_read_total_us.load(Ordering::Relaxed),
            apply_read_max_us: self.apply_read_max_us.load(Ordering::Relaxed),
            apply_batch_count: self.apply_batch_count.load(Ordering::Relaxed),
            apply_batch_total_us: self.apply_batch_total_us.load(Ordering::Relaxed),
            apply_batch_max_us: self.apply_batch_max_us.load(Ordering::Relaxed),
            mark_visible_count: self.mark_visible_count.load(Ordering::Relaxed),
            mark_visible_total_us: self.mark_visible_total_us.load(Ordering::Relaxed),
            mark_visible_max_us: self.mark_visible_max_us.load(Ordering::Relaxed),
            state_update_count: self.state_update_count.load(Ordering::Relaxed),
            state_update_total_us: self.state_update_total_us.load(Ordering::Relaxed),
            state_update_max_us: self.state_update_max_us.load(Ordering::Relaxed),
            commit_local_state_count: self.commit_local_state_count.load(Ordering::Relaxed),
            commit_local_state_total_us: self.commit_local_state_total_us.load(Ordering::Relaxed),
            commit_local_state_max_us: self.commit_local_state_max_us.load(Ordering::Relaxed),
            commit_local_durable_count: self.commit_local_durable_count.load(Ordering::Relaxed),
            commit_local_durable_total_us: self
                .commit_local_durable_total_us
                .load(Ordering::Relaxed),
            commit_local_durable_max_us: self.commit_local_durable_max_us.load(Ordering::Relaxed),
            commit_local_log_queue_count: self.commit_local_log_queue_count.load(Ordering::Relaxed),
            commit_local_log_queue_total_us: self
                .commit_local_log_queue_total_us
                .load(Ordering::Relaxed),
            commit_local_log_queue_max_us: self
                .commit_local_log_queue_max_us
                .load(Ordering::Relaxed),
            commit_local_log_append_count: self
                .commit_local_log_append_count
                .load(Ordering::Relaxed),
            commit_local_log_append_total_us: self
                .commit_local_log_append_total_us
                .load(Ordering::Relaxed),
            commit_local_log_append_max_us: self
                .commit_local_log_append_max_us
                .load(Ordering::Relaxed),
            commit_local_post_durable_state_count: self
                .commit_local_post_durable_state_count
                .load(Ordering::Relaxed),
            commit_local_post_durable_state_total_us: self
                .commit_local_post_durable_state_total_us
                .load(Ordering::Relaxed),
            commit_local_post_durable_state_max_us: self
                .commit_local_post_durable_state_max_us
                .load(Ordering::Relaxed),
            commit_remote_quorum_count: self.commit_remote_quorum_count.load(Ordering::Relaxed),
            commit_remote_quorum_total_us: self
                .commit_remote_quorum_total_us
                .load(Ordering::Relaxed),
            commit_remote_quorum_max_us: self.commit_remote_quorum_max_us.load(Ordering::Relaxed),
            commit_tail_count: self.commit_tail_count.load(Ordering::Relaxed),
            commit_tail_total_us: self.commit_tail_total_us.load(Ordering::Relaxed),
            commit_tail_max_us: self.commit_tail_max_us.load(Ordering::Relaxed),
            fast_path_count: self.fast_path_count.load(Ordering::Relaxed),
            slow_path_count: self.slow_path_count.load(Ordering::Relaxed),
        }
    }

    /// Snapshot the most frequent quorum-closing peers ordered by count.
    ///
    /// Purpose:
    /// - Surface which follower most often closes commit quorum without
    ///   exposing an unbounded map through metrics/logging.
    ///
    /// Inputs:
    /// - `limit`: maximum number of peers to return.
    ///
    /// Outputs:
    /// - Sorted vector of quorum-closing peer aggregates.
    fn quorum_closer_top(&self, limit: usize) -> Vec<CommitQuorumCloserStat> {
        let Ok(closers) = self.commit_quorum_closer.lock() else {
            return Vec::new();
        };
        let mut top = closers.values().cloned().collect::<Vec<_>>();
        top.sort_by(|a, b| {
            b.count
                .cmp(&a.count)
                .then_with(|| b.total_us.cmp(&a.total_us))
                .then_with(|| a.node_id.cmp(&b.node_id))
        });
        top.truncate(limit);
        top
    }

    fn record_fast_path(&self, fast_path: bool) {
        if fast_path {
            self.fast_path_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.slow_path_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_exec_progress(&self, dur: Duration, ok: Option<bool>) {
        let us = dur.as_micros().min(u128::from(u64::MAX)) as u64;
        self.exec_progress_count.fetch_add(1, Ordering::Relaxed);
        self.exec_progress_total_us.fetch_add(us, Ordering::Relaxed);
        self.exec_progress_max_us.fetch_max(us, Ordering::Relaxed);
        match ok {
            Some(true) => {
                self.exec_progress_true.fetch_add(1, Ordering::Relaxed);
                self.exec_progress_false_streak.store(0, Ordering::Relaxed);
            }
            Some(false) => {
                self.exec_progress_false.fetch_add(1, Ordering::Relaxed);
                self.exec_progress_false_streak
                    .fetch_add(1, Ordering::Relaxed);
            }
            None => {
                self.exec_progress_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn record_exec_recover(&self, dur: Duration, ok: Option<bool>) {
        let us = dur.as_micros().min(u128::from(u64::MAX)) as u64;
        self.exec_recover_count.fetch_add(1, Ordering::Relaxed);
        self.exec_recover_total_us.fetch_add(us, Ordering::Relaxed);
        self.exec_recover_max_us.fetch_max(us, Ordering::Relaxed);
        match ok {
            Some(true) => {
                self.exec_recover_true.fetch_add(1, Ordering::Relaxed);
            }
            Some(false) => {
                self.exec_recover_false.fetch_add(1, Ordering::Relaxed);
            }
            None => {
                self.exec_recover_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn record_apply_write(&self, dur: Duration) {
        let us = dur.as_micros().min(u128::from(u64::MAX)) as u64;
        self.apply_write_count.fetch_add(1, Ordering::Relaxed);
        self.apply_write_total_us.fetch_add(us, Ordering::Relaxed);
        self.apply_write_max_us.fetch_max(us, Ordering::Relaxed);
    }

    fn record_apply_read(&self, dur: Duration) {
        let us = dur.as_micros().min(u128::from(u64::MAX)) as u64;
        self.apply_read_count.fetch_add(1, Ordering::Relaxed);
        self.apply_read_total_us.fetch_add(us, Ordering::Relaxed);
        self.apply_read_max_us.fetch_max(us, Ordering::Relaxed);
    }

    fn record_apply_batch(&self, dur: Duration) {
        let us = dur.as_micros().min(u128::from(u64::MAX)) as u64;
        self.apply_batch_count.fetch_add(1, Ordering::Relaxed);
        self.apply_batch_total_us.fetch_add(us, Ordering::Relaxed);
        self.apply_batch_max_us.fetch_max(us, Ordering::Relaxed);
    }

    fn record_mark_visible(&self, dur: Duration) {
        let us = dur.as_micros().min(u128::from(u64::MAX)) as u64;
        self.mark_visible_count.fetch_add(1, Ordering::Relaxed);
        self.mark_visible_total_us.fetch_add(us, Ordering::Relaxed);
        self.mark_visible_max_us.fetch_max(us, Ordering::Relaxed);
    }

    fn record_state_update(&self, dur: Duration) {
        let us = dur.as_micros().min(u128::from(u64::MAX)) as u64;
        self.state_update_count.fetch_add(1, Ordering::Relaxed);
        self.state_update_total_us.fetch_add(us, Ordering::Relaxed);
        self.state_update_max_us.fetch_max(us, Ordering::Relaxed);
    }

    /// Record one coordinator-local commit handler sample excluding durable WAL wait.
    ///
    /// Inputs:
    /// - `dur`: wall time spent in local `rpc_commit` bookkeeping/state updates.
    ///
    /// Outputs:
    /// - Updates rolling count/total/max commit-local-state counters.
    fn record_commit_local_state(&self, dur: Duration) {
        let us = dur.as_micros().min(u128::from(u64::MAX)) as u64;
        self.commit_local_state_count
            .fetch_add(1, Ordering::Relaxed);
        self.commit_local_state_total_us
            .fetch_add(us, Ordering::Relaxed);
        self.commit_local_state_max_us
            .fetch_max(us, Ordering::Relaxed);
    }

    /// Record one synchronous durable WAL wait sample from the commit path.
    ///
    /// Inputs:
    /// - `dur`: wall time spent waiting for durable commit-log append/fsync.
    ///
    /// Outputs:
    /// - Updates rolling count/total/max commit-durable-wait counters.
    fn record_commit_local_durable(&self, dur: Duration) {
        let us = dur.as_micros().min(u128::from(u64::MAX)) as u64;
        self.commit_local_durable_count
            .fetch_add(1, Ordering::Relaxed);
        self.commit_local_durable_total_us
            .fetch_add(us, Ordering::Relaxed);
        self.commit_local_durable_max_us
            .fetch_max(us, Ordering::Relaxed);
    }

    /// Record queueing delay before the group-local commit-log worker starts an append.
    ///
    /// Inputs:
    /// - `dur`: time spent from enqueue until the batcher begins processing.
    ///
    /// Outputs:
    /// - Updates rolling queue-wait counters for sync commit diagnosis.
    fn record_commit_local_log_queue(&self, dur: Duration) {
        let us = dur.as_micros().min(u128::from(u64::MAX)) as u64;
        self.commit_local_log_queue_count
            .fetch_add(1, Ordering::Relaxed);
        self.commit_local_log_queue_total_us
            .fetch_add(us, Ordering::Relaxed);
        self.commit_local_log_queue_max_us
            .fetch_max(us, Ordering::Relaxed);
    }

    /// Record append execution time spent inside the commit-log backend call.
    ///
    /// Inputs:
    /// - `dur`: wall time inside `append_commits_with_options`.
    ///
    /// Outputs:
    /// - Updates rolling append-execution counters for sync commit diagnosis.
    fn record_commit_local_log_append(&self, dur: Duration) {
        let us = dur.as_micros().min(u128::from(u64::MAX)) as u64;
        self.commit_local_log_append_count
            .fetch_add(1, Ordering::Relaxed);
        self.commit_local_log_append_total_us
            .fetch_add(us, Ordering::Relaxed);
        self.commit_local_log_append_max_us
            .fetch_max(us, Ordering::Relaxed);
    }

    /// Record state-update work that occurs after a durable append completes.
    ///
    /// Inputs:
    /// - `dur`: wall time spent re-locking state and finalizing commit metadata
    ///   after durable WAL completion.
    ///
    /// Outputs:
    /// - Updates rolling post-durable state-update counters.
    fn record_commit_local_post_durable_state(&self, dur: Duration) {
        let us = dur.as_micros().min(u128::from(u64::MAX)) as u64;
        self.commit_local_post_durable_state_count
            .fetch_add(1, Ordering::Relaxed);
        self.commit_local_post_durable_state_total_us
            .fetch_add(us, Ordering::Relaxed);
        self.commit_local_post_durable_state_max_us
            .fetch_max(us, Ordering::Relaxed);
    }

    /// Record the coordinator wait needed to gather remote commit quorum ACKs.
    ///
    /// Inputs:
    /// - `dur`: wall time spent after local commit until remote quorum is reached.
    ///
    /// Outputs:
    /// - Updates rolling count/total/max remote-quorum-wait counters.
    fn record_commit_remote_quorum(&self, dur: Duration) {
        let us = dur.as_micros().min(u128::from(u64::MAX)) as u64;
        self.commit_remote_quorum_count
            .fetch_add(1, Ordering::Relaxed);
        self.commit_remote_quorum_total_us
            .fetch_add(us, Ordering::Relaxed);
        self.commit_remote_quorum_max_us
            .fetch_max(us, Ordering::Relaxed);
    }

    /// Record tail-follower completion time after commit quorum is already satisfied.
    ///
    /// Inputs:
    /// - `dur`: extra wall time to observe remaining follower commit RPCs finish.
    ///
    /// Outputs:
    /// - Updates rolling count/total/max commit-tail counters.
    fn record_commit_tail(&self, dur: Duration) {
        let us = dur.as_micros().min(u128::from(u64::MAX)) as u64;
        self.commit_tail_count.fetch_add(1, Ordering::Relaxed);
        self.commit_tail_total_us.fetch_add(us, Ordering::Relaxed);
        self.commit_tail_max_us.fetch_max(us, Ordering::Relaxed);
    }

    /// Record the remote peer whose ACK most often closes commit quorum.
    ///
    /// Inputs:
    /// - `peer`: follower id that moved quorum from unsatisfied to satisfied.
    /// - `dur`: total remote-quorum wait observed when that peer closed quorum.
    ///
    /// Outputs:
    /// - Updates per-peer closer counters for exported debug stats.
    fn record_commit_quorum_closer(&self, peer: NodeId, dur: Duration) {
        let us = dur.as_micros().min(u128::from(u64::MAX)) as u64;
        let Ok(mut closers) = self.commit_quorum_closer.lock() else {
            return;
        };
        let entry = closers
            .entry(peer)
            .or_insert_with(|| CommitQuorumCloserStat {
                node_id: peer,
                ..CommitQuorumCloserStat::default()
            });
        entry.count = entry.count.saturating_add(1);
        entry.total_us = entry.total_us.saturating_add(us);
        entry.max_us = entry.max_us.max(us);
    }

    fn exec_progress_false_streak(&self) -> u64 {
        self.exec_progress_false_streak.load(Ordering::Relaxed)
    }
}

impl Group {
    /// Construct one Accord group instance with worker threads and runtime state.
    ///
    /// Inputs:
    /// - `config`: quorum, timeout, batching, and durability behavior.
    /// - `transport`: RPC transport used for peer protocol messages.
    /// - `sm`: storage state machine used for key extraction and apply.
    /// - `commit_log`: optional WAL backend for commit persistence.
    ///
    /// Output:
    /// - A fully initialized `Group` with commit-log/apply worker channels wired.
    ///
    /// Design:
    /// - Commit-log requests are batched on a dedicated thread so we can coalesce
    ///   append + fsync cost across multiple transactions.
    /// - Apply work is offloaded to a blocking thread to avoid stalling the async runtime.
    pub fn new(
        config: Config,
        transport: Arc<dyn Transport>,
        sm: Arc<dyn StateMachine>,
        commit_log: Option<Arc<dyn CommitLog>>,
    ) -> Self {
        let commit_log_tx = commit_log.as_ref().map(|log| {
            let (tx, rx) = std_mpsc::channel::<CommitLogWork>();
            let log = log.clone();
            let batch_max = config.commit_log_batch_max.max(1);
            let batch_wait = config.commit_log_batch_wait;
            let group_id = config.group_id;
            std::thread::Builder::new()
                .name(format!("commit-log-{}", group_id))
                .spawn(move || {
                    // Batch commit-log appends to amortize syscalls/fsync.
                    let mut disconnected = false;
                    while !disconnected {
                        let first = match rx.recv() {
                            Ok(work) => work,
                            Err(_) => break,
                        };
                        let mut batch = Vec::with_capacity(batch_max);
                        batch.push(first);

                        let deadline = if batch_wait.is_zero() {
                            None
                        } else {
                            Some(std::time::Instant::now() + batch_wait)
                        };

                        while batch.len() < batch_max {
                            match rx.try_recv() {
                                Ok(work) => {
                                    batch.push(work);
                                    continue;
                                }
                                Err(std_mpsc::TryRecvError::Disconnected) => {
                                    disconnected = true;
                                    break;
                                }
                                Err(std_mpsc::TryRecvError::Empty) => {}
                            }

                            let Some(deadline) = deadline else {
                                break;
                            };
                            let now = std::time::Instant::now();
                            if now >= deadline {
                                break;
                            }
                            let remaining = deadline.saturating_duration_since(now);
                            match rx.recv_timeout(remaining) {
                                Ok(work) => batch.push(work),
                                Err(std_mpsc::RecvTimeoutError::Timeout) => break,
                                Err(std_mpsc::RecvTimeoutError::Disconnected) => {
                                    disconnected = true;
                                    break;
                                }
                            }
                        }

                        // If any request in this batch requires fsync-on-ack, run the whole
                        // batch as durable so every caller in this batch sees correct semantics.
                        let require_durable = batch.iter().any(|work| work.require_durable);
                        let mut entries = Vec::with_capacity(batch.len());
                        let mut completions = Vec::with_capacity(batch.len());
                        for work in batch {
                            entries.push(work.entry);
                            completions.push((work.done_tx, work.enqueued_at));
                        }

                        let append_start = std::time::Instant::now();
                        let append_result = log.append_commits_with_options(
                            entries,
                            CommitLogAppendOptions { require_durable },
                        );
                        let append_us =
                            append_start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
                        if let Err(err) = &append_result {
                            tracing::warn!(error = ?err, "commit log batch append failed");
                        }
                        let err_msg = append_result.err().map(|err| err.to_string());
                        for (done_tx, enqueued_at) in completions {
                            let queue_wait_us = append_start
                                .saturating_duration_since(enqueued_at)
                                .as_micros()
                                .min(u128::from(u64::MAX))
                                as u64;
                            let result = match &err_msg {
                                None => Ok(CommitLogAppendCompletion {
                                    queue_wait_us,
                                    append_us,
                                }),
                                Some(msg) => Err(anyhow::anyhow!(msg.clone())),
                            };
                            let _ = done_tx.send(result);
                        }
                    }
                })
                .expect("spawn commit log batcher");
            tx
        });

        let apply_tx = {
            let (tx, rx) = std_mpsc::channel::<ApplyWork>();
            let sm = sm.clone();
            let group_id = config.group_id;
            std::thread::Builder::new()
                .name(format!("apply-{}", group_id))
                .spawn(move || {
                    // Apply batches off the async runtime to avoid blocking.
                    while let Ok(work) = rx.recv() {
                        let apply_start = std::time::Instant::now();
                        let result = sm.apply_batch(&work.batch);
                        let apply_us =
                            apply_start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;

                        let _ = work.tx.send(ApplyResult {
                            apply_us,
                            visible_us: 0,
                            result,
                        });
                    }
                })
                .expect("spawn apply worker");
            Some(tx)
        };

        let mut initial_voters = config.members.iter().map(|m| m.id).collect::<Vec<_>>();
        initial_voters.sort_unstable();
        initial_voters.dedup();

        let mut state = State::new();
        // Seed txn counters from wall-clock lower bits so restarts do not
        // immediately reuse low txns before replay can advance the floor.
        state.next_txn_counter = initial_txn_counter_seed();

        Self {
            members: RwLock::new(config.members.clone()),
            voters: RwLock::new(initial_voters),
            config,
            transport,
            sm,
            commit_log,
            commit_log_tx,
            apply_tx,
            state: Mutex::new(state),
            execute_lock: Mutex::new(()),
            executor_notify: Notify::new(),
            executor_started: AtomicBool::new(false),
            metrics: Arc::new(GroupMetrics::default()),
            compact_counter: AtomicU64::new(0),
            peer_rr: AtomicU64::new(0),
            start_at: time::Instant::now(),
        }
    }

    /// Replace the current runtime membership for this group.
    ///
    /// This updates quorum/peer selection for new proposals and RPC handling.
    /// In-flight proposals continue on the previous view.
    pub fn update_members(&self, members: Vec<Member>) -> anyhow::Result<()> {
        let voters = members.iter().map(|m| m.id).collect::<Vec<_>>();
        self.update_membership(members, voters)
    }

    /// Replace runtime membership and explicit voter set for this group.
    pub fn update_membership(
        &self,
        mut members: Vec<Member>,
        mut voters: Vec<NodeId>,
    ) -> anyhow::Result<()> {
        if members.is_empty() {
            anyhow::bail!("group membership cannot be empty");
        }
        members.sort_by_key(|m| m.id);
        members.dedup_by_key(|m| m.id);
        voters.sort_unstable();
        voters.dedup();
        if voters.is_empty() {
            anyhow::bail!("group voter set cannot be empty");
        }
        let member_set = members.iter().map(|m| m.id).collect::<HashSet<_>>();
        for voter in &voters {
            if !member_set.contains(voter) {
                anyhow::bail!("voter {voter} must also be present in group members");
            }
        }
        let mut guard = self
            .members
            .write()
            .map_err(|_| anyhow::anyhow!("group membership lock poisoned"))?;
        *guard = members;
        let mut voters_guard = self
            .voters
            .write()
            .map_err(|_| anyhow::anyhow!("group voter lock poisoned"))?;
        *voters_guard = voters;
        Ok(())
    }

    /// Return the current runtime members.
    pub fn members(&self) -> Vec<Member> {
        self.members.read().map(|g| g.clone()).unwrap_or_default()
    }

    /// Return the current runtime voter set.
    pub fn voters(&self) -> Vec<NodeId> {
        self.voters.read().map(|g| g.clone()).unwrap_or_default()
    }

    fn voters_snapshot(&self) -> Vec<NodeId> {
        self.voters.read().map(|g| g.clone()).unwrap_or_default()
    }

    fn local_is_member(&self) -> bool {
        let local = self.config.node_id;
        self.members
            .read()
            .map(|m| m.iter().any(|member| member.id == local))
            .unwrap_or(false)
    }

    fn local_is_voter(&self) -> bool {
        let local = self.config.node_id;
        self.voters
            .read()
            .map(|voters| voters.iter().any(|id| *id == local))
            .unwrap_or(false)
    }

    fn peers_snapshot(&self) -> Vec<NodeId> {
        let local = self.config.node_id;
        self.members
            .read()
            .map(|members| {
                members
                    .iter()
                    .map(|m| m.id)
                    .filter(|id| *id != local)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    fn voter_peers_snapshot(&self) -> Vec<NodeId> {
        let local = self.config.node_id;
        self.voters
            .read()
            .map(|voters| {
                voters
                    .iter()
                    .copied()
                    .filter(|id| *id != local)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    fn quorum(&self) -> usize {
        let n = self.voters.read().map(|m| m.len()).unwrap_or(0);
        (n / 2) + 1
    }

    fn fast_quorum(&self) -> usize {
        self.quorum()
    }

    fn config_with_runtime_voters(&self) -> Config {
        let mut cfg = self.config.clone();
        cfg.members = self
            .voters_snapshot()
            .into_iter()
            .map(|id| Member { id })
            .collect();
        cfg
    }

    /// Queue one commit-log append request and return a receiver for completion.
    ///
    /// Inputs:
    /// - `entry`: WAL record to append.
    /// - `require_durable`: whether this append must include synchronous fsync.
    ///
    /// Output:
    /// - A `Receiver` that yields the append result once the commit-log batcher
    ///   has finished processing the request.
    fn enqueue_commit_log_append(
        &self,
        entry: CommitLogEntry,
        require_durable: bool,
    ) -> anyhow::Result<std_mpsc::Receiver<anyhow::Result<CommitLogAppendCompletion>>> {
        let Some(tx) = &self.commit_log_tx else {
            anyhow::bail!("commit log unavailable");
        };
        let (done_tx, done_rx) = std_mpsc::channel();
        tx.send(CommitLogWork {
            entry,
            require_durable,
            done_tx,
            enqueued_at: std::time::Instant::now(),
        })
        .map_err(|_| anyhow::anyhow!("commit log batcher closed"))?;
        Ok(done_rx)
    }

    /// Block until the commit-log batcher completes an append request.
    ///
    /// Inputs:
    /// - `done_rx`: completion channel returned by `enqueue_commit_log_append`.
    /// - `timeout`: maximum wait to prevent unbounded request stalls.
    ///
    /// Output:
    /// - `Ok(CommitLogAppendCompletion)` when append finished successfully.
    /// - `Err(...)` on timeout, channel closure, or WAL append failure.
    fn wait_commit_log_append(
        done_rx: std_mpsc::Receiver<anyhow::Result<CommitLogAppendCompletion>>,
        timeout: Duration,
    ) -> anyhow::Result<CommitLogAppendCompletion> {
        match tokio::task::block_in_place(|| done_rx.recv_timeout(timeout)) {
            Ok(res) => res,
            Err(std_mpsc::RecvTimeoutError::Timeout) => {
                Err(anyhow::anyhow!("commit log append timed out"))
            }
            Err(std_mpsc::RecvTimeoutError::Disconnected) => {
                Err(anyhow::anyhow!("commit log append response channel closed"))
            }
        }
    }

    pub fn handle(self: &Arc<Self>) -> Handle {
        Handle {
            group: self.clone(),
        }
    }

    pub async fn last_committed_for_keys(&self, keys: &[Vec<u8>]) -> Vec<Option<(TxnId, u64)>> {
        let key_refs = keys.iter().map(|key| key.as_slice()).collect::<Vec<_>>();
        self.last_committed_for_key_slices(&key_refs).await
    }

    /// Return per-key write barriers used by linearizable reads.
    ///
    /// Purpose:
    /// - Report the highest write this replica knows a read must wait for on
    ///   each key.
    ///
    /// Design:
    /// - Normally returns committed write hints.
    /// - In 1RTT fast-path mode, also scans PreAccepted/Accepted write records
    ///   because such records may already have been ACKed by their coordinator
    ///   while Commit dissemination is still asynchronous.
    ///
    /// Inputs:
    /// - `keys`: borrowed storage-key slices.
    ///
    /// Outputs:
    /// - One optional `(txn_id, seq)` barrier target per input key.
    pub async fn last_committed_for_key_slices(&self, keys: &[&[u8]]) -> Vec<Option<(TxnId, u64)>> {
        let state = self.state.lock().await;
        keys.iter()
            .map(|key| {
                let mut best = state.last_committed_write_by_key.get(*key).copied();
                if self.config.fast_path_1rtt {
                    best = fast_path_barrier_write_for_key(&state, key, best);
                }
                best
            })
            .collect()
    }

    pub async fn observe_last_committed(&self, keys: &[Vec<u8>], values: &[Option<(TxnId, u64)>]) {
        let key_refs = keys.iter().map(|key| key.as_slice()).collect::<Vec<_>>();
        self.observe_last_committed_slices(&key_refs, values).await;
    }

    pub async fn observe_last_committed_slices(
        &self,
        keys: &[&[u8]],
        values: &[Option<(TxnId, u64)>],
    ) {
        let mut state = self.state.lock().await;
        for (key, item) in keys.iter().zip(values.iter()) {
            let Some((txn_id, seq)) = item else { continue };
            match state.last_committed_write_by_key.get(*key) {
                Some((_, cur_seq)) if *cur_seq >= *seq => continue,
                _ => {
                    state
                        .last_committed_write_by_key
                        .insert((*key).to_vec(), (*txn_id, *seq));
                }
            }
        }
    }

    pub async fn executed_prefixes(&self) -> Vec<ExecutedPrefix> {
        let state = self.state.lock().await;
        state
            .executed_prefix_by_node
            .iter()
            .map(|(node_id, counter)| ExecutedPrefix {
                node_id: *node_id,
                counter: *counter,
            })
            .collect()
    }

    /// Seed lower bounds for executed prefixes after external snapshot/backfill.
    ///
    /// This is used when adding a learner via out-of-band state transfer: the
    /// learner receives KV state first, then needs executed-prefix floors so
    /// future per-origin counters can advance contiguously.
    pub async fn seed_executed_prefixes(&self, prefixes: &[ExecutedPrefix]) {
        let mut state = self.state.lock().await;
        let mut changed = false;
        for p in prefixes {
            let entry = state.executed_prefix_by_node.entry(p.node_id).or_insert(0);
            if p.counter > *entry {
                *entry = p.counter;
                changed = true;
            }
        }
        if changed {
            let floors = state.executed_prefix_by_node.clone();
            state.executed_out_of_order.retain(|txn_id| {
                let floor = floors.get(&txn_id.node_id).copied().unwrap_or(0);
                txn_id.counter > floor
            });
        }
    }

    pub async fn executed(&self, txn_id: TxnId) -> bool {
        if !self.local_is_member() {
            return false;
        }
        self.is_executed(txn_id).await
    }

    pub async fn mark_visible(&self, txn_id: TxnId) -> anyhow::Result<bool> {
        if !self.local_is_member() {
            return Ok(false);
        }
        let entry = {
            let mut state = self.state.lock().await;
            if state.visible_txns.contains(&txn_id) {
                return Ok(true);
            }
            let Some(entry) = state.executed_log.get(&txn_id).cloned() else {
                return Ok(false);
            };
            state.visible_txns.insert(txn_id);
            entry
        };

        let command = match entry.command {
            Some(command) => command,
            None => self
                .load_command_from_commit_log(txn_id)
                .unwrap_or_default(),
        };

        if command.is_empty() {
            return Ok(true);
        }

        self.sm.mark_visible(
            &command,
            ExecMeta {
                txn_id,
                seq: entry.seq,
            },
        )?;

        Ok(true)
    }

    pub async fn wait_executed(&self, txn_id: TxnId) -> anyhow::Result<()> {
        self.execute_until(txn_id).await
    }

    pub async fn replay_commits(&self) -> anyhow::Result<usize> {
        let Some(log) = &self.commit_log else {
            return Ok(0);
        };

        let entries = log.load()?;
        if entries.is_empty() {
            return Ok(0);
        }

        {
            let mut state = self.state.lock().await;
            let mut max_local_counter_seen = 0u64;
            for entry in &entries {
                if txn_group_id(entry.txn_id) != self.config.group_id {
                    continue;
                }
                if entry.txn_id.node_id == self.config.node_id {
                    max_local_counter_seen =
                        max_local_counter_seen.max(txn_local_counter(entry.txn_id.counter));
                }
                let deps = entry.deps.iter().copied().collect::<BTreeSet<_>>();
                let keys = if entry.command.is_empty() {
                    CommandKeys::default()
                } else {
                    self.sm.command_keys(&entry.command).unwrap_or_default()
                };

                let rec = state.records.entry(entry.txn_id).or_insert_with(|| Record {
                    promised: Ballot::zero(),
                    accepted_ballot: None,
                    command: None,
                    command_digest: None,
                    keys: None,
                    seq: 0,
                    deps: BTreeSet::new(),
                    status: Status::None,
                    updated_at: time::Instant::now(),
                });
                rec.command = Some(detach_command_bytes(&entry.command));
                rec.command_digest = Some(command_digest(&entry.command));
                rec.keys = Some(keys.clone());
                rec.seq = entry.seq.max(1);
                rec.deps = deps.clone();
                rec.status = rec.status.max(Status::Committed);
                let rec_seq = rec.seq;
                if keys.is_write() {
                    for key in keys.keys() {
                        match state.last_committed_write_by_key.get(key) {
                            Some((_, cur_seq)) if *cur_seq >= rec_seq => {}
                            _ => {
                                state
                                    .last_committed_write_by_key
                                    .insert(key.clone(), (entry.txn_id, rec_seq));
                            }
                        }
                    }
                }
                state.update_frontier(entry.txn_id, &keys, &deps);
                state.insert_committed(entry.txn_id, rec_seq);
            }
            state.next_txn_counter = state.next_txn_counter.max(max_local_counter_seen);
        }

        loop {
            match self.execute_progress().await {
                Ok(true) => continue,
                Ok(false) => break,
                Err(err) => return Err(err),
            }
        }

        let ids: Vec<TxnId> = {
            let state = self.state.lock().await;
            state.executed_log.keys().copied().collect()
        };
        for txn_id in ids {
            let _ = self.mark_visible(txn_id).await?;
        }

        Ok(entries.len())
    }

    pub fn spawn_executor(self: &Arc<Self>) {
        if self.executor_started.swap(true, Ordering::SeqCst) {
            return;
        }

        let group = self.clone();
        tokio::spawn(async move {
            loop {
                loop {
                    match group.execute_progress().await {
                        Ok(true) => continue,
                        Ok(false) => {
                            let streak = group.metrics.exec_progress_false_streak();
                            if streak < 16 {
                                break;
                            }
                            match group.executor_recover_once().await {
                                Ok(true) => continue,
                                Ok(false) => break,
                                Err(err) => {
                                    tracing::warn!(error = ?err, "executor recovery failed");
                                    break;
                                }
                            }
                        }
                        Err(err) => {
                            tracing::warn!(error = ?err, "executor progress failed");
                            break;
                        }
                    }
                }

                group.maybe_compact_state().await;
                tokio::select! {
                    _ = group.executor_notify.notified() => {}
                    _ = time::sleep(Duration::from_millis(25)) => {}
                }
            }
        });

        let group = self.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(500));
            loop {
                interval.tick().await;
                if let Err(err) = group.gossip_executed_prefixes().await {
                    tracing::debug!(error = ?err, "executed-prefix gossip failed");
                }
            }
        });
    }

    fn peers_round_robin(&self) -> Vec<NodeId> {
        let mut peers = self.voter_peers_snapshot();
        if peers.len() <= 1 {
            return peers;
        }
        let idx = (self.peer_rr.fetch_add(1, Ordering::Relaxed) as usize) % peers.len();
        peers.rotate_left(idx);
        peers
    }

    fn compose_txn_id(&self, local_counter: u64) -> anyhow::Result<TxnId> {
        let shard_bits = 64u32.saturating_sub(TXN_COUNTER_SHARD_SHIFT);
        let max_group_id = if shard_bits >= 64 {
            u64::MAX
        } else {
            (1u64 << shard_bits) - 1
        };
        anyhow::ensure!(
            self.config.group_id <= max_group_id,
            "group_id {} exceeds max {}",
            self.config.group_id,
            max_group_id
        );

        let local_mask = if TXN_COUNTER_SHARD_SHIFT >= 64 {
            u64::MAX
        } else {
            (1u64 << TXN_COUNTER_SHARD_SHIFT) - 1
        };
        anyhow::ensure!(
            local_counter <= local_mask,
            "txn counter overflow (counter={}, max={})",
            local_counter,
            local_mask
        );

        Ok(TxnId {
            node_id: self.config.node_id,
            counter: (self.config.group_id << TXN_COUNTER_SHARD_SHIFT) | local_counter,
        })
    }

    pub async fn debug_stats(&self) -> DebugStats {
        let state = self.state.lock().await;
        let metrics = self.metrics.snapshot();
        let committed_queue_ghost_len = state
            .committed_queue
            .iter()
            .filter(|(_, id)| !state.records.contains_key(id))
            .count();
        let mut records_status_none_len = 0usize;
        let mut records_status_preaccepted_len = 0usize;
        let mut records_status_accepted_len = 0usize;
        let mut records_status_committed_len = 0usize;
        let mut records_status_executing_len = 0usize;
        let mut records_status_executed_len = 0usize;
        let mut records_missing_command_len = 0usize;
        let mut records_missing_keys_len = 0usize;
        let mut records_committed_missing_command_len = 0usize;
        let mut records_committed_missing_keys_len = 0usize;
        for rec in state.records.values() {
            match rec.status {
                Status::None => records_status_none_len += 1,
                Status::PreAccepted => records_status_preaccepted_len += 1,
                Status::Accepted => records_status_accepted_len += 1,
                Status::Committed => records_status_committed_len += 1,
                Status::Executing => records_status_executing_len += 1,
                Status::Executed => records_status_executed_len += 1,
            }

            if rec.command.is_none() {
                records_missing_command_len += 1;
                if rec.status >= Status::Committed {
                    records_committed_missing_command_len += 1;
                }
            }
            if rec.keys.is_none() {
                records_missing_keys_len += 1;
                if rec.status >= Status::Committed {
                    records_committed_missing_keys_len += 1;
                }
            }
        }
        let executed_log_command_bytes = state.executed_log_bytes;
        let executed_log_deps_total = state.executed_log_deps_total;
        let commit_quorum_closer_top = self.metrics.quorum_closer_top(3);
        let mut executed_log_max_command_bytes = 0usize;
        let mut executed_log_max_deps_len = 0usize;
        for entry in state.executed_log.values() {
            executed_log_max_command_bytes =
                executed_log_max_command_bytes.max(entry.command.as_ref().map_or(0, Bytes::len));
            executed_log_max_deps_len = executed_log_max_deps_len.max(entry.deps.len());
        }
        DebugStats {
            records_len: state.records.len(),
            records_capacity: state.records.capacity(),
            records_status_none_len,
            records_status_preaccepted_len,
            records_status_accepted_len,
            records_status_committed_len,
            records_status_executing_len,
            records_status_executed_len,
            records_missing_command_len,
            records_missing_keys_len,
            records_committed_missing_command_len,
            records_committed_missing_keys_len,
            committed_queue_len: state.committed_queue.len(),
            committed_queue_ghost_len,
            frontier_keys_len: state.frontier_by_key.len(),
            frontier_entries_len: state.frontier_by_key.values().map(|v| v.len()).sum(),
            executed_prefix_nodes: state.executed_prefix_by_node.len(),
            executed_out_of_order_len: state.executed_out_of_order.len(),
            executed_log_len: state.executed_log.len(),
            executed_log_capacity: state.executed_log.capacity(),
            executed_log_order_capacity: state.executed_log_order.capacity(),
            executed_log_command_bytes,
            executed_log_max_command_bytes,
            executed_log_deps_total,
            executed_log_max_deps_len,
            reported_executed_peers: state.reported_executed_prefix_by_peer.len(),
            recovering_len: state.recovering.len(),
            read_waiters_len: state.read_waiters.len(),
            proposal_timeouts: state.proposal_timeouts,
            execute_timeouts: state.execute_timeouts,
            recovery_attempts: state.recovery_attempts,
            recovery_successes: state.recovery_successes,
            recovery_failures: state.recovery_failures,
            recovery_timeouts: state.recovery_timeouts,
            recovery_noops: state.recovery_noops,
            recovery_last_ms: state.recovery_last_ms,
            exec_progress_count: metrics.exec_progress_count,
            exec_progress_total_us: metrics.exec_progress_total_us,
            exec_progress_max_us: metrics.exec_progress_max_us,
            exec_progress_true: metrics.exec_progress_true,
            exec_progress_false: metrics.exec_progress_false,
            exec_progress_errors: metrics.exec_progress_errors,
            exec_recover_count: metrics.exec_recover_count,
            exec_recover_total_us: metrics.exec_recover_total_us,
            exec_recover_max_us: metrics.exec_recover_max_us,
            exec_recover_true: metrics.exec_recover_true,
            exec_recover_false: metrics.exec_recover_false,
            exec_recover_errors: metrics.exec_recover_errors,
            apply_write_count: metrics.apply_write_count,
            apply_write_total_us: metrics.apply_write_total_us,
            apply_write_max_us: metrics.apply_write_max_us,
            apply_read_count: metrics.apply_read_count,
            apply_read_total_us: metrics.apply_read_total_us,
            apply_read_max_us: metrics.apply_read_max_us,
            apply_batch_count: metrics.apply_batch_count,
            apply_batch_total_us: metrics.apply_batch_total_us,
            apply_batch_max_us: metrics.apply_batch_max_us,
            mark_visible_count: metrics.mark_visible_count,
            mark_visible_total_us: metrics.mark_visible_total_us,
            mark_visible_max_us: metrics.mark_visible_max_us,
            state_update_count: metrics.state_update_count,
            state_update_total_us: metrics.state_update_total_us,
            state_update_max_us: metrics.state_update_max_us,
            commit_local_state_count: metrics.commit_local_state_count,
            commit_local_state_total_us: metrics.commit_local_state_total_us,
            commit_local_state_max_us: metrics.commit_local_state_max_us,
            commit_local_durable_count: metrics.commit_local_durable_count,
            commit_local_durable_total_us: metrics.commit_local_durable_total_us,
            commit_local_durable_max_us: metrics.commit_local_durable_max_us,
            commit_local_log_queue_count: metrics.commit_local_log_queue_count,
            commit_local_log_queue_total_us: metrics.commit_local_log_queue_total_us,
            commit_local_log_queue_max_us: metrics.commit_local_log_queue_max_us,
            commit_local_log_append_count: metrics.commit_local_log_append_count,
            commit_local_log_append_total_us: metrics.commit_local_log_append_total_us,
            commit_local_log_append_max_us: metrics.commit_local_log_append_max_us,
            commit_local_post_durable_state_count: metrics.commit_local_post_durable_state_count,
            commit_local_post_durable_state_total_us: metrics
                .commit_local_post_durable_state_total_us,
            commit_local_post_durable_state_max_us: metrics.commit_local_post_durable_state_max_us,
            commit_remote_quorum_count: metrics.commit_remote_quorum_count,
            commit_remote_quorum_total_us: metrics.commit_remote_quorum_total_us,
            commit_remote_quorum_max_us: metrics.commit_remote_quorum_max_us,
            commit_tail_count: metrics.commit_tail_count,
            commit_tail_total_us: metrics.commit_tail_total_us,
            commit_tail_max_us: metrics.commit_tail_max_us,
            commit_quorum_closer_top,
            fast_path_count: metrics.fast_path_count,
            slow_path_count: metrics.slow_path_count,
        }
    }

    pub async fn rpc_pre_accept(&self, req: PreAcceptRequest) -> PreAcceptResponse {
        if !self.local_is_voter() {
            return PreAcceptResponse {
                ok: false,
                promised: Ballot::zero(),
                seq: 0,
                deps: Vec::new(),
            };
        }
        let now = time::Instant::now();
        let mut state = self.state.lock().await;
        if state.is_executed(&req.txn_id) {
            return PreAcceptResponse {
                ok: true,
                promised: Ballot::zero(),
                seq: 1,
                deps: Vec::new(),
            };
        }

        state.records.entry(req.txn_id).or_insert_with(|| Record {
            promised: Ballot::zero(),
            accepted_ballot: None,
            command: None,
            command_digest: None,
            keys: None,
            seq: 0,
            deps: BTreeSet::new(),
            status: Status::None,
            updated_at: now,
        });

        let current_promised = state
            .records
            .get(&req.txn_id)
            .expect("record must exist")
            .promised;
        if req.ballot < current_promised {
            let rec = state.records.get(&req.txn_id).expect("record must exist");
            return PreAcceptResponse {
                ok: false,
                promised: current_promised,
                seq: rec.seq,
                deps: rec.deps.iter().copied().collect(),
            };
        }

        {
            let rec = state
                .records
                .get_mut(&req.txn_id)
                .expect("record must exist");
            rec.promised = req.ballot;
            rec.updated_at = now;
        }

        if state
            .records
            .get(&req.txn_id)
            .is_some_and(|r| r.status >= Status::Committed)
        {
            let rec = state.records.get(&req.txn_id).expect("record must exist");
            return PreAcceptResponse {
                ok: true,
                promised: rec.promised,
                seq: rec.seq.max(1),
                deps: rec.deps.iter().copied().collect(),
            };
        }

        {
            let rec = state
                .records
                .get_mut(&req.txn_id)
                .expect("record must exist");
            let digest = command_digest(&req.command);
            if let Some(cmd) = &rec.command {
                if cmd.as_ref() != req.command.as_ref() {
                    return PreAcceptResponse {
                        ok: false,
                        promised: rec.promised,
                        seq: 0,
                        deps: Vec::new(),
                    };
                }
                if rec.command_digest.is_none() {
                    rec.command_digest = Some(digest);
                }
                if rec.keys.is_none() {
                    if cmd.is_empty() {
                        rec.keys = Some(CommandKeys::default());
                    } else {
                        match self.sm.command_keys(cmd) {
                            Ok(keys) => {
                                rec.keys = Some(keys);
                            }
                            Err(_) => {
                                return PreAcceptResponse {
                                    ok: false,
                                    promised: rec.promised,
                                    seq: 0,
                                    deps: Vec::new(),
                                };
                            }
                        }
                    }
                }
            } else {
                rec.command = Some(detach_command_bytes(&req.command));
                rec.command_digest = Some(digest);
                if req.command.is_empty() {
                    rec.keys = Some(CommandKeys::default());
                } else {
                    match self.sm.command_keys(&req.command) {
                        Ok(keys) => {
                            rec.keys = Some(keys);
                        }
                        Err(_) => {
                            return PreAcceptResponse {
                                ok: false,
                                promised: rec.promised,
                                seq: 0,
                                deps: Vec::new(),
                            };
                        }
                    }
                }
            }
        }

        let keys = state
            .records
            .get(&req.txn_id)
            .and_then(|r| r.keys.as_ref())
            .cloned()
            .context("missing keys for preaccepted txn")
            .unwrap_or_default();

        let runtime_cfg = self.config_with_runtime_voters();
        let (local_seq, local_deps) = state.compute_seq_deps(&runtime_cfg, req.txn_id, &keys);
        let mut merged_deps = local_deps;
        merged_deps.extend(req.deps);

        let merged_seq = local_seq.max(req.seq);

        let is_monotonic = {
            let rec = state.records.get(&req.txn_id).expect("record must exist");
            is_monotonic_update(rec.seq, &rec.deps, merged_seq, &merged_deps)
        };
        if !is_monotonic {
            let rec = state.records.get(&req.txn_id).expect("record must exist");
            return PreAcceptResponse {
                ok: false,
                promised: rec.promised,
                seq: rec.seq,
                deps: rec.deps.iter().copied().collect(),
            };
        }

        state.update_frontier(req.txn_id, &keys, &merged_deps);

        {
            let rec = state
                .records
                .get_mut(&req.txn_id)
                .expect("record must exist");
            rec.seq = merged_seq;
            rec.deps = merged_deps;
            rec.accepted_ballot = Some(req.ballot);
            rec.status = rec.status.max(Status::PreAccepted);
        }

        let rec = state.records.get(&req.txn_id).expect("record must exist");
        PreAcceptResponse {
            ok: true,
            promised: rec.promised,
            seq: rec.seq,
            deps: rec.deps.iter().copied().collect(),
        }
    }

    pub async fn rpc_accept(&self, req: AcceptRequest) -> AcceptResponse {
        if !self.local_is_voter() {
            return AcceptResponse {
                ok: false,
                promised: Ballot::zero(),
            };
        }
        let AcceptRequest {
            group_id: _,
            txn_id,
            ballot,
            command,
            command_digest: expected_digest,
            has_command,
            seq,
            deps,
        } = req;
        let mut incoming_command = if has_command { Some(command) } else { None };
        let deps_vec = deps;

        loop {
            let now = time::Instant::now();
            let mut state = self.state.lock().await;
            if state.is_executed(&txn_id) {
                return AcceptResponse {
                    ok: true,
                    promised: Ballot::zero(),
                };
            }
            state.records.entry(txn_id).or_insert_with(|| Record {
                promised: Ballot::zero(),
                accepted_ballot: None,
                command: None,
                command_digest: None,
                keys: None,
                seq: 0,
                deps: BTreeSet::new(),
                status: Status::None,
                updated_at: now,
            });

            let current_promised = state
                .records
                .get(&txn_id)
                .expect("record must exist")
                .promised;
            if ballot < current_promised {
                return AcceptResponse {
                    ok: false,
                    promised: current_promised,
                };
            }

            {
                let rec = state.records.get_mut(&txn_id).expect("record must exist");
                rec.promised = ballot;
                rec.updated_at = now;
            }

            if state
                .records
                .get(&txn_id)
                .is_some_and(|r| r.status >= Status::Committed)
            {
                let rec = state.records.get(&txn_id).expect("record must exist");
                return AcceptResponse {
                    ok: true,
                    promised: rec.promised,
                };
            }

            {
                let rec = state.records.get_mut(&txn_id).expect("record must exist");
                if rec.command.is_none() {
                    if let Some(cmd) = incoming_command.take() {
                        if command_digest(&cmd) != expected_digest {
                            return AcceptResponse {
                                ok: false,
                                promised: rec.promised,
                            };
                        }
                        let keys = if cmd.is_empty() {
                            CommandKeys::default()
                        } else {
                            match self.sm.command_keys(&cmd) {
                                Ok(keys) => keys,
                                Err(_) => {
                                    return AcceptResponse {
                                        ok: false,
                                        promised: rec.promised,
                                    };
                                }
                            }
                        };
                        rec.command = Some(detach_command_bytes(&cmd));
                        rec.command_digest = Some(expected_digest);
                        rec.keys = Some(keys);
                    } else {
                        drop(state);
                        match self.fetch_command_from_peers(txn_id, expected_digest).await {
                            Ok(Some(cmd)) => {
                                incoming_command = Some(cmd);
                                continue;
                            }
                            _ => {
                                let promised = {
                                    let state = self.state.lock().await;
                                    state
                                        .records
                                        .get(&txn_id)
                                        .map(|r| r.promised)
                                        .unwrap_or(ballot)
                                };
                                return AcceptResponse {
                                    ok: false,
                                    promised,
                                };
                            }
                        }
                    }
                } else {
                    let cmd = rec.command.as_ref().expect("command exists");
                    if command_digest(cmd) != expected_digest {
                        return AcceptResponse {
                            ok: false,
                            promised: rec.promised,
                        };
                    }
                    if rec.command_digest.is_none() {
                        rec.command_digest = Some(expected_digest);
                    }
                    if rec.keys.is_none() {
                        let keys = if cmd.is_empty() {
                            CommandKeys::default()
                        } else {
                            match self.sm.command_keys(cmd) {
                                Ok(keys) => keys,
                                Err(_) => {
                                    return AcceptResponse {
                                        ok: false,
                                        promised: rec.promised,
                                    };
                                }
                            }
                        };
                        rec.keys = Some(keys);
                    }
                }
            }

            let req_deps = deps_vec.iter().copied().collect::<BTreeSet<_>>();
            let is_monotonic = {
                let rec = state.records.get(&txn_id).expect("record must exist");
                is_monotonic_update(rec.seq, &rec.deps, seq, &req_deps)
            };
            if !is_monotonic {
                let rec = state.records.get(&txn_id).expect("record must exist");
                return AcceptResponse {
                    ok: false,
                    promised: rec.promised,
                };
            }

            let keys = state
                .records
                .get(&txn_id)
                .and_then(|r| r.keys.as_ref())
                .cloned()
                .unwrap_or_default();
            state.update_frontier(txn_id, &keys, &req_deps);

            {
                let rec = state.records.get_mut(&txn_id).expect("record must exist");
                rec.seq = seq;
                rec.deps = req_deps;
                rec.accepted_ballot = Some(ballot);
                rec.status = rec.status.max(Status::Accepted);
            }

            let rec = state.records.get(&txn_id).expect("record must exist");
            return AcceptResponse {
                ok: true,
                promised: rec.promised,
            };
        }
    }

    /// Handle incoming commit RPC and transition transaction state toward execution.
    ///
    /// Input:
    /// - `req`: commit metadata (txn id, ballot, command/digest, seq, deps).
    ///
    /// Output:
    /// - `CommitResponse { ok: true }` when this node accepts/records the commit.
    /// - `CommitResponse { ok: false }` when validation, durability, or command
    ///   recovery fails.
    ///
    /// Design:
    /// - In `SyncCommit` mode we durably append to WAL *before* moving status to
    ///   `Committed` and ACKing, so ACK implies local fsync durability.
    /// - In `AsyncCommit` mode we preserve historical behavior: update state and
    ///   ACK, then enqueue WAL append asynchronously.
    pub async fn rpc_commit(&self, req: CommitRequest) -> CommitResponse {
        self.rpc_commit_with_timings(req).await.0
    }

    /// Execute local commit handling and return passive timing breakdown.
    ///
    /// Purpose:
    /// - Let the coordinator attribute commit cost without changing the public
    ///   RPC response or ACK semantics.
    ///
    /// Design:
    /// - Reuses the exact `rpc_commit` logic and only accumulates wall-clock
    ///   timing around the synchronous durable append wait.
    ///
    /// Inputs:
    /// - `req`: commit metadata received locally or from the coordinator path.
    ///
    /// Outputs:
    /// - Tuple of `CommitResponse` plus passive local timing breakdown.
    async fn rpc_commit_with_timings(
        &self,
        req: CommitRequest,
    ) -> (CommitResponse, CommitRpcTimings) {
        let start = time::Instant::now();
        let mut timings = CommitRpcTimings::default();
        let response = if !self.local_is_member() {
            CommitResponse { ok: false }
        } else {
            let CommitRequest {
                group_id: _,
                txn_id,
                ballot,
                command,
                command_digest: expected_digest,
                has_command,
                seq,
                deps,
            } = req;
            let mut incoming_command = if has_command { Some(command) } else { None };
            let deps_vec = deps;

            'commit: loop {
                let now = time::Instant::now();
                let mut state = self.state.lock().await;
                if state.is_executed(&txn_id) {
                    break 'commit CommitResponse { ok: true };
                }
                state.records.entry(txn_id).or_insert_with(|| Record {
                    promised: Ballot::zero(),
                    accepted_ballot: None,
                    command: None,
                    command_digest: None,
                    keys: None,
                    seq: 0,
                    deps: BTreeSet::new(),
                    status: Status::None,
                    updated_at: now,
                });

                {
                    let rec = state.records.get_mut(&txn_id).expect("record must exist");
                    // Commit is final. Even if we've promised a higher ballot
                    // (for example, due to a concurrent recovery), still apply
                    // the commit as long as the command matches. Rejecting a
                    // late commit can strand replicas with long-lived
                    // PreAccepted records.
                    rec.promised = rec.promised.max(ballot);
                    rec.updated_at = now;
                }

                let (observed_status, committed_fast_path) = state
                    .records
                    .get(&txn_id)
                    .map(|r| {
                        let cmd_matches = r
                            .command
                            .as_ref()
                            .map(|cmd| command_digest(cmd) == expected_digest)
                            .unwrap_or(false);
                        let req_deps = deps_vec.iter().copied().collect::<BTreeSet<_>>();
                        let same_commit = r.seq == seq && r.deps == req_deps && cmd_matches;
                        (r.status, r.status >= Status::Committed && same_commit)
                    })
                    .expect("record must exist");

                if observed_status >= Status::Executing {
                    // Already being applied (or done). A late commit must not
                    // downgrade the record or re-insert it into the committed
                    // queue.
                    break 'commit CommitResponse { ok: true };
                }

                if committed_fast_path {
                    break 'commit CommitResponse { ok: true };
                }

                {
                    let rec = state.records.get_mut(&txn_id).expect("record must exist");
                    if rec.command.is_none() {
                        if let Some(cmd) = incoming_command.take() {
                            if command_digest(&cmd) != expected_digest {
                                break 'commit CommitResponse { ok: false };
                            }
                            let keys = if cmd.is_empty() {
                                CommandKeys::default()
                            } else {
                                match self.sm.command_keys(&cmd) {
                                    Ok(keys) => keys,
                                    Err(_) => break 'commit CommitResponse { ok: false },
                                }
                            };
                            rec.command = Some(detach_command_bytes(&cmd));
                            rec.command_digest = Some(expected_digest);
                            rec.keys = Some(keys);
                        } else {
                            drop(state);
                            match self.fetch_command_from_peers(txn_id, expected_digest).await {
                                Ok(Some(cmd)) => {
                                    incoming_command = Some(cmd);
                                    continue 'commit;
                                }
                                _ => break 'commit CommitResponse { ok: false },
                            }
                        }
                    } else {
                        let cmd = rec.command.as_ref().expect("command exists");
                        if command_digest(cmd) != expected_digest {
                            break 'commit CommitResponse { ok: false };
                        }
                        if rec.command_digest.is_none() {
                            rec.command_digest = Some(expected_digest);
                        }
                        if rec.keys.is_none() {
                            let keys = if cmd.is_empty() {
                                CommandKeys::default()
                            } else {
                                match self.sm.command_keys(cmd) {
                                    Ok(keys) => keys,
                                    Err(_) => break 'commit CommitResponse { ok: false },
                                }
                            };
                            rec.keys = Some(keys);
                        }
                    }
                }

                let require_durable_ack = self.config.commit_durability_mode.requires_durable_ack();
                let req_deps = deps_vec.iter().copied().collect::<BTreeSet<_>>();
                let seq = seq.max(1);
                let req_deps_vec = req_deps.iter().copied().collect::<Vec<_>>();
                let command_for_log = state
                    .records
                    .get(&txn_id)
                    .and_then(|r| r.command.clone())
                    .unwrap_or_default();

                if require_durable_ack {
                    // Strict durability mode: force WAL append + sync before
                    // any committed-state transition that could be ACKed to the
                    // coordinator.
                    let done_rx = match self.enqueue_commit_log_append(
                        CommitLogEntry {
                            txn_id,
                            seq,
                            deps: req_deps_vec.clone(),
                            command: command_for_log.clone(),
                        },
                        true,
                    ) {
                        Ok(done_rx) => done_rx,
                        Err(err) => {
                            tracing::warn!(
                                error = ?err,
                                txn_id = ?txn_id,
                                "failed to enqueue durable commit-log append"
                            );
                            break 'commit CommitResponse { ok: false };
                        }
                    };
                    drop(state);
                    let wait_start = time::Instant::now();
                    let append_completion =
                        match Self::wait_commit_log_append(done_rx, self.config.propose_timeout) {
                            Ok(completion) => completion,
                            Err(err) => {
                                timings.durable_wait_us = timings.durable_wait_us.saturating_add(
                                    wait_start.elapsed().as_micros().min(u128::from(u64::MAX))
                                        as u64,
                                );
                                tracing::warn!(
                                    error = ?err,
                                    txn_id = ?txn_id,
                                    "durable commit-log append failed"
                                );
                                break 'commit CommitResponse { ok: false };
                            }
                        };
                    timings.durable_queue_wait_us = timings
                        .durable_queue_wait_us
                        .saturating_add(append_completion.queue_wait_us);
                    timings.durable_append_us = timings
                        .durable_append_us
                        .saturating_add(append_completion.append_us);
                    timings.durable_wait_us = timings.durable_wait_us.saturating_add(
                        append_completion
                            .queue_wait_us
                            .saturating_add(append_completion.append_us),
                    );
                    let post_durable_state_start = time::Instant::now();
                    state = self.state.lock().await;
                    // Another task may have advanced this txn while we were
                    // waiting on WAL I/O; re-check terminal/committed status
                    // before mutating.
                    if state.is_executed(&txn_id) {
                        timings.post_durable_state_update_us =
                            timings.post_durable_state_update_us.saturating_add(
                                post_durable_state_start
                                    .elapsed()
                                    .as_micros()
                                    .min(u128::from(u64::MAX))
                                    as u64,
                            );
                        break 'commit CommitResponse { ok: true };
                    }
                    let committed_after_wait = state
                        .records
                        .get(&txn_id)
                        .map(|r| {
                            let cmd_matches = r
                                .command
                                .as_ref()
                                .map(|cmd| command_digest(cmd) == expected_digest)
                                .unwrap_or(false);
                            let same_commit = r.seq == seq && r.deps == req_deps && cmd_matches;
                            (r.status, r.status >= Status::Committed && same_commit)
                        })
                        .unwrap_or((Status::None, false));
                    if committed_after_wait.0 >= Status::Executing || committed_after_wait.1 {
                        timings.post_durable_state_update_us =
                            timings.post_durable_state_update_us.saturating_add(
                                post_durable_state_start
                                    .elapsed()
                                    .as_micros()
                                    .min(u128::from(u64::MAX))
                                    as u64,
                            );
                        break 'commit CommitResponse { ok: true };
                    }
                }

                let post_state_start = if require_durable_ack {
                    Some(time::Instant::now())
                } else {
                    None
                };

                let (prev_status, prev_seq) = state
                    .records
                    .get(&txn_id)
                    .map(|r| (r.status, r.seq))
                    .unwrap_or((Status::None, 0));
                let keys = state
                    .records
                    .get(&txn_id)
                    .and_then(|r| r.keys.as_ref())
                    .cloned()
                    .unwrap_or_default();
                if keys.is_write() {
                    for key in keys.keys() {
                        match state.last_committed_write_by_key.get(key) {
                            Some((_, cur_seq)) if *cur_seq >= seq => {}
                            _ => {
                                state
                                    .last_committed_write_by_key
                                    .insert(key.clone(), (txn_id, seq));
                            }
                        }
                    }
                }
                state.update_frontier(txn_id, &keys, &req_deps);

                {
                    let rec = state.records.get_mut(&txn_id).expect("record must exist");
                    rec.seq = seq;
                    rec.deps = req_deps;
                    rec.accepted_ballot = Some(rec.accepted_ballot.unwrap_or(ballot).max(ballot));
                    rec.status = rec.status.max(Status::Committed);
                }

                if let Some(post_state_start) = post_state_start {
                    timings.post_durable_state_update_us =
                        timings.post_durable_state_update_us.saturating_add(
                            post_state_start
                                .elapsed()
                                .as_micros()
                                .min(u128::from(u64::MAX)) as u64,
                        );
                }

                if prev_status >= Status::Committed && prev_seq != seq {
                    state.remove_committed(txn_id, prev_seq);
                }
                let should_notify = state.committed_queue.is_empty();
                state.insert_committed(txn_id, seq);
                drop(state);
                if should_notify {
                    self.executor_notify.notify_one();
                }
                if !require_durable_ack && self.commit_log_tx.is_some() {
                    // Async mode preserves original ACK semantics: enqueue WAL
                    // append after state transition and return without waiting
                    // for fsync.
                    let enqueue_res = self.enqueue_commit_log_append(
                        CommitLogEntry {
                            txn_id,
                            seq,
                            deps: req_deps_vec,
                            command: command_for_log,
                        },
                        false,
                    );
                    if let Err(err) = enqueue_res {
                        tracing::warn!(
                            error = ?err,
                            txn_id = ?txn_id,
                            "failed to enqueue commit-log append"
                        );
                    }
                }
                // Avoid per-commit wakeups; executor polls on a short interval
                // already.
                break 'commit CommitResponse { ok: true };
            }
        };

        timings.total_us = start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        timings.state_update_us = timings.total_us.saturating_sub(timings.durable_wait_us);
        (response, timings)
    }

    pub async fn rpc_fetch_command(&self, txn_id: TxnId) -> Option<Bytes> {
        let in_memory = {
            let state = self.state.lock().await;
            if let Some(rec) = state.records.get(&txn_id) {
                if let Some(cmd) = rec.command.as_ref() {
                    return Some(cmd.clone());
                }
            }
            state
                .executed_log
                .get(&txn_id)
                .and_then(|entry| entry.command.clone())
        };
        in_memory.or_else(|| self.load_command_from_commit_log(txn_id))
    }

    fn load_command_from_commit_log(&self, txn_id: TxnId) -> Option<Bytes> {
        let log = self.commit_log.as_ref()?;
        let entries = match log.load() {
            Ok(entries) => entries,
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    txn_id = ?txn_id,
                    "failed to load commit log while fetching command"
                );
                return None;
            }
        };
        entries
            .into_iter()
            .rev()
            .find(|entry| entry.txn_id == txn_id)
            .map(|entry| entry.command)
    }

    async fn fetch_command_from_peers(
        &self,
        txn_id: TxnId,
        expected_digest: [u8; 32],
    ) -> anyhow::Result<Option<Bytes>> {
        let peers = self.peers_round_robin();
        if peers.is_empty() {
            return Ok(None);
        }

        let mut futs = FuturesUnordered::new();
        for peer in peers {
            let transport = self.transport.clone();
            let group_id = self.config.group_id;
            futs.push(async move { transport.fetch_command(peer, group_id, txn_id).await });
        }

        while let Some(resp) = futs.next().await {
            if let Ok(Some(cmd)) = resp {
                if command_digest(&cmd) == expected_digest {
                    return Ok(Some(cmd));
                }
            }
        }

        Ok(None)
    }

    pub async fn rpc_recover(&self, req: RecoverRequest) -> RecoverResponse {
        if !self.local_is_voter() {
            let (command, command_digest, has_command) = recover_response_payload(None, None);
            return RecoverResponse {
                ok: false,
                promised: Ballot::zero(),
                status: TxnStatus::Unknown,
                accepted_ballot: None,
                command,
                command_digest,
                has_command,
                seq: 0,
                deps: Vec::new(),
            };
        }
        let now = time::Instant::now();
        let mut state = self.state.lock().await;
        if state.is_executed(&req.txn_id) {
            let (command, command_digest, seq, deps) = state
                .executed_log
                .get(&req.txn_id)
                .map(|entry| {
                    (
                        entry.command.clone(),
                        entry.command_digest,
                        entry.seq,
                        entry.deps.clone(),
                    )
                })
                .unwrap_or((None, None, 1, Vec::new()));
            drop(state);
            let command = command.or_else(|| self.load_command_from_commit_log(req.txn_id));
            let (command, command_digest, has_command) =
                recover_response_payload(command, command_digest);
            return RecoverResponse {
                ok: true,
                promised: Ballot::zero(),
                status: TxnStatus::Executed,
                accepted_ballot: None,
                command,
                command_digest,
                has_command,
                seq,
                deps,
            };
        }

        let rec = state.records.entry(req.txn_id).or_insert_with(|| Record {
            promised: Ballot::zero(),
            accepted_ballot: None,
            command: None,
            command_digest: None,
            keys: None,
            seq: 0,
            deps: BTreeSet::new(),
            status: Status::None,
            updated_at: now,
        });

        if rec.status >= Status::Committed {
            let promised = rec.promised;
            let status = status_to_txn_status(rec.status);
            let accepted_ballot = rec.accepted_ballot;
            let seq = rec.seq;
            let deps = rec.deps.iter().copied().collect::<Vec<_>>();
            let mut command = rec.command.clone();
            let digest_hint = rec.command_digest;
            drop(state);

            // When command bytes are absent from the in-memory record, try the
            // commit log before responding so recovery can avoid false
            // "committed but no command" stalls.
            if command.is_none() {
                command = self.load_command_from_commit_log(req.txn_id);
            }
            let (command, command_digest, has_command) =
                recover_response_payload(command, digest_hint);
            return RecoverResponse {
                ok: true,
                promised,
                status,
                accepted_ballot,
                command,
                command_digest,
                has_command,
                seq,
                deps,
            };
        }

        if req.ballot < rec.promised {
            let (command, command_digest, has_command) =
                recover_response_payload(rec.command.clone(), rec.command_digest);
            return RecoverResponse {
                ok: false,
                promised: rec.promised,
                status: status_to_txn_status(rec.status),
                accepted_ballot: rec.accepted_ballot,
                command,
                command_digest,
                has_command,
                seq: rec.seq,
                deps: rec.deps.iter().copied().collect(),
            };
        }

        rec.promised = req.ballot;
        rec.updated_at = now;
        let (command, command_digest, has_command) =
            recover_response_payload(rec.command.clone(), rec.command_digest);

        RecoverResponse {
            ok: true,
            promised: rec.promised,
            status: status_to_txn_status(rec.status),
            accepted_ballot: rec.accepted_ballot,
            command,
            command_digest,
            has_command,
            seq: rec.seq,
            deps: rec.deps.iter().copied().collect(),
        }
    }

    pub async fn rpc_report_executed(&self, req: ReportExecutedRequest) -> ReportExecutedResponse {
        let now = time::Instant::now();
        let mut state = self.state.lock().await;

        let mut prefixes = state
            .reported_executed_prefix_by_peer
            .remove(&req.from_node_id)
            .unwrap_or_default();
        for p in req.prefixes {
            prefixes.insert(p.node_id, p.counter);
        }
        state
            .reported_executed_prefix_by_peer
            .insert(req.from_node_id, prefixes);

        let runtime_members = self
            .voters_snapshot()
            .into_iter()
            .map(|id| Member { id })
            .collect::<Vec<_>>();
        let _ = Self::maybe_gc_executed_log_locked(
            self.config.node_id,
            &runtime_members,
            &mut state,
            now,
            self.config.executed_command_cache_max_bytes,
        );
        let _ = Self::maybe_compact_state_locked(&mut state, now);

        ReportExecutedResponse { ok: true }
    }

    async fn propose(self: &Arc<Self>, command: Bytes) -> anyhow::Result<ProposalResult> {
        self.propose_with_deps(command, None).await
    }

    async fn propose_read_with_deps(
        self: &Arc<Self>,
        command: Bytes,
        deps: Vec<(TxnId, u64)>,
    ) -> anyhow::Result<ProposalResult> {
        self.propose_with_deps(command, Some(deps)).await
    }

    async fn propose_with_deps(
        self: &Arc<Self>,
        command: Bytes,
        extra_deps: Option<Vec<(TxnId, u64)>>,
    ) -> anyhow::Result<ProposalResult> {
        let start = time::Instant::now();
        let is_read = self
            .sm
            .command_keys(&command)
            .map(|k| !k.is_write() && !k.reads.is_empty())
            .unwrap_or(false);
        if extra_deps.is_some() && !is_read {
            anyhow::bail!("explicit deps only supported for reads");
        }
        let needs_execution = is_read;
        let command_digest = command_digest(&command);

        let (txn_id, initial_ballot) = {
            let mut state = self.state.lock().await;
            state.next_txn_counter = state.next_txn_counter.saturating_add(1);
            let txn_id = self.compose_txn_id(state.next_txn_counter)?;
            let ballot = Ballot::initial(self.config.node_id);
            (txn_id, ballot)
        };

        let mut read_rx = None;
        if is_read {
            let (tx, rx) = oneshot::channel();
            let mut state = self.state.lock().await;
            state.read_waiters.insert(txn_id, tx);
            read_rx = Some(rx);
        }

        let deadline = start + self.config.propose_timeout;
        let mut ballot = initial_ballot;
        let mut backoff = Duration::from_millis(10);

        let forced = extra_deps.map(|deps| {
            let mut unique = BTreeSet::new();
            let mut max_seq = 0u64;
            for (txn_id, seq) in deps {
                unique.insert(txn_id);
                max_seq = max_seq.max(seq);
            }
            (unique.into_iter().collect::<Vec<_>>(), max_seq)
        });

        let result = loop {
            if time::Instant::now() > deadline {
                let mut state = self.state.lock().await;
                state.proposal_timeouts = state.proposal_timeouts.saturating_add(1);
                break Err(anyhow::anyhow!("proposal timed out"));
            }

            match self
                .propose_once(
                    txn_id,
                    ballot,
                    command.clone(),
                    command_digest,
                    needs_execution,
                    forced.as_ref(),
                )
                .await
            {
                Ok(phase) => {
                    if !needs_execution {
                        tracing::debug!(
                            txn_id = ?txn_id,
                            pre_accept_us = phase.pre_accept_us,
                            accept_us = phase.accept_us,
                            commit_us = phase.commit_us,
                            commit_local_state_update_us = phase.commit_local_state_update_us,
                            commit_local_durable_wait_us = phase.commit_local_durable_wait_us,
                            commit_local_log_queue_wait_us = phase.commit_local_log_queue_wait_us,
                            commit_local_log_append_us = phase.commit_local_log_append_us,
                            commit_local_post_durable_state_update_us = phase.commit_local_post_durable_state_update_us,
                            commit_remote_quorum_wait_us = phase.commit_remote_quorum_wait_us,
                            execute_us = phase.execute_us,
                            visible_us = phase.visible_us,
                            "write propose phase timings"
                        );
                    }
                    if let Some(rx) = read_rx {
                        let remaining = deadline.saturating_duration_since(time::Instant::now());
                        let v = time::timeout(remaining, rx)
                            .await
                            .context("read result timed out")?
                            .context("read waiter dropped")??;
                        break Ok(ProposalResult::Read(v));
                    }
                    break Ok(ProposalResult::Applied);
                }
                Err(ProposeOnceError::Rejected { promised }) => {
                    ballot = self.next_ballot_after(promised).await;
                    time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_millis(200));
                }
                Err(ProposeOnceError::NoQuorum(err)) => break Err(err),
            }
        };

        if result.is_err() && is_read {
            let mut state = self.state.lock().await;
            state.read_waiters.remove(&txn_id);
        }

        result
    }

    async fn gossip_executed_prefixes(&self) -> anyhow::Result<()> {
        let peers = self.peers_round_robin();
        if peers.is_empty() {
            return Ok(());
        }

        let prefixes = {
            let state = self.state.lock().await;
            state
                .executed_prefix_by_node
                .iter()
                .map(|(node_id, counter)| ExecutedPrefix {
                    node_id: *node_id,
                    counter: *counter,
                })
                .collect::<Vec<_>>()
        };

        let req = ReportExecutedRequest {
            group_id: self.config.group_id,
            from_node_id: self.config.node_id,
            prefixes,
        };

        for peer in peers {
            let _ = self.transport.report_executed(peer, req.clone()).await;
        }

        Ok(())
    }

    async fn maybe_compact_state(&self) {
        let now = time::Instant::now();
        let mut state = self.state.lock().await;
        let _ = Self::maybe_compact_state_locked(&mut state, now);
    }

    fn maybe_compact_state_locked(state: &mut State, now: time::Instant) -> bool {
        const COMPACT_INTERVAL: Duration = Duration::from_secs(2);
        const MIN_SHRINK_CAPACITY: usize = 16 * 1024;

        if now.duration_since(state.last_compact_at) < COMPACT_INTERVAL {
            return false;
        }

        let idle = state.records.is_empty()
            && state.committed_queue.is_empty()
            && state.recovering.is_empty()
            && state.read_waiters.is_empty();
        if !idle {
            return false;
        }

        let mut did = false;

        if state.records.capacity() > MIN_SHRINK_CAPACITY {
            state.records.shrink_to_fit();
            did = true;
        }
        if state.frontier_by_key.capacity() > MIN_SHRINK_CAPACITY
            && state.frontier_by_key.is_empty()
        {
            state.frontier_by_key.shrink_to_fit();
            did = true;
        }
        if state.executed_out_of_order.capacity() > MIN_SHRINK_CAPACITY
            && state.executed_out_of_order.is_empty()
        {
            state.executed_out_of_order.shrink_to_fit();
            did = true;
        }
        if state.recovering.capacity() > MIN_SHRINK_CAPACITY && state.recovering.is_empty() {
            state.recovering.shrink_to_fit();
            did = true;
        }
        if state.read_waiters.capacity() > MIN_SHRINK_CAPACITY && state.read_waiters.is_empty() {
            state.read_waiters.shrink_to_fit();
            did = true;
        }

        // If we've garbage-collected most of the executed log, also try to compact its metadata.
        if state.executed_log.len() < 1024 && state.executed_log.capacity() > MIN_SHRINK_CAPACITY {
            state.executed_log.shrink_to_fit();
            state.executed_log_order.shrink_to_fit();
            did = true;
        }

        if did {
            state.last_compact_at = now;
        }
        did
    }

    fn maybe_gc_executed_log_locked(
        node_id: NodeId,
        members: &[Member],
        state: &mut State,
        now: time::Instant,
        max_executed_command_cache_bytes: usize,
    ) -> usize {
        const GC_INTERVAL: Duration = Duration::from_millis(500);
        const MAX_GC_SCAN: usize = 16_384;
        const MAX_SHED_SCAN: usize = 8_192;
        if now.duration_since(state.last_executed_gc_at) < GC_INTERVAL {
            return 0;
        }

        let mut global_min_by_node: HashMap<NodeId, u64> = HashMap::new();
        for member in members {
            let origin = member.id;
            let mut min_prefix = state
                .executed_prefix_by_node
                .get(&origin)
                .copied()
                .unwrap_or(0);

            for peer in members {
                let peer_id = peer.id;
                if peer_id == node_id {
                    continue;
                }
                let Some(peer_prefixes) = state.reported_executed_prefix_by_peer.get(&peer_id)
                else {
                    min_prefix = 0;
                    break;
                };
                let reported = peer_prefixes.get(&origin).copied().unwrap_or(0);
                min_prefix = min_prefix.min(reported);
                if min_prefix == 0 {
                    break;
                }
            }

            global_min_by_node.insert(origin, min_prefix);
        }

        let mut removed = 0usize;
        // Bound per-tick work to avoid long mutex hold times on large logs.
        let gc_scan = state.executed_log_order.len().min(MAX_GC_SCAN);
        for _ in 0..gc_scan {
            let Some(id) = state.executed_log_order.pop_front() else {
                break;
            };
            if state.visible_txns.contains(&id) {
                let min_prefix = global_min_by_node.get(&id.node_id).copied().unwrap_or(0);
                if id.counter <= min_prefix {
                    if let Some(entry) = state.executed_log.remove(&id) {
                        state.executed_log_bytes = state
                            .executed_log_bytes
                            .saturating_sub(entry.command.as_ref().map_or(0, Bytes::len));
                        state.executed_log_deps_total = state
                            .executed_log_deps_total
                            .saturating_sub(entry.deps.len());
                    }
                    state.visible_txns.remove(&id);
                    removed = removed.saturating_add(1);
                    continue;
                }
            }
            state.executed_log_order.push_back(id);
        }

        // Keep executed metadata, but shed visible command payloads once the
        // configured in-memory command cache budget is exceeded.
        if state.executed_log_bytes > max_executed_command_cache_bytes {
            let mut overflow = state
                .executed_log_bytes
                .saturating_sub(max_executed_command_cache_bytes);
            let shed_scan = state.executed_log_order.len().min(MAX_SHED_SCAN);
            for _ in 0..shed_scan {
                if overflow == 0 {
                    break;
                }
                let Some(id) = state.executed_log_order.pop_front() else {
                    break;
                };
                if state.visible_txns.contains(&id) {
                    let min_prefix = global_min_by_node.get(&id.node_id).copied().unwrap_or(0);
                    if id.counter > min_prefix {
                        // Do not shed command bytes for entries that are not
                        // globally visible yet; lagging replicas may still
                        // need fetch/recovery payloads for these txns.
                        state.executed_log_order.push_back(id);
                        continue;
                    }
                    if let Some(entry) = state.executed_log.get_mut(&id) {
                        if let Some(command) = entry.command.take() {
                            let len = command.len();
                            if len > 0 {
                                state.executed_log_bytes =
                                    state.executed_log_bytes.saturating_sub(len);
                                overflow = overflow.saturating_sub(len);
                                removed = removed.saturating_add(1);
                            }
                        }
                    }
                }
                state.executed_log_order.push_back(id);
            }
        }

        state.last_executed_gc_at = now;
        removed
    }

    async fn propose_once(
        self: &Arc<Self>,
        txn_id: TxnId,
        ballot: Ballot,
        command: Bytes,
        command_digest: [u8; 32],
        needs_execution: bool,
        forced: Option<&(Vec<TxnId>, u64)>,
    ) -> Result<PhaseTimings, ProposeOnceError> {
        const SLOW_READ_PROPOSE_US: u64 = 50_000;
        const SLOW_WRITE_PROPOSE_US: u64 = 50_000;
        let peers = self.peers_round_robin();
        let local_is_voter = self.local_is_voter();
        let member_count = self.voters_snapshot().len().max(1);
        let quorum = self.quorum();
        let read_quorum = if needs_execution {
            member_count
        } else {
            quorum
        };
        let fast_quorum = if needs_execution {
            read_quorum
        } else {
            self.fast_quorum()
        };
        let rpc_timeout = self.config.rpc_timeout;
        let log_phases = true;
        let mut pre_accept_us = 0u64;
        let mut accept_us = 0u64;
        let mut execute_us = 0u64;
        let visible_us = 0u64;
        let mut received = 0usize;
        let mut ok = 0usize;
        let mut fast_path = false;
        let mut merged_seq = 0u64;
        let mut merged_deps_len = 0usize;

        let pre_accept_start = time::Instant::now();
        let (forced_deps, forced_seq) = forced
            .map(|(deps, seq)| (deps.clone(), *seq))
            .unwrap_or_else(|| (Vec::new(), 0u64));
        let forced_seq = if forced_deps.is_empty() {
            forced_seq
        } else {
            forced_seq.saturating_add(1)
        };
        let mut request_seq = forced_seq;
        let mut request_deps = forced_deps.clone();
        let mut oks: Vec<PreAcceptResponse> = Vec::new();
        let mut max_promised = ballot;

        if local_is_voter {
            let local = self
                .rpc_pre_accept(PreAcceptRequest {
                    group_id: self.config.group_id,
                    txn_id,
                    ballot,
                    command: command.clone(),
                    seq: forced_seq,
                    deps: forced_deps.clone(),
                })
                .await;

            if !local.ok {
                return Err(ProposeOnceError::Rejected {
                    promised: local.promised,
                });
            }
            received = 1;
            ok = 1;
            max_promised = max_promised.max(local.promised);
            request_seq = local.seq.max(forced_seq);
            request_deps = if forced_deps.is_empty() {
                local.deps.clone()
            } else {
                let mut deps = local.deps.clone();
                deps.extend(forced_deps.iter().copied());
                deps.into_iter()
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect()
            };
            oks.push(local);
        }

        let (tx, mut rx) = mpsc::channel::<anyhow::Result<PreAcceptResponse>>(peers.len().max(1));
        for peer in peers.iter().copied() {
            let transport = self.transport.clone();
            let tx = tx.clone();
            let req = PreAcceptRequest {
                group_id: self.config.group_id,
                txn_id,
                ballot,
                command: command.clone(),
                seq: request_seq,
                deps: request_deps.clone(),
            };
            tokio::spawn(async move {
                let resp = match time::timeout(rpc_timeout, transport.pre_accept(peer, req)).await {
                    Ok(resp) => resp.map_err(|e| anyhow::anyhow!("pre_accept rpc failed: {e}")),
                    Err(_) => Err(anyhow::anyhow!("pre_accept rpc timed out")),
                };
                let _ = tx.send(resp).await;
                Ok::<(), anyhow::Error>(())
            });
        }
        drop(tx);

        let deadline = time::Instant::now() + rpc_timeout;
        while ok < read_quorum {
            let remaining = deadline.saturating_duration_since(time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let recv = time::timeout(remaining, rx.recv()).await;
            let Ok(Some(resp)) = recv else {
                break;
            };
            received += 1;
            match resp {
                Ok(r) => {
                    max_promised = max_promised.max(r.promised);
                    if r.ok {
                        oks.push(r);
                        ok += 1;
                    }
                }
                Err(_) => {}
            }

            if ok >= read_quorum {
                break;
            }
        }
        pre_accept_us = pre_accept_us.saturating_add(pre_accept_start.elapsed().as_micros() as u64);

        if ok < read_quorum {
            if max_promised > ballot {
                return Err(ProposeOnceError::Rejected {
                    promised: max_promised,
                });
            }
            let err = ProposeOnceError::NoQuorum(anyhow::anyhow!(
                "failed to reach quorum during PreAccept (ok={ok}, quorum={read_quorum})"
            ));
            if log_phases {
                tracing::warn!(
                    txn_id = ?txn_id,
                    ballot = ?ballot,
                    ok = ok,
                    received = received,
                    quorum = read_quorum,
                    fast_quorum = fast_quorum,
                    pre_accept_us = pre_accept_us,
                    "read propose failed during pre_accept"
                );
            }
            return Err(err);
        }

        let mut merged = merge_preaccept(&oks);
        if !forced_deps.is_empty() {
            let mut merged_set = merged.deps.iter().copied().collect::<BTreeSet<_>>();
            merged_set.extend(forced_deps.iter().copied());
            merged.deps = merged_set.into_iter().collect();
            merged.seq = merged.seq.max(forced_seq);
        }
        merged_seq = merged_seq.max(merged.seq);
        merged_deps_len = merged_deps_len.max(merged.deps.len());

        fast_path |= forced_deps.is_empty()
            && received >= fast_quorum
            && oks.len() == fast_quorum
            && oks
                .iter()
                .all(|r| r.seq == merged.seq && r.deps == merged.deps);

        if !fast_path {
            let accept_start = time::Instant::now();
            let accept_res = self
                .run_accept_round(
                    txn_id,
                    ballot,
                    command.clone(),
                    command_digest,
                    merged.seq,
                    merged.deps.clone(),
                    self.config.inline_command_in_accept_commit,
                )
                .await?;
            accept_us = accept_start.elapsed().as_micros() as u64;
            if !accept_res.ok {
                if log_phases {
                    tracing::warn!(
                        txn_id = ?txn_id,
                        ballot = ?ballot,
                        pre_accept_us = pre_accept_us,
                        accept_us = accept_us,
                        fast_path = fast_path,
                        merged_seq = merged_seq,
                        merged_deps_len = merged_deps_len,
                        promised = ?accept_res.promised,
                        "read propose rejected during accept"
                    );
                }
                return Err(ProposeOnceError::Rejected {
                    promised: accept_res.promised,
                });
            }
        }

        let commit_start = time::Instant::now();
        let fast_ack_1rtt =
            !needs_execution && fast_path && self.config.fast_path_1rtt && local_is_voter;
        let commit_timings = if fast_ack_1rtt {
            let seq = merged.seq;
            let deps = merged.deps;
            let local_timings = self
                .commit_locally_for_fast_ack(
                    txn_id,
                    ballot,
                    command.clone(),
                    command_digest,
                    seq,
                    deps.clone(),
                )
                .await?;
            self.spawn_async_commit_dissemination(
                txn_id,
                ballot,
                command,
                command_digest,
                seq,
                deps,
                self.config.inline_command_in_accept_commit,
            );
            local_timings
        } else {
            self.run_commit_round(
                txn_id,
                ballot,
                command,
                command_digest,
                merged.seq,
                merged.deps,
                self.config.inline_command_in_accept_commit,
            )
            .await?
        };
        let commit_us = commit_start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        if needs_execution {
            let exec_start = time::Instant::now();
            if let Err(err) = self.execute_until(txn_id).await {
                execute_us = execute_us.saturating_add(exec_start.elapsed().as_micros() as u64);
                if log_phases {
                    tracing::warn!(
                        txn_id = ?txn_id,
                        ballot = ?ballot,
                        pre_accept_us = pre_accept_us,
                        accept_us = accept_us,
                        commit_us = commit_us,
                        commit_local_state_update_us = commit_timings.local_state_update_us,
                        commit_local_durable_wait_us = commit_timings.local_durable_wait_us,
                        commit_local_log_queue_wait_us = commit_timings.local_log_queue_wait_us,
                        commit_local_log_append_us = commit_timings.local_log_append_us,
                        commit_local_post_durable_state_update_us = commit_timings.local_post_durable_state_update_us,
                        commit_remote_quorum_wait_us = commit_timings.remote_quorum_wait_us,
                        execute_us = execute_us,
                        fast_path = fast_path,
                        merged_seq = merged_seq,
                        merged_deps_len = merged_deps_len,
                        error = ?err,
                        "read propose failed during execute"
                    );
                }
                return Err(ProposeOnceError::from(err));
            }
            execute_us = execute_us.saturating_add(exec_start.elapsed().as_micros() as u64);
        } else {
            // Write proposals return once the transaction is safely published
            // for the selected path; execution continues asynchronously.
            self.executor_notify.notify_one();
        }
        if !needs_execution {
            self.metrics.record_fast_path(fast_path);
        }
        let total_us = pre_accept_us + accept_us + commit_us + execute_us + visible_us;
        if log_phases {
            let slow_threshold = if needs_execution {
                SLOW_READ_PROPOSE_US
            } else {
                SLOW_WRITE_PROPOSE_US
            };
            if total_us >= slow_threshold {
                let op_kind = if needs_execution { "read" } else { "write" };
                tracing::info!(
                    txn_id = ?txn_id,
                    ballot = ?ballot,
                    op_kind = op_kind,
                    ok = ok,
                    received = received,
                    quorum = quorum,
                    fast_quorum = fast_quorum,
                    pre_accept_us = pre_accept_us,
                    accept_us = accept_us,
                    commit_us = commit_us,
                    commit_local_state_update_us = commit_timings.local_state_update_us,
                    commit_local_durable_wait_us = commit_timings.local_durable_wait_us,
                    commit_local_log_queue_wait_us = commit_timings.local_log_queue_wait_us,
                    commit_local_log_append_us = commit_timings.local_log_append_us,
                    commit_local_post_durable_state_update_us = commit_timings.local_post_durable_state_update_us,
                    commit_remote_quorum_wait_us = commit_timings.remote_quorum_wait_us,
                    execute_us = execute_us,
                    visible_us = visible_us,
                    total_us = total_us,
                    fast_path = fast_path,
                    merged_seq = merged_seq,
                    merged_deps_len = merged_deps_len,
                    "slow propose"
                );
            }
        }
        Ok(PhaseTimings {
            pre_accept_us,
            accept_us,
            commit_us,
            commit_local_state_update_us: commit_timings.local_state_update_us,
            commit_local_durable_wait_us: commit_timings.local_durable_wait_us,
            commit_local_log_queue_wait_us: commit_timings.local_log_queue_wait_us,
            commit_local_log_append_us: commit_timings.local_log_append_us,
            commit_local_post_durable_state_update_us: commit_timings
                .local_post_durable_state_update_us,
            commit_remote_quorum_wait_us: commit_timings.remote_quorum_wait_us,
            execute_us,
            visible_us,
        })
    }

    #[allow(dead_code)]
    async fn wait_all_executed(&self, txn_id: TxnId) -> anyhow::Result<()> {
        let deadline = time::Instant::now() + self.config.propose_timeout;
        loop {
            if time::Instant::now() > deadline {
                anyhow::bail!("timed out waiting for all replicas to execute {:?}", txn_id);
            }

            let local_done = self.is_executed(txn_id).await;
            if !local_done {
                time::sleep(Duration::from_millis(10)).await;
                continue;
            }

            let peers = self.peers_snapshot();
            if peers.is_empty() {
                return Ok(());
            }

            let mut futs = FuturesUnordered::new();
            for peer in peers {
                let transport = self.transport.clone();
                let group_id = self.config.group_id;
                futs.push(async move { transport.executed(peer, group_id, txn_id).await });
            }

            let mut ok = 0usize;
            let mut all_done = true;
            while let Some(resp) = futs.next().await {
                match resp {
                    Ok(executed) => {
                        ok += 1;
                        if !executed {
                            all_done = false;
                        }
                    }
                    Err(_) => {
                        all_done = false;
                    }
                }
            }

            if ok == 0 {
                all_done = false;
            }

            if all_done {
                return Ok(());
            }

            time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[allow(dead_code)]
    async fn mark_visible_all(&self, txn_id: TxnId) -> anyhow::Result<()> {
        let deadline = time::Instant::now() + self.config.propose_timeout;
        loop {
            if time::Instant::now() > deadline {
                anyhow::bail!(
                    "timed out waiting for all replicas to mark visible {:?}",
                    txn_id
                );
            }

            let local_ok = self.mark_visible(txn_id).await?;
            if !local_ok {
                time::sleep(Duration::from_millis(10)).await;
                continue;
            }

            let peers = self.peers_snapshot();
            if peers.is_empty() {
                return Ok(());
            }

            let mut futs = FuturesUnordered::new();
            for peer in peers {
                let transport = self.transport.clone();
                let group_id = self.config.group_id;
                futs.push(async move { transport.mark_visible(peer, group_id, txn_id).await });
            }

            let mut ok = 0usize;
            let mut all_ok = true;
            while let Some(resp) = futs.next().await {
                match resp {
                    Ok(true) => {
                        ok += 1;
                    }
                    Ok(false) => {
                        all_ok = false;
                    }
                    Err(_) => {
                        all_ok = false;
                    }
                }
            }

            if ok == 0 {
                all_ok = false;
            }

            if all_ok {
                return Ok(());
            }

            time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Publish a fast-path write locally before returning a 1RTT ACK.
    ///
    /// Purpose:
    /// - Preserve the coordinator-side ordering boundary that clients observe:
    ///   the write is locally `Committed`, present in read-barrier metadata, and
    ///   queued for execution before the proposal returns success.
    ///
    /// Design:
    /// - Reuses the normal commit RPC handler so WAL durability, command
    ///   validation, `last_committed` maintenance, and executor notification
    ///   stay identical to the regular Commit path.
    /// - Records only local commit timing. Remote Commit fanout is handled by
    ///   `spawn_async_commit_dissemination`.
    ///
    /// Inputs:
    /// - Final transaction identity, ballot, command digest, sequence, and deps.
    ///
    /// Outputs:
    /// - Local commit timing suitable for proposal metrics.
    async fn commit_locally_for_fast_ack(
        &self,
        txn_id: TxnId,
        ballot: Ballot,
        command: Bytes,
        command_digest: [u8; 32],
        seq: u64,
        deps: Vec<TxnId>,
    ) -> Result<CommitRoundTimings, ProposeOnceError> {
        let (local, local_timings) = self
            .rpc_commit_with_timings(CommitRequest {
                group_id: self.config.group_id,
                txn_id,
                ballot,
                command,
                command_digest,
                has_command: true,
                seq,
                deps,
            })
            .await;
        if !local.ok {
            return Err(ProposeOnceError::NoQuorum(anyhow::anyhow!(
                "local fast-path commit rejected"
            )));
        }

        let timings = CommitRoundTimings {
            local_state_update_us: local_timings.state_update_us,
            local_durable_wait_us: local_timings.durable_wait_us,
            local_log_queue_wait_us: local_timings.durable_queue_wait_us,
            local_log_append_us: local_timings.durable_append_us,
            local_post_durable_state_update_us: local_timings.post_durable_state_update_us,
            remote_quorum_wait_us: 0,
        };
        self.metrics
            .record_commit_local_state(Duration::from_micros(timings.local_state_update_us));
        self.metrics
            .record_commit_local_durable(Duration::from_micros(timings.local_durable_wait_us));
        self.metrics
            .record_commit_local_log_queue(Duration::from_micros(timings.local_log_queue_wait_us));
        self.metrics
            .record_commit_local_log_append(Duration::from_micros(timings.local_log_append_us));
        self.metrics
            .record_commit_local_post_durable_state(Duration::from_micros(
                timings.local_post_durable_state_update_us,
            ));
        Ok(timings)
    }

    /// Disseminate a fast-path Commit decision after the client ACK.
    ///
    /// Purpose:
    /// - Convert a locally published 1RTT fast-path write into the ordinary
    ///   committed state on all reachable replicas without adding a client
    ///   blocking round trip.
    ///
    /// Design:
    /// - Sends Commit to every peer and waits only inside this detached task.
    /// - Treats failure as a liveness event: read barriers and executor stall
    ///   recovery can still force the value from surviving PreAccepted records.
    ///
    /// Inputs:
    /// - Final transaction metadata and whether Commit RPCs should inline bytes.
    ///
    /// Outputs:
    /// - None. Any failed dissemination is logged for diagnosis.
    fn spawn_async_commit_dissemination(
        self: &Arc<Self>,
        txn_id: TxnId,
        ballot: Ballot,
        command: Bytes,
        command_digest: [u8; 32],
        seq: u64,
        deps: Vec<TxnId>,
        inline_command: bool,
    ) {
        let group = Arc::clone(self);
        tokio::spawn(async move {
            group
                .disseminate_commit_to_peers(
                    txn_id,
                    ballot,
                    command,
                    command_digest,
                    seq,
                    deps,
                    inline_command,
                )
                .await;
        });
    }

    /// Send Commit to peers for an already locally published fast-path write.
    ///
    /// Purpose:
    /// - Share the final Commit decision with peers after a 1RTT write ACK.
    ///
    /// Design:
    /// - Excludes local commit work to avoid duplicate WAL appends and local
    ///   timing samples.
    /// - Sends to all peers, counts voter ACKs for observability, and leaves
    ///   recovery responsible for peers that remain unavailable.
    ///
    /// Inputs:
    /// - Final transaction metadata and commit payload policy.
    ///
    /// Outputs:
    /// - None; failures are logged.
    async fn disseminate_commit_to_peers(
        &self,
        txn_id: TxnId,
        ballot: Ballot,
        command: Bytes,
        command_digest: [u8; 32],
        seq: u64,
        deps: Vec<TxnId>,
        inline_command: bool,
    ) {
        let peers = self.peers_snapshot();
        if peers.is_empty() {
            return;
        }

        let quorum = self.quorum();
        let voter_set = self.voters_snapshot().into_iter().collect::<HashSet<_>>();
        let mut ok = if self.local_is_voter() { 1 } else { 0 };
        let commit_timeout = self.config.propose_timeout;
        let (command_payload, has_command) = if inline_command {
            (command, true)
        } else {
            (Bytes::new(), false)
        };

        let mut in_flight = FuturesUnordered::new();
        for peer in peers {
            let transport = self.transport.clone();
            let count_for_quorum = voter_set.contains(&peer);
            let req = CommitRequest {
                group_id: self.config.group_id,
                txn_id,
                ballot,
                command: command_payload.clone(),
                command_digest,
                has_command,
                seq,
                deps: deps.clone(),
            };
            in_flight.push(async move {
                let peer_ok = match time::timeout(commit_timeout, transport.commit(peer, req)).await
                {
                    Ok(res) => res.ok().is_some_and(|r| r.ok),
                    Err(_) => false,
                };
                (peer, peer_ok, count_for_quorum)
            });
        }

        while let Some((peer, peer_ok, count_for_quorum)) = in_flight.next().await {
            if peer_ok && count_for_quorum {
                ok += 1;
            } else if !peer_ok {
                tracing::debug!(
                    txn_id = ?txn_id,
                    peer = peer,
                    "fast-path async commit dissemination failed for peer"
                );
            }
        }

        if ok < quorum {
            tracing::warn!(
                txn_id = ?txn_id,
                ok = ok,
                quorum = quorum,
                "fast-path async commit dissemination did not reach quorum"
            );
        }
    }

    /// Execute one Accept round and stop as soon as voter quorum is reached.
    ///
    /// Purpose:
    /// - Confirm chosen `(seq, deps, command)` with a majority before commit.
    ///
    /// Design:
    /// - Apply locally first when this node is a voter.
    /// - Fan out peer RPCs via `FuturesUnordered` and poll until quorum.
    /// - Drop remaining in-flight futures once quorum is satisfied to avoid
    ///   waiting on slow tail replicas.
    ///
    /// Inputs:
    /// - Transaction identity, ballot, command payload/digest, and chosen
    ///   sequence/dependency metadata.
    /// - `inline_command`: whether Accept/Commit RPCs carry full command bytes.
    ///
    /// Outputs:
    /// - `Ok(AcceptResponse { ok: true, .. })` when quorum accepts.
    /// - `Ok(AcceptResponse { ok: false, promised })` when a higher ballot is observed.
    /// - `Err(ProposeOnceError::NoQuorum)` when quorum cannot be reached in time.
    async fn run_accept_round(
        &self,
        txn_id: TxnId,
        ballot: Ballot,
        command: Bytes,
        command_digest: [u8; 32],
        seq: u64,
        deps: Vec<TxnId>,
        inline_command: bool,
    ) -> Result<AcceptResponse, ProposeOnceError> {
        let peers = self.peers_round_robin();
        let quorum = self.quorum();
        let local_is_voter = self.local_is_voter();
        let rpc_timeout = self.config.rpc_timeout;

        let (command_payload, has_command) = if inline_command {
            (command.clone(), true)
        } else {
            (Bytes::new(), false)
        };
        let mut ok = 0usize;
        let mut max_promised = ballot;

        if local_is_voter {
            let local = self
                .rpc_accept(AcceptRequest {
                    group_id: self.config.group_id,
                    txn_id,
                    ballot,
                    command: command_payload.clone(),
                    command_digest,
                    has_command,
                    seq,
                    deps: deps.clone(),
                })
                .await;
            max_promised = max_promised.max(local.promised);
            if !local.ok {
                return Ok(local);
            }
            ok = 1;
        }

        if ok >= quorum {
            return Ok(AcceptResponse {
                ok: true,
                promised: max_promised,
            });
        }

        let mut in_flight = FuturesUnordered::new();
        for peer in peers.iter().copied() {
            let transport = self.transport.clone();
            let req = AcceptRequest {
                group_id: self.config.group_id,
                txn_id,
                ballot,
                command: command_payload.clone(),
                command_digest,
                has_command,
                seq,
                deps: deps.clone(),
            };
            in_flight.push(async move {
                match time::timeout(rpc_timeout, transport.accept(peer, req)).await {
                    Ok(resp) => resp.map_err(|e| anyhow::anyhow!("accept rpc failed: {e}")),
                    Err(_) => Err(anyhow::anyhow!("accept rpc timed out")),
                }
            });
        }

        let deadline = time::Instant::now() + rpc_timeout;
        while ok < quorum {
            let remaining = deadline.saturating_duration_since(time::Instant::now());
            // Abort quorum wait when the round-level budget is exhausted.
            if remaining.is_zero() {
                break;
            }
            // Poll one completed RPC at a time; pending futures remain in-flight
            // and are dropped when quorum is satisfied. This does not try to
            // drain every response once a decision is already reached.
            let recv = time::timeout(remaining, in_flight.next()).await;
            let Ok(Some(resp)) = recv else {
                break;
            };
            if let Ok(r) = resp {
                max_promised = max_promised.max(r.promised);
                if r.ok {
                    ok += 1;
                    if ok >= quorum {
                        return Ok(AcceptResponse {
                            ok: true,
                            promised: max_promised,
                        });
                    }
                }
            }
        }

        if max_promised > ballot {
            return Ok(AcceptResponse {
                ok: false,
                promised: max_promised,
            });
        }

        Err(ProposeOnceError::NoQuorum(anyhow::anyhow!(
            "failed to reach quorum during Accept (ok={ok}, quorum={quorum})"
        )))
    }

    /// Execute one Commit round and stop as soon as voter quorum ACKs commit.
    ///
    /// Purpose:
    /// - Finalize a decided transaction and replicate commit status broadly.
    ///
    /// Design:
    /// - Apply local commit first when this node is a member.
    /// - Track quorum only from voter peers, while still sending commit RPCs to
    ///   all members.
    /// - Poll peer RPCs via `FuturesUnordered` and return immediately on quorum.
    ///
    /// Inputs:
    /// - Transaction identity, ballot, command payload/digest, and chosen
    ///   sequence/dependency metadata.
    /// - `inline_command`: whether Commit RPCs carry full command bytes.
    ///
    /// Outputs:
    /// - `Ok(CommitRoundTimings)` when voter quorum ACKs commit.
    /// - `Err(ProposeOnceError::NoQuorum)` when quorum cannot be reached in time.
    async fn run_commit_round(
        &self,
        txn_id: TxnId,
        ballot: Ballot,
        command: Bytes,
        command_digest: [u8; 32],
        seq: u64,
        deps: Vec<TxnId>,
        inline_command: bool,
    ) -> Result<CommitRoundTimings, ProposeOnceError> {
        let peers = self.peers_snapshot();
        let quorum = self.quorum();
        let local_is_member = self.local_is_member();
        let local_is_voter = self.local_is_voter();
        let voter_set = self.voters_snapshot().into_iter().collect::<HashSet<_>>();
        // Commit messages should eventually reach every replica. We don't block the coordinator on
        // slow replicas, but we allow more time for the RPC to complete to reduce the chance of
        // leaving long-lived PreAccepted records behind.
        let commit_timeout = self.config.propose_timeout;

        let (command_payload, has_command) = if inline_command {
            (command.clone(), true)
        } else {
            (Bytes::new(), false)
        };
        let mut ok = 0usize;
        let mut timings = CommitRoundTimings {
            local_state_update_us: 0,
            local_durable_wait_us: 0,
            local_log_queue_wait_us: 0,
            local_log_append_us: 0,
            local_post_durable_state_update_us: 0,
            remote_quorum_wait_us: 0,
        };
        if local_is_member {
            let (local, local_timings) = self
                .rpc_commit_with_timings(CommitRequest {
                    group_id: self.config.group_id,
                    txn_id,
                    ballot,
                    command: command_payload.clone(),
                    command_digest,
                    has_command,
                    seq,
                    deps: deps.clone(),
                })
                .await;
            if !local.ok {
                return Err(ProposeOnceError::NoQuorum(anyhow::anyhow!(
                    "local commit rejected"
                )));
            }
            timings.local_state_update_us = local_timings.state_update_us;
            timings.local_durable_wait_us = local_timings.durable_wait_us;
            timings.local_log_queue_wait_us = local_timings.durable_queue_wait_us;
            timings.local_log_append_us = local_timings.durable_append_us;
            timings.local_post_durable_state_update_us = local_timings.post_durable_state_update_us;
            if local_is_voter {
                ok = 1;
            }
        }

        if ok >= quorum {
            if local_is_member {
                self.metrics
                    .record_commit_local_state(Duration::from_micros(
                        timings.local_state_update_us,
                    ));
                self.metrics
                    .record_commit_local_durable(Duration::from_micros(
                        timings.local_durable_wait_us,
                    ));
                self.metrics
                    .record_commit_local_log_queue(Duration::from_micros(
                        timings.local_log_queue_wait_us,
                    ));
                self.metrics
                    .record_commit_local_log_append(Duration::from_micros(
                        timings.local_log_append_us,
                    ));
                self.metrics
                    .record_commit_local_post_durable_state(Duration::from_micros(
                        timings.local_post_durable_state_update_us,
                    ));
            }
            return Ok(timings);
        }

        let mut in_flight = FuturesUnordered::new();
        for peer in peers.iter().copied() {
            let transport = self.transport.clone();
            let count_for_quorum = voter_set.contains(&peer);
            let req = CommitRequest {
                group_id: self.config.group_id,
                txn_id,
                ballot,
                command: command_payload.clone(),
                command_digest,
                has_command,
                seq,
                deps: deps.clone(),
            };
            in_flight.push(async move {
                let rpc_start = time::Instant::now();
                let peer_ok = match time::timeout(commit_timeout, transport.commit(peer, req)).await
                {
                    Ok(res) => res.ok().is_some_and(|r| r.ok),
                    Err(_) => false,
                };
                let rpc_us = rpc_start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
                (peer, peer_ok, count_for_quorum, rpc_us)
            });
        }

        let remote_quorum_start = time::Instant::now();
        let deadline = time::Instant::now() + self.config.rpc_timeout;
        while ok < quorum {
            let remaining = deadline.saturating_duration_since(time::Instant::now());
            // Abort quorum wait when the round-level budget is exhausted.
            if remaining.is_zero() {
                break;
            }
            // Poll one completed RPC at a time; pending futures remain in-flight
            // and are dropped once commit quorum is satisfied. This does not
            // wait for non-quorum followers after the decision point.
            let recv = time::timeout(remaining, in_flight.next()).await;
            let Ok(Some((peer, peer_ok, count_for_quorum, _rpc_us))) = recv else {
                break;
            };
            if peer_ok && count_for_quorum {
                ok += 1;
                if ok >= quorum {
                    timings.remote_quorum_wait_us = remote_quorum_start
                        .elapsed()
                        .as_micros()
                        .min(u128::from(u64::MAX))
                        as u64;
                    if local_is_member {
                        self.metrics
                            .record_commit_local_state(Duration::from_micros(
                                timings.local_state_update_us,
                            ));
                        self.metrics
                            .record_commit_local_durable(Duration::from_micros(
                                timings.local_durable_wait_us,
                            ));
                        self.metrics
                            .record_commit_local_log_queue(Duration::from_micros(
                                timings.local_log_queue_wait_us,
                            ));
                        self.metrics
                            .record_commit_local_log_append(Duration::from_micros(
                                timings.local_log_append_us,
                            ));
                        self.metrics
                            .record_commit_local_post_durable_state(Duration::from_micros(
                                timings.local_post_durable_state_update_us,
                            ));
                    }
                    self.metrics
                        .record_commit_remote_quorum(Duration::from_micros(
                            timings.remote_quorum_wait_us,
                        ));
                    self.metrics.record_commit_quorum_closer(
                        peer,
                        Duration::from_micros(timings.remote_quorum_wait_us),
                    );
                    if !in_flight.is_empty() {
                        let metrics = self.metrics.clone();
                        let tail_start = time::Instant::now();
                        tokio::spawn(async move {
                            while in_flight.next().await.is_some() {}
                            metrics.record_commit_tail(tail_start.elapsed());
                        });
                    }
                    return Ok(timings);
                }
            }
        }

        Err(ProposeOnceError::NoQuorum(anyhow::anyhow!(
            "failed to reach quorum during Commit (ok={ok}, quorum={quorum})"
        )))
    }

    async fn execute_until(&self, target: TxnId) -> anyhow::Result<()> {
        let start = time::Instant::now();
        let deadline = time::Instant::now() + self.config.propose_timeout;
        let mut stall_logged = false;
        const STALL_LOG_THRESHOLD: Duration = Duration::from_millis(200);
        const RECOVERY_GRACE: Duration = Duration::from_millis(200);
        let preaccept_stall_threshold = self.config.preaccept_stall_hits.max(1);
        loop {
            if time::Instant::now() > deadline {
                let mut state = self.state.lock().await;
                state.execute_timeouts = state.execute_timeouts.saturating_add(1);
                let mut status_none = 0usize;
                let mut status_preaccepted = 0usize;
                let mut status_accepted = 0usize;
                let mut status_committed = 0usize;
                let mut status_executing = 0usize;
                let mut status_executed = 0usize;
                for rec in state.records.values() {
                    match rec.status {
                        Status::None => status_none += 1,
                        Status::PreAccepted => status_preaccepted += 1,
                        Status::Accepted => status_accepted += 1,
                        Status::Committed => status_committed += 1,
                        Status::Executing => status_executing += 1,
                        Status::Executed => status_executed += 1,
                    }
                }
                let (root, chain) = build_blocking_chain(&state);
                let blocking_dep = first_blocking_dep(&chain);
                tracing::warn!(
                    txn_id = ?target,
                    root = ?root,
                    chain = ?chain,
                    blocking_dep = ?blocking_dep,
                    records_len = state.records.len(),
                    committed_queue_len = state.committed_queue.len(),
                    recovering_len = state.recovering.len(),
                    read_waiters_len = state.read_waiters.len(),
                    executed_out_of_order_len = state.executed_out_of_order.len(),
                    executed_log_len = state.executed_log.len(),
                    executed_log_bytes = state.executed_log_bytes,
                    executed_log_deps_total = state.executed_log_deps_total,
                    status_none = status_none,
                    status_preaccepted = status_preaccepted,
                    status_accepted = status_accepted,
                    status_committed = status_committed,
                    status_executing = status_executing,
                    status_executed = status_executed,
                    "execute timed out"
                );
                drop(state);
                if let Some((dep, dep_status, dep_missing)) = blocking_dep {
                    let mut attempt_err: Option<anyhow::Error> = None;
                    let attempted = true;
                    if let Err(err) = self.recover_txn(dep).await {
                        attempt_err = Some(err);
                    }
                    let (dep_status_after, dep_in_committed) = {
                        let state = self.state.lock().await;
                        let dep_status_after = state.records.get(&dep).map(|r| r.status);
                        let dep_in_committed =
                            state.committed_queue.iter().any(|(_, id)| *id == dep);
                        (dep_status_after, dep_in_committed)
                    };
                    tracing::warn!(
                        txn_id = ?target,
                        blocking_dep = ?dep,
                        blocking_status = ?dep_status,
                        blocking_missing = dep_missing,
                        recovery_attempted = attempted,
                        recovery_error = ?attempt_err,
                        dep_status_after = ?dep_status_after,
                        dep_in_committed = dep_in_committed,
                        "execute timeout recovery attempt"
                    );
                }
                anyhow::bail!("execute timed out for txn {:?}", target);
            }

            if self.is_executed(target).await {
                return Ok(());
            }

            let progress = self.execute_progress().await?;
            if self.is_executed(target).await {
                return Ok(());
            }

            if !stall_logged && start.elapsed() >= STALL_LOG_THRESHOLD {
                let mut state = self.state.lock().await;
                let chain = build_blocking_chain_from(&state, target, 16);
                let chain_len = chain.len();
                let blocking = first_blocking_dep(&chain);
                let mut recover_target: Option<TxnId> = None;
                if let Some((dep, status, missing)) = blocking {
                    let should_consider =
                        matches!(status, Some(Status::PreAccepted | Status::Accepted));
                    if !missing && should_consider {
                        let count = {
                            let entry = state.stalled_preaccept_counts.entry(dep).or_insert(0);
                            *entry = entry.saturating_add(1);
                            *entry
                        };
                        if count >= preaccept_stall_threshold
                            && self.record_recovery_attempt(&mut state, dep, time::Instant::now())
                        {
                            state.stalled_preaccept_counts.insert(dep, 0);
                            recover_target = Some(dep);
                        }
                    }
                }
                let mut status_none = 0usize;
                let mut status_preaccepted = 0usize;
                let mut status_accepted = 0usize;
                let mut status_committed = 0usize;
                let mut status_executing = 0usize;
                let mut status_executed = 0usize;
                for rec in state.records.values() {
                    match rec.status {
                        Status::None => status_none += 1,
                        Status::PreAccepted => status_preaccepted += 1,
                        Status::Accepted => status_accepted += 1,
                        Status::Committed => status_committed += 1,
                        Status::Executing => status_executing += 1,
                        Status::Executed => status_executed += 1,
                    }
                }
                tracing::warn!(
                    txn_id = ?target,
                    elapsed_ms = start.elapsed().as_millis(),
                    progressed = progress,
                    chain_len = chain_len,
                    blocking_dep = ?blocking.map(|b| b.0),
                    blocking_status = ?blocking.and_then(|b| b.1),
                    blocking_missing = blocking.map(|b| b.2).unwrap_or(false),
                    records_len = state.records.len(),
                    committed_queue_len = state.committed_queue.len(),
                    recovering_len = state.recovering.len(),
                    read_waiters_len = state.read_waiters.len(),
                    executed_out_of_order_len = state.executed_out_of_order.len(),
                    executed_log_len = state.executed_log.len(),
                    executed_log_bytes = state.executed_log_bytes,
                    executed_log_deps_total = state.executed_log_deps_total,
                    status_none = status_none,
                    status_preaccepted = status_preaccepted,
                    status_accepted = status_accepted,
                    status_committed = status_committed,
                    status_executing = status_executing,
                    status_executed = status_executed,
                    "execute stall"
                );
                stall_logged = true;
                drop(state);
                if let Some(dep) = recover_target {
                    let attempt = self.recover_txn(dep).await;
                    tracing::warn!(
                        txn_id = ?target,
                        blocking_dep = ?dep,
                        recovery_ok = attempt.is_ok(),
                        recovery_error = attempt.err().map(|e| e.to_string()),
                        "execute stall preaccept recovery"
                    );
                }
            }

            if !progress {
                if start.elapsed() < RECOVERY_GRACE {
                    let _ =
                        time::timeout(Duration::from_millis(1), self.executor_notify.notified())
                            .await;
                    continue;
                }

                if let Some(dep) = self.find_recovery_target(target).await {
                    if self.should_recover(dep, start).await {
                        let _ = self.recover_txn(dep).await?;
                        continue;
                    }
                }

                let _ =
                    time::timeout(Duration::from_millis(1), self.executor_notify.notified()).await;
            }
        }
    }

    /// Returns `true` when dependency waiting has exceeded configured recovery delay.
    ///
    /// Input:
    /// - `txn_id`: blocked transaction candidate.
    /// - `waiting_since`: when this executor wait cycle began.
    ///
    /// Output:
    /// - `true` when recovery should be attempted now.
    /// - `false` when transaction is already executed or still within grace window.
    async fn should_recover(&self, txn_id: TxnId, waiting_since: time::Instant) -> bool {
        let now = time::Instant::now();
        let recovery_delay = self.recovery_delay();
        if now.duration_since(waiting_since) < recovery_delay {
            return false;
        }

        let mut state = self.state.lock().await;
        if state.is_executed(&txn_id) {
            return false;
        }

        let Some(rec) = state.records.get(&txn_id) else {
            return self.record_recovery_attempt(&mut state, txn_id, now);
        };

        if now.duration_since(rec.updated_at) < recovery_delay {
            return false;
        }

        self.record_recovery_attempt(&mut state, txn_id, now)
    }

    async fn is_executed(&self, txn_id: TxnId) -> bool {
        let state = self.state.lock().await;
        state.is_executed(&txn_id)
            || state
                .records
                .get(&txn_id)
                .is_some_and(|r| r.status == Status::Executed)
    }

    async fn executor_recover_once(&self) -> anyhow::Result<bool> {
        let start = time::Instant::now();
        let res = self.executor_recover_once_inner().await;
        self.metrics
            .record_exec_recover(start.elapsed(), res.as_ref().ok().copied());
        res
    }

    async fn executor_recover_once_inner(&self) -> anyhow::Result<bool> {
        if let Some(target) = self.pick_stall_recovery_target().await {
            if self.should_attempt_stall_recover() {
                self.recover_txn(target)
                    .await
                    .with_context(|| format!("stall recover txn {target:?}"))?;
                return Ok(true);
            }
        }

        let dep = {
            let state = self.state.lock().await;
            let Some(candidate) = state.committed_queue.iter().next().map(|(_, id)| *id) else {
                return Ok(false);
            };
            let Some(rec) = state.records.get(&candidate) else {
                return Ok(false);
            };
            let blocked = rec.deps.iter().any(|dep| !state.is_executed(dep));
            if !blocked {
                return Ok(false);
            }
            find_recovery_target_deep(&state, candidate, 100_000)
        };

        let Some(dep) = dep else {
            return Ok(false);
        };

        if !self.should_recover_due_to_stall(dep).await {
            return Ok(false);
        }

        self.recover_txn(dep)
            .await
            .with_context(|| format!("recover txn {dep:?}"))?;
        Ok(true)
    }

    fn should_log_exec_stall(&self) -> bool {
        const STALL_LOG_INTERVAL_US: u64 = 5_000_000;
        let now_us = self.start_at.elapsed().as_micros() as u64;
        let last = self.metrics.exec_stall_log_at_us.load(Ordering::Relaxed);
        if now_us.saturating_sub(last) < STALL_LOG_INTERVAL_US {
            return false;
        }
        self.metrics
            .exec_stall_log_at_us
            .store(now_us, Ordering::Relaxed);
        true
    }

    /// Rate-limits executor stall recovery probes.
    ///
    /// Design: use a monotonic timestamp gate so only one recovery probe is
    /// attempted within the configured window, preventing feedback loops when
    /// many workers detect the same stall.
    fn should_attempt_stall_recover(&self) -> bool {
        let recover_interval_us = self
            .config
            .stall_recover_interval
            .as_micros()
            .min(u128::from(u64::MAX)) as u64;
        // Guard against zero/too-small configured durations.
        let recover_interval_us = recover_interval_us.max(1_000);
        let now_us = self.start_at.elapsed().as_micros() as u64;
        let last = self
            .metrics
            .exec_stall_recover_at_us
            .load(Ordering::Relaxed);
        if now_us.saturating_sub(last) < recover_interval_us {
            return false;
        }
        self.metrics
            .exec_stall_recover_at_us
            .store(now_us, Ordering::Relaxed);
        true
    }

    async fn pick_stall_recovery_target(&self) -> Option<TxnId> {
        let state = self.state.lock().await;
        if state.committed_queue.is_empty() {
            return None;
        }
        let (_root, chain) = build_blocking_chain(&state);
        pick_recovery_from_chain(&chain)
    }

    /// Returns `true` when an executor-detected stall is old enough to recover.
    ///
    /// Input:
    /// - `txn_id`: recovery target picked from dependency-chain analysis.
    ///
    /// Output:
    /// - `true` when transaction has not advanced recently and recovery should run.
    async fn should_recover_due_to_stall(&self, txn_id: TxnId) -> bool {
        let now = time::Instant::now();
        let recovery_delay = self.recovery_delay();
        let mut state = self.state.lock().await;

        if state.is_executed(&txn_id) {
            return false;
        }

        let Some(rec) = state.records.get(&txn_id) else {
            return self.record_recovery_attempt(&mut state, txn_id, now);
        };

        if now.duration_since(rec.updated_at) < recovery_delay {
            return false;
        }

        self.record_recovery_attempt(&mut state, txn_id, now)
    }

    /// Computes the effective recovery delay used by both recovery triggers.
    ///
    /// Design: clamp to a floor so tiny RPC timeouts do not cause recovery
    /// thrash, while still honoring explicit operator tuning.
    fn recovery_delay(&self) -> Duration {
        self.config
            .rpc_timeout
            .min(Duration::from_millis(200))
            .max(self.config.recovery_min_delay)
    }

    async fn execute_progress(&self) -> anyhow::Result<bool> {
        let start = time::Instant::now();
        let res = self.execute_progress_inner().await;
        self.metrics
            .record_exec_progress(start.elapsed(), res.as_ref().ok().copied());
        res
    }

    async fn restore_apply_items_for_retry(&self, items: &[ApplyItem]) {
        let mut state = self.state.lock().await;
        for item in items {
            let Some(rec) = state.records.get_mut(&item.id) else {
                continue;
            };
            if rec.status != Status::Executing {
                continue;
            }
            rec.status = Status::Committed;
            rec.updated_at = time::Instant::now();
            state.insert_committed(item.id, item.seq);
        }
        self.executor_notify.notify_one();
    }

    async fn execute_progress_inner(&self) -> anyhow::Result<bool> {
        let _guard = self.execute_lock.lock().await;

        let mut to_apply = Vec::<ApplyItem>::new();
        let mut picked: Vec<TxnId> = Vec::new();
        let mut candidates: Vec<TxnId> = Vec::new();
        let mut expanded_candidates: Option<Vec<TxnId>> = None;
        let mut snapshot: Option<ExecSnapshot> = None;
        let exec_batch_max = self.config.execute_batch_max.max(1);
        let mut used_ready = false;
        {
            let mut state = self.state.lock().await;
            if state.committed_queue.is_empty() {
                return Ok(false);
            }

            // Fast-path: consume ready-to-execute commits without scanning.
            if !state.committed_ready.is_empty() {
                let ready_ids: Vec<TxnId> = state
                    .committed_ready
                    .iter()
                    .take(exec_batch_max)
                    .map(|(_, id)| *id)
                    .collect();
                for id in &ready_ids {
                    let Some(rec) = state.records.get_mut(id) else {
                        continue;
                    };
                    if rec.status != Status::Committed {
                        continue;
                    }

                    let (cmd, keys, seq) = {
                        rec.status = Status::Executing;
                        rec.updated_at = time::Instant::now();
                        let cmd = rec
                            .command
                            .clone()
                            .context("missing command bytes for committed txn")?;
                        let keys = rec.keys.clone().unwrap_or_default();
                        (cmd, keys, rec.seq)
                    };

                    state.remove_committed(*id, seq);
                    to_apply.push(ApplyItem {
                        id: *id,
                        command: cmd,
                        keys,
                        seq: seq.max(1),
                    });
                }
                if !to_apply.is_empty() {
                    picked = ready_ids;
                    used_ready = true;
                }
            }

            if !used_ready {
                // Slow-path: scan committed queue and fall back to SCC resolution for cycles.
                let deps_ready = |st: &State, id: TxnId| -> bool {
                    let Some(rec) = st.records.get(&id) else {
                        return false;
                    };
                    rec.deps.iter().all(|dep| st.is_executed(dep))
                };

                let scan_limit = exec_batch_max.max(32);
                for (_, id) in state.committed_queue.iter().take(scan_limit) {
                    let Some(rec) = state.records.get(id) else {
                        continue;
                    };
                    if rec.status != Status::Committed {
                        continue;
                    }
                    if deps_ready(&state, *id) {
                        picked.push(*id);
                        if picked.len() >= exec_batch_max {
                            break;
                        }
                    }
                }

                if picked.is_empty() {
                    const WINDOW_BASE: usize = 256;
                    const WINDOW_MAX: usize = 32_768;
                    let max_frontier_len = state
                        .frontier_by_key
                        .values()
                        .map(|v| v.len())
                        .max()
                        .unwrap_or(0);
                    let committed_len = state.committed_queue.len();
                    let mut window = WINDOW_BASE.max((max_frontier_len * 2).min(WINDOW_MAX));
                    window = window.min(committed_len.max(WINDOW_BASE));

                    candidates = state
                        .committed_queue
                        .iter()
                        .take(window)
                        .map(|(_, id)| *id)
                        .collect();

                    if window < committed_len {
                        let expanded = WINDOW_MAX.min(committed_len);
                        if expanded > window {
                            expanded_candidates = Some(
                                state
                                    .committed_queue
                                    .iter()
                                    .take(expanded)
                                    .map(|(_, id)| *id)
                                    .collect(),
                            );
                        }
                    }

                    let all_candidates = expanded_candidates.as_ref().unwrap_or(&candidates);
                    let mut deps = HashMap::with_capacity(all_candidates.len());
                    let mut status = HashMap::with_capacity(all_candidates.len());
                    let mut seq = HashMap::with_capacity(all_candidates.len());
                    for id in all_candidates {
                        if let Some(rec) = state.records.get(id) {
                            status.insert(*id, rec.status);
                            seq.insert(*id, rec.seq);
                            deps.insert(*id, rec.deps.iter().copied().collect());
                        }
                    }
                    snapshot = Some(ExecSnapshot {
                        deps,
                        status,
                        seq,
                        executed_prefix_by_node: state
                            .executed_prefix_by_node
                            .iter()
                            .map(|(k, v)| (*k, *v))
                            .collect(),
                        executed_out_of_order: state
                            .executed_out_of_order
                            .iter()
                            .copied()
                            .collect(),
                    });
                }

                for id in &picked {
                    let Some(rec) = state.records.get_mut(id) else {
                        continue;
                    };
                    if rec.status != Status::Committed {
                        continue;
                    }

                    let (cmd, keys, seq) = {
                        rec.status = Status::Executing;
                        rec.updated_at = time::Instant::now();
                        let cmd = rec
                            .command
                            .clone()
                            .context("missing command bytes for committed txn")?;
                        let keys = rec.keys.clone().unwrap_or_default();
                        (cmd, keys, rec.seq)
                    };

                    state.remove_committed(*id, seq);
                    to_apply.push(ApplyItem {
                        id: *id,
                        command: cmd,
                        keys,
                        seq: seq.max(1),
                    });
                }
            }
        }

        if picked.is_empty() {
            if let Some(snapshot) = snapshot.as_ref() {
                picked = pick_ready_scc(snapshot, &candidates);
                if picked.len() > exec_batch_max {
                    picked.truncate(exec_batch_max);
                }
                if picked.is_empty() {
                    if let Some(expanded) = expanded_candidates.as_ref() {
                        picked = pick_ready_scc(snapshot, expanded);
                        if picked.len() > exec_batch_max {
                            picked.truncate(exec_batch_max);
                        }
                    }
                }
            }

            if picked.is_empty() {
                if self.should_log_exec_stall() {
                    let state = self.state.lock().await;
                    let (root, chain) = build_blocking_chain(&state);
                    tracing::warn!(
                        root = ?root,
                        chain = ?chain,
                        committed_queue_len = state.committed_queue.len(),
                        executed_prefix_by_node = ?state.executed_prefix_by_node,
                        executed_out_of_order_len = state.executed_out_of_order.len(),
                        "execute stalled (no ready committed txn)"
                    );
                }
                return Ok(false);
            }

            let picked_set: HashSet<TxnId> = picked.iter().copied().collect();
            let deps_ready = |st: &State, id: TxnId| -> bool {
                let Some(rec) = st.records.get(&id) else {
                    return false;
                };
                rec.deps
                    .iter()
                    .all(|dep| st.is_executed(dep) || picked_set.contains(dep))
            };

            let mut state = self.state.lock().await;
            let all_ready = picked.iter().all(|id| {
                state
                    .records
                    .get(id)
                    .is_some_and(|r| r.status == Status::Committed)
                    && deps_ready(&state, *id)
            });
            if !all_ready {
                if self.should_log_exec_stall() {
                    let (root, chain) = build_blocking_chain(&state);
                    tracing::warn!(
                        root = ?root,
                        chain = ?chain,
                        committed_queue_len = state.committed_queue.len(),
                        executed_prefix_by_node = ?state.executed_prefix_by_node,
                        executed_out_of_order_len = state.executed_out_of_order.len(),
                        "execute stalled (no applicable committed txn)"
                    );
                }
                return Ok(false);
            }

            for id in &picked {
                let Some(rec) = state.records.get_mut(id) else {
                    continue;
                };
                if rec.status != Status::Committed {
                    continue;
                }

                let (cmd, keys, seq) = {
                    rec.status = Status::Executing;
                    rec.updated_at = time::Instant::now();
                    let cmd = rec
                        .command
                        .clone()
                        .context("missing command bytes for committed txn")?;
                    let keys = rec.keys.clone().unwrap_or_default();
                    (cmd, keys, rec.seq)
                };

                state.remove_committed(*id, seq);
                to_apply.push(ApplyItem {
                    id: *id,
                    command: cmd,
                    keys,
                    seq: seq.max(1),
                });
            }
        }

        if to_apply.is_empty() {
            if self.should_log_exec_stall() {
                let state = self.state.lock().await;
                let (root, chain) = build_blocking_chain(&state);
                tracing::warn!(
                    root = ?root,
                    chain = ?chain,
                    committed_queue_len = state.committed_queue.len(),
                    executed_prefix_by_node = ?state.executed_prefix_by_node,
                    executed_out_of_order_len = state.executed_out_of_order.len(),
                    "execute stalled (no applicable committed txn)"
                );
            }
            return Ok(false);
        }

        let mut write_batch = Vec::new();
        for item in &to_apply {
            if item.keys.is_write() {
                write_batch.push((
                    item.command.clone(),
                    ExecMeta {
                        seq: item.seq,
                        txn_id: item.id,
                    },
                ));
            }
        }

        if !write_batch.is_empty() {
            let write_batch_len = write_batch.len();
            let apply_inline = |batch: &[(Bytes, ExecMeta)]| -> ApplyResult {
                let apply_start = time::Instant::now();
                let result = self.sm.apply_batch(batch);
                let apply_us = apply_start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
                ApplyResult {
                    apply_us,
                    visible_us: 0,
                    result,
                }
            };

            let result = if let Some(tx) = &self.apply_tx {
                let (resp_tx, resp_rx) = oneshot::channel();
                let work = ApplyWork {
                    batch: write_batch,
                    tx: resp_tx,
                };
                match tx.send(work) {
                    Ok(()) => match resp_rx.await {
                        Ok(res) => res,
                        Err(_) => {
                            tracing::warn!("apply worker response dropped");
                            ApplyResult {
                                apply_us: 0,
                                visible_us: 0,
                                result: Err(anyhow::anyhow!("apply worker response dropped")),
                            }
                        }
                    },
                    Err(err) => apply_inline(&err.0.batch),
                }
            } else {
                apply_inline(&write_batch)
            };

            let apply_dur = Duration::from_micros(result.apply_us);
            let visible_dur = Duration::from_micros(result.visible_us);
            self.metrics.record_apply_batch(apply_dur);
            self.metrics.record_mark_visible(visible_dur);
            for _ in 0..write_batch_len {
                self.metrics.record_apply_write(apply_dur);
            }
            if let Err(err) = result.result {
                self.restore_apply_items_for_retry(&to_apply).await;
                return Err(err.context("state machine apply failed"));
            }
        }

        // Some commands intentionally have no read/write keys (for example,
        // control commands that mutate runtime metadata in the state machine).
        // These commands still need an `apply` callback when they reach execute.
        for item in &to_apply {
            if item.keys.is_write() || !item.keys.reads.is_empty() {
                continue;
            }
            let meta = ExecMeta {
                seq: item.seq,
                txn_id: item.id,
            };
            let start = time::Instant::now();
            if let Err(err) = self.sm.apply(&item.command, meta) {
                self.metrics.record_apply_write(start.elapsed());
                self.restore_apply_items_for_retry(&to_apply).await;
                return Err(err.context("state machine apply failed"));
            }
            self.metrics.record_apply_write(start.elapsed());
        }

        for item in &to_apply {
            let meta = ExecMeta {
                seq: item.seq,
                txn_id: item.id,
            };
            if item.keys.is_write() {
                continue;
            }

            if item.keys.reads.is_empty() {
                continue;
            }

            let tx = {
                let mut state = self.state.lock().await;
                state.read_waiters.remove(&item.id)
            };

            if let Some(tx) = tx {
                let start = time::Instant::now();
                let v = self.sm.read(&item.command, meta);
                self.metrics.record_apply_read(start.elapsed());
                let _ = tx.send(v);
            }
        }

        if let Some(log) = &self.commit_log {
            let applied = self
                .compact_counter
                .fetch_add(to_apply.len() as u64, Ordering::Relaxed);
            for item in &to_apply {
                if let Err(err) = log.mark_executed(item.id) {
                    tracing::warn!(error = ?err, txn_id = ?item.id, "commit log mark-executed failed");
                }
            }
            if applied + (to_apply.len() as u64) >= COMPACT_EVERY_APPLIED {
                self.compact_counter.store(0, Ordering::Relaxed);
                if let Err(err) = log.compact(COMPACT_MAX_DELETE) {
                    tracing::warn!(error = ?err, "commit log compaction failed");
                }
            }
        }

        let state_start = time::Instant::now();
        let mut state = self.state.lock().await;
        for item in &to_apply {
            let Some(mut rec) = state.records.remove(&item.id) else {
                continue;
            };
            if let Some(keys) = rec.keys.take() {
                state.remove_from_index(item.id, &keys);
            }
            if item.keys.is_write() {
                for key in &item.keys.writes {
                    state.last_write_by_key.insert(key.clone(), item.id);
                }
                // Preserve executed command bytes for recovery/fetch. If the
                // in-memory record lost command bytes, attempt commit-log load
                // before recording executed metadata.
                let command = rec
                    .command
                    .take()
                    .or_else(|| self.load_command_from_commit_log(item.id));
                if let Some(command) = command {
                    let digest = rec
                        .command_digest
                        .unwrap_or_else(|| command_digest(&command));
                    state.record_executed_value(
                        item.id,
                        ExecutedLogEntry {
                            command: Some(command),
                            command_digest: Some(digest),
                            seq: rec.seq.max(1),
                            deps: rec.deps.into_iter().collect(),
                        },
                    );
                }
            }
            state.mark_executed_and_wake(item.id);
            state.read_waiters.remove(&item.id);
            state.recovery_last_attempt.remove(&item.id);
            state.recovery_attempts_by_txn.remove(&item.id);
            state.stalled_preaccept_counts.remove(&item.id);
        }
        self.metrics.record_state_update(state_start.elapsed());

        Ok(true)
    }

    async fn find_recovery_target(&self, txn_id: TxnId) -> Option<TxnId> {
        let state = self.state.lock().await;
        let rec = state.records.get(&txn_id)?;
        for dep in &rec.deps {
            if state.is_executed(dep) {
                continue;
            }
            let Some(dep_rec) = state.records.get(dep) else {
                return Some(*dep);
            };
            if dep_rec.status < Status::Committed {
                return Some(*dep);
            }
        }
        None
    }

    async fn recover_txn(&self, txn_id: TxnId) -> anyhow::Result<()> {
        let start = time::Instant::now();
        let attempt = {
            let mut state = self.state.lock().await;
            if state.is_executed(&txn_id) {
                return Ok(());
            }
            if !state.recovering.insert(txn_id) {
                return Ok(());
            }
            state.recovery_attempts = state.recovery_attempts.saturating_add(1);
            let entry = state.recovery_attempts_by_txn.entry(txn_id).or_insert(0);
            *entry = entry.saturating_add(1);
            *entry
        };
        let result = self.recover_txn_inner(txn_id).await;

        let elapsed_ms = start.elapsed().as_millis() as u64;
        let err_str = result.as_ref().err().map(|e| e.to_string());
        let is_noop = matches!(result.as_ref().ok(), Some(RecoveryKind::Noop));
        if let Ok(kind) = result.as_ref() {
            tracing::info!(
                txn_id = ?txn_id,
                attempt = attempt,
                elapsed_ms = elapsed_ms,
                noop = matches!(kind, RecoveryKind::Noop),
                "recovery result"
            );
        } else if let Some(err) = err_str.as_ref() {
            tracing::warn!(
                txn_id = ?txn_id,
                attempt = attempt,
                elapsed_ms = elapsed_ms,
                timeout = err.contains("recovery timed out"),
                error = %err,
                "recovery result"
            );
        }
        let mut state = self.state.lock().await;
        state.recovering.remove(&txn_id);
        state.recovery_last_ms = elapsed_ms;
        match err_str {
            None => {
                state.recovery_successes = state.recovery_successes.saturating_add(1);
                if is_noop {
                    state.recovery_noops = state.recovery_noops.saturating_add(1);
                }
            }
            Some(msg) => {
                state.recovery_failures = state.recovery_failures.saturating_add(1);
                if msg.contains("recovery timed out") {
                    state.recovery_timeouts = state.recovery_timeouts.saturating_add(1);
                }
            }
        }
        result.map(|_| ())
    }

    fn record_recovery_attempt(
        &self,
        state: &mut State,
        txn_id: TxnId,
        now: time::Instant,
    ) -> bool {
        const RECOVERY_MIN_INTERVAL: Duration = Duration::from_secs(1);
        if let Some(last) = state.recovery_last_attempt.get(&txn_id) {
            if now.duration_since(*last) < RECOVERY_MIN_INTERVAL {
                return false;
            }
        }
        state.recovery_last_attempt.insert(txn_id, now);
        true
    }

    async fn recover_txn_inner(&self, txn_id: TxnId) -> anyhow::Result<RecoveryKind> {
        let peers = self.voter_peers_snapshot();
        let quorum = self.quorum();
        let local_is_voter = self.local_is_voter();
        let rpc_timeout = self.config.rpc_timeout;

        let mut ballot = self.next_ballot_after(Ballot::zero()).await;
        let deadline = time::Instant::now() + self.config.propose_timeout;

        loop {
            if time::Instant::now() > deadline {
                anyhow::bail!("recovery timed out for txn {:?}", txn_id);
            }

            let mut replies = Vec::new();
            let mut ok = 0usize;
            let mut max_promised = ballot;
            if local_is_voter {
                let local = self
                    .rpc_recover(RecoverRequest {
                        group_id: self.config.group_id,
                        txn_id,
                        ballot,
                    })
                    .await;

                max_promised = max_promised.max(local.promised);
                if !local.ok {
                    ballot = self.next_ballot_after(local.promised).await;
                    continue;
                }
                replies.push(local);
                ok = 1;
            }

            let (tx, mut rx) = mpsc::channel::<anyhow::Result<RecoverResponse>>(peers.len().max(1));
            for peer in peers.iter().copied() {
                let transport = self.transport.clone();
                let tx = tx.clone();
                let req = RecoverRequest {
                    group_id: self.config.group_id,
                    txn_id,
                    ballot,
                };
                tokio::spawn(async move {
                    let resp = match time::timeout(rpc_timeout, transport.recover(peer, req)).await
                    {
                        Ok(resp) => resp.map_err(|e| anyhow::anyhow!("recover rpc failed: {e}")),
                        Err(_) => Err(anyhow::anyhow!("recover rpc timed out")),
                    };
                    let _ = tx.send(resp).await;
                    Ok::<(), anyhow::Error>(())
                });
            }
            drop(tx);

            let deadline = time::Instant::now() + self.config.rpc_timeout;
            while ok < quorum {
                let remaining = deadline.saturating_duration_since(time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let recv = time::timeout(remaining, rx.recv()).await;
                let Ok(Some(resp)) = recv else {
                    break;
                };
                match resp {
                    Ok(r) => {
                        max_promised = max_promised.max(r.promised);
                        if r.ok {
                            replies.push(r);
                            ok += 1;
                            if ok >= quorum {
                                break;
                            }
                        }
                    }
                    Err(_) => {}
                }
            }

            if ok < quorum {
                anyhow::bail!("recovery failed to reach quorum (ok={ok}, quorum={quorum})");
            }

            let chosen = match choose_recovery_value(&replies)? {
                RecoveryChoice::Ready(value) => value,
                RecoveryChoice::MissingCommittedCommand { digest } => {
                    // A quorum reported committed status with a stable digest
                    // but omitted bytes. Try direct fetch before failing this
                    // recovery loop iteration.
                    if let Some(command) = self.fetch_command_from_peers(txn_id, digest).await? {
                        RecoveryValue {
                            command,
                            seq: replies.iter().map(|r| r.seq).max().unwrap_or(1).max(1),
                            deps: replies
                                .iter()
                                .flat_map(|r| r.deps.iter().copied())
                                .collect::<BTreeSet<_>>()
                                .into_iter()
                                .collect(),
                        }
                    } else {
                        anyhow::bail!(
                            "recovery saw committed txn digest but peer fetch returned no command"
                        );
                    }
                }
            };
            let is_noop = chosen.command.is_empty();
            if is_noop {
                tracing::debug!(txn_id = ?txn_id, "recovery committing noop");
            }

            let digest = command_digest(&chosen.command);
            let accept = self
                .run_accept_round(
                    txn_id,
                    ballot,
                    chosen.command.clone(),
                    digest,
                    chosen.seq,
                    chosen.deps.clone(),
                    true,
                )
                .await
                .map_err(|e| match e {
                    ProposeOnceError::Rejected { promised } => {
                        anyhow::anyhow!("accept rejected with promised ballot {:?}", promised)
                    }
                    ProposeOnceError::NoQuorum(err) => err,
                })?;
            if !accept.ok {
                ballot = self
                    .next_ballot_after(accept.promised.max(max_promised))
                    .await;
                continue;
            }

            self.run_commit_round(
                txn_id,
                ballot,
                chosen.command,
                digest,
                chosen.seq,
                chosen.deps,
                true,
            )
            .await
            .map_err(|e| match e {
                ProposeOnceError::Rejected { promised } => {
                    anyhow::anyhow!("commit rejected with promised ballot {:?}", promised)
                }
                ProposeOnceError::NoQuorum(err) => err,
            })?;
            return Ok(if is_noop {
                RecoveryKind::Noop
            } else {
                RecoveryKind::Value
            });
        }
    }

    async fn next_ballot_after(&self, after: Ballot) -> Ballot {
        let mut state = self.state.lock().await;
        state.next_ballot_counter = state.next_ballot_counter.max(after.counter);
        state.next_ballot_counter += 1;
        Ballot {
            counter: state.next_ballot_counter,
            node_id: self.config.node_id,
        }
    }
}

/// Return the strongest fast-path write a read barrier must observe for a key.
///
/// Purpose:
/// - Make quorum/all-peer read barriers safe for 1RTT writes by exposing
///   PreAccepted/Accepted writes that may already have been ACKed by their
///   coordinator but not yet received asynchronous Commit dissemination here.
///
/// Design:
/// - Starts with the normal committed-key hint and scans the key frontier for
///   non-final write records.
/// - Chooses the highest sequence so callers can wait on one per-key target;
///   Accord dependencies on that target carry earlier writes in the same chain.
///
/// Inputs:
/// - `state`: locked group state.
/// - `key`: storage key being read.
/// - `best`: existing committed hint, if any.
///
/// Outputs:
/// - Highest-sequence barrier target for the key.
fn fast_path_barrier_write_for_key(
    state: &State,
    key: &[u8],
    mut best: Option<(TxnId, u64)>,
) -> Option<(TxnId, u64)> {
    let Some(frontier) = state.frontier_by_key.get(key) else {
        return best;
    };

    for txn_id in frontier {
        let Some(rec) = state.records.get(txn_id) else {
            continue;
        };
        if rec.status < Status::PreAccepted {
            continue;
        }
        if !rec.keys.as_ref().is_some_and(|keys| keys.is_write()) {
            continue;
        }
        let seq = rec.seq.max(1);
        let replace = match best {
            None => true,
            Some((_, cur_seq)) => seq > cur_seq,
        };
        if replace {
            best = Some((*txn_id, seq));
        }
    }

    best
}

fn build_blocking_chain(state: &State) -> (Option<TxnId>, Vec<BlockedStep>) {
    const MAX_CHAIN: usize = 8;
    let root = state
        .committed_queue
        .iter()
        .find_map(|(_, id)| {
            let rec = state.records.get(id)?;
            if rec.status != Status::Committed {
                return None;
            }
            let has_blocking = rec.deps.iter().any(|dep| !state.is_executed(dep));
            if has_blocking {
                Some(*id)
            } else {
                None
            }
        })
        .or_else(|| state.committed_queue.iter().next().map(|(_, id)| *id));

    let Some(mut current) = root else {
        return (None, Vec::new());
    };

    let mut chain = Vec::new();
    let mut seen = HashSet::new();
    for _ in 0..MAX_CHAIN {
        if !seen.insert(current) {
            break;
        }
        let (status, blocking_dep, blocking_status, blocking_missing) =
            match state.records.get(&current) {
                Some(rec) => {
                    let dep = rec.deps.iter().find(|dep| !state.is_executed(dep)).copied();
                    let dep_status = dep.and_then(|d| state.records.get(&d).map(|r| r.status));
                    (rec.status, dep, dep_status, false)
                }
                None => (Status::None, None, None, true),
            };

        chain.push(BlockedStep {
            id: current,
            status,
            blocking_dep,
            blocking_status,
            blocking_missing,
        });

        let Some(next) = blocking_dep else {
            break;
        };
        current = next;
    }

    (root, chain)
}

fn build_blocking_chain_from(state: &State, root: TxnId, limit: usize) -> Vec<BlockedStep> {
    let mut chain = Vec::new();
    let mut seen = HashSet::new();
    let mut current = root;

    for _ in 0..limit {
        if !seen.insert(current) {
            break;
        }
        let (status, blocking_dep, blocking_status, blocking_missing) =
            match state.records.get(&current) {
                Some(rec) => {
                    let dep = rec.deps.iter().find(|dep| !state.is_executed(dep)).copied();
                    let dep_status = dep.and_then(|d| state.records.get(&d).map(|r| r.status));
                    (rec.status, dep, dep_status, false)
                }
                None => (Status::None, None, None, true),
            };

        chain.push(BlockedStep {
            id: current,
            status,
            blocking_dep,
            blocking_status,
            blocking_missing,
        });

        let Some(next) = blocking_dep else {
            break;
        };
        current = next;
    }

    chain
}

fn pick_recovery_from_chain(chain: &[BlockedStep]) -> Option<TxnId> {
    if chain.is_empty() {
        return None;
    }

    let mut index = HashMap::with_capacity(chain.len());
    for (idx, step) in chain.iter().enumerate() {
        index.insert(step.id, idx);
    }

    let mut cycle_range: Option<(usize, usize)> = None;
    for (idx, step) in chain.iter().enumerate() {
        if let Some(dep) = step.blocking_dep {
            if let Some(&dep_idx) = index.get(&dep) {
                let start = dep_idx.min(idx);
                let end = dep_idx.max(idx);
                cycle_range = Some((start, end));
                break;
            }
        }
    }

    if let Some((start, end)) = cycle_range {
        let mut candidates = Vec::new();
        for step in &chain[start..=end] {
            if step.status < Status::Committed || step.blocking_missing {
                candidates.push(step.id);
            }
        }
        if candidates.is_empty() {
            candidates.extend(chain[start..=end].iter().map(|s| s.id));
        }
        candidates.sort();
        return candidates.first().copied();
    }

    for step in chain {
        if step.blocking_missing {
            return Some(step.id);
        }
        if let Some(dep) = step.blocking_dep {
            let needs_recovery = step
                .blocking_status
                .map(|s| s < Status::Committed)
                .unwrap_or(true);
            if needs_recovery {
                return Some(dep);
            }
        }
    }

    None
}

fn first_blocking_dep(chain: &[BlockedStep]) -> Option<(TxnId, Option<Status>, bool)> {
    let mut fallback: Option<(TxnId, Option<Status>, bool)> = None;
    for step in chain {
        if let Some(dep) = step.blocking_dep {
            let status = step.blocking_status;
            if status.map(|s| s < Status::Committed).unwrap_or(true) {
                return Some((dep, status, step.blocking_missing));
            }
            if fallback.is_none() {
                fallback = Some((dep, status, step.blocking_missing));
            }
        }
        if step.blocking_missing {
            return Some((step.id, None, true));
        }
    }
    fallback
}

#[derive(Debug)]
struct RecoveryValue {
    command: Bytes,
    seq: u64,
    deps: Vec<TxnId>,
}

/// Result of merging recover replies.
///
/// Purpose:
/// - Distinguish a fully-resolved recovery value from the special case where
///   replicas agree on a committed digest but did not include command bytes.
///
/// Design:
/// - `Ready` carries concrete command/seq/deps for accept+commit.
/// - `MissingCommittedCommand` carries the required digest so caller can run a
///   peer `fetch_command` probe before retrying.
///
/// Inputs:
/// - Produced by `choose_recovery_value`.
///
/// Outputs:
/// - Consumed by `recover_txn_inner`.
#[derive(Debug)]
enum RecoveryChoice {
    Ready(RecoveryValue),
    MissingCommittedCommand { digest: [u8; 32] },
}

#[derive(Debug)]
enum ProposeOnceError {
    Rejected { promised: Ballot },
    NoQuorum(anyhow::Error),
}

impl From<anyhow::Error> for ProposeOnceError {
    fn from(value: anyhow::Error) -> Self {
        Self::NoQuorum(value)
    }
}

fn merge_preaccept(oks: &[PreAcceptResponse]) -> RecoveryValue {
    let seq = oks.iter().map(|r| r.seq).max().unwrap_or(1);
    let deps = oks
        .iter()
        .flat_map(|r| r.deps.iter().copied())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    RecoveryValue {
        command: Bytes::new(),
        seq,
        deps,
    }
}

/// Merge a quorum of recover replies into one recovery decision.
///
/// Purpose:
/// - Derive one deterministic value for accept/commit during recovery.
///
/// Design:
/// - Enforces command consistency when bytes are present.
/// - Uses explicit `has_command`/`command_digest` metadata to distinguish:
///   committed NOOP (legal empty command) vs committed missing non-empty
///   command (requires fetch before proposing).
///
/// Inputs:
/// - `replies`: quorum of `RecoverResponse` values.
///
/// Outputs:
/// - `RecoveryChoice::Ready` when value is fully known.
/// - `RecoveryChoice::MissingCommittedCommand` when digest is known but bytes
///   are absent from all committed/executed replies.
fn choose_recovery_value(replies: &[RecoverResponse]) -> anyhow::Result<RecoveryChoice> {
    let mut command: Option<Bytes> = None;
    let mut digest: Option<[u8; 32]> = None;
    let mut seq = 0u64;
    let mut deps = BTreeSet::new();
    let mut saw_committed = false;

    for r in replies {
        if matches!(r.status, TxnStatus::Committed | TxnStatus::Executed) {
            saw_committed = true;
        }
        if r.has_command && !r.command.is_empty() {
            if let Some(cmd) = &command {
                anyhow::ensure!(
                    cmd.as_ref() == r.command.as_ref(),
                    "conflicting command bytes"
                );
            } else {
                command = Some(r.command.clone());
            }
            let computed = command_digest(&r.command);
            if let Some(existing) = digest {
                anyhow::ensure!(existing == computed, "conflicting command digests");
            } else {
                digest = Some(computed);
            }
        } else if let Some(reply_digest) = r.command_digest {
            if let Some(existing) = digest {
                anyhow::ensure!(existing == reply_digest, "conflicting command digests");
            } else {
                digest = Some(reply_digest);
            }
        }
        seq = seq.max(r.seq);
        deps.extend(r.deps.iter().copied());
    }

    if command.is_none() && saw_committed {
        if let Some(digest) = digest {
            if digest == command_digest(&[]) {
                return Ok(RecoveryChoice::Ready(RecoveryValue {
                    command: Bytes::new(),
                    seq: seq.max(1),
                    deps: deps.into_iter().collect(),
                }));
            }
            return Ok(RecoveryChoice::MissingCommittedCommand { digest });
        }
        anyhow::bail!("recovery saw committed txn but no replica returned command or digest");
    }
    // If no replica can provide a value, recover by committing a NOOP command.
    // (This can happen when a txn was observed as a dependency but never reached quorum.)
    let command = command.unwrap_or_default();
    Ok(RecoveryChoice::Ready(RecoveryValue {
        command,
        seq: seq.max(1),
        deps: deps.into_iter().collect(),
    }))
}

fn find_recovery_target_deep(state: &State, start: TxnId, limit: usize) -> Option<TxnId> {
    let mut stack = vec![start];
    let mut visited = HashSet::<TxnId>::new();

    while let Some(id) = stack.pop() {
        if visited.len() >= limit {
            break;
        }
        if !visited.insert(id) {
            continue;
        }

        let rec = state.records.get(&id)?;
        for dep in &rec.deps {
            if state.is_executed(dep) {
                continue;
            }
            let Some(dep_rec) = state.records.get(dep) else {
                return Some(*dep);
            };
            if dep_rec.status < Status::Committed {
                return Some(*dep);
            }
            if dep_rec.status < Status::Executed {
                stack.push(*dep);
            }
        }
    }

    None
}

// Snapshot of execution-relevant state so SCC computation can run without holding the state lock.
struct ExecSnapshot {
    deps: HashMap<TxnId, Vec<TxnId>>,
    status: HashMap<TxnId, Status>,
    seq: HashMap<TxnId, u64>,
    executed_prefix_by_node: HashMap<NodeId, u64>,
    executed_out_of_order: HashSet<TxnId>,
}

fn is_executed_snapshot(snapshot: &ExecSnapshot, txn_id: &TxnId) -> bool {
    let prefix = snapshot
        .executed_prefix_by_node
        .get(&txn_id.node_id)
        .copied()
        .unwrap_or(0);
    txn_id.counter <= prefix || snapshot.executed_out_of_order.contains(txn_id)
}

fn scc_ready_snapshot(snapshot: &ExecSnapshot, scc: &[TxnId], id: TxnId) -> bool {
    let Some(deps) = snapshot.deps.get(&id) else {
        return false;
    };
    for dep in deps {
        if is_executed_snapshot(snapshot, dep) {
            continue;
        }
        if scc.iter().any(|x| x == dep) {
            continue;
        }
        return false;
    }
    true
}

fn pick_ready_scc(snapshot: &ExecSnapshot, candidates: &[TxnId]) -> Vec<TxnId> {
    if candidates.is_empty() {
        return Vec::new();
    }

    let sccs = kosaraju_scc_from_deps(candidates, &snapshot.deps);
    let mut ready = Vec::<Vec<TxnId>>::new();
    for scc in sccs {
        if scc.iter().all(|id| {
            snapshot
                .status
                .get(id)
                .is_some_and(|s| *s == Status::Committed)
        }) && scc.iter().all(|id| scc_ready_snapshot(snapshot, &scc, *id))
        {
            ready.push(scc);
        }
    }

    if ready.is_empty() {
        return Vec::new();
    }

    ready.sort_by_key(|scc| {
        scc.iter()
            .filter_map(|id| snapshot.seq.get(id).copied())
            .min()
            .unwrap_or(0)
    });

    let mut picked = ready.remove(0);
    picked.sort_by_key(|id| {
        let seq = snapshot.seq.get(id).copied().unwrap_or(0);
        (seq, *id)
    });
    picked
}

fn kosaraju_scc_from_deps(nodes: &[TxnId], deps: &HashMap<TxnId, Vec<TxnId>>) -> Vec<Vec<TxnId>> {
    let n = nodes.len();
    if n == 0 {
        return Vec::new();
    }

    let mut index = HashMap::<TxnId, usize>::with_capacity(n);
    for (i, id) in nodes.iter().enumerate() {
        index.insert(*id, i);
    }

    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut rev: Vec<Vec<usize>> = vec![Vec::new(); n];

    for (i, id) in nodes.iter().enumerate() {
        let Some(dep_list) = deps.get(id) else {
            continue;
        };
        for dep in dep_list {
            let Some(&j) = index.get(dep) else {
                continue;
            };
            adj[i].push(j);
            rev[j].push(i);
        }
    }

    let mut visited = vec![false; n];
    let mut order = Vec::<usize>::with_capacity(n);

    for v in 0..n {
        if visited[v] {
            continue;
        }
        visited[v] = true;
        let mut stack: Vec<(usize, usize)> = vec![(v, 0)];
        while let Some((node, next_idx)) = stack.pop() {
            if next_idx < adj[node].len() {
                let next = adj[node][next_idx];
                stack.push((node, next_idx + 1));
                if !visited[next] {
                    visited[next] = true;
                    stack.push((next, 0));
                }
            } else {
                order.push(node);
            }
        }
    }

    let mut comp_mark = vec![false; n];
    let mut out = Vec::<Vec<TxnId>>::new();

    for &v in order.iter().rev() {
        if comp_mark[v] {
            continue;
        }

        let mut component = Vec::<TxnId>::new();
        let mut stack = vec![v];
        comp_mark[v] = true;
        while let Some(node) = stack.pop() {
            component.push(nodes[node]);
            for &next in &rev[node] {
                if !comp_mark[next] {
                    comp_mark[next] = true;
                    stack.push(next);
                }
            }
        }

        out.push(component);
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accord::{CommitDurabilityMode, GroupId};
    use async_trait::async_trait;
    use std::collections::BTreeSet as StdBTreeSet;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{mpsc as std_mpsc, Arc, Mutex};
    use std::time::Duration as StdDuration;

    /// Scripted response for one `accept` RPC target in round tests.
    ///
    /// Purpose:
    /// - Control per-peer latency and success/rejection behavior for Accept rounds.
    ///
    /// Design:
    /// - Optional delay simulates slow peers.
    /// - Outcome captures either a concrete protocol response or a transport error.
    ///
    /// Inputs:
    /// - Delay budget and response outcome.
    ///
    /// Outputs:
    /// - Deterministic scripted `accept` behavior in tests.
    #[derive(Clone, Debug)]
    struct AcceptPeerPlan {
        delay: StdDuration,
        outcome: AcceptPeerOutcome,
    }

    #[derive(Clone, Debug)]
    /// Scripted Accept RPC outcome for one peer in tests.
    ///
    /// Purpose:
    /// - Represent either a valid protocol response or an injected transport failure.
    ///
    /// Design:
    /// - `Response` mirrors `AcceptResponse` fields used by quorum logic.
    /// - `TransportError` simulates failed RPC delivery/processing.
    ///
    /// Inputs:
    /// - `ok`/`promised` or static error message.
    ///
    /// Outputs:
    /// - Deterministic Accept behavior for one target peer.
    enum AcceptPeerOutcome {
        Response { ok: bool, promised: Ballot },
        TransportError(&'static str),
    }

    /// Scripted response for one `pre_accept` RPC target in proposal tests.
    ///
    /// Purpose:
    /// - Control remote PreAccept replies so fast-path tests can exercise
    ///   proposal code without live peer groups.
    ///
    /// Design:
    /// - Optional delay simulates slow peers.
    /// - Outcome captures either a concrete protocol response or a transport error.
    ///
    /// Inputs:
    /// - Delay budget and response outcome.
    ///
    /// Outputs:
    /// - Deterministic scripted `pre_accept` behavior in tests.
    #[derive(Clone, Debug)]
    struct PreAcceptPeerPlan {
        delay: StdDuration,
        outcome: PreAcceptPeerOutcome,
    }

    #[derive(Clone, Debug)]
    /// Scripted PreAccept RPC outcome for one peer in tests.
    ///
    /// Purpose:
    /// - Represent either a valid protocol response or an injected transport failure.
    ///
    /// Design:
    /// - `Response` mirrors `PreAcceptResponse` fields used by fast-path merge.
    ///
    /// Inputs:
    /// - `ok`/`promised` plus sequence/dependency metadata.
    ///
    /// Outputs:
    /// - Deterministic PreAccept behavior for one target peer.
    enum PreAcceptPeerOutcome {
        Response {
            ok: bool,
            promised: Ballot,
            seq: u64,
            deps: Vec<TxnId>,
        },
    }

    /// Scripted response for one `commit` RPC target in round tests.
    ///
    /// Purpose:
    /// - Control per-peer latency and success/failure behavior for Commit rounds.
    ///
    /// Design:
    /// - Optional delay simulates slow peers.
    /// - Outcome captures either a protocol ACK or a transport error.
    ///
    /// Inputs:
    /// - Delay budget and response outcome.
    ///
    /// Outputs:
    /// - Deterministic scripted `commit` behavior in tests.
    #[derive(Clone, Debug)]
    struct CommitPeerPlan {
        delay: StdDuration,
        outcome: CommitPeerOutcome,
    }

    #[derive(Clone, Debug)]
    /// Scripted Commit RPC outcome for one peer in tests.
    ///
    /// Purpose:
    /// - Represent either a valid commit ACK or an injected transport failure.
    ///
    /// Design:
    /// - `Response` carries the `ok` bit consumed by quorum counting.
    /// - `TransportError` simulates failed RPC delivery/processing.
    ///
    /// Inputs:
    /// - `ok` flag or static error message.
    ///
    /// Outputs:
    /// - Deterministic Commit behavior for one target peer.
    enum CommitPeerOutcome {
        Response { ok: bool },
        TransportError(&'static str),
    }

    /// Scriptable transport used by group-round unit tests.
    ///
    /// Purpose:
    /// - Provide deterministic per-peer behavior for PreAccept/Accept/Commit RPCs.
    ///
    /// Design:
    /// - PreAccept/Accept/Commit methods use configured per-peer plans.
    /// - All other RPC methods fail loudly to catch accidental test misuse.
    ///
    /// Inputs:
    /// - Optional preaccept, accept, and commit plans keyed by peer id.
    ///
    /// Outputs:
    /// - Transport behavior tailored to one test scenario.
    struct NoopTransport {
        pre_accept_plans: Arc<Mutex<HashMap<NodeId, PreAcceptPeerPlan>>>,
        accept_plans: Arc<Mutex<HashMap<NodeId, AcceptPeerPlan>>>,
        commit_plans: Arc<Mutex<HashMap<NodeId, CommitPeerPlan>>>,
    }

    impl NoopTransport {
        /// Build a transport with no scripted peer responses.
        ///
        /// Purpose:
        /// - Preserve prior "always fail" behavior for tests that do not use remote RPC paths.
        ///
        /// Design:
        /// - Initializes empty per-peer plan tables.
        ///
        /// Inputs:
        /// - None.
        ///
        /// Outputs:
        /// - `NoopTransport` where all RPC methods fail unless explicitly scripted.
        fn new() -> Self {
            Self {
                pre_accept_plans: Arc::new(Mutex::new(HashMap::new())),
                accept_plans: Arc::new(Mutex::new(HashMap::new())),
                commit_plans: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        /// Build a transport with scripted PreAccept/Accept/Commit peer plans.
        ///
        /// Purpose:
        /// - Configure deterministic remote behavior for proposal/round tests.
        ///
        /// Design:
        /// - Stores per-peer plans in shared mutex maps for lightweight lookup.
        ///
        /// Inputs:
        /// - `pre_accept_plans`: per-peer PreAccept behaviors.
        /// - `accept_plans`: per-peer Accept behaviors.
        /// - `commit_plans`: per-peer Commit behaviors.
        ///
        /// Outputs:
        /// - `NoopTransport` with scripted PreAccept/Accept/Commit responses.
        fn with_plans(
            pre_accept_plans: HashMap<NodeId, PreAcceptPeerPlan>,
            accept_plans: HashMap<NodeId, AcceptPeerPlan>,
            commit_plans: HashMap<NodeId, CommitPeerPlan>,
        ) -> Self {
            Self {
                pre_accept_plans: Arc::new(Mutex::new(pre_accept_plans)),
                accept_plans: Arc::new(Mutex::new(accept_plans)),
                commit_plans: Arc::new(Mutex::new(commit_plans)),
            }
        }
    }

    #[async_trait]
    impl Transport for NoopTransport {
        async fn pre_accept(
            &self,
            target: NodeId,
            _req: PreAcceptRequest,
        ) -> anyhow::Result<PreAcceptResponse> {
            let plan = self
                .pre_accept_plans
                .lock()
                .expect("pre_accept plan lock")
                .get(&target)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("transport not used in this test"))?;
            if !plan.delay.is_zero() {
                time::sleep(plan.delay).await;
            }
            match plan.outcome {
                PreAcceptPeerOutcome::Response {
                    ok,
                    promised,
                    seq,
                    deps,
                } => Ok(PreAcceptResponse {
                    ok,
                    promised,
                    seq,
                    deps,
                }),
            }
        }

        /// Execute scripted `accept` behavior for one target peer.
        ///
        /// Purpose:
        /// - Feed deterministic Accept outcomes into quorum-round tests.
        ///
        /// Design:
        /// - Reads one plan by target id, applies optional delay, then emits either
        ///   a protocol response or a transport error.
        ///
        /// Inputs:
        /// - `target`: peer id used to select the plan.
        /// - `_req`: protocol request payload (unused by scripted responses).
        ///
        /// Outputs:
        /// - `AcceptResponse`/error according to the configured per-peer plan.
        async fn accept(
            &self,
            target: NodeId,
            _req: AcceptRequest,
        ) -> anyhow::Result<AcceptResponse> {
            let plan = self
                .accept_plans
                .lock()
                .expect("accept plan lock")
                .get(&target)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("transport not used in this test"))?;
            if !plan.delay.is_zero() {
                // Simulate a slow peer without blocking executor threads.
                time::sleep(plan.delay).await;
            }
            match plan.outcome {
                AcceptPeerOutcome::Response { ok, promised } => Ok(AcceptResponse { ok, promised }),
                AcceptPeerOutcome::TransportError(msg) => Err(anyhow::anyhow!(msg)),
            }
        }

        /// Execute scripted `commit` behavior for one target peer.
        ///
        /// Purpose:
        /// - Feed deterministic Commit outcomes into quorum-round tests.
        ///
        /// Design:
        /// - Reads one plan by target id, applies optional delay, then emits either
        ///   a protocol ACK or a transport error.
        ///
        /// Inputs:
        /// - `target`: peer id used to select the plan.
        /// - `_req`: protocol request payload (unused by scripted responses).
        ///
        /// Outputs:
        /// - `CommitResponse`/error according to the configured per-peer plan.
        async fn commit(
            &self,
            target: NodeId,
            _req: CommitRequest,
        ) -> anyhow::Result<CommitResponse> {
            let plan = self
                .commit_plans
                .lock()
                .expect("commit plan lock")
                .get(&target)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("transport not used in this test"))?;
            if !plan.delay.is_zero() {
                // Simulate a slow peer without blocking executor threads.
                time::sleep(plan.delay).await;
            }
            match plan.outcome {
                CommitPeerOutcome::Response { ok } => Ok(CommitResponse { ok }),
                CommitPeerOutcome::TransportError(msg) => Err(anyhow::anyhow!(msg)),
            }
        }

        async fn recover(
            &self,
            _target: NodeId,
            _req: RecoverRequest,
        ) -> anyhow::Result<RecoverResponse> {
            Err(anyhow::anyhow!("transport not used in this test"))
        }

        async fn fetch_command(
            &self,
            _target: NodeId,
            _group_id: GroupId,
            _txn_id: TxnId,
        ) -> anyhow::Result<Option<Bytes>> {
            Err(anyhow::anyhow!("transport not used in this test"))
        }

        async fn report_executed(
            &self,
            _target: NodeId,
            _req: ReportExecutedRequest,
        ) -> anyhow::Result<ReportExecutedResponse> {
            Err(anyhow::anyhow!("transport not used in this test"))
        }

        async fn last_executed_prefix(
            &self,
            _target: NodeId,
            _group_id: GroupId,
        ) -> anyhow::Result<Vec<ExecutedPrefix>> {
            Err(anyhow::anyhow!("transport not used in this test"))
        }

        async fn executed(
            &self,
            _target: NodeId,
            _group_id: GroupId,
            _txn_id: TxnId,
        ) -> anyhow::Result<bool> {
            Err(anyhow::anyhow!("transport not used in this test"))
        }

        async fn mark_visible(
            &self,
            _target: NodeId,
            _group_id: GroupId,
            _txn_id: TxnId,
        ) -> anyhow::Result<bool> {
            Err(anyhow::anyhow!("transport not used in this test"))
        }
    }

    /// Minimal state machine that classifies every non-empty command as a write.
    ///
    /// Inputs:
    /// - `data`: opaque command bytes from Accord.
    ///
    /// Output:
    /// - One synthetic write key for non-empty commands, enabling write commit
    ///   bookkeeping in `rpc_commit` during unit tests.
    struct TestStateMachine;

    impl StateMachine for TestStateMachine {
        fn command_keys(&self, data: &[u8]) -> anyhow::Result<CommandKeys> {
            if data.is_empty() {
                return Ok(CommandKeys::default());
            }
            Ok(CommandKeys {
                reads: Vec::new(),
                writes: vec![b"unit-test-key".to_vec()],
            })
        }

        fn apply(&self, _data: &[u8], _meta: ExecMeta) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// Enumerate fixed-size subsets for small protocol model tests.
    ///
    /// Purpose:
    /// - Keep quorum-intersection checks exhaustive without introducing a
    ///   property-test dependency.
    ///
    /// Design:
    /// - Depth-first combination generator over tiny test inputs.
    ///
    /// Inputs:
    /// - `items`: candidate node ids.
    /// - `size`: desired subset cardinality.
    ///
    /// Outputs:
    /// - All subsets of `items` with exactly `size` elements.
    fn subsets_of_size(items: &[usize], size: usize) -> Vec<StdBTreeSet<usize>> {
        fn rec(
            items: &[usize],
            size: usize,
            idx: usize,
            cur: &mut StdBTreeSet<usize>,
            out: &mut Vec<StdBTreeSet<usize>>,
        ) {
            if cur.len() == size {
                out.push(cur.clone());
                return;
            }
            if idx >= items.len() {
                return;
            }
            let remaining_needed = size.saturating_sub(cur.len());
            if items.len().saturating_sub(idx) < remaining_needed {
                return;
            }

            cur.insert(items[idx]);
            rec(items, size, idx + 1, cur, out);
            cur.remove(&items[idx]);
            rec(items, size, idx + 1, cur, out);
        }

        let mut out = Vec::new();
        let mut cur = StdBTreeSet::new();
        rec(items, size, 0, &mut cur, &mut out);
        out
    }

    /// Commit log that blocks durable appends until the test releases it.
    ///
    /// Design:
    /// - Sends a one-time start signal when the durable append begins.
    /// - Waits on `release_rx` before returning success.
    struct BlockingDurableCommitLog {
        started_tx: Mutex<Option<std_mpsc::Sender<()>>>,
        release_rx: Mutex<std_mpsc::Receiver<()>>,
        append_calls: AtomicU64,
    }

    impl BlockingDurableCommitLog {
        /// Construct a blocking commit log with external start/release channels.
        ///
        /// Inputs:
        /// - `started_tx`: signaled once the first durable append starts.
        /// - `release_rx`: gate that must be released before append returns.
        ///
        /// Output:
        /// - Commit-log test double that lets tests control durable-append timing.
        fn new(started_tx: std_mpsc::Sender<()>, release_rx: std_mpsc::Receiver<()>) -> Self {
            Self {
                started_tx: Mutex::new(Some(started_tx)),
                release_rx: Mutex::new(release_rx),
                append_calls: AtomicU64::new(0),
            }
        }
    }

    impl CommitLog for BlockingDurableCommitLog {
        fn append_commits_with_options(
            &self,
            _entries: Vec<CommitLogEntry>,
            options: CommitLogAppendOptions,
        ) -> anyhow::Result<()> {
            self.append_calls.fetch_add(1, Ordering::Relaxed);
            if options.require_durable {
                if let Some(tx) = self.started_tx.lock().expect("started tx lock").take() {
                    let _ = tx.send(());
                }
                self.release_rx
                    .lock()
                    .expect("release rx lock")
                    .recv()
                    .map_err(|_| anyhow::anyhow!("release signal dropped"))?;
            }
            Ok(())
        }

        fn mark_executed(&self, _txn_id: TxnId) -> anyhow::Result<()> {
            Ok(())
        }

        fn load(&self) -> anyhow::Result<Vec<CommitLogEntry>> {
            Ok(Vec::new())
        }

        fn compact(&self, _max_delete: usize) -> anyhow::Result<usize> {
            Ok(0)
        }
    }

    /// Commit log that injects an error whenever durable append is requested.
    ///
    /// Design:
    /// - Used to verify sync-commit failure propagation from WAL to RPC caller.
    struct FailingDurableCommitLog;

    impl CommitLog for FailingDurableCommitLog {
        fn append_commits_with_options(
            &self,
            _entries: Vec<CommitLogEntry>,
            options: CommitLogAppendOptions,
        ) -> anyhow::Result<()> {
            if options.require_durable {
                anyhow::bail!("injected durable append failure");
            }
            Ok(())
        }

        fn mark_executed(&self, _txn_id: TxnId) -> anyhow::Result<()> {
            Ok(())
        }

        fn load(&self) -> anyhow::Result<Vec<CommitLogEntry>> {
            Ok(Vec::new())
        }

        fn compact(&self, _max_delete: usize) -> anyhow::Result<usize> {
            Ok(0)
        }
    }

    /// Build a compact, single-node group config for commit-path unit tests.
    ///
    /// Input:
    /// - `mode`: commit durability mode under test.
    ///
    /// Output:
    /// - Deterministic config with short timeouts and enabled commit-log batching.
    fn test_config(mode: CommitDurabilityMode) -> Config {
        Config {
            group_id: 1,
            node_id: 1,
            members: vec![Member { id: 1 }],
            rpc_timeout: StdDuration::from_millis(200),
            propose_timeout: StdDuration::from_secs(2),
            recovery_min_delay: StdDuration::from_millis(10),
            stall_recover_interval: StdDuration::from_millis(10),
            preaccept_stall_hits: 1,
            execute_batch_max: 16,
            inline_command_in_accept_commit: true,
            executed_command_cache_max_bytes: 64 * 1024 * 1024,
            commit_log_batch_max: 16,
            commit_log_batch_wait: StdDuration::from_micros(50),
            commit_durability_mode: mode,
            fast_path_1rtt: false,
        }
    }

    /// Build a multi-node config tuned for quorum-round unit tests.
    ///
    /// Purpose:
    /// - Provide deterministic quorum sizing and timeout behavior for Accept/Commit tests.
    ///
    /// Design:
    /// - Uses caller-supplied members and timeouts with otherwise stable defaults.
    /// - Supports "observer local node" tests by allowing `node_id` outside the voter set.
    ///
    /// Inputs:
    /// - `node_id`: local test node id.
    /// - `members`: runtime voter/member ids.
    /// - `rpc_timeout`: round-level peer wait budget.
    /// - `propose_timeout`: per-RPC timeout budget for commit fanout.
    ///
    /// Outputs:
    /// - Config suitable for direct `run_accept_round`/`run_commit_round` tests.
    fn round_test_config(
        node_id: NodeId,
        members: Vec<NodeId>,
        rpc_timeout: StdDuration,
        propose_timeout: StdDuration,
    ) -> Config {
        Config {
            group_id: 1,
            node_id,
            members: members.into_iter().map(|id| Member { id }).collect(),
            rpc_timeout,
            propose_timeout,
            recovery_min_delay: StdDuration::from_millis(10),
            stall_recover_interval: StdDuration::from_millis(10),
            preaccept_stall_hits: 1,
            execute_batch_max: 16,
            inline_command_in_accept_commit: true,
            executed_command_cache_max_bytes: 64 * 1024 * 1024,
            commit_log_batch_max: 16,
            commit_log_batch_wait: StdDuration::from_micros(50),
            commit_durability_mode: CommitDurabilityMode::AsyncCommit,
            fast_path_1rtt: false,
        }
    }

    /// Build a deterministic commit request for local `rpc_commit` tests.
    ///
    /// Input:
    /// - `counter`: transaction counter to make test txn IDs unique.
    ///
    /// Output:
    /// - Valid commit request with fixed command payload and digest.
    fn test_commit_request(counter: u64) -> CommitRequest {
        let command = Bytes::from_static(b"set unit-test-key value");
        CommitRequest {
            group_id: 1,
            txn_id: TxnId {
                node_id: 1,
                counter,
            },
            ballot: Ballot::initial(1),
            command: command.clone(),
            command_digest: command_digest(&command),
            has_command: true,
            seq: 1,
            deps: Vec::new(),
        }
    }

    /// Ensure sync-commit mode never ACKs before the durable append completes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sync_commit_waits_for_durable_append_before_ack() {
        let (started_tx, started_rx) = std_mpsc::channel();
        let (release_tx, release_rx) = std_mpsc::channel();
        let commit_log = Arc::new(BlockingDurableCommitLog::new(started_tx, release_rx));
        let group = Arc::new(Group::new(
            test_config(CommitDurabilityMode::SyncCommit),
            Arc::new(NoopTransport::new()),
            Arc::new(TestStateMachine),
            Some(commit_log),
        ));

        let req = test_commit_request(10);
        let group_task = group.clone();
        let join = tokio::spawn(async move { group_task.rpc_commit(req).await });

        tokio::task::block_in_place(|| {
            started_rx
                .recv_timeout(StdDuration::from_secs(1))
                .expect("durable append should start")
        });
        assert!(
            !join.is_finished(),
            "sync commit returned before durable append completed"
        );

        release_tx.send(()).expect("release durable append");
        let resp = tokio::time::timeout(StdDuration::from_secs(1), join)
            .await
            .expect("commit should finish after release")
            .expect("join should succeed");
        assert!(
            resp.ok,
            "commit should succeed after durable append completion"
        );
    }

    /// Ensure commit-round metrics expose synchronous durable wait on the local path.
    ///
    /// Purpose:
    /// - Prove the new passive breakdown isolates WAL/fsync wait without
    ///   changing sync-commit ACK semantics.
    ///
    /// Design:
    /// - Run a single-node commit round against the blocking durable commit log.
    /// - Assert the returned timings and exported debug stats both report a
    ///   non-zero durable-wait sample.
    ///
    /// Inputs:
    /// - One blocking durable commit-log test double.
    ///
    /// Outputs:
    /// - Non-zero local durable timing metrics and zero remote-quorum samples.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sync_commit_round_records_local_durable_wait_metrics() {
        let (started_tx, started_rx) = std_mpsc::channel();
        let (release_tx, release_rx) = std_mpsc::channel();
        let commit_log = Arc::new(BlockingDurableCommitLog::new(started_tx, release_rx));
        let group = Arc::new(Group::new(
            test_config(CommitDurabilityMode::SyncCommit),
            Arc::new(NoopTransport::new()),
            Arc::new(TestStateMachine),
            Some(commit_log),
        ));

        let command = Bytes::from_static(b"sync-commit-round-metrics");
        let group_task = group.clone();
        let join = tokio::spawn(async move {
            group_task
                .run_commit_round(
                    TxnId {
                        node_id: 1,
                        counter: 12,
                    },
                    Ballot::initial(1),
                    command.clone(),
                    command_digest(&command),
                    1,
                    Vec::new(),
                    true,
                )
                .await
        });

        tokio::task::block_in_place(|| {
            started_rx
                .recv_timeout(StdDuration::from_secs(1))
                .expect("durable append should start")
        });
        assert!(
            !join.is_finished(),
            "commit round returned before durable append completed"
        );

        release_tx.send(()).expect("release durable append");
        let timings = tokio::time::timeout(StdDuration::from_secs(1), join)
            .await
            .expect("commit round should finish after release")
            .expect("join should succeed")
            .expect("commit round should succeed");

        assert!(
            timings.local_durable_wait_us > 0,
            "commit round should report a durable wait sample"
        );
        assert_eq!(
            timings.remote_quorum_wait_us, 0,
            "single-node commit should not report remote quorum wait"
        );

        let stats = group.debug_stats().await;
        assert_eq!(stats.commit_local_durable_count, 1);
        assert!(
            stats.commit_local_durable_total_us > 0,
            "exported durable wait should be non-zero"
        );
        assert_eq!(stats.commit_local_log_queue_count, 1);
        assert_eq!(stats.commit_local_log_append_count, 1);
        assert!(
            stats.commit_local_log_append_total_us > 0,
            "exported append execution should be non-zero"
        );
        assert_eq!(stats.commit_local_post_durable_state_count, 1);
        assert_eq!(stats.commit_remote_quorum_count, 0);
        assert_eq!(stats.commit_tail_count, 0);
    }

    /// Ensure sync-commit mode surfaces durable append failures to callers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sync_commit_fails_when_durable_append_fails() {
        let group = Arc::new(Group::new(
            test_config(CommitDurabilityMode::SyncCommit),
            Arc::new(NoopTransport::new()),
            Arc::new(TestStateMachine),
            Some(Arc::new(FailingDurableCommitLog)),
        ));

        let resp = group.rpc_commit(test_commit_request(11)).await;
        assert!(!resp.ok, "sync commit must fail when durable append fails");

        let stats = group.debug_stats().await;
        assert_eq!(
            stats.records_status_committed_len, 0,
            "failed durable commit must not transition record to committed"
        );
        assert_eq!(
            stats.committed_queue_len, 0,
            "failed durable commit must not enqueue execution"
        );
    }

    /// Verify Accept round returns immediately once quorum ACKs, without waiting
    /// for a much slower peer response.
    ///
    /// Purpose:
    /// - Validate Stage 4A behavior: quorum completion should not block on tail peers.
    ///
    /// Design:
    /// - Two peers ACK quickly and one peer responds much later.
    /// - The test asserts wall-clock completion well below the slow-peer delay.
    ///
    /// Inputs:
    /// - Scripted per-peer Accept delays and responses.
    ///
    /// Outputs:
    /// - Successful Accept response with bounded elapsed time.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn accept_round_reaches_quorum_without_waiting_for_slowest_peer() {
        let ballot = Ballot::initial(7);
        let transport = NoopTransport::with_plans(
            HashMap::new(),
            HashMap::from([
                (
                    1,
                    AcceptPeerPlan {
                        delay: StdDuration::from_millis(5),
                        outcome: AcceptPeerOutcome::Response {
                            ok: true,
                            promised: ballot,
                        },
                    },
                ),
                (
                    2,
                    AcceptPeerPlan {
                        delay: StdDuration::from_millis(10),
                        outcome: AcceptPeerOutcome::Response {
                            ok: true,
                            promised: ballot,
                        },
                    },
                ),
                (
                    3,
                    AcceptPeerPlan {
                        delay: StdDuration::from_millis(400),
                        outcome: AcceptPeerOutcome::Response {
                            ok: true,
                            promised: ballot,
                        },
                    },
                ),
            ]),
            HashMap::new(),
        );
        let group = Group::new(
            round_test_config(
                99,
                vec![1, 2, 3],
                StdDuration::from_millis(120),
                StdDuration::from_secs(1),
            ),
            Arc::new(transport),
            Arc::new(TestStateMachine),
            None,
        );

        let command = Bytes::from_static(b"accept-round-test");
        let start = time::Instant::now();
        let resp = group
            .run_accept_round(
                TxnId {
                    node_id: 7,
                    counter: 1,
                },
                ballot,
                command.clone(),
                command_digest(&command),
                1,
                Vec::new(),
                true,
            )
            .await
            .expect("accept round should reach quorum");
        let elapsed = start.elapsed();

        assert!(resp.ok, "accept quorum should succeed");
        assert!(
            elapsed < StdDuration::from_millis(200),
            "accept round should return at quorum instead of waiting for 400ms slow peer (elapsed={elapsed:?})"
        );
    }

    /// Verify Accept round reports rejection when a higher promised ballot is observed.
    ///
    /// Purpose:
    /// - Preserve ballot monotonicity behavior while changing fanout implementation.
    ///
    /// Design:
    /// - One peer returns `ok=false` with a higher promised ballot and the others fail.
    ///
    /// Inputs:
    /// - Scripted Accept plans with mixed rejection and transport failures.
    ///
    /// Outputs:
    /// - Non-quorum `AcceptResponse` carrying the higher promised ballot.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn accept_round_returns_rejection_when_higher_ballot_observed() {
        let ballot = Ballot::initial(7);
        let higher = Ballot {
            counter: ballot.counter.saturating_add(3),
            node_id: 3,
        };
        let transport = NoopTransport::with_plans(
            HashMap::new(),
            HashMap::from([
                (
                    1,
                    AcceptPeerPlan {
                        delay: StdDuration::from_millis(5),
                        outcome: AcceptPeerOutcome::Response {
                            ok: false,
                            promised: higher,
                        },
                    },
                ),
                (
                    2,
                    AcceptPeerPlan {
                        delay: StdDuration::from_millis(5),
                        outcome: AcceptPeerOutcome::TransportError("injected accept failure"),
                    },
                ),
                (
                    3,
                    AcceptPeerPlan {
                        delay: StdDuration::from_millis(5),
                        outcome: AcceptPeerOutcome::TransportError("injected accept failure"),
                    },
                ),
            ]),
            HashMap::new(),
        );
        let group = Group::new(
            round_test_config(
                99,
                vec![1, 2, 3],
                StdDuration::from_millis(80),
                StdDuration::from_secs(1),
            ),
            Arc::new(transport),
            Arc::new(TestStateMachine),
            None,
        );

        let command = Bytes::from_static(b"accept-reject-test");
        let resp = group
            .run_accept_round(
                TxnId {
                    node_id: 7,
                    counter: 2,
                },
                ballot,
                command.clone(),
                command_digest(&command),
                1,
                Vec::new(),
                true,
            )
            .await
            .expect("accept round should surface rejection instead of no-quorum");

        assert!(!resp.ok, "accept should reject on higher promised ballot");
        assert_eq!(
            resp.promised, higher,
            "accept rejection should carry highest promised ballot"
        );
    }

    /// Verify Commit round returns immediately once voter quorum ACKs, without
    /// waiting for a much slower peer response.
    ///
    /// Purpose:
    /// - Validate Stage 4A behavior on commit fanout.
    ///
    /// Design:
    /// - Two peers ACK quickly and one peer responds much later.
    /// - The test asserts wall-clock completion well below the slow-peer delay.
    ///
    /// Inputs:
    /// - Scripted per-peer Commit delays and responses.
    ///
    /// Outputs:
    /// - Successful commit result with bounded elapsed time.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn commit_round_reaches_quorum_without_waiting_for_slowest_peer() {
        let ballot = Ballot::initial(7);
        let transport = NoopTransport::with_plans(
            HashMap::new(),
            HashMap::new(),
            HashMap::from([
                (
                    1,
                    CommitPeerPlan {
                        delay: StdDuration::from_millis(5),
                        outcome: CommitPeerOutcome::Response { ok: true },
                    },
                ),
                (
                    2,
                    CommitPeerPlan {
                        delay: StdDuration::from_millis(10),
                        outcome: CommitPeerOutcome::Response { ok: true },
                    },
                ),
                (
                    3,
                    CommitPeerPlan {
                        delay: StdDuration::from_millis(400),
                        outcome: CommitPeerOutcome::Response { ok: true },
                    },
                ),
            ]),
        );
        let group = Group::new(
            round_test_config(
                99,
                vec![1, 2, 3],
                StdDuration::from_millis(120),
                StdDuration::from_secs(1),
            ),
            Arc::new(transport),
            Arc::new(TestStateMachine),
            None,
        );

        let command = Bytes::from_static(b"commit-round-test");
        let start = time::Instant::now();
        group
            .run_commit_round(
                TxnId {
                    node_id: 7,
                    counter: 3,
                },
                ballot,
                command.clone(),
                command_digest(&command),
                1,
                Vec::new(),
                true,
            )
            .await
            .expect("commit round should reach quorum");
        let elapsed = start.elapsed();

        assert!(
            elapsed < StdDuration::from_millis(200),
            "commit round should return at quorum instead of waiting for 400ms slow peer (elapsed={elapsed:?})"
        );

        time::sleep(StdDuration::from_millis(450)).await;
        let stats = group.debug_stats().await;
        assert_eq!(stats.commit_remote_quorum_count, 1);
        assert!(
            stats.commit_remote_quorum_total_us > 0,
            "commit round should record remote quorum wait"
        );
        assert_eq!(stats.commit_tail_count, 1);
        assert!(
            stats.commit_tail_total_us > 0,
            "commit round should record the follower tail after quorum"
        );
        assert_eq!(stats.commit_quorum_closer_top.len(), 1);
        assert_eq!(stats.commit_quorum_closer_top[0].node_id, 2);
        assert_eq!(stats.commit_quorum_closer_top[0].count, 1);
    }

    /// Verify Commit round returns a no-quorum error when enough voter ACKs are
    /// not observed before timeout.
    ///
    /// Purpose:
    /// - Preserve existing error behavior while changing fanout implementation.
    ///
    /// Design:
    /// - One peer ACKs quickly, one fails, and one is slower than the round timeout.
    ///
    /// Inputs:
    /// - Scripted per-peer Commit delays and responses.
    ///
    /// Outputs:
    /// - `ProposeOnceError::NoQuorum` result.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn commit_round_returns_no_quorum_when_acks_are_insufficient() {
        let ballot = Ballot::initial(7);
        let transport = NoopTransport::with_plans(
            HashMap::new(),
            HashMap::new(),
            HashMap::from([
                (
                    1,
                    CommitPeerPlan {
                        delay: StdDuration::from_millis(5),
                        outcome: CommitPeerOutcome::Response { ok: true },
                    },
                ),
                (
                    2,
                    CommitPeerPlan {
                        delay: StdDuration::from_millis(5),
                        outcome: CommitPeerOutcome::TransportError("injected commit failure"),
                    },
                ),
                (
                    3,
                    CommitPeerPlan {
                        delay: StdDuration::from_millis(300),
                        outcome: CommitPeerOutcome::Response { ok: true },
                    },
                ),
            ]),
        );
        let group = Group::new(
            round_test_config(
                99,
                vec![1, 2, 3],
                StdDuration::from_millis(80),
                StdDuration::from_secs(1),
            ),
            Arc::new(transport),
            Arc::new(TestStateMachine),
            None,
        );

        let command = Bytes::from_static(b"commit-noquorum-test");
        let res = group
            .run_commit_round(
                TxnId {
                    node_id: 7,
                    counter: 4,
                },
                ballot,
                command.clone(),
                command_digest(&command),
                1,
                Vec::new(),
                true,
            )
            .await;

        match res {
            Err(ProposeOnceError::NoQuorum(_)) => {}
            other => panic!("expected no-quorum error, got {other:?}"),
        }
    }

    /// Verify 1RTT fast-path writes do not wait for remote Commit quorum.
    ///
    /// Purpose:
    /// - Protect the client-visible optimization: after identical PreAccept
    ///   quorum and successful local publish, the proposal may return before
    ///   slow remote Commit RPCs finish.
    ///
    /// Design:
    /// - Script one fast PreAccept peer to close quorum.
    /// - Script all Commit peers slower than the proposal RPC timeout; the old
    ///   client-blocking Commit path would fail or wait, while the 1RTT path
    ///   succeeds after local publish.
    ///
    /// Inputs:
    /// - Three-voter group with `fast_path_1rtt=true` and async commit durability.
    ///
    /// Outputs:
    /// - Applied proposal result and local committed/barrier-visible state.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fast_path_1rtt_write_returns_after_local_publish_without_remote_commit_quorum() {
        let ballot = Ballot::initial(1);
        let transport = NoopTransport::with_plans(
            HashMap::from([
                (
                    2,
                    PreAcceptPeerPlan {
                        delay: StdDuration::from_millis(5),
                        outcome: PreAcceptPeerOutcome::Response {
                            ok: true,
                            promised: ballot,
                            seq: 1,
                            deps: Vec::new(),
                        },
                    },
                ),
                (
                    3,
                    PreAcceptPeerPlan {
                        delay: StdDuration::from_millis(500),
                        outcome: PreAcceptPeerOutcome::Response {
                            ok: true,
                            promised: ballot,
                            seq: 1,
                            deps: Vec::new(),
                        },
                    },
                ),
            ]),
            HashMap::new(),
            HashMap::from([
                (
                    2,
                    CommitPeerPlan {
                        delay: StdDuration::from_millis(500),
                        outcome: CommitPeerOutcome::Response { ok: true },
                    },
                ),
                (
                    3,
                    CommitPeerPlan {
                        delay: StdDuration::from_millis(500),
                        outcome: CommitPeerOutcome::Response { ok: true },
                    },
                ),
            ]),
        );
        let mut cfg = round_test_config(
            1,
            vec![1, 2, 3],
            StdDuration::from_millis(80),
            StdDuration::from_secs(1),
        );
        cfg.fast_path_1rtt = true;
        let group = Arc::new(Group::new(
            cfg,
            Arc::new(transport),
            Arc::new(TestStateMachine),
            None,
        ));

        let start = time::Instant::now();
        let res = group
            .propose(Bytes::from_static(b"fast-path-1rtt-write"))
            .await
            .expect("fast-path proposal should succeed without remote commit quorum");
        let elapsed = start.elapsed();

        assert!(matches!(res, ProposalResult::Applied));
        assert!(
            elapsed < StdDuration::from_millis(200),
            "1RTT proposal should return before slow Commit RPCs finish (elapsed={elapsed:?})"
        );

        let key = b"unit-test-key".as_slice();
        let barriers = group.last_committed_for_key_slices(&[key]).await;
        let txn_id = barriers[0]
            .map(|item| item.0)
            .expect("fast-path write should be barrier-visible");
        {
            let state = group.state.lock().await;
            let rec = state.records.get(&txn_id).expect("local committed record");
            assert_eq!(rec.status, Status::Committed);
        }
    }

    /// Verify read barriers expose uncommitted fast-path candidates.
    ///
    /// Purpose:
    /// - Prevent linearizable reads from missing a write that may have already
    ///   been ACKed by another coordinator while Commit dissemination is still
    ///   in flight.
    ///
    /// Design:
    /// - Insert a local PreAccepted write and query the per-key barrier helper
    ///   with 1RTT mode enabled.
    ///
    /// Inputs:
    /// - One PreAccept request for the test write key.
    ///
    /// Outputs:
    /// - Barrier target includes the PreAccepted transaction.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fast_path_barrier_reports_preaccepted_write() {
        let mut cfg = round_test_config(
            1,
            vec![1, 2, 3],
            StdDuration::from_millis(80),
            StdDuration::from_secs(1),
        );
        cfg.fast_path_1rtt = true;
        let group = Group::new(
            cfg,
            Arc::new(NoopTransport::new()),
            Arc::new(TestStateMachine),
            None,
        );
        let txn_id = TxnId {
            node_id: 2,
            counter: (1u64 << TXN_COUNTER_SHARD_SHIFT) | 7,
        };
        let resp = group
            .rpc_pre_accept(PreAcceptRequest {
                group_id: 1,
                txn_id,
                ballot: Ballot::initial(2),
                command: Bytes::from_static(b"preaccepted-write"),
                seq: 0,
                deps: Vec::new(),
            })
            .await;
        assert!(resp.ok, "preaccept should succeed");

        let key = b"unit-test-key".as_slice();
        let barriers = group.last_committed_for_key_slices(&[key]).await;
        assert_eq!(barriers[0].map(|item| item.0), Some(txn_id));
        assert_eq!(barriers[0].map(|item| item.1), Some(resp.seq));
    }

    /// Verify recovery can force a fast-path value from quorum intersection.
    ///
    /// Purpose:
    /// - Capture the core 1RTT recovery rule: if a recovery quorum intersects
    ///   the ACKing fast quorum at one PreAccepted replica, the command bytes
    ///   and metadata must be enough to continue recovery.
    ///
    /// Design:
    /// - Merge one PreAccepted reply carrying the command with one Unknown reply.
    ///
    /// Inputs:
    /// - Quorum replies representing the surviving intersection case.
    ///
    /// Outputs:
    /// - Recovery choice keeps the PreAccepted command instead of choosing NOOP.
    #[test]
    fn choose_recovery_value_uses_preaccepted_fast_path_command() {
        let cmd = Bytes::from_static(b"acked-fast-path-command");
        let reply = RecoverResponse {
            ok: true,
            promised: Ballot::zero(),
            status: TxnStatus::PreAccepted,
            accepted_ballot: Some(Ballot::initial(1)),
            command: cmd.clone(),
            command_digest: Some(command_digest(&cmd)),
            has_command: true,
            seq: 11,
            deps: Vec::new(),
        };
        let unknown = RecoverResponse {
            ok: true,
            promised: Ballot::zero(),
            status: TxnStatus::Unknown,
            accepted_ballot: None,
            command: Bytes::new(),
            command_digest: None,
            has_command: false,
            seq: 0,
            deps: Vec::new(),
        };

        let choice =
            choose_recovery_value(&[reply, unknown]).expect("preaccepted recovery should merge");
        match choice {
            RecoveryChoice::Ready(value) => {
                assert_eq!(value.command, cmd);
                assert_eq!(value.seq, 11);
            }
            other => panic!("expected ready recovery value, got {other:?}"),
        }
    }

    /// Exhaustively check quorum intersections used by 1RTT recovery.
    ///
    /// Purpose:
    /// - Provide a small model checker for the fast-path proof obligation:
    ///   every recovery/read quorum must intersect the ACKing fast quorum, even
    ///   after any tolerated crash-stop failures.
    ///
    /// Design:
    /// - Enumerates majority quorums for 3, 5, and 7 voters.
    /// - Enumerates every failure set up to `f=(n-1)/2` and every live recovery
    ///   quorum that remains.
    ///
    /// Inputs:
    /// - Synthetic node ids `0..n`.
    ///
    /// Outputs:
    /// - Assertion failure if any recovery quorum can miss the fast quorum.
    #[test]
    fn fast_path_quorum_model_checks_recovery_and_read_intersections() {
        for n in [3usize, 5, 7] {
            let quorum = (n / 2) + 1;
            let faults = (n - 1) / 2;
            let nodes = (0..n).collect::<Vec<_>>();
            let quorums = subsets_of_size(&nodes, quorum);

            for fast_quorum in &quorums {
                for recovery_quorum in &quorums {
                    assert!(
                        !fast_quorum.is_disjoint(recovery_quorum),
                        "recovery quorum missed fast quorum for n={n}"
                    );
                }

                for failure_count in 0..=faults {
                    for failed in subsets_of_size(&nodes, failure_count) {
                        let live = nodes
                            .iter()
                            .copied()
                            .filter(|node| !failed.contains(node))
                            .collect::<Vec<_>>();
                        if live.len() < quorum {
                            continue;
                        }
                        for recovery_quorum in subsets_of_size(&live, quorum) {
                            assert!(
                                !fast_quorum.is_disjoint(&recovery_quorum),
                                "live recovery quorum missed fast quorum for n={n}, failed={failed:?}"
                            );
                        }
                    }
                }
            }
        }
    }

    /// Model-check conflicting fast-path quorum responses.
    ///
    /// Purpose:
    /// - Cover the conflicting-write case for the 1RTT fast path: two writes on
    ///   the same key cannot both fast-commit with incompatible dependency
    ///   metadata because their fast quorums intersect at a serialized replica.
    ///
    /// Design:
    /// - Simulates write A reaching its fast quorum first.
    /// - Simulates write B reaching another fast quorum where intersection
    ///   replicas report A as a dependency and non-intersection replicas do not.
    /// - Asserts B can be fast only when every B response contains the same dep
    ///   set, which means B depends on A at all responders.
    ///
    /// Inputs:
    /// - Synthetic majority quorums for 3, 5, and 7 voters.
    ///
    /// Outputs:
    /// - Assertion failure if two conflicting writes can both fast-commit
    ///   without a dependency edge.
    #[test]
    fn fast_path_conflict_model_requires_intersection_dependency() {
        const A: usize = 10_001;
        for n in [3usize, 5, 7] {
            let quorum = (n / 2) + 1;
            let nodes = (0..n).collect::<Vec<_>>();
            let quorums = subsets_of_size(&nodes, quorum);

            for a_quorum in &quorums {
                for b_quorum in &quorums {
                    let mut b_response_deps = Vec::new();
                    for node in b_quorum {
                        let saw_a = a_quorum.contains(node);
                        let deps = if saw_a {
                            StdBTreeSet::from([A])
                        } else {
                            StdBTreeSet::new()
                        };
                        b_response_deps.push(deps);
                    }

                    let b_fast = b_response_deps
                        .iter()
                        .all(|deps| deps == &b_response_deps[0]);
                    if b_fast {
                        assert!(
                            b_response_deps[0].contains(&A),
                            "conflicting B fast-pathed without depending on A for n={n}"
                        );
                    }
                }
            }
        }
    }

    /// Verify recovery merge accepts explicitly-encoded committed NOOP replies.
    ///
    /// Purpose:
    /// - Prevent false "missing command" failures when committed value is the
    ///   empty NOOP command.
    ///
    /// Design:
    /// - Build one committed reply with `has_command=true` and empty command.
    /// - Assert merged choice is a ready NOOP value.
    ///
    /// Inputs:
    /// - One committed recover reply with empty command payload.
    ///
    /// Outputs:
    /// - `RecoveryChoice::Ready` containing empty command bytes.
    #[test]
    fn choose_recovery_value_accepts_explicit_committed_noop() {
        let reply = RecoverResponse {
            ok: true,
            promised: Ballot::zero(),
            status: TxnStatus::Committed,
            accepted_ballot: None,
            command: Bytes::new(),
            command_digest: Some(command_digest(&[])),
            has_command: true,
            seq: 7,
            deps: Vec::new(),
        };

        let choice = choose_recovery_value(&[reply]).expect("noop recovery merge should succeed");
        match choice {
            RecoveryChoice::Ready(value) => {
                assert!(value.command.is_empty(), "expected merged NOOP command");
                assert_eq!(value.seq, 7);
            }
            other => panic!("expected ready recovery value, got {other:?}"),
        }
    }

    /// Verify recovery merge asks caller to fetch bytes when digest is known
    /// but no committed reply includes command bytes.
    ///
    /// Purpose:
    /// - Cover the recovery edge path that previously produced repeated
    ///   committed-missing-command stalls.
    ///
    /// Design:
    /// - Build one committed reply with digest metadata and `has_command=false`.
    /// - Assert merge output requests command fetch for that digest.
    ///
    /// Inputs:
    /// - One committed recover reply with missing command bytes.
    ///
    /// Outputs:
    /// - `RecoveryChoice::MissingCommittedCommand`.
    #[test]
    fn choose_recovery_value_flags_missing_committed_command() {
        let cmd = Bytes::from_static(b"recover-me");
        let digest = command_digest(&cmd);
        let reply = RecoverResponse {
            ok: true,
            promised: Ballot::zero(),
            status: TxnStatus::Committed,
            accepted_ballot: None,
            command: Bytes::new(),
            command_digest: Some(digest),
            has_command: false,
            seq: 3,
            deps: Vec::new(),
        };

        let choice =
            choose_recovery_value(&[reply]).expect("missing-command recovery merge should succeed");
        match choice {
            RecoveryChoice::MissingCommittedCommand { digest: got } => {
                assert_eq!(got, digest);
            }
            other => panic!("expected missing-command recovery choice, got {other:?}"),
        }
    }

    /// Verify recovery merge rejects conflicting committed digests.
    ///
    /// Purpose:
    /// - Preserve safety by rejecting incompatible committed values.
    ///
    /// Design:
    /// - Feed two committed replies with different digest metadata.
    /// - Assert merge returns an error.
    ///
    /// Inputs:
    /// - Two committed recover replies with distinct digests.
    ///
    /// Outputs:
    /// - Error from `choose_recovery_value`.
    #[test]
    fn choose_recovery_value_rejects_conflicting_committed_digests() {
        let a = RecoverResponse {
            ok: true,
            promised: Ballot::zero(),
            status: TxnStatus::Committed,
            accepted_ballot: None,
            command: Bytes::new(),
            command_digest: Some(command_digest(b"a")),
            has_command: false,
            seq: 1,
            deps: Vec::new(),
        };
        let b = RecoverResponse {
            ok: true,
            promised: Ballot::zero(),
            status: TxnStatus::Committed,
            accepted_ballot: None,
            command: Bytes::new(),
            command_digest: Some(command_digest(b"b")),
            has_command: false,
            seq: 1,
            deps: Vec::new(),
        };

        let err = choose_recovery_value(&[a, b]).expect_err("conflicting digests should fail");
        assert!(
            err.to_string().contains("conflicting command digests"),
            "unexpected error: {err}"
        );
    }
}
