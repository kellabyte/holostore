//! Consensus engine and executor for a single Accord group.
//!
//! This file contains the Accord proposal path, conflict tracking, and the
//! execution loop that applies committed commands to the state machine.
//! It also wires in batching and background workers (commit-log and apply) to
//! keep IO and storage costs amortized.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::sync::{mpsc, oneshot, Mutex, Notify};
use tokio::time;

use super::state::{
    is_monotonic_update, status_to_txn_status, ExecutedLogEntry, Record, State, Status,
};
use super::types::{
    txn_group_id, AcceptRequest, AcceptResponse, Ballot, CommandKeys, CommitLog, CommitLogEntry,
    CommitRequest, CommitResponse, Config, ExecMeta, ExecutedPrefix, Member, NodeId,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse, ReportExecutedRequest,
    ReportExecutedResponse, StateMachine, Transport, TxnId, TxnStatus, TXN_COUNTER_SHARD_SHIFT,
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
struct PhaseTimings {
    pre_accept_us: u64,
    accept_us: u64,
    commit_us: u64,
    execute_us: u64,
    visible_us: u64,
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
    pub fast_path_count: u64,
    pub slow_path_count: u64,
}

impl Handle {
    pub async fn propose(&self, command: Vec<u8>) -> anyhow::Result<ProposalResult> {
        self.group.propose(command).await
    }

    pub async fn propose_read_with_deps(
        &self,
        command: Vec<u8>,
        deps: Vec<(TxnId, u64)>,
    ) -> anyhow::Result<ProposalResult> {
        self.group.propose_read_with_deps(command, deps).await
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
    commit_log_tx: Option<std_mpsc::Sender<CommitLogEntry>>,
    apply_tx: Option<std_mpsc::Sender<ApplyWork>>,
    state: Mutex<State>,
    execute_lock: Mutex<()>,
    executor_notify: Notify,
    executor_started: AtomicBool,
    metrics: GroupMetrics,
    compact_counter: AtomicU64,
    peer_rr: AtomicU64,
    start_at: time::Instant,
}

/// Internal execution plan item (one committed txn).
#[derive(Clone, Debug)]
struct ApplyItem {
    id: TxnId,
    command: Vec<u8>,
    keys: CommandKeys,
    seq: u64,
}

/// Work item sent to the apply worker (write batch + response channel).
struct ApplyWork {
    batch: Vec<(Vec<u8>, ExecMeta)>,
    tx: oneshot::Sender<ApplyResult>,
}

#[derive(Clone, Copy, Debug)]
struct ApplyResult {
    apply_us: u64,
    visible_us: u64,
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
            fast_path_count: self.fast_path_count.load(Ordering::Relaxed),
            slow_path_count: self.slow_path_count.load(Ordering::Relaxed),
        }
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

    fn exec_progress_false_streak(&self) -> u64 {
        self.exec_progress_false_streak.load(Ordering::Relaxed)
    }
}

impl Group {
    pub fn new(
        config: Config,
        transport: Arc<dyn Transport>,
        sm: Arc<dyn StateMachine>,
        commit_log: Option<Arc<dyn CommitLog>>,
    ) -> Self {
        let commit_log_tx = commit_log.as_ref().map(|log| {
            let (tx, rx) = std_mpsc::channel::<CommitLogEntry>();
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
                            Ok(entry) => entry,
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
                                Ok(entry) => {
                                    batch.push(entry);
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
                                Ok(entry) => batch.push(entry),
                                Err(std_mpsc::RecvTimeoutError::Timeout) => break,
                                Err(std_mpsc::RecvTimeoutError::Disconnected) => {
                                    disconnected = true;
                                    break;
                                }
                            }
                        }

                        if let Err(err) = log.append_commits(batch) {
                            tracing::warn!(error = ?err, "commit log batch append failed");
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
                        sm.apply_batch(&work.batch);
                        let apply_us =
                            apply_start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;

                        let visible_start = std::time::Instant::now();
                        for (command, meta) in &work.batch {
                            sm.mark_visible(command, *meta);
                        }
                        let visible_us = visible_start
                            .elapsed()
                            .as_micros()
                            .min(u128::from(u64::MAX))
                            as u64;

                        let _ = work.tx.send(ApplyResult {
                            apply_us,
                            visible_us,
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
            metrics: GroupMetrics::default(),
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

    pub fn handle(self: &Arc<Self>) -> Handle {
        Handle {
            group: self.clone(),
        }
    }

    pub async fn last_committed_for_keys(&self, keys: &[Vec<u8>]) -> Vec<Option<(TxnId, u64)>> {
        let state = self.state.lock().await;
        keys.iter()
            .map(|k| state.last_committed_write_by_key.get(k).copied())
            .collect()
    }

    pub async fn observe_last_committed(&self, keys: &[Vec<u8>], values: &[Option<(TxnId, u64)>]) {
        let mut state = self.state.lock().await;
        for (key, item) in keys.iter().zip(values.iter()) {
            let Some((txn_id, seq)) = item else { continue };
            match state.last_committed_write_by_key.get(key) {
                Some((_, cur_seq)) if *cur_seq >= *seq => continue,
                _ => {
                    state
                        .last_committed_write_by_key
                        .insert(key.clone(), (*txn_id, *seq));
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

        if entry.command.is_empty() {
            return Ok(true);
        }

        self.sm.mark_visible(
            &entry.command,
            ExecMeta {
                txn_id,
                seq: entry.seq,
            },
        );

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
                rec.command = Some(entry.command.clone());
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
        let mut executed_log_max_command_bytes = 0usize;
        let mut executed_log_max_deps_len = 0usize;
        for entry in state.executed_log.values() {
            executed_log_max_command_bytes =
                executed_log_max_command_bytes.max(entry.command.len());
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
                if cmd.as_slice() != req.command.as_slice() {
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
                rec.command = Some(req.command.clone());
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
                        rec.command = Some(cmd);
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

    pub async fn rpc_commit(&self, req: CommitRequest) -> CommitResponse {
        if !self.local_is_member() {
            return CommitResponse { ok: false };
        }
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

        loop {
            let now = time::Instant::now();
            let mut state = self.state.lock().await;
            if state.is_executed(&txn_id) {
                return CommitResponse { ok: true };
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
                // Commit is final. Even if we've promised a higher ballot (e.g. due to a concurrent
                // recovery), we still apply the commit as long as the command matches. Rejecting a
                // late commit can strand replicas with long-lived PreAccepted records.
                rec.promised = rec.promised.max(ballot);
                rec.updated_at = now;
            }

            let (prev_status, prev_seq, committed_fast_path) = state
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
                    (
                        r.status,
                        r.seq,
                        r.status >= Status::Committed && same_commit,
                    )
                })
                .expect("record must exist");

            if prev_status >= Status::Executing {
                // Already being applied (or done). A late commit must not downgrade the record or
                // re-insert it into the committed queue.
                return CommitResponse { ok: true };
            }

            if committed_fast_path {
                return CommitResponse { ok: true };
            }

            {
                let rec = state.records.get_mut(&txn_id).expect("record must exist");
                if rec.command.is_none() {
                    if let Some(cmd) = incoming_command.take() {
                        if command_digest(&cmd) != expected_digest {
                            return CommitResponse { ok: false };
                        }
                        let keys = if cmd.is_empty() {
                            CommandKeys::default()
                        } else {
                            match self.sm.command_keys(&cmd) {
                                Ok(keys) => keys,
                                Err(_) => return CommitResponse { ok: false },
                            }
                        };
                        rec.command = Some(cmd);
                        rec.command_digest = Some(expected_digest);
                        rec.keys = Some(keys);
                    } else {
                        drop(state);
                        match self.fetch_command_from_peers(txn_id, expected_digest).await {
                            Ok(Some(cmd)) => {
                                incoming_command = Some(cmd);
                                continue;
                            }
                            _ => return CommitResponse { ok: false },
                        }
                    }
                } else {
                    let cmd = rec.command.as_ref().expect("command exists");
                    if command_digest(cmd) != expected_digest {
                        return CommitResponse { ok: false };
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
                                Err(_) => return CommitResponse { ok: false },
                            }
                        };
                        rec.keys = Some(keys);
                    }
                }
            }

            let req_deps = deps_vec.iter().copied().collect::<BTreeSet<_>>();
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

            let seq = seq.max(1);
            let req_deps_vec = req_deps.iter().copied().collect::<Vec<_>>();
            {
                let rec = state.records.get_mut(&txn_id).expect("record must exist");
                rec.seq = seq;
                rec.deps = req_deps;
                rec.accepted_ballot = Some(rec.accepted_ballot.unwrap_or(ballot).max(ballot));
                rec.status = rec.status.max(Status::Committed);
            }

            if prev_status >= Status::Committed && prev_seq != seq {
                state.remove_committed(txn_id, prev_seq);
            }
            let should_notify = state.committed_queue.is_empty();
            state.insert_committed(txn_id, seq);

            let command_for_log = state
                .records
                .get(&txn_id)
                .and_then(|r| r.command.clone())
                .unwrap_or_default();
            drop(state);
            if should_notify {
                self.executor_notify.notify_one();
            }
            if let Some(tx) = &self.commit_log_tx {
                let entry = CommitLogEntry {
                    txn_id,
                    seq,
                    deps: req_deps_vec,
                    command: command_for_log,
                };
                if tx.send(entry).is_err() {
                    tracing::warn!("commit log batcher closed");
                }
            }
            // Avoid per-commit wakeups; executor polls on a short interval already.
            return CommitResponse { ok: true };
        }
    }

    pub async fn rpc_fetch_command(&self, txn_id: TxnId) -> Option<Vec<u8>> {
        let state = self.state.lock().await;
        if let Some(rec) = state.records.get(&txn_id) {
            if let Some(cmd) = rec.command.as_ref() {
                return Some(cmd.clone());
            }
        }
        state
            .executed_log
            .get(&txn_id)
            .map(|entry| entry.command.clone())
    }

    async fn fetch_command_from_peers(
        &self,
        txn_id: TxnId,
        expected_digest: [u8; 32],
    ) -> anyhow::Result<Option<Vec<u8>>> {
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
            return RecoverResponse {
                ok: false,
                promised: Ballot::zero(),
                status: TxnStatus::Unknown,
                accepted_ballot: None,
                command: Vec::new(),
                seq: 0,
                deps: Vec::new(),
            };
        }
        let now = time::Instant::now();
        let mut state = self.state.lock().await;
        if state.is_executed(&req.txn_id) {
            if let Some(entry) = state.executed_log.get(&req.txn_id) {
                return RecoverResponse {
                    ok: true,
                    promised: Ballot::zero(),
                    status: TxnStatus::Executed,
                    accepted_ballot: None,
                    command: entry.command.clone(),
                    seq: entry.seq,
                    deps: entry.deps.clone(),
                };
            }
            return RecoverResponse {
                ok: true,
                promised: Ballot::zero(),
                status: TxnStatus::Executed,
                accepted_ballot: None,
                command: Vec::new(),
                seq: 1,
                deps: Vec::new(),
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
            return RecoverResponse {
                ok: true,
                promised: rec.promised,
                status: status_to_txn_status(rec.status),
                accepted_ballot: rec.accepted_ballot,
                command: rec.command.clone().unwrap_or_default(),
                seq: rec.seq,
                deps: rec.deps.iter().copied().collect(),
            };
        }

        if req.ballot < rec.promised {
            return RecoverResponse {
                ok: false,
                promised: rec.promised,
                status: status_to_txn_status(rec.status),
                accepted_ballot: rec.accepted_ballot,
                command: rec.command.clone().unwrap_or_default(),
                seq: rec.seq,
                deps: rec.deps.iter().copied().collect(),
            };
        }

        rec.promised = req.ballot;
        rec.updated_at = now;

        RecoverResponse {
            ok: true,
            promised: rec.promised,
            status: status_to_txn_status(rec.status),
            accepted_ballot: rec.accepted_ballot,
            command: rec.command.clone().unwrap_or_default(),
            seq: rec.seq,
            deps: rec.deps.iter().copied().collect(),
        }
    }

    pub async fn rpc_report_executed(&self, req: ReportExecutedRequest) -> ReportExecutedResponse {
        let now = time::Instant::now();
        let mut state = self.state.lock().await;

        let mut prefixes = HashMap::new();
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
        );
        let _ = Self::maybe_compact_state_locked(&mut state, now);

        ReportExecutedResponse { ok: true }
    }

    async fn propose(&self, command: Vec<u8>) -> anyhow::Result<ProposalResult> {
        self.propose_with_deps(command, None).await
    }

    async fn propose_read_with_deps(
        &self,
        command: Vec<u8>,
        deps: Vec<(TxnId, u64)>,
    ) -> anyhow::Result<ProposalResult> {
        self.propose_with_deps(command, Some(deps)).await
    }

    async fn propose_with_deps(
        &self,
        command: Vec<u8>,
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
    ) -> usize {
        const GC_INTERVAL: Duration = Duration::from_millis(500);
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
        let mut new_order = VecDeque::with_capacity(state.executed_log_order.len());
        while let Some(id) = state.executed_log_order.pop_front() {
            if !state.visible_txns.contains(&id) {
                new_order.push_back(id);
                continue;
            }
            let min_prefix = global_min_by_node.get(&id.node_id).copied().unwrap_or(0);
            if id.counter <= min_prefix {
                if let Some(entry) = state.executed_log.remove(&id) {
                    state.executed_log_bytes =
                        state.executed_log_bytes.saturating_sub(entry.command.len());
                    state.executed_log_deps_total = state
                        .executed_log_deps_total
                        .saturating_sub(entry.deps.len());
                }
                state.visible_txns.remove(&id);
                removed += 1;
            } else {
                new_order.push_back(id);
            }
        }
        state.executed_log_order = new_order;
        state.last_executed_gc_at = now;
        removed
    }

    async fn propose_once(
        &self,
        txn_id: TxnId,
        ballot: Ballot,
        command: Vec<u8>,
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
        let mut commit_us = 0u64;
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
        self.run_commit_round(
            txn_id,
            ballot,
            command,
            command_digest,
            merged.seq,
            merged.deps,
            self.config.inline_command_in_accept_commit,
        )
        .await?;
        commit_us = commit_us.saturating_add(commit_start.elapsed().as_micros() as u64);
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
            // Fast-ack writes: return after commit. Execution happens asynchronously.
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

    async fn run_accept_round(
        &self,
        txn_id: TxnId,
        ballot: Ballot,
        command: Vec<u8>,
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
            (Vec::new(), false)
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

        let (tx, mut rx) = mpsc::channel::<anyhow::Result<AcceptResponse>>(peers.len().max(1));
        for peer in peers.iter().copied() {
            let transport = self.transport.clone();
            let tx = tx.clone();
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
            tokio::spawn(async move {
                let resp = match time::timeout(rpc_timeout, transport.accept(peer, req)).await {
                    Ok(resp) => resp.map_err(|e| anyhow::anyhow!("accept rpc failed: {e}")),
                    Err(_) => Err(anyhow::anyhow!("accept rpc timed out")),
                };
                let _ = tx.send(resp).await;
                Ok::<(), anyhow::Error>(())
            });
        }
        drop(tx);

        let deadline = time::Instant::now() + rpc_timeout;
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
                        ok += 1;
                        if ok >= quorum {
                            return Ok(AcceptResponse {
                                ok: true,
                                promised: max_promised,
                            });
                        }
                    }
                }
                Err(_) => {}
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

    async fn run_commit_round(
        &self,
        txn_id: TxnId,
        ballot: Ballot,
        command: Vec<u8>,
        command_digest: [u8; 32],
        seq: u64,
        deps: Vec<TxnId>,
        inline_command: bool,
    ) -> Result<(), ProposeOnceError> {
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
            (Vec::new(), false)
        };
        let mut ok = 0usize;
        if local_is_member {
            let local = self
                .rpc_commit(CommitRequest {
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
            if local_is_voter {
                ok = 1;
            }
        }

        if ok >= quorum {
            return Ok(());
        }

        let (tx, mut rx) = mpsc::channel::<(bool, bool)>(peers.len().max(1));
        for peer in peers.iter().copied() {
            let transport = self.transport.clone();
            let tx = tx.clone();
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
            tokio::spawn(async move {
                let ok = match time::timeout(commit_timeout, transport.commit(peer, req)).await {
                    Ok(res) => res.ok().is_some_and(|r| r.ok),
                    Err(_) => false,
                };
                let _ = tx.send((ok, count_for_quorum)).await;
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
            let Ok(Some((peer_ok, count_for_quorum))) = recv else {
                break;
            };
            if peer_ok && count_for_quorum {
                ok += 1;
                if ok >= quorum {
                    return Ok(());
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

    async fn should_recover(&self, txn_id: TxnId, waiting_since: time::Instant) -> bool {
        let now = time::Instant::now();
        const MIN_RECOVERY_DELAY: Duration = Duration::from_secs(1);
        let recovery_delay = self
            .config
            .rpc_timeout
            .min(Duration::from_millis(200))
            .max(MIN_RECOVERY_DELAY);
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

    fn should_attempt_stall_recover(&self) -> bool {
        const STALL_RECOVER_INTERVAL_US: u64 = 200_000;
        let now_us = self.start_at.elapsed().as_micros() as u64;
        let last = self
            .metrics
            .exec_stall_recover_at_us
            .load(Ordering::Relaxed);
        if now_us.saturating_sub(last) < STALL_RECOVER_INTERVAL_US {
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

    async fn should_recover_due_to_stall(&self, txn_id: TxnId) -> bool {
        let now = time::Instant::now();
        const MIN_RECOVERY_DELAY: Duration = Duration::from_secs(1);
        let recovery_delay = self
            .config
            .rpc_timeout
            .min(Duration::from_millis(200))
            .max(MIN_RECOVERY_DELAY);
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

    async fn execute_progress(&self) -> anyhow::Result<bool> {
        let start = time::Instant::now();
        let res = self.execute_progress_inner().await;
        self.metrics
            .record_exec_progress(start.elapsed(), res.as_ref().ok().copied());
        res
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
                        executed_prefix_by_node: state.executed_prefix_by_node.clone(),
                        executed_out_of_order: state.executed_out_of_order.clone(),
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
            let apply_inline = |batch: &[(Vec<u8>, ExecMeta)]| -> ApplyResult {
                let apply_start = time::Instant::now();
                self.sm.apply_batch(batch);
                let apply_us = apply_start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;

                let visible_start = time::Instant::now();
                for (command, meta) in batch {
                    self.sm.mark_visible(command, *meta);
                }
                let visible_us = visible_start
                    .elapsed()
                    .as_micros()
                    .min(u128::from(u64::MAX)) as u64;
                ApplyResult {
                    apply_us,
                    visible_us,
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
            self.sm.apply(&item.command, meta);
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
                if let Some(command) = rec.command.take() {
                    state.record_executed_value(
                        item.id,
                        ExecutedLogEntry {
                            command,
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

            let chosen = choose_recovery_value(&replies)?;
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
    command: Vec<u8>,
    seq: u64,
    deps: Vec<TxnId>,
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
        command: Vec::new(),
        seq,
        deps,
    }
}

fn choose_recovery_value(replies: &[RecoverResponse]) -> anyhow::Result<RecoveryValue> {
    let mut command: Option<Vec<u8>> = None;
    let mut seq = 0u64;
    let mut deps = BTreeSet::new();
    let mut saw_committed = false;

    for r in replies {
        if matches!(r.status, TxnStatus::Committed | TxnStatus::Executed) {
            saw_committed = true;
        }
        if !r.command.is_empty() {
            if let Some(cmd) = &command {
                anyhow::ensure!(
                    cmd.as_slice() == r.command.as_slice(),
                    "conflicting command bytes"
                );
            } else {
                command = Some(r.command.clone());
            }
        }
        seq = seq.max(r.seq);
        deps.extend(r.deps.iter().copied());
    }

    // If no replica can provide a value, recover by committing a NOOP command.
    // (This can happen when a txn was observed as a dependency but never reached quorum.)
    if command.is_none() && saw_committed {
        anyhow::bail!("recovery saw committed txn but no replica returned command");
    }
    let command = command.unwrap_or_default();
    Ok(RecoveryValue {
        command,
        seq: seq.max(1),
        deps: deps.into_iter().collect(),
    })
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
