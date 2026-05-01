//! Runtime metrics and exported debug snapshots for an Accord group.
//!
//! Purpose:
//! - Keep passive timing counters and debug snapshots out of the protocol flow.
//!
//! Design:
//! - Counters are atomics so hot paths can update them without taking the group
//!   state lock; debug collection builds a point-in-time snapshot on demand.
//!
//! Inputs:
//! - Protocol phase timings, executor/apply timings, and state snapshots.
//!
//! Outputs:
//! - `DebugStats` plus lightweight internal timing structs consumed by other
//!   group submodules.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use bytes::Bytes;

use super::{Group, NodeId, Status};

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
pub(super) struct CommitRoundTimings {
    pub(super) local_state_update_us: u64,
    pub(super) local_durable_wait_us: u64,
    pub(super) local_log_queue_wait_us: u64,
    pub(super) local_log_append_us: u64,
    pub(super) local_post_durable_state_update_us: u64,
    pub(super) remote_quorum_wait_us: u64,
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
pub(super) struct CommitRpcTimings {
    pub(super) total_us: u64,
    pub(super) durable_wait_us: u64,
    pub(super) durable_queue_wait_us: u64,
    pub(super) durable_append_us: u64,
    pub(super) state_update_us: u64,
    pub(super) post_durable_state_update_us: u64,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct PhaseTimings {
    pub(super) pre_accept_us: u64,
    pub(super) accept_us: u64,
    pub(super) commit_us: u64,
    pub(super) commit_local_state_update_us: u64,
    pub(super) commit_local_durable_wait_us: u64,
    pub(super) commit_local_log_queue_wait_us: u64,
    pub(super) commit_local_log_append_us: u64,
    pub(super) commit_local_post_durable_state_update_us: u64,
    pub(super) commit_remote_quorum_wait_us: u64,
    pub(super) execute_us: u64,
    pub(super) visible_us: u64,
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

/// Atomically updated counters for `DebugStats`.
#[derive(Default)]
pub(super) struct GroupMetrics {
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
    pub(super) exec_stall_log_at_us: AtomicU64,
    pub(super) exec_stall_recover_at_us: AtomicU64,
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

    pub(super) fn record_fast_path(&self, fast_path: bool) {
        if fast_path {
            self.fast_path_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.slow_path_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn record_exec_progress(&self, dur: Duration, ok: Option<bool>) {
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

    pub(super) fn record_exec_recover(&self, dur: Duration, ok: Option<bool>) {
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

    pub(super) fn record_apply_write(&self, dur: Duration) {
        let us = dur.as_micros().min(u128::from(u64::MAX)) as u64;
        self.apply_write_count.fetch_add(1, Ordering::Relaxed);
        self.apply_write_total_us.fetch_add(us, Ordering::Relaxed);
        self.apply_write_max_us.fetch_max(us, Ordering::Relaxed);
    }

    pub(super) fn record_apply_read(&self, dur: Duration) {
        let us = dur.as_micros().min(u128::from(u64::MAX)) as u64;
        self.apply_read_count.fetch_add(1, Ordering::Relaxed);
        self.apply_read_total_us.fetch_add(us, Ordering::Relaxed);
        self.apply_read_max_us.fetch_max(us, Ordering::Relaxed);
    }

    pub(super) fn record_apply_batch(&self, dur: Duration) {
        let us = dur.as_micros().min(u128::from(u64::MAX)) as u64;
        self.apply_batch_count.fetch_add(1, Ordering::Relaxed);
        self.apply_batch_total_us.fetch_add(us, Ordering::Relaxed);
        self.apply_batch_max_us.fetch_max(us, Ordering::Relaxed);
    }

    pub(super) fn record_mark_visible(&self, dur: Duration) {
        let us = dur.as_micros().min(u128::from(u64::MAX)) as u64;
        self.mark_visible_count.fetch_add(1, Ordering::Relaxed);
        self.mark_visible_total_us.fetch_add(us, Ordering::Relaxed);
        self.mark_visible_max_us.fetch_max(us, Ordering::Relaxed);
    }

    pub(super) fn record_state_update(&self, dur: Duration) {
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
    pub(super) fn record_commit_local_state(&self, dur: Duration) {
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
    pub(super) fn record_commit_local_durable(&self, dur: Duration) {
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
    pub(super) fn record_commit_local_log_queue(&self, dur: Duration) {
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
    pub(super) fn record_commit_local_log_append(&self, dur: Duration) {
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
    pub(super) fn record_commit_local_post_durable_state(&self, dur: Duration) {
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
    pub(super) fn record_commit_remote_quorum(&self, dur: Duration) {
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
    pub(super) fn record_commit_tail(&self, dur: Duration) {
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
    pub(super) fn record_commit_quorum_closer(&self, peer: NodeId, dur: Duration) {
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

    pub(super) fn exec_progress_false_streak(&self) -> u64 {
        self.exec_progress_false_streak.load(Ordering::Relaxed)
    }
}

impl Group {
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
            executed_prefix_nodes: state.executed_prefix_by_stream.len(),
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
}
