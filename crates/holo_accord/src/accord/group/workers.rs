//! Worker channels and constructors for group-local blocking work.
//!
//! Purpose:
//! - Isolate commit-log batching and state-machine apply worker plumbing from
//!   protocol code.
//!
//! Design:
//! - Worker request structs stay module-private to the group implementation.
//! - `Group::new` wires the workers without changing the runtime layout of
//!   `Group` itself.
//!
//! Inputs:
//! - Group configuration, optional commit log, and state-machine handle.
//!
//! Outputs:
//! - Initialized worker senders and completion metadata used by commit/apply paths.

use super::metrics::GroupMetrics;
use super::*;

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
pub(super) struct CommitLogAppendCompletion {
    pub(super) queue_wait_us: u64,
    pub(super) append_us: u64,
}

/// Internal execution plan item (one committed txn).
#[derive(Clone, Debug)]
pub(super) struct ApplyItem {
    pub(super) id: TxnId,
    pub(super) command: Bytes,
    pub(super) keys: CommandKeys,
    pub(super) seq: u64,
}

/// Work item sent to the apply worker (write batch + response channel).
pub(super) struct ApplyWork {
    pub(super) batch: Vec<(Bytes, ExecMeta)>,
    pub(super) tx: oneshot::Sender<ApplyResult>,
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
pub(super) struct CommitLogWork {
    pub(super) entry: CommitLogEntry,
    pub(super) require_durable: bool,
    pub(super) done_tx: std_mpsc::Sender<anyhow::Result<CommitLogAppendCompletion>>,
    pub(super) enqueued_at: std::time::Instant,
}

pub(super) struct ApplyResult {
    pub(super) apply_us: u64,
    pub(super) visible_us: u64,
    pub(super) result: anyhow::Result<()>,
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

        Self {
            members: RwLock::new(config.members.clone()),
            voters: RwLock::new(initial_voters),
            config,
            transport,
            sm,
            commit_log,
            commit_log_tx,
            apply_tx,
            state: Mutex::new(State::new()),
            execute_lock: Mutex::new(()),
            executor_notify: Notify::new(),
            executor_started: AtomicBool::new(false),
            metrics: Arc::new(GroupMetrics::default()),
            compact_counter: AtomicU64::new(0),
            peer_rr: AtomicU64::new(0),
            start_at: time::Instant::now(),
        }
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
    pub(super) fn enqueue_commit_log_append(
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
    pub(super) fn wait_commit_log_append(
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
}
