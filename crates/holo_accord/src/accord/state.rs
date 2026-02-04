//! In-memory state and indexes for a single Accord group.
//!
//! This module holds the record table plus several secondary indexes that are
//! used for conflict detection and execution ordering. It is intentionally
//! separate from the consensus logic so that state operations remain testable
//! and easy to reason about.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

use tokio::sync::oneshot;
use tokio::time;

use super::{Ballot, CommandKeys, NodeId, TxnId, TxnStatus};

/// Lifecycle state for a transaction record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Status {
    None,
    PreAccepted,
    Accepted,
    Committed,
    Executing,
    Executed,
}

/// In-memory record for a transaction and its consensus metadata.
#[derive(Clone, Debug)]
pub(super) struct Record {
    pub(super) promised: Ballot,
    pub(super) accepted_ballot: Option<Ballot>,
    pub(super) command: Option<Vec<u8>>,
    pub(super) command_digest: Option<[u8; 32]>,
    pub(super) keys: Option<CommandKeys>,
    pub(super) seq: u64,
    pub(super) deps: BTreeSet<TxnId>,
    pub(super) status: Status,
    pub(super) updated_at: time::Instant,
}

/// Mutable group state (protected by a mutex in the consensus engine).
///
/// The structure is optimized for:
/// - quick dependency discovery (`frontier_by_key`, `last_*_by_key`)
/// - ordered execution (`committed_queue`, `committed_ready`)
/// - compact replay metadata (`executed_*`)
#[derive(Debug)]
pub(super) struct State {
    pub(super) next_txn_counter: u64,
    pub(super) next_ballot_counter: u64,
    pub(super) records: HashMap<TxnId, Record>,
    pub(super) frontier_by_key: HashMap<Vec<u8>, BTreeSet<TxnId>>,
    pub(super) last_write_by_key: HashMap<Vec<u8>, TxnId>,
    pub(super) last_committed_write_by_key: HashMap<Vec<u8>, (TxnId, u64)>,
    /// All committed transactions, ordered by sequence for deterministic scans.
    pub(super) committed_queue: BTreeSet<(u64, TxnId)>,
    /// Subset of committed transactions whose dependencies are already executed.
    pub(super) committed_ready: BTreeSet<(u64, TxnId)>,
    /// Remaining unmet dependencies for a committed transaction.
    pub(super) pending_committed_deps: HashMap<TxnId, usize>,
    /// Reverse dependency index to wake dependents when a txn executes.
    pub(super) committed_dependents: HashMap<TxnId, HashSet<TxnId>>,
    /// Dependencies we registered per txn so we can unlink on removal.
    pub(super) committed_dep_links: HashMap<TxnId, Vec<TxnId>>,
    pub(super) executed_prefix_by_node: HashMap<NodeId, u64>,
    pub(super) reported_executed_prefix_by_peer: HashMap<NodeId, HashMap<NodeId, u64>>,
    pub(super) executed_out_of_order: HashSet<TxnId>,
    pub(super) executed_log: HashMap<TxnId, ExecutedLogEntry>,
    pub(super) executed_log_order: VecDeque<TxnId>,
    pub(super) executed_log_bytes: usize,
    pub(super) executed_log_deps_total: usize,
    pub(super) visible_txns: HashSet<TxnId>,
    pub(super) recovering: HashSet<TxnId>,
    pub(super) recovery_attempts_by_txn: HashMap<TxnId, u64>,
    pub(super) recovery_last_attempt: HashMap<TxnId, time::Instant>,
    pub(super) stalled_preaccept_counts: HashMap<TxnId, u32>,
    pub(super) read_waiters: HashMap<TxnId, oneshot::Sender<anyhow::Result<Option<Vec<u8>>>>>,
    pub(super) proposal_timeouts: u64,
    pub(super) execute_timeouts: u64,
    pub(super) recovery_attempts: u64,
    pub(super) recovery_successes: u64,
    pub(super) recovery_failures: u64,
    pub(super) recovery_timeouts: u64,
    pub(super) recovery_noops: u64,
    pub(super) recovery_last_ms: u64,
    pub(super) last_executed_gc_at: time::Instant,
    pub(super) last_compact_at: time::Instant,
}

/// Value stored for executed transactions to satisfy reads and recovery.
#[derive(Clone, Debug)]
pub(super) struct ExecutedLogEntry {
    pub(super) command: Vec<u8>,
    pub(super) seq: u64,
    pub(super) deps: Vec<TxnId>,
}

pub(super) fn is_monotonic_update(
    current_seq: u64,
    current_deps: &BTreeSet<TxnId>,
    new_seq: u64,
    new_deps: &BTreeSet<TxnId>,
) -> bool {
    new_seq >= current_seq && current_deps.is_subset(new_deps)
}

pub(super) fn status_to_txn_status(status: Status) -> TxnStatus {
    match status {
        Status::None => TxnStatus::Unknown,
        Status::PreAccepted => TxnStatus::PreAccepted,
        Status::Accepted => TxnStatus::Accepted,
        Status::Committed => TxnStatus::Committed,
        Status::Executing => TxnStatus::Committed,
        Status::Executed => TxnStatus::Executed,
    }
}

impl State {
    pub(super) fn new() -> Self {
        let now = time::Instant::now();
        Self {
            next_txn_counter: 0,
            next_ballot_counter: 0,
            records: HashMap::new(),
            frontier_by_key: HashMap::new(),
            last_write_by_key: HashMap::new(),
            last_committed_write_by_key: HashMap::new(),
            committed_queue: BTreeSet::new(),
            committed_ready: BTreeSet::new(),
            pending_committed_deps: HashMap::new(),
            committed_dependents: HashMap::new(),
            committed_dep_links: HashMap::new(),
            executed_prefix_by_node: HashMap::new(),
            reported_executed_prefix_by_peer: HashMap::new(),
            executed_out_of_order: HashSet::new(),
            executed_log: HashMap::new(),
            executed_log_order: VecDeque::new(),
            executed_log_bytes: 0,
            executed_log_deps_total: 0,
            visible_txns: HashSet::new(),
            recovering: HashSet::new(),
            recovery_attempts_by_txn: HashMap::new(),
            recovery_last_attempt: HashMap::new(),
            stalled_preaccept_counts: HashMap::new(),
            read_waiters: HashMap::new(),
            proposal_timeouts: 0,
            execute_timeouts: 0,
            recovery_attempts: 0,
            recovery_successes: 0,
            recovery_failures: 0,
            recovery_timeouts: 0,
            recovery_noops: 0,
            recovery_last_ms: 0,
            last_executed_gc_at: now,
            last_compact_at: now,
        }
    }

    pub(super) fn is_executed(&self, txn_id: &TxnId) -> bool {
        let prefix = self
            .executed_prefix_by_node
            .get(&txn_id.node_id)
            .copied()
            .unwrap_or(0);
        txn_id.counter <= prefix || self.executed_out_of_order.contains(txn_id)
    }

    pub(super) fn mark_executed(&mut self, txn_id: TxnId) {
        let prefix = self
            .executed_prefix_by_node
            .entry(txn_id.node_id)
            .or_insert(0);

        if txn_id.counter <= *prefix {
            return;
        }

        if txn_id.counter == *prefix + 1 {
            *prefix += 1;
            loop {
                let next = TxnId {
                    node_id: txn_id.node_id,
                    counter: *prefix + 1,
                };
                if self.executed_out_of_order.remove(&next) {
                    *prefix += 1;
                    continue;
                }
                break;
            }
        } else {
            self.executed_out_of_order.insert(txn_id);
        }
    }

    /// Register a newly committed transaction and update ready tracking.
    pub(super) fn insert_committed(&mut self, txn_id: TxnId, seq: u64) {
        self.committed_queue.insert((seq, txn_id));

        let Some(rec) = self.records.get(&txn_id) else {
            return;
        };

        let mut missing = 0usize;
        let mut registered = Vec::new();
        for dep in rec.deps.iter() {
            if self.is_executed(dep) {
                continue;
            }
            missing = missing.saturating_add(1);
            self.committed_dependents
                .entry(*dep)
                .or_default()
                .insert(txn_id);
            registered.push(*dep);
        }

        if missing == 0 {
            self.committed_ready.insert((seq, txn_id));
        } else {
            self.pending_committed_deps.insert(txn_id, missing);
        }

        if !registered.is_empty() {
            self.committed_dep_links.insert(txn_id, registered);
        }
    }

    /// Remove a committed transaction from queues and reverse indexes.
    pub(super) fn remove_committed(&mut self, txn_id: TxnId, seq: u64) {
        self.committed_queue.remove(&(seq, txn_id));
        self.committed_ready.remove(&(seq, txn_id));
        self.pending_committed_deps.remove(&txn_id);
        if let Some(deps) = self.committed_dep_links.remove(&txn_id) {
            for dep in deps {
                if let Some(set) = self.committed_dependents.get_mut(&dep) {
                    set.remove(&txn_id);
                    if set.is_empty() {
                        self.committed_dependents.remove(&dep);
                    }
                }
            }
        }
    }

    /// Mark a transaction executed and move newly unblocked dependents to ready.
    pub(super) fn mark_executed_and_wake(&mut self, txn_id: TxnId) {
        self.mark_executed(txn_id);

        let Some(dependents) = self.committed_dependents.remove(&txn_id) else {
            return;
        };

        for dependent in dependents {
            let Some(pending) = self.pending_committed_deps.get_mut(&dependent) else {
                continue;
            };
            if *pending > 0 {
                *pending -= 1;
            }
            if *pending == 0 {
                self.pending_committed_deps.remove(&dependent);
                if let Some(rec) = self.records.get(&dependent) {
                    if rec.status >= Status::Committed {
                        let seq = rec.seq.max(1);
                        self.committed_ready.insert((seq, dependent));
                    }
                }
            }
        }
    }

    pub(super) fn record_executed_value(&mut self, txn_id: TxnId, value: ExecutedLogEntry) {
        let new_len = value.command.len();
        let new_deps = value.deps.len();
        if let Some(old) = self.executed_log.insert(txn_id, value) {
            self.executed_log_bytes = self.executed_log_bytes + new_len - old.command.len();
            self.executed_log_deps_total = self.executed_log_deps_total + new_deps - old.deps.len();
        } else {
            self.executed_log_order.push_back(txn_id);
            self.executed_log_bytes += new_len;
            self.executed_log_deps_total += new_deps;
        }
    }

    pub(super) fn compute_seq_deps(
        &self,
        config: &super::Config,
        txn_id: TxnId,
        keys: &CommandKeys,
    ) -> (u64, BTreeSet<TxnId>) {
        let mut deps = BTreeSet::new();
        let mut max_seq = 0u64;
        for key in keys.keys() {
            if let Some((last_committed, committed_seq)) =
                self.last_committed_write_by_key.get(key).copied()
            {
                if last_committed != txn_id
                {
                    deps.insert(last_committed);
                    max_seq = max_seq.max(committed_seq);
                }
            }
            if let Some(last) = self.last_write_by_key.get(key).copied() {
                if last != txn_id {
                    let stable = self.global_stable_executed_prefix(config, last.node_id);
                    if last.counter > stable {
                        deps.insert(last);
                        max_seq = max_seq.max(self.txn_seq_hint(&last));
                    }
                }
            }

            let Some(entries) = self.frontier_by_key.get(key) else {
                continue;
            };
            for other in entries {
                if *other == txn_id {
                    continue;
                }
                let Some(rec) = self.records.get(other) else {
                    continue;
                };
                if self.is_executed(other) || rec.status == Status::Executed {
                    continue;
                }
                let other_is_write = rec.keys.as_ref().is_some_and(|k| k.is_write());
                if !other_is_write {
                    continue;
                }
                deps.insert(*other);
                max_seq = max_seq.max(rec.seq);
            }
        }

        (max_seq.saturating_add(1).max(1), deps)
    }

    fn global_stable_executed_prefix(&self, config: &super::Config, origin: NodeId) -> u64 {
        let mut min_prefix = self
            .executed_prefix_by_node
            .get(&origin)
            .copied()
            .unwrap_or(0);

        for member in &config.members {
            let peer_id = member.id;
            if peer_id == config.node_id {
                continue;
            }

            let Some(peer_prefixes) = self.reported_executed_prefix_by_peer.get(&peer_id) else {
                return 0;
            };

            let reported = peer_prefixes.get(&origin).copied().unwrap_or(0);
            min_prefix = min_prefix.min(reported);
            if min_prefix == 0 {
                return 0;
            }
        }

        min_prefix
    }

    fn txn_seq_hint(&self, txn_id: &TxnId) -> u64 {
        if let Some(rec) = self.records.get(txn_id) {
            return rec.seq;
        }
        if let Some(entry) = self.executed_log.get(txn_id) {
            return entry.seq;
        }
        0
    }

    pub(super) fn update_frontier(
        &mut self,
        txn_id: TxnId,
        keys: &CommandKeys,
        deps: &BTreeSet<TxnId>,
    ) {
        if !keys.is_write() {
            return;
        }

        for key in keys.keys() {
            let frontier = self.frontier_by_key.entry(key.clone()).or_default();
            for dep in deps {
                if *dep == txn_id {
                    continue;
                }
                frontier.remove(dep);
            }
            frontier.insert(txn_id);
        }
    }

    pub(super) fn remove_from_index(&mut self, txn_id: TxnId, keys: &CommandKeys) {
        let mut empty_keys = Vec::<Vec<u8>>::new();
        for key in keys.keys() {
            let Some(entries) = self.frontier_by_key.get_mut(key) else {
                continue;
            };
            entries.remove(&txn_id);
            if entries.is_empty() {
                empty_keys.push(key.clone());
            }
        }
        for key in empty_keys {
            self.frontier_by_key.remove(&key);
        }
    }
}
