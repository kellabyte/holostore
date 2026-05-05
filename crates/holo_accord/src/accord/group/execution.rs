//! Execution, visibility, read barriers, and state compaction.
//!
//! Purpose:
//! - Advance committed Accord transactions into state-machine effects and keep
//!   read/visibility metadata current.
//!
//! Design:
//! - Executor methods keep the existing lock boundaries and apply-worker path.
//! - SCC helpers remain local to execution so cycle handling can be understood
//!   apart from proposal and recovery rounds.
//!
//! Inputs:
//! - Committed records, read waiters, executed-prefix reports, and state-machine
//!   callbacks.
//!
//! Outputs:
//! - Applied writes, read responses, visibility markers, and compacted state.

use super::graph::{build_blocking_chain, build_blocking_chain_from, first_blocking_dep};
use super::workers::{ApplyItem, ApplyResult, ApplyWork};
use super::*;

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

// Snapshot of execution-relevant state so SCC computation can run without holding the state lock.
struct ExecSnapshot {
    deps: HashMap<TxnId, Vec<TxnId>>,
    status: HashMap<TxnId, Status>,
    seq: HashMap<TxnId, u64>,
    executed_prefix_by_stream: HashMap<TxnProgressKey, u64>,
    executed_out_of_order: HashSet<TxnId>,
}

/// Pick ready committed transactions on the dependency path to a target.
///
/// Purpose:
/// - Let `execute_until(target)` make progress on the target read/write without
///   first draining unrelated committed work that happens to have a lower
///   sequence number.
///
/// Design:
/// - Walks the target's dependency graph depth-first.
/// - Emits dependencies before dependents so one executor batch can apply a
///   ready chain in dependency order.
/// - Stops at missing, non-committed, executing, or cyclic dependencies and lets
///   the normal scheduler/recovery path handle those cases.
///
/// Inputs:
/// - `state`: locked group state.
/// - `target`: transaction that a foreground caller is waiting on.
/// - `limit`: maximum number of transaction ids to return.
///
/// Outputs:
/// - A dependency-ordered list of committed transactions that can execute now.
pub(super) fn pick_target_execution_path(state: &State, target: TxnId, limit: usize) -> Vec<TxnId> {
    fn visit(
        state: &State,
        txn_id: TxnId,
        limit: usize,
        visiting: &mut HashSet<TxnId>,
        selected: &mut HashSet<TxnId>,
        out: &mut Vec<TxnId>,
    ) {
        if out.len() >= limit || state.is_executed(&txn_id) || selected.contains(&txn_id) {
            return;
        }
        if !visiting.insert(txn_id) {
            return;
        }

        let Some(rec) = state.records.get(&txn_id) else {
            visiting.remove(&txn_id);
            return;
        };
        if rec.status < Status::Committed {
            visiting.remove(&txn_id);
            return;
        }

        for dep in rec.deps.iter().copied() {
            if state.is_executed(&dep) || selected.contains(&dep) {
                continue;
            }
            visit(state, dep, limit, visiting, selected, out);
            if out.len() >= limit {
                visiting.remove(&txn_id);
                return;
            }
        }

        let deps_ready = rec
            .deps
            .iter()
            .all(|dep| state.is_executed(dep) || selected.contains(dep));
        if deps_ready && rec.status == Status::Committed && out.len() < limit {
            selected.insert(txn_id);
            out.push(txn_id);
        }
        visiting.remove(&txn_id);
    }

    if limit == 0 {
        return Vec::new();
    }
    let mut visiting = HashSet::new();
    let mut selected = HashSet::new();
    let mut out = Vec::new();
    visit(state, target, limit, &mut visiting, &mut selected, &mut out);
    out
}

fn is_executed_snapshot(snapshot: &ExecSnapshot, txn_id: &TxnId) -> bool {
    let prefix = snapshot
        .executed_prefix_by_stream
        .get(&txn_progress_key(*txn_id))
        .copied()
        .unwrap_or(0);
    txn_seq(*txn_id) <= prefix || snapshot.executed_out_of_order.contains(txn_id)
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

impl Group {
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
            .executed_prefix_by_stream
            .iter()
            .map(|(stream, counter)| ExecutedPrefix {
                node_id: stream.node_id,
                epoch: stream.epoch,
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
            let stream = TxnProgressKey {
                node_id: p.node_id,
                epoch: p.epoch,
            };
            let entry = state.executed_prefix_by_stream.entry(stream).or_insert(0);
            if p.counter > *entry {
                *entry = p.counter;
                changed = true;
            }
        }
        if changed {
            let floors = state.executed_prefix_by_stream.clone();
            state.executed_out_of_order.retain(|txn_id| {
                let floor = floors.get(&txn_progress_key(*txn_id)).copied().unwrap_or(0);
                txn_seq(*txn_id) > floor
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
        let (command, seq) = {
            let state = self.state.lock().await;
            let Some(entry) = state.executed_log.get(&txn_id) else {
                return Ok(false);
            };
            if entry.visible {
                return Ok(true);
            }
            (entry.command.clone(), entry.seq)
        };

        let command = match command {
            Some(command) => command,
            None => self
                .load_command_from_commit_log(txn_id)
                .unwrap_or_default(),
        };

        if !command.is_empty() {
            self.sm.mark_visible(&command, ExecMeta { txn_id, seq })?;
        }

        let mut state = self.state.lock().await;
        if let Some(entry) = state.executed_log.get_mut(&txn_id) {
            entry.visible = true;
        }

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
                if entry.txn_id.node_id == self.config.node_id
                    && txn_epoch(entry.txn_id) == self.config.txn_epoch
                {
                    max_local_counter_seen = max_local_counter_seen.max(txn_seq(entry.txn_id));
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

    async fn gossip_executed_prefixes(&self) -> anyhow::Result<()> {
        let peers = self.peers_round_robin();
        if peers.is_empty() {
            return Ok(());
        }

        let prefixes = {
            let state = self.state.lock().await;
            state
                .executed_prefix_by_stream
                .iter()
                .map(|(stream, counter)| ExecutedPrefix {
                    node_id: stream.node_id,
                    epoch: stream.epoch,
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

    pub(super) fn maybe_compact_state_locked(state: &mut State, now: time::Instant) -> bool {
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

    pub(super) fn maybe_gc_executed_log_locked(
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

        let mut global_min_by_stream: HashMap<TxnProgressKey, u64> = HashMap::new();
        for (stream, local_prefix) in &state.executed_prefix_by_stream {
            let mut min_prefix = *local_prefix;
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
                let reported = peer_prefixes.get(stream).copied().unwrap_or(0);
                min_prefix = min_prefix.min(reported);
                if min_prefix == 0 {
                    break;
                }
            }

            global_min_by_stream.insert(*stream, min_prefix);
        }

        let mut removed = 0usize;
        // Bound per-tick work to avoid long mutex hold times on large logs.
        let gc_scan = state.executed_log_order.len().min(MAX_GC_SCAN);
        for _ in 0..gc_scan {
            let Some(id) = state.executed_log_order.pop_front() else {
                break;
            };
            let visible = state
                .executed_log
                .get(&id)
                .is_some_and(|entry| entry.visible);
            if visible {
                let min_prefix = global_min_by_stream
                    .get(&txn_progress_key(id))
                    .copied()
                    .unwrap_or(0);
                if txn_seq(id) <= min_prefix {
                    if let Some(entry) = state.executed_log.remove(&id) {
                        state.executed_log_bytes = state
                            .executed_log_bytes
                            .saturating_sub(entry.command.as_ref().map_or(0, Bytes::len));
                        state.executed_log_deps_total = state
                            .executed_log_deps_total
                            .saturating_sub(entry.deps.len());
                        state.remove_stable_key_indexes(id, &entry.keys);
                    }
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
                let visible = state
                    .executed_log
                    .get(&id)
                    .is_some_and(|entry| entry.visible);
                if visible {
                    let min_prefix = global_min_by_stream
                        .get(&txn_progress_key(id))
                        .copied()
                        .unwrap_or(0);
                    if txn_seq(id) > min_prefix {
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

    pub(super) async fn execute_until(&self, target: TxnId) -> anyhow::Result<()> {
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

            let progress = self.execute_target_progress(target).await?;
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
                        self.recover_txn(dep).await?;
                        continue;
                    }
                }

                let _ =
                    time::timeout(Duration::from_millis(1), self.executor_notify.notified()).await;
            }
        }
    }

    pub(super) async fn is_executed(&self, txn_id: TxnId) -> bool {
        let state = self.state.lock().await;
        state.is_executed(&txn_id)
            || state
                .records
                .get(&txn_id)
                .is_some_and(|r| r.status == Status::Executed)
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

    pub(super) async fn execute_progress(&self) -> anyhow::Result<bool> {
        let start = time::Instant::now();
        let res = self.execute_progress_inner(None).await;
        self.metrics
            .record_exec_progress(start.elapsed(), res.as_ref().ok().copied());
        res
    }

    /// Drive execution with a foreground target preference.
    ///
    /// Purpose:
    /// - Keep linearizable reads from waiting behind unrelated committed writes
    ///   when the read's own dependency path is already executable.
    ///
    /// Design:
    /// - Shares the normal executor implementation and metrics.
    /// - Supplies a priority target that is considered before the global
    ///   sequence-ordered ready queue.
    ///
    /// Inputs:
    /// - `target`: transaction whose caller is waiting.
    ///
    /// Outputs:
    /// - `Ok(true)` when any transaction executed.
    /// - `Ok(false)` when no safe progress was available.
    async fn execute_target_progress(&self, target: TxnId) -> anyhow::Result<bool> {
        let start = time::Instant::now();
        let res = self.execute_progress_inner(Some(target)).await;
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

    async fn execute_progress_inner(&self, priority_target: Option<TxnId>) -> anyhow::Result<bool> {
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

            if let Some(target) = priority_target {
                let target_path = pick_target_execution_path(&state, target, exec_batch_max);
                for id in &target_path {
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
                    picked = target_path;
                    used_ready = true;
                }
            }

            // Fast-path: consume ready-to-execute commits without scanning.
            if !used_ready && !state.committed_ready.is_empty() {
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
                        executed_prefix_by_stream: state
                            .executed_prefix_by_stream
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
                        executed_prefix_by_stream = ?state.executed_prefix_by_stream,
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
                        executed_prefix_by_stream = ?state.executed_prefix_by_stream,
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
                    executed_prefix_by_stream = ?state.executed_prefix_by_stream,
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
                            keys: item.keys.writes.clone(),
                            seq: rec.seq.max(1),
                            deps: rec.deps.into_iter().collect(),
                            visible: true,
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
}
