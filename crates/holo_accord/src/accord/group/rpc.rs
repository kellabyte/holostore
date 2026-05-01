//! Peer RPC handlers for the Accord group protocol.
//!
//! Purpose:
//! - Handle inbound PreAccept, Accept, Commit, Recover, command-fetch, and
//!   executed-prefix reports.
//!
//! Design:
//! - Handlers mutate the same group state as coordinator paths, preserving the
//!   original locking and durability order.
//!
//! Inputs:
//! - Protocol request structs from the transport layer.
//!
//! Outputs:
//! - Protocol response structs plus local state/WAL updates.

use super::metrics::CommitRpcTimings;
use super::*;

impl Group {
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
    pub(super) async fn rpc_commit_with_timings(
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

    pub(super) fn load_command_from_commit_log(&self, txn_id: TxnId) -> Option<Bytes> {
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

    pub(super) async fn fetch_command_from_peers(
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
            prefixes.insert(
                TxnProgressKey {
                    node_id: p.node_id,
                    epoch: p.epoch,
                },
                p.counter,
            );
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
}
