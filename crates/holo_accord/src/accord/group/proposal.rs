//! Client proposal and quorum-round coordinator logic.
//!
//! Purpose:
//! - Drive PreAccept, optional Accept, Commit, and fast-path dissemination for
//!   client-submitted commands.
//!
//! Design:
//! - The public `Handle` forwards into private `Group` proposal methods.
//! - Quorum rounds keep the original early-return behavior by dropping slow
//!   tail futures after quorum has been reached.
//!
//! Inputs:
//! - Client commands, optional read dependencies, runtime peer snapshots, and
//!   transport responses.
//!
//! Outputs:
//! - Applied write acknowledgements, read results, and phase timing samples.

use super::metrics::{CommitRoundTimings, PhaseTimings};
use super::recovery::RecoveryValue;
use super::*;

#[derive(Debug)]
pub(super) enum ProposeOnceError {
    Rejected { promised: Ballot },
    NoQuorum(anyhow::Error),
}

impl From<anyhow::Error> for ProposeOnceError {
    fn from(value: anyhow::Error) -> Self {
        Self::NoQuorum(value)
    }
}

pub(super) fn merge_preaccept(oks: &[PreAcceptResponse]) -> RecoveryValue {
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

impl Group {
    pub fn handle(self: &Arc<Self>) -> Handle {
        Handle {
            group: self.clone(),
        }
    }

    pub(super) async fn propose(
        self: &Arc<Self>,
        command: Bytes,
    ) -> anyhow::Result<ProposalResult> {
        self.propose_with_deps(command, None).await
    }

    pub(super) async fn propose_read_with_deps(
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
            if let Ok(r) = resp {
                max_promised = max_promised.max(r.promised);
                if r.ok {
                    oks.push(r);
                    ok += 1;
                }
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
    #[allow(clippy::too_many_arguments)]
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
    #[allow(clippy::too_many_arguments)]
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
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_accept_round(
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
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_commit_round(
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
}
