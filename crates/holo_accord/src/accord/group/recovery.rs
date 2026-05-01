//! Recovery target selection and transaction recovery rounds.
//!
//! Purpose:
//! - Repair missing or stalled transactions so execution can continue safely.
//!
//! Design:
//! - Recovery keeps the previous quorum merge rules and delegates Accept/Commit
//!   rounds back to the proposal module helpers on `Group`.
//!
//! Inputs:
//! - Blocked transaction ids, Recover replies, and command fetch responses.
//!
//! Outputs:
//! - Recommitted values or noops, plus recovery counters in group state.

use super::graph::{build_blocking_chain, pick_recovery_from_chain};
use super::proposal::ProposeOnceError;
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RecoveryKind {
    Value,
    Noop,
}

#[derive(Debug)]
pub(super) struct RecoveryValue {
    pub(super) command: Bytes,
    pub(super) seq: u64,
    pub(super) deps: Vec<TxnId>,
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
pub(super) enum RecoveryChoice {
    Ready(RecoveryValue),
    MissingCommittedCommand { digest: [u8; 32] },
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
pub(super) fn choose_recovery_value(replies: &[RecoverResponse]) -> anyhow::Result<RecoveryChoice> {
    let has_committed_value = replies
        .iter()
        .any(|r| matches!(r.status, TxnStatus::Committed | TxnStatus::Executed));
    let mut command: Option<Bytes> = None;
    let mut digest: Option<[u8; 32]> = None;
    let mut seq = 0u64;
    let mut deps = BTreeSet::new();

    for r in replies.iter().filter(|r| {
        !has_committed_value || matches!(r.status, TxnStatus::Committed | TxnStatus::Executed)
    }) {
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

    if command.is_none() && has_committed_value {
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

impl Group {
    /// Returns `true` when dependency waiting has exceeded configured recovery delay.
    ///
    /// Input:
    /// - `txn_id`: blocked transaction candidate.
    /// - `waiting_since`: when this executor wait cycle began.
    ///
    /// Output:
    /// - `true` when recovery should be attempted now.
    /// - `false` when transaction is already executed or still within grace window.
    pub(super) async fn should_recover(&self, txn_id: TxnId, waiting_since: time::Instant) -> bool {
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

    pub(super) async fn executor_recover_once(&self) -> anyhow::Result<bool> {
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

    pub(super) async fn find_recovery_target(&self, txn_id: TxnId) -> Option<TxnId> {
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

    pub(super) async fn recover_txn(&self, txn_id: TxnId) -> anyhow::Result<()> {
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

    pub(super) fn record_recovery_attempt(
        &self,
        state: &mut State,
        txn_id: TxnId,
        now: time::Instant,
    ) -> bool {
        let min_interval = self.recovery_retry_interval();
        if let Some(last) = state.recovery_last_attempt.get(&txn_id) {
            if now.duration_since(*last) < min_interval {
                return false;
            }
        }
        state.recovery_last_attempt.insert(txn_id, now);
        true
    }

    /// Minimum per-transaction spacing between recovery attempts.
    ///
    /// This keeps duplicate executor probes bounded, while avoiding a fixed
    /// one-second hole in the latency tail after a transient recovery failure.
    fn recovery_retry_interval(&self) -> Duration {
        self.config
            .stall_recover_interval
            .min(Duration::from_millis(250))
            .max(Duration::from_millis(50))
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
                if let Ok(r) = resp {
                    max_promised = max_promised.max(r.promised);
                    if r.ok {
                        replies.push(r);
                        ok += 1;
                        if ok >= quorum {
                            break;
                        }
                    }
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
}
