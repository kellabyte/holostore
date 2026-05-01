//! Runtime membership and identity helpers for an Accord group.
//!
//! Purpose:
//! - Keep quorum sizing, peer selection, and transaction/ballot identity helpers
//!   separate from protocol phase logic.
//!
//! Design:
//! - Runtime members and voters remain in the existing `RwLock`s owned by `Group`.
//! - Helper methods use snapshots so callers do not hold membership locks across
//!   asynchronous protocol work.
//!
//! Inputs:
//! - Runtime membership updates and local group configuration.
//!
//! Outputs:
//! - Member/voter snapshots, quorum sizes, transaction ids, and ballots.

use super::*;

impl Group {
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

    pub(super) fn voters_snapshot(&self) -> Vec<NodeId> {
        self.voters.read().map(|g| g.clone()).unwrap_or_default()
    }

    pub(super) fn local_is_member(&self) -> bool {
        let local = self.config.node_id;
        self.members
            .read()
            .map(|m| m.iter().any(|member| member.id == local))
            .unwrap_or(false)
    }

    pub(super) fn local_is_voter(&self) -> bool {
        let local = self.config.node_id;
        self.voters
            .read()
            .map(|voters| voters.contains(&local))
            .unwrap_or(false)
    }

    pub(super) fn peers_snapshot(&self) -> Vec<NodeId> {
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

    pub(super) fn voter_peers_snapshot(&self) -> Vec<NodeId> {
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

    pub(super) fn quorum(&self) -> usize {
        let n = self.voters.read().map(|m| m.len()).unwrap_or(0);
        (n / 2) + 1
    }

    pub(super) fn fast_quorum(&self) -> usize {
        self.quorum()
    }

    pub(super) fn config_with_runtime_voters(&self) -> Config {
        let mut cfg = self.config.clone();
        cfg.members = self
            .voters_snapshot()
            .into_iter()
            .map(|id| Member { id })
            .collect();
        cfg
    }

    pub(super) fn peers_round_robin(&self) -> Vec<NodeId> {
        let mut peers = self.voter_peers_snapshot();
        if peers.len() <= 1 {
            return peers;
        }
        let idx = (self.peer_rr.fetch_add(1, Ordering::Relaxed) as usize) % peers.len();
        peers.rotate_left(idx);
        peers
    }

    pub(super) fn compose_txn_id(&self, local_counter: u64) -> anyhow::Result<TxnId> {
        Ok(TxnId {
            node_id: self.config.node_id,
            counter: make_txn_counter(self.config.group_id, self.config.txn_epoch, local_counter)?,
        })
    }

    pub(super) async fn next_ballot_after(&self, after: Ballot) -> Ballot {
        let mut state = self.state.lock().await;
        state.next_ballot_counter = state.next_ballot_counter.max(after.counter);
        state.next_ballot_counter += 1;
        Ballot {
            counter: state.next_ballot_counter,
            node_id: self.config.node_id,
        }
    }
}
