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

mod execution;
mod graph;
mod membership;
mod metrics;
mod proposal;
mod recovery;
mod rpc;
mod workers;

#[cfg(test)]
mod tests;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::sync::{mpsc, oneshot, Mutex, Notify};
use tokio::time;

use super::state::{
    is_monotonic_update, status_to_txn_status, ExecutedLogEntry, Record, State, Status,
};
use super::types::{
    make_txn_counter, txn_epoch, txn_group_id, txn_progress_key, txn_seq, AcceptRequest,
    AcceptResponse, Ballot, CommandKeys, CommitLog, CommitLogAppendOptions, CommitLogEntry,
    CommitRequest, CommitResponse, Config, ExecMeta, ExecutedPrefix, Member, NodeId,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse, ReportExecutedRequest,
    ReportExecutedResponse, StateMachine, Transport, TxnId, TxnProgressKey, TxnStatus,
};

pub use metrics::{CommitQuorumCloserStat, DebugStats};

const COMPACT_EVERY_APPLIED: u64 = 1024;
const COMPACT_MAX_DELETE: usize = 4096;

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
    commit_log_tx: Option<std_mpsc::Sender<workers::CommitLogWork>>,
    apply_tx: Option<std_mpsc::Sender<workers::ApplyWork>>,
    state: Mutex<State>,
    execute_lock: Mutex<()>,
    executor_notify: Notify,
    executor_started: AtomicBool,
    metrics: Arc<metrics::GroupMetrics>,
    compact_counter: AtomicU64,
    peer_rr: AtomicU64,
    start_at: time::Instant,
}
