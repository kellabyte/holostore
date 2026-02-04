//! Accord module wiring.
//!
//! `group` contains the consensus engine and executor, `state` holds the
//! in-memory indexes and queues, and `types` defines the shared request/response
//! and trait contracts (transport, commit log, state machine).

mod group;
mod state;
mod types;

pub use group::{DebugStats, Group, Handle, ProposalResult};
pub use types::{
    txn_group_id, AcceptRequest, AcceptResponse, Ballot, CommandKeys, CommitLog, CommitLogEntry,
    CommitRequest, CommitResponse, Config, ExecMeta, ExecutedPrefix, GroupId, Member, NodeId,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse, ReportExecutedRequest,
    ReportExecutedResponse, StateMachine, Transport, TxnId, TxnStatus, TXN_COUNTER_SHARD_SHIFT,
};
