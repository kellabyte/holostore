//! Accord module wiring.
//!
//! `group` contains the consensus engine and executor, `state` holds the
//! in-memory indexes and queues, and `types` defines the shared request/response
//! and trait contracts (transport, commit log, state machine).

mod group;
mod state;
mod types;

pub use group::{CommitQuorumCloserStat, DebugStats, Group, Handle, ProposalResult};
pub use types::{
    make_txn_counter, txn_epoch, txn_group_id, txn_id_with_seq, txn_progress_key, txn_seq,
    AcceptRequest, AcceptResponse, Ballot, CommandKeys, CommitDurabilityMode, CommitLog,
    CommitLogAppendOptions, CommitLogCheckpointStatus, CommitLogEntry, CommitRequest,
    CommitResponse, Config, ExecMeta, ExecutedPrefix, GroupId, Member, NodeId, PreAcceptRequest,
    PreAcceptResponse, RecoverRequest, RecoverResponse, ReportExecutedRequest,
    ReportExecutedResponse, StateMachine, Transport, TxnId, TxnProgressKey, TxnStatus,
    MAX_TXN_EPOCH, MAX_TXN_GROUP_ID, MAX_TXN_SEQ, TXN_COUNTER_EPOCH_BITS, TXN_COUNTER_SEQ_BITS,
    TXN_COUNTER_SHARD_SHIFT,
};
