//! gRPC service handlers that adapt network requests into Accord operations.
//!
//! This module is the server-side counterpart to `transport.rs`, translating
//! protobuf messages into local Accord types and returning responses.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use holo_accord::accord::{self, ExecutedPrefix, NodeId};
use serde_json;

use crate::cluster::{ClusterCommand, MemberState, ShardDesc};
use crate::kv::Version;
use crate::volo_gen::holo_store::rpc;
use crate::NodeState;

/// gRPC service implementation backed by a shared `NodeState`.
#[derive(Clone)]
pub struct RpcService {
    pub state: Arc<NodeState>,
}

impl RpcService {
    async fn maybe_delay(&self) {
        let delay = self.state.rpc_handler_delay;
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
    }
}

impl rpc::HoloRpc for RpcService {
    /// Handle a single pre-accept RPC request.
    async fn pre_accept(
        &self,
        req: volo_grpc::Request<rpc::PreAcceptRequest>,
    ) -> Result<volo_grpc::Response<rpc::PreAcceptResponse>, volo_grpc::Status> {
        self.maybe_delay().await;
        let start = std::time::Instant::now();
        let _inflight = self.state.rpc_handler_stats.track_pre_accept();
        let req = req.into_inner();
        let group = self
            .state
            .group(req.group_id)
            .ok_or_else(|| volo_grpc::Status::not_found("unknown group"))?;

        let txn_id = req
            .txn_id
            .ok_or_else(|| volo_grpc::Status::invalid_argument("missing txn_id"))?;

        let ballot = req
            .ballot
            .ok_or_else(|| volo_grpc::Status::invalid_argument("missing ballot"))?;

        let deps = req
            .deps
            .into_iter()
            .map(|d| accord::TxnId {
                node_id: d.node_id,
                counter: d.counter,
            })
            .collect();

        let resp = group
            .rpc_pre_accept(accord::PreAcceptRequest {
                group_id: req.group_id,
                txn_id: accord::TxnId {
                    node_id: txn_id.node_id,
                    counter: txn_id.counter,
                },
                ballot: accord::Ballot {
                    counter: ballot.counter,
                    node_id: ballot.node_id,
                },
                command: req.command.to_vec(),
                seq: req.seq,
                deps,
            })
            .await;
        let us = start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        self.state.rpc_handler_stats.record_pre_accept(us);

        let deps = resp
            .deps
            .into_iter()
            .map(|d| rpc::TxnId {
                node_id: d.node_id,
                counter: d.counter,
            })
            .collect();

        Ok(volo_grpc::Response::new(rpc::PreAcceptResponse {
            ok: resp.ok,
            seq: resp.seq,
            deps,
            promised: Some(rpc::Ballot {
                counter: resp.promised.counter,
                node_id: resp.promised.node_id,
            }),
        }))
    }

    /// Handle a batched pre-accept RPC request.
    async fn pre_accept_batch(
        &self,
        req: volo_grpc::Request<rpc::PreAcceptBatchRequest>,
    ) -> Result<volo_grpc::Response<rpc::PreAcceptBatchResponse>, volo_grpc::Status> {
        self.maybe_delay().await;
        let start = std::time::Instant::now();
        let _inflight = self.state.rpc_handler_stats.track_pre_accept();
        let req = req.into_inner();
        let mut responses = Vec::with_capacity(req.requests.len());

        for item in req.requests {
            let group = self
                .state
                .group(item.group_id)
                .ok_or_else(|| volo_grpc::Status::not_found("unknown group"))?;

            let txn_id = item
                .txn_id
                .ok_or_else(|| volo_grpc::Status::invalid_argument("missing txn_id"))?;

            let ballot = item
                .ballot
                .ok_or_else(|| volo_grpc::Status::invalid_argument("missing ballot"))?;

            let deps = item
                .deps
                .into_iter()
                .map(|d| accord::TxnId {
                    node_id: d.node_id,
                    counter: d.counter,
                })
                .collect();

            let resp = group
                .rpc_pre_accept(accord::PreAcceptRequest {
                    group_id: item.group_id,
                    txn_id: accord::TxnId {
                        node_id: txn_id.node_id,
                        counter: txn_id.counter,
                    },
                    ballot: accord::Ballot {
                        counter: ballot.counter,
                        node_id: ballot.node_id,
                    },
                    command: item.command.to_vec(),
                    seq: item.seq,
                    deps,
                })
                .await;

            let deps = resp
                .deps
                .into_iter()
                .map(|d| rpc::TxnId {
                    node_id: d.node_id,
                    counter: d.counter,
                })
                .collect();

            responses.push(rpc::PreAcceptResponse {
                ok: resp.ok,
                seq: resp.seq,
                deps,
                promised: Some(rpc::Ballot {
                    counter: resp.promised.counter,
                    node_id: resp.promised.node_id,
                }),
            });
        }

        let resp = volo_grpc::Response::new(rpc::PreAcceptBatchResponse { responses });
        let us = start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        self.state.rpc_handler_stats.record_pre_accept_batch(us);
        Ok(resp)
    }

    /// Handle a single accept RPC request.
    async fn accept(
        &self,
        req: volo_grpc::Request<rpc::AcceptRequest>,
    ) -> Result<volo_grpc::Response<rpc::AcceptResponse>, volo_grpc::Status> {
        self.maybe_delay().await;
        let start = std::time::Instant::now();
        let _inflight = self.state.rpc_handler_stats.track_accept();
        let req = req.into_inner();
        let group = self
            .state
            .group(req.group_id)
            .ok_or_else(|| volo_grpc::Status::not_found("unknown group"))?;

        let txn_id = req
            .txn_id
            .ok_or_else(|| volo_grpc::Status::invalid_argument("missing txn_id"))?;

        let ballot = req
            .ballot
            .ok_or_else(|| volo_grpc::Status::invalid_argument("missing ballot"))?;

        let deps = req
            .deps
            .into_iter()
            .map(|d| accord::TxnId {
                node_id: d.node_id,
                counter: d.counter,
            })
            .collect();
        // Decide whether a command payload is present.
        let has_command = req.has_command || !req.command.is_empty();
        let command_digest = parse_command_digest(req.command_digest)?;
        let command = if has_command {
            req.command.to_vec()
        } else {
            Vec::new()
        };

        let resp = group
            .rpc_accept(accord::AcceptRequest {
                group_id: req.group_id,
                txn_id: accord::TxnId {
                    node_id: txn_id.node_id,
                    counter: txn_id.counter,
                },
                ballot: accord::Ballot {
                    counter: ballot.counter,
                    node_id: ballot.node_id,
                },
                command,
                command_digest,
                has_command,
                seq: req.seq,
                deps,
            })
            .await;

        let resp = volo_grpc::Response::new(rpc::AcceptResponse {
            ok: resp.ok,
            promised: Some(rpc::Ballot {
                counter: resp.promised.counter,
                node_id: resp.promised.node_id,
            }),
        });
        let us = start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        self.state.rpc_handler_stats.record_accept(us);
        Ok(resp)
    }

    /// Handle a batched accept RPC request.
    async fn accept_batch(
        &self,
        req: volo_grpc::Request<rpc::AcceptBatchRequest>,
    ) -> Result<volo_grpc::Response<rpc::AcceptBatchResponse>, volo_grpc::Status> {
        self.maybe_delay().await;
        let start = std::time::Instant::now();
        let _inflight = self.state.rpc_handler_stats.track_accept();
        let req = req.into_inner();
        let mut responses = Vec::with_capacity(req.requests.len());

        for item in req.requests {
            let group = self
                .state
                .group(item.group_id)
                .ok_or_else(|| volo_grpc::Status::not_found("unknown group"))?;

            let txn_id = item
                .txn_id
                .ok_or_else(|| volo_grpc::Status::invalid_argument("missing txn_id"))?;

            let ballot = item
                .ballot
                .ok_or_else(|| volo_grpc::Status::invalid_argument("missing ballot"))?;

            let deps = item
                .deps
                .into_iter()
                .map(|d| accord::TxnId {
                    node_id: d.node_id,
                    counter: d.counter,
                })
                .collect();
            // Decide whether a command payload is present.
            let has_command = item.has_command || !item.command.is_empty();
            let command_digest = parse_command_digest(item.command_digest)?;
            let command = if has_command {
                item.command.to_vec()
            } else {
                Vec::new()
            };

            let resp = group
                .rpc_accept(accord::AcceptRequest {
                    group_id: item.group_id,
                    txn_id: accord::TxnId {
                        node_id: txn_id.node_id,
                        counter: txn_id.counter,
                    },
                    ballot: accord::Ballot {
                        counter: ballot.counter,
                        node_id: ballot.node_id,
                    },
                    command,
                    command_digest,
                    has_command,
                    seq: item.seq,
                    deps,
                })
                .await;

            responses.push(rpc::AcceptResponse {
                ok: resp.ok,
                promised: Some(rpc::Ballot {
                    counter: resp.promised.counter,
                    node_id: resp.promised.node_id,
                }),
            });
        }

        let resp = volo_grpc::Response::new(rpc::AcceptBatchResponse { responses });
        let us = start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        self.state.rpc_handler_stats.record_accept_batch(us);
        Ok(resp)
    }

    /// Handle a single commit RPC request.
    async fn commit(
        &self,
        req: volo_grpc::Request<rpc::CommitRequest>,
    ) -> Result<volo_grpc::Response<rpc::CommitResponse>, volo_grpc::Status> {
        self.maybe_delay().await;
        let start = std::time::Instant::now();
        let _inflight = self.state.rpc_handler_stats.track_commit();
        let req = req.into_inner();
        let group = self
            .state
            .group(req.group_id)
            .ok_or_else(|| volo_grpc::Status::not_found("unknown group"))?;

        let txn_id = req
            .txn_id
            .ok_or_else(|| volo_grpc::Status::invalid_argument("missing txn_id"))?;

        let ballot = req
            .ballot
            .ok_or_else(|| volo_grpc::Status::invalid_argument("missing ballot"))?;

        let deps = req
            .deps
            .into_iter()
            .map(|d| accord::TxnId {
                node_id: d.node_id,
                counter: d.counter,
            })
            .collect();
        // Decide whether a command payload is present.
        let has_command = req.has_command || !req.command.is_empty();
        let command_digest = parse_command_digest(req.command_digest)?;
        let command = if has_command {
            req.command.to_vec()
        } else {
            Vec::new()
        };

        let resp = group
            .rpc_commit(accord::CommitRequest {
                group_id: req.group_id,
                txn_id: accord::TxnId {
                    node_id: txn_id.node_id,
                    counter: txn_id.counter,
                },
                ballot: accord::Ballot {
                    counter: ballot.counter,
                    node_id: ballot.node_id,
                },
                command,
                command_digest,
                has_command,
                seq: req.seq,
                deps,
            })
            .await;

        let resp = volo_grpc::Response::new(rpc::CommitResponse { ok: resp.ok });
        let us = start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        self.state.rpc_handler_stats.record_commit(us);
        Ok(resp)
    }

    /// Handle a batched commit RPC request.
    async fn commit_batch(
        &self,
        req: volo_grpc::Request<rpc::CommitBatchRequest>,
    ) -> Result<volo_grpc::Response<rpc::CommitBatchResponse>, volo_grpc::Status> {
        self.maybe_delay().await;
        let start = std::time::Instant::now();
        let _inflight = self.state.rpc_handler_stats.track_commit();
        let req = req.into_inner();
        let mut responses = Vec::with_capacity(req.requests.len());

        for item in req.requests {
            let group = self
                .state
                .group(item.group_id)
                .ok_or_else(|| volo_grpc::Status::not_found("unknown group"))?;

            let txn_id = item
                .txn_id
                .ok_or_else(|| volo_grpc::Status::invalid_argument("missing txn_id"))?;

            let ballot = item
                .ballot
                .ok_or_else(|| volo_grpc::Status::invalid_argument("missing ballot"))?;

            let deps = item
                .deps
                .into_iter()
                .map(|d| accord::TxnId {
                    node_id: d.node_id,
                    counter: d.counter,
                })
                .collect();
            // Decide whether a command payload is present.
            let has_command = item.has_command || !item.command.is_empty();
            let command_digest = parse_command_digest(item.command_digest)?;
            let command = if has_command {
                item.command.to_vec()
            } else {
                Vec::new()
            };

            let resp = group
                .rpc_commit(accord::CommitRequest {
                    group_id: item.group_id,
                    txn_id: accord::TxnId {
                        node_id: txn_id.node_id,
                        counter: txn_id.counter,
                    },
                    ballot: accord::Ballot {
                        counter: ballot.counter,
                        node_id: ballot.node_id,
                    },
                    command,
                    command_digest,
                    has_command,
                    seq: item.seq,
                    deps,
                })
                .await;

            responses.push(rpc::CommitResponse { ok: resp.ok });
        }

        let resp = volo_grpc::Response::new(rpc::CommitBatchResponse { responses });
        let us = start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        self.state.rpc_handler_stats.record_commit_batch(us);
        Ok(resp)
    }

    /// Handle a single recover RPC request.
    async fn recover(
        &self,
        req: volo_grpc::Request<rpc::RecoverRequest>,
    ) -> Result<volo_grpc::Response<rpc::RecoverResponse>, volo_grpc::Status> {
        self.maybe_delay().await;
        let req = req.into_inner();
        let group = self
            .state
            .group(req.group_id)
            .ok_or_else(|| volo_grpc::Status::not_found("unknown group"))?;

        let txn_id = req
            .txn_id
            .ok_or_else(|| volo_grpc::Status::invalid_argument("missing txn_id"))?;

        let ballot = req
            .ballot
            .ok_or_else(|| volo_grpc::Status::invalid_argument("missing ballot"))?;

        let resp = group
            .rpc_recover(accord::RecoverRequest {
                group_id: req.group_id,
                txn_id: accord::TxnId {
                    node_id: txn_id.node_id,
                    counter: txn_id.counter,
                },
                ballot: accord::Ballot {
                    counter: ballot.counter,
                    node_id: ballot.node_id,
                },
            })
            .await;

        let deps = resp
            .deps
            .into_iter()
            .map(|d| rpc::TxnId {
                node_id: d.node_id,
                counter: d.counter,
            })
            .collect();

        let status = match resp.status {
            // Map local status into the wire enum.
            accord::TxnStatus::Unknown => rpc::TxnStatus::TXN_STATUS_UNKNOWN,
            accord::TxnStatus::PreAccepted => rpc::TxnStatus::TXN_STATUS_PREACCEPTED,
            accord::TxnStatus::Accepted => rpc::TxnStatus::TXN_STATUS_ACCEPTED,
            accord::TxnStatus::Committed => rpc::TxnStatus::TXN_STATUS_COMMITTED,
            accord::TxnStatus::Executed => rpc::TxnStatus::TXN_STATUS_EXECUTED,
        };

        let accepted_ballot = resp.accepted_ballot.map(|b| rpc::Ballot {
            counter: b.counter,
            node_id: b.node_id,
        });

        Ok(volo_grpc::Response::new(rpc::RecoverResponse {
            ok: resp.ok,
            promised: Some(rpc::Ballot {
                counter: resp.promised.counter,
                node_id: resp.promised.node_id,
            }),
            status,
            accepted_ballot,
            command: resp.command.into(),
            seq: resp.seq,
            deps,
        }))
    }

    /// Handle a batched recover RPC request.
    async fn recover_batch(
        &self,
        req: volo_grpc::Request<rpc::RecoverBatchRequest>,
    ) -> Result<volo_grpc::Response<rpc::RecoverBatchResponse>, volo_grpc::Status> {
        self.maybe_delay().await;
        let req = req.into_inner();
        let mut responses = Vec::with_capacity(req.requests.len());

        for item in req.requests {
            let group = self
                .state
                .group(item.group_id)
                .ok_or_else(|| volo_grpc::Status::not_found("unknown group"))?;

            let txn_id = item
                .txn_id
                .ok_or_else(|| volo_grpc::Status::invalid_argument("missing txn_id"))?;

            let ballot = item
                .ballot
                .ok_or_else(|| volo_grpc::Status::invalid_argument("missing ballot"))?;

            let resp = group
                .rpc_recover(accord::RecoverRequest {
                    group_id: item.group_id,
                    txn_id: accord::TxnId {
                        node_id: txn_id.node_id,
                        counter: txn_id.counter,
                    },
                    ballot: accord::Ballot {
                        counter: ballot.counter,
                        node_id: ballot.node_id,
                    },
                })
                .await;

            let deps = resp
                .deps
                .into_iter()
                .map(|d| rpc::TxnId {
                    node_id: d.node_id,
                    counter: d.counter,
                })
                .collect();

            let status = match resp.status {
                // Map local status into the wire enum.
                accord::TxnStatus::Unknown => rpc::TxnStatus::TXN_STATUS_UNKNOWN,
                accord::TxnStatus::PreAccepted => rpc::TxnStatus::TXN_STATUS_PREACCEPTED,
                accord::TxnStatus::Accepted => rpc::TxnStatus::TXN_STATUS_ACCEPTED,
                accord::TxnStatus::Committed => rpc::TxnStatus::TXN_STATUS_COMMITTED,
                accord::TxnStatus::Executed => rpc::TxnStatus::TXN_STATUS_EXECUTED,
            };

            let accepted_ballot = resp.accepted_ballot.map(|b| rpc::Ballot {
                counter: b.counter,
                node_id: b.node_id,
            });

            responses.push(rpc::RecoverResponse {
                ok: resp.ok,
                promised: Some(rpc::Ballot {
                    counter: resp.promised.counter,
                    node_id: resp.promised.node_id,
                }),
                status,
                accepted_ballot,
                command: resp.command.into(),
                seq: resp.seq,
                deps,
            });
        }

        Ok(volo_grpc::Response::new(rpc::RecoverBatchResponse {
            responses,
        }))
    }

    /// Handle a fetch_command RPC request.
    async fn fetch_command(
        &self,
        req: volo_grpc::Request<rpc::FetchCommandRequest>,
    ) -> Result<volo_grpc::Response<rpc::FetchCommandResponse>, volo_grpc::Status> {
        self.maybe_delay().await;
        let req = req.into_inner();
        let group = self
            .state
            .group(req.group_id)
            .ok_or_else(|| volo_grpc::Status::not_found("unknown group"))?;
        let txn_id = req
            .txn_id
            .ok_or_else(|| volo_grpc::Status::invalid_argument("missing txn_id"))?;
        let command = group
            .rpc_fetch_command(accord::TxnId {
                node_id: txn_id.node_id,
                counter: txn_id.counter,
            })
            .await;
        // Return a "has_command" flag so callers can distinguish empty payloads.
        let (has_command, command) = match command {
            Some(cmd) => (true, cmd.into()),
            None => (false, Bytes::new()),
        };
        Ok(volo_grpc::Response::new(rpc::FetchCommandResponse {
            has_command,
            command,
        }))
    }

    /// Handle a report_executed RPC request.
    async fn report_executed(
        &self,
        req: volo_grpc::Request<rpc::ReportExecutedRequest>,
    ) -> Result<volo_grpc::Response<rpc::ReportExecutedResponse>, volo_grpc::Status> {
        self.maybe_delay().await;
        let req = req.into_inner();
        let group = self
            .state
            .group(req.group_id)
            .ok_or_else(|| volo_grpc::Status::not_found("unknown group"))?;

        let prefixes = req
            .prefixes
            .into_iter()
            .map(|p| accord::ExecutedPrefix {
                node_id: p.node_id,
                counter: p.counter,
            })
            .collect();

        let resp = group
            .rpc_report_executed(accord::ReportExecutedRequest {
                group_id: req.group_id,
                from_node_id: req.from_node_id,
                prefixes,
            })
            .await;

        Ok(volo_grpc::Response::new(rpc::ReportExecutedResponse {
            ok: resp.ok,
        }))
    }

    /// Handle a last_committed RPC request.
    async fn last_committed(
        &self,
        req: volo_grpc::Request<rpc::LastCommittedRequest>,
    ) -> Result<volo_grpc::Response<rpc::LastCommittedResponse>, volo_grpc::Status> {
        self.maybe_delay().await;
        let req = req.into_inner();
        let group = self
            .state
            .group(req.group_id)
            .ok_or_else(|| volo_grpc::Status::not_found("unknown group"))?;

        let keys = req.keys.into_iter().map(|k| k.to_vec()).collect::<Vec<_>>();
        let results = group.last_committed_for_keys(&keys).await;
        let mut items = Vec::with_capacity(keys.len());
        for (key, item) in keys.into_iter().zip(results.into_iter()) {
            if let Some((txn_id, seq)) = item {
                items.push(rpc::LastCommittedItem {
                    key: key.into(),
                    present: true,
                    txn_id: Some(rpc::TxnId {
                        node_id: txn_id.node_id,
                        counter: txn_id.counter,
                    }),
                    seq,
                });
            } else {
                // Encode missing values explicitly.
                items.push(rpc::LastCommittedItem {
                    key: key.into(),
                    present: false,
                    txn_id: None,
                    seq: 0,
                });
            }
        }

        Ok(volo_grpc::Response::new(rpc::LastCommittedResponse {
            items,
        }))
    }

    /// Handle a last_executed_prefix RPC request.
    async fn last_executed_prefix(
        &self,
        req: volo_grpc::Request<rpc::LastExecutedPrefixRequest>,
    ) -> Result<volo_grpc::Response<rpc::LastExecutedPrefixResponse>, volo_grpc::Status> {
        self.maybe_delay().await;
        let req = req.into_inner();
        let group = self
            .state
            .group(req.group_id)
            .ok_or_else(|| volo_grpc::Status::not_found("unknown group"))?;
        let prefixes = group.executed_prefixes().await;
        let prefixes = prefixes
            .into_iter()
            .map(|p| rpc::ExecutedPrefix {
                node_id: p.node_id,
                counter: p.counter,
            })
            .collect();
        Ok(volo_grpc::Response::new(rpc::LastExecutedPrefixResponse {
            prefixes,
        }))
    }

    /// Handle a seed_executed_prefix RPC request.
    async fn seed_executed_prefix(
        &self,
        req: volo_grpc::Request<rpc::SeedExecutedPrefixRequest>,
    ) -> Result<volo_grpc::Response<rpc::SeedExecutedPrefixResponse>, volo_grpc::Status> {
        self.maybe_delay().await;
        let req = req.into_inner();
        let group = self
            .state
            .group(req.group_id)
            .ok_or_else(|| volo_grpc::Status::not_found("unknown group"))?;
        let prefixes = req
            .prefixes
            .into_iter()
            .map(|p| accord::ExecutedPrefix {
                node_id: p.node_id,
                counter: p.counter,
            })
            .collect::<Vec<_>>();
        group.seed_executed_prefixes(&prefixes).await;
        Ok(volo_grpc::Response::new(rpc::SeedExecutedPrefixResponse {
            ok: true,
        }))
    }

    /// Handle an executed RPC request.
    async fn executed(
        &self,
        req: volo_grpc::Request<rpc::ExecutedRequest>,
    ) -> Result<volo_grpc::Response<rpc::ExecutedResponse>, volo_grpc::Status> {
        self.maybe_delay().await;
        let req = req.into_inner();
        let group = self
            .state
            .group(req.group_id)
            .ok_or_else(|| volo_grpc::Status::not_found("unknown group"))?;
        let txn_id = req
            .txn_id
            .ok_or_else(|| volo_grpc::Status::invalid_argument("missing txn_id"))?;
        let executed = group
            .executed(accord::TxnId {
                node_id: txn_id.node_id,
                counter: txn_id.counter,
            })
            .await;
        Ok(volo_grpc::Response::new(rpc::ExecutedResponse { executed }))
    }

    /// Handle a mark_visible RPC request.
    async fn mark_visible(
        &self,
        req: volo_grpc::Request<rpc::MarkVisibleRequest>,
    ) -> Result<volo_grpc::Response<rpc::MarkVisibleResponse>, volo_grpc::Status> {
        self.maybe_delay().await;
        let req = req.into_inner();
        let group = self
            .state
            .group(req.group_id)
            .ok_or_else(|| volo_grpc::Status::not_found("unknown group"))?;
        let txn_id = req
            .txn_id
            .ok_or_else(|| volo_grpc::Status::invalid_argument("missing txn_id"))?;
        let ok = group
            .mark_visible(accord::TxnId {
                node_id: txn_id.node_id,
                counter: txn_id.counter,
            })
            .await
            .map_err(|err| volo_grpc::Status::internal(err.to_string()))?;
        Ok(volo_grpc::Response::new(rpc::MarkVisibleResponse { ok }))
    }

    /// Handle a local KV GET RPC (non-consensus, used for quorum reads).
    async fn kv_get(
        &self,
        req: volo_grpc::Request<rpc::KvGetRequest>,
    ) -> Result<volo_grpc::Response<rpc::KvGetResponse>, volo_grpc::Status> {
        self.maybe_delay().await;
        let req = req.into_inner();
        let resp = match self.state.kv_latest(&req.key) {
            Some((value, version)) => rpc::KvGetResponse {
                has_value: true,
                value: value.into(),
                version: Some(to_rpc_version(version)),
            },
            None => rpc::KvGetResponse {
                has_value: false,
                value: Bytes::new(),
                version: Some(to_rpc_version(Version::zero())),
            },
        };
        Ok(volo_grpc::Response::new(resp))
    }

    /// Handle a local KV batch GET RPC (non-consensus, used for quorum reads).
    async fn kv_batch_get(
        &self,
        req: volo_grpc::Request<rpc::KvBatchGetRequest>,
    ) -> Result<volo_grpc::Response<rpc::KvBatchGetResponse>, volo_grpc::Status> {
        self.maybe_delay().await;
        let req = req.into_inner();
        let keys = req.keys.into_iter().map(|k| k.to_vec()).collect::<Vec<_>>();
        let values = self.state.kv_latest_batch(&keys);
        let mut responses = Vec::with_capacity(values.len());
        for item in values {
            match item {
                Some((value, version)) => responses.push(rpc::KvGetResponse {
                    has_value: true,
                    value: value.into(),
                    version: Some(to_rpc_version(version)),
                }),
                None => responses.push(rpc::KvGetResponse {
                    has_value: false,
                    value: Bytes::new(),
                    version: Some(to_rpc_version(Version::zero())),
                }),
            }
        }
        Ok(volo_grpc::Response::new(rpc::KvBatchGetResponse {
            responses,
        }))
    }

    /// Handle a join RPC, returning the initial membership string.
    async fn join(
        &self,
        req: volo_grpc::Request<rpc::JoinRequest>,
    ) -> Result<volo_grpc::Response<rpc::JoinResponse>, volo_grpc::Status> {
        self.maybe_delay().await;
        let req = req.into_inner();
        let cmd = crate::cluster::ClusterCommand::AddNode {
            node_id: req.node_id,
            grpc_addr: req.grpc_addr.to_string(),
            redis_addr: req.redis_addr.to_string(),
        };
        self.state
            .propose_meta_command(cmd)
            .await
            .map_err(|e| volo_grpc::Status::internal(format!("meta propose failed: {e}")))?;
        Ok(volo_grpc::Response::new(rpc::JoinResponse {
            initial_members: self.state.cluster_store.members_string().into(),
        }))
    }

    async fn cluster_state(
        &self,
        _req: volo_grpc::Request<rpc::ClusterStateRequest>,
    ) -> Result<volo_grpc::Response<rpc::ClusterStateResponse>, volo_grpc::Status> {
        let state = self.state.cluster_store.state();
        let ops_by_index = self.state.cluster_store.meta_ops_total_by_index();
        let lag_by_index = self.state.meta_group_lag_by_index().await;
        let recovery = self.state.recovery_checkpoint_snapshot();
        let proposal = self.state.meta_proposal_stats_peek();
        let split_health = self.state.split_health_snapshot();
        let proposal_total_avg_us = if proposal.total.count == 0 {
            0.0
        } else {
            proposal.total.total_us as f64 / proposal.total.count as f64
        };
        let proposal_by_index = proposal
            .by_index
            .iter()
            .map(|(idx, snap)| {
                let avg_us = if snap.count == 0 {
                    0.0
                } else {
                    snap.total_us as f64 / snap.count as f64
                };
                (
                    *idx,
                    serde_json::json!({
                        "count": snap.count,
                        "errors": snap.errors,
                        "avg_us": avg_us,
                        "max_us": snap.max_us,
                    }),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();

        let now_ms = crate::unix_time_ms();
        let stuck_threshold_ms = self.state.meta_move_stuck_warn_ms();
        let rebalances_stuck = state
            .meta_rebalances
            .values()
            .filter(|mv| {
                let last = mv.last_progress_unix_ms.max(mv.started_unix_ms);
                last > 0 && now_ms.saturating_sub(last) >= stuck_threshold_ms
            })
            .count();

        let mut root = serde_json::to_value(&state)
            .map_err(|e| volo_grpc::Status::internal(format!("serialize state failed: {e}")))?;
        root["meta_health"] = serde_json::json!({
            "ops_by_index": ops_by_index,
            "lag_by_index": lag_by_index,
            "proposal_total": {
                "count": proposal.total.count,
                "errors": proposal.total.errors,
                "avg_us": proposal_total_avg_us,
                "max_us": proposal.total.max_us,
            },
            "proposal_by_index": proposal_by_index,
            "rebalances_inflight": state.meta_rebalances.len(),
            "rebalances_stuck": rebalances_stuck,
            "stuck_threshold_ms": stuck_threshold_ms,
        });
        root["recovery_health"] = serde_json::json!({
            "checkpoint_successes": recovery.success_count,
            "checkpoint_failures": recovery.failure_count,
            "checkpoint_manual_triggers": recovery.manual_trigger_count,
            "checkpoint_manual_trigger_failures": recovery.manual_trigger_failure_count,
            "checkpoint_pressure_skips": recovery.pressure_skip_count,
            "checkpoint_manifest_parse_errors": recovery.manifest_parse_error_count,
            "last_attempt_ms": recovery.last_attempt_ms,
            "last_success_ms": recovery.last_success_ms,
            "max_lag_entries": recovery.max_lag_entries,
            "blocked_groups": recovery.blocked_groups,
            "paused": recovery.paused,
            "last_run_reason": recovery.last_run_reason,
            "last_free_bytes": recovery.last_free_bytes,
            "last_free_pct": recovery.last_free_pct,
            "last_error": recovery.last_error,
        });
        root["split_health"] = serde_json::to_value(&split_health).map_err(|e| {
            volo_grpc::Status::internal(format!("serialize split health failed: {e}"))
        })?;

        let json = serde_json::to_string_pretty(&root)
            .map_err(|e| volo_grpc::Status::internal(format!("serialize state failed: {e}")))?;
        Ok(volo_grpc::Response::new(rpc::ClusterStateResponse {
            json: json.into(),
        }))
    }

    async fn cluster_add_node(
        &self,
        req: volo_grpc::Request<rpc::ClusterAddNodeRequest>,
    ) -> Result<volo_grpc::Response<rpc::ClusterAddNodeResponse>, volo_grpc::Status> {
        let req = req.into_inner();
        let cmd = crate::cluster::ClusterCommand::AddNode {
            node_id: req.node_id,
            grpc_addr: req.grpc_addr.to_string(),
            redis_addr: req.redis_addr.to_string(),
        };
        self.state
            .propose_meta_command(cmd)
            .await
            .map_err(|e| volo_grpc::Status::internal(format!("meta propose failed: {e}")))?;
        Ok(volo_grpc::Response::new(rpc::ClusterAddNodeResponse {
            ok: true,
        }))
    }

    async fn cluster_remove_node(
        &self,
        req: volo_grpc::Request<rpc::ClusterRemoveNodeRequest>,
    ) -> Result<volo_grpc::Response<rpc::ClusterRemoveNodeResponse>, volo_grpc::Status> {
        let req = req.into_inner();
        // This begins staged decommissioning; a background rebalancer drains
        // replicas and later finalizes the member as Removed.
        let cmd = crate::cluster::ClusterCommand::RemoveNode {
            node_id: req.node_id,
        };
        self.state
            .propose_meta_command(cmd)
            .await
            .map_err(|e| volo_grpc::Status::internal(format!("meta propose failed: {e}")))?;
        Ok(volo_grpc::Response::new(rpc::ClusterRemoveNodeResponse {
            ok: true,
        }))
    }

    async fn cluster_split_meta_range(
        &self,
        req: volo_grpc::Request<rpc::ClusterSplitMetaRangeRequest>,
    ) -> Result<volo_grpc::Response<rpc::ClusterSplitMetaRangeResponse>, volo_grpc::Status> {
        let req = req.into_inner();
        let split_hash = req.split_hash;
        let snapshot = self.state.cluster_store.state();
        let source = snapshot
            .meta_ranges
            .iter()
            .find(|range| split_hash >= range.start_hash && split_hash <= range.end_hash)
            .ok_or_else(|| {
                volo_grpc::Status::failed_precondition(
                    "split hash does not map to any existing meta range",
                )
            })?
            .clone();
        if split_hash == source.start_hash {
            return Err(volo_grpc::Status::invalid_argument(
                "split hash must be greater than the source range start",
            ));
        }
        let max_meta_groups = self.state.meta_handles.len().max(1);
        let target_meta_index = if req.target_meta_index == 0 {
            self.state
                .cluster_store
                .first_free_meta_index(max_meta_groups)
                .ok_or_else(|| {
                    volo_grpc::Status::failed_precondition(
                        "no free meta group slot available; raise --meta-groups",
                    )
                })?
        } else {
            req.target_meta_index as usize
        };
        if target_meta_index >= max_meta_groups {
            return Err(volo_grpc::Status::failed_precondition(format!(
                "target meta index {} exceeds configured meta groups {}",
                target_meta_index, max_meta_groups
            )));
        }
        drop(snapshot);

        let cmd = crate::cluster::ClusterCommand::SplitMetaRange {
            split_hash,
            target_meta_index,
        };
        self.state
            .propose_meta_command(cmd)
            .await
            .map_err(|e| volo_grpc::Status::internal(format!("meta propose failed: {e}")))?;

        Ok(volo_grpc::Response::new(
            rpc::ClusterSplitMetaRangeResponse {
                ok: true,
                left_meta_index: source.meta_index as u64,
                right_meta_index: target_meta_index as u64,
            },
        ))
    }

    async fn cluster_meta_rebalance(
        &self,
        req: volo_grpc::Request<rpc::ClusterMetaRebalanceRequest>,
    ) -> Result<volo_grpc::Response<rpc::ClusterMetaRebalanceResponse>, volo_grpc::Status> {
        let req = req.into_inner();
        if req.replicas.is_empty() {
            return Err(volo_grpc::Status::invalid_argument(
                "at least one replica is required",
            ));
        }
        let leaseholder = (req.leaseholder != 0).then_some(req.leaseholder);
        let planned = self
            .state
            .cluster_store
            .plan_meta_rebalance_command(req.meta_range_id, req.replicas.clone(), leaseholder)
            .map_err(|e| volo_grpc::Status::failed_precondition(format!("{e}")))?;
        if let Some(cmd) = planned {
            self.state
                .propose_meta_command(cmd)
                .await
                .map_err(|e| volo_grpc::Status::internal(format!("meta propose failed: {e}")))?;
        }
        Ok(volo_grpc::Response::new(
            rpc::ClusterMetaRebalanceResponse { ok: true },
        ))
    }

    async fn range_split(
        &self,
        req: volo_grpc::Request<rpc::RangeSplitRequest>,
    ) -> Result<volo_grpc::Response<rpc::RangeSplitResponse>, volo_grpc::Status> {
        let req = req.into_inner();
        let split_key = req.split_key.to_vec();
        let Some(target_shard_index) = self
            .state
            .cluster_store
            .first_free_shard_index(self.state.data_shards.max(1))
        else {
            return Err(volo_grpc::Status::failed_precondition(
                "no free data shard available for split",
            ));
        };
        let cmd = crate::cluster::ClusterCommand::SplitRange {
            split_key: split_key.clone(),
            target_shard_index,
            target_replicas: None,
            target_leaseholder: None,
            skip_migration: false,
        };
        self.state
            .propose_meta_command(cmd)
            .await
            .map_err(|e| volo_grpc::Status::internal(format!("meta propose failed: {e}")))?;
        let state = self.state.cluster_store.state();
        let (left, right) = state
            .shards
            .windows(2)
            .find(|pair| pair[0].end_key == split_key)
            .map(|pair| (pair[0].shard_id, pair[1].shard_id))
            .unwrap_or((0, 0));
        Ok(volo_grpc::Response::new(rpc::RangeSplitResponse {
            ok: true,
            left_shard_id: left,
            right_shard_id: right,
        }))
    }

    async fn range_merge(
        &self,
        req: volo_grpc::Request<rpc::RangeMergeRequest>,
    ) -> Result<volo_grpc::Response<rpc::RangeMergeResponse>, volo_grpc::Status> {
        let req = req.into_inner();
        let action = req.action;
        let action_i32 = i32::from(action);

        let propose = |cmd: ClusterCommand| async move {
            self.state
                .propose_meta_command(cmd)
                .await
                .map_err(|e| volo_grpc::Status::internal(format!("meta propose failed: {e}")))?;
            Ok::<(), volo_grpc::Status>(())
        };

        if action_i32 == i32::from(rpc::RangeMergeAction::RANGE_MERGE_ACTION_PAUSE)
            || action_i32 == i32::from(rpc::RangeMergeAction::RANGE_MERGE_ACTION_RESUME)
        {
            let paused = action_i32 == i32::from(rpc::RangeMergeAction::RANGE_MERGE_ACTION_PAUSE);
            let state = self.state.cluster_store.state();
            if !state.shard_merges.contains_key(&req.left_shard_id) {
                return Err(volo_grpc::Status::failed_precondition(format!(
                    "no in-flight merge for shard {}",
                    req.left_shard_id
                )));
            }
            drop(state);
            propose(ClusterCommand::PauseRangeMerge {
                left_shard_id: req.left_shard_id,
                paused,
            })
            .await?;
            let message = if paused {
                "merge paused"
            } else {
                "merge resumed"
            };
            return Ok(volo_grpc::Response::new(rpc::RangeMergeResponse {
                ok: true,
                merged_shard_id: req.left_shard_id,
                message: message.into(),
            }));
        }

        if action_i32 == i32::from(rpc::RangeMergeAction::RANGE_MERGE_ACTION_CANCEL) {
            let state = self.state.cluster_store.state();
            if !state.shard_merges.contains_key(&req.left_shard_id) {
                return Err(volo_grpc::Status::failed_precondition(format!(
                    "no in-flight merge for shard {}",
                    req.left_shard_id
                )));
            }
            drop(state);
            propose(ClusterCommand::AbortRangeMerge {
                left_shard_id: req.left_shard_id,
            })
            .await?;
            return Ok(volo_grpc::Response::new(rpc::RangeMergeResponse {
                ok: true,
                merged_shard_id: req.left_shard_id,
                message: "merge canceled".into(),
            }));
        }

        if action_i32 != i32::from(rpc::RangeMergeAction::RANGE_MERGE_ACTION_START) {
            return Err(volo_grpc::Status::invalid_argument(format!(
                "unsupported merge action {}",
                action_i32
            )));
        }

        let state = self.state.cluster_store.state();
        let idx = state
            .shards
            .iter()
            .position(|s| s.shard_id == req.left_shard_id)
            .ok_or_else(|| {
                volo_grpc::Status::failed_precondition(format!(
                    "unknown shard id {}",
                    req.left_shard_id
                ))
            })?;
        if idx + 1 >= state.shards.len() {
            return Err(volo_grpc::Status::failed_precondition(
                "merge requires a right-hand neighbor",
            ));
        }
        let left = &state.shards[idx];
        let right = &state.shards[idx + 1];
        let left_shard_id = left.shard_id;
        let right_shard_id = right.shard_id;
        if !left.end_key.is_empty() && left.end_key != right.start_key {
            return Err(volo_grpc::Status::failed_precondition(
                "ranges are not adjacent",
            ));
        }
        if left.replicas != right.replicas {
            return Err(volo_grpc::Status::failed_precondition(
                "left/right range replicas must match for merge",
            ));
        }
        if left.leaseholder != right.leaseholder {
            return Err(volo_grpc::Status::failed_precondition(
                "left/right range leaseholder must match for merge",
            ));
        }
        if state.shard_rebalances.contains_key(&left.shard_id)
            || state.shard_rebalances.contains_key(&right.shard_id)
        {
            return Err(volo_grpc::Status::failed_precondition(
                "cannot merge while either range has an in-flight move",
            ));
        }
        if let Some(existing) = state.shard_merges.get(&left_shard_id) {
            if existing.right_shard_id == right_shard_id {
                return Ok(volo_grpc::Response::new(rpc::RangeMergeResponse {
                    ok: true,
                    merged_shard_id: req.left_shard_id,
                    message: "merge already in progress".into(),
                }));
            }
            return Err(volo_grpc::Status::failed_precondition(
                "left range already has an in-flight merge",
            ));
        }
        if state.shard_merges.values().any(|m| {
            m.left_shard_id == left_shard_id
                || m.right_shard_id == left_shard_id
                || m.left_shard_id == right_shard_id
                || m.right_shard_id == right_shard_id
        }) {
            return Err(volo_grpc::Status::failed_precondition(
                "one of the ranges already participates in an in-flight merge",
            ));
        }
        drop(state);

        propose(ClusterCommand::BeginRangeMerge {
            left_shard_id,
            right_shard_id,
        })
        .await?;
        Ok(volo_grpc::Response::new(rpc::RangeMergeResponse {
            ok: true,
            merged_shard_id: req.left_shard_id,
            message: "merge started".into(),
        }))
    }

    async fn range_rebalance(
        &self,
        req: volo_grpc::Request<rpc::RangeRebalanceRequest>,
    ) -> Result<volo_grpc::Response<rpc::RangeRebalanceResponse>, volo_grpc::Status> {
        let req = req.into_inner();
        let desired_leaseholder = (req.leaseholder != 0).then_some(req.leaseholder);
        let planned = self
            .state
            .cluster_store
            .plan_rebalance_command(req.shard_id, req.replicas, desired_leaseholder)
            .map_err(|e| volo_grpc::Status::failed_precondition(e.to_string()))?;
        let Some(cmd) = planned else {
            return Ok(volo_grpc::Response::new(rpc::RangeRebalanceResponse {
                ok: true,
            }));
        };
        self.state
            .propose_meta_command(cmd)
            .await
            .map_err(|e| volo_grpc::Status::internal(format!("meta propose failed: {e}")))?;
        Ok(volo_grpc::Response::new(rpc::RangeRebalanceResponse {
            ok: true,
        }))
    }

    async fn range_stats(
        &self,
        _req: volo_grpc::Request<rpc::RangeStatsRequest>,
    ) -> Result<volo_grpc::Response<rpc::RangeStatsResponse>, volo_grpc::Status> {
        let local = self
            .state
            .local_range_stats_detailed()
            .await
            .map_err(|e| volo_grpc::Status::internal(format!("range stats failed: {e}")))?;
        let ranges = local
            .into_iter()
            .map(|item| rpc::RangeStat {
                shard_id: item.shard_id,
                shard_index: item.shard_index as u64,
                record_count: item.record_count,
                is_leaseholder: item.is_leaseholder,
                write_ops_total: item.write_ops_total,
                read_ops_total: item.read_ops_total,
                write_bytes_total: item.write_bytes_total,
                queue_depth: item.queue_depth,
                write_tail_latency_ms: item.write_tail_latency_ms,
                hot_key_concentration_bps: item.hot_key_concentration_bps,
                write_hot_buckets: item.write_hot_buckets,
                read_hot_buckets: item.read_hot_buckets,
            })
            .collect();
        Ok(volo_grpc::Response::new(rpc::RangeStatsResponse {
            node_id: self.state.node_id,
            ranges,
        }))
    }

    async fn range_snapshot_latest(
        &self,
        req: volo_grpc::Request<rpc::RangeSnapshotLatestRequest>,
    ) -> Result<volo_grpc::Response<rpc::RangeSnapshotLatestResponse>, volo_grpc::Status> {
        let req = req.into_inner();
        let limit = if req.limit == 0 {
            1024
        } else {
            req.limit as usize
        };
        let (entries, next_cursor, done) = self
            .state
            .snapshot_range_latest_page(
                req.shard_index as usize,
                req.start_key.as_ref(),
                req.end_key.as_ref(),
                req.cursor.as_ref(),
                limit,
                req.reverse,
            )
            .map_err(|e| volo_grpc::Status::internal(format!("range snapshot failed: {e}")))?;
        let entries = entries
            .into_iter()
            .map(|entry| rpc::RangeSnapshotLatestEntry {
                key: entry.key.into(),
                value: entry.value.into(),
                version: Some(to_rpc_version(entry.version)),
            })
            .collect();
        Ok(volo_grpc::Response::new(rpc::RangeSnapshotLatestResponse {
            entries,
            next_cursor: next_cursor.into(),
            done,
        }))
    }

    async fn range_write_latest(
        &self,
        req: volo_grpc::Request<rpc::RangeWriteLatestRequest>,
    ) -> Result<volo_grpc::Response<rpc::RangeWriteLatestResponse>, volo_grpc::Status> {
        let req = req.into_inner();
        let mut entries = Vec::with_capacity(req.entries.len());
        for entry in req.entries {
            entries.push(crate::RangeWriteEntry {
                key: entry.key.to_vec(),
                value: entry.value.to_vec(),
            });
        }

        let applied = self
            .state
            .write_range_latest_replicated(
                req.shard_index as usize,
                req.start_key.as_ref(),
                req.end_key.as_ref(),
                &entries,
            )
            .await
            .map_err(|e| volo_grpc::Status::internal(format!("range write failed: {e}")))?;

        Ok(volo_grpc::Response::new(rpc::RangeWriteLatestResponse {
            applied,
        }))
    }

    async fn range_write_latest_conditional(
        &self,
        req: volo_grpc::Request<rpc::RangeWriteLatestConditionalRequest>,
    ) -> Result<volo_grpc::Response<rpc::RangeWriteLatestConditionalResponse>, volo_grpc::Status>
    {
        let req = req.into_inner();
        let mut entries = Vec::with_capacity(req.entries.len());
        for entry in req.entries {
            let expected_version = from_rpc_version_required(entry.expected_version)?;
            entries.push(crate::RangeConditionalWriteEntry {
                key: entry.key.to_vec(),
                value: entry.value.to_vec(),
                expected_version,
            });
        }

        let result = self
            .state
            .write_range_latest_conditional_replicated(
                req.shard_index as usize,
                req.start_key.as_ref(),
                req.end_key.as_ref(),
                &entries,
            )
            .await
            .map_err(|e| {
                volo_grpc::Status::internal(format!("range conditional write failed: {e}"))
            })?;

        let applied_versions = result
            .applied_versions
            .into_iter()
            .map(|item| rpc::RangeWriteLatestConditionalAppliedVersion {
                key: item.key.into(),
                version: Some(to_rpc_version(item.version)),
            })
            .collect();

        Ok(volo_grpc::Response::new(
            rpc::RangeWriteLatestConditionalResponse {
                applied: result.applied,
                conflicts: result.conflicts,
                applied_versions,
            },
        ))
    }

    async fn range_apply_latest(
        &self,
        req: volo_grpc::Request<rpc::RangeApplyLatestRequest>,
    ) -> Result<volo_grpc::Response<rpc::RangeApplyLatestResponse>, volo_grpc::Status> {
        let req = req.into_inner();
        if !req.admin {
            return Err(volo_grpc::Status::permission_denied(
                "range_apply_latest is reserved for admin/backfill",
            ));
        }
        let mut entries = Vec::with_capacity(req.entries.len());
        for entry in req.entries {
            let version = from_rpc_version_required(entry.version)?;
            entries.push(crate::RangeSnapshotLatestEntry {
                key: entry.key.to_vec(),
                value: entry.value.to_vec(),
                version,
            });
        }
        let applied = self
            .state
            .apply_range_latest_page(
                req.shard_index as usize,
                req.start_key.as_ref(),
                req.end_key.as_ref(),
                &entries,
            )
            .map_err(|e| volo_grpc::Status::internal(format!("range apply failed: {e}")))?;
        Ok(volo_grpc::Response::new(rpc::RangeApplyLatestResponse {
            applied,
        }))
    }

    async fn range_apply_latest_conditional(
        &self,
        req: volo_grpc::Request<rpc::RangeApplyLatestConditionalRequest>,
    ) -> Result<volo_grpc::Response<rpc::RangeApplyLatestConditionalResponse>, volo_grpc::Status>
    {
        let req = req.into_inner();
        if !req.admin {
            return Err(volo_grpc::Status::permission_denied(
                "range_apply_latest_conditional is reserved for admin/backfill",
            ));
        }
        let mut entries = Vec::with_capacity(req.entries.len());
        for entry in req.entries {
            let version = from_rpc_version_required(entry.version)?;
            let expected_version = from_rpc_version_required(entry.expected_version)?;
            entries.push(crate::RangeConditionalLatestEntry {
                key: entry.key.to_vec(),
                value: entry.value.to_vec(),
                version,
                expected_version,
            });
        }

        let result = self
            .state
            .apply_range_latest_page_conditional(
                req.shard_index as usize,
                req.start_key.as_ref(),
                req.end_key.as_ref(),
                &entries,
            )
            .map_err(|e| {
                volo_grpc::Status::internal(format!("range conditional apply failed: {e}"))
            })?;

        Ok(volo_grpc::Response::new(
            rpc::RangeApplyLatestConditionalResponse {
                applied: result.applied,
                conflicts: result.conflicts,
            },
        ))
    }

    async fn cluster_freeze(
        &self,
        req: volo_grpc::Request<rpc::ClusterFreezeRequest>,
    ) -> Result<volo_grpc::Response<rpc::ClusterFreezeResponse>, volo_grpc::Status> {
        let req = req.into_inner();
        let cmd = crate::cluster::ClusterCommand::SetFrozen { frozen: req.frozen };
        self.state
            .propose_meta_command(cmd)
            .await
            .map_err(|e| volo_grpc::Status::internal(format!("meta propose failed: {e}")))?;
        Ok(volo_grpc::Response::new(rpc::ClusterFreezeResponse {
            ok: true,
        }))
    }

    async fn cluster_checkpoint_control(
        &self,
        req: volo_grpc::Request<rpc::ClusterCheckpointControlRequest>,
    ) -> Result<volo_grpc::Response<rpc::ClusterCheckpointControlResponse>, volo_grpc::Status> {
        let req = req.into_inner();
        let action =
            rpc::ClusterCheckpointAction::try_from_i32(req.action.inner()).ok_or_else(|| {
                volo_grpc::Status::invalid_argument(format!(
                    "invalid checkpoint action {}",
                    req.action.inner()
                ))
            })?;

        let message = match action.inner() {
            x if x == rpc::ClusterCheckpointAction::CLUSTER_CHECKPOINT_ACTION_STATUS.inner() => {
                "checkpoint status".to_string()
            }
            x if x == rpc::ClusterCheckpointAction::CLUSTER_CHECKPOINT_ACTION_PAUSE.inner() => {
                self.state
                    .recovery_checkpoint_set_paused(true)
                    .await
                    .map_err(|e| volo_grpc::Status::failed_precondition(e.to_string()))?;
                "checkpoint paused".to_string()
            }
            x if x == rpc::ClusterCheckpointAction::CLUSTER_CHECKPOINT_ACTION_RESUME.inner() => {
                self.state
                    .recovery_checkpoint_set_paused(false)
                    .await
                    .map_err(|e| volo_grpc::Status::failed_precondition(e.to_string()))?;
                "checkpoint resumed".to_string()
            }
            x if x == rpc::ClusterCheckpointAction::CLUSTER_CHECKPOINT_ACTION_TRIGGER.inner() => {
                self.state
                    .recovery_checkpoint_trigger()
                    .await
                    .map_err(|e| volo_grpc::Status::failed_precondition(e.to_string()))?;
                "checkpoint triggered".to_string()
            }
            _ => {
                return Err(volo_grpc::Status::invalid_argument(
                    "invalid checkpoint action".to_string(),
                ));
            }
        };

        let snapshot = self.state.recovery_checkpoint_snapshot();
        Ok(volo_grpc::Response::new(
            rpc::ClusterCheckpointControlResponse {
                ok: true,
                message: message.into(),
                paused: snapshot.paused,
                checkpoint_successes: snapshot.success_count,
                checkpoint_failures: snapshot.failure_count,
                checkpoint_manual_triggers: snapshot.manual_trigger_count,
                checkpoint_manual_trigger_failures: snapshot.manual_trigger_failure_count,
                checkpoint_pressure_skips: snapshot.pressure_skip_count,
                checkpoint_manifest_parse_errors: snapshot.manifest_parse_error_count,
                last_attempt_ms: snapshot.last_attempt_ms,
                last_success_ms: snapshot.last_success_ms,
                max_lag_entries: snapshot.max_lag_entries,
                blocked_groups: snapshot.blocked_groups,
                last_run_reason: snapshot.last_run_reason.into(),
                last_free_bytes: snapshot.last_free_bytes,
                last_free_pct: snapshot.last_free_pct,
                last_error: snapshot.last_error.into(),
            },
        ))
    }
}

/// Convert a local version into its RPC representation.
fn to_rpc_version(version: Version) -> rpc::Version {
    rpc::Version {
        seq: version.seq,
        txn_id: Some(rpc::TxnId {
            node_id: version.txn_id.node_id,
            counter: version.txn_id.counter,
        }),
    }
}

/// Convert an RPC version into a local version, rejecting missing txn ids.
fn from_rpc_version_required(version: Option<rpc::Version>) -> Result<Version, volo_grpc::Status> {
    let version = version.ok_or_else(|| volo_grpc::Status::invalid_argument("missing version"))?;
    let txn_id = version
        .txn_id
        .ok_or_else(|| volo_grpc::Status::invalid_argument("missing version.txn_id"))?;
    Ok(Version {
        seq: version.seq,
        txn_id: accord::TxnId {
            node_id: txn_id.node_id,
            counter: txn_id.counter,
        },
    })
}

/// Parse a 32-byte command digest from the wire format.
fn parse_command_digest(bytes: Bytes) -> Result<[u8; 32], volo_grpc::Status> {
    if bytes.len() != 32 {
        // Reject invalid sizes to prevent inconsistent digests.
        return Err(volo_grpc::Status::invalid_argument(
            "invalid command_digest length",
        ));
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(bytes.as_ref());
    Ok(out)
}

async fn propose_meta(state: Arc<NodeState>, cmd: ClusterCommand) -> anyhow::Result<()> {
    state.propose_meta_command(cmd).await
}

async fn wait_for_local_epoch(
    state: Arc<NodeState>,
    min_epoch: u64,
    timeout: Duration,
) -> anyhow::Result<u64> {
    let deadline = Instant::now() + timeout;
    loop {
        let local_epoch = state.cluster_store.epoch();
        if local_epoch > min_epoch {
            return Ok(local_epoch);
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for local epoch to advance beyond {} (now={})",
                min_epoch,
                local_epoch
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_local_state(
    state: Arc<NodeState>,
    min_epoch: u64,
    frozen: bool,
    timeout: Duration,
) -> anyhow::Result<u64> {
    let deadline = Instant::now() + timeout;
    loop {
        let snapshot = state.cluster_store.state();
        if snapshot.epoch > min_epoch && snapshot.frozen == frozen {
            return Ok(snapshot.epoch);
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for local state (epoch>{}, frozen={}) (now_epoch={}, now_frozen={})",
                min_epoch,
                frozen,
                snapshot.epoch,
                snapshot.frozen
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_cluster_converged(
    state: Arc<NodeState>,
    min_epoch: u64,
    frozen: bool,
    min_shards: Option<usize>,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let snapshot = state.cluster_store.state();
        let members = snapshot
            .members
            .values()
            .filter(|m| m.state != MemberState::Removed)
            .map(|m| m.node_id)
            .collect::<Vec<_>>();
        let mut all_ok = true;
        let mut reason = String::new();

        for node_id in members {
            let node_state = if node_id == state.node_id {
                snapshot.clone()
            } else {
                match state.transport.cluster_state(node_id).await {
                    Ok(s) => s,
                    Err(err) => {
                        all_ok = false;
                        reason = format!("node {} state fetch failed: {}", node_id, err);
                        break;
                    }
                }
            };
            let shard_ok = min_shards
                .map(|n| node_state.shards.len() >= n)
                .unwrap_or(true);
            if !(node_state.epoch >= min_epoch && node_state.frozen == frozen && shard_ok) {
                all_ok = false;
                reason = format!(
                    "node {} not converged (epoch={}, frozen={}, shards={})",
                    node_id,
                    node_state.epoch,
                    node_state.frozen,
                    node_state.shards.len()
                );
                break;
            }
        }

        if all_ok {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for cluster convergence: {}", reason);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_shard_quiesced(
    state: Arc<NodeState>,
    shard_index: usize,
    timeout: Duration,
) -> anyhow::Result<()> {
    let group_id = crate::GROUP_DATA_BASE + shard_index as u64;
    let group = state
        .group(group_id)
        .ok_or_else(|| anyhow::anyhow!("missing group for shard index {}", shard_index))?;
    let deadline = Instant::now() + timeout;

    loop {
        let stats = group.debug_stats().await;
        let pending = stats.records_status_preaccepted_len
            + stats.records_status_accepted_len
            + stats.records_status_committed_len
            + stats.records_status_executing_len
            + stats.committed_queue_len
            + stats.read_waiters_len;
        if pending == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for shard {} quiesce (preaccepted={}, accepted={}, committed={}, executing={}, committed_queue={}, read_waiters={})",
                shard_index,
                stats.records_status_preaccepted_len,
                stats.records_status_accepted_len,
                stats.records_status_committed_len,
                stats.records_status_executing_len,
                stats.committed_queue_len,
                stats.read_waiters_len
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_shard_prefix_converged(
    state: Arc<NodeState>,
    shard: &ShardDesc,
    timeout: Duration,
) -> anyhow::Result<()> {
    let group_id = crate::GROUP_DATA_BASE + shard.shard_index as u64;
    let deadline = Instant::now() + timeout;
    let group = state
        .group(group_id)
        .ok_or_else(|| anyhow::anyhow!("missing group for shard index {}", shard.shard_index))?;

    loop {
        let mut prefixes_by_node = Vec::<(NodeId, Vec<ExecutedPrefix>)>::new();
        let mut fetch_err = None;
        for replica in &shard.replicas {
            let prefixes = if *replica == state.node_id {
                group.executed_prefixes().await
            } else {
                match state
                    .transport
                    .last_executed_prefix(*replica, group_id)
                    .await
                {
                    Ok(p) => p,
                    Err(err) => {
                        fetch_err =
                            Some(format!("replica {} prefix fetch failed: {}", replica, err));
                        break;
                    }
                }
            };
            prefixes_by_node.push((*replica, prefixes));
        }
        if fetch_err.is_none() && shard_prefixes_converged(&prefixes_by_node) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            if let Some(err) = fetch_err {
                anyhow::bail!(
                    "timed out waiting for shard {} prefix convergence: {}",
                    shard.shard_id,
                    err
                );
            }
            anyhow::bail!(
                "timed out waiting for shard {} prefix convergence across replicas {:?}",
                shard.shard_id,
                shard.replicas
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn shard_prefixes_converged(prefixes_by_node: &[(NodeId, Vec<ExecutedPrefix>)]) -> bool {
    use std::collections::BTreeMap;

    let mut required: BTreeMap<NodeId, u64> = BTreeMap::new();
    for (_, prefixes) in prefixes_by_node {
        for p in prefixes {
            required
                .entry(p.node_id)
                .and_modify(|counter| *counter = (*counter).max(p.counter))
                .or_insert(p.counter);
        }
    }

    prefixes_by_node.iter().all(|(_, prefixes)| {
        required
            .iter()
            .all(|(origin, req)| prefix_counter(prefixes, *origin) >= *req)
    })
}

fn prefix_counter(prefixes: &[ExecutedPrefix], node_id: NodeId) -> u64 {
    prefixes
        .iter()
        .find(|p| p.node_id == node_id)
        .map(|p| p.counter)
        .unwrap_or(0)
}

async fn wait_for_merge_visible(
    state: Arc<NodeState>,
    left_shard_id: u64,
    right_shard_id: u64,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let snapshot = state.cluster_store.state();
        let has_left = snapshot.shards.iter().any(|s| s.shard_id == left_shard_id);
        let has_right = snapshot.shards.iter().any(|s| s.shard_id == right_shard_id);
        if has_left && !has_right {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for merge visibility (left={}, right={}, has_left={}, has_right={})",
                left_shard_id,
                right_shard_id,
                has_left,
                has_right
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
