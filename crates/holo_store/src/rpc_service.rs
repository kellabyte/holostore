//! gRPC service handlers that adapt network requests into Accord operations.
//!
//! This module is the server-side counterpart to `transport.rs`, translating
//! protobuf messages into local Accord types and returning responses.

use std::sync::Arc;

use bytes::Bytes;
use holo_accord::accord;

use crate::kv::Version;
use crate::volo_gen::holo_store::rpc;
use crate::NodeState;

/// gRPC service implementation backed by a shared `NodeState`.
#[derive(Clone)]
pub struct RpcService {
    pub state: Arc<NodeState>,
}

impl rpc::HoloRpc for RpcService {
    /// Handle a single pre-accept RPC request.
    async fn pre_accept(
        &self,
        req: volo_grpc::Request<rpc::PreAcceptRequest>,
    ) -> Result<volo_grpc::Response<rpc::PreAcceptResponse>, volo_grpc::Status> {
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

    /// Handle an executed RPC request.
    async fn executed(
        &self,
        req: volo_grpc::Request<rpc::ExecutedRequest>,
    ) -> Result<volo_grpc::Response<rpc::ExecutedResponse>, volo_grpc::Status> {
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
        Ok(volo_grpc::Response::new(rpc::ExecutedResponse {
            executed,
        }))
    }

    /// Handle a mark_visible RPC request.
    async fn mark_visible(
        &self,
        req: volo_grpc::Request<rpc::MarkVisibleRequest>,
    ) -> Result<volo_grpc::Response<rpc::MarkVisibleResponse>, volo_grpc::Status> {
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
        Ok(volo_grpc::Response::new(rpc::KvBatchGetResponse { responses }))
    }

    /// Handle a join RPC, returning the initial membership string.
    async fn join(
        &self,
        _req: volo_grpc::Request<rpc::JoinRequest>,
    ) -> Result<volo_grpc::Response<rpc::JoinResponse>, volo_grpc::Status> {
        Ok(volo_grpc::Response::new(rpc::JoinResponse {
            initial_members: self.state.initial_members.clone().into(),
        }))
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
