use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use futures_util::{FutureExt, SinkExt, StreamExt};
use holo_accord::accord::DebugStats;
use redis_protocol::codec::Resp2;
use redis_protocol::resp2::types::BytesFrame;
use redis_protocol::resp2::types::Resp2Frame;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

use crate::{wal, NodeState};

const MAX_BATCHED_SET_OPS: usize = 32;
const MAX_BATCHED_GET_OPS: usize = 128;

fn merge_debug_stats(dst: &mut DebugStats, src: DebugStats) {
    dst.records_len += src.records_len;
    dst.records_capacity += src.records_capacity;
    dst.records_status_none_len += src.records_status_none_len;
    dst.records_status_preaccepted_len += src.records_status_preaccepted_len;
    dst.records_status_accepted_len += src.records_status_accepted_len;
    dst.records_status_committed_len += src.records_status_committed_len;
    dst.records_status_executing_len += src.records_status_executing_len;
    dst.records_status_executed_len += src.records_status_executed_len;
    dst.records_missing_command_len += src.records_missing_command_len;
    dst.records_missing_keys_len += src.records_missing_keys_len;
    dst.records_committed_missing_command_len += src.records_committed_missing_command_len;
    dst.records_committed_missing_keys_len += src.records_committed_missing_keys_len;
    dst.committed_queue_len += src.committed_queue_len;
    dst.committed_queue_ghost_len += src.committed_queue_ghost_len;
    dst.frontier_keys_len += src.frontier_keys_len;
    dst.frontier_entries_len += src.frontier_entries_len;
    dst.executed_prefix_nodes = dst.executed_prefix_nodes.max(src.executed_prefix_nodes);
    dst.executed_out_of_order_len += src.executed_out_of_order_len;
    dst.executed_log_len += src.executed_log_len;
    dst.executed_log_capacity += src.executed_log_capacity;
    dst.executed_log_order_capacity += src.executed_log_order_capacity;
    dst.executed_log_command_bytes += src.executed_log_command_bytes;
    dst.executed_log_max_command_bytes =
        dst.executed_log_max_command_bytes.max(src.executed_log_max_command_bytes);
    dst.executed_log_deps_total += src.executed_log_deps_total;
    dst.executed_log_max_deps_len = dst.executed_log_max_deps_len.max(src.executed_log_max_deps_len);
    dst.reported_executed_peers = dst.reported_executed_peers.max(src.reported_executed_peers);
    dst.recovering_len += src.recovering_len;
    dst.read_waiters_len += src.read_waiters_len;
    dst.proposal_timeouts += src.proposal_timeouts;
    dst.execute_timeouts += src.execute_timeouts;
    dst.recovery_attempts += src.recovery_attempts;
    dst.recovery_successes += src.recovery_successes;
    dst.recovery_failures += src.recovery_failures;
    dst.recovery_timeouts += src.recovery_timeouts;
    dst.recovery_noops += src.recovery_noops;
    dst.recovery_last_ms = dst.recovery_last_ms.max(src.recovery_last_ms);
    dst.exec_progress_count += src.exec_progress_count;
    dst.exec_progress_total_us += src.exec_progress_total_us;
    dst.exec_progress_max_us = dst.exec_progress_max_us.max(src.exec_progress_max_us);
    dst.exec_progress_true += src.exec_progress_true;
    dst.exec_progress_false += src.exec_progress_false;
    dst.exec_progress_errors += src.exec_progress_errors;
    dst.exec_recover_count += src.exec_recover_count;
    dst.exec_recover_total_us += src.exec_recover_total_us;
    dst.exec_recover_max_us = dst.exec_recover_max_us.max(src.exec_recover_max_us);
    dst.exec_recover_true += src.exec_recover_true;
    dst.exec_recover_false += src.exec_recover_false;
    dst.exec_recover_errors += src.exec_recover_errors;
    dst.apply_write_count += src.apply_write_count;
    dst.apply_write_total_us += src.apply_write_total_us;
    dst.apply_write_max_us = dst.apply_write_max_us.max(src.apply_write_max_us);
    dst.apply_read_count += src.apply_read_count;
    dst.apply_read_total_us += src.apply_read_total_us;
    dst.apply_read_max_us = dst.apply_read_max_us.max(src.apply_read_max_us);
    dst.apply_batch_count += src.apply_batch_count;
    dst.apply_batch_total_us += src.apply_batch_total_us;
    dst.apply_batch_max_us = dst.apply_batch_max_us.max(src.apply_batch_max_us);
    dst.mark_visible_count += src.mark_visible_count;
    dst.mark_visible_total_us += src.mark_visible_total_us;
    dst.mark_visible_max_us = dst.mark_visible_max_us.max(src.mark_visible_max_us);
    dst.state_update_count += src.state_update_count;
    dst.state_update_total_us += src.state_update_total_us;
    dst.state_update_max_us = dst.state_update_max_us.max(src.state_update_max_us);
    dst.fast_path_count += src.fast_path_count;
    dst.slow_path_count += src.slow_path_count;
}

#[derive(Clone, Debug)]
pub enum KvOp {
    Get { key: Vec<u8> },
    Set { key: Vec<u8>, value: Vec<u8> },
    HoloStats,
}

#[derive(Clone, Debug)]
pub enum KvResult {
    Ok,
    Value(Option<Vec<u8>>),
}

pub async fn run(addr: SocketAddr, state: Arc<NodeState>) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (socket, _) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_conn(socket, state).await {
                tracing::debug!(error = ?err, "redis connection closed");
            }
        });
    }
}

async fn handle_conn(socket: TcpStream, state: Arc<NodeState>) -> anyhow::Result<()> {
    let mut framed = Framed::new(socket, Resp2::default());
    let max_batched_set_ops = read_env_usize("HOLO_REDIS_SET_BATCH_MAX", MAX_BATCHED_SET_OPS);
    let max_batched_get_ops = read_env_usize("HOLO_REDIS_GET_BATCH_MAX", MAX_BATCHED_GET_OPS);

    async fn feed_resp(
        framed: &mut Framed<TcpStream, Resp2>,
        resp: BytesFrame,
    ) -> anyhow::Result<()> {
        framed.feed(resp).await?;
        Ok(())
    }

    async fn flush_resp(framed: &mut Framed<TcpStream, Resp2>) -> anyhow::Result<()> {
        <Framed<TcpStream, Resp2> as SinkExt<BytesFrame>>::flush(framed).await?;
        Ok(())
    }

    let mut pending_op: Option<KvOp> = None;
    let mut pending_resp: Option<BytesFrame> = None;
    let mut stream_closed = false;

    while !stream_closed || pending_op.is_some() || pending_resp.is_some() {
        if let Some(resp) = pending_resp.take() {
            feed_resp(&mut framed, resp).await?;
            flush_resp(&mut framed).await?;
            continue;
        }

        let op = if let Some(op) = pending_op.take() {
            op
        } else {
            let Some(frame) = framed.next().await else {
                stream_closed = true;
                continue;
            };
            let frame = frame?;
            match parse_command(frame) {
                Ok(Some(op)) => op,
                Ok(None) => continue,
                Err(err) => {
                    feed_resp(&mut framed, BytesFrame::Error(format!("ERR {err}").into())).await?;
                    flush_resp(&mut framed).await?;
                    continue;
                }
            }
        };

        match op {
            KvOp::HoloStats => {
                let mut stats_opt: Option<DebugStats> = None;
                for shard in 0..state.data_shards {
                    let group_id = crate::GROUP_DATA_BASE + shard as u64;
                    let data_group = state
                        .groups
                        .get(&group_id)
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("missing data group {group_id}"))?;
                    let shard_stats = data_group.debug_stats().await;
                    match stats_opt.as_mut() {
                        Some(total) => merge_debug_stats(total, shard_stats),
                        None => stats_opt = Some(shard_stats),
                    }
                }
                let stats = stats_opt.ok_or_else(|| anyhow::anyhow!("missing data groups"))?;
                let rpc_stats = state.transport.stats().snapshot_and_reset();
                let client_batch = state.client_batch_stats_snapshot();
                let pre_accept_avg_batch = if rpc_stats.pre_accept_batches == 0 {
                    0.0
                } else {
                    rpc_stats.pre_accept_items as f64 / rpc_stats.pre_accept_batches as f64
                };
                let accept_avg_batch = if rpc_stats.accept_batches == 0 {
                    0.0
                } else {
                    rpc_stats.accept_items as f64 / rpc_stats.accept_batches as f64
                };
                let commit_avg_batch = if rpc_stats.commit_batches == 0 {
                    0.0
                } else {
                    rpc_stats.commit_items as f64 / rpc_stats.commit_batches as f64
                };
                let client_set_avg_batch = if client_batch.set_batches == 0 {
                    0.0
                } else {
                    client_batch.set_items as f64 / client_batch.set_batches as f64
                };
                let client_set_avg_wait_us = if client_batch.set_batches == 0 {
                    0.0
                } else {
                    client_batch.set_wait_total_us as f64 / client_batch.set_batches as f64
                };
                let client_get_avg_batch = if client_batch.get_batches == 0 {
                    0.0
                } else {
                    client_batch.get_items as f64 / client_batch.get_batches as f64
                };
                let client_get_avg_wait_us = if client_batch.get_batches == 0 {
                    0.0
                } else {
                    client_batch.get_wait_total_us as f64 / client_batch.get_batches as f64
                };
                let wal_stats = wal::stats_snapshot();
                let wal_fsync_avg_us = if wal_stats.fsync_count == 0 {
                    0.0
                } else {
                    wal_stats.fsync_total_us as f64 / wal_stats.fsync_count as f64
                };
                let wal_batch_avg_items = if wal_stats.batch_count == 0 {
                    0.0
                } else {
                    wal_stats.batch_items as f64 / wal_stats.batch_count as f64
                };
                let wal_batch_avg_bytes = if wal_stats.batch_count == 0 {
                    0.0
                } else {
                    wal_stats.batch_total_bytes as f64 / wal_stats.batch_count as f64
                };
                let msg = format!(
                    "records.len={} records.cap={} records.status.none={} records.status.preaccepted={} records.status.accepted={} records.status.committed={} records.status.executing={} records.status.executed={} records.missing.command={} records.missing.keys={} records.committed_missing.command={} records.committed_missing.keys={} committed_queue.len={} committed_queue.ghost={} frontier.keys={} frontier.entries={} executed_prefix_nodes={} executed_out_of_order.len={} executed_log.len={} executed_log.cap={} executed_log_order.cap={} executed_log.command_bytes={} executed_log.max_command_bytes={} executed_log.deps_total={} executed_log.max_deps_len={} reported_executed_peers={} recovering.len={} read_waiters.len={} proposal_timeouts={} execute_timeouts={} recovery.attempts={} recovery.successes={} recovery.failures={} recovery.timeouts={} recovery.noops={} recovery.last_ms={} exec.progress.count={} exec.progress.total_us={} exec.progress.max_us={} exec.progress.true={} exec.progress.false={} exec.progress.errors={} exec.recover.count={} exec.recover.total_us={} exec.recover.max_us={} exec.recover.true={} exec.recover.false={} exec.recover.errors={} apply.write.count={} apply.write.total_us={} apply.write.max_us={} apply.read.count={} apply.read.total_us={} apply.read.max_us={} apply.batch.count={} apply.batch.total_us={} apply.batch.max_us={} apply.visible.count={} apply.visible.total_us={} apply.visible.max_us={} apply.state.count={} apply.state.total_us={} apply.state.max_us={} fast_path.count={} slow_path.count={} rpc.pre_accept.batches={} rpc.pre_accept.items={} rpc.pre_accept.avg_batch={:.2} rpc.pre_accept.max_batch={} rpc.accept.batches={} rpc.accept.items={} rpc.accept.avg_batch={:.2} rpc.accept.max_batch={} rpc.commit.batches={} rpc.commit.items={} rpc.commit.avg_batch={:.2} rpc.commit.max_batch={} client.set.batches={} client.set.items={} client.set.avg_batch={:.2} client.set.max_batch={} client.set.avg_wait_us={:.2} client.set.max_wait_us={} client.get.batches={} client.get.items={} client.get.avg_batch={:.2} client.get.max_batch={} client.get.avg_wait_us={:.2} client.get.max_wait_us={} wal.fsync.count={} wal.fsync.total_us={} wal.fsync.avg_us={:.2} wal.fsync.max_us={} wal.batch.count={} wal.batch.items={} wal.batch.avg_items={:.2} wal.batch.max_items={} wal.batch.total_bytes={} wal.batch.avg_bytes={:.2} wal.batch.max_bytes={}",
                    stats.records_len,
                    stats.records_capacity,
                    stats.records_status_none_len,
                    stats.records_status_preaccepted_len,
                    stats.records_status_accepted_len,
                    stats.records_status_committed_len,
                    stats.records_status_executing_len,
                    stats.records_status_executed_len,
                    stats.records_missing_command_len,
                    stats.records_missing_keys_len,
                    stats.records_committed_missing_command_len,
                    stats.records_committed_missing_keys_len,
                    stats.committed_queue_len,
                    stats.committed_queue_ghost_len,
                    stats.frontier_keys_len,
                    stats.frontier_entries_len,
                    stats.executed_prefix_nodes,
                    stats.executed_out_of_order_len,
                    stats.executed_log_len,
                    stats.executed_log_capacity,
                    stats.executed_log_order_capacity,
                    stats.executed_log_command_bytes,
                    stats.executed_log_max_command_bytes,
                    stats.executed_log_deps_total,
                    stats.executed_log_max_deps_len,
                    stats.reported_executed_peers,
                    stats.recovering_len,
                    stats.read_waiters_len,
                    stats.proposal_timeouts,
                    stats.execute_timeouts,
                    stats.recovery_attempts,
                    stats.recovery_successes,
                    stats.recovery_failures,
                    stats.recovery_timeouts,
                    stats.recovery_noops,
                    stats.recovery_last_ms,
                    stats.exec_progress_count,
                    stats.exec_progress_total_us,
                    stats.exec_progress_max_us,
                    stats.exec_progress_true,
                    stats.exec_progress_false,
                    stats.exec_progress_errors,
                    stats.exec_recover_count,
                    stats.exec_recover_total_us,
                    stats.exec_recover_max_us,
                    stats.exec_recover_true,
                    stats.exec_recover_false,
                    stats.exec_recover_errors,
                    stats.apply_write_count,
                    stats.apply_write_total_us,
                    stats.apply_write_max_us,
                    stats.apply_read_count,
                    stats.apply_read_total_us,
                    stats.apply_read_max_us,
                    stats.apply_batch_count,
                    stats.apply_batch_total_us,
                    stats.apply_batch_max_us,
                    stats.mark_visible_count,
                    stats.mark_visible_total_us,
                    stats.mark_visible_max_us,
                    stats.state_update_count,
                    stats.state_update_total_us,
                    stats.state_update_max_us,
                    stats.fast_path_count,
                    stats.slow_path_count,
                    rpc_stats.pre_accept_batches,
                    rpc_stats.pre_accept_items,
                    pre_accept_avg_batch,
                    rpc_stats.pre_accept_max_batch,
                    rpc_stats.accept_batches,
                    rpc_stats.accept_items,
                    accept_avg_batch,
                    rpc_stats.accept_max_batch,
                    rpc_stats.commit_batches,
                    rpc_stats.commit_items,
                    commit_avg_batch,
                    rpc_stats.commit_max_batch,
                    client_batch.set_batches,
                    client_batch.set_items,
                    client_set_avg_batch,
                    client_batch.set_max_items,
                    client_set_avg_wait_us,
                    client_batch.set_wait_max_us,
                    client_batch.get_batches,
                    client_batch.get_items,
                    client_get_avg_batch,
                    client_batch.get_max_items,
                    client_get_avg_wait_us,
                    client_batch.get_wait_max_us,
                    wal_stats.fsync_count,
                    wal_stats.fsync_total_us,
                    wal_fsync_avg_us,
                    wal_stats.fsync_max_us,
                    wal_stats.batch_count,
                    wal_stats.batch_items,
                    wal_batch_avg_items,
                    wal_stats.batch_max_items,
                    wal_stats.batch_total_bytes,
                    wal_batch_avg_bytes,
                    wal_stats.batch_max_bytes,
                );
                feed_resp(
                    &mut framed,
                    BytesFrame::BulkString(bytes::Bytes::from(msg.into_bytes())),
                )
                .await?;
                flush_resp(&mut framed).await?;
            }
            KvOp::Get { key } => {
                let mut keys: Vec<Vec<u8>> = Vec::with_capacity(max_batched_get_ops);
                keys.push(key);

                while keys.len() < max_batched_get_ops && !stream_closed {
                    match framed.next().now_or_never() {
                        Some(Some(Ok(frame))) => match parse_command(frame) {
                            Ok(Some(KvOp::Get { key })) => keys.push(key),
                            Ok(Some(other)) => {
                                pending_op = Some(other);
                                break;
                            }
                            Ok(None) => {}
                            Err(err) => {
                                pending_resp = Some(BytesFrame::Error(format!("ERR {err}").into()));
                                break;
                            }
                        },
                        Some(Some(Err(err))) => anyhow::bail!("failed to read RESP frame: {err}"),
                        Some(None) => {
                            stream_closed = true;
                            break;
                        }
                        None => break,
                    }
                }

                let batch_len = keys.len();
                if batch_len == 1 {
                    let key = keys.pop().unwrap_or_default();
                    let result = state.execute(KvOp::Get { key }).await;
                    let resp = match result {
                        Ok(KvResult::Ok) => BytesFrame::Error("ERR GET returned OK".into()),
                        Ok(KvResult::Value(None)) => BytesFrame::Null,
                        Ok(KvResult::Value(Some(v))) => {
                            BytesFrame::BulkString(bytes::Bytes::from(v))
                        }
                        Err(err) => BytesFrame::Error(format!("ERR {err}").into()),
                    };
                    feed_resp(&mut framed, resp).await?;
                    flush_resp(&mut framed).await?;
                } else {
                    match state.execute_batch_get(keys).await {
                        Ok(values) => {
                            for v in values {
                                let frame = match v {
                                    None => BytesFrame::Null,
                                    Some(bytes) => {
                                        BytesFrame::BulkString(bytes::Bytes::from(bytes))
                                    }
                                };
                                feed_resp(&mut framed, frame).await?;
                            }
                            flush_resp(&mut framed).await?;
                        }
                        Err(err) => {
                            let msg = format!("ERR {err}");
                            for _ in 0..batch_len {
                                feed_resp(&mut framed, BytesFrame::Error(msg.clone().into()))
                                    .await?;
                            }
                            flush_resp(&mut framed).await?;
                        }
                    }
                }
            }
            KvOp::Set { key, value } => {
                let mut batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(max_batched_set_ops);
                batch.push((key, value));

                while batch.len() < max_batched_set_ops && !stream_closed {
                    match framed.next().now_or_never() {
                        Some(Some(Ok(frame))) => match parse_command(frame) {
                            Ok(Some(KvOp::Set { key, value })) => batch.push((key, value)),
                            Ok(Some(other)) => {
                                pending_op = Some(other);
                                break;
                            }
                            Ok(None) => {}
                            Err(err) => {
                                pending_resp = Some(BytesFrame::Error(format!("ERR {err}").into()));
                                break;
                            }
                        },
                        Some(Some(Err(err))) => anyhow::bail!("failed to read RESP frame: {err}"),
                        Some(None) => {
                            stream_closed = true;
                            break;
                        }
                        None => break,
                    }
                }

                let batch_len = batch.len();
                if batch_len == 1 {
                    let (key, value) = batch.pop().unwrap_or_default();
                    let result = state.execute(KvOp::Set { key, value }).await;
                    let resp = match result {
                        Ok(KvResult::Ok) => {
                            BytesFrame::SimpleString(bytes::Bytes::from_static(b"OK"))
                        }
                        Ok(KvResult::Value(_)) => {
                            BytesFrame::Error("ERR SET returned value".into())
                        }
                        Err(err) => BytesFrame::Error(format!("ERR {err}").into()),
                    };
                    feed_resp(&mut framed, resp).await?;
                    flush_resp(&mut framed).await?;
                } else {
                    match state.execute_batch_set(batch).await {
                        Ok(()) => {
                            for _ in 0..batch_len {
                                feed_resp(
                                    &mut framed,
                                    BytesFrame::SimpleString(bytes::Bytes::from_static(b"OK")),
                                )
                                .await?;
                            }
                            flush_resp(&mut framed).await?;
                        }
                        Err(err) => {
                            let msg = format!("ERR {err}");
                            for _ in 0..batch_len {
                                feed_resp(&mut framed, BytesFrame::Error(msg.clone().into()))
                                    .await?;
                            }
                            flush_resp(&mut framed).await?;
                        }
                    }
                }
            }
        }
    }

    flush_resp(&mut framed).await?;
    Ok(())
}

fn parse_command(frame: BytesFrame) -> anyhow::Result<Option<KvOp>> {
    let BytesFrame::Array(parts) = frame else {
        anyhow::bail!("expected array frame");
    };

    if parts.is_empty() {
        return Ok(None);
    }

    let cmd = frame_str_upper(&parts[0]).ok_or_else(|| anyhow::anyhow!("invalid command"))?;
    match cmd.as_str() {
        "HOLOSTATS" => {
            anyhow::ensure!(parts.len() == 1, "HOLOSTATS expects 0 arguments");
            Ok(Some(KvOp::HoloStats))
        }
        "GET" => {
            anyhow::ensure!(parts.len() == 2, "GET expects 1 argument");
            let key = frame_bytes(&parts[1]).ok_or_else(|| anyhow::anyhow!("invalid key"))?;
            Ok(Some(KvOp::Get { key }))
        }
        "SET" => {
            anyhow::ensure!(parts.len() == 3, "SET expects 2 arguments");
            let key = frame_bytes(&parts[1]).ok_or_else(|| anyhow::anyhow!("invalid key"))?;
            let value = frame_bytes(&parts[2]).ok_or_else(|| anyhow::anyhow!("invalid value"))?;
            Ok(Some(KvOp::Set { key, value }))
        }
        other => anyhow::bail!("unknown command {other}"),
    }
}

fn frame_str_upper(frame: &BytesFrame) -> Option<String> {
    frame.as_str().map(|s| s.to_ascii_uppercase())
}

fn frame_bytes(frame: &BytesFrame) -> Option<Vec<u8>> {
    match frame {
        BytesFrame::BulkString(b) | BytesFrame::SimpleString(b) => Some(b.to_vec()),
        _ => None,
    }
}

fn read_env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}
