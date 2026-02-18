use std::net::SocketAddr as EmbedSocketAddr;
use std::path::PathBuf as EmbedPathBuf;
use std::time::Duration as EmbedDuration;

include!("main.rs");

/// Minimal embeddable configuration for running a HoloStore node inside another process.
#[derive(Clone, Debug)]
pub struct EmbeddedNodeConfig {
    pub node_id: u64,
    pub listen_redis: EmbedSocketAddr,
    pub listen_grpc: EmbedSocketAddr,
    pub bootstrap: bool,
    pub join: Option<EmbedSocketAddr>,
    pub initial_members: String,
    pub data_dir: EmbedPathBuf,
    pub ready_timeout: EmbedDuration,
    pub max_shards: usize,
    pub initial_ranges: usize,
    /// Optional routing mode override for data keys (`hash` or `range`).
    pub routing_mode: Option<String>,
}

impl EmbeddedNodeConfig {
    pub fn single_node(
        node_id: u64,
        listen_redis: EmbedSocketAddr,
        listen_grpc: EmbedSocketAddr,
        data_dir: EmbedPathBuf,
    ) -> Self {
        Self {
            node_id,
            listen_redis,
            listen_grpc,
            bootstrap: true,
            join: None,
            initial_members: format!("{node_id}@{listen_grpc}"),
            data_dir,
            ready_timeout: EmbedDuration::from_secs(20),
            max_shards: 1,
            initial_ranges: 1,
            routing_mode: None,
        }
    }
}

pub struct EmbeddedNodeHandle {
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl EmbeddedNodeHandle {
    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        match self.task.await {
            Ok(res) => res,
            Err(err) => Err(anyhow::anyhow!("holo_store task join failed: {err}")),
        }
    }

    pub fn abort(&self) {
        self.task.abort();
    }
}

pub fn build_node_args(config: &EmbeddedNodeConfig) -> anyhow::Result<NodeArgs> {
    use clap::Parser;

    let mut argv = vec![
        "holo-store-node".to_string(),
        "--node-id".to_string(),
        config.node_id.to_string(),
        "--listen-redis".to_string(),
        config.listen_redis.to_string(),
        "--listen-grpc".to_string(),
        config.listen_grpc.to_string(),
        "--initial-members".to_string(),
        config.initial_members.clone(),
        "--data-dir".to_string(),
        config.data_dir.display().to_string(),
        "--max-shards".to_string(),
        config.max_shards.max(1).to_string(),
        "--initial-ranges".to_string(),
        config
            .initial_ranges
            .max(1)
            .min(config.max_shards.max(1))
            .to_string(),
    ];

    if config.bootstrap {
        argv.push("--bootstrap".to_string());
    } else if let Some(join) = config.join {
        argv.push("--join".to_string());
        argv.push(join.to_string());
    } else {
        anyhow::bail!("embedded node config requires either bootstrap=true or join address");
    }

    if let Some(routing_mode) = config
        .routing_mode
        .as_ref()
        .map(|mode| mode.trim())
        .filter(|mode| !mode.is_empty())
    {
        argv.push("--routing-mode".to_string());
        argv.push(routing_mode.to_string());
    }

    NodeArgs::try_parse_from(argv).map_err(|err| anyhow::anyhow!(err.to_string()))
}

pub async fn start_embedded_node(config: EmbeddedNodeConfig) -> anyhow::Result<EmbeddedNodeHandle> {
    let args = build_node_args(&config)?;
    let wait_grpc = config.listen_grpc;
    let wait_redis = config.listen_redis;
    let wait_timeout = config.ready_timeout.max(EmbedDuration::from_secs(1));

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        run_node_with_shutdown(args, async move {
            let _ = shutdown_rx.await;
            Ok::<(), std::io::Error>(())
        })
        .await
    });

    wait_for_listeners(wait_grpc, wait_redis, wait_timeout, &task).await?;

    Ok(EmbeddedNodeHandle {
        shutdown_tx: Some(shutdown_tx),
        task,
    })
}

async fn wait_for_listeners(
    grpc_addr: EmbedSocketAddr,
    redis_addr: EmbedSocketAddr,
    timeout: EmbedDuration,
    task: &tokio::task::JoinHandle<anyhow::Result<()>>,
) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    let mut grpc_ready = false;
    let mut redis_ready = false;

    loop {
        if task.is_finished() {
            return Err(anyhow::anyhow!(
                "embedded holo_store exited before listeners became ready"
            ));
        }

        if !grpc_ready {
            grpc_ready = tokio::net::TcpStream::connect(grpc_addr).await.is_ok();
        }
        if !redis_ready {
            redis_ready = tokio::net::TcpStream::connect(redis_addr).await.is_ok();
        }

        if grpc_ready && redis_ready {
            return Ok(());
        }

        if std::time::Instant::now() >= deadline {
            return Err(anyhow::anyhow!(
                "timeout waiting for holo_store listeners (grpc_ready={grpc_ready}, redis_ready={redis_ready}); grpc={grpc_addr}, redis={redis_addr}"
            ));
        }

        tokio::time::sleep(EmbedDuration::from_millis(100)).await;
    }
}

/// Simplified version metadata used by the public client API.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RpcVersion {
    pub seq: u64,
    pub txn_node_id: u64,
    pub txn_counter: u64,
}

impl RpcVersion {
    pub const fn zero() -> Self {
        Self {
            seq: 0,
            txn_node_id: 0,
            txn_counter: 0,
        }
    }
}

/// One latest-visible KV row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LatestEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub version: RpcVersion,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConditionalLatestEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub version: RpcVersion,
    pub expected_version: RpcVersion,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicatedWriteEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicatedConditionalWriteEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub expected_version: RpcVersion,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConditionalApplyResult {
    pub applied: u64,
    pub conflicts: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConditionalAppliedVersion {
    pub key: Vec<u8>,
    pub version: RpcVersion,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicatedConditionalApplyResult {
    pub applied: u64,
    pub conflicts: u64,
    pub applied_versions: Vec<ConditionalAppliedVersion>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeStat {
    pub shard_id: u64,
    pub shard_index: usize,
    pub record_count: u64,
    pub is_leaseholder: bool,
}

#[derive(Clone)]
pub struct HoloStoreClient {
    target: EmbedSocketAddr,
    timeout: EmbedDuration,
    client: volo_gen::holo_store::rpc::HoloRpcClient,
}

impl HoloStoreClient {
    pub fn with_timeout(target: EmbedSocketAddr, timeout: EmbedDuration) -> Self {
        let client = volo_gen::holo_store::rpc::HoloRpcClientBuilder::new("holo_store.rpc.HoloRpc")
            .address(volo::net::Address::from(target))
            .build();

        Self {
            target,
            timeout: timeout.max(EmbedDuration::from_millis(1)),
            client,
        }
    }

    pub fn new(target: EmbedSocketAddr) -> Self {
        Self::with_timeout(target, EmbedDuration::from_secs(10))
    }

    pub fn target(&self) -> EmbedSocketAddr {
        self.target
    }

    pub fn timeout(&self) -> EmbedDuration {
        self.timeout
    }

    pub async fn cluster_state_json(&self) -> anyhow::Result<String> {
        let request = volo_gen::holo_store::rpc::ClusterStateRequest {};
        let response = tokio::time::timeout(self.timeout, self.client.cluster_state(request))
            .await
            .map_err(|_| anyhow::anyhow!("cluster_state rpc timed out for {}", self.target))?
            .map_err(|err| {
                anyhow::anyhow!("cluster_state rpc failed for {}: {err}", self.target)
            })?;
        Ok(response.into_inner().json.to_string())
    }

    pub async fn range_stats(&self) -> anyhow::Result<Vec<RangeStat>> {
        let request = volo_gen::holo_store::rpc::RangeStatsRequest {};
        let response = tokio::time::timeout(self.timeout, self.client.range_stats(request))
            .await
            .map_err(|_| anyhow::anyhow!("range_stats rpc timed out for {}", self.target))?
            .map_err(|err| anyhow::anyhow!("range_stats rpc failed for {}: {err}", self.target))?
            .into_inner();

        let mut out = Vec::with_capacity(response.ranges.len());
        for item in response.ranges {
            out.push(RangeStat {
                shard_id: item.shard_id,
                shard_index: item.shard_index as usize,
                record_count: item.record_count,
                is_leaseholder: item.is_leaseholder,
            });
        }
        Ok(out)
    }

    pub async fn range_snapshot_latest(
        &self,
        shard_index: usize,
        start_key: &[u8],
        end_key: &[u8],
        cursor: &[u8],
        limit: usize,
        reverse: bool,
    ) -> anyhow::Result<(Vec<LatestEntry>, Vec<u8>, bool)> {
        let request = volo_gen::holo_store::rpc::RangeSnapshotLatestRequest {
            shard_index: shard_index as u64,
            start_key: start_key.to_vec().into(),
            end_key: end_key.to_vec().into(),
            cursor: cursor.to_vec().into(),
            limit: limit.clamp(1, u32::MAX as usize) as u32,
            reverse,
        };

        let response =
            tokio::time::timeout(self.timeout, self.client.range_snapshot_latest(request))
                .await
                .map_err(|_| {
                    anyhow::anyhow!("range_snapshot_latest rpc timed out for {}", self.target)
                })?
                .map_err(|err| {
                    anyhow::anyhow!(
                        "range_snapshot_latest rpc failed for {}: {err}",
                        self.target
                    )
                })?
                .into_inner();

        let mut entries = Vec::with_capacity(response.entries.len());
        for entry in response.entries {
            entries.push(LatestEntry {
                key: entry.key.to_vec(),
                value: entry.value.to_vec(),
                version: from_rpc_version_required(entry.version)?,
            });
        }

        Ok((entries, response.next_cursor.to_vec(), response.done))
    }

    pub async fn range_apply_latest(
        &self,
        shard_index: usize,
        start_key: &[u8],
        end_key: &[u8],
        entries: Vec<LatestEntry>,
    ) -> anyhow::Result<u64> {
        let rpc_entries = entries
            .into_iter()
            .map(
                |entry| volo_gen::holo_store::rpc::RangeSnapshotLatestEntry {
                    key: entry.key.into(),
                    value: entry.value.into(),
                    version: Some(to_rpc_version(entry.version)),
                },
            )
            .collect();

        let request = volo_gen::holo_store::rpc::RangeApplyLatestRequest {
            shard_index: shard_index as u64,
            start_key: start_key.to_vec().into(),
            end_key: end_key.to_vec().into(),
            entries: rpc_entries,
            admin: true,
        };

        let response = tokio::time::timeout(self.timeout, self.client.range_apply_latest(request))
            .await
            .map_err(|_| anyhow::anyhow!("range_apply_latest rpc timed out for {}", self.target))?
            .map_err(|err| {
                anyhow::anyhow!("range_apply_latest rpc failed for {}: {err}", self.target)
            })?;

        Ok(response.into_inner().applied)
    }

    pub async fn range_apply_latest_conditional(
        &self,
        shard_index: usize,
        start_key: &[u8],
        end_key: &[u8],
        entries: Vec<ConditionalLatestEntry>,
    ) -> anyhow::Result<ConditionalApplyResult> {
        let rpc_entries = entries
            .into_iter()
            .map(
                |entry| volo_gen::holo_store::rpc::RangeApplyLatestConditionalEntry {
                    key: entry.key.into(),
                    value: entry.value.into(),
                    version: Some(to_rpc_version(entry.version)),
                    expected_version: Some(to_rpc_version(entry.expected_version)),
                },
            )
            .collect();

        let request = volo_gen::holo_store::rpc::RangeApplyLatestConditionalRequest {
            shard_index: shard_index as u64,
            start_key: start_key.to_vec().into(),
            end_key: end_key.to_vec().into(),
            entries: rpc_entries,
            admin: true,
        };

        let response = tokio::time::timeout(
            self.timeout,
            self.client.range_apply_latest_conditional(request),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "range_apply_latest_conditional rpc timed out for {}",
                self.target
            )
        })?
        .map_err(|err| {
            anyhow::anyhow!(
                "range_apply_latest_conditional rpc failed for {}: {err}",
                self.target
            )
        })?
        .into_inner();

        Ok(ConditionalApplyResult {
            applied: response.applied,
            conflicts: response.conflicts,
        })
    }

    pub async fn range_write_latest(
        &self,
        shard_index: usize,
        start_key: &[u8],
        end_key: &[u8],
        entries: Vec<ReplicatedWriteEntry>,
    ) -> anyhow::Result<u64> {
        let rpc_entries = entries
            .into_iter()
            .map(|entry| volo_gen::holo_store::rpc::RangeWriteLatestEntry {
                key: entry.key.into(),
                value: entry.value.into(),
            })
            .collect();

        let request = volo_gen::holo_store::rpc::RangeWriteLatestRequest {
            shard_index: shard_index as u64,
            start_key: start_key.to_vec().into(),
            end_key: end_key.to_vec().into(),
            entries: rpc_entries,
        };

        let response = tokio::time::timeout(self.timeout, self.client.range_write_latest(request))
            .await
            .map_err(|_| anyhow::anyhow!("range_write_latest rpc timed out for {}", self.target))?
            .map_err(|err| {
                anyhow::anyhow!("range_write_latest rpc failed for {}: {err}", self.target)
            })?
            .into_inner();

        Ok(response.applied)
    }

    pub async fn range_write_latest_conditional(
        &self,
        shard_index: usize,
        start_key: &[u8],
        end_key: &[u8],
        entries: Vec<ReplicatedConditionalWriteEntry>,
    ) -> anyhow::Result<ReplicatedConditionalApplyResult> {
        let rpc_entries = entries
            .into_iter()
            .map(
                |entry| volo_gen::holo_store::rpc::RangeWriteLatestConditionalEntry {
                    key: entry.key.into(),
                    value: entry.value.into(),
                    expected_version: Some(to_rpc_version(entry.expected_version)),
                },
            )
            .collect();

        let request = volo_gen::holo_store::rpc::RangeWriteLatestConditionalRequest {
            shard_index: shard_index as u64,
            start_key: start_key.to_vec().into(),
            end_key: end_key.to_vec().into(),
            entries: rpc_entries,
        };

        let response = tokio::time::timeout(
            self.timeout,
            self.client.range_write_latest_conditional(request),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "range_write_latest_conditional rpc timed out for {}",
                self.target
            )
        })?
        .map_err(|err| {
            anyhow::anyhow!(
                "range_write_latest_conditional rpc failed for {}: {err}",
                self.target
            )
        })?
        .into_inner();

        let mut applied_versions = Vec::with_capacity(response.applied_versions.len());
        for item in response.applied_versions {
            applied_versions.push(ConditionalAppliedVersion {
                key: item.key.to_vec(),
                version: from_rpc_version_required(item.version)?,
            });
        }

        Ok(ReplicatedConditionalApplyResult {
            applied: response.applied,
            conflicts: response.conflicts,
            applied_versions,
        })
    }

    pub async fn range_split(&self, split_key: &[u8]) -> anyhow::Result<(u64, u64)> {
        let request = volo_gen::holo_store::rpc::RangeSplitRequest {
            split_key: split_key.to_vec().into(),
        };

        let response = tokio::time::timeout(self.timeout, self.client.range_split(request))
            .await
            .map_err(|_| anyhow::anyhow!("range_split rpc timed out for {}", self.target))?
            .map_err(|err| anyhow::anyhow!("range_split rpc failed for {}: {err}", self.target))?
            .into_inner();

        Ok((response.left_shard_id, response.right_shard_id))
    }

    pub async fn range_rebalance(
        &self,
        shard_id: u64,
        replicas: &[u64],
        leaseholder: Option<u64>,
    ) -> anyhow::Result<()> {
        if replicas.is_empty() {
            anyhow::bail!("range_rebalance requires at least one replica");
        }

        let request = volo_gen::holo_store::rpc::RangeRebalanceRequest {
            shard_id,
            replicas: replicas.to_vec(),
            leaseholder: leaseholder.unwrap_or(0),
        };

        tokio::time::timeout(self.timeout, self.client.range_rebalance(request))
            .await
            .map_err(|_| anyhow::anyhow!("range_rebalance rpc timed out for {}", self.target))?
            .map_err(|err| {
                anyhow::anyhow!("range_rebalance rpc failed for {}: {err}", self.target)
            })?;

        Ok(())
    }
}

impl std::fmt::Debug for HoloStoreClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HoloStoreClient")
            .field("target", &self.target)
            .field("timeout", &self.timeout)
            .finish()
    }
}

fn to_rpc_version(version: RpcVersion) -> volo_gen::holo_store::rpc::Version {
    volo_gen::holo_store::rpc::Version {
        seq: version.seq,
        txn_id: Some(volo_gen::holo_store::rpc::TxnId {
            node_id: version.txn_node_id,
            counter: version.txn_counter,
        }),
    }
}

fn from_rpc_version_required(
    version: Option<volo_gen::holo_store::rpc::Version>,
) -> anyhow::Result<RpcVersion> {
    let version = version.ok_or_else(|| anyhow::anyhow!("missing rpc version"))?;
    let txn_id = version
        .txn_id
        .ok_or_else(|| anyhow::anyhow!("missing rpc version txn_id"))?;

    Ok(RpcVersion {
        seq: version.seq,
        txn_node_id: txn_id.node_id,
        txn_counter: txn_id.counter,
    })
}
