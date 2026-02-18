//! HoloFusion runtime wiring for embedded HoloStore + DataFusion/Ballista + PostgreSQL protocol.
//!
//! A single HoloFusion process hosts:
//! - an embedded HoloStore node for replicated storage,
//! - a DataFusion/Ballista SQL engine for query planning/execution,
//! - a PostgreSQL wire server for client compatibility.
//!
//! This module is the composition root that starts these subsystems,
//! bootstraps catalog metadata, and exposes health endpoints.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use ballista::prelude::{SessionConfigExt, SessionContextExt};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_pg_catalog::setup_pg_catalog;
use datafusion_postgres::auth::AuthManager;
use datafusion_postgres::hooks::set_show::SetShowHook;
use datafusion_postgres::QueryHook;
use datafusion_postgres::{serve_with_hooks, ServerOptions};
use holo_store::{start_embedded_node, EmbeddedNodeConfig, HoloStoreClient};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tracing::{info, warn};

mod ballista_codec;
mod catalog;
pub mod metadata;
pub mod metrics;
mod mutation;
mod pg_catalog_ext;
mod pg_compat;
pub mod provider;
pub mod topology;

use ballista_codec::HoloFusionLogicalExtensionCodec;
use catalog::{bootstrap_catalog, sync_catalog_from_metadata};
use metadata::ensure_metadata_migration;
use metrics::PushdownMetrics;
use mutation::{DmlHook, DmlRuntimeConfig};
use pg_catalog_ext::install_pg_locks_table;
use pg_compat::{current_unix_timestamp_ns, register_pg_compat_udfs};

/// Interval used by the background metadata-to-catalog synchronization loop.
const CATALOG_SYNC_INTERVAL: Duration = Duration::from_secs(1);

/// Coarse runtime health states surfaced by the health HTTP endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthState {
    /// Startup is in progress.
    Bootstrapping = 0,
    /// All primary subsystems are available.
    Ready = 1,
    /// Service is functional but with reduced capability.
    Degraded = 2,
    /// Service is intentionally not ready (shutdown or fatal issue).
    NotReady = 3,
}

impl HealthState {
    /// Reconstructs enum state from an atomic byte value.
    fn from_u8(raw: u8) -> Self {
        // Decision: unknown values default to `Bootstrapping` to avoid
        // accidentally reporting readiness on corrupted state.
        // Decision: evaluate `match raw {` to choose the correct SQL/storage control path.
        match raw {
            1 => Self::Ready,
            2 => Self::Degraded,
            3 => Self::NotReady,
            _ => Self::Bootstrapping,
        }
    }
}

/// Shared mutable runtime health object used by server tasks.
#[derive(Clone)]
pub struct RuntimeHealth {
    /// Atomic enum backing store for fast read access.
    state: Arc<AtomicU8>,
    /// Human-readable detail string paired with state.
    detail: Arc<tokio::sync::RwLock<String>>,
}

impl RuntimeHealth {
    /// Creates an initial bootstrapping health state.
    fn new() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(HealthState::Bootstrapping as u8)),
            detail: Arc::new(tokio::sync::RwLock::new("bootstrapping".to_string())),
        }
    }

    /// Returns the current coarse health state.
    pub fn state(&self) -> HealthState {
        HealthState::from_u8(self.state.load(Ordering::SeqCst))
    }

    /// Returns the current detailed health message.
    pub async fn detail(&self) -> String {
        self.detail.read().await.clone()
    }

    /// Updates both coarse state and detailed message atomically enough for health checks.
    async fn set(&self, state: HealthState, detail: impl Into<String>) {
        self.state.store(state as u8, Ordering::SeqCst);
        *self.detail.write().await = detail.into();
    }
}

/// Runtime configuration for one HoloFusion node process.
#[derive(Clone, Debug)]
pub struct HoloFusionConfig {
    /// PostgreSQL wire host.
    pub pg_host: String,
    /// PostgreSQL wire port.
    pub pg_port: u16,
    /// Health HTTP bind address.
    pub health_addr: SocketAddr,
    /// Enables Ballista standalone SQL mode when `true`.
    pub enable_ballista_sql: bool,
    /// Optional artificial prewrite delay used by DML path tests.
    pub dml_prewrite_delay: Duration,
    /// Timeout for SQL statements handled by DML/transaction hook (`0` disables timeout).
    pub dml_statement_timeout: Duration,
    /// Maximum number of concurrent hook-managed statements admitted at once.
    pub dml_max_inflight_statements: usize,
    /// Maximum concurrent read statements admitted by the SQL admission controller.
    pub dml_max_inflight_reads: usize,
    /// Maximum concurrent write statements admitted by the SQL admission controller.
    pub dml_max_inflight_writes: usize,
    /// Maximum concurrent explicit-transaction statements admitted by the SQL admission controller.
    pub dml_max_inflight_txns: usize,
    /// Maximum concurrent background/elastic statements admitted by the SQL admission controller.
    pub dml_max_inflight_background: usize,
    /// Maximum queued statements allowed per admission class before overload rejection.
    pub dml_admission_queue_limit: usize,
    /// Maximum time one statement may wait in admission queue (`0` disables wait and rejects immediately).
    pub dml_admission_wait_timeout: Duration,
    /// Maximum number of rows a single mutation/snapshot scan can materialize.
    pub dml_max_scan_rows: usize,
    /// Maximum number of staged rows allowed in one explicit transaction.
    pub dml_max_txn_staged_rows: usize,
    /// Embedded HoloStore node configuration.
    pub holostore: EmbeddedNodeConfig,
}

impl HoloFusionConfig {
    /// Loads configuration from environment variables with sensible defaults.
    pub fn from_env() -> Result<Self> {
        let pg_host =
            std::env::var("HOLO_FUSION_PG_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let pg_port = parse_port(std::env::var("HOLO_FUSION_PG_PORT").ok(), 55432)?;
        let health_addr = parse_socket_addr(
            std::env::var("HOLO_FUSION_HEALTH_ADDR").ok(),
            "127.0.0.1:18081",
        )?;
        let enable_ballista_sql = std::env::var("HOLO_FUSION_ENABLE_BALLISTA_SQL")
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(true);
        let dml_prewrite_delay_ms =
            parse_u64(std::env::var("HOLO_FUSION_DML_PREWRITE_DELAY_MS").ok(), 0)?;
        let dml_statement_timeout_ms = parse_u64(
            std::env::var("HOLO_FUSION_DML_STATEMENT_TIMEOUT_MS").ok(),
            0,
        )?;
        let dml_max_inflight_statements = parse_usize(
            std::env::var("HOLO_FUSION_DML_MAX_INFLIGHT_STATEMENTS").ok(),
            1024,
        )?
        .max(1);
        let dml_max_inflight_reads = parse_usize(
            std::env::var("HOLO_FUSION_DML_MAX_INFLIGHT_READS").ok(),
            dml_max_inflight_statements,
        )?
        .max(1);
        let dml_max_inflight_writes = parse_usize(
            std::env::var("HOLO_FUSION_DML_MAX_INFLIGHT_WRITES").ok(),
            dml_max_inflight_statements,
        )?
        .max(1);
        let dml_max_inflight_txns = parse_usize(
            std::env::var("HOLO_FUSION_DML_MAX_INFLIGHT_TXNS").ok(),
            (dml_max_inflight_statements / 2).max(1),
        )?
        .max(1);
        let dml_max_inflight_background = parse_usize(
            std::env::var("HOLO_FUSION_DML_MAX_INFLIGHT_BACKGROUND").ok(),
            (dml_max_inflight_statements / 4).max(1),
        )?
        .max(1);
        let dml_admission_queue_limit = parse_usize(
            std::env::var("HOLO_FUSION_DML_ADMISSION_QUEUE_LIMIT").ok(),
            4096,
        )?
        .max(1);
        let dml_admission_wait_timeout_ms = parse_u64(
            std::env::var("HOLO_FUSION_DML_ADMISSION_WAIT_TIMEOUT_MS").ok(),
            0,
        )?;
        let dml_max_scan_rows =
            parse_usize(std::env::var("HOLO_FUSION_DML_MAX_SCAN_ROWS").ok(), 100_000)?.max(1);
        let dml_max_txn_staged_rows = parse_usize(
            std::env::var("HOLO_FUSION_DML_MAX_TXN_STAGED_ROWS").ok(),
            100_000,
        )?
        .max(1);

        let node_id = std::env::var("HOLO_FUSION_NODE_ID")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(1);
        let redis_addr = parse_socket_addr(
            std::env::var("HOLO_FUSION_HOLOSTORE_REDIS_ADDR").ok(),
            "127.0.0.1:16379",
        )?;
        let grpc_addr = parse_socket_addr(
            std::env::var("HOLO_FUSION_HOLOSTORE_GRPC_ADDR").ok(),
            "127.0.0.1:15051",
        )?;
        let data_dir = std::env::var("HOLO_FUSION_HOLOSTORE_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(format!(".holo_fusion/node-{node_id}")));
        let bootstrap = std::env::var("HOLO_FUSION_BOOTSTRAP")
            .ok()
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(true);
        let join = std::env::var("HOLO_FUSION_JOIN_ADDR")
            .ok()
            .map(|v| parse_socket_addr(Some(v), "0.0.0.0:0"))
            .transpose()?;
        let initial_members = std::env::var("HOLO_FUSION_INITIAL_MEMBERS")
            .unwrap_or_else(|_| format!("{node_id}@{grpc_addr}"));
        let max_shards =
            parse_usize(std::env::var("HOLO_FUSION_HOLOSTORE_MAX_SHARDS").ok(), 1)?.max(1);
        let initial_ranges = parse_usize(
            std::env::var("HOLO_FUSION_HOLOSTORE_INITIAL_RANGES").ok(),
            1,
        )?
        .max(1)
        .min(max_shards);
        // Decision: accept only known routing modes so invalid values do not
        // silently alter storage behavior.
        let routing_mode = std::env::var("HOLO_FUSION_HOLOSTORE_ROUTING_MODE")
            .ok()
            .map(|v| v.trim().to_ascii_lowercase())
            .filter(|v| v == "hash" || v == "range");

        Ok(Self {
            pg_host,
            pg_port,
            health_addr,
            enable_ballista_sql,
            dml_prewrite_delay: Duration::from_millis(dml_prewrite_delay_ms),
            dml_statement_timeout: Duration::from_millis(dml_statement_timeout_ms),
            dml_max_inflight_statements,
            dml_max_inflight_reads,
            dml_max_inflight_writes,
            dml_max_inflight_txns,
            dml_max_inflight_background,
            dml_admission_queue_limit,
            dml_admission_wait_timeout: Duration::from_millis(dml_admission_wait_timeout_ms),
            dml_max_scan_rows,
            dml_max_txn_staged_rows,
            holostore: EmbeddedNodeConfig {
                node_id,
                listen_redis: redis_addr,
                listen_grpc: grpc_addr,
                bootstrap,
                join,
                initial_members,
                data_dir,
                ready_timeout: Duration::from_secs(20),
                max_shards,
                initial_ranges,
                routing_mode,
            },
        })
    }
}

/// Runs the server until Ctrl-C is received.
pub async fn run(config: HoloFusionConfig) -> Result<()> {
    run_with_shutdown(config, tokio::signal::ctrl_c()).await
}

/// Runs the full HoloFusion runtime with an externally supplied shutdown signal.
pub async fn run_with_shutdown<F>(config: HoloFusionConfig, shutdown: F) -> Result<()>
where
    F: std::future::Future<Output = Result<(), std::io::Error>> + Send,
{
    let default_target_partitions = std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(4)
        .max(1);
    let sql_target_partitions = parse_usize(
        std::env::var("HOLO_FUSION_SQL_TARGET_PARTITIONS").ok(),
        default_target_partitions,
    )?
    .max(1);
    let sql_sort_spill_reservation_bytes = parse_usize(
        std::env::var("HOLO_FUSION_SQL_SORT_SPILL_RESERVATION_BYTES").ok(),
        16 * 1024 * 1024,
    )?
    .max(1);
    let sql_spill_compression =
        normalize_spill_compression(std::env::var("HOLO_FUSION_SQL_SPILL_COMPRESSION").ok());

    let postmaster_start_time_ns =
        current_unix_timestamp_ns().context("capture pg postmaster start time")?;
    let health = RuntimeHealth::new();
    let pushdown_metrics = Arc::new(PushdownMetrics::default());
    health
        .set(HealthState::Bootstrapping, "starting health server")
        .await;

    let (health_shutdown_tx, health_shutdown_rx) = watch::channel(false);
    let health_task = tokio::spawn(run_health_server(
        config.health_addr,
        health.clone(),
        pushdown_metrics.clone(),
        health_shutdown_rx,
    ));

    health
        .set(HealthState::Bootstrapping, "starting embedded holostore")
        .await;
    let holostore = start_embedded_node(config.holostore.clone())
        .await
        .context("start embedded holostore")?;
    let holostore_client = HoloStoreClient::new(config.holostore.listen_grpc);

    health
        .set(HealthState::Bootstrapping, "starting embedded ballista")
        .await;

    // Decision: prefer Ballista standalone mode when enabled, but degrade to a
    // local DataFusion context if Ballista startup fails.
    let (session_context, ballista_mode) = if config.enable_ballista_sql {
        let codec = Arc::new(HoloFusionLogicalExtensionCodec::new(
            pushdown_metrics.clone(),
        ));
        let session_config = configured_session_config(
            SessionConfig::new_with_ballista(),
            sql_target_partitions,
            sql_sort_spill_reservation_bytes,
            sql_spill_compression.as_str(),
        )
        .with_information_schema(true)
        .with_ballista_logical_extension_codec(codec);
        let session_state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(session_config)
            .build();
        // Decision: evaluate `match SessionContext::standalone_with_state(session_state).await {` to choose the correct SQL/storage control path.
        match SessionContext::standalone_with_state(session_state).await {
            Ok(ctx) => (Arc::new(ctx), BallistaMode::Standalone),
            Err(err) => {
                health
                    .set(
                        HealthState::Degraded,
                        format!("ballista unavailable, falling back to local datafusion: {err}"),
                    )
                    .await;
                (
                    Arc::new(SessionContext::new_with_config(configured_session_config(
                        SessionConfig::new(),
                        sql_target_partitions,
                        sql_sort_spill_reservation_bytes,
                        sql_spill_compression.as_str(),
                    ))),
                    BallistaMode::LocalFallback,
                )
            }
        }
    } else {
        (
            Arc::new(SessionContext::new_with_config(configured_session_config(
                SessionConfig::new(),
                sql_target_partitions,
                sql_sort_spill_reservation_bytes,
                sql_spill_compression.as_str(),
            ))),
            BallistaMode::Disabled,
        )
    };

    let auth_manager = Arc::new(AuthManager::new());
    setup_pg_catalog(&session_context, "datafusion", auth_manager.clone())
        .context("setup pg_catalog")?;
    let dml_config = DmlRuntimeConfig {
        prewrite_delay: config.dml_prewrite_delay,
        statement_timeout: config.dml_statement_timeout,
        max_inflight_statements: config.dml_max_inflight_statements,
        max_inflight_reads: config.dml_max_inflight_reads,
        max_inflight_writes: config.dml_max_inflight_writes,
        max_inflight_txns: config.dml_max_inflight_txns,
        max_inflight_background: config.dml_max_inflight_background,
        admission_queue_limit: config.dml_admission_queue_limit,
        admission_wait_timeout: config.dml_admission_wait_timeout,
        max_scan_rows: config.dml_max_scan_rows,
        max_txn_staged_rows: config.dml_max_txn_staged_rows,
    };
    let dml_hook = Arc::new(DmlHook::new(
        dml_config,
        holostore_client.clone(),
        pushdown_metrics.clone(),
    ));
    register_pg_compat_udfs(
        &session_context,
        postmaster_start_time_ns,
        dml_hook.txn_introspection(),
    );
    health
        .set(
            HealthState::Bootstrapping,
            "running metadata migration/backfill",
        )
        .await;
    let metadata_migration = ensure_metadata_migration(&holostore_client)
        .await
        .context("run metadata migration/backfill")?;
    if metadata_migration.migrated_tables > 0
        || metadata_migration.from_schema_version != metadata_migration.to_schema_version
    {
        info!(
            from_schema_version = metadata_migration.from_schema_version,
            to_schema_version = metadata_migration.to_schema_version,
            resumed_from_checkpoint = metadata_migration
                .resumed_from_checkpoint
                .map(|id| id.to_string())
                .unwrap_or_else(|| "none".to_string()),
            scanned_tables = metadata_migration.scanned_tables,
            migrated_tables = metadata_migration.migrated_tables,
            "metadata migration completed"
        );
    }

    health
        .set(HealthState::Bootstrapping, "bootstrapping SQL catalog")
        .await;
    let bootstrap = bootstrap_catalog(
        session_context.as_ref(),
        holostore_client.clone(),
        pushdown_metrics.clone(),
    )
    .await
    .context("bootstrap holo_fusion catalog")?;
    let catalog_sync_task = tokio::spawn(run_catalog_sync_loop(
        session_context.clone(),
        holostore_client.clone(),
        pushdown_metrics.clone(),
        health_shutdown_tx.subscribe(),
    ));
    let server_opts = ServerOptions::new()
        .with_host(config.pg_host.clone())
        .with_port(config.pg_port);

    install_pg_locks_table(
        session_context.as_ref(),
        "datafusion",
        dml_hook.txn_introspection(),
    )
    .context("install pg_catalog.pg_locks compatibility table")?;
    let query_hooks: Vec<Arc<dyn QueryHook>> = vec![Arc::new(SetShowHook), dml_hook];
    let sql_task = tokio::spawn(async move {
        serve_with_hooks(session_context, &server_opts, auth_manager, query_hooks)
            .await
            .map_err(|e| anyhow!(e))
    });

    // Decision: publish readiness details that reflect Ballista availability so
    // operators can distinguish full and degraded modes.
    // Decision: evaluate `match ballista_mode {` to choose the correct SQL/storage control path.
    match ballista_mode {
        BallistaMode::Standalone => {
            health
                .set(
                    HealthState::Ready,
                    format!(
                        "ready (holostore+ballista+pg; tables={})",
                        bootstrap.registered_tables.join(",")
                    ),
                )
                .await;
        }
        BallistaMode::LocalFallback => {
            health
                .set(
                    HealthState::Degraded,
                    format!(
                        "degraded (holostore+pg; local datafusion only; tables={})",
                        bootstrap.registered_tables.join(",")
                    ),
                )
                .await;
        }
        BallistaMode::Disabled => {
            health
                .set(
                    HealthState::Ready,
                    format!(
                        "ready (holostore+pg; ballista sql disabled; tables={})",
                        bootstrap.registered_tables.join(",")
                    ),
                )
                .await;
        }
    }

    shutdown.await?;

    health
        .set(HealthState::NotReady, "shutdown requested")
        .await;
    let _ = health_shutdown_tx.send(true);

    sql_task.abort();
    let _ = sql_task.await;
    let _ = catalog_sync_task.await;

    let _ = holostore.shutdown().await;

    // Decision: treat health task errors as fatal because they indicate
    // incomplete shutdown or hidden runtime faults.
    // Decision: evaluate `match health_task.await {` to choose the correct SQL/storage control path.
    match health_task.await {
        Ok(res) => {
            // Decision: evaluate `if let Err(err) = res {` to choose the correct SQL/storage control path.
            if let Err(err) = res {
                return Err(err).context("health server task failed");
            }
        }
        Err(err) => {
            return Err(anyhow!("health server join failed: {err}"));
        }
    }

    Ok(())
}

/// Applies distributed SQL execution defaults for production query planning.
fn configured_session_config(
    config: SessionConfig,
    target_partitions: usize,
    sort_spill_reservation_bytes: usize,
    spill_compression: &str,
) -> SessionConfig {
    config
        .with_target_partitions(target_partitions.max(1))
        .with_repartition_aggregations(true)
        .with_repartition_joins(true)
        .with_repartition_sorts(true)
        .with_repartition_windows(true)
        .set_str(
            "datafusion.execution.sort_spill_reservation_bytes",
            sort_spill_reservation_bytes.to_string().as_str(),
        )
        .set_str("datafusion.execution.spill_compression", spill_compression)
}

/// Normalizes spill compression configuration to valid DataFusion values.
fn normalize_spill_compression(value: Option<String>) -> String {
    match value
        .as_deref()
        .map(|raw| raw.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("zstd") => "zstd".to_string(),
        Some("uncompressed") => "uncompressed".to_string(),
        Some("lz4_frame") | None => "lz4_frame".to_string(),
        Some(other) => {
            warn!(
                value = other,
                "invalid HOLO_FUSION_SQL_SPILL_COMPRESSION; using lz4_frame"
            );
            "lz4_frame".to_string()
        }
    }
}

/// Runtime mode selected for DataFusion/Ballista integration.
#[derive(Clone, Copy, Debug)]
enum BallistaMode {
    /// Ballista standalone session context started successfully.
    Standalone,
    /// Ballista failed and local DataFusion context is used instead.
    LocalFallback,
    /// Ballista path explicitly disabled in configuration.
    Disabled,
}

/// Runs a minimal HTTP health server and exits when shutdown is signaled.
async fn run_health_server(
    addr: SocketAddr,
    health: RuntimeHealth,
    pushdown_metrics: Arc<PushdownMetrics>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind health server {addr}"))?;

    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                // Decision: stop serving once sender dropped or shutdown flag is set.
                if changed.is_err() || *shutdown_rx.borrow() {
                    return Ok(());
                }
            }
            accept = listener.accept() => {
                let (stream, _) = accept.context("accept health connection")?;
                let health = health.clone();
                let pushdown_metrics = pushdown_metrics.clone();
                tokio::spawn(async move {
                    let _ = handle_health_connection(stream, health, pushdown_metrics).await;
                });
            }
        }
    }
}

/// Periodically syncs DataFusion catalog entries from persisted table metadata.
async fn run_catalog_sync_loop(
    session_context: Arc<SessionContext>,
    holostore_client: HoloStoreClient,
    pushdown_metrics: Arc<PushdownMetrics>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                // Decision: stop syncing once sender dropped or shutdown flag is set.
                if changed.is_err() || *shutdown_rx.borrow() {
                    return Ok(());
                }
            }
            _ = tokio::time::sleep(CATALOG_SYNC_INTERVAL) => {
                // Decision: evaluate `match sync_catalog_from_metadata(` to choose the correct SQL/storage control path.
                match sync_catalog_from_metadata(
                    session_context.as_ref(),
                    holostore_client.clone(),
                    pushdown_metrics.clone(),
                ).await {
                    Ok(new_tables) => {
                        // Decision: log only non-empty registration sets to
                        // keep background logs high-signal.
                        // Decision: evaluate `if !new_tables.is_empty() {` to choose the correct SQL/storage control path.
                        if !new_tables.is_empty() {
                            info!(
                                tables = %new_tables.join(","),
                                "catalog sync registered new tables"
                            );
                        }
                    }
                    Err(err) => {
                        warn!(error = %err, "catalog sync failed");
                    }
                }
            }
        }
    }
}

/// Handles one HTTP health connection and writes a text/plain response.
async fn handle_health_connection(
    mut stream: TcpStream,
    health: RuntimeHealth,
    pushdown_metrics: Arc<PushdownMetrics>,
) -> Result<()> {
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await.context("read health request")?;
    let req = String::from_utf8_lossy(&buf[..n]);
    let path = req
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");

    let state = health.state();
    let detail = health.detail().await;
    // Decision: evaluate `let (status, body) = match path {` to choose the correct SQL/storage control path.
    let (status, body) = match path {
        "/live" => (200, "live\n".to_string()),
        "/metrics" => (200, pushdown_metrics.render_text()),
        "/ready" => {
            // Decision: report readiness only when runtime is in `Ready` state;
            // all other states return 503 to gate traffic.
            // Decision: evaluate `if matches!(state, HealthState::Ready) {` to choose the correct SQL/storage control path.
            if matches!(state, HealthState::Ready) {
                (200, "ready\n".to_string())
            } else {
                (503, format!("not-ready ({state:?})\n"))
            }
        }
        "/state" | "/" => (200, format!("state={state:?}\ndetail={detail}\n")),
        _ => (404, "not-found\n".to_string()),
    };

    let response = format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        http_status_text(status),
        body.len(),
        body
    );
    stream
        .write_all(response.as_bytes())
        .await
        .context("write health response")?;
    stream.flush().await.context("flush health response")?;
    Ok(())
}

/// Maps HTTP status codes to reason phrases.
fn http_status_text(status: u16) -> &'static str {
    // Decision: evaluate `match status {` to choose the correct SQL/storage control path.
    match status {
        200 => "OK",
        404 => "Not Found",
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}

/// Parses an optional port override with fallback default.
fn parse_port(value: Option<String>, default_port: u16) -> Result<u16> {
    // Decision: evaluate `match value {` to choose the correct SQL/storage control path.
    match value {
        Some(raw) => raw
            .parse::<u16>()
            .with_context(|| format!("invalid port value: {raw}")),
        None => Ok(default_port),
    }
}

/// Parses an optional `usize` with fallback default.
fn parse_usize(value: Option<String>, default_value: usize) -> Result<usize> {
    // Decision: evaluate `match value {` to choose the correct SQL/storage control path.
    match value {
        Some(raw) => raw
            .parse::<usize>()
            .with_context(|| format!("invalid usize value: {raw}")),
        None => Ok(default_value),
    }
}

/// Parses an optional `u64` with fallback default.
fn parse_u64(value: Option<String>, default_value: u64) -> Result<u64> {
    // Decision: evaluate `match value {` to choose the correct SQL/storage control path.
    match value {
        Some(raw) => raw
            .parse::<u64>()
            .with_context(|| format!("invalid u64 value: {raw}")),
        None => Ok(default_value),
    }
}

/// Parses an optional socket address with fallback default.
fn parse_socket_addr(value: Option<String>, default_addr: &str) -> Result<SocketAddr> {
    let raw = value.unwrap_or_else(|| default_addr.to_string());
    raw.parse::<SocketAddr>()
        .with_context(|| format!("invalid socket address: {raw}"))
}
