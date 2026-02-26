use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use holo_fusion::{run_with_shutdown, HoloFusionConfig};
use holo_store::EmbeddedNodeConfig;
use tempfile::TempDir;
use tokio::sync::oneshot;
use tokio_postgres::NoTls;

const ORDERS_TABLE_DDL: &str = "CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    status TEXT,
    total_cents BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (order_id)
);";

#[derive(Clone, Copy)]
struct BenchSloTargets {
    read_p95_ms: u64,
    mixed_p95_ms: u64,
    write_p95_ms: u64,
}

impl BenchSloTargets {
    fn from_env() -> Self {
        Self {
            read_p95_ms: std::env::var("HOLO_FUSION_SLO_READ_P95_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(50),
            mixed_p95_ms: std::env::var("HOLO_FUSION_SLO_MIXED_P95_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(80),
            write_p95_ms: std::env::var("HOLO_FUSION_SLO_WRITE_P95_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(100),
        }
    }
}

#[derive(Clone, Copy)]
struct LatencySummary {
    p50: Duration,
    p95: Duration,
    p99: Duration,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "phase7 benchmark harness; run explicitly in perf environments"]
async fn phase7_benchmark_and_slo_package() -> Result<()> {
    let harness = BenchHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;
    seed_orders(&client, 10_000).await?;

    let read = run_read_heavy(&client, 1_000).await?;
    let mixed = run_mixed(&client, 1_000).await?;
    let write = run_write_heavy(&client, 1_000).await?;

    println!(
        "read_heavy: p50={}ms p95={}ms p99={}ms",
        read.p50.as_millis(),
        read.p95.as_millis(),
        read.p99.as_millis()
    );
    println!(
        "mixed: p50={}ms p95={}ms p99={}ms",
        mixed.p50.as_millis(),
        mixed.p95.as_millis(),
        mixed.p99.as_millis()
    );
    println!(
        "write_heavy: p50={}ms p95={}ms p99={}ms",
        write.p50.as_millis(),
        write.p95.as_millis(),
        write.p99.as_millis()
    );

    let enforce = std::env::var("HOLO_FUSION_ENFORCE_SLO")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if enforce {
        let targets = BenchSloTargets::from_env();
        assert!(
            read.p95.as_millis() <= targets.read_p95_ms as u128,
            "read-heavy p95={}ms exceeds target={}ms",
            read.p95.as_millis(),
            targets.read_p95_ms
        );
        assert!(
            mixed.p95.as_millis() <= targets.mixed_p95_ms as u128,
            "mixed p95={}ms exceeds target={}ms",
            mixed.p95.as_millis(),
            targets.mixed_p95_ms
        );
        assert!(
            write.p95.as_millis() <= targets.write_p95_ms as u128,
            "write-heavy p95={}ms exceeds target={}ms",
            write.p95.as_millis(),
            targets.write_p95_ms
        );
    }

    Ok(())
}

async fn seed_orders(client: &tokio_postgres::Client, rows: i64) -> Result<()> {
    let inserted = client
        .execute(
            "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at)
             SELECT
               900000 + i,
               5000 + (i % 1000),
               'seed',
               100 + (i % 10000),
               TIMESTAMP '2026-02-12 00:00:00' + (i || ' seconds')::interval
             FROM generate_series(1, $1::BIGINT) AS g(i)",
            &[&rows],
        )
        .await
        .context("seed benchmark rows")?;
    if inserted != rows as u64 {
        anyhow::bail!("expected to seed {rows} rows, inserted {inserted}");
    }
    Ok(())
}

async fn run_read_heavy(client: &tokio_postgres::Client, ops: usize) -> Result<LatencySummary> {
    let mut latencies = Vec::with_capacity(ops);
    for idx in 0..ops {
        let order_id = 900001i64 + (idx as i64 % 10_000);
        let started = Instant::now();
        let _row = client
            .query_one(
                "SELECT total_cents FROM orders WHERE order_id = $1::BIGINT",
                &[&order_id],
            )
            .await
            .context("read-heavy query")?;
        latencies.push(started.elapsed());
    }
    Ok(summarize(latencies))
}

async fn run_write_heavy(client: &tokio_postgres::Client, ops: usize) -> Result<LatencySummary> {
    let mut latencies = Vec::with_capacity(ops);
    for idx in 0..ops {
        let order_id = 900001i64 + (idx as i64 % 10_000);
        let total_cents = 200 + idx as i64;
        let started = Instant::now();
        let updated = client
            .execute(
                "UPDATE orders SET total_cents = $1::BIGINT WHERE order_id = $2::BIGINT",
                &[&total_cents, &order_id],
            )
            .await
            .context("write-heavy update")?;
        if updated != 1 {
            anyhow::bail!("write-heavy update expected 1 row, got {updated}");
        }
        latencies.push(started.elapsed());
    }
    Ok(summarize(latencies))
}

async fn run_mixed(client: &tokio_postgres::Client, ops: usize) -> Result<LatencySummary> {
    let mut latencies = Vec::with_capacity(ops);
    for idx in 0..ops {
        let started = Instant::now();
        if idx % 3 == 0 {
            let order_id = 900001i64 + (idx as i64 % 10_000);
            let _ = client
                .query_one(
                    "SELECT status FROM orders WHERE order_id = $1::BIGINT",
                    &[&order_id],
                )
                .await
                .context("mixed read op")?;
        } else if idx % 3 == 1 {
            let order_id = 900001i64 + (idx as i64 % 10_000);
            let updated = client
                .execute(
                    "UPDATE orders SET status = 'mixed' WHERE order_id = $1::BIGINT",
                    &[&order_id],
                )
                .await
                .context("mixed update op")?;
            if updated != 1 {
                anyhow::bail!("mixed update expected 1 row, got {updated}");
            }
        } else {
            let count = client
                .query_one(
                    "SELECT COUNT(*)::BIGINT FROM orders WHERE order_id >= 900001 AND order_id < 901001",
                    &[],
                )
                .await
                .context("mixed aggregate op")?
                .try_get::<_, i64>(0)?;
            if count <= 0 {
                anyhow::bail!("mixed aggregate returned non-positive count");
            }
        }
        latencies.push(started.elapsed());
    }
    Ok(summarize(latencies))
}

fn summarize(mut latencies: Vec<Duration>) -> LatencySummary {
    latencies.sort_unstable();
    LatencySummary {
        p50: percentile(&latencies, 0.50),
        p95: percentile(&latencies, 0.95),
        p99: percentile(&latencies, 0.99),
    }
}

fn percentile(values: &[Duration], ratio: f64) -> Duration {
    if values.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((values.len() - 1) as f64 * ratio).round() as usize;
    values[idx]
}

struct BenchHarness {
    pg_host: String,
    pg_port: u16,
    shutdown_tx: Option<oneshot::Sender<()>>,
    runtime_task: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
    _temp_dir: TempDir,
}

impl BenchHarness {
    async fn start() -> Result<Self> {
        let temp_dir = TempDir::new().context("create temp dir")?;
        let pg_port = free_port()?;
        let health_port = free_port()?;
        let redis_port = free_port()?;
        let grpc_port = free_port()?;

        let pg_host = "127.0.0.1".to_string();
        let health_addr = format!("127.0.0.1:{health_port}").parse()?;
        let redis_addr = format!("127.0.0.1:{redis_port}").parse()?;
        let grpc_addr = format!("127.0.0.1:{grpc_port}").parse()?;

        let config = HoloFusionConfig {
            pg_host: pg_host.clone(),
            pg_port,
            health_addr,
            enable_ballista_sql: false,
            dml_prewrite_delay: Duration::ZERO,
            dml_statement_timeout: Duration::ZERO,
            dml_max_inflight_statements: 1024,
            dml_max_inflight_reads: 1024,
            dml_max_inflight_writes: 1024,
            dml_max_inflight_txns: ((1024) / 2).max(1),
            dml_max_inflight_background: ((1024) / 4).max(1),
            dml_admission_queue_limit: 4096,
            dml_admission_wait_timeout: Duration::ZERO,
            dml_max_scan_rows: 100_000,
            dml_max_txn_staged_rows: 100_000,
            catalog_sync_interval: Duration::from_millis(1_000),
            catalog_sync_max_interval: Duration::from_millis(15_000),
            catalog_sync_busy_apply_ops: 8,
            catalog_sync_busy_apply_rows: 8_192,
            holostore: EmbeddedNodeConfig {
                node_id: 1,
                listen_redis: redis_addr,
                listen_grpc: grpc_addr,
                bootstrap: true,
                join: None,
                initial_members: format!("1@{grpc_addr}"),
                data_dir: temp_dir.path().join("bench-node-1"),
                ready_timeout: Duration::from_secs(30),
                max_shards: 1,
                initial_ranges: 1,
                routing_mode: None,
            },
        };

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let runtime_task = tokio::spawn(async move {
            run_with_shutdown(config, async move {
                let _ = shutdown_rx.await;
                Ok::<(), std::io::Error>(())
            })
            .await
        });

        wait_for_ready(health_addr, Duration::from_secs(30))
            .context("wait for benchmark harness readiness")?;
        ensure_orders_table(pg_host.as_str(), pg_port)
            .await
            .context("bootstrap orders table for benchmark harness")?;

        Ok(Self {
            pg_host,
            pg_port,
            shutdown_tx: Some(shutdown_tx),
            runtime_task: Some(runtime_task),
            _temp_dir: temp_dir,
        })
    }

    async fn connect_pg(&self) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>)> {
        let conn = format!(
            "host={} port={} user=postgres dbname=datafusion",
            self.pg_host, self.pg_port
        );
        let (client, connection) = tokio_postgres::connect(&conn, NoTls).await?;
        let guard = tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok((client, guard))
    }
}

impl Drop for BenchHarness {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.runtime_task.take() {
            task.abort();
        }
    }
}

fn free_port() -> Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").context("bind ephemeral port")?;
    Ok(listener.local_addr()?.port())
}

fn wait_for_ready(addr: std::net::SocketAddr, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(status) = health_status(addr, "/ready") {
            if status == 200 {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for /ready on {addr}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn health_status(addr: std::net::SocketAddr, path: &str) -> Result<u16> {
    use std::io::{Read, Write};

    let mut stream = std::net::TcpStream::connect(addr).context("connect health server")?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes())?;
    let mut resp = String::new();
    stream.read_to_string(&mut resp)?;
    let status = resp
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| anyhow::anyhow!("invalid health response: {resp}"))?
        .parse::<u16>()?;
    Ok(status)
}

async fn ensure_orders_table(host: &str, port: u16) -> Result<()> {
    let conn = format!("host={host} port={port} user=postgres dbname=datafusion");
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last_err = anyhow::anyhow!("orders table bootstrap failed");
    loop {
        match tokio_postgres::connect(&conn, NoTls).await {
            Ok((client, connection)) => {
                let guard = tokio::spawn(async move {
                    let _ = connection.await;
                });
                match client.batch_execute(ORDERS_TABLE_DDL).await {
                    Ok(_) => {
                        guard.abort();
                        return Ok(());
                    }
                    Err(err) => {
                        last_err = err.into();
                        guard.abort();
                    }
                }
            }
            Err(err) => {
                last_err = err.into();
            }
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(last_err).context("bootstrap orders table for benchmark harness")
}
