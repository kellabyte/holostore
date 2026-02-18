use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use holo_fusion::metadata::find_table_metadata_by_name;
use holo_fusion::provider::{encode_table_primary_key, table_key_end};
use holo_fusion::{run_with_shutdown, HoloFusionConfig};
use holo_store::{EmbeddedNodeConfig, HoloStoreClient};
use serde::Deserialize;
use serial_test::serial;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[serial]
async fn phase3_distributed_insert_and_scan_across_nodes() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;

    let node1 = NodePorts::new()?;
    let node2 = NodePorts::new()?;

    let initial_members = format!("1@{},2@{}", node1.grpc_addr, node2.grpc_addr);
    let node1_cfg = HoloFusionConfig {
        pg_host: "127.0.0.1".to_string(),
        pg_port: node1.pg_port,
        health_addr: node1.health_addr,
        enable_ballista_sql: false,
        dml_prewrite_delay: Duration::from_millis(250),
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
        holostore: EmbeddedNodeConfig {
            node_id: 1,
            listen_redis: node1.redis_addr,
            listen_grpc: node1.grpc_addr,
            bootstrap: true,
            join: None,
            initial_members: initial_members.clone(),
            data_dir: temp_dir.path().join("node-1"),
            ready_timeout: Duration::from_secs(30),
            max_shards: 2,
            initial_ranges: 1,
            routing_mode: Some("range".to_string()),
        },
    };
    let node2_cfg = HoloFusionConfig {
        pg_host: "127.0.0.1".to_string(),
        pg_port: node2.pg_port,
        health_addr: node2.health_addr,
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
        holostore: EmbeddedNodeConfig {
            node_id: 2,
            listen_redis: node2.redis_addr,
            listen_grpc: node2.grpc_addr,
            bootstrap: false,
            join: Some(node1.grpc_addr),
            initial_members,
            data_dir: temp_dir.path().join("node-2"),
            ready_timeout: Duration::from_secs(30),
            max_shards: 2,
            initial_ranges: 1,
            routing_mode: Some("range".to_string()),
        },
    };

    let mut runtime1 = RunningNode::start(node1_cfg, node1).await?;
    let mut runtime2 = RunningNode::start(node2_cfg, node2).await?;

    let admin_client = HoloStoreClient::new(runtime1.grpc_addr);
    wait_for(
        Duration::from_secs(30),
        Duration::from_millis(200),
        || async {
            let state = cluster_state(&admin_client).await?;
            Ok(state.members.len() >= 2 && !state.shards.is_empty())
        },
    )
    .await
    .context("wait for 2-node cluster state")?;
    ensure_orders_table("127.0.0.1", runtime1.pg_port)
        .await
        .context("bootstrap orders table on node1")?;
    ensure_orders_table("127.0.0.1", runtime2.pg_port)
        .await
        .context("bootstrap orders table on node2")?;
    let orders_table = find_table_metadata_by_name(&admin_client, "orders")
        .await
        .context("read metadata for orders table")?
        .context("orders table metadata should exist")?;
    let orders_table_id = orders_table.table_id;

    let split_key = encode_table_primary_key(orders_table_id, 5_000);
    admin_client
        .range_split(&split_key)
        .await
        .context("split data range at orders(5000)")?;

    wait_for(
        Duration::from_secs(30),
        Duration::from_millis(200),
        || async {
            let state = cluster_state(&admin_client).await?;
            Ok(state.shards.iter().any(|s| s.start_key == split_key))
        },
    )
    .await
    .context("wait for split visibility")?;

    let state = cluster_state(&admin_client).await?;
    let right = state
        .shards
        .iter()
        .find(|s| s.start_key == split_key)
        .ok_or_else(|| anyhow::anyhow!("right split shard not found"))?;
    admin_client
        .range_rebalance(right.shard_id, &[1, 2], Some(2))
        .await
        .context("rebalance right shard leaseholder to node 2")?;

    wait_for(
        Duration::from_secs(30),
        Duration::from_millis(200),
        || async {
            let state = cluster_state(&admin_client).await?;
            Ok(state
                .shards
                .iter()
                .find(|s| s.shard_id == right.shard_id)
                .map(|s| s.leaseholder == 2)
                .unwrap_or(false))
        },
    )
    .await
    .context("wait for leaseholder move to node 2")?;

    let (pg_client, _pg_guard) = connect_pg("127.0.0.1", runtime1.pg_port).await?;
    pg_client
        .batch_execute(
            "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at) VALUES
             (1001, 10, 'left', 100, TIMESTAMP '2026-02-11 10:00:00'),
             (9001, 20, 'right', 200, TIMESTAMP '2026-02-11 10:01:00');",
        )
        .await
        .context("insert rows through node 1 pg endpoint")?;

    let queried = pg_client
        .query(
            "SELECT order_id, status FROM orders WHERE order_id IN (1001, 9001) ORDER BY order_id",
            &[],
        )
        .await
        .context("query distributed rows through node 1")?;
    assert_eq!(queried.len(), 2);
    assert_eq!(queried[0].try_get::<_, i64>(0)?, 1001);
    assert_eq!(queried[0].try_get::<_, String>(1)?, "left");
    assert_eq!(queried[1].try_get::<_, i64>(0)?, 9001);
    assert_eq!(queried[1].try_get::<_, String>(1)?, "right");

    let updated = pg_client
        .execute(
            "UPDATE orders SET status = 'bulk' WHERE order_id >= 1001 AND order_id <= 9001",
            &[],
        )
        .await
        .context("run distributed UPDATE across split shards")?;
    assert_eq!(updated, 2);

    let updated_retry = pg_client
        .execute(
            "UPDATE orders SET status = 'bulk' WHERE order_id >= 1001 AND order_id <= 9001",
            &[],
        )
        .await
        .context("retry distributed UPDATE across split shards")?;
    assert_eq!(updated_retry, 2);

    let after_update = pg_client
        .query(
            "SELECT order_id, status FROM orders WHERE order_id IN (1001, 9001) ORDER BY order_id",
            &[],
        )
        .await
        .context("query rows after distributed UPDATE")?;
    assert_eq!(after_update.len(), 2);
    assert_eq!(after_update[0].try_get::<_, String>(1)?, "bulk");
    assert_eq!(after_update[1].try_get::<_, String>(1)?, "bulk");

    let deleted = pg_client
        .execute(
            "DELETE FROM orders WHERE order_id >= 9001 AND order_id <= 9001",
            &[],
        )
        .await
        .context("run distributed DELETE on right shard")?;
    assert_eq!(deleted, 1);

    let deleted_retry = pg_client
        .execute(
            "DELETE FROM orders WHERE order_id >= 9001 AND order_id <= 9001",
            &[],
        )
        .await
        .context("retry distributed DELETE on right shard")?;
    assert_eq!(deleted_retry, 0);

    let after_delete = pg_client
        .query(
            "SELECT order_id FROM orders WHERE order_id >= 1001 ORDER BY order_id",
            &[],
        )
        .await
        .context("query rows after distributed DELETE")?;
    assert_eq!(after_delete.len(), 1);
    assert_eq!(after_delete[0].try_get::<_, i64>(0)?, 1001);

    pg_client
        .batch_execute(
            "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at) VALUES
             (1101, 30, 'left_base', 300, TIMESTAMP '2026-02-11 10:02:00'),
             (9101, 40, 'right_base', 400, TIMESTAMP '2026-02-11 10:03:00');",
        )
        .await
        .context("seed rows for distributed rollback conflict test")?;

    let (pg_client_node2, _pg_guard_node2) = connect_pg("127.0.0.1", runtime2.pg_port).await?;
    let (pg_client_main, _pg_guard_main) = connect_pg("127.0.0.1", runtime1.pg_port).await?;
    let update_task = tokio::spawn(async move {
        pg_client_main
            .execute(
                "UPDATE orders SET status = 'txn_bulk' WHERE order_id >= 1101 AND order_id <= 9101",
                &[],
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(60)).await;

    let right_conflict_rows = pg_client_node2
        .execute(
            "UPDATE orders SET status = 'right_won' WHERE order_id = 9101",
            &[],
        )
        .await
        .context("write conflicting right-shard row via node 2")?;
    assert_eq!(right_conflict_rows, 1);

    let main_update = update_task
        .await
        .context("join distributed rollback conflict task")?;
    let main_err = match main_update {
        Ok(rows) => anyhow::bail!("expected distributed update conflict, got rows={rows}"),
        Err(err) => err,
    };
    assert_sqlstate(&main_err, "40001");

    let rollback_check = pg_client
        .query(
            "SELECT order_id, status FROM orders WHERE order_id IN (1101, 9101) ORDER BY order_id",
            &[],
        )
        .await
        .context("verify rollback after distributed conflict")?;
    assert_eq!(rollback_check.len(), 2);
    assert_eq!(rollback_check[0].try_get::<_, i64>(0)?, 1101);
    assert_eq!(rollback_check[0].try_get::<_, String>(1)?, "left_base");
    assert_eq!(rollback_check[1].try_get::<_, i64>(0)?, 9101);
    assert_eq!(rollback_check[1].try_get::<_, String>(1)?, "right_won");

    pg_client
        .execute("BEGIN", &[])
        .await
        .context("start distributed explicit transaction commit test")?;
    let tx_left = pg_client
        .execute(
            "UPDATE orders SET status = 'phase6_txn_commit' WHERE order_id = 1101",
            &[],
        )
        .await
        .context("stage left-shard row in explicit transaction")?;
    assert_eq!(tx_left, 1);
    let tx_right = pg_client
        .execute(
            "UPDATE orders SET status = 'phase6_txn_commit' WHERE order_id = 9101",
            &[],
        )
        .await
        .context("stage right-shard row in explicit transaction")?;
    assert_eq!(tx_right, 1);
    pg_client
        .execute("COMMIT", &[])
        .await
        .context("commit distributed explicit transaction")?;

    let committed_rows = pg_client
        .query(
            "SELECT order_id, status FROM orders WHERE order_id IN (1101, 9101) ORDER BY order_id",
            &[],
        )
        .await
        .context("verify distributed explicit transaction commit")?;
    assert_eq!(committed_rows.len(), 2);
    assert_eq!(committed_rows[0].try_get::<_, i64>(0)?, 1101);
    assert_eq!(
        committed_rows[0].try_get::<_, String>(1)?,
        "phase6_txn_commit"
    );
    assert_eq!(committed_rows[1].try_get::<_, i64>(0)?, 9101);
    assert_eq!(
        committed_rows[1].try_get::<_, String>(1)?,
        "phase6_txn_commit"
    );

    pg_client
        .execute("BEGIN", &[])
        .await
        .context("start distributed explicit transaction conflict test")?;
    let tx_conflict_left = pg_client
        .execute(
            "UPDATE orders SET status = 'phase6_txn_loser' WHERE order_id = 1101",
            &[],
        )
        .await
        .context("stage left row before distributed commit conflict")?;
    assert_eq!(tx_conflict_left, 1);
    let tx_conflict_right = pg_client
        .execute(
            "UPDATE orders SET status = 'phase6_txn_loser' WHERE order_id = 9101",
            &[],
        )
        .await
        .context("stage right row before distributed commit conflict")?;
    assert_eq!(tx_conflict_right, 1);

    pg_client_node2
        .execute("BEGIN", &[])
        .await
        .context("start conflicting transaction on node 2")?;
    let external_right = pg_client_node2
        .execute(
            "UPDATE orders SET status = 'phase6_conflict_winner_txn2' WHERE order_id = 9101",
            &[],
        )
        .await
        .context("stage right-shard conflicting row via node 2 before COMMIT")?;
    assert_eq!(external_right, 1);
    pg_client_node2
        .execute("COMMIT", &[])
        .await
        .context("commit conflicting transaction on node 2")?;

    let commit_conflict = pg_client
        .execute("COMMIT", &[])
        .await
        .expect_err("distributed COMMIT should conflict when right shard version changed");
    assert_sqlstate(&commit_conflict, "40001");

    let aborted_err = pg_client
        .execute("SELECT 1", &[])
        .await
        .expect_err("session should be aborted after distributed commit conflict");
    assert_sqlstate(&aborted_err, "25P02");

    pg_client
        .execute("ROLLBACK", &[])
        .await
        .context("clear aborted transaction state after distributed conflict")?;

    let final_after_conflict = pg_client
        .query(
            "SELECT order_id, status FROM orders WHERE order_id IN (1101, 9101) ORDER BY order_id",
            &[],
        )
        .await
        .context("verify distributed commit conflict rollback semantics")?;
    assert_eq!(final_after_conflict.len(), 2);
    assert_eq!(final_after_conflict[0].try_get::<_, i64>(0)?, 1101);
    assert_eq!(
        final_after_conflict[0].try_get::<_, String>(1)?,
        "phase6_txn_commit"
    );
    assert_eq!(final_after_conflict[1].try_get::<_, i64>(0)?, 9101);
    assert_eq!(
        final_after_conflict[1].try_get::<_, String>(1)?,
        "phase6_conflict_winner_txn2"
    );

    let state = cluster_state(&admin_client).await?;
    let right = state
        .shards
        .iter()
        .find(|s| s.start_key == split_key)
        .ok_or_else(|| anyhow::anyhow!("right split shard not found after inserts"))?;
    let right_client = HoloStoreClient::new(runtime2.grpc_addr);
    let (right_rows, _, _) = right_client
        .range_snapshot_latest(
            right.shard_index,
            &split_key,
            &table_key_end(orders_table_id),
            &[],
            100,
            false,
        )
        .await
        .context("snapshot right shard rows from node 2")?;
    assert!(
        !right_rows.is_empty(),
        "expected right shard rows on node 2 after distributed insert"
    );

    runtime1.shutdown().await?;
    runtime2.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[serial]
async fn phase9_insert_select_survives_split_route_churn_without_partial_apply() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;

    let node1 = NodePorts::new()?;
    let node2 = NodePorts::new()?;

    let initial_members = format!("1@{},2@{}", node1.grpc_addr, node2.grpc_addr);
    let node1_cfg = HoloFusionConfig {
        pg_host: "127.0.0.1".to_string(),
        pg_port: node1.pg_port,
        health_addr: node1.health_addr,
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
        holostore: EmbeddedNodeConfig {
            node_id: 1,
            listen_redis: node1.redis_addr,
            listen_grpc: node1.grpc_addr,
            bootstrap: true,
            join: None,
            initial_members: initial_members.clone(),
            data_dir: temp_dir.path().join("phase9-split-churn-node-1"),
            ready_timeout: Duration::from_secs(30),
            max_shards: 2,
            initial_ranges: 1,
            routing_mode: Some("range".to_string()),
        },
    };
    let node2_cfg = HoloFusionConfig {
        pg_host: "127.0.0.1".to_string(),
        pg_port: node2.pg_port,
        health_addr: node2.health_addr,
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
        holostore: EmbeddedNodeConfig {
            node_id: 2,
            listen_redis: node2.redis_addr,
            listen_grpc: node2.grpc_addr,
            bootstrap: false,
            join: Some(node1.grpc_addr),
            initial_members,
            data_dir: temp_dir.path().join("phase9-split-churn-node-2"),
            ready_timeout: Duration::from_secs(30),
            max_shards: 2,
            initial_ranges: 1,
            routing_mode: Some("range".to_string()),
        },
    };

    let mut runtime1 = RunningNode::start(node1_cfg, node1).await?;
    let mut runtime2 = RunningNode::start(node2_cfg, node2).await?;

    let admin_client = HoloStoreClient::new(runtime1.grpc_addr);
    wait_for(
        Duration::from_secs(30),
        Duration::from_millis(200),
        || async {
            let state = cluster_state(&admin_client).await?;
            Ok(state.members.len() >= 2 && !state.shards.is_empty())
        },
    )
    .await
    .context("wait for 2-node cluster state")?;

    let (pg_client, _pg_guard) = connect_pg("127.0.0.1", runtime1.pg_port).await?;
    pg_client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS sales_facts (
                order_id BIGINT NOT NULL,
                customer_id BIGINT NOT NULL,
                merchant_id BIGINT NOT NULL,
                region_id BIGINT NOT NULL,
                event_day BIGINT NOT NULL,
                status VARCHAR NOT NULL,
                amount_cents BIGINT NOT NULL,
                PRIMARY KEY (order_id)
            );",
        )
        .await
        .context("create sales_facts")?;

    let insert_stmt = "INSERT INTO sales_facts (
        order_id, customer_id, merchant_id, region_id, event_day, status, amount_cents
    )
    WITH base AS (
      SELECT COALESCE(MAX(order_id), 0) AS max_id
      FROM sales_facts
    )
    SELECT
      base.max_id + i AS order_id,
      100000 + (i % 500000) AS customer_id,
      1 + (i % 20000) AS merchant_id,
      1 + (i % 20) AS region_id,
      1 + ((i - 1) / 86400) AS event_day,
      CASE
        WHEN i % 10 < 6 THEN 'paid'
        WHEN i % 10 < 8 THEN 'shipped'
        WHEN i % 10 = 8 THEN 'pending'
        ELSE 'cancelled'
      END AS status,
      100 + ((i * 37) % 20000) AS amount_cents
    FROM base
    CROSS JOIN generate_series(1, 5000) AS g(i);";

    let written = pg_client
        .execute(insert_stmt, &[])
        .await
        .context("insert batch 1")?;
    assert_eq!(written, 5000);
    let written = pg_client
        .execute(insert_stmt, &[])
        .await
        .context("insert batch 2")?;
    assert_eq!(written, 5000);

    let sales_facts_table = find_table_metadata_by_name(&admin_client, "sales_facts")
        .await
        .context("read metadata for sales_facts table")?
        .context("sales_facts table metadata should exist")?;
    let split_key = encode_table_primary_key(sales_facts_table.table_id, 12_500);
    admin_client
        .range_split(&split_key)
        .await
        .context("split sales_facts range during ingest")?;

    let written = pg_client
        .execute(insert_stmt, &[])
        .await
        .context("insert batch 3 after split request")?;
    assert_eq!(written, 5000);

    wait_for(
        Duration::from_secs(30),
        Duration::from_millis(200),
        || async {
            let state = cluster_state(&admin_client).await?;
            Ok(state.shards.iter().any(|s| s.start_key == split_key))
        },
    )
    .await
    .context("wait for split visibility after ingest")?;

    let written = pg_client
        .execute(insert_stmt, &[])
        .await
        .context("insert batch 4 after split propagation")?;
    assert_eq!(written, 5000);

    let row = pg_client
        .query_one("SELECT COUNT(order_id)::BIGINT FROM sales_facts", &[])
        .await
        .context("count sales_facts rows")?;
    let count: i64 = row.try_get(0)?;
    assert_eq!(count, 20_000);

    runtime1.shutdown().await?;
    runtime2.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[serial]
async fn phase7_replica_record_convergence_and_node_failure_read_continuity() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;

    let node1 = NodePorts::new()?;
    let node2 = NodePorts::new()?;
    let node3 = NodePorts::new()?;

    let initial_members = format!(
        "1@{},2@{},3@{}",
        node1.grpc_addr, node2.grpc_addr, node3.grpc_addr
    );
    let node1_cfg = HoloFusionConfig {
        pg_host: "127.0.0.1".to_string(),
        pg_port: node1.pg_port,
        health_addr: node1.health_addr,
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
        holostore: EmbeddedNodeConfig {
            node_id: 1,
            listen_redis: node1.redis_addr,
            listen_grpc: node1.grpc_addr,
            bootstrap: true,
            join: None,
            initial_members: initial_members.clone(),
            data_dir: temp_dir.path().join("phase7-node-1"),
            ready_timeout: Duration::from_secs(30),
            max_shards: 1,
            initial_ranges: 1,
            routing_mode: Some("range".to_string()),
        },
    };
    let node2_cfg = HoloFusionConfig {
        pg_host: "127.0.0.1".to_string(),
        pg_port: node2.pg_port,
        health_addr: node2.health_addr,
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
        holostore: EmbeddedNodeConfig {
            node_id: 2,
            listen_redis: node2.redis_addr,
            listen_grpc: node2.grpc_addr,
            bootstrap: false,
            join: Some(node1.grpc_addr),
            initial_members: initial_members.clone(),
            data_dir: temp_dir.path().join("phase7-node-2"),
            ready_timeout: Duration::from_secs(30),
            max_shards: 1,
            initial_ranges: 1,
            routing_mode: Some("range".to_string()),
        },
    };
    let node3_cfg = HoloFusionConfig {
        pg_host: "127.0.0.1".to_string(),
        pg_port: node3.pg_port,
        health_addr: node3.health_addr,
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
        holostore: EmbeddedNodeConfig {
            node_id: 3,
            listen_redis: node3.redis_addr,
            listen_grpc: node3.grpc_addr,
            bootstrap: false,
            join: Some(node1.grpc_addr),
            initial_members,
            data_dir: temp_dir.path().join("phase7-node-3"),
            ready_timeout: Duration::from_secs(30),
            max_shards: 1,
            initial_ranges: 1,
            routing_mode: Some("range".to_string()),
        },
    };

    let mut runtime1 = RunningNode::start(node1_cfg, node1).await?;
    let mut runtime2 = RunningNode::start(node2_cfg, node2).await?;
    let mut runtime3 = RunningNode::start(node3_cfg, node3).await?;

    let admin_client = HoloStoreClient::new(runtime1.grpc_addr);
    wait_for(
        Duration::from_secs(30),
        Duration::from_millis(200),
        || async {
            let state = cluster_state(&admin_client).await?;
            Ok(state.members.len() >= 3 && !state.shards.is_empty())
        },
    )
    .await
    .context("wait for 3-node cluster state")?;
    ensure_orders_table("127.0.0.1", runtime1.pg_port)
        .await
        .context("bootstrap orders table on node1 for phase7 replication test")?;
    ensure_orders_table("127.0.0.1", runtime2.pg_port)
        .await
        .context("bootstrap orders table on node2 for phase7 replication test")?;
    ensure_orders_table("127.0.0.1", runtime3.pg_port)
        .await
        .context("bootstrap orders table on node3 for phase7 replication test")?;

    let state = cluster_state(&admin_client).await?;
    let shard = state
        .shards
        .first()
        .ok_or_else(|| anyhow::anyhow!("missing shard in cluster state"))?
        .clone();
    admin_client
        .range_rebalance(shard.shard_id, &[1, 2, 3], Some(1))
        .await
        .context("rebalance shard to replicas [1,2,3] with leaseholder node 1")?;

    let (pg_client, _pg_guard) = connect_pg("127.0.0.1", runtime1.pg_port).await?;
    let mut insert_sql = String::from(
        "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at) VALUES ",
    );
    let mut expected_rows = 0usize;
    for order_id in 700001i64..701001i64 {
        if expected_rows > 0 {
            insert_sql.push_str(", ");
        }
        let customer_id = 9000 + (order_id % 10);
        let total_cents = 1000 + (order_id % 1000);
        insert_sql.push_str(
            format!(
            "({order_id}, {customer_id}, 'phase7', {total_cents}, TIMESTAMP '2026-02-11 16:00:00')"
        )
            .as_str(),
        );
        expected_rows += 1;
    }
    insert_sql.push(';');

    pg_client
        .batch_execute(insert_sql.as_str())
        .await
        .context("insert phase7 rows through node 1 pg endpoint")?;

    let node1_client = HoloStoreClient::new(runtime1.grpc_addr);
    let node2_client = HoloStoreClient::new(runtime2.grpc_addr);
    let node3_client = HoloStoreClient::new(runtime3.grpc_addr);
    wait_for(
        Duration::from_secs(60),
        Duration::from_millis(250),
        || async {
            let count_1 = shard_record_count(&node1_client, shard.shard_id).await?;
            let count_2 = shard_record_count(&node2_client, shard.shard_id).await?;
            let count_3 = shard_record_count(&node3_client, shard.shard_id).await?;
            let min_count = *[count_1, count_2, count_3].iter().min().unwrap_or(&0);
            let max_count = *[count_1, count_2, count_3].iter().max().unwrap_or(&0);
            Ok(min_count >= expected_rows as u64 && max_count.saturating_sub(min_count) <= 1)
        },
    )
    .await
    .context("wait for per-replica record counts to converge")?;

    admin_client
        .range_rebalance(shard.shard_id, &[1, 2, 3], Some(2))
        .await
        .context("move leaseholder to node 2 before node1 shutdown")?;
    wait_for(
        Duration::from_secs(30),
        Duration::from_millis(200),
        || async {
            let state = cluster_state(&node2_client).await?;
            Ok(state
                .shards
                .iter()
                .find(|s| s.shard_id == shard.shard_id)
                .map(|s| s.leaseholder == 2)
                .unwrap_or(false))
        },
    )
    .await
    .context("wait for leaseholder transfer to node 2")?;

    runtime1.shutdown().await?;

    let (pg_client_node2, _pg_guard_node2) = connect_pg("127.0.0.1", runtime2.pg_port).await?;
    wait_for(
        Duration::from_secs(20),
        Duration::from_millis(200),
        || async {
            let row = pg_client_node2
                .query_one(
                    "SELECT COUNT(*)::BIGINT FROM orders WHERE order_id >= 700001 AND order_id < 701001",
                    &[],
                )
                .await
                .context("count phase7 rows from node 2 after node1 shutdown")?;
            let count: i64 = row.try_get(0)?;
            Ok(count == expected_rows as i64)
        },
    )
    .await
    .context("verify read continuity on surviving node after node failure")?;

    runtime2.shutdown().await?;
    runtime3.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[serial]
async fn phase7_create_table_visibility_and_writes_across_nodes() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;

    let node1 = NodePorts::new()?;
    let node2 = NodePorts::new()?;

    let initial_members = format!("1@{},2@{}", node1.grpc_addr, node2.grpc_addr);
    let node1_cfg = HoloFusionConfig {
        pg_host: "127.0.0.1".to_string(),
        pg_port: node1.pg_port,
        health_addr: node1.health_addr,
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
        holostore: EmbeddedNodeConfig {
            node_id: 1,
            listen_redis: node1.redis_addr,
            listen_grpc: node1.grpc_addr,
            bootstrap: true,
            join: None,
            initial_members: initial_members.clone(),
            data_dir: temp_dir.path().join("phase7-ddl-node-1"),
            ready_timeout: Duration::from_secs(30),
            max_shards: 1,
            initial_ranges: 1,
            routing_mode: Some("range".to_string()),
        },
    };
    let node2_cfg = HoloFusionConfig {
        pg_host: "127.0.0.1".to_string(),
        pg_port: node2.pg_port,
        health_addr: node2.health_addr,
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
        holostore: EmbeddedNodeConfig {
            node_id: 2,
            listen_redis: node2.redis_addr,
            listen_grpc: node2.grpc_addr,
            bootstrap: false,
            join: Some(node1.grpc_addr),
            initial_members,
            data_dir: temp_dir.path().join("phase7-ddl-node-2"),
            ready_timeout: Duration::from_secs(30),
            max_shards: 1,
            initial_ranges: 1,
            routing_mode: Some("range".to_string()),
        },
    };

    let mut runtime1 = RunningNode::start(node1_cfg, node1).await?;
    let mut runtime2 = RunningNode::start(node2_cfg, node2).await?;

    let admin_client = HoloStoreClient::new(runtime1.grpc_addr);
    wait_for(
        Duration::from_secs(30),
        Duration::from_millis(200),
        || async {
            let state = cluster_state(&admin_client).await?;
            Ok(state.members.len() >= 2 && !state.shards.is_empty())
        },
    )
    .await
    .context("wait for 2-node cluster state for ddl test")?;

    let (pg_node1, _pg_guard_1) = connect_pg("127.0.0.1", runtime1.pg_port).await?;
    pg_node1
        .batch_execute(
            "CREATE TABLE orders_phase7_ddl (
                order_id BIGINT NOT NULL,
                customer_id BIGINT NOT NULL,
                status TEXT,
                total_cents BIGINT NOT NULL,
                created_at TIMESTAMP NOT NULL,
                PRIMARY KEY (order_id)
            );",
        )
        .await
        .context("create table on node1")?;

    let (pg_node2, _pg_guard_2) = connect_pg("127.0.0.1", runtime2.pg_port).await?;
    wait_for(
        Duration::from_secs(30),
        Duration::from_millis(250),
        || async {
            match pg_node2
                .query_one("SELECT COUNT(*)::BIGINT FROM orders_phase7_ddl", &[])
                .await
            {
                Ok(row) => {
                    let count: i64 = row.try_get(0)?;
                    Ok(count == 0)
                }
                Err(err) => {
                    if let Some(db) = err.as_db_error() {
                        let code = db.code().code();
                        let message = db.message().to_ascii_lowercase();
                        if code == "42P01"
                            || (code == "XX000"
                                && (message.contains("table") && message.contains("not found")))
                        {
                            return Ok(false);
                        }
                        return Err(anyhow::anyhow!(
                            "query on node2 failed code={} message={}",
                            code,
                            db.message()
                        ));
                    }
                    Err(anyhow::anyhow!("query on node2 failed: {err}"))
                }
            }
        },
    )
    .await
    .context("wait for CREATE TABLE metadata visibility on node2")?;

    pg_node2
        .batch_execute(
            "INSERT INTO orders_phase7_ddl (order_id, customer_id, status, total_cents, created_at) VALUES
             (91001, 10, 'paid', 111, TIMESTAMP '2026-02-11 20:00:00'),
             (91002, 11, 'pending', 222, TIMESTAMP '2026-02-11 20:00:01');",
        )
        .await
        .context("insert rows on node2 for created table")?;

    wait_for(
        Duration::from_secs(20),
        Duration::from_millis(200),
        || async {
            let row = pg_node1
                .query_one("SELECT COUNT(*)::BIGINT FROM orders_phase7_ddl", &[])
                .await
                .context("count created table rows on node1")?;
            let count: i64 = row.try_get(0)?;
            Ok(count == 2)
        },
    )
    .await
    .context("wait for cross-node visibility of inserted rows")?;

    runtime1.shutdown().await?;
    runtime2.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[serial]
async fn phase7_partial_shard_availability_and_retry_behavior() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;

    let node1 = NodePorts::new()?;
    let node2 = NodePorts::new()?;
    let node3 = NodePorts::new()?;

    let initial_members = format!(
        "1@{},2@{},3@{}",
        node1.grpc_addr, node2.grpc_addr, node3.grpc_addr
    );
    let node1_cfg = HoloFusionConfig {
        pg_host: "127.0.0.1".to_string(),
        pg_port: node1.pg_port,
        health_addr: node1.health_addr,
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
        holostore: EmbeddedNodeConfig {
            node_id: 1,
            listen_redis: node1.redis_addr,
            listen_grpc: node1.grpc_addr,
            bootstrap: true,
            join: None,
            initial_members: initial_members.clone(),
            data_dir: temp_dir.path().join("phase7-partial-node-1"),
            ready_timeout: Duration::from_secs(30),
            max_shards: 2,
            initial_ranges: 1,
            routing_mode: Some("range".to_string()),
        },
    };
    let node2_cfg = HoloFusionConfig {
        pg_host: "127.0.0.1".to_string(),
        pg_port: node2.pg_port,
        health_addr: node2.health_addr,
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
        holostore: EmbeddedNodeConfig {
            node_id: 2,
            listen_redis: node2.redis_addr,
            listen_grpc: node2.grpc_addr,
            bootstrap: false,
            join: Some(node1.grpc_addr),
            initial_members: initial_members.clone(),
            data_dir: temp_dir.path().join("phase7-partial-node-2"),
            ready_timeout: Duration::from_secs(30),
            max_shards: 2,
            initial_ranges: 1,
            routing_mode: Some("range".to_string()),
        },
    };
    let node3_cfg = HoloFusionConfig {
        pg_host: "127.0.0.1".to_string(),
        pg_port: node3.pg_port,
        health_addr: node3.health_addr,
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
        holostore: EmbeddedNodeConfig {
            node_id: 3,
            listen_redis: node3.redis_addr,
            listen_grpc: node3.grpc_addr,
            bootstrap: false,
            join: Some(node1.grpc_addr),
            initial_members,
            data_dir: temp_dir.path().join("phase7-partial-node-3"),
            ready_timeout: Duration::from_secs(30),
            max_shards: 2,
            initial_ranges: 1,
            routing_mode: Some("range".to_string()),
        },
    };

    let mut runtime1 = RunningNode::start(node1_cfg, node1).await?;
    let mut runtime2 = RunningNode::start(node2_cfg, node2).await?;
    let mut runtime3 = RunningNode::start(node3_cfg, node3).await?;

    let admin_client = HoloStoreClient::new(runtime1.grpc_addr);
    wait_for(
        Duration::from_secs(30),
        Duration::from_millis(200),
        || async {
            let state = cluster_state(&admin_client).await?;
            Ok(state.members.len() >= 3 && !state.shards.is_empty())
        },
    )
    .await
    .context("wait for 3-node cluster state")?;
    ensure_orders_table("127.0.0.1", runtime1.pg_port).await?;
    ensure_orders_table("127.0.0.1", runtime2.pg_port).await?;
    ensure_orders_table("127.0.0.1", runtime3.pg_port).await?;

    let orders_table = find_table_metadata_by_name(&admin_client, "orders")
        .await?
        .context("orders metadata missing")?;
    let split_key = encode_table_primary_key(orders_table.table_id, 5_000);
    admin_client.range_split(&split_key).await?;

    wait_for(
        Duration::from_secs(30),
        Duration::from_millis(200),
        || async {
            let state = cluster_state(&admin_client).await?;
            Ok(state.shards.iter().any(|s| s.start_key == split_key))
        },
    )
    .await
    .context("wait for split visibility")?;

    let state = cluster_state(&admin_client).await?;
    let right = state
        .shards
        .iter()
        .find(|s| s.start_key == split_key)
        .ok_or_else(|| anyhow::anyhow!("right split shard not found"))?;
    admin_client
        .range_rebalance(right.shard_id, &[1, 2, 3], Some(3))
        .await
        .context("move right shard leaseholder to node3")?;

    let (pg_client, _pg_guard) = connect_pg("127.0.0.1", runtime1.pg_port).await?;
    pg_client
        .batch_execute(
            "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at) VALUES
             (7701, 70, 'left', 100, TIMESTAMP '2026-02-12 01:00:00'),
             (9701, 71, 'right', 200, TIMESTAMP '2026-02-12 01:00:01');",
        )
        .await
        .context("seed rows across split shards")?;

    runtime3.shutdown().await?;

    let _ = wait_for(
        Duration::from_secs(30),
        Duration::from_millis(250),
        || async {
            let state = cluster_state(&admin_client).await?;
            Ok(state
                .shards
                .iter()
                .find(|s| s.shard_id == right.shard_id)
                .map(|s| s.leaseholder == 1 || s.leaseholder == 2)
                .unwrap_or(false))
        },
    )
    .await;

    let mut attempts = 0usize;
    loop {
        attempts = attempts.saturating_add(1);
        match pg_client
            .execute(
                "UPDATE orders SET status = 'recovered' WHERE order_id >= 7701 AND order_id <= 9701",
                &[],
            )
            .await
        {
            Ok(rows) => {
                assert_eq!(rows, 2);
                break;
            }
            Err(err) => {
                if attempts >= 120 {
                    return Err(err).context("distributed retry budget exhausted");
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }

    let rows = pg_client
        .query(
            "SELECT order_id, status FROM orders WHERE order_id IN (7701, 9701) ORDER BY order_id",
            &[],
        )
        .await
        .context("verify distributed retry update results")?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].try_get::<_, String>(1)?, "recovered");
    assert_eq!(rows[1].try_get::<_, String>(1)?, "recovered");

    runtime1.shutdown().await?;
    runtime2.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[serial]
async fn phase7_restart_rejoin_visibility_and_rollback_correctness() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;

    let node1 = NodePorts::new()?;
    let node2 = NodePorts::new()?;

    let initial_members = format!("1@{},2@{}", node1.grpc_addr, node2.grpc_addr);
    let node1_cfg = HoloFusionConfig {
        pg_host: "127.0.0.1".to_string(),
        pg_port: node1.pg_port,
        health_addr: node1.health_addr,
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
        holostore: EmbeddedNodeConfig {
            node_id: 1,
            listen_redis: node1.redis_addr,
            listen_grpc: node1.grpc_addr,
            bootstrap: true,
            join: None,
            initial_members: initial_members.clone(),
            data_dir: temp_dir.path().join("phase7-restart-node-1"),
            ready_timeout: Duration::from_secs(30),
            max_shards: 1,
            initial_ranges: 1,
            routing_mode: Some("range".to_string()),
        },
    };
    let node2_cfg = HoloFusionConfig {
        pg_host: "127.0.0.1".to_string(),
        pg_port: node2.pg_port,
        health_addr: node2.health_addr,
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
        holostore: EmbeddedNodeConfig {
            node_id: 2,
            listen_redis: node2.redis_addr,
            listen_grpc: node2.grpc_addr,
            bootstrap: false,
            join: Some(node1.grpc_addr),
            initial_members,
            data_dir: temp_dir.path().join("phase7-restart-node-2"),
            ready_timeout: Duration::from_secs(30),
            max_shards: 1,
            initial_ranges: 1,
            routing_mode: Some("range".to_string()),
        },
    };

    let mut runtime1 = RunningNode::start(node1_cfg.clone(), node1).await?;
    let mut runtime2 = RunningNode::start(node2_cfg.clone(), node2).await?;

    let admin_client = HoloStoreClient::new(runtime1.grpc_addr);
    wait_for(
        Duration::from_secs(30),
        Duration::from_millis(200),
        || async {
            let state = cluster_state(&admin_client).await?;
            Ok(state.members.len() >= 2 && !state.shards.is_empty())
        },
    )
    .await
    .context("wait for 2-node cluster state")?;
    ensure_orders_table("127.0.0.1", runtime1.pg_port).await?;
    ensure_orders_table("127.0.0.1", runtime2.pg_port).await?;

    admin_client
        .range_rebalance(
            cluster_state(&admin_client)
                .await?
                .shards
                .first()
                .ok_or_else(|| anyhow::anyhow!("missing shard"))?
                .shard_id,
            &[1, 2],
            Some(1),
        )
        .await
        .context("rebalance shard replicas before restart test")?;

    let (pg_node1, _guard_node1) = connect_pg("127.0.0.1", runtime1.pg_port).await?;
    pg_node1
        .execute(
            "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at)
             VALUES (7801, 1, 'stable', 100, TIMESTAMP '2026-02-12 02:00:00')",
            &[],
        )
        .await
        .context("seed row before restart")?;

    runtime2.shutdown().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    runtime2 = RunningNode::start(node2_cfg, node2).await?;
    ensure_orders_table("127.0.0.1", runtime2.pg_port).await?;

    let (pg_node2, _guard_node2) = connect_pg("127.0.0.1", runtime2.pg_port).await?;
    wait_for(
        Duration::from_secs(20),
        Duration::from_millis(200),
        || async {
            let row = pg_node2
                .query_one(
                    "SELECT COUNT(*)::BIGINT FROM orders WHERE order_id = 7801",
                    &[],
                )
                .await?;
            let count: i64 = row.try_get(0)?;
            Ok(count == 1)
        },
    )
    .await
    .context("wait for rejoined node visibility of existing data")?;

    pg_node2
        .execute("BEGIN", &[])
        .await
        .context("begin transaction on rejoined node")?;
    pg_node2
        .execute(
            "UPDATE orders SET status = 'tx_temp' WHERE order_id = 7801",
            &[],
        )
        .await
        .context("stage update on rejoined node")?;
    pg_node2
        .execute(
            "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at)
             VALUES (7802, 2, 'tx_temp', 200, TIMESTAMP '2026-02-12 02:00:01')",
            &[],
        )
        .await
        .context("stage insert on rejoined node")?;
    pg_node2
        .execute("ROLLBACK", &[])
        .await
        .context("rollback transaction on rejoined node")?;

    let status = pg_node1
        .query_one("SELECT status FROM orders WHERE order_id = 7801", &[])
        .await
        .context("verify base row after rollback on rejoined node")?
        .try_get::<_, String>(0)?;
    assert_eq!(status, "stable");

    let rolled_back_insert = pg_node1
        .query_one(
            "SELECT COUNT(*)::BIGINT FROM orders WHERE order_id = 7802",
            &[],
        )
        .await
        .context("verify inserted row absent after rollback on rejoined node")?
        .try_get::<_, i64>(0)?;
    assert_eq!(rolled_back_insert, 0);

    runtime1.shutdown().await?;
    runtime2.shutdown().await?;
    Ok(())
}

async fn connect_pg(
    host: &str,
    port: u16,
) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>)> {
    let conn = format!("host={host} port={port} user=postgres dbname=datafusion");
    let (client, connection) = tokio_postgres::connect(&conn, NoTls)
        .await
        .context("connect pg")?;
    let guard = tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok((client, guard))
}

async fn ensure_orders_table(host: &str, port: u16) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last_err = anyhow::anyhow!("orders table bootstrap failed");
    loop {
        match connect_pg(host, port).await {
            Ok((client, guard)) => match client.batch_execute(ORDERS_TABLE_DDL).await {
                Ok(_) => {
                    guard.abort();
                    return Ok(());
                }
                Err(err) => {
                    last_err = err.into();
                    guard.abort();
                }
            },
            Err(err) => {
                last_err = err;
            }
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(last_err).context("create orders table for distributed test")
}

fn assert_sqlstate(err: &tokio_postgres::Error, expected: &str) {
    let actual = err.code().map(|code| code.code());
    assert_eq!(
        actual,
        Some(expected),
        "unexpected SQLSTATE for error: {err}"
    );
}

fn free_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("bind ephemeral port")?;
    Ok(listener.local_addr()?.port())
}

async fn wait_for_ready_or_task_exit(
    task: &mut tokio::task::JoinHandle<anyhow::Result<()>>,
    addr: SocketAddr,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(status) = health_status(addr, "/ready") {
            if status == 200 {
                return Ok(());
            }
        }

        if task.is_finished() {
            let joined = task.await.context("join node task")?;
            joined.context("node task exited before readiness")?;
            anyhow::bail!("node task exited before readiness without error");
        }

        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for /ready on {addr}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn health_status(addr: SocketAddr, path: &str) -> Result<u16> {
    let mut stream = TcpStream::connect(addr).context("connect health server")?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .context("write health request")?;

    let mut resp = String::new();
    stream
        .read_to_string(&mut resp)
        .context("read health response")?;
    let status = resp
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| anyhow::anyhow!("invalid health response: {resp}"))?
        .parse::<u16>()
        .context("parse health status code")?;
    Ok(status)
}

async fn wait_for<F, Fut>(timeout: Duration, interval: Duration, mut check: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if check().await? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for condition");
        }
        tokio::time::sleep(interval).await;
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ClusterStateView {
    #[serde(default)]
    members: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    shards: Vec<ShardView>,
}

#[derive(Debug, Clone, Deserialize)]
struct ShardView {
    shard_id: u64,
    shard_index: usize,
    leaseholder: u64,
    #[serde(default)]
    start_key: Vec<u8>,
}

async fn cluster_state(client: &HoloStoreClient) -> Result<ClusterStateView> {
    let raw = client.cluster_state_json().await?;
    serde_json::from_str(&raw).context("parse cluster state json")
}

async fn shard_record_count(client: &HoloStoreClient, shard_id: u64) -> Result<u64> {
    let stats = client.range_stats().await?;
    Ok(stats
        .iter()
        .find(|stat| stat.shard_id == shard_id)
        .map(|stat| stat.record_count)
        .unwrap_or(0))
}

#[derive(Clone, Copy)]
struct NodePorts {
    pg_port: u16,
    health_addr: SocketAddr,
    redis_addr: SocketAddr,
    grpc_addr: SocketAddr,
}

impl NodePorts {
    fn new() -> Result<Self> {
        let pg_port = free_port()?;
        let health_addr: SocketAddr = format!("127.0.0.1:{}", free_port()?).parse()?;
        let redis_addr: SocketAddr = format!("127.0.0.1:{}", free_port()?).parse()?;
        let grpc_addr: SocketAddr = format!("127.0.0.1:{}", free_port()?).parse()?;
        Ok(Self {
            pg_port,
            health_addr,
            redis_addr,
            grpc_addr,
        })
    }
}

struct RunningNode {
    pg_port: u16,
    grpc_addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
}

impl RunningNode {
    async fn start(config: HoloFusionConfig, ports: NodePorts) -> Result<Self> {
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let mut task = tokio::spawn(async move {
            run_with_shutdown(config, async move {
                let _ = shutdown_rx.await;
                Ok::<(), std::io::Error>(())
            })
            .await
        });

        wait_for_ready_or_task_exit(&mut task, ports.health_addr, Duration::from_secs(30))
            .await
            .context("wait for node readiness")?;

        Ok(Self {
            pg_port: ports.pg_port,
            grpc_addr: ports.grpc_addr,
            shutdown_tx: Some(shutdown_tx),
            task: Some(task),
        })
    }

    async fn shutdown(&mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        Ok(())
    }
}

impl Drop for RunningNode {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
