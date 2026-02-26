use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use holo_fusion::metadata::{
    find_table_metadata_by_name, update_table_primary_key_distribution, PrimaryKeyDistribution,
};
use holo_fusion::metrics::PushdownMetrics;
use holo_fusion::provider::{HoloStoreTableProvider, OrdersSeedRow};
use holo_fusion::{run_with_shutdown, HoloFusionConfig};
use holo_store::{EmbeddedNodeConfig, HoloStoreClient};
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase1_smoke_select_1() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    let row = client
        .query_one("SELECT 1", &[])
        .await
        .context("run SELECT 1")?;
    let value: i64 = row.try_get(0).context("decode SELECT 1 result as i64")?;
    assert_eq!(value, 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase1_pg_postmaster_start_time_available() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    let row = client
        .query_one(
            "SELECT pg_catalog.pg_postmaster_start_time() IS NOT NULL",
            &[],
        )
        .await
        .context("query pg_postmaster_start_time compatibility function")?;
    let is_present: bool = row.try_get(0)?;
    assert!(is_present, "pg_postmaster_start_time should return a value");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase1_pg_is_in_recovery_available_and_false() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    let schema_qualified = client
        .query_one("SELECT pg_catalog.pg_is_in_recovery()", &[])
        .await
        .context("query pg_catalog.pg_is_in_recovery compatibility function")?;
    let is_in_recovery_schema_qualified: bool = schema_qualified.try_get(0)?;
    assert!(
        !is_in_recovery_schema_qualified,
        "pg_catalog.pg_is_in_recovery should be false"
    );

    let unqualified = client
        .query_one("SELECT pg_is_in_recovery()", &[])
        .await
        .context("query pg_is_in_recovery compatibility function")?;
    let is_in_recovery_unqualified: bool = unqualified.try_get(0)?;
    assert!(
        !is_in_recovery_unqualified,
        "pg_is_in_recovery should be false"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase1_txid_current_available_and_nonzero() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    let schema_qualified = client
        .query_one("SELECT pg_catalog.txid_current()", &[])
        .await
        .context("query pg_catalog.txid_current compatibility function")?;
    let txid_schema_qualified: i64 = schema_qualified.try_get(0)?;
    assert!(
        txid_schema_qualified > 0,
        "pg_catalog.txid_current should return a positive xid"
    );

    let unqualified = client
        .query_one("SELECT txid_current()", &[])
        .await
        .context("query txid_current compatibility function")?;
    let txid_unqualified: i64 = unqualified.try_get(0)?;
    assert!(
        txid_unqualified > 0,
        "txid_current should return a positive xid"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase1_pg_age_available_for_temporal_and_xid_forms() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    let temporal = client
        .query_one(
            "SELECT pg_catalog.age(
                TIMESTAMP '2026-02-12 00:00:00',
                TIMESTAMP '2026-01-12 00:00:00'
            ) IS NOT NULL",
            &[],
        )
        .await
        .context("query temporal pg_catalog.age overload")?;
    assert!(temporal.try_get::<_, bool>(0)?);

    let xid_binary = client
        .query_one("SELECT pg_catalog.age(10::BIGINT, 3::BIGINT)", &[])
        .await
        .context("query xid binary pg_catalog.age overload")?;
    assert_eq!(xid_binary.try_get::<_, i64>(0)?, 7);

    let xid_unary = client
        .query_one("SELECT pg_catalog.age(10::BIGINT) IS NOT NULL", &[])
        .await
        .context("query xid unary pg_catalog.age overload")?;
    assert!(xid_unary.try_get::<_, bool>(0)?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase1_pg_locks_available_and_tracks_explicit_transactions() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client_a, _guard_a) = harness.connect_pg().await?;
    let (client_b, _guard_b) = harness.connect_pg().await?;

    let before = client_b
        .query_one("SELECT COUNT(*)::BIGINT FROM pg_catalog.pg_locks", &[])
        .await
        .context("query pg_locks before BEGIN")?;
    let before_count: i64 = before.try_get(0)?;
    assert_eq!(before_count, 0);

    client_a
        .execute("BEGIN", &[])
        .await
        .context("start explicit transaction for pg_locks test")?;

    let during = client_b
        .query(
            "SELECT locktype, mode, granted
             FROM pg_catalog.pg_locks
             ORDER BY locktype",
            &[],
        )
        .await
        .context("query pg_locks during active transaction")?;
    assert_eq!(during.len(), 2);
    let locktypes = during
        .iter()
        .map(|row| row.try_get::<_, String>(0))
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("decode locktype values")?;
    assert!(locktypes.contains(&"transactionid".to_string()));
    assert!(locktypes.contains(&"virtualxid".to_string()));
    for row in during {
        assert_eq!(row.try_get::<_, String>(1)?, "ExclusiveLock");
        assert!(row.try_get::<_, bool>(2)?);
    }

    client_a
        .execute("ROLLBACK", &[])
        .await
        .context("rollback explicit transaction for pg_locks test")?;

    let after = client_b
        .query_one("SELECT COUNT(*)::BIGINT FROM pg_catalog.pg_locks", &[])
        .await
        .context("query pg_locks after ROLLBACK")?;
    let after_count: i64 = after.try_get(0)?;
    assert_eq!(after_count, 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase1_datagrip_create_table_probe_path_is_supported() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS datagrip_probe_orders (
                order_id BIGINT NOT NULL,
                customer_id BIGINT NOT NULL,
                status TEXT,
                total_cents BIGINT NOT NULL,
                created_at TIMESTAMP NOT NULL,
                PRIMARY KEY (order_id)
            );",
        )
        .await
        .context("create table through postgres wire for datagrip probe path")?;

    let postmaster = client
        .query_one(
            "SELECT pg_catalog.pg_postmaster_start_time() IS NOT NULL",
            &[],
        )
        .await
        .context("run datagrip probe: pg_postmaster_start_time")?;
    assert!(postmaster.try_get::<_, bool>(0)?);

    let age_probe = client
        .query_one("SELECT pg_catalog.age(1::INT4) IS NOT NULL", &[])
        .await
        .context("run datagrip probe: age(int4)")?;
    assert!(age_probe.try_get::<_, bool>(0)?);

    let recovery_probe = client
        .query_one("SELECT pg_is_in_recovery() = false", &[])
        .await
        .context("run datagrip probe: pg_is_in_recovery")?;
    assert!(recovery_probe.try_get::<_, bool>(0)?);

    let txid_probe = client
        .query_one("SELECT txid_current() > 0", &[])
        .await
        .context("run datagrip probe: txid_current")?;
    assert!(txid_probe.try_get::<_, bool>(0)?);

    let pg_locks_probe = client
        .query_one("SELECT COUNT(*)::BIGINT FROM pg_catalog.pg_locks", &[])
        .await
        .context("run datagrip probe: pg_locks")?;
    let _locks_count: i64 = pg_locks_probe.try_get(0)?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase2_seeded_orders_scan_and_filter() -> Result<()> {
    let harness = TestHarness::start().await?;
    let holostore_client = harness.holostore_client();

    let rows = vec![
        OrdersSeedRow::new(1001, 42, Some("paid"), 1299, 1_707_654_000_000_000_000),
        OrdersSeedRow::new(1002, 42, Some("shipped"), 2599, 1_707_654_100_000_000_000),
        OrdersSeedRow::new(1003, 99, Some("pending"), 399, 1_707_654_200_000_000_000),
    ];

    let table = find_table_metadata_by_name(&holostore_client, "orders")
        .await
        .context("read metadata for orders table")?
        .context("orders table metadata should exist")?;
    let provider = HoloStoreTableProvider::from_table_metadata(
        &table,
        holostore_client.clone(),
        Arc::new(PushdownMetrics::default()),
    )
    .context("build provider for seeded orders test")?;
    let applied = provider
        .upsert_orders_rows(&rows)
        .await
        .context("seed orders through provider upsert path")?;
    assert_eq!(applied, rows.len() as u64);

    let (client, _guard) = harness.connect_pg().await?;
    let result = client
        .query(
            "SELECT order_id, customer_id, status, total_cents FROM orders ORDER BY order_id",
            &[],
        )
        .await
        .context("query seeded orders")?;
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].try_get::<_, i64>(0)?, 1001);
    assert_eq!(result[0].try_get::<_, i64>(1)?, 42);
    assert_eq!(result[0].try_get::<_, String>(2)?, "paid");
    assert_eq!(result[0].try_get::<_, i64>(3)?, 1299);

    let filtered = client
        .query(
            "SELECT order_id FROM orders WHERE order_id >= 1002 AND order_id < 1003 ORDER BY order_id",
            &[],
        )
        .await
        .context("query filtered orders by pushed PK bounds")?;
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].try_get::<_, i64>(0)?, 1002);

    let metrics = harness.metrics_body().context("fetch pushdown metrics")?;
    assert!(
        metrics.contains("pushdown_support_exact="),
        "missing exact pushdown metric in body: {metrics}"
    );
    assert!(
        metrics.contains("scan_requests="),
        "missing scan request metric in body: {metrics}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase4_insert_values_round_trip() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    client
        .batch_execute(
            "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at) VALUES
             (2001, 77, 'paid', 1999, TIMESTAMP '2026-02-11 10:00:00'),
             (2002, 88, 'pending', 499, TIMESTAMP '2026-02-11 10:01:00');",
        )
        .await
        .context("insert rows through postgres wire")?;

    let rows = client
        .query(
            "SELECT order_id, customer_id, status, total_cents FROM orders WHERE order_id >= 2001 ORDER BY order_id",
            &[],
        )
        .await
        .context("select inserted rows")?;

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].try_get::<_, i64>(0)?, 2001);
    assert_eq!(rows[0].try_get::<_, i64>(1)?, 77);
    assert_eq!(rows[0].try_get::<_, String>(2)?, "paid");
    assert_eq!(rows[0].try_get::<_, i64>(3)?, 1999);
    assert_eq!(rows[1].try_get::<_, i64>(0)?, 2002);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase4_insert_select_generate_series_round_trip() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    let before = client
        .query_one("SELECT COUNT(order_id)::BIGINT FROM orders", &[])
        .await
        .context("count rows before insert-select")?
        .try_get::<_, i64>(0)?;
    assert_eq!(before, 0);

    let insert_stmt = "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at)
         SELECT
           (SELECT COALESCE(MAX(order_id), 0) FROM orders) + i,
           2000 + (i % 500),
           CASE
             WHEN i % 4 = 0 THEN 'paid'
             WHEN i % 4 = 1 THEN 'pending'
             WHEN i % 4 = 2 THEN 'shipped'
             ELSE 'cancelled'
           END,
           500 + ((i * 37) % 50000),
           TIMESTAMP '2026-02-11 00:00:00' + (i || ' seconds')::interval
         FROM generate_series(1, 10000) AS g(i)";

    for run in 1..=3 {
        let inserted = client
            .execute(insert_stmt, &[])
            .await
            .with_context(|| format!("run insert-select generate_series iteration {run}"))?;
        assert_eq!(inserted, 10_000);

        let after = client
            .query_one("SELECT COUNT(order_id)::BIGINT FROM orders", &[])
            .await
            .with_context(|| format!("count rows after insert-select iteration {run}"))?
            .try_get::<_, i64>(0)?;
        assert_eq!(after, (run as i64) * 10_000);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase10_insert_select_max_pk_fast_path_scales_past_scan_guardrail() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    let insert_stmt = "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at)
         WITH base AS (
           SELECT COALESCE(MAX(order_id), 0) AS max_id
           FROM orders
         )
         SELECT
           base.max_id + i,
           2000 + (i % 500),
           CASE
             WHEN i % 4 = 0 THEN 'paid'
             WHEN i % 4 = 1 THEN 'pending'
             WHEN i % 4 = 2 THEN 'shipped'
             ELSE 'cancelled'
           END,
           500 + ((i * 37) % 50000),
           TIMESTAMP '2026-02-11 00:00:00' + (i || ' seconds')::interval
         FROM base
         CROSS JOIN generate_series(1, 20000) AS g(i)";

    for run in 1..=7 {
        let inserted = client
            .execute(insert_stmt, &[])
            .await
            .with_context(|| format!("run insert-select max-pk batch iteration {run}"))?;
        assert_eq!(inserted, 20_000);
    }

    let max_order_id = client
        .query_one("SELECT COALESCE(MAX(order_id), 0) FROM orders", &[])
        .await
        .context("read max order_id after repeated insert-select batches")?
        .try_get::<_, i64>(0)?;
    assert_eq!(max_order_id, 140_000);

    let terminal_row_count = client
        .query_one(
            "SELECT COUNT(*)::BIGINT FROM orders WHERE order_id = 140000",
            &[],
        )
        .await
        .context("verify terminal inserted row exists")?
        .try_get::<_, i64>(0)?;
    assert_eq!(terminal_row_count, 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase4_insert_values_duplicate_primary_key_returns_23505() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    client
        .execute(
            "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at)
             VALUES (9101, 77, 'paid', 1299, TIMESTAMP '2026-02-11 00:00:00')",
            &[],
        )
        .await
        .context("insert baseline row into orders")?;

    let duplicate_err = client
        .execute(
            "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at)
             VALUES (9101, 88, 'pending', 2399, TIMESTAMP '2026-02-11 00:00:01')",
            &[],
        )
        .await
        .expect_err("duplicate INSERT should fail with unique-violation");
    assert_sqlstate(&duplicate_err, "23505");

    let count = client
        .query_one(
            "SELECT COUNT(order_id)::BIGINT FROM orders WHERE order_id = 9101",
            &[],
        )
        .await
        .context("count surviving duplicate-key row")?
        .try_get::<_, i64>(0)?;
    assert_eq!(count, 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase4_insert_select_duplicate_primary_key_returns_23505() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    client
        .batch_execute(
            "CREATE TABLE sales_facts (
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
        SELECT
          i AS order_id,
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
        FROM generate_series(1, 2000) AS g(i)";

    let inserted = client
        .execute(insert_stmt, &[])
        .await
        .context("run initial insert-select into sales_facts")?;
    assert_eq!(inserted, 2_000);

    let duplicate_err = client
        .execute(insert_stmt, &[])
        .await
        .expect_err("duplicate insert-select should fail with unique-violation");
    assert_sqlstate(&duplicate_err, "23505");

    let count = client
        .query_one("SELECT COUNT(order_id)::BIGINT FROM sales_facts", &[])
        .await
        .context("count rows after duplicate insert-select failure")?
        .try_get::<_, i64>(0)?;
    assert_eq!(count, 2_000);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase11_create_unique_secondary_index_rejects_duplicate_inserts() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    client
        .batch_execute(
            "CREATE TABLE users_idx (
                user_id BIGINT NOT NULL,
                email TEXT,
                country TEXT NOT NULL,
                status TEXT NOT NULL,
                PRIMARY KEY (user_id)
            );",
        )
        .await
        .context("create users_idx table")?;

    client
        .batch_execute(
            "INSERT INTO users_idx (user_id, email, country, status) VALUES
                (1, 'a@example.com', 'ca', 'active'),
                (2, 'b@example.com', 'us', 'active');",
        )
        .await
        .context("insert baseline users")?;

    client
        .batch_execute("CREATE UNIQUE INDEX users_idx_email_uq ON users_idx (email);")
        .await
        .context("create unique email index")?;

    let duplicate_err = client
        .execute(
            "INSERT INTO users_idx (user_id, email, country, status)
             VALUES (3, 'a@example.com', 'fr', 'active')",
            &[],
        )
        .await
        .expect_err("duplicate unique-index key should fail");
    assert_sqlstate(&duplicate_err, "23505");
    assert_error_contains(&duplicate_err, "users_idx_email_uq");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase11_create_unique_secondary_index_detects_backfill_duplicates() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    client
        .batch_execute(
            "CREATE TABLE users_dup_idx (
                user_id BIGINT NOT NULL,
                email TEXT,
                region TEXT NOT NULL,
                PRIMARY KEY (user_id)
            );",
        )
        .await
        .context("create users_dup_idx table")?;

    client
        .batch_execute(
            "INSERT INTO users_dup_idx (user_id, email, region) VALUES
                (1, 'dup@example.com', 'ca'),
                (2, 'dup@example.com', 'us');",
        )
        .await
        .context("insert duplicate email rows before unique index")?;

    let err = client
        .batch_execute("CREATE UNIQUE INDEX users_dup_idx_email_uq ON users_dup_idx (email);")
        .await
        .expect_err("unique-index backfill should fail on duplicates");
    assert_sqlstate(&err, "23505");
    assert_error_contains(&err, "users_dup_idx_email_uq");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase11_create_hash_secondary_index_and_drop() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    client
        .batch_execute(
            "CREATE TABLE sales_idx (
                order_id BIGINT NOT NULL,
                merchant_id BIGINT NOT NULL,
                event_day BIGINT NOT NULL,
                status TEXT NOT NULL,
                amount_cents BIGINT NOT NULL,
                PRIMARY KEY (order_id)
            );",
        )
        .await
        .context("create sales_idx table")?;

    client
        .batch_execute(
            "INSERT INTO sales_idx (order_id, merchant_id, event_day, status, amount_cents)
             SELECT
               i,
               1 + (i % 10),
               1 + ((i - 1) / 100),
               CASE WHEN i % 2 = 0 THEN 'paid' ELSE 'pending' END,
               100 + i
             FROM generate_series(1, 500) AS g(i);",
        )
        .await
        .context("seed sales_idx rows")?;

    client
        .batch_execute(
            "CREATE INDEX sales_idx_mday_hash
             ON sales_idx USING HASH (merchant_id, event_day)
             INCLUDE (status, amount_cents)
             WITH (hash_buckets = 16);",
        )
        .await
        .context("create hash secondary index with include columns")?;

    client
        .batch_execute("DROP INDEX sales_idx_mday_hash;")
        .await
        .context("drop hash secondary index")?;

    client
        .batch_execute(
            "INSERT INTO sales_idx (order_id, merchant_id, event_day, status, amount_cents)
             VALUES (1001, 3, 8, 'paid', 4242);",
        )
        .await
        .context("insert row after index drop")?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase4_create_table_registers_metadata_and_supports_insert_select() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    client
        .batch_execute(
            "CREATE TABLE orders_custom (
                order_id BIGINT NOT NULL,
                customer_id BIGINT NOT NULL,
                status TEXT,
                total_cents BIGINT NOT NULL,
                created_at TIMESTAMP NOT NULL,
                PRIMARY KEY (order_id)
            );",
        )
        .await
        .context("create orders_custom table")?;

    client
        .batch_execute(
            "INSERT INTO orders_custom (order_id, customer_id, status, total_cents, created_at) VALUES
             (81001, 31, 'paid', 900, TIMESTAMP '2026-02-11 19:00:00'),
             (81002, 32, 'pending', 1900, TIMESTAMP '2026-02-11 19:00:01');",
        )
        .await
        .context("insert rows into created table")?;

    let count = client
        .query_one("SELECT COUNT(*)::BIGINT FROM orders_custom", &[])
        .await
        .context("count rows from created table")?
        .try_get::<_, i64>(0)?;
    assert_eq!(count, 2);

    let updated = client
        .execute(
            "UPDATE orders_custom SET status = 'settled' WHERE order_id = 81001",
            &[],
        )
        .await
        .context("update row in created table")?;
    assert_eq!(updated, 1);

    let deleted = client
        .execute("DELETE FROM orders_custom WHERE order_id = 81002", &[])
        .await
        .context("delete row in created table")?;
    assert_eq!(deleted, 1);

    let duplicate_err = client
        .batch_execute(
            "CREATE TABLE orders_custom (
                order_id BIGINT NOT NULL,
                customer_id BIGINT NOT NULL,
                status TEXT,
                total_cents BIGINT NOT NULL,
                created_at TIMESTAMP NOT NULL,
                PRIMARY KEY (order_id)
            );",
        )
        .await
        .expect_err("duplicate create table should fail");
    assert_sqlstate(&duplicate_err, "42P07");

    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS orders_custom (
                order_id BIGINT NOT NULL,
                customer_id BIGINT NOT NULL,
                status TEXT,
                total_cents BIGINT NOT NULL,
                created_at TIMESTAMP NOT NULL,
                PRIMARY KEY (order_id)
            );",
        )
        .await
        .context("create table if not exists should succeed")?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase10_hash_primary_key_create_insert_and_scan() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    client
        .batch_execute(
            "CREATE TABLE sales_hash (
                order_id BIGINT NOT NULL,
                amount_cents BIGINT NOT NULL,
                status TEXT,
                PRIMARY KEY (order_id) USING HASH
            ) WITH (hash_buckets = 8);",
        )
        .await
        .context("create hash-distributed primary-key table")?;

    let inserted = client
        .execute(
            "INSERT INTO sales_hash (order_id, amount_cents, status)
             SELECT i, 100 + (i % 5000), CASE WHEN i % 2 = 0 THEN 'ok' ELSE 'hold' END
             FROM generate_series(1, 2000) AS g(i);",
            &[],
        )
        .await
        .context("insert rows into hash primary-key table")?;
    assert_eq!(inserted, 2_000);

    let count = client
        .query_one("SELECT COUNT(*)::BIGINT FROM sales_hash", &[])
        .await
        .context("count rows from hash table")?
        .try_get::<_, i64>(0)?;
    assert_eq!(count, 2_000);

    let bounded = client
        .query_one(
            "SELECT COUNT(*)::BIGINT
             FROM sales_hash
             WHERE order_id >= 500 AND order_id <= 520",
            &[],
        )
        .await
        .context("bounded PK scan over hash table")?
        .try_get::<_, i64>(0)?;
    assert_eq!(bounded, 21);

    let metadata = find_table_metadata_by_name(&harness.holostore_client(), "sales_hash")
        .await
        .context("lookup sales_hash metadata")?
        .context("sales_hash metadata should exist")?;
    assert_eq!(
        metadata.primary_key_distribution,
        PrimaryKeyDistribution::Hash
    );
    assert_eq!(metadata.primary_key_hash_buckets, Some(8));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase10_hash_primary_key_migration_backfills_online() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    client
        .batch_execute(
            "CREATE TABLE sales_migrate (
                order_id BIGINT NOT NULL,
                amount_cents BIGINT NOT NULL,
                status TEXT NOT NULL,
                PRIMARY KEY (order_id)
            );",
        )
        .await
        .context("create range-distributed migration table")?;

    let inserted = client
        .execute(
            "INSERT INTO sales_migrate (order_id, amount_cents, status)
             SELECT i, 100 + (i % 500), 'seed'
             FROM generate_series(1, 1500) AS g(i);",
            &[],
        )
        .await
        .context("seed migration source rows")?;
    assert_eq!(inserted, 1_500);

    let metadata_before = find_table_metadata_by_name(&harness.holostore_client(), "sales_migrate")
        .await
        .context("read sales_migrate metadata before migration")?
        .context("sales_migrate metadata should exist")?;
    assert_eq!(
        metadata_before.primary_key_distribution,
        PrimaryKeyDistribution::Range
    );
    let source_provider = HoloStoreTableProvider::from_table_metadata(
        &metadata_before,
        harness.holostore_client(),
        Arc::new(PushdownMetrics::default()),
    )
    .context("build source provider before hash migration")?;
    let source_rows = source_provider
        .scan_generic_rows_with_versions_by_primary_key_bounds(None, None, None)
        .await
        .context("scan source rows before hash migration")?;
    assert_eq!(source_rows.len(), 1_500);
    let cleanup_keys = source_rows
        .iter()
        .map(|row| row.primary_key)
        .collect::<Vec<_>>();

    let mut metadata_after = metadata_before.clone();
    metadata_after.primary_key_distribution = PrimaryKeyDistribution::Hash;
    metadata_after.primary_key_hash_buckets = Some(16);
    let target_provider = HoloStoreTableProvider::from_table_metadata(
        &metadata_after,
        harness.holostore_client(),
        Arc::new(PushdownMetrics::default()),
    )
    .context("build target hash provider for migration")?;
    let migrated_values = source_rows
        .iter()
        .map(|row| row.values.clone())
        .collect::<Vec<_>>();
    let migrated = target_provider
        .upsert_generic_rows(migrated_values.as_slice())
        .await
        .context("backfill hash layout rows")?;
    assert_eq!(migrated, 1_500);

    update_table_primary_key_distribution(
        &harness.holostore_client(),
        "sales_migrate",
        PrimaryKeyDistribution::Hash,
        Some(16),
    )
    .await
    .context("persist hash distribution metadata after backfill")?;

    source_provider
        .tombstone_generic_rows_by_primary_key(cleanup_keys.as_slice())
        .await
        .context("cleanup stale range-layout rows after hash migration")?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let metadata = find_table_metadata_by_name(&harness.holostore_client(), "sales_migrate")
        .await
        .context("lookup migrated table metadata")?
        .context("sales_migrate metadata should exist")?;
    assert_eq!(
        metadata.primary_key_distribution,
        PrimaryKeyDistribution::Hash
    );
    assert_eq!(metadata.primary_key_hash_buckets, Some(16));

    let count = client
        .query_one("SELECT COUNT(*)::BIGINT FROM sales_migrate", &[])
        .await
        .context("count rows after hash migration")?
        .try_get::<_, i64>(0)?;
    assert_eq!(count, 1_500);

    let inserted_after = client
        .execute(
            "INSERT INTO sales_migrate (order_id, amount_cents, status)
             SELECT 1500 + i, 200 + (i % 700), 'post'
             FROM generate_series(1, 500) AS g(i);",
            &[],
        )
        .await
        .context("insert rows after hash migration")?;
    assert_eq!(inserted_after, 500);

    let total = client
        .query_one("SELECT COUNT(*)::BIGINT FROM sales_migrate", &[])
        .await
        .context("count rows after post-migration insert")?
        .try_get::<_, i64>(0)?;
    assert_eq!(total, 2_000);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase8_generic_row_model_create_insert_select() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    client
        .batch_execute(
            "CREATE TABLE app_events (
                event_id BIGINT NOT NULL,
                user_name TEXT NOT NULL,
                payload_size DOUBLE PRECISION NOT NULL,
                processed BOOLEAN NOT NULL,
                created_at TIMESTAMP NOT NULL,
                PRIMARY KEY (event_id)
            );",
        )
        .await
        .context("create app_events row_v1 table")?;

    client
        .batch_execute(
            "INSERT INTO app_events (event_id, user_name, payload_size, processed, created_at) VALUES
             (1, 'alice', 42.5, true, TIMESTAMP '2026-02-12 00:00:00'),
             (2, 'bob', 13.0, false, TIMESTAMP '2026-02-12 00:00:01');",
        )
        .await
        .context("insert rows into app_events")?;

    let rows = client
        .query(
            "SELECT event_id, user_name, payload_size, processed
             FROM app_events
             ORDER BY event_id",
            &[],
        )
        .await
        .context("select rows from app_events")?;
    assert_eq!(rows.len(), 2);

    let first_id: i64 = rows[0].try_get(0)?;
    let first_name: String = rows[0].try_get(1)?;
    let first_payload: f64 = rows[0].try_get(2)?;
    let first_processed: bool = rows[0].try_get(3)?;
    assert_eq!(first_id, 1);
    assert_eq!(first_name, "alice");
    assert!((first_payload - 42.5f64).abs() < f64::EPSILON);
    assert!(first_processed);

    let second_id: i64 = rows[1].try_get(0)?;
    let second_name: String = rows[1].try_get(1)?;
    let second_payload: f64 = rows[1].try_get(2)?;
    let second_processed: bool = rows[1].try_get(3)?;
    assert_eq!(second_id, 2);
    assert_eq!(second_name, "bob");
    assert!((second_payload - 13.0f64).abs() < f64::EPSILON);
    assert!(!second_processed);

    let updated = client
        .execute(
            "UPDATE app_events
             SET payload_size = 99.75, processed = true
             WHERE event_id = 2",
            &[],
        )
        .await
        .context("update row_v1 row in app_events")?;
    assert_eq!(updated, 1);

    let updated_row = client
        .query_one(
            "SELECT payload_size, processed FROM app_events WHERE event_id = 2",
            &[],
        )
        .await
        .context("verify updated row_v1 row in app_events")?;
    let updated_payload: f64 = updated_row.try_get(0)?;
    let updated_processed: bool = updated_row.try_get(1)?;
    assert!((updated_payload - 99.75f64).abs() < f64::EPSILON);
    assert!(updated_processed);

    let deleted = client
        .execute("DELETE FROM app_events WHERE event_id = 1", &[])
        .await
        .context("delete row_v1 row from app_events")?;
    assert_eq!(deleted, 1);

    let remaining = client
        .query_one("SELECT COUNT(*)::BIGINT FROM app_events", &[])
        .await
        .context("count remaining row_v1 rows in app_events")?
        .try_get::<_, i64>(0)?;
    assert_eq!(remaining, 1);

    client
        .execute("BEGIN", &[])
        .await
        .context("begin explicit transaction for row_v1 DML boundary check")?;

    let tx_update_err = client
        .execute(
            "UPDATE app_events SET payload_size = 50.0 WHERE event_id = 2",
            &[],
        )
        .await
        .expect_err("row_v1 UPDATE inside explicit transaction must fail until supported");
    assert_sqlstate(&tx_update_err, "0A000");

    let tx_delete_err = client
        .execute("DELETE FROM app_events WHERE event_id = 2", &[])
        .await
        .expect_err("row_v1 DELETE inside explicit transaction must fail until supported");
    assert_sqlstate(&tx_delete_err, "0A000");

    client
        .execute("ROLLBACK", &[])
        .await
        .context("rollback explicit transaction for row_v1 DML boundary check")?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase8_row_v1_defaults_and_check_constraints() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    client
        .batch_execute(
            "CREATE TABLE metric_defaults (
                metric_id BIGINT NOT NULL,
                status TEXT NOT NULL DEFAULT 'new',
                sample_count BIGINT NOT NULL DEFAULT 0,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (metric_id),
                CHECK (sample_count >= 0),
                CONSTRAINT status_non_empty CHECK (status <> '')
            );",
        )
        .await
        .context("create row_v1 table with defaults and checks")?;

    let inserted = client
        .execute(
            "INSERT INTO metric_defaults (metric_id) VALUES (1), (2)",
            &[],
        )
        .await
        .context("insert rows that rely on DEFAULT values")?;
    assert_eq!(inserted, 2);

    let row = client
        .query_one(
            "SELECT status, sample_count, created_at IS NOT NULL
             FROM metric_defaults
             WHERE metric_id = 1",
            &[],
        )
        .await
        .context("read back defaulted row values")?;
    assert_eq!(row.try_get::<_, String>(0)?, "new");
    assert_eq!(row.try_get::<_, i64>(1)?, 0);
    assert!(row.try_get::<_, bool>(2)?);

    let check_insert_err = client
        .execute(
            "INSERT INTO metric_defaults (metric_id, sample_count) VALUES (3, -1)",
            &[],
        )
        .await
        .expect_err("negative sample_count must violate CHECK");
    assert_error_contains(&check_insert_err, "check constraint");

    let null_insert_err = client
        .execute(
            "INSERT INTO metric_defaults (metric_id, status) VALUES (4, NULL)",
            &[],
        )
        .await
        .expect_err("explicit NULL status must violate not-null");
    assert_error_contains(&null_insert_err, "violates not-null constraint");

    let check_update_err = client
        .execute(
            "UPDATE metric_defaults SET sample_count = -5 WHERE metric_id = 1",
            &[],
        )
        .await
        .expect_err("UPDATE must enforce CHECK constraints");
    assert_sqlstate(&check_update_err, "23514");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase8_uint64_type_coverage_and_range_enforcement() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    client
        .batch_execute(
            "CREATE TABLE metrics_uint64 (
                id BIGINT NOT NULL,
                counter BIGINT UNSIGNED NOT NULL DEFAULT 18446744073709551614,
                PRIMARY KEY (id),
                CHECK (counter > 9007199254740992)
            );",
        )
        .await
        .context("create uint64 coverage table")?;

    client
        .execute("INSERT INTO metrics_uint64 (id) VALUES (1)", &[])
        .await
        .context("insert row relying on uint64 default")?;

    let count = client
        .query_one(
            "SELECT COUNT(*)::BIGINT FROM metrics_uint64 WHERE counter > 9007199254740992",
            &[],
        )
        .await
        .context("read back row using large-uint64 predicate")?
        .try_get::<_, i64>(0)?;
    assert_eq!(count, 1);

    client
        .execute(
            "UPDATE metrics_uint64 SET counter = 18446744073709551615 WHERE id = 1",
            &[],
        )
        .await
        .context("update uint64 to max range value")?;

    let negative_err = client
        .execute("UPDATE metrics_uint64 SET counter = -1 WHERE id = 1", &[])
        .await
        .expect_err("negative update should fail for uint64");
    assert_sqlstate(&negative_err, "22023");

    let overflow_err = client
        .execute(
            "UPDATE metrics_uint64 SET counter = 18446744073709551616 WHERE id = 1",
            &[],
        )
        .await
        .expect_err("overflowing update should fail for uint64");
    assert_sqlstate(&overflow_err, "22023");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase5_update_delete_strict_semantics_and_prepared_retry() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    client
        .batch_execute(
            "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at) VALUES
             (3001, 11, 'new', 100, TIMESTAMP '2026-02-11 12:00:00'),
             (3002, 11, 'new', 200, TIMESTAMP '2026-02-11 12:01:00'),
             (3003, 22, 'new', 300, TIMESTAMP '2026-02-11 12:02:00');",
        )
        .await
        .context("seed rows for update/delete")?;

    let updated = client
        .execute(
            "UPDATE orders SET status = $1, total_cents = $2 WHERE order_id = $3",
            &[&"paid", &150i64, &3001i64],
        )
        .await
        .context("run prepared UPDATE")?;
    assert_eq!(updated, 1);

    let retried_update = client
        .execute(
            "UPDATE orders SET status = $1, total_cents = $2 WHERE order_id = $3",
            &[&"paid", &150i64, &3001i64],
        )
        .await
        .context("retry prepared UPDATE")?;
    assert_eq!(retried_update, 1);

    let row = client
        .query_one(
            "SELECT status, total_cents FROM orders WHERE order_id = 3001",
            &[],
        )
        .await
        .context("read updated row")?;
    assert_eq!(row.try_get::<_, String>(0)?, "paid");
    assert_eq!(row.try_get::<_, i64>(1)?, 150);

    let deleted = client
        .execute("DELETE FROM orders WHERE order_id = $1", &[&3002i64])
        .await
        .context("delete with prepared statement")?;
    assert_eq!(deleted, 1);

    let retried_delete = client
        .execute("DELETE FROM orders WHERE order_id = $1", &[&3002i64])
        .await
        .context("retry delete with prepared statement")?;
    assert_eq!(retried_delete, 0);

    let remaining = client
        .query(
            "SELECT order_id FROM orders WHERE order_id >= 3001 ORDER BY order_id",
            &[],
        )
        .await
        .context("read remaining rows after delete")?;
    assert_eq!(remaining.len(), 2);
    assert_eq!(remaining[0].try_get::<_, i64>(0)?, 3001);
    assert_eq!(remaining[1].try_get::<_, i64>(0)?, 3003);

    let full_update_err = client
        .execute("UPDATE orders SET status = 'x'", &[])
        .await
        .expect_err("full table UPDATE must fail");
    assert_sqlstate(&full_update_err, "0A000");

    let non_pk_update_err = client
        .execute("UPDATE orders SET status = 'x' WHERE customer_id = 11", &[])
        .await
        .expect_err("non-PK UPDATE must fail");
    assert_sqlstate(&non_pk_update_err, "0A000");

    let full_delete_err = client
        .execute("DELETE FROM orders", &[])
        .await
        .expect_err("full table DELETE must fail");
    assert_sqlstate(&full_delete_err, "0A000");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase5_concurrent_update_conflict_reports_40001() -> Result<()> {
    let harness = TestHarness::start_with_dml_prewrite_delay(Duration::from_millis(200)).await?;
    let (seed_client, _seed_guard) = harness.connect_pg().await?;

    seed_client
        .batch_execute(
            "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at) VALUES
             (4001, 33, 'open', 500, TIMESTAMP '2026-02-11 13:00:00');",
        )
        .await
        .context("seed row for concurrent conflict test")?;

    let (client_a, _guard_a) = harness.connect_pg().await?;
    let (client_b, _guard_b) = harness.connect_pg().await?;

    let update_a = client_a.execute(
        "UPDATE orders SET status = 'alpha' WHERE order_id = 4001",
        &[],
    );
    let update_b = client_b.execute(
        "UPDATE orders SET status = 'beta' WHERE order_id = 4001",
        &[],
    );
    let (res_a, res_b) = tokio::join!(update_a, update_b);

    let mut applied = 0usize;
    let mut conflicts = 0usize;
    for result in [res_a, res_b] {
        match result {
            Ok(rows) => {
                assert_eq!(rows, 1);
                applied += 1;
            }
            Err(err) => {
                assert_sqlstate(&err, "40001");
                conflicts += 1;
            }
        }
    }

    assert_eq!(applied, 1, "expected exactly one successful update");
    assert_eq!(conflicts, 1, "expected exactly one write-write conflict");

    let row = seed_client
        .query_one("SELECT status FROM orders WHERE order_id = 4001", &[])
        .await
        .context("read row after concurrent conflict")?;
    let status = row.try_get::<_, String>(0)?;
    assert!(
        status == "alpha" || status == "beta",
        "unexpected final status after concurrent conflict: {status}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase6_begin_commit_snapshot_and_read_your_writes() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client_a, _guard_a) = harness.connect_pg().await?;
    let (client_b, _guard_b) = harness.connect_pg().await?;

    let commit_without_tx = client_a
        .execute("COMMIT", &[])
        .await
        .context("COMMIT without BEGIN should be a no-op")?;
    assert_eq!(commit_without_tx, 0);

    let rollback_without_tx = client_a
        .execute("ROLLBACK", &[])
        .await
        .context("ROLLBACK without BEGIN should be a no-op")?;
    assert_eq!(rollback_without_tx, 0);

    client_a
        .execute("BEGIN", &[])
        .await
        .context("start explicit transaction")?;
    client_a
        .execute(
            "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at)
             VALUES (6001, 61, 'txn_visible', 100, TIMESTAMP '2026-02-11 14:00:00')",
            &[],
        )
        .await
        .context("insert row inside transaction")?;

    let in_tx_count = client_a
        .query_one(
            "SELECT COUNT(*)::BIGINT FROM orders WHERE order_id = 6001",
            &[],
        )
        .await
        .context("read own writes inside transaction")?
        .try_get::<_, i64>(0)?;
    assert_eq!(in_tx_count, 1);

    let outside_count_before_commit = client_b
        .query_one(
            "SELECT COUNT(*)::BIGINT FROM orders WHERE order_id = 6001",
            &[],
        )
        .await
        .context("outside session read before commit")?
        .try_get::<_, i64>(0)?;
    assert_eq!(outside_count_before_commit, 0);

    client_b
        .execute(
            "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at)
             VALUES (6002, 62, 'outside', 200, TIMESTAMP '2026-02-11 14:01:00')",
            &[],
        )
        .await
        .context("insert concurrent row outside transaction")?;

    let snapshot_count = client_a
        .query_one(
            "SELECT COUNT(*)::BIGINT FROM orders WHERE order_id = 6002",
            &[],
        )
        .await
        .context("snapshot read should not see post-BEGIN row")?
        .try_get::<_, i64>(0)?;
    assert_eq!(snapshot_count, 0);

    client_a
        .execute("COMMIT", &[])
        .await
        .context("commit transaction")?;

    let outside_count_after_commit = client_b
        .query_one(
            "SELECT COUNT(*)::BIGINT FROM orders WHERE order_id = 6001",
            &[],
        )
        .await
        .context("outside session read after commit")?
        .try_get::<_, i64>(0)?;
    assert_eq!(outside_count_after_commit, 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase6_rollback_discards_uncommitted_changes() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    client
        .execute(
            "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at)
             VALUES (6101, 71, 'seed', 300, TIMESTAMP '2026-02-11 15:00:00')",
            &[],
        )
        .await
        .context("seed rollback test row")?;

    client
        .execute("BEGIN", &[])
        .await
        .context("start rollback test transaction")?;
    client
        .execute(
            "UPDATE orders SET status = 'updated_in_tx' WHERE order_id = 6101",
            &[],
        )
        .await
        .context("update row inside transaction")?;
    client
        .execute(
            "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at)
             VALUES (6102, 72, 'inserted_in_tx', 400, TIMESTAMP '2026-02-11 15:01:00')",
            &[],
        )
        .await
        .context("insert row inside transaction")?;

    let in_tx_rows = client
        .query(
            "SELECT order_id, status FROM orders WHERE order_id BETWEEN 6101 AND 6102 ORDER BY order_id",
            &[],
        )
        .await
        .context("read rows inside transaction before rollback")?;
    assert_eq!(in_tx_rows.len(), 2);

    client
        .execute("ROLLBACK", &[])
        .await
        .context("rollback transaction")?;

    let base_row = client
        .query_one("SELECT status FROM orders WHERE order_id = 6101", &[])
        .await
        .context("read base row after rollback")?;
    assert_eq!(base_row.try_get::<_, String>(0)?, "seed");

    let rolled_back_insert = client
        .query_one(
            "SELECT COUNT(*)::BIGINT FROM orders WHERE order_id = 6102",
            &[],
        )
        .await
        .context("verify inserted row is gone after rollback")?
        .try_get::<_, i64>(0)?;
    assert_eq!(rolled_back_insert, 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase6_commit_conflict_aborts_transaction_until_rollback() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client_a, _guard_a) = harness.connect_pg().await?;
    let (client_b, _guard_b) = harness.connect_pg().await?;

    client_a
        .execute(
            "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at)
             VALUES (6201, 81, 'open', 500, TIMESTAMP '2026-02-11 16:00:00')",
            &[],
        )
        .await
        .context("seed conflict row")?;

    client_a
        .execute("BEGIN", &[])
        .await
        .context("start transaction A")?;
    client_b
        .execute("BEGIN", &[])
        .await
        .context("start transaction B")?;

    client_a
        .execute(
            "UPDATE orders SET status = 'winner' WHERE order_id = 6201",
            &[],
        )
        .await
        .context("stage update in transaction A")?;
    client_b
        .execute(
            "UPDATE orders SET status = 'loser' WHERE order_id = 6201",
            &[],
        )
        .await
        .context("stage update in transaction B")?;

    client_a
        .execute("COMMIT", &[])
        .await
        .context("commit transaction A")?;

    let conflict = client_b
        .execute("COMMIT", &[])
        .await
        .expect_err("transaction B commit should conflict");
    assert_sqlstate(&conflict, "40001");

    let aborted = client_b
        .execute("SELECT 1", &[])
        .await
        .expect_err("aborted transaction should block statements");
    assert_sqlstate(&aborted, "25P02");

    client_b
        .execute("ROLLBACK", &[])
        .await
        .context("clear aborted transaction state with rollback")?;

    let select_after_rollback = client_b
        .query_one("SELECT 1", &[])
        .await
        .context("session should be usable after rollback")?;
    assert_eq!(select_after_rollback.try_get::<_, i64>(0)?, 1);

    let final_row = client_a
        .query_one("SELECT status FROM orders WHERE order_id = 6201", &[])
        .await
        .context("read winner row after conflict")?;
    assert_eq!(final_row.try_get::<_, String>(0)?, "winner");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase7_transaction_control_protocol_parity_simple_vs_extended() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    let commit_extended = client
        .execute("COMMIT", &[])
        .await
        .context("extended COMMIT without BEGIN should be a no-op")?;
    assert_eq!(commit_extended, 0);

    let commit_simple = client
        .simple_query("COMMIT")
        .await
        .context("simple COMMIT without BEGIN should be a no-op")?;
    assert!(
        matches!(
            commit_simple.as_slice(),
            [tokio_postgres::SimpleQueryMessage::CommandComplete(0)]
        ),
        "unexpected simple-query COMMIT response: {commit_simple:?}"
    );

    client
        .simple_query("BEGIN")
        .await
        .context("simple protocol BEGIN should succeed")?;

    let begin_again_simple = client
        .simple_query("BEGIN")
        .await
        .context("simple protocol BEGIN while tx active should be a no-op")?;
    assert!(
        matches!(
            begin_again_simple.as_slice(),
            [tokio_postgres::SimpleQueryMessage::CommandComplete(0)]
        ),
        "unexpected simple-query BEGIN response: {begin_again_simple:?}"
    );

    client
        .simple_query("ROLLBACK")
        .await
        .context("simple protocol ROLLBACK should succeed")?;

    client
        .execute("BEGIN", &[])
        .await
        .context("extended BEGIN should succeed")?;
    let begin_again_extended = client
        .execute("BEGIN", &[])
        .await
        .context("extended BEGIN while tx active should be a no-op")?;
    assert_eq!(begin_again_extended, 0);
    client
        .execute("ROLLBACK", &[])
        .await
        .context("extended ROLLBACK should succeed")?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase7_statement_timeout_maps_to_57014() -> Result<()> {
    let harness = TestHarness::start_with_dml_config(DmlHarnessConfig {
        prewrite_delay: Duration::from_millis(250),
        statement_timeout: Duration::from_millis(50),
        ..DmlHarnessConfig::default()
    })
    .await?;
    let (client, _guard) = harness.connect_pg().await?;

    client
        .execute(
            "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at)
             VALUES (7301, 33, 'base', 100, TIMESTAMP '2026-02-12 00:00:00')",
            &[],
        )
        .await
        .context("seed timeout row")?;

    let timeout_err = client
        .execute(
            "UPDATE orders SET status = 'late' WHERE order_id = 7301",
            &[],
        )
        .await
        .expect_err("update should time out with configured statement timeout");
    assert_sqlstate(&timeout_err, "57014");

    let row = client
        .query_one("SELECT status FROM orders WHERE order_id = 7301", &[])
        .await
        .context("read row after timeout")?;
    assert_eq!(row.try_get::<_, String>(0)?, "base");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase7_admission_control_rejects_overload_with_53300() -> Result<()> {
    let harness = TestHarness::start_with_dml_config(DmlHarnessConfig {
        prewrite_delay: Duration::from_millis(250),
        max_inflight_statements: 1,
        ..DmlHarnessConfig::default()
    })
    .await?;
    let (seed, _seed_guard) = harness.connect_pg().await?;

    seed.batch_execute(
        "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at) VALUES
         (7401, 10, 'base', 100, TIMESTAMP '2026-02-12 00:00:00'),
         (7402, 11, 'base', 200, TIMESTAMP '2026-02-12 00:00:01');",
    )
    .await
    .context("seed admission rows")?;

    let (client_a, _guard_a) = harness.connect_pg().await?;
    let (client_b, _guard_b) = harness.connect_pg().await?;

    let update_a = tokio::spawn(async move {
        client_a
            .execute("UPDATE orders SET status = 'a' WHERE order_id = 7401", &[])
            .await
    });
    tokio::time::sleep(Duration::from_millis(25)).await;
    let update_b = client_b
        .execute("UPDATE orders SET status = 'b' WHERE order_id = 7402", &[])
        .await;

    let result_a = update_a.await.context("join first overload update task")?;

    let mut overload_errors = 0usize;
    for result in [result_a, update_b] {
        match result {
            Ok(_) => {}
            Err(err) => {
                assert_sqlstate(&err, "53300");
                overload_errors += 1;
            }
        }
    }
    assert_eq!(
        overload_errors, 1,
        "expected exactly one overload rejection"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase7_scan_and_txn_staging_limits_map_to_54000() -> Result<()> {
    let harness = TestHarness::start_with_dml_config(DmlHarnessConfig {
        max_scan_rows: 1,
        max_txn_staged_rows: 2,
        ..DmlHarnessConfig::default()
    })
    .await?;
    let (client, _guard) = harness.connect_pg().await?;

    client
        .batch_execute(
            "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at) VALUES
             (7501, 1, 'base', 100, TIMESTAMP '2026-02-12 00:00:00'),
             (7502, 2, 'base', 200, TIMESTAMP '2026-02-12 00:00:01');",
        )
        .await
        .context("seed rows for scan limit test")?;

    let scan_limit_err = client
        .execute(
            "UPDATE orders SET status = 'limited' WHERE order_id >= 7501 AND order_id <= 7502",
            &[],
        )
        .await
        .expect_err("scan row limit should reject multi-row UPDATE");
    assert_sqlstate(&scan_limit_err, "54000");

    client
        .execute("BEGIN", &[])
        .await
        .context("begin transaction for staged row limit test")?;

    client
        .execute(
            "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at)
             VALUES (7601, 11, 'tx', 100, TIMESTAMP '2026-02-12 00:00:10')",
            &[],
        )
        .await
        .context("stage first tx row")?;
    client
        .execute(
            "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at)
             VALUES (7602, 12, 'tx', 100, TIMESTAMP '2026-02-12 00:00:11')",
            &[],
        )
        .await
        .context("stage second tx row")?;

    let stage_limit_err = client
        .execute(
            "INSERT INTO orders (order_id, customer_id, status, total_cents, created_at)
             VALUES (7603, 13, 'tx', 100, TIMESTAMP '2026-02-12 00:00:12')",
            &[],
        )
        .await
        .expect_err("third staged row should exceed transaction staging limit");
    assert_sqlstate(&stage_limit_err, "54000");

    client
        .execute("ROLLBACK", &[])
        .await
        .context("rollback after staged row limit failure")?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase7_metrics_expose_txn_distributed_and_guardrail_counters() -> Result<()> {
    let harness = TestHarness::start().await?;
    let metrics = harness.metrics_body().context("fetch metrics body")?;
    for key in [
        "tx_begin_count=",
        "tx_commit_count=",
        "tx_rollback_count=",
        "tx_conflict_count=",
        "distributed_write_apply_ops=",
        "distributed_write_rollback_ops=",
        "distributed_write_conflicts=",
        "statement_timeout_count=",
        "admission_reject_count=",
        "scan_row_limit_reject_count=",
        "txn_stage_limit_reject_count=",
        "query_execution_started=",
        "query_execution_completed=",
        "query_execution_failed=",
        "stage_events=",
        "scan_retry_count=",
        "scan_reroute_count=",
        "scan_chunk_count=",
        "scan_duplicate_rows_skipped=",
        "optimizer_plan_table_scan=",
        "optimizer_plan_index_scan=",
        "optimizer_plan_index_only=",
        "optimizer_bad_miss_count=",
        "optimizer_estimated_rows_total=",
        "optimizer_actual_rows_total=",
        "active_query_sessions=",
        "ingest_rows_ingested_total=",
        "ingest_jobs_started=",
        "ingest_jobs_completed=",
        "ingest_jobs_failed=",
        "ingest_jobs_tracked=",
    ] {
        assert!(
            metrics.contains(key),
            "missing metric key '{key}' in body: {metrics}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase10_metrics_expose_circuit_and_ingest_status() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    client
        .batch_execute(
            "CREATE TABLE phase10_metrics_ingest (
                order_id BIGINT NOT NULL,
                amount_cents BIGINT NOT NULL,
                status TEXT NOT NULL,
                PRIMARY KEY (order_id) USING HASH
            ) WITH (hash_buckets = 4);",
        )
        .await
        .context("create phase10 metrics ingest table")?;

    client
        .execute(
            "INSERT INTO phase10_metrics_ingest (order_id, amount_cents, status)
             SELECT i, 100 + (i % 1000), 'ok'
             FROM generate_series(1, 2000) AS g(i);",
            &[],
        )
        .await
        .context("run bulk ingest for metrics exposure test")?;

    let metrics = harness.metrics_body().context("fetch metrics body")?;
    assert!(
        metrics.contains("distributed_write_target_"),
        "expected per-target metrics in body: {metrics}"
    );
    assert!(
        metrics.contains("_circuit_state="),
        "expected circuit state metrics in body: {metrics}"
    );
    assert!(
        metrics.contains("ingest_job_"),
        "expected ingest job metrics in body: {metrics}"
    );
    assert!(
        metrics.contains("status=completed"),
        "expected completed ingest status in body: {metrics}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase9_explain_standard_returns_query_plan_column() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    let rows = client
        .query(
            "EXPLAIN SELECT order_id FROM orders WHERE order_id >= 1 ORDER BY order_id LIMIT 5",
            &[],
        )
        .await
        .context("run standard explain query")?;
    assert!(!rows.is_empty(), "expected explain output rows");
    let line: String = rows[0]
        .try_get(0)
        .context("read query plan column from explain output")?;
    assert!(
        !line.trim().is_empty(),
        "expected non-empty query plan output"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase9_explain_dist_reports_stage_placement() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    let rows = client
        .query(
            "EXPLAIN (DIST) SELECT order_id FROM orders WHERE order_id >= 1 ORDER BY order_id LIMIT 5",
            &[],
        )
        .await
        .context("run explain dist query")?;
    assert!(
        !rows.is_empty(),
        "expected explain output rows for stage placement"
    );

    let mut has_query_stage = false;
    let mut has_scan_stage = false;
    for row in &rows {
        let stage: String = row
            .try_get(0)
            .context("read stage column from explain output")?;
        if stage == "query" {
            has_query_stage = true;
        }
        if stage == "scan" {
            has_scan_stage = true;
        }
    }

    assert!(
        has_query_stage,
        "expected query stage row in explain output"
    );
    assert!(has_scan_stage, "expected scan stage row in explain output");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase9_explain_dist_json_includes_distribution_sections() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    let rows = client
        .query(
            "EXPLAIN (ANALYZE, DIST, FORMAT JSON) SELECT order_id FROM orders WHERE order_id >= 1 ORDER BY order_id LIMIT 5",
            &[],
        )
        .await
        .context("run explain analyze dist json query")?;
    assert!(
        !rows.is_empty(),
        "expected explain analyze dist json output rows"
    );
    let payload: String = rows[0].try_get(0).context("read json explain payload")?;
    assert!(
        payload.contains("\"distribution\""),
        "expected distribution section in JSON explain output: {payload}"
    );
    assert!(
        payload.contains("\"query_execution_id\""),
        "expected query_execution_id in JSON explain output: {payload}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase12_optimizer_prefers_index_scan_for_selective_filter() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    client
        .batch_execute(
            "CREATE TABLE sales_optimizer (
                order_id BIGINT NOT NULL,
                merchant_id BIGINT NOT NULL,
                event_day BIGINT NOT NULL,
                status TEXT NOT NULL,
                amount_cents BIGINT NOT NULL,
                PRIMARY KEY (order_id)
            );",
        )
        .await
        .context("create sales_optimizer table")?;
    client
        .execute(
            "INSERT INTO sales_optimizer (order_id, merchant_id, event_day, status, amount_cents)
             SELECT
               i,
               1 + (i % 500),
               1 + ((i - 1) / 1000),
               CASE
                   WHEN i % 10 < 6 THEN 'paid'
                   WHEN i % 10 < 8 THEN 'shipped'
                   ELSE 'pending'
               END,
               100 + (i % 2000)
             FROM generate_series(1, 8000) AS g(i);",
            &[],
        )
        .await
        .context("seed sales_optimizer rows")?;
    client
        .batch_execute(
            "CREATE INDEX sales_optimizer_status_day
             ON sales_optimizer (status, event_day, merchant_id)
             INCLUDE (amount_cents);",
        )
        .await
        .context("create secondary index for optimizer test")?;

    let count: i64 = client
        .query_one(
            "SELECT COUNT(*)::BIGINT
             FROM sales_optimizer
             WHERE status = 'shipped' AND event_day = 2;",
            &[],
        )
        .await
        .context("run selective filter query")?
        .try_get(0)?;
    assert!(count > 0, "expected selective query to return rows");

    let metrics = harness.metrics_body().context("fetch metrics body")?;
    let table_plans = metric_value(&metrics, "optimizer_plan_table_scan").unwrap_or(0);
    let index_plans = metric_value(&metrics, "optimizer_plan_index_scan").unwrap_or(0);
    let index_only_plans = metric_value(&metrics, "optimizer_plan_index_only").unwrap_or(0);
    assert!(
        index_plans + index_only_plans >= 1,
        "expected at least one optimizer index-family plan; table_plans={table_plans} index_plans={index_plans} index_only_plans={index_only_plans} body={metrics}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase12_optimizer_prefers_table_scan_for_low_selectivity_non_covering_index() -> Result<()>
{
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    client
        .batch_execute(
            "CREATE TABLE sales_optimizer_low_sel (
                order_id BIGINT NOT NULL,
                merchant_id BIGINT NOT NULL,
                event_day BIGINT NOT NULL,
                status TEXT NOT NULL,
                amount_cents BIGINT NOT NULL,
                PRIMARY KEY (order_id)
            );",
        )
        .await
        .context("create sales_optimizer_low_sel table")?;
    client
        .execute(
            "INSERT INTO sales_optimizer_low_sel (order_id, merchant_id, event_day, status, amount_cents)
             SELECT
               i,
               1 + (i % 500),
               1,
               CASE
                   WHEN i % 10 < 6 THEN 'paid'
                   WHEN i % 10 < 8 THEN 'shipped'
                   ELSE 'pending'
               END,
               100 + (i % 2000)
             FROM generate_series(1, 20000) AS g(i);",
            &[],
        )
        .await
        .context("seed sales_optimizer_low_sel rows")?;
    client
        .batch_execute(
            "CREATE INDEX sales_optimizer_low_sel_status_day
             ON sales_optimizer_low_sel (status, event_day, merchant_id);",
        )
        .await
        .context("create non-covering secondary index for low selectivity test")?;

    let metrics_before = harness
        .metrics_body()
        .context("fetch metrics body before query")?;
    let table_before = metric_value(&metrics_before, "optimizer_plan_table_scan").unwrap_or(0);
    let index_before = metric_value(&metrics_before, "optimizer_plan_index_scan").unwrap_or(0);
    let index_only_before = metric_value(&metrics_before, "optimizer_plan_index_only").unwrap_or(0);

    let rows = client
        .query(
            "SELECT merchant_id, event_day, COUNT(*) AS orders, SUM(amount_cents) AS gross_cents
             FROM sales_optimizer_low_sel
             WHERE event_day = 1
               AND status IN ('paid', 'shipped')
             GROUP BY merchant_id, event_day
             HAVING COUNT(*) >= 1
             ORDER BY gross_cents DESC
             LIMIT 200;",
            &[],
        )
        .await
        .context("run low-selectivity aggregate query")?;
    assert!(!rows.is_empty(), "expected aggregate query rows");

    let metrics_after = harness
        .metrics_body()
        .context("fetch metrics body after query")?;
    let table_after = metric_value(&metrics_after, "optimizer_plan_table_scan").unwrap_or(0);
    let index_after = metric_value(&metrics_after, "optimizer_plan_index_scan").unwrap_or(0);
    let index_only_after = metric_value(&metrics_after, "optimizer_plan_index_only").unwrap_or(0);

    let table_delta = table_after.saturating_sub(table_before);
    let index_delta = index_after.saturating_sub(index_before);
    let index_only_delta = index_only_after.saturating_sub(index_only_before);
    assert!(
        table_delta >= 1,
        "expected table-scan preference for low-selectivity non-covering query; table_delta={table_delta} index_delta={index_delta} index_only_delta={index_only_delta} metrics={metrics_after}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase13_grouped_aggregate_topk_pushdown_returns_ranked_rows() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    client
        .batch_execute(
            "CREATE TABLE sales_pushdown (
                order_id BIGINT NOT NULL,
                customer_id BIGINT NOT NULL,
                merchant_id BIGINT NOT NULL,
                region_id BIGINT NOT NULL,
                event_day BIGINT NOT NULL,
                status TEXT NOT NULL,
                amount_cents BIGINT NOT NULL,
                PRIMARY KEY (order_id)
            );",
        )
        .await
        .context("create sales_pushdown table")?;

    client
        .batch_execute(
            "INSERT INTO sales_pushdown (
               order_id, customer_id, merchant_id, region_id, event_day, status, amount_cents
             )
             SELECT
               i,
               100000 + (i % 500000),
               1 + (i % 20000),
               1 + (i % 20),
               1 + ((i - 1) / 86400),
               CASE
                 WHEN i % 10 < 6 THEN 'paid'
                 WHEN i % 10 < 8 THEN 'shipped'
                 WHEN i % 10 = 8 THEN 'pending'
                 ELSE 'cancelled'
               END,
               100 + ((i * 37) % 20000)
             FROM generate_series(1, 20000) AS g(i);",
        )
        .await
        .context("seed sales_pushdown rows")?;

    client
        .batch_execute(
            "CREATE INDEX sales_pushdown_status_day_merchant
             ON sales_pushdown (status, event_day, merchant_id);",
        )
        .await
        .context("create range secondary index for grouped aggregate pushdown test")?;

    let rows = client
        .query(
            "SELECT merchant_id, event_day, COUNT(*) AS orders, SUM(amount_cents) AS gross_cents
             FROM sales_pushdown
             WHERE event_day = 1
               AND status IN ('paid', 'shipped')
             GROUP BY merchant_id, event_day
             HAVING COUNT(*) >= 1
             ORDER BY gross_cents DESC
             LIMIT 200;",
            &[],
        )
        .await
        .context("run grouped aggregate top-k query")?;

    assert_eq!(rows.len(), 200, "expected top-k result cardinality");
    let mut prev_gross = i64::MAX;
    for row in &rows {
        let gross: i64 = row
            .try_get("gross_cents")
            .context("read gross_cents from grouped aggregate output")?;
        assert!(
            gross <= prev_gross,
            "expected descending gross_cents order; prev={prev_gross} current={gross}"
        );
        prev_gross = gross;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase13_projected_scan_omits_non_nullable_columns_without_null_violations() -> Result<()> {
    let harness = TestHarness::start().await?;
    let (client, _guard) = harness.connect_pg().await?;

    client
        .batch_execute(
            "CREATE TABLE sales_projection_guard (
                order_id BIGINT NOT NULL,
                customer_id BIGINT NOT NULL,
                merchant_id BIGINT NOT NULL,
                region_id BIGINT NOT NULL,
                event_day BIGINT NOT NULL,
                status TEXT NOT NULL,
                amount_cents BIGINT NOT NULL,
                PRIMARY KEY (order_id)
            );",
        )
        .await
        .context("create sales_projection_guard table")?;

    client
        .batch_execute(
            "INSERT INTO sales_projection_guard (
               order_id, customer_id, merchant_id, region_id, event_day, status, amount_cents
             )
             SELECT
               i,
               100000 + (i % 500000),
               1 + (i % 20),
               1 + ((i * 7) % 20),
               1 + ((i - 1) % 120),
               CASE
                 WHEN i % 10 < 6 THEN 'paid'
                 WHEN i % 10 < 8 THEN 'shipped'
                 WHEN i % 10 = 8 THEN 'pending'
                 ELSE 'cancelled'
               END,
               100 + ((i * 37) % 20000)
             FROM generate_series(1, 30000) AS g(i);",
        )
        .await
        .context("seed sales_projection_guard rows")?;

    client
        .batch_execute(
            "CREATE INDEX sales_projection_guard_status_day_merchant
             ON sales_projection_guard (status, event_day, merchant_id)
             INCLUDE (amount_cents);",
        )
        .await
        .context("create covering secondary index for projected scan regression")?;

    let metrics_before = harness
        .metrics_body()
        .context("fetch metrics body before projected-scan query")?;
    let index_before = metric_value(&metrics_before, "optimizer_plan_index_scan").unwrap_or(0);
    let index_only_before = metric_value(&metrics_before, "optimizer_plan_index_only").unwrap_or(0);

    let rows = client
        .query(
            "SELECT merchant_id, event_day, COUNT(*) AS orders, SUM(amount_cents) AS gross_cents
             FROM sales_projection_guard
             WHERE status = 'paid'
               AND event_day IN (20, 21, 22, 23)
             GROUP BY merchant_id, event_day
             HAVING COUNT(*) >= 3
             ORDER BY gross_cents DESC
             LIMIT 200;",
            &[],
        )
        .await
        .context("run projected aggregate scan over non-nullable table")?;
    assert!(
        !rows.is_empty(),
        "expected projected aggregate query to return rows"
    );
    for row in &rows {
        let _merchant_id: i64 = row.try_get("merchant_id")?;
        let _event_day: i64 = row.try_get("event_day")?;
        let _orders: i64 = row.try_get("orders")?;
        let _gross_cents: i64 = row.try_get("gross_cents")?;
    }

    let metrics_after = harness
        .metrics_body()
        .context("fetch metrics body after projected-scan query")?;
    let index_after = metric_value(&metrics_after, "optimizer_plan_index_scan").unwrap_or(0);
    let index_only_after = metric_value(&metrics_after, "optimizer_plan_index_only").unwrap_or(0);
    assert!(
        index_after.saturating_sub(index_before)
            + index_only_after.saturating_sub(index_only_before)
            >= 1,
        "expected index-family path for projected scan regression; metrics={metrics_after}"
    );

    Ok(())
}

fn free_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("bind ephemeral port")?;
    Ok(listener.local_addr()?.port())
}

fn wait_for_ready(addr: SocketAddr, timeout: Duration) -> Result<()> {
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

fn metric_value(body: &str, key: &str) -> Option<u64> {
    let prefix = format!("{key}=");
    body.lines()
        .find_map(|line| line.strip_prefix(prefix.as_str()))
        .and_then(|raw| raw.trim().parse::<u64>().ok())
}

fn health_body(addr: SocketAddr, path: &str) -> Result<String> {
    let mut stream = TcpStream::connect(addr).context("connect health server")?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .context("write health request")?;

    let mut resp = String::new();
    stream
        .read_to_string(&mut resp)
        .context("read health response")?;
    let split = resp
        .split_once("\r\n\r\n")
        .ok_or_else(|| anyhow::anyhow!("invalid health response: {resp}"))?;
    Ok(split.1.to_string())
}

fn assert_sqlstate(err: &tokio_postgres::Error, expected: &str) {
    let actual = err.code().map(|code| code.code());
    assert_eq!(
        actual,
        Some(expected),
        "unexpected SQLSTATE for error: {err}"
    );
}

fn assert_error_contains(err: &tokio_postgres::Error, needle: &str) {
    let message = if let Some(db) = err.as_db_error() {
        let detail = db.detail().unwrap_or_default();
        let hint = db.hint().unwrap_or_default();
        format!("{} {} {}", db.message(), detail, hint)
    } else {
        err.to_string()
    }
    .to_ascii_lowercase();
    assert!(
        message.contains(&needle.to_ascii_lowercase()),
        "expected error message to contain '{needle}', got: {err}"
    );
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

    Err(last_err).context("bootstrap orders table for smoke tests")
}

struct TestHarness {
    pg_host: String,
    pg_port: u16,
    health_addr: SocketAddr,
    grpc_addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    runtime_task: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
    _temp_dir: TempDir,
}

#[derive(Clone, Copy)]
struct DmlHarnessConfig {
    prewrite_delay: Duration,
    statement_timeout: Duration,
    max_inflight_statements: usize,
    max_scan_rows: usize,
    max_txn_staged_rows: usize,
    max_shards: usize,
    initial_ranges: usize,
}

impl Default for DmlHarnessConfig {
    fn default() -> Self {
        Self {
            prewrite_delay: Duration::ZERO,
            statement_timeout: Duration::ZERO,
            max_inflight_statements: 1024,
            max_scan_rows: 100_000,
            max_txn_staged_rows: 100_000,
            max_shards: 1,
            initial_ranges: 1,
        }
    }
}

impl TestHarness {
    async fn start() -> Result<Self> {
        Self::start_with_dml_config(DmlHarnessConfig::default()).await
    }

    async fn start_with_dml_prewrite_delay(dml_prewrite_delay: Duration) -> Result<Self> {
        Self::start_with_dml_config(DmlHarnessConfig {
            prewrite_delay: dml_prewrite_delay,
            ..DmlHarnessConfig::default()
        })
        .await
    }

    async fn start_with_dml_config(dml: DmlHarnessConfig) -> Result<Self> {
        let temp_dir = TempDir::new().context("create temp dir")?;

        let pg_port = free_port()?;
        let health_port = free_port()?;
        let redis_port = free_port()?;
        let grpc_port = free_port()?;

        let pg_host = "127.0.0.1".to_string();
        let health_addr: SocketAddr = format!("127.0.0.1:{health_port}").parse()?;
        let redis_addr: SocketAddr = format!("127.0.0.1:{redis_port}").parse()?;
        let grpc_addr: SocketAddr = format!("127.0.0.1:{grpc_port}").parse()?;

        let config = HoloFusionConfig {
            pg_host: pg_host.clone(),
            pg_port,
            health_addr,
            enable_ballista_sql: false,
            dml_prewrite_delay: dml.prewrite_delay,
            dml_statement_timeout: dml.statement_timeout,
            dml_max_inflight_statements: dml.max_inflight_statements,
            dml_max_inflight_reads: dml.max_inflight_statements,
            dml_max_inflight_writes: dml.max_inflight_statements,
            dml_max_inflight_txns: ((dml.max_inflight_statements) / 2).max(1),
            dml_max_inflight_background: ((dml.max_inflight_statements) / 4).max(1),
            dml_admission_queue_limit: 4096,
            dml_admission_wait_timeout: Duration::ZERO,
            dml_max_scan_rows: dml.max_scan_rows,
            dml_max_txn_staged_rows: dml.max_txn_staged_rows,
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
                data_dir: temp_dir.path().join("node-1"),
                ready_timeout: Duration::from_secs(30),
                max_shards: dml.max_shards.max(1),
                initial_ranges: dml.initial_ranges.max(1).min(dml.max_shards.max(1)),
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
            .context("wait for runtime readiness")?;
        ensure_orders_table(pg_host.as_str(), pg_port)
            .await
            .context("bootstrap orders table for smoke tests")?;

        Ok(Self {
            pg_host,
            pg_port,
            health_addr,
            grpc_addr,
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
        let (client, connection) = tokio_postgres::connect(&conn, NoTls)
            .await
            .context("connect to pg wire server")?;
        let guard = tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok((client, guard))
    }

    fn holostore_client(&self) -> HoloStoreClient {
        HoloStoreClient::new(self.grpc_addr)
    }

    fn metrics_body(&self) -> Result<String> {
        health_body(self.health_addr, "/metrics")
    }
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        if let Some(task) = self.runtime_task.take() {
            task.abort();
        }
    }
}
