//! Fault-injected crash/restart durability test for commit ACK semantics.
//!
//! Purpose:
//! - Prove that `async_commit` can ACK writes that are not fsync-durable.
//! - Prove that `sync_commit` preserves ACKed writes across a simulated
//!   power-loss restart.
//!
//! Design:
//! - The test enables `HOLO_WAL_FAULT_TRUNCATE_UNSYNCED_ON_OPEN=true`.
//! - On restart, the WAL drops any bytes beyond the last fsync-confirmed offset.
//! - This models "process survived ACK but machine lost unsynced page cache".
//! - We also delete the Fjall storage directory between runs so replay comes
//!   from WAL only (avoids false positives from storage cache persistence).

mod common;

use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant};

use common::{
    cleanup_dir, pick_free_port, read_keys, spawn_node_custom_env, test_dir, wait_for_port,
    wait_for_redis_ready, write_keys, IO_TIMEOUT,
};

/// Hard timeout for each mode sub-test.
const TEST_TIMEOUT: Duration = Duration::from_secs(90);

/// Panic if a mode sub-test exceeds the allowed wall-clock budget.
///
/// Inputs:
/// - `start`: mode sub-test start time.
/// - `step`: current step label for diagnostics.
/// - `node`: active node process for stdout/stderr capture.
///
/// Output:
/// - Returns normally when within timeout budget.
/// - Panics with captured logs when the sub-test exceeds `TEST_TIMEOUT`.
fn check_timeout(start: Instant, step: &str, node: &mut common::NodeProcess) {
    if start.elapsed() <= TEST_TIMEOUT {
        return;
    }
    let stdout = node.read_stdout();
    let stderr = node.read_stderr();
    panic!(
        "wal_durability_fault timed out during {step}\nnode stdout:\n{stdout}\nnode stderr:\n{stderr}"
    );
}

/// Remove Fjall storage between crash and restart to force WAL replay.
///
/// Inputs:
/// - `data_dir`: test data directory that contains `storage/` and `wal/`.
///
/// Output:
/// - Best-effort deletion of `storage/`; WAL directory is left intact.
fn reset_storage_for_replay(data_dir: &Path) {
    let storage_dir = data_dir.join("storage");
    let _ = std::fs::remove_dir_all(&storage_dir);
}

/// Execute one crash/restart scenario under the requested commit durability mode.
///
/// Inputs:
/// - `commit_mode`: `async_commit` or `sync_commit`.
/// - `expect_survive`: whether ACKed values should survive restart.
///
/// Output:
/// - Returns normally when observed behavior matches expected durability mode.
/// - Panics with diagnostics when restart outcome violates expectations.
fn run_mode_case(commit_mode: &str, expect_survive: bool) {
    let mode_start = Instant::now();
    let data_dir = test_dir(&format!("wal-durability-fault-{commit_mode}"));
    cleanup_dir(&data_dir);

    let redis_port = pick_free_port().expect("pick redis port");
    let grpc_port = pick_free_port().expect("pick grpc port");
    let redis_addr: SocketAddr = format!("127.0.0.1:{redis_port}")
        .parse()
        .expect("parse redis addr");
    let grpc_addr: SocketAddr = format!("127.0.0.1:{grpc_port}")
        .parse()
        .expect("parse grpc addr");

    // Keep WAL fsync thresholds effectively disabled for background sync so the
    // ACK policy difference (`async_commit` vs `sync_commit`) is the key factor.
    let envs = [
        ("HOLO_WAL_ENGINE", "file"),
        ("HOLO_COMMIT_DURABILITY_MODE", commit_mode),
        ("HOLO_WAL_PERSIST_MODE", "sync_data"),
        ("HOLO_WAL_PERSIST_EVERY", "1000000000"),
        ("HOLO_WAL_PERSIST_INTERVAL_US", "3600000000"),
        ("HOLO_WAL_PERSIST_ASYNC", "false"),
        ("HOLO_WAL_FAULT_TRUNCATE_UNSYNCED_ON_OPEN", "true"),
    ];

    let mut node = spawn_node_custom_env(
        1,
        &data_dir,
        redis_addr,
        grpc_addr,
        true,
        None,
        format!("1@{grpc_addr}"),
        "local",
        1,
        &envs,
    );
    check_timeout(mode_start, "spawn", &mut node);
    node.assert_running("after spawn");
    wait_for_port(grpc_addr, IO_TIMEOUT);
    wait_for_port(redis_addr, IO_TIMEOUT);
    assert!(
        wait_for_redis_ready(redis_addr, IO_TIMEOUT),
        "redis did not become ready in mode {commit_mode}"
    );

    let writes = (0..8)
        .map(|i| (format!("k{i}"), format!("v{i}")))
        .collect::<Vec<_>>();
    let keys = writes.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>();
    write_keys(redis_addr, &writes).expect("write keys");

    // Ensure the writes were ACKed and visible before crash.
    let visible_deadline = Instant::now() + IO_TIMEOUT;
    loop {
        let values = read_keys(redis_addr, &keys);
        let all_visible = values
            .iter()
            .enumerate()
            .all(|(idx, v)| v.as_deref() == Some(format!("v{idx}").as_str()));
        if all_visible {
            break;
        }
        if Instant::now() >= visible_deadline {
            let stderr = node.read_stderr();
            panic!("writes not visible before crash in mode {commit_mode}\n{stderr}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    node.child.kill().ok();
    let _ = node.child.wait();
    check_timeout(mode_start, "post-crash", &mut node);

    // Remove storage state so recovery must depend on durable WAL bytes.
    reset_storage_for_replay(&data_dir);

    let mut node = spawn_node_custom_env(
        1,
        &data_dir,
        redis_addr,
        grpc_addr,
        true,
        None,
        format!("1@{grpc_addr}"),
        "local",
        1,
        &envs,
    );
    check_timeout(mode_start, "restart spawn", &mut node);
    node.assert_running("after restart");
    wait_for_port(grpc_addr, IO_TIMEOUT);
    wait_for_port(redis_addr, IO_TIMEOUT);
    assert!(
        wait_for_redis_ready(redis_addr, IO_TIMEOUT),
        "redis did not become ready after restart in mode {commit_mode}"
    );

    let read_deadline = Instant::now() + IO_TIMEOUT;
    loop {
        let values = read_keys(redis_addr, &keys);
        let recovered = values
            .iter()
            .enumerate()
            .filter(|(idx, v)| v.as_deref() == Some(format!("v{idx}").as_str()))
            .count();

        if expect_survive {
            if recovered == keys.len() {
                break;
            }
            if Instant::now() >= read_deadline {
                let stderr = node.read_stderr();
                panic!(
                    "expected all keys after restart in mode {commit_mode}, recovered {recovered}/{}\n{stderr}",
                    keys.len()
                );
            }
        } else {
            // For async commit under power-loss simulation, at least one ACKed
            // write must be lost because no per-ACK fsync barrier was required.
            if recovered < keys.len() {
                break;
            }
            if Instant::now() >= read_deadline {
                let stderr = node.read_stderr();
                panic!(
                    "expected at least one lost key after restart in mode {commit_mode}, recovered {recovered}/{}\n{stderr}",
                    keys.len()
                );
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    node.child.kill().ok();
    let _ = node.child.wait();
    cleanup_dir(&data_dir);
}

/// Validate durability behavior under simulated power-loss restart.
///
/// Expected results:
/// - `async_commit`: at least one ACKed write is lost.
/// - `sync_commit`: all ACKed writes are recovered.
#[test]
fn crash_fault_injection_distinguishes_async_and_sync_commit() {
    run_mode_case("async_commit", false);
    run_mode_case("sync_commit", true);
}
