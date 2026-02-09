//! Process-level crash/restart tests for WAL durability and replay.
//!
//! Test flow:
//! 1) Start a single-node holo-store process with a fresh data directory.
//! 2) Write a small key/value set over the Redis protocol.
//! 3) Wait until all keys are visible to reads (commit + apply completed).
//! 4) Kill the node process (simulated crash) without graceful shutdown.
//! 5) Restart the node using the same WAL/data directory.
//! 6) Read back the keys and verify every value is recovered.
//!
//! Failure model covered:
//! - Abrupt process termination while relying on the WAL/commit log for durability.
//! - Recovery via WAL replay on restart.
//!
//! Verification:
//! - The test asserts that every written key is present after restart.
//! - Any missing value indicates a WAL durability/replay regression.
//! - Timeouts or connection failures indicate a startup or RPC/Redis readiness issue.

mod common;

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use common::{
    cleanup_dir, pick_free_port, read_keys, spawn_node, test_dir, wait_for_port,
    wait_for_redis_ready, write_keys, IO_TIMEOUT,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(60);

fn check_timeout(start: Instant, step: &str, node: &mut common::NodeProcess) {
    if start.elapsed() <= TEST_TIMEOUT {
        return;
    }
    let stdout = node.read_stdout();
    let stderr = node.read_stderr();
    panic!("wal_crash timed out during {step}\nnode stdout:\n{stdout}\nnode stderr:\n{stderr}");
}

/// Verify that replay after a crash preserves committed writes across shards.
#[test]
fn wal_replay_after_crash() {
    let test_start = Instant::now();
    let data_dir = test_dir("wal-crash");
    cleanup_dir(&data_dir);
    let redis_port = match pick_free_port() {
        Ok(port) => port,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping wal_replay_after_crash: cannot bind local port ({err})");
            return;
        }
        Err(err) => panic!("bind ephemeral port failed: {err}"),
    };
    let grpc_port = match pick_free_port() {
        Ok(port) => port,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping wal_replay_after_crash: cannot bind local port ({err})");
            return;
        }
        Err(err) => panic!("bind ephemeral port failed: {err}"),
    };
    let redis_addr: SocketAddr = format!("127.0.0.1:{redis_port}").parse().unwrap();
    let grpc_addr: SocketAddr = format!("127.0.0.1:{grpc_port}").parse().unwrap();

    let mut node = spawn_node(&data_dir, redis_addr, grpc_addr);
    check_timeout(test_start, "spawn", &mut node);
    node.assert_running("after spawn");
    wait_for_port(grpc_addr, IO_TIMEOUT);
    wait_for_port(redis_addr, IO_TIMEOUT);
    if !wait_for_redis_ready(redis_addr, IO_TIMEOUT) {
        let stdout = node.read_stdout();
        let stderr = node.read_stderr();
        panic!(
            "redis port {redis_addr} did not respond to PING in time\nnode stdout:\n{stdout}\nnode stderr:\n{stderr}"
        );
    }

    let mut keys = Vec::new();
    for i in 0..10 {
        keys.push((format!("k{i}"), format!("v{i}")));
    }
    check_timeout(test_start, "before write", &mut node);
    if let Err(err) = write_keys(redis_addr, &keys) {
        let stderr = node.read_stderr();
        panic!("{err}\nnode stderr:\n{stderr}");
    }
    // Ensure writes are applied before simulating a crash.
    let key_names: Vec<String> = keys.iter().map(|(k, _)| k.clone()).collect();
    let verify_deadline = Instant::now() + IO_TIMEOUT;
    loop {
        let values = read_keys(redis_addr, &key_names);
        let mut all_ok = true;
        for (idx, value) in values.into_iter().enumerate() {
            let expected = format!("v{idx}");
            if value.as_deref() != Some(expected.as_str()) {
                all_ok = false;
                break;
            }
        }
        if all_ok {
            break;
        }
        if Instant::now() >= verify_deadline {
            let stderr = node.read_stderr();
            panic!("writes not visible before crash\nnode stderr:\n{stderr}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    node.child.kill().ok();
    let _ = node.child.wait();
    check_timeout(test_start, "after first shutdown", &mut node);

    let mut node = spawn_node(&data_dir, redis_addr, grpc_addr);
    check_timeout(test_start, "restart spawn", &mut node);
    node.assert_running("after restart");
    wait_for_port(grpc_addr, IO_TIMEOUT);
    wait_for_port(redis_addr, IO_TIMEOUT);
    if !wait_for_redis_ready(redis_addr, IO_TIMEOUT) {
        let stdout = node.read_stdout();
        let stderr = node.read_stderr();
        panic!(
            "redis port {redis_addr} did not respond to PING in time\nnode stdout:\n{stdout}\nnode stderr:\n{stderr}"
        );
    }

    check_timeout(test_start, "before read", &mut node);
    let values = read_keys(redis_addr, &key_names);
    for (idx, value) in values.into_iter().enumerate() {
        let expected = format!("v{idx}");
        assert_eq!(value.as_deref(), Some(expected.as_str()));
    }

    node.child.kill().ok();
    let _ = node.child.wait();
    cleanup_dir(&data_dir);
}
