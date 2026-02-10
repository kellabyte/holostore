//! Meta-plane fault-injection integration tests.
//!
//! These scenarios exercise crash/restart and partition-heal style behavior for
//! metadata operations across multiple meta ranges.

mod common;

use std::net::SocketAddr;
use std::path::Path;
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use common::{
    cleanup_dir, holoctl_bin, pick_free_port, spawn_node_custom_env, test_dir, wait_for_port,
    wait_for_redis_ready, IO_TIMEOUT,
};
use serde_json::Value;

const TEST_TIMEOUT: Duration = Duration::from_secs(180);

fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn run_holoctl(target: SocketAddr, args: &[&str]) -> String {
    let mut cmd = Command::new(holoctl_bin());
    cmd.arg("--target").arg(target.to_string());
    for arg in args {
        cmd.arg(arg);
    }
    let out = cmd.output().expect("run holoctl");
    if !out.status.success() {
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        panic!(
            "holoctl failed for args {:?}\nstdout:\n{}\nstderr:\n{}",
            args, stdout, stderr
        );
    }
    String::from_utf8_lossy(&out.stdout).to_string()
}

fn run_holoctl_result(target: SocketAddr, args: &[&str]) -> std::process::Output {
    let mut cmd = Command::new(holoctl_bin());
    cmd.arg("--target").arg(target.to_string());
    for arg in args {
        cmd.arg(arg);
    }
    cmd.output().expect("run holoctl")
}

fn read_state(target: SocketAddr) -> Value {
    let text = run_holoctl(target, &["state"]);
    serde_json::from_str(&text).expect("parse cluster state json")
}

fn wait_for_condition<F>(timeout: Duration, mut pred: F)
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        if pred() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("condition not met in {:?}", timeout);
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn spawn_node(
    node_id: u64,
    data_dir: &Path,
    redis_addr: SocketAddr,
    grpc_addr: SocketAddr,
    bootstrap: bool,
    join: Option<SocketAddr>,
    members: &str,
    envs: &[(&str, &str)],
) -> common::NodeProcess {
    spawn_node_custom_env(
        node_id,
        &data_dir.to_path_buf(),
        redis_addr,
        grpc_addr,
        bootstrap,
        join,
        members.to_string(),
        "local",
        4,
        envs,
    )
}

#[test]
fn meta_split_recovers_after_leader_crash_during_command() {
    let _lock = test_lock();
    let base_dir = test_dir("meta-split-crash");
    cleanup_dir(&base_dir);

    let redis_addrs: Vec<SocketAddr> = (0..3)
        .map(|_| {
            format!("127.0.0.1:{}", pick_free_port().unwrap())
                .parse()
                .unwrap()
        })
        .collect();
    let grpc_addrs: Vec<SocketAddr> = (0..3)
        .map(|_| {
            format!("127.0.0.1:{}", pick_free_port().unwrap())
                .parse()
                .unwrap()
        })
        .collect();
    let members = format!(
        "1@{},2@{},3@{}",
        grpc_addrs[0], grpc_addrs[1], grpc_addrs[2]
    );

    let node1_env = [
        ("HOLO_META_GROUPS", "3"),
        ("HOLO_META_SPLIT_ENABLED", "false"),
        ("HOLO_META_BALANCE_ENABLED", "false"),
        ("HOLO_REBALANCE_ENABLED", "false"),
        ("HOLO_RANGE_MGR_INTERVAL_MS", "10000"),
        ("HOLO_REBALANCE_INTERVAL_MS", "10000"),
    ];
    let delayed_env = [
        ("HOLO_META_GROUPS", "3"),
        ("HOLO_META_SPLIT_ENABLED", "false"),
        ("HOLO_META_BALANCE_ENABLED", "false"),
        ("HOLO_REBALANCE_ENABLED", "false"),
        ("HOLO_RANGE_MGR_INTERVAL_MS", "10000"),
        ("HOLO_REBALANCE_INTERVAL_MS", "10000"),
        ("HOLO_RPC_HANDLER_DELAY_MS", "250"),
    ];

    let mut node1 = spawn_node(
        1,
        &base_dir.join("node1"),
        redis_addrs[0],
        grpc_addrs[0],
        true,
        None,
        &members,
        &node1_env,
    );
    let mut node2 = spawn_node(
        2,
        &base_dir.join("node2"),
        redis_addrs[1],
        grpc_addrs[1],
        false,
        Some(grpc_addrs[0]),
        &members,
        &delayed_env,
    );
    let mut node3 = spawn_node(
        3,
        &base_dir.join("node3"),
        redis_addrs[2],
        grpc_addrs[2],
        false,
        Some(grpc_addrs[0]),
        &members,
        &delayed_env,
    );
    for (grpc, redis) in grpc_addrs.iter().zip(redis_addrs.iter()) {
        wait_for_port(*grpc, IO_TIMEOUT);
        wait_for_port(*redis, IO_TIMEOUT);
        assert!(wait_for_redis_ready(*redis, IO_TIMEOUT));
    }

    // Fire split request and crash leader while replicas are slow.
    let handle = thread::spawn({
        let target = grpc_addrs[0];
        move || {
            run_holoctl_result(
                target,
                &[
                    "split-meta",
                    "--split-hash",
                    "9223372036854775808",
                    "--target-meta-index",
                    "0",
                ],
            )
        }
    });
    thread::sleep(Duration::from_millis(50));
    node1.child.kill().ok();
    let _ = node1.child.wait();
    let _ = handle.join();

    // Restart former leader as a joiner and ensure split can be committed.
    node1 = spawn_node(
        1,
        &base_dir.join("node1"),
        redis_addrs[0],
        grpc_addrs[0],
        false,
        Some(grpc_addrs[1]),
        &members,
        &node1_env,
    );
    wait_for_port(grpc_addrs[0], TEST_TIMEOUT);
    wait_for_port(redis_addrs[0], TEST_TIMEOUT);
    assert!(wait_for_redis_ready(redis_addrs[0], TEST_TIMEOUT));

    // Retry split on surviving leader if needed.
    let _ = run_holoctl_result(
        grpc_addrs[1],
        &[
            "split-meta",
            "--split-hash",
            "4611686018427387904",
            "--target-meta-index",
            "0",
        ],
    );

    wait_for_condition(TEST_TIMEOUT, || {
        let s = read_state(grpc_addrs[1]);
        s["meta_ranges"].as_array().map(|v| v.len()).unwrap_or(0) >= 2
    });

    let _ = (&mut node1, &mut node2, &mut node3);
}

#[test]
fn meta_rebalance_recovers_from_restart_during_lease_transfer() {
    const LEASE_TRANSFER_TIMEOUT: Duration = Duration::from_secs(300);

    let _lock = test_lock();
    let base_dir = test_dir("meta-lease-restart");
    cleanup_dir(&base_dir);

    let redis_addrs: Vec<SocketAddr> = (0..4)
        .map(|_| {
            format!("127.0.0.1:{}", pick_free_port().unwrap())
                .parse()
                .unwrap()
        })
        .collect();
    let grpc_addrs: Vec<SocketAddr> = (0..4)
        .map(|_| {
            format!("127.0.0.1:{}", pick_free_port().unwrap())
                .parse()
                .unwrap()
        })
        .collect();
    let members_bootstrap = format!(
        "1@{},2@{},3@{}",
        grpc_addrs[0], grpc_addrs[1], grpc_addrs[2]
    );
    let members_with_4 = format!(
        "1@{},2@{},3@{},4@{}",
        grpc_addrs[0], grpc_addrs[1], grpc_addrs[2], grpc_addrs[3]
    );
    let envs = [
        ("HOLO_META_GROUPS", "2"),
        ("HOLO_META_SPLIT_ENABLED", "false"),
        ("HOLO_META_BALANCE_ENABLED", "true"),
        ("HOLO_META_MGR_INTERVAL_MS", "100"),
        ("HOLO_REBALANCE_ENABLED", "false"),
        ("HOLO_RANGE_MGR_INTERVAL_MS", "10000"),
        ("HOLO_REBALANCE_INTERVAL_MS", "10000"),
    ];

    let mut node1 = spawn_node(
        1,
        &base_dir.join("node1"),
        redis_addrs[0],
        grpc_addrs[0],
        true,
        None,
        &members_bootstrap,
        &envs,
    );
    let mut node2 = spawn_node(
        2,
        &base_dir.join("node2"),
        redis_addrs[1],
        grpc_addrs[1],
        false,
        Some(grpc_addrs[0]),
        &members_bootstrap,
        &envs,
    );
    let mut node3 = spawn_node(
        3,
        &base_dir.join("node3"),
        redis_addrs[2],
        grpc_addrs[2],
        false,
        Some(grpc_addrs[0]),
        &members_bootstrap,
        &envs,
    );
    for (grpc, redis) in grpc_addrs[..3].iter().zip(redis_addrs[..3].iter()) {
        wait_for_port(*grpc, IO_TIMEOUT);
        wait_for_port(*redis, IO_TIMEOUT);
        assert!(wait_for_redis_ready(*redis, IO_TIMEOUT));
    }

    // Add and start node4 so it can join staged meta move.
    run_holoctl(
        grpc_addrs[0],
        &[
            "add-node",
            "--node-id",
            "4",
            "--grpc-addr",
            &grpc_addrs[3].to_string(),
            "--redis-addr",
            &redis_addrs[3].to_string(),
        ],
    );
    let mut node4 = spawn_node(
        4,
        &base_dir.join("node4"),
        redis_addrs[3],
        grpc_addrs[3],
        false,
        Some(grpc_addrs[0]),
        &members_with_4,
        &envs,
    );
    wait_for_port(grpc_addrs[3], LEASE_TRANSFER_TIMEOUT);
    wait_for_port(redis_addrs[3], LEASE_TRANSFER_TIMEOUT);
    assert!(wait_for_redis_ready(redis_addrs[3], LEASE_TRANSFER_TIMEOUT));

    // Move meta range 1 from node3 -> node4 and transfer lease to node2.
    run_holoctl(
        grpc_addrs[0],
        &[
            "meta-rebalance",
            "--meta-range-id",
            "1",
            "--replica",
            "1",
            "--replica",
            "2",
            "--replica",
            "4",
            "--leaseholder",
            "2",
        ],
    );

    wait_for_condition(LEASE_TRANSFER_TIMEOUT, || {
        let s = read_state(grpc_addrs[0]);
        if s["meta_rebalances"]["1"]["phase"]
            .as_str()
            .map(|p| p == "JointConfig" || p == "LeaseTransferred")
            .unwrap_or(false)
        {
            return true;
        }
        let done = s["meta_rebalances"]
            .as_object()
            .map(|m| m.is_empty())
            .unwrap_or(false);
        if !done {
            return false;
        }
        let Some(range) = s["meta_ranges"]
            .as_array()
            .and_then(|arr| arr.iter().find(|r| r["meta_range_id"].as_u64() == Some(1)))
        else {
            return false;
        };
        let mut replicas = range["replicas"]
            .as_array()
            .map(|arr| arr.iter().filter_map(|v| v.as_u64()).collect::<Vec<_>>())
            .unwrap_or_default();
        replicas.sort_unstable();
        replicas == vec![1, 2, 4] && range["leaseholder"].as_u64() == Some(2)
    });

    // Restart target leaseholder while transfer/finalize is in progress.
    node2.child.kill().ok();
    let _ = node2.child.wait();
    node2 = spawn_node(
        2,
        &base_dir.join("node2"),
        redis_addrs[1],
        grpc_addrs[1],
        false,
        Some(grpc_addrs[0]),
        &members_with_4,
        &envs,
    );
    wait_for_port(grpc_addrs[1], LEASE_TRANSFER_TIMEOUT);
    wait_for_port(redis_addrs[1], LEASE_TRANSFER_TIMEOUT);
    assert!(wait_for_redis_ready(redis_addrs[1], LEASE_TRANSFER_TIMEOUT));

    wait_for_condition(LEASE_TRANSFER_TIMEOUT, || {
        let s = read_state(grpc_addrs[0]);
        let done = s["meta_rebalances"]
            .as_object()
            .map(|m| m.is_empty())
            .unwrap_or(false);
        if !done {
            return false;
        }
        let Some(range) = s["meta_ranges"]
            .as_array()
            .and_then(|arr| arr.iter().find(|r| r["meta_range_id"].as_u64() == Some(1)))
        else {
            return false;
        };
        let mut replicas = range["replicas"]
            .as_array()
            .map(|arr| arr.iter().filter_map(|v| v.as_u64()).collect::<Vec<_>>())
            .unwrap_or_default();
        replicas.sort_unstable();
        replicas == vec![1, 2, 4] && range["leaseholder"].as_u64() == Some(2)
    });

    let _ = (&mut node1, &mut node2, &mut node3, &mut node4);
}

#[test]
fn meta_plane_partition_heal_across_meta_groups() {
    let _lock = test_lock();
    let base_dir = test_dir("meta-partition-heal");
    cleanup_dir(&base_dir);

    let redis_addrs: Vec<SocketAddr> = (0..3)
        .map(|_| {
            format!("127.0.0.1:{}", pick_free_port().unwrap())
                .parse()
                .unwrap()
        })
        .collect();
    let grpc_addrs: Vec<SocketAddr> = (0..3)
        .map(|_| {
            format!("127.0.0.1:{}", pick_free_port().unwrap())
                .parse()
                .unwrap()
        })
        .collect();
    let members = format!(
        "1@{},2@{},3@{}",
        grpc_addrs[0], grpc_addrs[1], grpc_addrs[2]
    );
    let envs = [
        ("HOLO_META_GROUPS", "3"),
        ("HOLO_META_SPLIT_ENABLED", "false"),
        ("HOLO_META_BALANCE_ENABLED", "true"),
        ("HOLO_META_MGR_INTERVAL_MS", "100"),
        ("HOLO_REBALANCE_ENABLED", "false"),
        ("HOLO_RANGE_MGR_INTERVAL_MS", "10000"),
        ("HOLO_REBALANCE_INTERVAL_MS", "10000"),
    ];

    let mut node1 = spawn_node(
        1,
        &base_dir.join("node1"),
        redis_addrs[0],
        grpc_addrs[0],
        true,
        None,
        &members,
        &envs,
    );
    let mut node2 = spawn_node(
        2,
        &base_dir.join("node2"),
        redis_addrs[1],
        grpc_addrs[1],
        false,
        Some(grpc_addrs[0]),
        &members,
        &envs,
    );
    let mut node3 = spawn_node(
        3,
        &base_dir.join("node3"),
        redis_addrs[2],
        grpc_addrs[2],
        false,
        Some(grpc_addrs[0]),
        &members,
        &envs,
    );
    for (grpc, redis) in grpc_addrs.iter().zip(redis_addrs.iter()) {
        wait_for_port(*grpc, IO_TIMEOUT);
        wait_for_port(*redis, IO_TIMEOUT);
        assert!(wait_for_redis_ready(*redis, IO_TIMEOUT));
    }

    // Build multiple meta groups first.
    run_holoctl(
        grpc_addrs[0],
        &[
            "split-meta",
            "--split-hash",
            "9223372036854775808",
            "--target-meta-index",
            "0",
        ],
    );
    run_holoctl(
        grpc_addrs[0],
        &[
            "split-meta",
            "--split-hash",
            "4611686018427387904",
            "--target-meta-index",
            "0",
        ],
    );

    // Partition node2 (hard crash), continue control-plane ops on remaining quorum.
    node2.child.kill().ok();
    let _ = node2.child.wait();
    run_holoctl(
        grpc_addrs[0],
        &[
            "add-node",
            "--node-id",
            "4",
            "--grpc-addr",
            "127.0.0.1:15054",
            "--redis-addr",
            "127.0.0.1:16382",
        ],
    );
    run_holoctl(grpc_addrs[0], &["split", "--split-key", "key:50000"]);

    // Heal partition by restarting node2 and verify converged metadata.
    node2 = spawn_node(
        2,
        &base_dir.join("node2"),
        redis_addrs[1],
        grpc_addrs[1],
        false,
        Some(grpc_addrs[0]),
        &members,
        &envs,
    );
    wait_for_port(grpc_addrs[1], TEST_TIMEOUT);
    wait_for_port(redis_addrs[1], TEST_TIMEOUT);
    assert!(wait_for_redis_ready(redis_addrs[1], TEST_TIMEOUT));

    wait_for_condition(TEST_TIMEOUT, || {
        let leader = read_state(grpc_addrs[0]);
        let healed = read_state(grpc_addrs[1]);
        leader["epoch"] == healed["epoch"]
            && healed["members"]["4"]["state"].as_str() == Some("Active")
            && healed["meta_ranges"]
                .as_array()
                .map(|v| v.len())
                .unwrap_or(0)
                >= 3
    });

    let _ = (&mut node1, &mut node2, &mut node3);
}
