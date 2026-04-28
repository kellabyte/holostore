#!/usr/bin/env python3
"""Verify post-fault recovery using both fresh workloads and known acknowledged writes."""

from __future__ import annotations

import json
from pathlib import Path

from helper_assertions import always, reachable, sometimes
from helper_holostore import HISTORY_ROOT, reachable_nodes, redis_get
from helper_process import wait_until
from helper_scenarios import (
    WorkloadScenario,
    env_bool,
    env_int,
    process_details,
    run_checker,
    run_workload,
    scenario_value_prefix,
)


def latest_acknowledged_file() -> str | None:
    """Return the newest recovery acknowledgment file if one exists."""
    candidates = sorted(Path(HISTORY_ROOT).glob("recovery-acked-*.json"))
    return str(candidates[-1]) if candidates else None


def main() -> None:
    """Wait for recovery, run a clean mixed workload, and probe known keys when available."""
    recovery_nodes = wait_until(
        lambda: reachable_nodes() if len(reachable_nodes()) >= env_int("RECOVERY_MIN_REACHABLE", 2) else [],
        timeout_s=env_int("RECOVERY_TIMEOUT_S", 180),
        interval_s=1.0,
        description="recovery template node reachability",
    )
    reachable("Recovery template found reachable nodes", {"reachable_nodes": recovery_nodes})

    scenario = WorkloadScenario(
        name="recovery-check",
        clients=env_int("CLIENTS", 2),
        keys=env_int("KEYS", 6),
        set_pct=env_int("SET_PCT", 50),
        duration="10s",
        allow_errors=env_bool("ALLOW_ERRORS", False),
        key_prefix=f"antithesis_recovery_check_{scenario_value_prefix('keys')}",
        value_prefix=scenario_value_prefix("recovery_check"),
        checksum_values=env_bool("CHECKSUM_VALUES", True),
        seed=env_int("SEED", 0),
    )
    run = run_workload(scenario)
    always(run.process.returncode == 0, "Recovery-check workload exits successfully", {**process_details(run.process), "summary": run.summary, "history": run.history_path, "reachable_nodes": recovery_nodes})

    checker = run_checker(run.history_path, allow_errors=scenario.allow_errors, log_name="checker-recovery-check")
    always(checker.process.returncode == 0, "Recovery-check successful operations are linearizable", {**process_details(checker.process), "history": run.history_path, "checker_summary": checker.summary, "reachable_nodes": recovery_nodes})

    ack_path = latest_acknowledged_file()
    if ack_path:
        acknowledged = json.loads(Path(ack_path).read_text(encoding="utf-8")).get("acknowledged", {})
        matched_keys = 0
        endpoint = recovery_nodes[0]
        for key, values in list(acknowledged.items())[:5]:
            observed = redis_get(endpoint, key)
            always(observed is not None, "Acknowledged recovery keys remain readable after recovery", {"endpoint": endpoint, "key": key, "acknowledged_values": values, "acknowledged_path": ack_path})
            always(observed in values, "Recovered values must come from acknowledged writes for the same key", {"endpoint": endpoint, "key": key, "observed": observed, "acknowledged_values": values, "acknowledged_path": ack_path})
            matched_keys += 1
        sometimes(matched_keys > 0, "Recovery check validated acknowledged writes after faults stopped", {"endpoint": endpoint, "acknowledged_path": ack_path, "matched_keys": matched_keys})


if __name__ == "__main__":
    main()
