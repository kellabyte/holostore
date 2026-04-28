#!/usr/bin/env python3
"""Verify that the cluster eventually becomes healthy enough for a clean linearizability check."""

from __future__ import annotations

from helper_assertions import always, reachable, sometimes
from helper_holostore import reachable_nodes
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


def main() -> None:
    """Wait for a majority of nodes, then run a tiny clean workload and checker."""
    recovery_nodes = wait_until(
        lambda: reachable_nodes() if len(reachable_nodes()) >= env_int("RECOVERY_MIN_REACHABLE", 2) else [],
        timeout_s=env_int("RECOVERY_TIMEOUT_S", 180),
        interval_s=1.0,
        description="post-fault recovery",
    )
    reachable("Recovery probe found reachable nodes", {"reachable_nodes": recovery_nodes})

    scenario = WorkloadScenario(
        name="eventual-recovery",
        clients=env_int("CLIENTS", 2),
        keys=env_int("KEYS", 4),
        set_pct=env_int("SET_PCT", 50),
        duration="8s",
        allow_errors=env_bool("ALLOW_ERRORS", False),
        key_prefix=f"antithesis_eventual_recovery_{scenario_value_prefix('keys')}",
        value_prefix=scenario_value_prefix("eventual_recovery"),
        checksum_values=env_bool("CHECKSUM_VALUES", True),
        seed=env_int("SEED", 0),
    )
    run = run_workload(scenario)
    always(run.process.returncode == 0, "Eventually recovery workload exits successfully", {**process_details(run.process), "summary": run.summary, "history": run.history_path, "reachable_nodes": recovery_nodes})

    checker = run_checker(run.history_path, allow_errors=scenario.allow_errors, log_name="checker-eventually-recovery")
    always(checker.process.returncode == 0, "Eventually recovery successful operations are linearizable", {**process_details(checker.process), "history": run.history_path, "checker_summary": checker.summary, "reachable_nodes": recovery_nodes})
    sometimes(bool(checker.summary.get("ok", False)), "Eventually recovery produced a passing history", {"checker_summary": checker.summary, "history": run.history_path})


if __name__ == "__main__":
    main()

