#!/usr/bin/env python3
"""Run a workload that forces client reconnects while checking successful operations."""

from __future__ import annotations

from helper_assertions import always, reachable, sometimes
from helper_scenarios import (
    WorkloadScenario,
    env_bool,
    env_int,
    env_str,
    process_details,
    run_checker,
    run_workload,
    scenario_value_prefix,
)


def main() -> None:
    """Exercise protocol handling while clients reconnect under load."""
    scenario = WorkloadScenario(
        name="disconnects",
        clients=env_int("CLIENTS", 4),
        keys=env_int("KEYS", 16),
        set_pct=env_int("SET_PCT", 60),
        duration=env_str("DURATION", "20s"),
        fail_fast=env_bool("FAIL_FAST", False),
        allow_errors=env_bool("ALLOW_ERRORS", True),
        fault_disconnect_pct=env_int("FAULT_DISCONNECT_PCT", 10),
        key_prefix=env_str("KEY_PREFIX", "antithesis_shared_"),
        value_prefix=env_str("VALUE_PREFIX", scenario_value_prefix("disconnects")),
        checksum_values=env_bool("CHECKSUM_VALUES", True),
        seed=env_int("SEED", 0),
    )
    reachable("Starting disconnect workload", {"scenario": scenario.name})
    run = run_workload(scenario)
    always(run.process.returncode == 0, "Disconnect workload exits successfully", {**process_details(run.process), "summary": run.summary, "history": run.history_path})

    checker = run_checker(run.history_path, allow_errors=scenario.allow_errors, log_name="checker-disconnects")
    always(checker.process.returncode == 0, "Disconnect scenario successful GET/SET operations are linearizable", {**process_details(checker.process), "history": run.history_path, "checker_summary": checker.summary})
    sometimes(int(run.summary.get("errors", 0)) > 0, "Disconnect scenario observed transient client errors", {"summary": run.summary})
    sometimes(bool(checker.summary.get("ok", False)), "Disconnect scenario produced a passing history", {"checker_summary": checker.summary})


if __name__ == "__main__":
    main()

