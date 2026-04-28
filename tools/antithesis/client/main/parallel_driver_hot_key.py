#!/usr/bin/env python3
"""Run a hot-key write-contention workload under Antithesis."""

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
    """Stress one key to maximize conflicting writes and read visibility pressure."""
    scenario = WorkloadScenario(
        name="hot-key",
        clients=env_int("CLIENTS", 6),
        keys=env_int("KEYS", 1),
        set_pct=env_int("SET_PCT", 90),
        duration=env_str("DURATION", "20s"),
        allow_errors=env_bool("ALLOW_ERRORS", False),
        fault_disconnect_pct=env_int("FAULT_DISCONNECT_PCT", 0),
        key_prefix=env_str("KEY_PREFIX", "antithesis_shared_"),
        value_prefix=env_str("VALUE_PREFIX", scenario_value_prefix("hot_key")),
        checksum_values=env_bool("CHECKSUM_VALUES", True),
        seed=env_int("SEED", 0),
    )
    reachable("Starting hot-key contention workload", {"scenario": scenario.name})
    run = run_workload(scenario)
    always(run.process.returncode == 0, "Hot-key workload exits successfully", {**process_details(run.process), "summary": run.summary, "history": run.history_path})

    checker = run_checker(run.history_path, allow_errors=scenario.allow_errors, log_name="checker-hot-key")
    always(checker.process.returncode == 0, "Hot-key successful GET/SET operations are linearizable", {**process_details(checker.process), "history": run.history_path, "checker_summary": checker.summary})
    sometimes(int(run.summary.get("ok_sets", 0)) > 0, "Hot-key workload observed successful writes", {"summary": run.summary})
    sometimes(bool(checker.summary.get("ok", False)), "Hot-key workload produced a passing history", {"checker_summary": checker.summary})


if __name__ == "__main__":
    main()

