#!/usr/bin/env python3
"""Run a moderate-concurrency baseline register workload under Antithesis."""

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
    """Exercise mixed GET/SET traffic over a shared keyspace."""
    scenario = WorkloadScenario(
        name="baseline",
        clients=env_int("CLIENTS", 3),
        keys=env_int("KEYS", 12),
        set_pct=env_int("SET_PCT", 50),
        duration=env_str("DURATION", "20s"),
        allow_errors=env_bool("ALLOW_ERRORS", False),
        fault_disconnect_pct=env_int("FAULT_DISCONNECT_PCT", 0),
        key_prefix=env_str("KEY_PREFIX", "antithesis_shared_"),
        value_prefix=env_str("VALUE_PREFIX", scenario_value_prefix("baseline")),
        checksum_values=env_bool("CHECKSUM_VALUES", True),
        seed=env_int("SEED", 0),
    )
    reachable("Starting baseline register workload", {"scenario": scenario.name})
    run = run_workload(scenario)
    always(run.process.returncode == 0, "Baseline workload exits successfully", {**process_details(run.process), "summary": run.summary, "history": run.history_path})

    checker = run_checker(run.history_path, allow_errors=scenario.allow_errors, log_name="checker-baseline")
    always(checker.process.returncode == 0, "Baseline successful GET/SET operations are linearizable", {**process_details(checker.process), "history": run.history_path, "checker_summary": checker.summary})
    sometimes(int(run.summary.get("ok_sets", 0)) > 0, "Baseline workload observed successful writes", {"summary": run.summary})
    sometimes(int(run.summary.get("value_gets", 0)) + int(run.summary.get("nil_gets", 0)) > 0, "Baseline workload observed successful reads", {"summary": run.summary})


if __name__ == "__main__":
    main()

