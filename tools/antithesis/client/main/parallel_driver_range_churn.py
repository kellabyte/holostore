#!/usr/bin/env python3
"""Run a multi-key workload intended to overlap with aggressive range splitting."""

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
    """Stress many keys so split and migration paths see live client traffic."""
    scenario = WorkloadScenario(
        name="range-churn",
        clients=env_int("CLIENTS", 4),
        keys=env_int("KEYS", 64),
        set_pct=env_int("SET_PCT", 70),
        duration=env_str("DURATION", "30s"),
        allow_errors=env_bool("ALLOW_ERRORS", False),
        fault_disconnect_pct=env_int("FAULT_DISCONNECT_PCT", 0),
        key_prefix=env_str("KEY_PREFIX", f"antithesis_range_churn_{scenario_value_prefix('keys')}"),
        value_prefix=env_str("VALUE_PREFIX", scenario_value_prefix("range_churn")),
        checksum_values=env_bool("CHECKSUM_VALUES", True),
        seed=env_int("SEED", 0),
    )
    reachable("Starting range churn workload", {"scenario": scenario.name})
    run = run_workload(scenario)
    always(run.process.returncode == 0, "Range-churn workload exits successfully", {**process_details(run.process), "summary": run.summary, "history": run.history_path})

    checker = run_checker(run.history_path, allow_errors=scenario.allow_errors, log_name="checker-range-churn")
    always(checker.process.returncode == 0, "Range-churn successful GET/SET operations are linearizable", {**process_details(checker.process), "history": run.history_path, "checker_summary": checker.summary})
    sometimes(int(run.summary.get("ok_sets", 0)) > 0, "Range-churn workload observed successful writes", {"summary": run.summary})
    sometimes(bool(checker.summary.get("ok", False)), "Range-churn workload produced a passing history", {"checker_summary": checker.summary})


if __name__ == "__main__":
    main()

