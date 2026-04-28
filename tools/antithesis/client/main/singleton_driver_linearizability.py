#!/usr/bin/env python3
"""Run the fastest useful end-to-end linearizability check for HoloStore."""

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
    """Run one short mixed workload, then check its history with Porcupine."""
    scenario = WorkloadScenario(
        name="singleton",
        clients=env_int("CLIENTS", 6),
        keys=env_int("KEYS", 8),
        set_pct=env_int("SET_PCT", 60),
        duration=env_str("DURATION", "60s"),
        op_timeout=env_str("OP_TIMEOUT", "5s"),
        fail_fast=env_bool("FAIL_FAST", False),
        allow_errors=env_bool("ALLOW_ERRORS", True),
        fault_disconnect_pct=env_int("FAULT_DISCONNECT_PCT", 5),
        key_prefix=env_str("KEY_PREFIX", "antithesis_shared_"),
        value_prefix=env_str("VALUE_PREFIX", scenario_value_prefix("singleton")),
        checksum_values=env_bool("CHECKSUM_VALUES", True),
        seed=env_int("SEED", 0),
    )
    reachable(
        "starting HoloStore singleton linearizability workload",
        {"scenario": scenario.name, "key_prefix": scenario.key_prefix},
    )

    run = run_workload(scenario)
    always(
        run.process.returncode == 0,
        "holo-workload exits successfully",
        {
            "scenario": scenario.name,
            "history": run.history_path,
            "summary": run.summary,
            **process_details(run.process),
        },
    )

    checker = run_checker(run.history_path, allow_errors=scenario.allow_errors, log_name="checker-singleton")
    always(
        checker.process.returncode == 0,
        "HoloStore successful GET/SET operations are linearizable",
        {
            "scenario": scenario.name,
            "history": run.history_path,
            "workload_summary": run.summary,
            "checker_summary": checker.summary,
            **process_details(checker.process),
        },
    )
    sometimes(
        int(run.summary.get("ok_sets", 0)) > 0,
        "Singleton workload observed at least one successful write",
        {"history": run.history_path, "summary": run.summary},
    )
    sometimes(
        int(run.summary.get("value_gets", 0)) + int(run.summary.get("nil_gets", 0)) > 0,
        "Singleton workload observed at least one successful read",
        {"history": run.history_path, "summary": run.summary},
    )
    sometimes(
        bool(checker.summary.get("ok", False)),
        "Porcupine checker observed at least one passing singleton history",
        {"history": run.history_path, "checker_summary": checker.summary},
    )


if __name__ == "__main__":
    main()

