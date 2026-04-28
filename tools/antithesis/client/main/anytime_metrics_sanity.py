#!/usr/bin/env python3
"""Probe HOLOMETRICS and assert only invariants that remain valid under faults."""

from __future__ import annotations

import math
import time

from helper_assertions import always, always_or_unreachable, reachable, sometimes
from helper_holostore import fetch_holometrics, redis_nodes


def parse_prometheus_metrics(text: str) -> dict[str, list[float]]:
    """Parse a small subset of Prometheus text exposition into numeric samples."""
    parsed: dict[str, list[float]] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            metric, raw_value = line.rsplit(" ", 1)
            name = metric.split("{", 1)[0]
            value = float(raw_value)
        except ValueError:
            raise AssertionError(f"unparseable metric line: {line}")
        parsed.setdefault(name, []).append(value)
    return parsed


def main() -> None:
    """Check that metric text parses and counter-like samples do not go backward in one probe."""
    endpoints = redis_nodes()
    saw_metrics_from: list[str] = []
    saw_split_metric = False
    saw_slow_path_metric = False

    for endpoint in endpoints:
        first = fetch_holometrics(endpoint)
        if not first:
            continue
        time.sleep(0.2)
        second = fetch_holometrics(endpoint)
        first_metrics = parse_prometheus_metrics(first)
        second_metrics = parse_prometheus_metrics(second or first)
        saw_metrics_from.append(endpoint)
        reachable("Fetched HOLOMETRICS from node", {"endpoint": endpoint, "metric_names": sorted(first_metrics.keys())[:12]})

        for name, values in first_metrics.items():
            for value in values:
                always(math.isfinite(value), "HOLOMETRICS values are finite numbers", {"endpoint": endpoint, "metric": name, "value": value})
                always(value >= 0, "HOLOMETRICS numeric values are non-negative", {"endpoint": endpoint, "metric": name, "value": value})
            if "split" in name or "merge" in name:
                saw_split_metric = True
            if "slow" in name or "fast" in name:
                saw_slow_path_metric = True

        for name, first_values in first_metrics.items():
            if not name.endswith("_total"):
                continue
            second_values = second_metrics.get(name)
            if not second_values:
                continue
            for first_value, second_value in zip(first_values, second_values):
                always_or_unreachable(
                    second_value >= first_value,
                    "Counter-like HOLOMETRICS samples do not go backward during one probe",
                    {
                        "endpoint": endpoint,
                        "metric": name,
                        "first_value": first_value,
                        "second_value": second_value,
                    },
                )

    sometimes(len(saw_metrics_from) > 0, "At least one node returned HOLOMETRICS", {"endpoints": endpoints, "reachable_metrics_nodes": saw_metrics_from})
    for endpoint in endpoints:
        sometimes(endpoint in saw_metrics_from, "Each node exposes HOLOMETRICS on some timelines", {"endpoint": endpoint, "reachable_metrics_nodes": saw_metrics_from})
    sometimes(saw_split_metric, "Split or merge metrics were observed", {"reachable_metrics_nodes": saw_metrics_from})
    sometimes(saw_slow_path_metric, "Fast-path or slow-path metrics were observed", {"reachable_metrics_nodes": saw_metrics_from})


if __name__ == "__main__":
    main()

