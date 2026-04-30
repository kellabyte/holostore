#!/usr/bin/env python3
"""Generate Markdown and graph artifacts for HoloStore/etcd benchmark runs."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
from pathlib import Path
from typing import Any


PERCENTILES = ["p50_ms", "p75_ms", "p90_ms", "p95_ms", "p99_ms", "p99_9_ms"]
OVERLOAD_THROUGHPUT_RATIO = 0.98
OVERLOAD_DRAIN_SECONDS = 0.5
GRAPH_FIGURE_FACE = "#0b0f14"
GRAPH_AXES_FACE = "#111827"
GRAPH_TEXT = "#f9fafb"
GRAPH_GRID = "#374151"
GRAPH_SPINE = "#6b7280"
GRAPH_TARGET_LINE = "#9ca3af"
GRAPH_SPLIT_MARKER = "#ef4444"
GRAPH_LINEWIDTH = 1.0
GRAPH_LINE_ALPHA = 0.82
GRAPH_GRID_ALPHA = 0.18
GRAPH_TARGET_LINEWIDTH = 1.0
GRAPH_SERIES_COLORS = [
    "#60a5fa",
    "#f59e0b",
    "#34d399",
    "#c084fc",
    "#f472b6",
    "#22d3ee",
]
_PLOT = None


def pyplot() -> Any:
    """Load matplotlib only when graph rendering is needed."""
    global _PLOT
    if _PLOT is None:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as imported_plot

        _PLOT = imported_plot
    return _PLOT


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-dir",
        action="append",
        default=[],
        help="benchmark output directory containing metrics.csv and summary.json",
    )
    parser.add_argument("--out", default="report.md", help="Markdown report path")
    parser.add_argument("--title", default="HoloStore vs etcd Benchmark Report")
    return parser.parse_args()


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload if isinstance(payload, dict) else {}


def summary_from_config(
    run_dir: Path,
    config: dict[str, Any],
    failure: dict[str, Any],
) -> dict[str, Any]:
    target = config.get("target") or failure.get("target") or run_dir.name
    scenario = config.get("scenario") or failure.get("scenario") or run_dir.name
    return {
        "target": target,
        "scenario": scenario,
        "rate": config.get("rate", ""),
        "duration": config.get("duration", ""),
        "workers": config.get("workers", ""),
        "connections": config.get("connections", config.get("workers", "")),
        "worker_headroom": config.get("worker_headroom", ""),
        "write_pct": config.get("write_pct", ""),
        "contention": config.get("contention", ""),
        "keys": config.get("keys", ""),
        "requests_scheduled": 0,
        "completed": 0,
        "ok": 0,
        "errors": 0,
        "scheduled_throughput_per_second": config.get("rate", 0),
        "completed_throughput_per_second": 0,
        "corrected_latency": {},
        "service_latency": {},
        "start_lag": {},
        "client_saturation": {},
    }


def load_run(run_dir: Path) -> dict[str, Any]:
    summary_path = run_dir / "summary.json"
    metrics_path = run_dir / "metrics.csv"
    config = read_json(run_dir / "config.json")
    failure = read_json(run_dir / "failure.json")
    missing_artifacts = [
        name
        for name, path in (("summary.json", summary_path), ("metrics.csv", metrics_path))
        if not path.exists()
    ]
    if missing_artifacts and not failure:
        failure = {
            "target": config.get("target") or run_dir.name,
            "scenario": config.get("scenario") or run_dir.name,
            "phase": "artifact-check",
            "exit_code": "",
            "reason": "missing benchmark artifact(s): " + ", ".join(missing_artifacts),
        }
    summary = read_json(summary_path)
    if not summary:
        summary = summary_from_config(run_dir, config, failure)
    if "target" not in summary:
        summary["target"] = config.get("target") or failure.get("target") or run_dir.name
    if "scenario" not in summary:
        summary["scenario"] = (
            config.get("scenario") or failure.get("scenario") or run_dir.name
        )
    if failure:
        summary["failed"] = True
        summary["failure"] = failure

    if metrics_path.exists():
        with metrics_path.open("r", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
    else:
        rows = []
    events, event_files = load_events(run_dir, summary)

    label = f"{summary.get('target', run_dir.name)}:{summary.get('scenario', run_dir.name)}"
    return {
        "dir": run_dir,
        "summary": summary,
        "metrics": rows,
        "events": events,
        "event_files": event_files,
        "config": config,
        "failure": failure,
        "label": label,
    }


def f(row: dict[str, str], key: str) -> float:
    raw = row.get(key, "0")
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(value) or math.isinf(value):
        return 0.0
    return value


def series(run: dict[str, Any], key: str) -> tuple[list[float], list[float]]:
    rows = run["metrics"]
    return [f(row, "second") for row in rows], [f(row, key) for row in rows]


def load_events(run_dir: Path, summary: dict[str, Any]) -> tuple[list[dict[str, Any]], list[Path]]:
    """Load optional benchmark timeline event CSVs and normalize them to seconds."""
    event_files = sorted(
        path
        for pattern in ("events.csv", "events-*.csv")
        for path in run_dir.glob(pattern)
        if path.is_file()
    )
    scheduled_start_ms = as_float(summary.get("scheduled_start_unix_ms", 0))
    events: list[dict[str, Any]] = []
    for path in event_files:
        with path.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                event = dict(row)
                unix_ms = as_float(event.get("unix_ms", 0))
                if scheduled_start_ms > 0 and unix_ms > 0:
                    event["second"] = (unix_ms - scheduled_start_ms) / 1000.0
                else:
                    event["second"] = as_float(event.get("second", 0))
                event["source_file"] = path.name
                events.append(event)
    events.sort(key=lambda event: as_float(event.get("second", 0)))
    return events, event_files


def split_events(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return split start/end events from metrics-bearing runs."""
    events: list[dict[str, Any]] = []
    for run in runs:
        for event in run.get("events", []):
            if event.get("event") in {"split_start", "split_end"}:
                events.append(event)
    return sorted(events, key=lambda event: as_float(event.get("second", 0)))


def split_event_windows(runs: list[dict[str, Any]]) -> list[tuple[dict[str, Any], dict[str, Any] | None]]:
    """Pair split_start events with same-operation split_end events when present."""
    events = split_events(runs)
    ends_by_operation: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        if event.get("event") == "split_end":
            ends_by_operation.setdefault(str(event.get("operation_id", "")), []).append(event)
    for matches in ends_by_operation.values():
        matches.sort(key=lambda event: as_float(event.get("second", 0)))

    windows: list[tuple[dict[str, Any], dict[str, Any] | None]] = []
    for event in events:
        if event.get("event") != "split_start":
            continue
        operation_id = str(event.get("operation_id", ""))
        start_second = as_float(event.get("second", 0))
        end_event = None
        candidates = ends_by_operation.get(operation_id, [])
        for index, candidate in enumerate(candidates):
            if as_float(candidate.get("second", 0)) >= start_second:
                end_event = candidate
                del candidates[index]
                break
        windows.append((event, end_event))
    return windows


def add_split_event_markers(axis: Any, runs: list[dict[str, Any]]) -> None:
    """Overlay split start/end markers on timeline charts when events are present."""
    windows = split_event_windows(runs)
    if not windows:
        return

    x_min, x_max = axis.get_xlim()
    labels_used: set[str] = set()
    for start, end in windows:
        start_second = as_float(start.get("second", 0))
        end_second = as_float(end.get("second", 0)) if end is not None else None
        window_end = end_second if end_second is not None else start_second
        if window_end < x_min or start_second > x_max:
            continue
        if x_min <= start_second <= x_max:
            label = "split start" if "split start" not in labels_used else None
            axis.axvline(
                start_second,
                color=GRAPH_SPLIT_MARKER,
                linestyle=":",
                linewidth=1.4,
                alpha=0.95,
                label=label,
                zorder=5,
            )
            labels_used.add("split start")
        if end_second is not None and x_min <= end_second <= x_max:
            label = "split end" if "split end" not in labels_used else None
            axis.axvline(
                end_second,
                color=GRAPH_SPLIT_MARKER,
                linestyle=":",
                linewidth=1.4,
                alpha=0.95,
                label=label,
                zorder=5,
            )
            labels_used.add("split end")


def series_color(index: int) -> str:
    return GRAPH_SERIES_COLORS[index % len(GRAPH_SERIES_COLORS)]


def lighten_color(hex_color: str, amount: float = 0.35) -> str:
    """Return a lighter shade of a hex color for same-store companion lines."""
    hex_color = hex_color.lstrip("#")
    red = int(hex_color[0:2], 16)
    green = int(hex_color[2:4], 16)
    blue = int(hex_color[4:6], 16)
    red = int(red + (255 - red) * amount)
    green = int(green + (255 - green) * amount)
    blue = int(blue + (255 - blue) * amount)
    return f"#{red:02x}{green:02x}{blue:02x}"


def throughput_line_specs(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Describe successful-throughput and error companion lines for plotting."""
    specs: list[dict[str, Any]] = []
    for index, run in enumerate(runs):
        color = series_color(index)
        specs.append(
            {
                "run": run,
                "key": "ok",
                "label": f"{run['label']} ok",
                "color": color,
                "linestyle": "-",
                "linewidth": GRAPH_LINEWIDTH,
            }
        )
        specs.append(
            {
                "run": run,
                "key": "errors",
                "label": f"{run['label']} errors",
                "color": lighten_color(color),
                "linestyle": ":",
                "linewidth": GRAPH_LINEWIDTH,
            }
        )
    return specs


def target_throughput_rates(runs: list[dict[str, Any]]) -> list[float]:
    """Return distinct offered-throughput rates that should be drawn as guides."""
    rates = {
        as_float(
            run["summary"].get(
                "scheduled_throughput_per_second",
                run["summary"].get("rate", 0),
            )
        )
        for run in runs
        if run.get("summary")
    }
    return sorted(rate for rate in rates if rate > 0)


def dark_figure() -> tuple[Any, Any]:
    """Create a matplotlib figure/axis pair with the benchmark dark theme."""
    plot = pyplot()
    figure, axis = plot.subplots(figsize=(11, 5.5), facecolor=GRAPH_FIGURE_FACE)
    axis.set_facecolor(GRAPH_AXES_FACE)
    return plot, axis


def finish_dark_plot(axis: Any) -> None:
    """Apply dark-mode text, grid, spines, and legend styling."""
    axis.title.set_color(GRAPH_TEXT)
    axis.xaxis.label.set_color(GRAPH_TEXT)
    axis.yaxis.label.set_color(GRAPH_TEXT)
    axis.tick_params(axis="both", colors=GRAPH_TEXT)
    for spine in axis.spines.values():
        spine.set_color(GRAPH_SPINE)
    axis.grid(True, color=GRAPH_GRID, alpha=GRAPH_GRID_ALPHA)
    legend = axis.legend()
    if legend is not None:
        legend.get_frame().set_facecolor(GRAPH_AXES_FACE)
        legend.get_frame().set_edgecolor(GRAPH_SPINE)
        legend.get_frame().set_alpha(0.95)
        for text in legend.get_texts():
            text.set_color(GRAPH_TEXT)


def save_plot(path: Path) -> None:
    plot = pyplot()
    path.parent.mkdir(parents=True, exist_ok=True)
    plot.tight_layout()
    plot.savefig(path, dpi=150, facecolor=plot.gcf().get_facecolor())
    plot.close()


def plot_throughput(runs: list[dict[str, Any]], graphs_dir: Path) -> Path:
    _, axis = dark_figure()
    path = graphs_dir / "throughput.png"
    for target_rate in target_throughput_rates(runs):
        axis.axhline(
            target_rate,
            label=f"target {target_rate:g}/s",
            linewidth=GRAPH_TARGET_LINEWIDTH,
            linestyle="--",
            color=GRAPH_TARGET_LINE,
            alpha=GRAPH_LINE_ALPHA,
            zorder=1,
        )
    for spec in throughput_line_specs(runs):
        x, y = series(spec["run"], spec["key"])
        axis.plot(
            x,
            y,
            label=spec["label"],
            linewidth=spec["linewidth"],
            linestyle=spec["linestyle"],
            color=spec["color"],
            alpha=GRAPH_LINE_ALPHA,
            zorder=2,
        )
    axis.set_title("Successful Throughput Over Time")
    axis.set_xlabel("Second")
    axis.set_ylabel("Operations / second")
    add_split_event_markers(axis, runs)
    finish_dark_plot(axis)
    save_plot(path)
    return path


def plot_error_rate(runs: list[dict[str, Any]], graphs_dir: Path) -> Path | None:
    if not any(any(f(row, "errors") > 0 for row in run["metrics"]) for run in runs):
        return None
    _, axis = dark_figure()
    path = graphs_dir / "errors.png"
    for index, run in enumerate(runs):
        x, y = series(run, "errors")
        axis.plot(
            x,
            y,
            label=run["label"],
            linewidth=GRAPH_LINEWIDTH,
            color=series_color(index),
            alpha=GRAPH_LINE_ALPHA,
        )
    axis.set_title("Errors Over Time")
    axis.set_xlabel("Second")
    axis.set_ylabel("Errors / second")
    add_split_event_markers(axis, runs)
    finish_dark_plot(axis)
    save_plot(path)
    return path


def latency_percentile_label(percentile: str) -> str:
    """Format a metrics percentile key for chart titles and legends."""
    if percentile.endswith("_ms"):
        percentile = percentile[:-3]
    return percentile.replace("_", ".")


def plot_compare_latency(runs: list[dict[str, Any]], graphs_dir: Path, percentile: str) -> Path:
    _, axis = dark_figure()
    path = graphs_dir / f"compare_{percentile}.png"
    for index, run in enumerate(runs):
        x, y = series(run, percentile)
        axis.plot(
            x,
            y,
            label=run["label"],
            linewidth=GRAPH_LINEWIDTH,
            color=series_color(index),
            alpha=GRAPH_LINE_ALPHA,
        )
    axis.set_title(latency_percentile_label(percentile))
    axis.set_xlabel("Second")
    axis.set_ylabel("Milliseconds")
    add_split_event_markers(axis, runs)
    finish_dark_plot(axis)
    save_plot(path)
    return path


def plot_compare_series(
    runs: list[dict[str, Any]],
    graphs_dir: Path,
    key: str,
    title: str,
    filename: str,
) -> Path:
    _, axis = dark_figure()
    path = graphs_dir / filename
    for index, run in enumerate(runs):
        x, y = series(run, key)
        axis.plot(
            x,
            y,
            label=run["label"],
            linewidth=GRAPH_LINEWIDTH,
            color=series_color(index),
            alpha=GRAPH_LINE_ALPHA,
        )
    axis.set_title(title)
    axis.set_xlabel("Second")
    axis.set_ylabel("Milliseconds")
    add_split_event_markers(axis, runs)
    finish_dark_plot(axis)
    save_plot(path)
    return path


def safe_label(label: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in label).strip("_")


def plot_run_latency(run: dict[str, Any], graphs_dir: Path) -> Path:
    _, axis = dark_figure()
    path = graphs_dir / f"latency_{safe_label(run['label'])}.png"
    for index, percentile in enumerate(PERCENTILES):
        x, y = series(run, percentile)
        axis.plot(
            x,
            y,
            label=latency_percentile_label(percentile),
            linewidth=GRAPH_LINEWIDTH,
            color=series_color(index),
            alpha=GRAPH_LINE_ALPHA,
        )
    x, y = series(run, "max_ms")
    axis.plot(
        x,
        y,
        label="max",
        linewidth=GRAPH_LINEWIDTH,
        linestyle="--",
        color=series_color(len(PERCENTILES)),
        alpha=GRAPH_LINE_ALPHA,
    )
    axis.set_title(f"Corrected Latency Percentiles: {run['label']}")
    axis.set_xlabel("Second")
    axis.set_ylabel("Milliseconds")
    add_split_event_markers(axis, [run])
    finish_dark_plot(axis)
    save_plot(path)
    return path


def graph_description(graph_path: Path) -> str:
    """Return a short reader-facing description for a generated graph."""
    stem = graph_path.stem
    split_marker_note = (
        " When present, red dotted vertical markers show shard split start/end."
    )
    descriptions = {
        "throughput": (
            "Successful operations per second and failed operations per second "
            "over time; the dashed line is the offered target rate."
            + split_marker_note
        ),
        "compare_p99_ms": (
            "Corrected p99 latency by one-second bucket. Corrected latency is "
            "measured from scheduled start time, so it includes client queueing."
            + split_marker_note
        ),
        "compare_p99_9_ms": (
            "Corrected p99.9 latency by one-second bucket. This emphasizes "
            "extreme tail latency and includes client queueing."
            + split_marker_note
        ),
        "start_lag_p99": (
            "p99 delay between when the benchmark scheduled an operation and "
            "when a client worker actually started it. Spikes mean the load "
            "generator briefly fell behind; corrected latency keeps those "
            "delayed operations anchored to their scheduled time to avoid "
            "hiding coordinated omission."
            + split_marker_note
        ),
        "errors": "Failed operations per second by target." + split_marker_note,
    }
    if stem.startswith("latency_"):
        return (
            "Single-target corrected latency percentiles over time; max is the "
            "slowest operation observed in each one-second bucket."
            + split_marker_note
        )
    return descriptions.get(stem, "Benchmark metric over time.")


def rel(path: Path, base: Path) -> str:
    try:
        return path.resolve().relative_to(base.resolve()).as_posix()
    except ValueError:
        return os.path.relpath(path.resolve(), base.resolve())


def fmt(value: Any, digits: int = 2) -> str:
    if isinstance(value, int):
        return str(value)
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{number:.{digits}f}"


def as_float(value: Any, default: float = 0.0) -> float:
    """Convert summary values to finite floats for report heuristics."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(number) or math.isinf(number):
        return default
    return number


def as_int(value: Any, default: int = 0) -> int:
    """Convert summary values to ints for report heuristics."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def is_failed(run: dict[str, Any]) -> bool:
    return bool(run.get("failure")) or bool(run["summary"].get("failed"))


def failure_reason(run: dict[str, Any]) -> str:
    failure = run.get("failure") or run["summary"].get("failure") or {}
    reason = failure.get("reason") or failure.get("message")
    if reason:
        return str(reason)
    phase = failure.get("phase")
    exit_code = failure.get("exit_code")
    if phase or exit_code:
        return f"{phase or 'benchmark'} failed with exit {exit_code}"
    return "target failed before producing a complete benchmark result"


def overload_status(run: dict[str, Any]) -> dict[str, Any]:
    """Classify whether a run fell behind the offered benchmark load."""
    summary = run["summary"]
    saturation = summary.get("client_saturation") or {}
    offered = as_float(
        summary.get("scheduled_throughput_per_second", summary.get("rate", 0))
    )
    completed = as_float(summary.get("completed_throughput_per_second", 0))
    drain_seconds = as_float(saturation.get("drain_seconds", 0))
    errors = as_int(summary.get("errors", 0))
    failed = is_failed(run)

    reasons: list[str] = []
    if failed:
        reasons.append(failure_reason(run))
    else:
        if offered > 0 and completed < offered * OVERLOAD_THROUGHPUT_RATIO:
            pct = completed / offered * 100.0
            reasons.append(f"completed {pct:.1f}% of offered throughput")
        if saturation.get("completed_within_scheduled_duration") is False:
            reasons.append(f"drained {fmt(drain_seconds)}s after schedule")
        elif drain_seconds > OVERLOAD_DRAIN_SECONDS:
            reasons.append(f"drained {fmt(drain_seconds)}s after schedule")
        if errors > 0:
            reasons.append(f"{errors} errors")

    return {
        "failed": failed,
        "overloaded": bool(reasons) and not failed,
        "offered": offered,
        "completed": completed,
        "drain_seconds": drain_seconds,
        "errors": errors,
        "reasons": reasons,
    }


def overload_row(run: dict[str, Any]) -> list[str]:
    """Build one Markdown table row describing overload status for a run."""
    summary = run["summary"]
    service = summary.get("service_latency", {})
    start_lag = summary.get("start_lag", {})
    status = overload_status(run)
    if status["failed"]:
        label = "FAILED"
    elif status["overloaded"]:
        label = "OVERLOADED"
    else:
        label = "ok"
    return [
        summary.get("target", ""),
        summary.get("scenario", ""),
        label,
        fmt(status["offered"]),
        fmt(status["completed"]),
        fmt(status["drain_seconds"]),
        str(status["errors"]),
        fmt(start_lag.get("p95_ms", 0)),
        fmt(service.get("p95_ms", 0)),
        "; ".join(status["reasons"]) if status["reasons"] else "",
    ]


def overload_section(runs: list[dict[str, Any]]) -> list[str]:
    """Build the prominent Markdown overload warning section."""
    overloaded: list[str] = []
    failed: list[str] = []
    for run in runs:
        status = overload_status(run)
        target = run["summary"].get("target", run["label"])
        if status["failed"] and target not in failed:
            failed.append(target)
        elif status["overloaded"] and target not in overloaded:
            overloaded.append(target)
    headers = [
        "target",
        "scenario",
        "status",
        "offered/s",
        "completed/s",
        "drain s",
        "errors",
        "start lag p95 ms",
        "service p95 ms",
        "reason",
    ]

    if failed or overloaded:
        intro_parts: list[str] = []
        if failed:
            intro_parts.append(
                "CAUTION: failed target(s): "
                + ", ".join(f"`{target}`" for target in failed)
                + ". Results for failed rows are incomplete."
            )
        if overloaded:
            intro_parts.append(
                "CAUTION: overloaded database(s): "
                + ", ".join(f"`{target}`" for target in overloaded)
                + ". Corrected latency for overloaded rows includes backlog and "
                "should not be read as steady-state service latency."
            )
        intro = " ".join(intro_parts)
    else:
        intro = (
            "No database overload detected by the throughput, drain-time, "
            "and error heuristics."
        )

    return [
        "## Overload Status",
        "",
        intro,
        "",
        *markdown_table(headers, [overload_row(run) for run in runs]),
        "",
    ]


def summary_row(run: dict[str, Any]) -> list[str]:
    summary = run["summary"]
    latency = summary.get("corrected_latency", {})
    service = summary.get("service_latency", {})
    start_lag = summary.get("start_lag", {})
    saturation = summary.get("client_saturation", {})
    workers = str(summary.get("workers", ""))
    requested = summary.get("workers_requested")
    if requested and str(requested) != workers:
        workers = f"{workers} ({requested})"
    connections = str(summary.get("connections", workers))
    return [
        "FAILED" if is_failed(run) else "ok",
        summary.get("target", ""),
        summary.get("scenario", ""),
        str(summary.get("rate", "")),
        str(summary.get("duration", "")),
        workers,
        connections,
        str(summary.get("write_pct", "")),
        summary.get("contention", ""),
        str(summary.get("completed", "")),
        str(summary.get("errors", "")),
        fmt(summary.get("completed_throughput_per_second", 0)),
        fmt(latency.get("p50_ms", 0)),
        fmt(latency.get("p95_ms", 0)),
        fmt(latency.get("p99_ms", 0)),
        fmt(latency.get("p99_9_ms", 0)),
        fmt(latency.get("max_ms", 0)),
        fmt(service.get("p95_ms", 0)),
        fmt(start_lag.get("p95_ms", 0)),
        str(saturation.get("worker_starved", "")),
    ]


def markdown_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return lines


def md_escape(value: Any) -> str:
    """Escape compact table cell text used for benchmark error samples."""
    text = str(value)
    return text.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")


def failure_row(run: dict[str, Any]) -> list[str]:
    failure = run.get("failure") or run["summary"].get("failure") or {}
    summary = run["summary"]
    return [
        str(summary.get("target", "")),
        str(summary.get("scenario", "")),
        str(failure.get("phase", "")),
        str(failure.get("exit_code", "")),
        failure_reason(run),
        str(failure.get("failed_at", "")),
    ]


def failure_section(runs: list[dict[str, Any]]) -> list[str]:
    failed_runs = [run for run in runs if is_failed(run)]
    if not failed_runs:
        return []
    return [
        "## Failed Targets",
        "",
        "This report includes incomplete target(s). Missing metrics or summaries mean the benchmark process failed before it could finish writing normal artifacts.",
        "",
        *markdown_table(
            ["target", "scenario", "phase", "exit code", "reason", "failed at"],
            [failure_row(run) for run in failed_runs],
        ),
        "",
    ]


def error_category_rows(run: dict[str, Any]) -> list[list[str]]:
    """Build category-count rows for one run from summary.json error data."""
    summary = run["summary"]
    categories = summary.get("error_categories") or []
    if not categories and as_int(summary.get("errors", 0)) > 0:
        categories = [
            {
                "category": "uncategorized",
                "count": summary.get("errors", 0),
                "percent_of_errors": 100.0,
                "first_second": summary.get("first_error_second", ""),
                "last_second": summary.get("last_error_second", ""),
                "sample": "summary did not include categorized error samples",
            }
        ]
    rows: list[list[str]] = []
    for category in categories:
        rows.append(
            [
                str(summary.get("target", "")),
                str(summary.get("scenario", "")),
                md_escape(category.get("category", "")),
                str(category.get("count", "")),
                fmt(category.get("percent_of_errors", 0)),
                str(category.get("first_second", "")),
                str(category.get("last_second", "")),
                md_escape(category.get("sample", "")),
            ]
        )
    return rows


def top_error_message_rows(run: dict[str, Any]) -> list[list[str]]:
    """Build sampled top-message rows for one run from bounded summary data."""
    summary = run["summary"]
    rows: list[list[str]] = []
    for message in summary.get("top_error_messages") or []:
        rows.append(
            [
                str(summary.get("target", "")),
                str(summary.get("scenario", "")),
                md_escape(message.get("category", "")),
                str(message.get("count", "")),
                fmt(message.get("percent_of_errors", 0)),
                str(message.get("first_second", "")),
                str(message.get("last_second", "")),
                md_escape(message.get("sample", message.get("message", ""))),
            ]
        )
    return rows


def error_breakdown_section(runs: list[dict[str, Any]]) -> list[str]:
    """Build a prominent Markdown section describing categorized errors."""
    category_rows: list[list[str]] = []
    message_rows: list[list[str]] = []
    untracked: list[str] = []
    for run in runs:
        category_rows.extend(error_category_rows(run))
        message_rows.extend(top_error_message_rows(run))
        omitted = as_int(run["summary"].get("untracked_error_messages", 0))
        if omitted > 0:
            untracked.append(f"{run['label']}: {omitted}")

    if not category_rows:
        return [
            "## Error Breakdown",
            "",
            "No operation errors were recorded.",
            "",
        ]

    lines = [
        "## Error Breakdown",
        "",
        "Operation errors are aggregated by category so failure storms remain visible without logging every failed request.",
        "",
        *markdown_table(
            [
                "target",
                "scenario",
                "category",
                "count",
                "% errors",
                "first sec",
                "last sec",
                "sample",
            ],
            category_rows,
        ),
        "",
    ]
    if message_rows:
        lines.extend(
            [
                "### Top Error Samples",
                "",
                *markdown_table(
                    [
                        "target",
                        "scenario",
                        "category",
                        "count",
                        "% errors",
                        "first sec",
                        "last sec",
                        "sample",
                    ],
                    message_rows,
                ),
                "",
            ]
        )
    if untracked:
        lines.extend(
            [
                "Some error-message samples were omitted after the bounded tracker filled: "
                + "; ".join(untracked)
                + ". Category counts remain exact.",
                "",
            ]
        )
    return lines


def write_report(runs: list[dict[str, Any]], out: Path, title: str) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    graphs_dir = out.parent / "graphs"
    metric_runs = [run for run in runs if run["metrics"]]
    graph_paths: list[Path] = []
    graph_error = ""
    if metric_runs:
        try:
            graph_paths = [
                plot_throughput(metric_runs, graphs_dir),
                plot_compare_series(
                    metric_runs,
                    graphs_dir,
                    "start_lag_p99_ms",
                    "Client Request Lag p99",
                    "start_lag_p99.png",
                ),
                plot_compare_latency(metric_runs, graphs_dir, "p99_ms"),
                plot_compare_latency(metric_runs, graphs_dir, "p99_9_ms"),
            ]
            error_graph = plot_error_rate(metric_runs, graphs_dir)
            if error_graph is not None:
                graph_paths.append(error_graph)
            graph_paths.extend(plot_run_latency(run, graphs_dir) for run in metric_runs)
        except ModuleNotFoundError as exc:
            if exc.name != "matplotlib":
                raise
            graph_error = "Graph generation skipped because matplotlib is not installed."

    headers = [
        "status",
        "target",
        "scenario",
        "rate",
        "duration",
        "workers",
        "connections",
        "write %",
        "contention",
        "completed",
        "errors",
        "throughput/s",
        "p50 ms",
        "p95 ms",
        "p99 ms",
        "p99.9 ms",
        "max ms",
        "service p95 ms",
        "start lag p95 ms",
        "worker starved",
    ]

    generated = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    lines = [
        f"# {title}",
        "",
        f"Generated: {generated}",
        "",
        "Latency percentiles use corrected latency measured from each operation's scheduled start time to completion time, so queueing caused by overload is included.",
        "",
        *failure_section(runs),
        *overload_section(runs),
        *error_breakdown_section(runs),
        "## Summary",
        "",
        *markdown_table(headers, [summary_row(run) for run in runs]),
        "",
        "## Graphs",
        "",
    ]

    if graph_paths:
        for graph_path in graph_paths:
            lines.append(f"![{graph_path.stem}]({rel(graph_path, out.parent)})")
            lines.append("")
            lines.append(f"_{graph_description(graph_path)}_")
            lines.append("")
    else:
        if graph_error:
            lines.append(graph_error)
        else:
            lines.append("No graphs were generated because no metrics were available.")
        lines.append("")

    lines.extend(["## Artifacts", ""])
    for run in runs:
        run_dir = run["dir"]
        lines.append(f"- {run['label']}: `{rel(run_dir, out.parent)}`")
        for filename, label in (
            ("metrics.csv", "metrics"),
            ("errors.csv", "errors"),
            ("summary.json", "summary"),
            ("config.json", "config"),
            ("failure.json", "failure"),
        ):
            artifact = run_dir / filename
            if artifact.exists():
                lines.append(f"  - {label}: `{rel(artifact, out.parent)}`")
            elif is_failed(run) and filename in {"metrics.csv", "summary.json"}:
                lines.append(f"  - {label}: missing")
        for event_file in run.get("event_files", []):
            lines.append(f"  - events: `{rel(event_file, out.parent)}`")

    out.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    run_dirs = [Path(path) for path in args.run_dir] or [Path(".")]
    runs = [load_run(run_dir) for run_dir in run_dirs]
    write_report(runs, Path(args.out), args.title)
    print(f"wrote report: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
