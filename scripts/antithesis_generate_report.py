#!/usr/bin/env python3
"""Generate a compact Markdown report for the local Antithesis suite.

The full-suite shell runner records one tab-separated row per harness step or
test command, plus stdout/stderr logs. This script joins those command results
with the known correctness-test catalog and the artifacts emitted under
``.tmp/antithesis/history`` so a failing local run leaves a readable summary.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class TestInfo:
    """Human-readable metadata for one harness step or correctness test."""

    title: str
    purpose: str
    anomalies: str
    artifact_prefixes: tuple[str, ...] = ()


TEST_INFO: dict[str, TestInfo] = {
    "build-images": TestInfo(
        "Build local images",
        "Builds the node, client, health-checker, and config images used by the local suite.",
        "Packaging or toolchain failures that would prevent the correctness tests from running.",
    ),
    "start-stack": TestInfo(
        "Start local cluster",
        "Starts the three-node HoloStore Docker Compose cluster plus client and health-checker.",
        "Container startup failures, bad Compose wiring, missing bind mounts, and broken service discovery.",
    ),
    "wait-for-setup": TestInfo(
        "Wait for setup",
        "Waits for the health-checker to write the setup-complete sentinel after all nodes are ready.",
        "Premature readiness, Redis reachability failures, and missing shared history storage.",
        ("setup-complete",),
    ),
    "first_prepare": TestInfo(
        "Prepare shared history",
        "Verifies the health-checker sentinel exists and /history is writable.",
        "Harness setup bugs, premature setup_complete, and missing shared artifact storage.",
        ("setup-complete",),
    ),
    "singleton_driver_linearizability": TestInfo(
        "Singleton linearizability",
        "Runs the shortest useful mixed GET/SET workload and checks it immediately with Porcupine.",
        "Per-key linearizability violations, stale reads, lost writes, wrong-value reads, out-of-thin-air reads, and checksum/key-binding corruption.",
        ("history-singleton", "workload-singleton", "checker-singleton"),
    ),
    "parallel_driver_baseline_registers": TestInfo(
        "Baseline registers",
        "Runs moderate multi-key concurrency over a shared keyspace.",
        "Ordinary register regressions, cross-client ordering bugs, accidental keyspace collisions, and read freshness problems across nodes.",
        ("history-baseline", "workload-baseline", "checker-baseline"),
    ),
    "parallel_driver_hot_key": TestInfo(
        "Hot-key contention",
        "Drives one key with high write contention.",
        "Accord dependency ordering bugs, missed conflicts, reordered committed writes, stale reads after completed writes, duplicate application, and contention-specific visibility bugs.",
        ("history-hot-key", "workload-hot-key", "checker-hot-key"),
    ),
    "parallel_driver_range_churn": TestInfo(
        "Range churn",
        "Drives many keys while full-suite defaults enable aggressive range splitting.",
        "Missing range ownership, duplicate active owners, wrong-range reads, split/migration lost writes, stale reads during descriptor churn, and key/value mixups across ranges.",
        ("history-range-churn", "workload-range-churn", "checker-range-churn"),
    ),
    "parallel_driver_disconnects": TestInfo(
        "Client disconnects",
        "Injects client reconnects while checking successful operations for linearizability.",
        "Redis protocol/session bugs, ambiguous retry handling, server-side panics under valid requests, and incorrect treatment of disconnected or timed-out operations.",
        ("history-disconnects", "workload-disconnects", "checker-disconnects"),
    ),
    "anytime_health": TestInfo(
        "Anytime health",
        "Probes Redis reachability without requiring every node to be reachable.",
        "Harness crashes and timelines where no node can serve basic Redis traffic.",
    ),
    "anytime_metrics_sanity": TestInfo(
        "Metrics sanity",
        "Fetches HOLOMETRICS from reachable nodes and checks basic numeric invariants.",
        "Malformed metrics, negative counters, local counter regressions, and metrics-path crashes.",
    ),
    "eventually_recovery": TestInfo(
        "Eventually recovery",
        "Waits for reachability and runs a small fresh linearizability workload.",
        "Failure-to-recover, stuck membership/routing, post-fault stale reads, and unsafe recovery serving.",
        ("history-eventual-recovery", "workload-eventual-recovery", "checker-eventually-recovery"),
    ),
    "singleton_driver_crash_recovery": TestInfo(
        "Crash recovery workload",
        "Runs a set-heavy workload and records acknowledged successful writes.",
        "Durability regressions around acknowledged commits and missing evidence for replay checks.",
        ("history-crash-recovery", "workload-crash-recovery", "recovery-acked"),
    ),
    "eventually_recovery_check": TestInfo(
        "Recovery check",
        "Waits for reachable nodes, runs a fresh mixed workload, and spot-checks acknowledged values when available.",
        "Acknowledged-write loss, WAL replay ordering bugs, duplicate/idempotency mistakes, and recovered nodes serving invalid values.",
        ("history-recovery-check", "workload-recovery-check", "checker-recovery-check", "recovery-acked"),
    ),
    "finally_check_linearizability": TestInfo(
        "Final merged linearizability",
        "Merges all completed history files, offsets client IDs, aligns histories by absolute start time, and checks the merged history.",
        "Cross-driver ordering anomalies, shared-key safety bugs, and history merge/collision mistakes.",
        ("merged-history", "checker-final-merged"),
    ),
}


def parse_args() -> argparse.Namespace:
    """Parse command-line inputs for report generation."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history-dir", required=True, type=Path)
    parser.add_argument("--results", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument(
        "--root-dir",
        type=Path,
        default=None,
        help="repo root used to render report paths relative to the workspace",
    )
    return parser.parse_args()


def load_results(path: Path) -> list[dict[str, str]]:
    """Load the full-suite TSV command result file."""

    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def read_tail(path_text: str, limit: int = 4000, root_dir: Path | None = None) -> str:
    """Read a bounded tail from a log path, returning an empty string if absent."""

    if not path_text:
        return ""
    path = Path(path_text)
    if not path.is_absolute() and root_dir is not None:
        path = root_dir / path
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    return text[-limit:].strip()


def escape_cell(text: str) -> str:
    """Escape text for a compact Markdown table cell."""

    return text.replace("|", "\\|").replace("\n", "<br>")


def status_label(status: str) -> str:
    """Return a stable display label for a command status."""

    if status == "PASS":
        return "PASS"
    if status == "FAIL":
        return "FAIL"
    if status == "SKIP":
        return "SKIP"
    return status or "UNKNOWN"


def normalize_root(root_dir: Path | None, history_dir: Path) -> Path:
    """Return the repo root used for report path redaction."""

    if root_dir is not None:
        return root_dir.resolve()
    history = history_dir.resolve()
    if history.name == "history" and history.parent.name == "antithesis" and history.parent.parent.name == ".tmp":
        return history.parent.parent.parent
    return Path.cwd().resolve()


def display_path(path_text: str | Path, root_dir: Path) -> str:
    """Render a path without leaking local absolute workspace prefixes."""

    path = Path(path_text)
    try:
        relative = path.resolve().relative_to(root_dir)
        return relative.as_posix()
    except (OSError, ValueError):
        text = str(path_text)
        root_text = root_dir.as_posix()
        if text == root_text:
            return "."
        if text.startswith(root_text + "/"):
            return text[len(root_text) + 1 :]
        return text


def sanitize_text(text: str, root_dir: Path) -> str:
    """Replace absolute workspace paths in report text with relative paths."""

    if not text:
        return text
    root_text = root_dir.as_posix()
    return text.replace(root_text + "/", "").replace(root_text, ".")


def related_artifacts(history_dir: Path, name: str) -> list[Path]:
    """Find artifacts associated with one command based on configured prefixes."""

    info = TEST_INFO.get(name)
    prefixes = info.artifact_prefixes if info else (name,)
    matches: list[Path] = []
    for prefix in prefixes:
        matches.extend(path for path in history_dir.glob(f"{prefix}*") if path.is_file())
    return sorted(set(matches), key=lambda path: path.name)


def load_json(path: Path) -> dict[str, object]:
    """Best-effort JSON loader for checker and workload summaries."""

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def summarize_json_artifacts(paths: Iterable[Path]) -> list[str]:
    """Extract short metrics from workload and checker summary artifacts."""

    summaries: list[str] = []
    for path in paths:
        if not (path.name.endswith(".summary.json") or path.name.endswith(".checker-summary.json")):
            continue
        data = load_json(path)
        if not data:
            continue
        if path.name.endswith(".checker-summary.json"):
            fields = [
                f"ok={data.get('ok')}",
                f"keys={data.get('keys_checked')}",
                f"ops={data.get('ops_checked')}",
                f"errors_ignored={data.get('errors_ignored')}",
                f"out_of_thin_air={data.get('out_of_thin_air')}",
                f"checksum={data.get('checksum_values_validated')}",
            ]
        else:
            fields = [
                f"ops={data.get('ops')}",
                f"ok_sets={data.get('ok_sets')}",
                f"value_gets={data.get('value_gets')}",
                f"nil_gets={data.get('nil_gets')}",
                f"errors={data.get('errors')}",
            ]
        summaries.append(f"{path.name}: " + ", ".join(fields))
    return summaries


def artifact_failures(paths: Iterable[Path]) -> list[str]:
    """Return failure reasons found in checker summary artifacts."""

    failures: list[str] = []
    for path in paths:
        if not path.name.endswith(".checker-summary.json"):
            continue
        data = load_json(path)
        if not data:
            continue
        if data.get("ok") is False:
            failures.append(f"{path.name}: checker ok=false")
        if data.get("out_of_thin_air") is True:
            failures.append(f"{path.name}: out_of_thin_air=true")
        if data.get("checksum_values_validated") is False and data.get("ops_checked", 0):
            failures.append(f"{path.name}: checksum validation did not complete")
    return failures


def effective_status(row: dict[str, str], artifacts: Iterable[Path]) -> str:
    """Combine command status with checker artifacts to decide pass/fail."""

    status = row.get("status", "")
    if status == "FAIL":
        return "FAIL"
    if artifact_failures(artifacts):
        return "FAIL"
    return status


def artifact_lines(paths: Iterable[Path], root_dir: Path) -> list[str]:
    """Format artifact paths for the Markdown report."""

    return [f"  - `{display_path(path, root_dir)}`" for path in paths]


def write_report(history_dir: Path, results: list[dict[str, str]], out: Path, root_dir: Path | None = None) -> None:
    """Write the full Markdown report to ``out``."""

    root = normalize_root(root_dir, history_dir)
    artifacts_by_name = {row.get("name", ""): related_artifacts(history_dir, row.get("name", "")) for row in results}
    effective_by_index = [
        effective_status(row, artifacts_by_name.get(row.get("name", ""), [])) for row in results
    ]
    failed = [row for row, status in zip(results, effective_by_index) if status == "FAIL"]
    skipped = [row for row, status in zip(results, effective_by_index) if status == "SKIP"]
    passed = [row for row, status in zip(results, effective_by_index) if status == "PASS"]
    overall = "FAIL" if failed or not results else "PASS"
    generated_at = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    lines: list[str] = [
        "# Antithesis Full Suite Report",
        "",
        f"- Generated: {generated_at}",
        f"- Overall: **{overall}**",
        f"- Passed: {len(passed)}",
        f"- Failed: {len(failed)}",
        f"- Skipped: {len(skipped)}",
        f"- Artifact directory: `{display_path(history_dir, root)}`",
        "",
        "## Tests Run",
        "",
        "| Result | Test | What it looks for |",
        "| --- | --- | --- |",
    ]

    for row, status in zip(results, effective_by_index):
        name = row.get("name", "")
        info = TEST_INFO.get(name, TestInfo(name, "No description recorded.", "No anomaly catalog recorded."))
        lines.append(
            f"| {status_label(status)} | {escape_cell(info.title)} | {escape_cell(info.anomalies)} |"
        )

    lines.extend(["", "## Details", ""])
    if not results:
        lines.extend(["No suite command results were recorded.", ""])

    for row, status in zip(results, effective_by_index):
        name = row.get("name", "")
        info = TEST_INFO.get(name, TestInfo(name, "No description recorded.", "No anomaly catalog recorded."))
        artifacts = artifacts_by_name.get(name, [])
        summaries = summarize_json_artifacts(artifacts)
        failures = artifact_failures(artifacts)
        display_status = status_label(status)

        lines.extend(
            [
                f"### {info.title} - {display_status}",
                "",
                f"- Command: `{sanitize_text(row.get('command', ''), root)}`",
                f"- Exit code: `{row.get('exit_code', '')}`",
                f"- Started: `{row.get('started_at', '')}`",
                f"- Ended: `{row.get('ended_at', '')}`",
                f"- Purpose: {info.purpose}",
                f"- Anomalies: {info.anomalies}",
            ]
        )

        if summaries:
            lines.append("- Summary:")
            lines.extend(f"  - {summary}" for summary in summaries)

        if failures:
            lines.append("- Failure signals:")
            lines.extend(f"  - {failure}" for failure in failures)

        if artifacts:
            lines.append("- Related artifacts:")
            lines.extend(artifact_lines(artifacts[:12], root))
            if len(artifacts) > 12:
                lines.append(f"  - ... {len(artifacts) - 12} more")

        if status == "FAIL":
            stdout_tail = sanitize_text(read_tail(row.get("stdout", ""), root_dir=root), root)
            stderr_tail = sanitize_text(read_tail(row.get("stderr", ""), root_dir=root), root)
            if stdout_tail:
                lines.extend(["", "Stdout tail:", "", "```text", stdout_tail, "```"])
            if stderr_tail:
                lines.extend(["", "Stderr tail:", "", "```text", stderr_tail, "```"])
            for path in artifacts:
                if not (path.name.startswith("checker-") and path.suffix in {".stdout", ".stderr"}):
                    continue
                tail = sanitize_text(read_tail(str(path), limit=1600), root)
                if tail:
                    lines.extend(["", f"{path.name} tail:", "", "```text", tail, "```"])

        lines.append("")

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")
    Path(f"{out}.status").write_text(overall + "\n", encoding="utf-8")


def main() -> int:
    """Generate the requested report and return a process status code."""

    args = parse_args()
    results = load_results(args.results)
    write_report(args.history_dir, results, args.out, args.root_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
