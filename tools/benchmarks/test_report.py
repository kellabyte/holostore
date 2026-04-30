#!/usr/bin/env python3
"""Unit tests for benchmark report helpers."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


REPORT_PATH = Path(__file__).with_name("report.py")
SPEC = importlib.util.spec_from_file_location("bench_report", REPORT_PATH)
assert SPEC is not None
bench_report = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(bench_report)


def make_run(**summary_overrides: object) -> dict[str, object]:
    summary: dict[str, object] = {
        "target": "etcd",
        "scenario": "write-uniform",
        "scheduled_throughput_per_second": 1000.0,
        "completed_throughput_per_second": 995.0,
        "errors": 0,
        "service_latency": {"p95_ms": 20.0},
        "start_lag": {"p95_ms": 2.0},
        "client_saturation": {
            "completed_within_scheduled_duration": True,
            "drain_seconds": 0.05,
        },
    }
    summary.update(summary_overrides)
    return {"summary": summary, "label": f"{summary['target']}:{summary['scenario']}"}


class ReportOverloadTests(unittest.TestCase):
    def test_overload_detected_when_completed_throughput_trails_offered(self) -> None:
        run = make_run(completed_throughput_per_second=700.0)

        status = bench_report.overload_status(run)

        self.assertTrue(status["overloaded"])
        self.assertIn("completed 70.0% of offered throughput", status["reasons"])

    def test_overload_detected_when_run_drains_after_schedule(self) -> None:
        run = make_run(
            client_saturation={
                "completed_within_scheduled_duration": False,
                "drain_seconds": 3.25,
            }
        )

        status = bench_report.overload_status(run)

        self.assertTrue(status["overloaded"])
        self.assertIn("drained 3.25s after schedule", status["reasons"])

    def test_overload_section_names_overloaded_database(self) -> None:
        ok_run = make_run(target="holostore")
        overloaded_run = make_run(target="etcd", completed_throughput_per_second=600.0)

        lines = bench_report.overload_section([ok_run, overloaded_run])

        text = "\n".join(lines)
        self.assertIn("CAUTION: overloaded database(s): `etcd`", text)
        self.assertIn("| etcd | write-uniform | OVERLOADED |", text)
        self.assertIn("| holostore | write-uniform | ok |", text)

    def test_failed_run_is_loaded_without_summary_or_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "etcd"
            run_dir.mkdir()
            (run_dir / "config.json").write_text(
                json.dumps(
                    {
                        "target": "etcd",
                        "scenario": "write-uniform",
                        "rate": 10000,
                        "duration": "240s",
                        "workers": 256,
                        "connections": 128,
                        "write_pct": 100,
                        "contention": "uniform",
                        "keys": 1000000,
                    }
                ),
                encoding="utf-8",
            )
            (run_dir / "failure.json").write_text(
                json.dumps(
                    {
                        "target": "etcd",
                        "scenario": "write-uniform",
                        "phase": "benchmark",
                        "exit_code": 137,
                        "reason": "benchmark exited 137; the container was killed",
                    }
                ),
                encoding="utf-8",
            )

            run = bench_report.load_run(run_dir)

            self.assertEqual(run["metrics"], [])
            self.assertTrue(run["summary"]["failed"])
            self.assertEqual(run["summary"]["target"], "etcd")
            self.assertEqual(run["summary"]["connections"], 128)

    def test_report_calls_out_failed_target_and_missing_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dir = root / "etcd"
            run_dir.mkdir()
            (run_dir / "failure.json").write_text(
                json.dumps(
                    {
                        "target": "etcd",
                        "scenario": "write-uniform",
                        "phase": "benchmark",
                        "exit_code": 137,
                        "reason": "benchmark exited 137; the container was killed",
                    }
                ),
                encoding="utf-8",
            )
            run = bench_report.load_run(run_dir)

            out = root / "report.md"
            bench_report.write_report([run], out, "Failure Report")

            text = out.read_text(encoding="utf-8")
            self.assertIn("## Failed Targets", text)
            self.assertIn("CAUTION: failed target(s): `etcd`", text)
            self.assertIn("| FAILED | etcd | write-uniform |", text)
            self.assertIn("- metrics: missing", text)
            self.assertIn("- summary: missing", text)

    def test_throughput_plot_specs_use_successes_and_dotted_errors(self) -> None:
        runs = [
            {"label": "holostore:uniform"},
            {"label": "etcd:uniform"},
        ]

        specs = bench_report.throughput_line_specs(runs)

        self.assertEqual([spec["key"] for spec in specs], ["ok", "errors", "ok", "errors"])
        self.assertEqual(specs[0]["linestyle"], "-")
        self.assertEqual(specs[1]["linestyle"], ":")
        self.assertIn("holostore:uniform ok", specs[0]["label"])
        self.assertIn("holostore:uniform errors", specs[1]["label"])
        self.assertNotEqual(specs[0]["color"], specs[1]["color"])

    def test_target_throughput_rates_are_deduplicated(self) -> None:
        runs = [
            make_run(target="holostore", scheduled_throughput_per_second=5000.0),
            make_run(target="etcd", scheduled_throughput_per_second=5000.0),
            make_run(target="other", scheduled_throughput_per_second=10000.0),
        ]

        rates = bench_report.target_throughput_rates(runs)

        self.assertEqual(rates, [5000.0, 10000.0])

    def test_latency_percentile_label_strips_metric_suffix(self) -> None:
        self.assertEqual(bench_report.latency_percentile_label("p99_ms"), "p99")
        self.assertEqual(bench_report.latency_percentile_label("p99_9_ms"), "p99.9")

    def test_graph_description_explains_start_lag(self) -> None:
        description = bench_report.graph_description(Path("graphs/start_lag_p99.png"))

        self.assertIn("p99 delay", description)
        self.assertIn("scheduled an operation", description)
        self.assertIn("client worker actually started", description)
        self.assertIn("coordinated omission", description)

    def test_load_events_normalizes_unix_millis_to_run_seconds(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            (run_dir / "events-node1.csv").write_text(
                "\n".join(
                    [
                        "unix_ms,target,event,operation_id,node_id,shard_id,shard_index,target_shard_index,split_key,reason,metadata_only",
                        "1500,holostore,split_start,op1,1,10,0,1,key:050,adaptive,true",
                        "1750,holostore,split_end,op1,1,10,0,1,key:050,adaptive,true",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            events, event_files = bench_report.load_events(
                run_dir,
                {"scheduled_start_unix_ms": 1000},
            )

        self.assertEqual([path.name for path in event_files], ["events-node1.csv"])
        self.assertEqual([event["second"] for event in events], [0.5, 0.75])

    def test_split_event_windows_pair_start_and_end_by_operation(self) -> None:
        run = {
            "events": [
                {"event": "split_end", "operation_id": "op1", "second": 2.0},
                {"event": "split_start", "operation_id": "op1", "second": 1.0},
            ]
        }

        windows = bench_report.split_event_windows([run])

        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0][0]["event"], "split_start")
        self.assertEqual(windows[0][1]["event"], "split_end")

    def test_error_breakdown_section_lists_categories_and_samples(self) -> None:
        run = make_run(
            target="holostore",
            errors=42,
            error_categories=[
                {
                    "category": "resp_error",
                    "count": 40,
                    "percent_of_errors": 95.238,
                    "first_second": 12,
                    "last_second": 19,
                    "sample": "ERR proposal timed out",
                }
            ],
            top_error_messages=[
                {
                    "category": "resp_error",
                    "count": 40,
                    "percent_of_errors": 95.238,
                    "first_second": 12,
                    "last_second": 19,
                    "sample": "ERR proposal timed out",
                }
            ],
        )

        lines = bench_report.error_breakdown_section([run])

        text = "\n".join(lines)
        self.assertIn("## Error Breakdown", text)
        self.assertIn("| holostore | write-uniform | resp_error | 40 | 95.24 | 12 | 19 | ERR proposal timed out |", text)
        self.assertIn("### Top Error Samples", text)

    def test_error_breakdown_section_falls_back_for_old_summaries(self) -> None:
        run = make_run(target="etcd", errors=3)

        lines = bench_report.error_breakdown_section([run])

        text = "\n".join(lines)
        self.assertIn("| etcd | write-uniform | uncategorized | 3 | 100.00 |", text)


if __name__ == "__main__":
    unittest.main()
