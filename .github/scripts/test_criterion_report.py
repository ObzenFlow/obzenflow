# SPDX-License-Identifier: MIT OR Apache-2.0
# SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
# https://obzenflow.dev

from collections import Counter
import json
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import patch

import criterion_report as report


def current_inventory():
    """Case IDs audited against the 16 existing Cargo targets, not report rules."""
    cases = {}
    cases["journal_components"] = [
        f"causal_components/{operation}/w{width}_p{payload}"
        for operation in ("journal_clock_restore", "byte_accounting", "frontier_from_record", "merge_overlapping", "prepare_append")
        for width, payload in ((1, 256), (32, 256), (1024, 256), (32, 8192))
    ] + [f"disk_components/reader_next/{dimension}" for dimension in ("p256_g1", "p8192_g1", "p256_g64")]
    cases["journal_hot_path"] = [
        f"{operation}/clock_{clock}/advanced_inputs_{advanced}/payload_{payload}"
        for operation in ("record_accounting/canonical_bytes", "causal_record_work/journal_clock_restore", "causal_record_work/clock_clone", "causal_record_work/clock_json_bytes", "journal_record_read/open_and_read")
        for clock, advanced, payload in ((1, 0, 256), (33, 0, 256), (33, 4, 256), (33, 32, 256), (1025, 1024, 256), (33, 32, 8192))
    ] + [f"journal_append_cost/complete_append/{variant}" for variant in (
        "ordinary_business_64", "ordinary_business_group_64", "large_business_64", "mixed_group_64", "execution_fact_group_64",
    )] + [f"reader_dispatch/full/actual_reader/readers_{count}" for count in (1, 8, 32)] + [
        f"hotspots/{operation}/{variant}"
        for operation in ("append", "sequential_read", "reopened_scan", "reader_creation")
        for variant in ("narrow_observed", "wide_observed", "wide_distinct", "wide_absent_control", "large_observed")
    ] + [
        "hotspots/append/clock_1025_absent_control", "hotspots/first_process_open_and_scan/wide_observed",
        "cross_journal_reads/eight_journals_1024_records", "observation_handling/capture_for_record",
        "journal_refresh/append_after_eof", "mixed_journal/distinct_12mib_then_reuse", "mixed_journal/two_concurrent_scans",
    ] + [f"observation_handling/{operation}/clock_{clock}" for operation in ("validation", "live_submission") for clock in (1, 33, 128)] + [
        f"journal_refresh/metrics/{count}_journals/{state}" for count in (1, 4) for state in ("advancing", "unchanged")
    ]
    cases["validation_boundaries"] = [
        "archive_validation/export/inputs_1000", "archive_validation/admit_and_read/inputs_1000",
        "replay_validation/streaming_comparison/inputs_10000", "metrics_validation/tail_refresh/data_and_error_64",
        "studio_validation/project_and_snapshot/inputs_64",
    ]
    cases["pipeline_execution"] = [
        f"{operation}/{depth}_stages" for operation in ("total_execution_time", "execution_time_per_event") for depth in (1, 3, 5, 10)
    ] + ["metrics_reporting/render_100_stages", "metrics_reporting/publish_pair_100_stages/0", "metrics_reporting/publish_pair_100_stages/4"] + [
        f"causal_record_costs/{operation}/{count}" for operation in ("journal_clock_restore", "byte_budget", "authored") for count in (1, 32, 1024)
    ]
    cases["pipeline_throughput"] = [f"completed_flow/{variant}" for variant in (
        "disk_shallow_steady", "disk_deep_steady", "memory_shallow_control", "memory_deep_control", "disk_constrained_capacity_2", "disk_sparse_arrivals",
    )]
    for depth in (1, 2, 3, 4, 5, 20, 100):
        cases[f"per_event_latency_{depth}_stage"] = [f"{depth}_stage_latency/median_latency"]
    cases["per_event_latency_100_stage_memory"] = ["100_stage_latency_memory/median_latency"]
    cases["idle_cpu_usage"] = ["idle_cpu_usage/cpu_percentage"] + [f"idle_cpu_by_depth/{depth}_stages" for depth in (1, 10, 20, 100)]
    cases["waiting_for_gun_cpu_usage"] = ["waiting_for_gun_cpu_usage/cpu_percentage"]
    cases["tokio_worker_3_stage_experiment"] = [f"3_stage_worker_experiments/{variant}" for variant in (
        "4_workers_1to1_ratio", "3_workers_avoid_ratio", "6_workers_excess", "single_threaded",
    )] + ["5_stage_control/default_runtime"]
    return cases


class ClassificationTests(unittest.TestCase):
    def test_current_inventory_is_covered_once(self):
        inventory = current_inventory()
        self.assertEqual(set(inventory), set(report.RULES))
        self.assertEqual(len(inventory), 16)
        categories = Counter()
        for target, ids in inventory.items():
            self.assertEqual(len(ids), len(set(ids)))
            for case_id in ids:
                with self.subTest(target=target, case_id=case_id):
                    category, work = report.classify(target, case_id)
                    self.assertNotEqual(category, report.UNKNOWN)
                    self.assertNotIn("{", work)
                    self.assertEqual(sum(re.fullmatch(pattern, case_id) is not None
                                         for pattern, _, _ in report.RULES[target]), 1)
                    categories[category] += 1
        self.assertEqual(categories, {report.READ: 30, report.APPEND: 11, report.MIXED: 2, report.CAUSAL: 53,
                                     report.OBSERVE: 16, report.RUNTIME: 11, report.FLOW: 22, report.ARCHIVE: 3})

    def test_semantic_boundaries(self):
        for target, case_id, category, description in (
            ("journal_components", "causal_components/prepare_append/w32_p256", report.CAUSAL, "no physical append"),
            ("journal_hot_path", "mixed_journal/two_concurrent_scans", report.READ, "two readers"),
            ("journal_hot_path", "journal_refresh/append_after_eof", report.MIXED, "append, read and EOF"),
            ("journal_hot_path", "reader_dispatch/full/actual_reader/readers_8", report.READ, "Concurrent readers: 8"),
            ("idle_cpu_usage", "idle_cpu_usage/cpu_percentage", report.RUNTIME, "not CPU %"),
            ("pipeline_execution", "execution_time_per_event/3_stages", report.FLOW, "divided by 100 (110 emitted inputs)"),
        ):
            with self.subTest(case_id=case_id):
                actual, work = report.classify(target, case_id)
                self.assertEqual(actual, category)
                self.assertIn(description, work)
        self.assertEqual(report.classify("journal_hot_path", "new/operation")[0], report.UNKNOWN)
        self.assertEqual(report.classify("new_target", "completed_flow/disk_shallow_steady")[0], report.UNKNOWN)


class MeasurementTests(unittest.TestCase):
    def setUp(self):
        Path("target").mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix="criterion-report-test-", dir="target")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def fixture(self, directory="case/new", case_id="completed_flow/disk_shallow_steady"):
        directory = self.root / directory
        directory.mkdir(parents=True)
        data = {
            "benchmark.json": {"full_id": case_id},
            "estimates.json": {"median": {"point_estimate": 12345, "confidence_interval": {
                "confidence_level": 0.9, "lower_bound": 11000, "upper_bound": 13000,
            }}},
            "sample.json": {"times": [24000, 37000], "iters": [2, 3]},
        }
        for name, value in data.items():
            (directory / name).write_text(json.dumps(value))
        return directory

    def test_units_confidence_samples_and_current_results_only(self):
        self.fixture()
        self.fixture("case/base", "old/baseline")
        self.fixture("case/reference", "old/reference")
        rows, errors = report.read_measurements(self.root)
        self.assertFalse(errors)
        self.assertEqual(len(rows), 1)
        markdown = report.render_report("pipeline_throughput", rows, errors, {}, "success")
        self.assertIn("**COMPLETE**", markdown)
        self.assertIn("| 12.345 | 11–13 (90%) | 2 |", markdown)
        self.assertNotIn("old/", markdown)

    def test_malformed_partial_and_duplicate_results_remain_visible(self):
        self.fixture("good/new")
        broken = self.fixture("broken/new", "broken/case")
        (broken / "estimates.json").write_text("{truncated")
        missing = self.fixture("missing/new", "missing/case")
        (missing / "benchmark.json").unlink()
        self.fixture("duplicate/new")
        rows, errors = report.read_measurements(self.root)
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(errors), 3)
        markdown = report.render_report("pipeline_throughput", rows, errors, {}, "failure")
        self.assertIn("**INCOMPLETE**", markdown)
        self.assertIn("partial evidence", markdown)
        self.assertIn("completed_flow/disk_shallow_steady", markdown)
        self.assertIn("duplicate Criterion ID", markdown)
        self.assertIn("Missing or unreadable measurements", markdown)
        self.assertIn("**INCOMPLETE**", report.render_report("pipeline_throughput", rows, errors, {}, "success"))

    def test_empty_or_invalid_measurements_are_not_zeroes(self):
        rows, errors = report.read_measurements(self.root)
        self.assertEqual(rows, [])
        self.assertTrue(errors)
        directory = self.fixture()
        for sample in ({"times": [], "iters": []}, {"times": [1], "iters": [0]},
                       {"times": [1], "iters": [1, 2]}, {"times": [float("nan")], "iters": [1]}):
            with self.subTest(sample=sample):
                (directory / "sample.json").write_text(json.dumps(sample))
                rows, errors = report.read_measurements(self.root)
                self.assertEqual(rows, [])
                self.assertTrue(errors)

    def test_invalid_median_is_not_reported(self):
        directory = self.fixture()
        for point in (float("nan"), float("inf"), -1, True, "12345"):
            with self.subTest(point=point):
                (directory / "estimates.json").write_text(json.dumps({"median": {
                    "point_estimate": point, "confidence_interval": {
                        "lower_bound": 11000, "upper_bound": 13000, "confidence_level": 0.95,
                    },
                }}))
                rows, errors = report.read_measurements(self.root)
                self.assertFalse(rows)
                self.assertTrue(errors)

    def test_unknown_ids_and_markdown_characters_are_preserved_safely(self):
        self.fixture(case_id="new/operation|<script>`\nend")
        rows, errors = report.read_measurements(self.root)
        markdown = report.render_report("pipeline_throughput", rows, errors, {"Rust": "one\ntwo"}, "success")
        self.assertIn("## Uncategorised", markdown)
        self.assertIn("new/operation&#124;&lt;script&gt;&#96; end", markdown)
        self.assertIn("| Rust | one two |", markdown)
        self.assertNotIn("## Journal appends", markdown)

    def test_failed_or_skipped_command_is_incomplete_even_with_valid_rows(self):
        self.fixture()
        rows, errors = report.read_measurements(self.root)
        for outcome in ("failure", "cancelled", "skipped"):
            self.assertIn("**INCOMPLETE**", report.render_report("pipeline_throughput", rows, errors, {}, outcome))

    def test_main_writes_report_and_copyable_summary_on_failure(self):
        summary = self.root / "summary.md"
        output = self.root / "reports"
        args = ["criterion_report.py", "--target", "pipeline_throughput", "--criterion-dir", str(self.root),
                "--output-dir", str(output), "--outcome", "failure", "--preview"]
        with patch("sys.argv", args), patch.dict("os.environ", {"GITHUB_STEP_SUMMARY": str(summary)}, clear=True):
            self.assertEqual(report.main(), 1)
        markdown = next(output.glob("*.md")).read_text()
        self.assertIn("No current measurements found", markdown)
        self.assertIn("PREVIEW", markdown)
        self.assertIn("unavailable (local preview)", markdown)
        self.assertIn(f"```markdown\n{markdown}```", summary.read_text())

    def test_main_uses_only_requested_attempt_and_records_ci_identity(self):
        self.fixture("attempt-1/old/new", "old/result")
        self.fixture("attempt-2/current/new")
        summary = self.root / "summary.md"
        output = self.root / "reports"
        args = ["criterion_report.py", "--target", "pipeline_throughput", "--criterion-dir", str(self.root / "attempt-2"),
                "--output-dir", str(output), "--outcome", "success"]
        environment = {"GITHUB_STEP_SUMMARY": str(summary), "GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "2",
                       "GITHUB_REPOSITORY": "example/project", "CRITERION_ARTIFACT_URL": "https://github.com/example/project/actions/runs/123/artifacts/456"}
        def command(*args):
            return "a" * 40 if args[0] == "git" else "test toolchain"
        with patch("sys.argv", args), patch.dict("os.environ", environment, clear=True), patch.object(report, "command_output", command):
            self.assertEqual(report.main(), 0)
        path = output / "criterion-pipeline_throughput-aaaaaaaaaaaa-123-attempt-2.md"
        markdown = path.read_text()
        self.assertIn("**COMPLETE**", markdown)
        self.assertIn("| Measured checkout SHA | " + "a" * 40, markdown)
        self.assertIn("https://github.com/example/project/actions/runs/123/attempts/2", markdown)
        self.assertIn("/artifacts/456", markdown)
        self.assertNotIn("old/result", markdown)
        self.assertIn(f"```markdown\n{markdown}```", summary.read_text())

    def test_summary_fence_and_size_limit(self):
        self.assertIn("````markdown\ntext ``` text\n````", report.copyable_summary("text ``` text\n"))
        self.assertIn("exceeds the Actions summary limit", report.copyable_summary("µ" * 600_000))


if __name__ == "__main__":
    unittest.main()
