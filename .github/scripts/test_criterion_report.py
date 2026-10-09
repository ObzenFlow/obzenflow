# SPDX-License-Identifier: MIT OR Apache-2.0
# SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
# https://obzenflow.dev

import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import criterion_report as report


# One declared case per category; the real inventory is declared by the
# benchmark crate and proven by native runs, not repeated here.
INVENTORY = {
    "journal_hot_path": [
        ("reader_dispatch/full/actual_reader/readers_8", "read", "Concurrent readers: 8; spawn tasks and read 64 records each"),
        ("journal_append_cost/complete_append/ordinary_business_64", "append", "Append/group-append 64 records"),
        ("journal_refresh/append_after_eof", "read_write", "64 append, read and EOF-check pairs"),
    ],
    "journal_components": [("causal_components/journal_clock_restore/w1_p256", "causal", "Restore one journal clock")],
    "validation_boundaries": [
        ("metrics_validation/tail_refresh/data_and_error_64", "observe", "One stage metrics snapshot"),
        ("archive_validation/export/inputs_1000", "archive", "JSONL export of an existing 1,000-input archive"),
    ],
    "idle_cpu_usage": [("idle_process_cpu/window_2s/stages_1", "runtime", "Process CPU time in a 2 s idle window")],
    "per_event_latency": [("3_stage_latency/median_latency", "flow", "Per-run median latency of 100 post-warm-up inputs")],
}
CASES = sum(len(cases) for cases in INVENTORY.values())
# Two suite shards; one selection narrows its target to a case filter.
SHARDS = {"one": ["journal_hot_path", "journal_components", "validation_boundaries"],
          "two": ["idle_cpu_usage", "per_event_latency"]}
FILTERS = {"per_event_latency": "^3_stage_latency/"}
HOST = {"cpu": "AMD EPYC 7763 64-Core Processor", "logical_cpus": 4, "os": "x86_64-linux", "image": "ubuntu24 20261001.1"}


class MeasurementTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[2] / "target", prefix="criterion-report-test-")
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
        self.assertEqual(rows, [report.Measurement("completed_flow/disk_shallow_steady", 12345, 11000, 13000, 0.9, 2)])
        self.assertEqual(report.micros(rows[0].median_ns), "12.345")

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
        self.assertTrue(any("duplicate Criterion ID" in error for error in errors))

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

    def test_markdown_characters_are_escaped(self):
        self.assertEqual(report.cell("new/operation|<script>`\nend"), "new/operation&#124;&lt;script&gt;&#96; end")

    def test_summary_fence_and_size_limit(self):
        self.assertIn("````markdown\ntext ``` text\n````", report.copyable_summary("text ``` text\n"))
        self.assertIn("exceeds the Actions summary limit", report.copyable_summary("µ" * 600_000))
        markdown = "Criterion observations and artefact links\n"
        self.assertIn(f"```markdown\n{markdown}```", report.performance_summary(markdown))

    def test_help_advertises_the_interfaces_capability_detection_probes(self):
        output = io.StringIO()
        with patch("sys.argv", ["criterion_report.py", "--help"]), contextlib.redirect_stdout(output):
            with self.assertRaises(SystemExit) as exit:
                report.main()
        self.assertEqual(exit.exception.code, 0)
        for flag in ("--performance-dir", "--output-dir", "--outcome"):
            self.assertIn(flag, output.getvalue())


class PerformanceReportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[2] / "target", prefix="performance-report-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.performance = self.root / "performance"
        self.performance.mkdir()

    def write(self, path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))

    def phase(self, target):
        shard = next(name for name, targets in SHARDS.items() if target in targets)
        return self.performance / "suite" / shard / target

    def assembly(self, status):
        self.write(self.performance / "assembly.json", {
            "outcome": {"status": status}, "end_to_end_seconds": 840.5,
            "rust": ["rustc 1.93.0 (254b59607 2026-01-19)\nbinary: rustc\nhost: x86_64-unknown-linux-gnu"],
            "reference_public_api_adapters": {"reference-journal-benchmarks": None},
            "stages": [{"stage": "build", "name": "candidate-default", "host": HOST, "elapsed_seconds": 300,
                        "outcome": {"status": "passed"}},
                       {"stage": "suite", "name": "two", "host": HOST, "elapsed_seconds": 280,
                        "outcome": {"status": status}}]})

    def fixture(self):
        self.write(self.performance / "plan.json", {
            "run_id": "this-attempt", "source": {"commit": "measured-commit", "content_sha256": "measured-content"},
            "reference": {"commit": "reference-commit", "rule": "merge_base", "main": "refs/remotes/origin/main"},
            "targets": [{"name": target, "required-features": []} for target in INVENTORY],
            "suite": [{"name": shard, "select": [{"target": target, "cases": FILTERS.get(target)} for target in targets]}
                      for shard, targets in SHARDS.items()]})
        self.assembly("passed")
        self.write(self.performance / "comparison-policy.json", {"sample_size": 40, "warm_up_ms": 1000, "measurement_ms": 3000})
        self.write(self.performance / "qualification.json", {"outcome": {"status": "passed"}})
        self.write(self.performance / "negative-controls.json", {"slowdown": {"outcome": "regressed"},
                   "missing_work_rejected_by_completion_oracle": True, "missing_studio_projection_output_rejected": True})
        for target, cases in INVENTORY.items():
            phase = self.phase(target)
            self.write(phase / "outcome.json", {"outcome": {"status": "passed"}, "elapsed_seconds": 20})
            self.write(phase / "cases.json", [case for case, _, _ in cases])
            self.write(phase / "selected.json", [case for case, _, _ in cases])
            self.write(phase / "declarations.json", [{"case": case, "category": category, "timed": timed}
                                                      for case, category, timed in cases])
            for index, (case, _, _) in enumerate(cases):
                directory = phase / "criterion" / str(index) / "new"
                self.write(directory / "benchmark.json", {"full_id": case})
                self.write(directory / "estimates.json", {"median": {"point_estimate": 3000, "confidence_interval": {
                    "lower_bound": 2900, "upper_bound": 3100, "confidence_level": 0.95}}})
                self.write(directory / "sample.json", {"times": [3000, 6000], "iters": [1, 2]})
        case = "reader_dispatch/full/actual_reader/readers_8"
        comparison = {"suites": {"journal_hot_path": {"cases": [case]}}, "decisions": {case: {"outcome": "passed"}}}
        for phase, point in (("before", 1000), ("candidate", 1100), ("after", 1050)):
            comparison[phase] = {case: {"estimate": {"point_estimate": point, "confidence_interval": {
                "lower_bound": point - 10, "upper_bound": point + 10, "confidence_level": 0.95}}}}
        self.write(self.performance / "comparison.json", comparison)

    def declarations(self, target):
        return self.phase(target) / "declarations.json"

    def test_declared_categories_group_cases_and_comparison_uses_identified_candidate(self):
        self.fixture()
        markdown, errors = report.render_performance(self.performance, "success")
        self.assertFalse(errors)
        self.assertIn(f"**{CASES} cases**", markdown)
        for title, _ in report.CATEGORIES.values():
            self.assertEqual(markdown.count("## " + title + "\n"), 1)
        self.assertNotIn("## Uncategorised", markdown)
        self.assertIn("| 1 [0.99–1.01; 95%] | 1.1 [1.09–1.11; 95%] | 1.05 [1.04–1.06; 95%] | passed |", markdown)
        self.assertIn("Process CPU time in a 2 s idle window", markdown)
        self.assertIn("measured-commit", markdown)
        self.assertIn("measured-content", markdown)
        self.assertIn("| Reference SHA | reference-commit |", markdown)
        self.assertIn("| Reference selection | merge base with main (where this change started) |", markdown)
        self.assertIn("| Reference public API adapter | none |", markdown)
        self.assertIn("| Rust | rustc 1.93.0 (254b59607 2026-01-19) |", markdown)
        self.assertIn("| End-to-end wall time | 840.50 s from planning to the last stage |", markdown)
        self.assertIn("| build | candidate-default | AMD EPYC 7763 64-Core Processor | 4 | ubuntu24 20261001.1 | 300.00 | passed |", markdown)
        self.assertIn("| two | per_event_latency | default | ^3_stage_latency/ | 20.00 | 1/1 | passed |", markdown)
        self.assertIn("| one | journal_hot_path | default | all | 20.00 | 3/3 | passed |", markdown)
        self.assertIn("Result: **passed**.", markdown)
        self.assertLess(len(report.performance_summary(markdown).encode()), 1_000_000)

    def test_missing_case_is_incomplete_even_when_measurement_process_succeeded(self):
        self.fixture()
        (self.phase("per_event_latency") / "criterion/0/new/sample.json").unlink()
        markdown, errors = report.render_performance(self.performance, "success")
        self.assertTrue(errors)
        self.assertIn("**INCOMPLETE**", markdown)
        self.assertIn(f"**{CASES - 1} cases**", markdown)
        self.assertIn("two/per_event_latency: measured case IDs differ from the shard's selection", markdown)

    def test_undeclared_or_miscategorised_cases_stay_visible_as_evidence_errors(self):
        self.fixture()
        self.write(self.declarations("per_event_latency"), [])
        self.write(self.declarations("journal_components"), [{"case": "causal_components/journal_clock_restore/w1_p256",
                                                               "category": "codec", "timed": "Restore one journal clock"}])
        markdown, errors = report.render_performance(self.performance, "success")
        self.assertIn("**INCOMPLETE**", markdown)
        self.assertIn("per_event_latency: 3_stage_latency/median_latency: no case declaration.", errors)
        self.assertTrue(any("unknown category 'codec'" in error for error in errors))
        self.assertIn("## Uncategorised", markdown)
        self.assertIn("| per_event_latency | 3_stage_latency/median_latency | No case declaration |", markdown)

    def test_failed_gate_keeps_full_suite_and_native_decision(self):
        self.fixture()
        self.write(self.performance / "qualification.json", {"outcome": {"status": "failed", "detail": "regression found"}})
        self.assembly("failed")
        markdown, errors = report.render_performance(self.performance, "failure")
        self.assertFalse(errors)
        self.assertIn("**FAILED**", markdown)
        self.assertIn("Result: **not passed**. regression found", markdown)
        self.assertIn(f"**{CASES} cases**", markdown)

    def test_unavailable_target_keeps_its_build_outcome_and_other_rows(self):
        self.fixture()
        phase = self.phase("idle_cpu_usage")
        for name in ("cases.json", "selected.json", "declarations.json", "criterion/0/new/benchmark.json",
                     "criterion/0/new/estimates.json", "criterion/0/new/sample.json"):
            (phase / name).unlink()
        self.write(phase / "outcome.json", {"outcome": {"status": "failed", "detail": "candidate-default-build: compilation rejected"},
                                            "elapsed_seconds": 0})
        self.assembly("failed")
        markdown, errors = report.render_performance(self.performance, "failure")
        self.assertIn("two/idle_cpu_usage: no case selection; failed: candidate-default-build: compilation rejected", errors)
        self.assertIn("| two | idle_cpu_usage | default | all | 0.00 | 0/? | failed: candidate-default-build: compilation rejected |", markdown)
        self.assertIn(f"**{CASES - 1} cases**", markdown)
        self.assertIn("**FAILED**", markdown)

    def test_missing_stage_evidence_is_incomplete_without_a_previous_run(self):
        self.fixture()
        (self.performance / "assembly.json").unlink()
        markdown, errors = report.render_performance(self.performance, "failure")
        self.assertIn("**INCOMPLETE**", markdown)
        self.assertTrue(any(error.startswith("assembly.json:") for error in errors))
        self.assertIn("| End-to-end wall time | unavailable |", markdown)

    def test_early_failure_still_writes_copyable_report_and_does_not_find_previous_run(self):
        self.fixture()  # Valid but unrelated evidence must not be selected.
        output = self.root / "markdown"
        summary = self.root / "summary.md"
        with patch("sys.argv", ["criterion_report.py", "--performance-dir", "", "--output-dir", str(output), "--outcome", "failure"]), \
             patch.dict("os.environ", {"GITHUB_STEP_SUMMARY": str(summary)}, clear=True):
            self.assertEqual(report.main(), 1)
        markdown = next(output.glob("*.md")).read_text()
        self.assertIn("**INCOMPLETE**", markdown)
        self.assertIn("no previous run is substituted", markdown)
        self.assertNotIn("measured-commit", markdown)
        self.assertIn("| Run URL | unavailable (local run) |", markdown)
        self.assertIn("Copy the complete report as Markdown", summary.read_text())
        self.assertIn(f"```markdown\n{markdown}```", summary.read_text())

    def test_main_records_ci_identity_for_the_requested_attempt(self):
        self.fixture()
        output = self.root / "markdown"
        environment = {"GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "2", "GITHUB_REPOSITORY": "example/project",
                       "CRITERION_ARTIFACT_URL": "https://github.com/example/project/actions/runs/123/artifacts/456"}
        with patch("sys.argv", ["criterion_report.py", "--performance-dir", str(self.performance), "--output-dir", str(output),
                                "--outcome", "success"]), patch.dict("os.environ", environment, clear=True):
            self.assertEqual(report.main(), 0)
        markdown = (output / "performance-123-attempt-2.md").read_text()
        self.assertIn("https://github.com/example/project/actions/runs/123/attempts/2", markdown)
        self.assertIn("/artifacts/456", markdown)
        self.assertIn("**PASSED**", markdown)


if __name__ == "__main__":
    unittest.main()
