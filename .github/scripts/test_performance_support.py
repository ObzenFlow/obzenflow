# SPDX-License-Identifier: MIT OR Apache-2.0
# SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
# https://obzenflow.dev

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import performance_support as support


REPOSITORY = Path(__file__).resolve().parents[2]


def run(root, *args):
    return subprocess.run(["git", "-c", "user.name=fixture", "-c", "user.email=fixture@invalid", *args],
                          cwd=root, check=True, capture_output=True, text=True).stdout.strip()


class ResolveTests(unittest.TestCase):
    """B4: only commits the dispatched ref contains may run with its cache scope."""

    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="revision-trust-")
        root = cls.root = Path(cls.temp.name)
        run(root, "init", "-q")

        def commit(message):
            run(root, "commit", "-q", "--allow-empty", "-m", message)
            return run(root, "rev-parse", "HEAD")

        cls.released = commit("released")
        run(root, "tag", "-a", "v0.2.6", "-m", "release")
        cls.merged = commit("merged")
        run(root, "update-ref", "refs/remotes/origin/main", cls.merged)
        run(root, "checkout", "-q", "-b", "topic")
        cls.feature = commit("feature")
        run(root, "update-ref", "refs/remotes/origin/topic", cls.feature)
        # Present locally but contained in no ref: models a fork or PR-head object.
        cls.foreign = run(root, "commit-tree", "-m", "fork", "-p", cls.merged, run(root, "rev-parse", "HEAD^{tree}"))

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def test_contained_revisions_resolve_to_full_commits(self):
        for dispatch, requested, expected in [
            ("refs/heads/main", "", self.merged),
            ("refs/heads/main", "   ", self.merged),
            ("refs/heads/main", "0.2.6", self.released),
            ("refs/heads/main", " v0.2.6 ", self.released),
            ("refs/heads/main", self.released[:7], self.released),
            ("refs/heads/main", self.released.upper(), self.released),
            ("refs/heads/topic", "", self.feature),
            ("refs/heads/topic", self.feature, self.feature),
            ("refs/heads/topic", self.merged, self.merged),
            ("refs/tags/v0.2.6", "", self.released),
        ]:
            with self.subTest(dispatch=dispatch, requested=requested):
                self.assertEqual(support.resolve(self.root, requested, dispatch), expected)

    def test_uncontained_unknown_and_invalid_revisions_are_rejected(self):
        for dispatch, requested, message in [
            ("refs/heads/main", self.feature, "not contained in refs/heads/main"),
            ("refs/heads/main", self.foreign, "not contained"),
            ("refs/heads/topic", self.foreign, "not contained"),
            ("refs/heads/main", "f" * 40, "not an unambiguous commit"),
            ("refs/heads/main", "0.2.7", "not an unambiguous commit"),
            ("refs/heads/main", "main", "revision must be"),
            ("refs/heads/main", "refs/tags/v0.2.6", "revision must be"),
            ("refs/heads/main", "a" * 41, "revision must be"),
        ]:
            with self.subTest(dispatch=dispatch, requested=requested):
                with self.assertRaisesRegex(support.Rejected, message):
                    support.resolve(self.root, requested, dispatch)

    def test_rejection_writes_summary_and_no_commit_output(self):
        with tempfile.TemporaryDirectory() as directory:
            output, summary = Path(directory, "output"), Path(directory, "summary")
            environment = {"REQUESTED_REVISION": self.foreign, "DISPATCH_REF": "refs/heads/main",
                           "GITHUB_OUTPUT": str(output), "GITHUB_STEP_SUMMARY": str(summary)}
            with patch("sys.argv", ["performance_support.py", "resolve"]), patch.dict(os.environ, environment, clear=True), \
                 patch.object(support.Path, "cwd", return_value=self.root), patch("builtins.print"):
                self.assertEqual(support.main(), 1)
            self.assertFalse(output.exists())
            self.assertIn("not contained in refs/heads/main", summary.read_text())


class DetectTests(unittest.TestCase):
    """B3: support is detected from interfaces before any benchmark compilation."""

    FORMATTER_HELP = "usage: criterion_report.py --performance-dir P --output-dir O --outcome {success,failure}"
    XTASK_HELP = "cargo xtask performance <stage> --run-id <id>\n\nStages: plan, build, qualify, measure, assemble\nplan writes ..."

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="performance-support-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for path in support.CONFIGURATION:
            self.write(path, "version = 1\n")
        self.write(support.FORMATTER, "# formatter\n")
        self.calls = []

    def write(self, path, text):
        (self.root / path).parent.mkdir(parents=True, exist_ok=True)
        (self.root / path).write_text(text)

    def probe(self, formatter=(0, FORMATTER_HELP), xtask=(0, XTASK_HELP)):
        def run(args, root):
            self.calls.append(args)
            self.assertEqual(root, self.root)
            return formatter if args[1:] == [support.FORMATTER, "--help"] else xtask
        return run

    def test_untagged_checkout_with_interfaces_is_supported(self):
        detection = support.detect(self.root, self.probe())
        self.assertEqual(detection.outcome, "supported")
        self.assertEqual(self.calls[-1], ["cargo", "xtask", "performance", "--help"])

    def test_absent_interfaces_are_unsupported_before_any_probe(self):
        (self.root / ".config/performance-shards.toml").unlink()
        self.assertEqual(support.detect(self.root, self.probe()), support.Detection(
            "unsupported", ".config/performance-shards.toml is absent"))
        self.assertEqual(self.calls, [])

    def test_missing_or_older_formatter_is_unsupported(self):
        older = (0, "usage: criterion_report.py --target T --criterion-dir C --output-dir O --outcome X")
        self.assertEqual(support.detect(self.root, self.probe(formatter=older)).outcome, "unsupported")
        self.assertIn("--performance-dir", support.detect(self.root, self.probe(formatter=older)).detail)
        (self.root / support.FORMATTER).unlink()
        self.assertEqual(support.detect(self.root, self.probe()).outcome, "unsupported")

    def test_xtask_without_every_stage_is_unsupported(self):
        for xtask, expected in [
            ((1, "error: unknown xtask command: performance --help\n"), "does not provide the performance stages"),
            ((0, "Stages: plan, build, measure\n"), "does not advertise qualify, assemble"),
        ]:
            with self.subTest(expected=expected):
                detection = support.detect(self.root, self.probe(xtask=xtask))
                self.assertEqual(detection.outcome, "unsupported")
                self.assertIn(expected, detection.detail)

    def test_malformed_configuration_and_probe_errors_are_not_relabelled_unsupported(self):
        for prepare, probe, expected in [
            (lambda: self.write(".config/performance-policy.toml", "version = [\n"), self.probe(), "malformed"),
            (lambda: None, self.probe(formatter=(1, "ImportError: no module")), "ImportError"),
            (lambda: None, self.probe(xtask=(101, "error[E0425]: cannot find value")), "E0425"),
            (lambda: None, self.probe(xtask=(127, "No such file or directory: 'cargo'")), "cargo"),
        ]:
            with self.subTest(expected=expected):
                self.setUp()
                prepare()
                detection = support.detect(self.root, probe)
                self.assertEqual(detection.outcome, "failed")
                self.assertIn(expected, detection.detail)

    def test_unsupported_report_needs_no_selected_checkout_helper(self):
        (self.root / support.FORMATTER).unlink()
        for path in support.CONFIGURATION:
            (self.root / path).unlink()
        run(self.root, "init", "-q")
        run(self.root, "commit", "-q", "--allow-empty", "-m", "old release")
        sha = run(self.root, "rev-parse", "HEAD")
        with tempfile.TemporaryDirectory() as directory:
            output, summary, reports = Path(directory, "output"), Path(directory, "summary"), Path(directory, "reports")
            environment = {"GITHUB_OUTPUT": str(output), "GITHUB_STEP_SUMMARY": str(summary), "GITHUB_RUN_ID": "7",
                           "GITHUB_RUN_ATTEMPT": "1", "REQUESTED_REVISION": "0.2.5", "DISPATCH_REF": "refs/heads/main"}
            with patch("sys.argv", ["performance_support.py", "detect", "--report-dir", str(reports)]), \
                 patch.dict(os.environ, environment, clear=True), \
                 patch.object(support.Path, "cwd", return_value=self.root), patch("builtins.print"):
                self.assertEqual(support.main(), 1)
            self.assertEqual(output.read_text(), "outcome=unsupported\n")
            markdown = (reports / "performance-7-attempt-1.md").read_text()
            self.assertIn("**UNSUPPORTED**. No measurements ran.", markdown)
            self.assertIn(f"| Measured checkout SHA | {sha} |", markdown)
            self.assertIn("| Requested revision | 0.2.5 |", markdown)
            self.assertIn(".config/performance-policy.toml is absent", markdown)
            self.assertEqual(summary.read_text(), markdown)

    def test_failed_detection_report_is_labelled_distinctly(self):
        markdown = support.report(support.Detection("failed", "cargo xtask performance --help exited 101"), "a" * 40, {})
        self.assertIn("**DETECTION FAILED**", markdown)
        self.assertIn("not an unsupported result", markdown)
        self.assertNotIn("UNSUPPORTED", markdown)

    def test_current_checkout_provides_the_probed_interfaces(self):
        def run(args, root):
            if args[0] == "cargo":
                return 0, self.XTASK_HELP  # The xtask help contract has its own Rust test.
            return support.probe(args, root)
        self.assertEqual(support.detect(REPOSITORY, run).outcome, "supported")


class PruneTests(unittest.TestCase):
    """A4: cache only what a later run can reuse."""

    def test_stale_references_and_workspace_artefacts_are_pruned(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            current, stale = "a" * 64, "b" * 64
            for identity in (current, stale):
                (root / "target/validation-reference" / identity / "source").mkdir(parents=True)
            (root / "target/validation-reference/not-an-identity").mkdir()
            deps = root / "target/validation-candidate/release/deps"
            deps.mkdir(parents=True)
            fingerprint = root / "target/validation-candidate/release/.fingerprint/obzenflow_core-1a2b"
            fingerprint.mkdir(parents=True)
            for name in ("libobzenflow_core-1a2b.rlib", "journal_hot_path-3c4d", "journal_hot_path-3c4d.d",
                         "libserde-5e6f.rlib", "libc-7a8b.rlib"):
                (deps / name).write_text("")
            debug = root / "target/debug/deps"
            debug.mkdir(parents=True)
            (debug / "xtask-9c0d").write_text("")
            (debug / "libtoml-1e2f.rlib").write_text("")
            performance = root / "target/test-runs/run/performance"
            (performance / "build/reference-journal-benchmarks").mkdir(parents=True)
            (performance / "build/reference-journal-benchmarks/reference-build.json").write_text(
                json.dumps({"source_tree_sha256": current}))
            support.prune(root, performance, names={"obzenflow_core", "journal_hot_path", "xtask"})
            self.assertTrue((root / "target/validation-reference" / current).is_dir())
            self.assertFalse((root / "target/validation-reference" / stale).exists())
            self.assertTrue((root / "target/validation-reference/not-an-identity").is_dir())
            self.assertEqual(sorted(path.name for path in deps.iterdir()), ["libc-7a8b.rlib", "libserde-5e6f.rlib"])
            self.assertFalse(fingerprint.exists())
            self.assertEqual([path.name for path in debug.iterdir()], ["libtoml-1e2f.rlib"])

    def test_unknown_reference_identity_prunes_no_reference_build(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identity = root / "target/validation-reference" / ("c" * 64)
            identity.mkdir(parents=True)
            support.prune(root, None, names=set())
            self.assertTrue(identity.is_dir())


if __name__ == "__main__":
    sys.exit(unittest.main())
