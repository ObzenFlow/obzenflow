#!/usr/bin/env python3
# SPDX-License-Identifier: MIT OR Apache-2.0
# SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
# https://obzenflow.dev
"""Trusted steps of the manual Performance workflow (FLOWIP-080v B3/B4/B7).

The workflow copies this file out of the dispatched ref before switching to the
selected revision, so resolution, capability detection and unsupported reports
never depend on helpers from the checkout they judge.
"""

import argparse
from dataclasses import dataclass
import html
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tomllib


VERSION = re.compile(r"v?([0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?)")
SHA = re.compile(r"[0-9a-fA-F]{7,40}")
IDENTITY = re.compile(r"[0-9a-f]{64}")
CONFIGURATION = (".config/performance-policy.toml", ".config/performance-shards.toml")
FORMATTER = ".github/scripts/criterion_report.py"
FORMATTER_FLAGS = ("--performance-dir", "--output-dir", "--outcome")
# The workflow runs each of these on its own runner (B7).
STAGES = ("plan", "build", "qualify", "measure", "assemble")


class Rejected(Exception):
    """A requested revision this workflow refuses to measure."""


def git(root, *args):
    return subprocess.run(["git", *args], cwd=root, capture_output=True, text=True)


def resolve(root, requested, dispatch_ref):
    """Resolve only among this repository's fetched branches and tags, and require
    the dispatched ref to contain the result: a run writes caches in that ref's
    scope, so measured code may only write where its ref already contains it."""
    requested = requested.strip()
    scope = ("refs/remotes/origin/" + dispatch_ref.removeprefix("refs/heads/")
             if dispatch_ref.startswith("refs/heads/") else dispatch_ref)
    if not requested:
        spec = scope
    elif version := VERSION.fullmatch(requested):
        spec = f"refs/tags/v{version[1]}"
    elif SHA.fullmatch(requested):
        spec = requested
    else:
        raise Rejected("revision must be a version (0.2.6 or v0.2.6), a commit SHA (7–40 hex characters), "
                       "or blank for the dispatched ref")
    found = git(root, "rev-parse", "--verify", "--quiet", f"{spec}^{{commit}}")
    if found.returncode != 0:
        raise Rejected(f"{requested or dispatch_ref} is not an unambiguous commit in this repository's branches or tags")
    sha = found.stdout.strip()
    contained = git(root, "merge-base", "--is-ancestor", sha, scope)
    if contained.returncode == 1:
        raise Rejected(f"{sha} is not contained in {dispatch_ref}; run Performance from a branch or tag that contains it")
    if contained.returncode != 0:
        raise RuntimeError(f"could not check containment in {dispatch_ref}: {contained.stderr.strip()}")
    return sha


@dataclass(frozen=True)
class Detection:
    outcome: str  # supported, unsupported or failed
    detail: str


def probe(args, root):
    try:
        result = subprocess.run(args, cwd=root, capture_output=True, text=True)
    except OSError as error:
        return 127, str(error)
    return result.returncode, result.stdout + result.stderr


def tail(output):
    return " ".join(output.strip().splitlines()[-3:]) or "no output"


def detect(root, run=probe):
    """Support comes from the selected checkout's interfaces, never its tag, SHA
    or ancestry. Absent interfaces are unsupported; probes that cannot run or
    malformed configuration remain detection failures."""
    for path in CONFIGURATION:
        file = root / path
        if not file.is_file():
            return Detection("unsupported", f"{path} is absent")
        try:
            tomllib.loads(file.read_text())
        except (tomllib.TOMLDecodeError, UnicodeDecodeError) as error:
            return Detection("failed", f"{path} is malformed: {error}")
    if not (root / FORMATTER).is_file():
        return Detection("unsupported", f"{FORMATTER} is absent")
    code, output = run([sys.executable, FORMATTER, "--help"], root)
    if code != 0:
        return Detection("failed", f"{FORMATTER} --help exited {code}: {tail(output)}")
    missing = [flag for flag in FORMATTER_FLAGS if flag not in output]
    if missing:
        return Detection("unsupported", f"{FORMATTER} does not support {', '.join(missing)}")
    code, output = run(["cargo", "xtask", "performance", "--help"], root)
    stages = next((line.removeprefix("Stages:") for line in output.splitlines() if line.startswith("Stages:")), None)
    if stages is None:
        # An older validator rejects the unknown command; that is absence, not failure.
        if code != 0 and "unknown xtask command" not in output:
            return Detection("failed", f"cargo xtask performance --help exited {code}: {tail(output)}")
        return Detection("unsupported", "cargo xtask does not provide the performance stages")
    if code != 0:
        return Detection("failed", f"cargo xtask performance --help exited {code}: {tail(output)}")
    missing = [stage for stage in STAGES if stage not in (name.strip() for name in stages.split(","))]
    if missing:
        return Detection("unsupported", f"cargo xtask performance does not advertise {', '.join(missing)}")
    return Detection("supported", "native performance stages and report formatter are available")


def cell(value):
    return html.escape(str(value), quote=False).replace("|", "&#124;").replace("`", "&#96;").replace("\n", " ")


def report(detection, sha, environment):
    """The dispatch workflow's own explanation when measurement cannot start."""
    title = "UNSUPPORTED" if detection.outcome == "unsupported" else "DETECTION FAILED"
    explanation = ("The selected checkout does not provide the native performance interfaces this workflow runs."
                   if detection.outcome == "unsupported" else
                   "Capability detection did not complete. This is not an unsupported result; see the step log.")
    rows = {
        "Measured checkout SHA": sha,
        "Requested revision": environment.get("REQUESTED_REVISION", "").strip() or "blank (latest on the dispatched ref)",
        "Dispatched ref": environment.get("DISPATCH_REF", "unknown"),
        "Run / attempt": f"{environment.get('GITHUB_RUN_ID', 'local')} / {environment.get('GITHUB_RUN_ATTEMPT', '1')}",
    }
    lines = ["# Performance report", "", f"Native outcome: **{title}**. No measurements ran.", "",
             "| Context | Value |", "| --- | --- |"]
    lines += [f"| {cell(key)} | {cell(value)} |" for key, value in rows.items()]
    lines += ["", explanation, "", f"Detail: {cell(detection.detail)}"]
    return "\n".join(lines) + "\n"


def prune(root, performance_dir, names=None):
    """Keep what a later run can reuse: the current reference build and third-party
    artefacts. Workspace crates rebuild after every fresh checkout, and other
    reference identities are stale. An unknown identity prunes no references."""
    removed = []
    records = sorted(performance_dir.glob("build/*/reference-build.json")) if performance_dir else []
    current = {json.loads(record.read_text()).get("source_tree_sha256") for record in records} - {None}
    references = root / "target/validation-reference"
    if current and references.is_dir():
        for entry in references.iterdir():
            if entry.is_dir() and IDENTITY.fullmatch(entry.name) and entry.name not in current:
                shutil.rmtree(entry)
                removed.append(entry)
    if names is None:
        metadata = json.loads(subprocess.run(
            ["cargo", "metadata", "--no-deps", "--format-version", "1", "--offline"],
            cwd=root, capture_output=True, text=True, check=True).stdout)
        names = {name.replace("-", "_") for package in metadata["packages"]
                 for name in [package["name"], *(target["name"] for target in package["targets"])]}
    for target in ("target/validation-candidate", "target"):
        for profile in ("debug", "release"):
            for kind in ("deps", "build", ".fingerprint", "incremental"):
                directory = root / target / profile / kind
                if not directory.is_dir():
                    continue
                for entry in directory.iterdir():
                    stem = entry.name.removeprefix("lib").split(".")[0].rpartition("-")[0]
                    if stem.replace("-", "_") not in names:
                        continue
                    if entry.is_dir() and not entry.is_symlink():
                        shutil.rmtree(entry)
                    else:
                        entry.unlink()
                    removed.append(entry)
    return removed


def append(name, text):
    path = os.environ.get(name)
    if path:
        with open(path, "a", encoding="utf-8") as file:
            file.write(text)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("resolve", help="Resolve REQUESTED_REVISION within DISPATCH_REF")
    detect_command = commands.add_parser("detect", help="Detect native performance support in the current checkout")
    detect_command.add_argument("--report-dir", type=Path, required=True)
    prune_command = commands.add_parser("prune", help="Prune non-reusable compilation output before caching")
    prune_command.add_argument("--performance-dir", default="")
    args = parser.parse_args()
    root = Path.cwd()
    environment = os.environ

    if args.command == "resolve":
        requested, dispatch = environment.get("REQUESTED_REVISION", ""), environment["DISPATCH_REF"]
        try:
            sha = resolve(root, requested, dispatch)
        except Rejected as rejection:
            append("GITHUB_STEP_SUMMARY", f"Performance revision rejected: requested `{cell(requested.strip() or 'blank')}` "
                                          f"from `{cell(dispatch)}`. {cell(rejection)}\n")
            print(f"::error::{rejection}")
            return 1
        append("GITHUB_OUTPUT", f"sha={sha}\n")
        append("GITHUB_STEP_SUMMARY", f"Measured revision `{sha}` (requested `{cell(requested.strip() or 'blank')}`) is contained in "
                                      f"`{cell(dispatch)}`; caches from this run are scoped to that ref.\n")
        print(f"Measurement revision {requested.strip() or '(latest)'} resolved to {sha}, contained in {dispatch}")
        return 0

    if args.command == "detect":
        detection = detect(root)
        append("GITHUB_OUTPUT", f"outcome={detection.outcome}\n")
        print(f"Performance support: {detection.outcome}: {detection.detail}")
        if detection.outcome == "supported":
            return 0
        sha = git(root, "rev-parse", "HEAD").stdout.strip() or "unknown"
        text = report(detection, sha, environment)
        args.report_dir.mkdir(parents=True, exist_ok=True)
        identity = f"{environment.get('GITHUB_RUN_ID', 'local')}-attempt-{environment.get('GITHUB_RUN_ATTEMPT', '1')}"
        (args.report_dir / f"performance-{identity}.md").write_text(text, encoding="utf-8")
        append("GITHUB_STEP_SUMMARY", text)
        print(f"::error::{detection.detail}")
        return 1

    removed = prune(root, Path(args.performance_dir) if args.performance_dir else None)
    print(f"Pruned {len(removed)} non-reusable compilation entries before caching")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
