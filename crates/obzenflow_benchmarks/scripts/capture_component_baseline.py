#!/usr/bin/env python3
# SPDX-License-Identifier: MIT OR Apache-2.0
# SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
# https://obzenflow.dev

"""Preserve complete public-operation Criterion results; perform no measurements."""

import argparse
import datetime
import hashlib
import json
import math
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[3]
CONTRACT = "public-operations-v1"


def command(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline")
    parser.add_argument("output", type=pathlib.Path)
    parser.add_argument("--profile", choices=("test", "bench"), required=True)
    parser.add_argument("--criterion-dir", type=pathlib.Path, default=ROOT / "target/criterion")
    parser.add_argument("--command", required=True, help="Exact successful measurement command")
    parser.add_argument("--suite", choices=("components", "hot-path"), default="components")
    parser.add_argument("--work-json", type=pathlib.Path, help="Required hot-path completed-work census")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", args.baseline):
        parser.error("baseline must be a simple Criterion baseline name")
    groups = ("causal_components", "disk_components") if args.suite == "components" else (
        "record_accounting", "causal_record_work", "journal_record_read",
        "reader_dispatch", "journal_append_cost",
    )
    expected = 23 if args.suite == "components" else 37
    cases = []
    for group in groups:
        for benchmark_path in sorted((args.criterion_dir / group).glob(f"**/{args.baseline}/benchmark.json")):
            directory = benchmark_path.parent
            data = {name: json.loads((directory / f"{name}.json").read_text())
                    for name in ("benchmark", "estimates", "sample")}
            sample = data["sample"]
            if len(sample["iters"]) < 20 or len(sample["iters"]) != len(sample["times"]):
                raise ValueError(f"Incomplete samples: {benchmark_path}")
            if not all(math.isfinite(value) and value > 0 for value in sample["iters"] + sample["times"]):
                raise ValueError(f"Invalid samples: {benchmark_path}")
            cases.append(data)
    names = {case["benchmark"]["full_id"] for case in cases}
    if len(cases) != expected or len(names) != expected:
        raise ValueError(f"Contract requires all {expected} unique cases; found {len(cases)}")
    work = None
    if args.suite == "hot-path":
        if args.work_json is None:
            parser.error("hot-path captures require --work-json")
        work = json.loads(args.work_json.read_text())
        if work.get("measurement_contract") != CONTRACT:
            raise ValueError("Incomparable measurement contract")
        observed = [item["case"] for item in work["cases"]]
        if len(observed) != expected or set(observed) != names:
            raise ValueError("Work census does not match the complete Criterion case inventory")
    # Hash the source set rather than maintaining a list of private implementation
    # files that makes each framework refactor change this tool.
    paths = subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard",
         "--", "Cargo.toml", "Cargo.lock", "crates"], cwd=ROOT
    ).decode().split("\0")
    sources = {ROOT / path for path in paths if path and
               (path == "Cargo.lock" or pathlib.Path(path).suffix in (".rs", ".toml", ".py"))}
    sources = {path for path in sources if path.is_file()}
    rustc = command("rustc", "-Vv")
    report = {
        "measurement_contract": CONTRACT,
        "suite": args.suite,
        "baseline": args.baseline,
        "captured_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "revision": command("git", "rev-parse", "HEAD"),
        "working_tree": command("git", "status", "--short"),
        "rustc": rustc,
        "platform": sys.platform,
        "toolchain_host": next(line.removeprefix("host: ") for line in rustc.splitlines() if line.startswith("host: ")),
        "hardware_model_and_memory": "not inspected",
        "profile": args.profile,
        "features": ["components" if args.suite == "components" else "journal-benchmarks"],
        "async_workers": 2,
        "maximum_blocking_workers": 2,
        "command": args.command,
        "source_sha256": {str(path.relative_to(ROOT)): digest(path) for path in sorted(sources)},
        "cases": sorted(cases, key=lambda case: case["benchmark"]["full_id"]),
    }
    if work is not None:
        report["work_observations"] = work
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Captured {len(cases)} Criterion cases to {args.output}")


if __name__ == "__main__":
    main()
