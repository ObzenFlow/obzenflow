#!/usr/bin/env python3
# SPDX-License-Identifier: MIT OR Apache-2.0
# SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
# https://obzenflow.dev

"""Preserve Criterion's own raw data and provenance; perform no new measurements."""

import argparse
import datetime
import hashlib
import json
import math
import pathlib
import platform
import re
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[3]
GROUPS = (
    "causal_components",
    "disk_components",
    "decode_dispatch",
    "report_discovery",
    "ready_report_handoff",
    "parent_admission",
    "parent_publication",
)


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
    parser.add_argument("--command", required=True, help="Exact successful Cargo benchmark command")
    parser.add_argument("--suite", choices=("components", "supervision", "hot-path"), default="components")
    parser.add_argument("--work-json", type=pathlib.Path, help="Required operation censuses for instrumented suites")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", args.baseline):
        parser.error("baseline must be a simple Criterion baseline name")
    groups = GROUPS if args.suite == "components" else (
        "supervision_discovery", "journal_read_controls", "report_definition_resolution", "journal_append_costs",
    )
    expected = 59 if args.suite == "components" else 30
    if args.suite == "hot-path":
        groups = ("report_accounting", "causal_record_work", "record_reconstruction",
                  "reader_dispatch", "supervisor_fan_in", "journal_append_cost")
        expected = 138
    cases = []
    for group in groups:
        for benchmark_path in sorted((args.criterion_dir / group).glob(f"**/{args.baseline}/benchmark.json")):
            directory = benchmark_path.parent
            data = {
                name: json.loads((directory / f"{name}.json").read_text())
                for name in ("benchmark", "estimates", "sample")
            }
            sample = data["sample"]
            if len(sample["iters"]) < 20 or len(sample["iters"]) != len(sample["times"]):
                raise ValueError(f"Incomplete samples: {benchmark_path}")
            if not all(math.isfinite(value) and value > 0 for value in sample["iters"] + sample["times"]):
                raise ValueError(f"Invalid samples: {benchmark_path}")
            cases.append(data)
    if len(cases) != expected or len({case["benchmark"]["full_id"] for case in cases}) != expected:
        raise ValueError(f"Contract requires all {expected} unique cases; found {len(cases)}")
    work = None
    if args.suite in ("supervision", "hot-path"):
        if args.work_json is None:
            parser.error("instrumented suites require --work-json")
        work = json.loads(args.work_json.read_text())
        names = [item["case"] for item in (work if isinstance(work, list) else work["cases"])]
        if len(names) != expected or set(names) != {case["benchmark"]["full_id"] for case in cases}:
            raise ValueError("Work census does not match the complete Criterion case inventory")
    sources = list((ROOT / "crates/obzenflow_benchmarks/benches/journal_components").glob("*.rs"))
    sources += [ROOT / path for path in (
        "Cargo.lock",
        "crates/obzenflow_benchmarks/Cargo.toml",
        "crates/obzenflow_benchmarks/COMPONENTS.md",
        "crates/obzenflow_infra/src/testing/journal_bench.rs",
        "crates/obzenflow_runtime/src/pipeline/benchmarks.rs",
        "crates/obzenflow_core/src/event/causal.rs",
        "crates/obzenflow_core/src/event/journal_record.rs",
        "crates/obzenflow_core/src/journal/limits.rs",
        "crates/obzenflow_core/src/journal/reader/storage.rs",
        "crates/obzenflow_infra/src/journal/disk/codec/mod.rs",
        "crates/obzenflow_infra/src/journal/disk/identity.rs",
        "crates/obzenflow_infra/src/journal/disk/reader.rs",
        "crates/obzenflow_infra/src/journal/disk/journal.rs",
        "crates/obzenflow_runtime/src/supervised_base/report_reader.rs",
        "crates/obzenflow_runtime/src/supervised_base/publication.rs",
        "crates/obzenflow_runtime/src/pipeline/fsm/journal.rs",
    )]
    if args.suite in ("supervision", "hot-path"):
        sources += list((ROOT / "crates/obzenflow_benchmarks/benches/supervision_selection").glob("*.rs"))
        sources += [ROOT / path for path in (
            "crates/obzenflow_benchmarks/SUPERVISION_SELECTION.md",
            "crates/obzenflow_core/Cargo.toml",
            "crates/obzenflow_core/src/benchmark.rs",
            "crates/obzenflow_core/src/lib.rs",
            "crates/obzenflow_runtime/Cargo.toml",
            "crates/obzenflow_infra/Cargo.toml",
            "crates/obzenflow_infra/src/journal/disk/codec/frame.rs",
            "crates/obzenflow_infra/src/journal/disk/codec/definitions.rs",
            "crates/obzenflow_infra/src/journal/disk/scanner.rs",
            "crates/obzenflow_core/src/event/supervisor_record.rs",
            "crates/obzenflow_core/src/event/payloads/journal_payload.rs",
            "crates/obzenflow_core/src/event/payloads/supervision_report.rs",
            "crates/obzenflow_core/src/journal/archive/manifest.rs",
            "crates/obzenflow_core/src/journal/journal_trait.rs",
            "crates/obzenflow_core/src/journal/storage.rs",
            "crates/obzenflow_core/src/journal/reader/reports.rs",
            "crates/obzenflow_infra/src/journal/disk/codec/routing.rs",
            "crates/obzenflow_infra/src/journal/disk/codec/layout.rs",
            "crates/obzenflow_infra/src/journal/disk/report_reader.rs",
            "crates/obzenflow_infra/src/journal/memory/journal.rs",
            "crates/obzenflow_infra/src/journal/memory/reader.rs",
        )]
    if args.suite == "hot-path":
        sources += list((ROOT / "crates/obzenflow_benchmarks/benches/journal_hot_path").glob("*.rs"))
        sources += [ROOT / path for path in (
            "crates/obzenflow_benchmarks/JOURNAL_HOT_PATH.md",
            "crates/obzenflow_benchmarks/scripts/capture_component_baseline.py",
            "crates/obzenflow_infra/src/journal/disk/codec/benchmark.rs",
            "crates/obzenflow_core/src/event/vector_clock.rs",
            "crates/obzenflow_runtime/src/testing/pipeline.rs",
            "crates/obzenflow_runtime/src/pipeline/fsm/actions.rs",
        )]
    report = {
        "measurement_contract": {"components":"journal-components-v1", "supervision":"supervision-selection-v1", "hot-path":"journal-hot-path-v2"}[args.suite],
        "baseline": args.baseline,
        "captured_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "revision": command("git", "rev-parse", "HEAD"),
        "working_tree": command("git", "status", "--short"),
        "rustc": command("rustc", "-Vv"),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "profile": args.profile,
        "features": ["components" if args.suite == "components" else "supervision-benchmarks"],
        "async_workers": 2,
        "maximum_blocking_workers": 2,
        "command": args.command,
        "source_sha256": {str(path.relative_to(ROOT)): digest(path) for path in sorted(sources)},
        "cases": sorted(cases, key=lambda case: case["benchmark"]["full_id"]),
    }
    if work is not None:
        report["work_observations"] = work
    if args.suite == "hot-path":
        report["maximum_blocking_workers"] = {
            "default": 2,
            "live_supervisor_fan_in": sorted({
                item["input"]["blocking_workers"] for item in work
                if item["input"].get("concurrent_appends")
            }),
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Captured {len(cases)} Criterion cases to {args.output}")


if __name__ == "__main__":
    main()
