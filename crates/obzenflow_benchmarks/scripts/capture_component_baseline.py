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
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[3]
GROUPS = (
    "causal_components",
    "disk_components",
    "decode_dispatch",
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
    parser.add_argument("--command", required=True, help="Exact successful measurement command")
    parser.add_argument("--suite", choices=("components", "hot-path", "supervision-capacity", "projection-capacity"), default="components")
    parser.add_argument("--work-json", type=pathlib.Path, help="Required operation censuses for instrumented suites")
    parser.add_argument("--case", action="append", default=[], help="Exact selected capacity case; repeat for each case")
    parser.add_argument("--executable", type=pathlib.Path, help="Preserved executable used for capacity measurements")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", args.baseline):
        parser.error("baseline must be a simple Criterion baseline name")
    capacity = args.suite.endswith("-capacity")
    if args.case and not capacity:
        parser.error("partial capture is restricted to explicitly selected capacity experiments")
    if capacity and (args.executable is None or not args.executable.is_file()):
        parser.error("capacity capture requires the preserved --executable")
    groups = GROUPS if args.suite == "components" else (
        "record_accounting", "causal_record_work", "record_reconstruction",
        "reader_dispatch", "journal_append_cost",
    )
    expected = 33 if args.suite == "components" else 75
    if capacity:
        groups = ("journal_projection_capacity",) if args.suite == "projection-capacity" else (
            "parent_lifecycle_application", "supervision_phase_costs", "parent_publication_pressure",
        )
        expected = 18 if args.suite == "projection-capacity" else 80
        if args.case:
            if len(set(args.case)) != len(args.case):
                parser.error("duplicate selected case")
            expected = len(args.case)
    cases = []
    for group in groups:
        for benchmark_path in sorted((args.criterion_dir / group).glob(f"**/{args.baseline}/benchmark.json")):
            directory = benchmark_path.parent
            data = {
                name: json.loads((directory / f"{name}.json").read_text())
                for name in ("benchmark", "estimates", "sample")
            }
            sample = data["sample"]
            if len(sample["iters"]) < (40 if capacity else 20) or len(sample["iters"]) != len(sample["times"]):
                raise ValueError(f"Incomplete samples: {benchmark_path}")
            if not all(math.isfinite(value) and value > 0 for value in sample["iters"] + sample["times"]):
                raise ValueError(f"Invalid samples: {benchmark_path}")
            cases.append(data)
    if len(cases) != expected or len({case["benchmark"]["full_id"] for case in cases}) != expected:
        raise ValueError(f"Contract requires all {expected} unique cases; found {len(cases)}")
    if args.case and set(args.case) != {case["benchmark"]["full_id"] for case in cases}:
        raise ValueError("Measured cases differ from the explicitly selected scope")
    work = None
    if args.suite == "hot-path" or capacity:
        if args.work_json is None:
            parser.error("instrumented suites require --work-json")
        work = json.loads(args.work_json.read_text())
        if capacity and work.get("completed_selected_cases") is not True:
            raise ValueError("Incomplete capacity invocation cannot be captured as a completed baseline")
        names = [item["case"] for item in (work if isinstance(work, list) else work["cases"])]
        if len(names) != expected or set(names) != {case["benchmark"]["full_id"] for case in cases}:
            raise ValueError("Work census does not match the complete Criterion case inventory")
    sources = list((ROOT / "crates/obzenflow_benchmarks/benches/journal_components").glob("*.rs"))
    sources += [ROOT / path for path in (
        "Cargo.lock",
        "crates/obzenflow_benchmarks/Cargo.toml",
        "crates/obzenflow_benchmarks/README.md",
        "crates/obzenflow_infra/src/testing/journal_bench.rs",
        "crates/obzenflow_core/src/event/causal.rs",
        "crates/obzenflow_core/src/event/journal_record.rs",
        "crates/obzenflow_core/src/journal/limits.rs",
        "crates/obzenflow_core/src/journal/reader/storage.rs",
        "crates/obzenflow_infra/src/journal/disk/codec/mod.rs",
        "crates/obzenflow_infra/src/journal/disk/identity.rs",
        "crates/obzenflow_infra/src/journal/disk/reader.rs",
        "crates/obzenflow_infra/src/journal/disk/journal.rs",
        "crates/obzenflow_runtime/src/supervised_base/publication.rs",
    )]
    if args.suite == "hot-path":
        sources += [ROOT / path for path in (
            "crates/obzenflow_core/Cargo.toml",
            "crates/obzenflow_core/src/benchmark.rs",
            "crates/obzenflow_core/src/lib.rs",
            "crates/obzenflow_runtime/Cargo.toml",
            "crates/obzenflow_infra/Cargo.toml",
            "crates/obzenflow_infra/src/journal/disk/codec/frame.rs",
            "crates/obzenflow_infra/src/journal/disk/codec/definitions.rs",
            "crates/obzenflow_infra/src/journal/disk/scanner.rs",
            "crates/obzenflow_core/src/event/payloads/journal_payload.rs",
            "crates/obzenflow_core/src/journal/archive/manifest.rs",
            "crates/obzenflow_core/src/journal/journal_trait.rs",
            "crates/obzenflow_core/src/journal/storage.rs",
            "crates/obzenflow_infra/src/journal/disk/codec/routing.rs",
            "crates/obzenflow_infra/src/journal/disk/codec/layout.rs",
            "crates/obzenflow_infra/src/journal/memory/journal.rs",
            "crates/obzenflow_infra/src/journal/memory/reader.rs",
        )]
    if capacity:
        sources += list((ROOT / "crates/obzenflow_benchmarks/src/support").rglob("*.rs"))
        sources += [ROOT / path for path in (
            "crates/obzenflow_benchmarks/benches/supervision_capacity.rs",
            "crates/obzenflow_benchmarks/benches/projection_capacity.rs",
            "crates/obzenflow_runtime/src/pipeline/benchmark.rs",
            "crates/obzenflow_runtime/src/pipeline/benchmark/pressure.rs",
            "crates/obzenflow_runtime/src/metrics/benchmark.rs",
            "crates/obzenflow_runtime/src/metrics/buffer.rs",
            "crates/obzenflow_runtime/src/metrics/fsm.rs",
            "crates/obzenflow_runtime/src/pipeline/fsm/actions.rs",
            "crates/obzenflow_runtime/src/pipeline/fsm/transitions.rs",
            "crates/obzenflow_runtime/src/stages/common/stage_lifecycle.rs",
            "crates/obzenflow_infra/src/benchmark.rs",
            "crates/obzenflow_infra/src/benchmark/studio.rs",
            "crates/obzenflow_infra/src/testing/studio_capacity.rs",
            "crates/obzenflow_infra/src/web/endpoints/studio/mod.rs",
            "crates/obzenflow_infra/src/web/endpoints/studio/stream.rs",
            "crates/obzenflow_benchmarks/scripts/capture_component_baseline.py",
        )]
    if args.suite == "hot-path":
        sources += list((ROOT / "crates/obzenflow_benchmarks/benches/journal_hot_path").glob("*.rs"))
        sources += list((ROOT / "crates/obzenflow_benchmarks/src/support").glob("*.rs"))
        sources += [ROOT / "crates/obzenflow_infra/src/benchmark.rs", ROOT / "crates/obzenflow_infra/src/lib.rs", ROOT / "crates/obzenflow_benchmarks/src/lib.rs"]
        sources += [ROOT / path for path in (
            "crates/obzenflow_benchmarks/scripts/capture_component_baseline.py",
            "crates/obzenflow_infra/src/journal/disk/codec/benchmark.rs",
            "crates/obzenflow_core/src/event/vector_clock.rs",
            "crates/obzenflow_runtime/src/testing/pipeline.rs",
            "crates/obzenflow_runtime/src/pipeline/fsm/actions.rs",
        )]
    rustc = command("rustc", "-Vv")
    toolchain_host = next(line.removeprefix("host: ") for line in rustc.splitlines() if line.startswith("host: "))
    report = {
        "measurement_contract": {"components":"journal-components-v2", "hot-path":"journal-hot-path-v3", "supervision-capacity":"supervision-capacity-v1", "projection-capacity":"projection-capacity-v1"}[args.suite],
        "baseline": args.baseline,
        "captured_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "revision": command("git", "rev-parse", "HEAD"),
        "working_tree": command("git", "status", "--short"),
        "rustc": rustc,
        "platform": sys.platform,
        "toolchain_host": toolchain_host,
        "hardware_model_and_memory": "not inspected",
        "profile": args.profile,
        "features": ["capacity-benchmarks" if capacity else "components" if args.suite == "components" else "journal-benchmarks"],
        "async_workers": 2,
        "maximum_blocking_workers": 2,
        "command": args.command,
        "source_sha256": {str(path.relative_to(ROOT)): digest(path) for path in sorted(sources)},
        "cases": sorted(cases, key=lambda case: case["benchmark"]["full_id"]),
    }
    if work is not None:
        report["work_observations"] = work
    if capacity:
        report["scope"] = {"selected_cases": args.case or "all fixture cases", "supported_capacity_claim": False}
        report["executable"] = {"path":str(args.executable),"sha256":digest(args.executable)}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Captured {len(cases)} Criterion cases to {args.output}")


if __name__ == "__main__":
    main()
