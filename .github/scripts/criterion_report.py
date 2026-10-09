#!/usr/bin/env python3
# SPDX-License-Identifier: MIT OR Apache-2.0
# SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
# https://obzenflow.dev
"""Format identified native performance evidence; never run or judge benchmarks."""

import argparse
from dataclasses import dataclass
import html
import json
import math
import os
from pathlib import Path
import platform
import re
import shlex
import subprocess


# Order describes operations, independently of which Cargo target owns them.
READ, APPEND, MIXED, CAUSAL, OBSERVE, RUNTIME, FLOW, ARCHIVE, UNKNOWN = range(9)
CATEGORIES = (
    ("Reading and upstream consumption", "Journal reads underpin upstream consumption. These cases do not isolate subscription selection, merging, receipts or contract checks."),
    ("Journal appends", "Complete append operations include storage encoding. There is no separate private codec measurement."),
    ("Journal write/read interaction", "These cases time writing and reading together; their duration cannot be attributed to either operation alone."),
    ("Causal and record bookkeeping", "Clock, frontier and record accounting work. Canonical JSON byte accounting is not the production disk codec."),
    ("Observations, metrics and reporting", "Observation handling, metrics refresh, projection and rendering have separate timing boundaries."),
    ("Supervision and runtime scheduling", "Composite lifecycle and worker experiments, not isolated supervisor dispatch. CPU-labelled cases report elapsed sampling-routine time, not CPU percentage."),
    ("Complete pipeline processing", "Completed flows, execution wrappers and per-run event-latency medians are different measurements; see each row's timed work."),
    ("Archive and replay operations", "Export, admission and comparison of existing archives; replay comparison excludes the original executions."),
    ("Uncategorised", "These results remain visible. Add an operation mapping after checking their timing boundary."),
)

# Match full Criterion IDs, never filesystem-sanitised directory names. Parameter
# suffixes are deliberately retained in the report. New operations fall through.
RULES = {
    "journal_components": [
        (r"disk_components/reader_next/.+", READ, "Read 64 records; reader opening excluded"),
        (r"causal_components/journal_clock_restore/.+", CAUSAL, "Restore one journal clock"),
        (r"causal_components/byte_accounting/.+", CAUSAL, "Canonical JSON byte accounting for one record"),
        (r"causal_components/frontier_from_record/.+", CAUSAL, "Extract one record's causal frontier"),
        (r"causal_components/merge_overlapping/.+", CAUSAL, "Merge two overlapping frontiers; target cloning excluded"),
        (r"causal_components/prepare_append/.+", CAUSAL, "Prepare one append clock; no physical append"),
    ],
    "journal_hot_path": [
        (r"journal_record_read/open_and_read/.+", READ, "Open a reader and read/admit two records"),
        (r"reader_dispatch/full/actual_reader/readers_(1|8|32)", READ, "Concurrent readers: {0}; spawn tasks and read 64 records each; opening excluded"),
        (r"hotspots/sequential_read/.+", READ, "Read 1,024 records; reader creation excluded"),
        (r"hotspots/reopened_scan/.+", READ, "Reopen journal, create reader and scan 1,024 records; warmed process"),
        (r"hotspots/reader_creation/.+", READ, "Create one reader over a prefilled 1,024-record journal"),
        (r"hotspots/first_process_open_and_scan/wide_observed", READ, "Fresh-process open, reconstruction and scan of 1,024 records; launch excluded, filesystem cache uncontrolled"),
        (r"cross_journal_reads/eight_journals_1024_records", READ, "Create readers and scan eight journals, 128 records each"),
        (r"mixed_journal/two_concurrent_scans", READ, "Create two readers and concurrently scan 64 records each"),
        (r"journal_append_cost/complete_append/.+", APPEND, "Append/group-append 64 records; destination setup and readback excluded"),
        (r"hotspots/append/.+", APPEND, "Append 64 records including authored cloning and causal preparation"),
        (r"journal_refresh/append_after_eof", MIXED, "64 append, read and EOF-check pairs"),
        (r"mixed_journal/distinct_12mib_then_reuse", MIXED, "192 interleaved appends and reads; 96 distinct 128 KiB provenance entries, then reuse"),
        (r"record_accounting/canonical_bytes/.+", CAUSAL, "Canonical JSON byte accounting for one record"),
        (r"causal_record_work/journal_clock_restore/.+", CAUSAL, "Restore one journal clock"),
        (r"causal_record_work/clock_clone/.+", CAUSAL, "Clone one vector clock"),
        (r"causal_record_work/clock_json_bytes/.+", CAUSAL, "Canonical JSON byte accounting for one clock"),
        (r"observation_handling/capture_for_record", OBSERVE, "64 observation captures and live offers"),
        (r"observation_handling/validation/clock_.+", OBSERVE, "Validate 64 observation packets"),
        (r"observation_handling/live_submission/clock_.+", OBSERVE, "Submit 64 observation packets"),
        (r"journal_refresh/metrics/(1|4)_journals/(advancing|unchanged)", OBSERVE, "Refresh metrics tails (journal count: {0}, {1}); append setup excluded"),
    ],
    "validation_boundaries": [
        (r"archive_validation/export/inputs_1000", ARCHIVE, "JSONL export of an existing 1,000-input archive"),
        (r"archive_validation/admit_and_read/inputs_1000", ARCHIVE, "Open, admit and fully read a 1,000-input archive"),
        (r"replay_validation/streaming_comparison/inputs_10000", ARCHIVE, "Compare existing live/replay 10,000-input archives; execution and report writing excluded"),
        (r"metrics_validation/tail_refresh/data_and_error_64", OBSERVE, "One stage metrics snapshot from 66 data and one error record"),
        (r"studio_validation/project_and_snapshot/inputs_64", OBSERVE, "Project 69 preloaded records to frames, measurements and snapshots; journal reads excluded"),
    ],
    "pipeline_execution": [
        (r"total_execution_time/.+", FLOW, "Whole flow wrapper including construction, execution and teardown"),
        (r"execution_time_per_event/.+", FLOW, "Build/run duration divided by 100 (110 emitted inputs)"),
        (r"metrics_reporting/render_100_stages", OBSERVE, "Render Prometheus metrics for 100 stages"),
        (r"metrics_reporting/publish_pair_100_stages/(0|4)", OBSERVE, "Publish application/infrastructure snapshots for 100 stages, {0} concurrent renderers"),
        (r"causal_record_costs/journal_clock_restore/.+", CAUSAL, "Restore one journal clock"),
        (r"causal_record_costs/byte_budget/.+", CAUSAL, "Canonical JSON byte accounting for one record"),
        (r"causal_record_costs/authored/.+", CAUSAL, "Reconstruct one authored event from a record"),
    ],
    "pipeline_throughput": [
        (r"completed_flow/.+", FLOW, "128-input completed flow through drain; build excluded"),
    ],
    "idle_cpu_usage": [
        (r"idle_cpu_usage/cpu_percentage|idle_cpu_by_depth/(1|10|20|100)_stages", RUNTIME, "Elapsed CPU-sampling routine including setup, waits and stop (not CPU %)"),
    ],
    "waiting_for_gun_cpu_usage": [
        (r"waiting_for_gun_cpu_usage/cpu_percentage", RUNTIME, "Elapsed CPU-sampling routine including setup, waits and stop (not CPU %)"),
    ],
    "tokio_worker_3_stage_experiment": [
        (r"3_stage_worker_experiments/.+|5_stage_control/default_runtime", RUNTIME, "Reported per-run median event latency under this worker configuration"),
    ],
}
for depth in (1, 2, 3, 4, 5, 20, 100):
    RULES[f"per_event_latency_{depth}_stage"] = [
        (rf"{depth}_stage_latency/median_latency", FLOW, "Reported per-run median event latency"),
    ]
RULES["per_event_latency_100_stage_memory"] = [
    (r"100_stage_latency_memory/median_latency", FLOW, "Reported per-run median event latency"),
]


def classify(target, case_id):
    for pattern, category, work in RULES.get(target, []):
        match = re.fullmatch(pattern, case_id)
        if match:
            return category, work.format(*match.groups())
    return UNKNOWN, "Criterion-reported duration; timing boundary not classified"


@dataclass(frozen=True)
class Measurement:
    case_id: str
    median_ns: float
    lower_ns: float
    upper_ns: float
    confidence: float
    samples: int


def number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError("expected a finite number")
    return value


def read_measurements(root):
    rows, errors, seen = [], [], set()
    # Union also exposes an interrupted result with a missing benchmark.json.
    directories = sorted({p.parent for name in ("benchmark.json", "estimates.json", "sample.json")
                          for p in root.glob(f"**/new/{name}")})
    for directory in directories:
        try:
            benchmark, estimates, sample = (
                json.loads((directory / name).read_text())
                for name in ("benchmark.json", "estimates.json", "sample.json")
            )
            case_id = benchmark["full_id"]
            if not isinstance(case_id, str) or not case_id.strip():
                raise ValueError("missing full Criterion ID")
            if case_id in seen:
                raise ValueError(f"duplicate Criterion ID: {case_id}")
            seen.add(case_id)
            median = estimates["median"]
            interval = median["confidence_interval"]
            point, lower, upper, confidence = map(number, (
                median["point_estimate"], interval["lower_bound"],
                interval["upper_bound"], interval["confidence_level"],
            ))
            if not 0 <= lower <= point <= upper or not 0 < confidence < 1:
                raise ValueError("invalid median confidence interval")
            times, iters = sample["times"], sample["iters"]
            if not isinstance(times, list) or not isinstance(iters, list) or not times or len(times) != len(iters):
                raise ValueError("missing or mismatched sample arrays")
            if any(number(t) < 0 for t in times) or any(number(i) <= 0 for i in iters):
                raise ValueError("invalid sample time or iteration count")
            rows.append(Measurement(case_id, point, lower, upper, confidence, len(times)))
        except (OSError, ValueError, KeyError, TypeError) as error:
            errors.append(f"{directory.relative_to(root)}: {error}")
    if not directories:
        errors.append("No current measurements found (new/benchmark.json, estimates.json and sample.json).")
    return sorted(rows, key=lambda row: row.case_id), errors


def cell(value):
    return html.escape(str(value), quote=False).replace("|", "&#124;").replace("`", "&#96;").replace("\n", " ").replace("\r", " ")


def micros(ns):
    # Criterion's median is already per iteration. Do not divide by sample.iters.
    return f"{ns / 1000:.6f}".rstrip("0").rstrip(".")


def render_report(target, rows, errors, context, outcome, preview=False):
    status = "PREVIEW (existing local measurements)" if preview else (
        "COMPLETE" if outcome == "success" and not errors else "INCOMPLETE"
    )
    command_status = "not verified (local preview)" if preview else outcome
    lines = [f"# Criterion report: {cell(target)}", "", f"Status: **{status}**. Measurement command: **{cell(command_status)}**.", "",
             "| Context | Value |", "| --- | --- |"]
    lines.extend(f"| {cell(key)} | {cell(value)} |" for key, value in context.items())
    lines += ["", f"Reported cases: **{len(rows)}**. All durations are **µs**. Confidence intervals describe the median estimate; samples are Criterion sample counts.",
              "", "Compare the same case ID, timed work, profile and environment across runs. Durations are not summed across categories. No timing regression threshold is applied."]
    if outcome != "success" and not preview:
        lines += ["", "The measurement command did not succeed. Available rows are partial evidence, not a successful target run."]
    if errors:
        lines += ["", "## Missing or unreadable measurements", ""]
        lines.extend(f"- {cell(error)}" for error in errors)
    for category, (title, note) in enumerate(CATEGORIES):
        selected = [(row, classify(target, row.case_id)[1]) for row in rows
                    if classify(target, row.case_id)[0] == category]
        if not selected:
            continue
        lines += ["", f"## {title}", "", note, "",
                  "| Case ID | Timed work | Median (µs) | Median confidence interval (µs) | Samples |",
                  "| --- | --- | ---: | --- | ---: |"]
        for row, work in selected:
            interval = f"{micros(row.lower_ns)}–{micros(row.upper_ns)} ({row.confidence * 100:g}%)"
            lines.append(f"| {cell(row.case_id)} | {cell(work)} | {micros(row.median_ns)} | {interval} | {row.samples} |")
    return "\n".join(lines) + "\n"


def copyable_summary(report, title="Criterion"):
    fence = "`" * max(3, 1 + max((len(m[0]) for m in re.finditer(r"`+", report)), default=0))
    summary = (f"## Copyable {title} report\n\nCopy the Markdown below, or download this job's `.md` report artefact.\n\n"
               f"{fence}markdown\n{report}{fence}\n")
    if len(summary.encode("utf-8")) > 1_000_000:
        return f"## {title} report\n\nThe report exceeds the Actions summary limit. Download this job's complete `.md` report artefact.\n"
    return summary


def read_json(path, errors):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError) as error:
        errors.append(f"{path.name}: {error}")
        return None


def comparison_measurement(value):
    estimate = value["estimate"]
    interval = estimate["confidence_interval"]
    point, low, high, confidence = map(number, (estimate["point_estimate"], interval["lower_bound"],
                                               interval["upper_bound"], interval["confidence_level"]))
    if not 0 <= low <= point <= high or not 0 < confidence < 1:
        raise ValueError("invalid comparison estimate")
    return f"{micros(point)} [{micros(low)}–{micros(high)}; {confidence * 100:g}%]"


def render_performance(directory, outcome):
    errors, rows, comparisons, target_rows = [], [], [], []
    context = context_for("all declared targets", "per target below", False)
    context.update({"Report format": "performance-operations-v2", "Command": "cargo xtask test --lane performance"})
    native, phases, execution, qualification, controls = {}, {}, {}, {}, {}
    if directory is None:
        errors.append("The native validator did not identify a run directory; no previous run is substituted.")
    else:
        native = read_json(directory / "report.json", errors) or {}
        performance = directory / "performance"
        phases = read_json(performance / "phases.json", errors) or {}
        execution = read_json(performance / "suite-execution.json", errors) or {}
        qualification = read_json(performance / "qualification.json", errors) or {}
        controls = read_json(performance / "negative-controls.json", errors) or {}
        comparison = read_json(performance / "comparison.json", errors) or {}
        plan = read_json(performance / "suite-plan.json", errors) or []
        context["Measured checkout SHA"] = native.get("source", {}).get("commit", "unavailable")
        context["Source content SHA256"] = native.get("source", {}).get("content_sha256", "unavailable")
        context["Native run ID"] = native.get("run_id", "unavailable")
        policy = read_json(performance / "comparison-policy.json", errors) or {}
        driver = read_json(performance / "measurement-driver.json", errors) or {}
        context["Reference SHA"] = policy.get("baseline_revision", "unavailable")
        context["Reference public API adapter"] = driver.get("reference_public_api_adapter") or "none"
        context["Qualification sampling"] = f"{policy.get('sample_size', '?')} samples; {policy.get('warm_up_ms', '?')} ms warm-up; {policy.get('measurement_ms', '?')} ms measurement"
        context["Full-suite execution mode"] = execution.get("mode", "not executed")
        context["CPU assignments"] = json.dumps(execution.get("cpu_groups", []))
        context["Runtime workers / analysis threads"] = f"{execution.get('tokio_default_workers', '?')} / {execution.get('rayon_threads', '?')} (authored runtime overrides retained)"
        if not plan:
            errors.append("No declared full-suite inventory is available.")
        for target in plan:
            name = target["name"]
            phase = performance / "suite" / name
            record = read_json(phase / "outcome.json", errors) or {}
            expected = read_json(phase / "cases.json", errors)
            measured, problems = read_measurements(phase / "criterion")
            errors.extend(f"{name}: {problem}" for problem in problems)
            if expected is None or {row.case_id for row in measured} != set(expected):
                errors.append(f"{name}: measured case IDs differ from the executable inventory.")
            rows.extend((name, row) for row in measured)
            elapsed = record.get("elapsed_seconds")
            target_rows.append([name, ",".join(target.get("required-features", [])) or "default",
                                record.get("cpus", []), f"{elapsed:.2f}" if isinstance(elapsed, (int, float)) else "unavailable",
                                f"{len(measured)}/{len(expected) if expected is not None else '?'}",
                                "passed" if record.get("passed") else record.get("error") or "not completed"])
        for target, suite in comparison.get("suites", {}).items():
            for case in suite["cases"]:
                try:
                    estimates = [comparison_measurement(comparison[phase][case]) for phase in ("before", "candidate", "after")]
                    decision = comparison["decisions"][case]
                    verdict = decision["outcome"] + (": " + decision["detail"] if decision.get("detail") else "")
                    comparisons.append((target, case, estimates, verdict))
                except (KeyError, ValueError, TypeError) as error:
                    errors.append(f"comparison {case}: {error}")
        if not comparisons:
            errors.append("No completed native comparisons are available.")
    status = native.get("outcome", {}).get("status", "incomplete")
    if outcome != "success" or errors:
        status = status if status in ("failed", "incomplete") else "incomplete"
    lines = ["# Performance report", "", f"Native outcome: **{cell(status.upper())}**. CI measurement step: **{cell(outcome)}**.", "",
             "| Context | Value |", "| --- | --- |"]
    lines.extend(f"| {cell(key)} | {cell(value)} |" for key, value in context.items())
    lines += ["", f"Full-suite observations: **{len(rows)} cases**. Native comparison results: **{len(comparisons)} cases**.",
              "", "The comparison gate runs alone, with adjacent reference-before, candidate and reference-after trials. Its thresholds and controls are unchanged.",
              "", "Full-suite observations retain authored sampling settings. On Linux, the two CPU partitions share memory, caches and disk bandwidth; compare only runs with the same execution mode, CPU capacity, case, features and profile. These observations do not apply regression thresholds and are not directly comparable to the former isolated-runner matrix.",
              "", "All estimates are in **µs**. Category durations are not summed.", "", "## Qualification", "",
              f"Result: **{'passed' if qualification.get('passed') else 'not passed'}**. {cell(qualification.get('error') or '')}",
              f"Slowdown control: **{cell(controls.get('slowdown', {}).get('outcome', 'unavailable'))}**; missing-reader output rejected: **{cell(controls.get('missing_work_rejected_by_completion_oracle', 'unavailable'))}**; missing-Studio output rejected: **{cell(controls.get('missing_studio_projection_output_rejected', 'unavailable'))}**.",
              "", "## Phase durations", "", "Elapsed wall time, including preparation and analysis. Concurrent target durations must not be summed to estimate job duration.", "",
              "| Phase | Seconds |", "| --- | ---: |"]
    lines.extend(f"| {cell(phase)} | {number(phases[phase]):.2f} |" for phase in ("prepare_reference", "build", "qualification", "full_suite") if phase in phases)
    lines += ["", "## Target completion", "", "| Target | Features | CPUs | Seconds | Cases | Outcome |", "| --- | --- | --- | ---: | ---: | --- |"]
    lines.extend("| " + " | ".join(cell(value) for value in row) + " |" for row in target_rows)
    if errors:
        lines += ["", "## Missing or unreadable evidence", ""]
        lines.extend(f"- {cell(error)}" for error in errors)
    for category, (title, note) in enumerate(CATEGORIES):
        selected = [(target, row) for target, row in rows if classify(target, row.case_id)[0] == category]
        qualified = [(target, case, estimates, verdict) for target, case, estimates, verdict in comparisons
                     if classify(target, case)[0] == category]
        if not selected and not qualified:
            continue
        lines += ["", f"## {title}", "", note]
        if qualified:
            lines += ["", "### Isolated comparison gate", "", "Estimates show median [confidence interval; level]. Decisions come from the native validator.", "",
                      "| Case ID | Reference before (µs) | Candidate (µs) | Reference after (µs) | Decision |",
                      "| --- | --- | --- | --- | --- |"]
            for _, case, estimates, verdict in qualified:
                lines.append("| " + " | ".join(cell(value) for value in [case, *estimates, verdict]) + " |")
        if selected:
            lines += ["", "### Full-suite observations", "", "| Target | Case ID | Timed work | Median (µs) | Median confidence interval (µs) | Samples |",
                      "| --- | --- | --- | ---: | --- | ---: |"]
            for target, row in sorted(selected, key=lambda pair: (pair[0], pair[1].case_id)):
                interval = f"{micros(row.lower_ns)}–{micros(row.upper_ns)} ({row.confidence * 100:g}%)"
                lines.append("| " + " | ".join(cell(value) for value in [target, row.case_id, classify(target, row.case_id)[1],
                                                                          micros(row.median_ns), interval, row.samples]) + " |")
    return "\n".join(lines) + "\n", errors


def performance_summary(report):
    copyable = copyable_summary(report, "performance")
    # Show rendered tables as well as a single copyable Markdown block.
    summary = report + "\n<details><summary>Copy the complete report as Markdown</summary>\n\n" + copyable + "\n</details>\n"
    return summary if len(summary.encode("utf-8")) <= 1_000_000 else copyable


def command_output(*command):
    try:
        return subprocess.check_output(command, text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unavailable"


def context_for(target, features, preview):
    cpu = "unavailable"
    if Path("/proc/cpuinfo").exists():
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.partition(":")[2].strip()
                break
    elif platform.system() == "Darwin":
        cpu = command_output("sysctl", "-n", "machdep.cpu.brand_string")
    command = shlex.join(["cargo", "bench", "--locked", "-p", "obzenflow_benchmarks",
                          "--bench", target, "--features", features])
    context = {
        "Report format": "criterion-operations-v1",
        "Measured checkout SHA": "unavailable (local preview)" if preview else command_output("git", "rev-parse", "HEAD"),
        "Target": target,
        "Configured features": features or "default",
        "Profile": "unavailable (local preview)" if preview else "bench (optimised)",
        "Expected CI command" if preview else "Command": command,
    }
    if preview:
        context["Environment"] = "unavailable for existing measurements; current host is not asserted as their origin"
    else:
        run_url = "unavailable (local run)"
        if os.getenv("GITHUB_RUN_ID") and os.getenv("GITHUB_REPOSITORY"):
            run_url = f"{os.getenv('GITHUB_SERVER_URL', 'https://github.com')}/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']}/attempts/{os.getenv('GITHUB_RUN_ATTEMPT', '1')}"
        context.update({
            "Run / attempt": f"{os.getenv('GITHUB_RUN_ID', 'local')} / {os.getenv('GITHUB_RUN_ATTEMPT', '1')}",
            "Run URL": run_url,
            "Rust": command_output("rustc", "-Vv"),
            "Runner": f"{os.getenv('RUNNER_ENVIRONMENT', 'unknown')} / {os.getenv('ImageOS', 'unknown')} {os.getenv('ImageVersion', '')}",
            "OS / architecture": f"{platform.platform()} / {platform.machine()}",
            "CPU / logical CPUs": f"{cpu} / {os.cpu_count()}",
        })
    if os.getenv("CRITERION_ARTIFACT_URL"):
        context["Raw Criterion artefact"] = os.environ["CRITERION_ARTIFACT_URL"]
    return context


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target")
    parser.add_argument("--features", default="")
    parser.add_argument("--criterion-dir", type=Path)
    parser.add_argument("--performance-dir", help="Exact native run directory; empty means the validator did not start")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--outcome", choices=("success", "failure", "cancelled", "skipped"), required=True)
    parser.add_argument("--preview", action="store_true", help="Do not attribute existing local results to this checkout or host")
    args = parser.parse_args()
    if args.performance_dir is not None:
        try:
            report, errors = render_performance(Path(args.performance_dir) if args.performance_dir else None, args.outcome)
        except (OSError, ValueError, KeyError, TypeError) as error:
            errors = [str(error)]
            report = f"# Performance report\n\n**INCOMPLETE**: could not read native evidence: {cell(error)}\n"
        args.output_dir.mkdir(parents=True, exist_ok=True)
        identity = f"{os.getenv('GITHUB_RUN_ID', 'local')}-attempt-{os.getenv('GITHUB_RUN_ATTEMPT', '1')}"
        path = args.output_dir / f"performance-{identity}.md"
        path.write_text(report, encoding="utf-8")
        if os.getenv("GITHUB_STEP_SUMMARY"):
            with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as summary:
                summary.write(performance_summary(report))
        print(f"Wrote {path}: {len(errors)} evidence errors")
        return 1 if errors else 0
    if not args.target or args.criterion_dir is None:
        parser.error("use --performance-dir or both --target and --criterion-dir")
    rows, errors = read_measurements(args.criterion_dir)
    context = context_for(args.target, args.features, args.preview)
    report = render_report(args.target, rows, errors, context, args.outcome, args.preview)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    target = re.sub(r"[^a-zA-Z0-9_-]", "_", args.target)
    identity = "preview" if args.preview else (
        f"{context['Measured checkout SHA'][:12]}-{os.getenv('GITHUB_RUN_ID', 'local')}-attempt-{os.getenv('GITHUB_RUN_ATTEMPT', '1')}"
    )
    path = args.output_dir / f"criterion-{target}-{identity}.md"
    path.write_text(report, encoding="utf-8")
    if os.getenv("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as summary:
            summary.write(copyable_summary(report))
    print(f"Wrote {path}: {len(rows)} cases, {len(errors)} measurement errors")
    # Always leave the report available, even when the measurement set is broken.
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
