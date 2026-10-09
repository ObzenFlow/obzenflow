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
import re


# Keys are the benchmark crate's declared categories; order describes operations,
# independently of which Cargo target owns them.
CATEGORIES = {
    "read": ("Reading and upstream consumption", "Journal reads underpin upstream consumption. These cases do not isolate subscription selection, merging, receipts or contract checks."),
    "append": ("Journal appends", "Complete append operations include storage encoding. There is no separate private codec measurement."),
    "read_write": ("Journal write/read interaction", "These cases time writing and reading together; their duration cannot be attributed to either operation alone."),
    "causal": ("Causal and record bookkeeping", "Clock, frontier and record accounting work. Canonical JSON byte accounting is not the production disk codec."),
    "observe": ("Observations, metrics and reporting", "Observation handling, metrics refresh, projection and rendering have separate timing boundaries."),
    "runtime": ("Supervision and runtime scheduling", "Process CPU time used by idle or waiting pipelines in a declared window, not elapsed time or CPU percentage. These cases do not isolate supervisor dispatch."),
    "flow": ("Complete pipeline processing", "Completed flows, execution wrappers and per-run event-latency medians are different measurements; see each row's timed work."),
    "archive": ("Archive and replay operations", "Export, admission and comparison of existing archives; replay comparison excludes the original executions."),
}
UNCATEGORISED = ("Uncategorised", "These results have no valid case declaration. Declare the case next to its benchmark registration.")
# How the plan chose the gate's reference (FLOWIP-080v B10).
RULES = {
    "merge_base": "merge base with main (where this change started)",
    "first_parent": "first parent on main (the commit before this one)",
    "pin": "pinned for re-qualification",
}


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


def copyable_summary(report, title="performance"):
    fence = "`" * max(3, 1 + max((len(m[0]) for m in re.finditer(r"`+", report)), default=0))
    summary = (f"## Copyable {title} report\n\nCopy the Markdown below, or download this job's `.md` report artefact.\n\n"
               f"{fence}markdown\n{report}{fence}\n")
    if len(summary.encode("utf-8")) > 1_000_000:
        return f"## {title} report\n\nThe report exceeds the Actions summary limit. Download this job's complete `.md` report artefact.\n"
    return summary


def read_json(path, errors, root=None):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError) as error:
        errors.append(f"{path.relative_to(root) if root else path.name}: {error}")
        return None


def read_declarations(path, errors, target):
    """Map each declared case to its (category, timed work); invalid entries are evidence errors."""
    declared = {}
    for entry in read_json(path, errors) or []:
        try:
            case, category, timed = entry["case"], entry["category"], entry["timed"]
            if category not in CATEGORIES:
                raise ValueError(f"unknown category {category!r}")
            if not isinstance(case, str) or not isinstance(timed, str) or not case or not timed:
                raise ValueError("missing case ID or timed work")
            declared[case] = (category, timed)
        except (KeyError, TypeError, ValueError) as error:
            errors.append(f"{target}: invalid case declaration: {error}")
    return declared


def outcome_text(record):
    """Render a native outcome (`status` plus optional `detail`)."""
    outcome = record.get("outcome") if isinstance(record, dict) else None
    if not isinstance(outcome, dict) or "status" not in outcome:
        return "not completed"
    if outcome["status"] == "passed":
        return "passed"
    return f"{outcome['status']}: {outcome.get('detail') or 'no detail'}"


def comparison_measurement(value):
    estimate = value["estimate"]
    interval = estimate["confidence_interval"]
    point, low, high, confidence = map(number, (estimate["point_estimate"], interval["lower_bound"],
                                               interval["upper_bound"], interval["confidence_level"]))
    if not 0 <= low <= point <= high or not 0 < confidence < 1:
        raise ValueError("invalid comparison estimate")
    return f"{micros(point)} [{micros(low)}–{micros(high)}; {confidence * 100:g}%]"


def seconds(value):
    return f"{value:.2f}" if isinstance(value, (int, float)) and not isinstance(value, bool) else "unavailable"


def render_performance(directory, outcome):
    """Render a run's assembled evidence. `directory` is its performance directory."""
    errors, rows, comparisons, target_rows, stage_rows, declared = [], [], [], [], [], {}
    context = context_for()
    assembly, qualification, controls = {}, {}, {}
    if directory is None:
        errors.append("The run did not identify a performance directory; no previous run is substituted.")
    else:
        def load(path):
            return read_json(path, errors, directory)

        assembly = load(directory / "assembly.json") or {}
        plan = load(directory / "plan.json") or {}
        qualification = load(directory / "qualification.json") or {}
        controls = load(directory / "negative-controls.json") or {}
        comparison = load(directory / "comparison.json") or {}
        policy = load(directory / "comparison-policy.json") or {}
        source, reference = plan.get("source") or {}, plan.get("reference") or {}
        context["Measured checkout SHA"] = source.get("commit", "unavailable")
        context["Source content SHA256"] = source.get("content_sha256", "unavailable")
        context["Native run ID"] = plan.get("run_id", "unavailable")
        context["Reference SHA"] = reference.get("commit", "unavailable")
        context["Reference selection"] = RULES.get(reference.get("rule"), "unavailable")
        adapters = sorted({str(value) for value in (assembly.get("reference_public_api_adapters") or {}).values() if value})
        context["Reference public API adapter"] = ", ".join(adapters) or "none"
        context["Rust"] = "; ".join(str(version).splitlines()[0] for version in assembly.get("rust") or [] if str(version).strip()) or "unavailable"
        context["Qualification sampling"] = f"{policy.get('sample_size', '?')} samples; {policy.get('warm_up_ms', '?')} ms warm-up; {policy.get('measurement_ms', '?')} ms measurement"
        wall = seconds(assembly.get("end_to_end_seconds"))
        context["End-to-end wall time"] = wall if wall == "unavailable" else f"{wall} s from planning to the last stage"
        for row in assembly.get("stages") or []:
            host = row.get("host") or {}
            stage_rows.append([row.get("stage"), row.get("name"), host.get("cpu", "unknown"), host.get("logical_cpus", "?"),
                               host.get("image", "unknown"), seconds(row.get("elapsed_seconds")), outcome_text(row)])
        features = {target["name"]: ",".join(target.get("required-features", [])) or "default" for target in plan.get("targets", [])}
        if not plan.get("suite"):
            errors.append("No planned suite shards are available.")
        for shard in plan.get("suite", []):
            for selector in shard["select"]:
                name = selector["target"]
                phase = directory / "suite" / shard["name"] / name
                label = f"{shard['name']}/{name}"
                record = load(phase / "outcome.json") or {}
                measured, problems = read_measurements(phase / "criterion")
                errors.extend(f"{label}: {problem}" for problem in problems)
                selected = None
                if (phase / "selected.json").exists():
                    selected = load(phase / "selected.json")
                    declared.setdefault(name, {}).update(read_declarations(phase / "declarations.json", errors, name))
                else:
                    errors.append(f"{label}: no case selection; {outcome_text(record)}")
                if selected is not None and {row.case_id for row in measured} != set(selected):
                    errors.append(f"{label}: measured case IDs differ from the shard's selection.")
                for row in measured:
                    if row.case_id not in declared.get(name, {}):
                        errors.append(f"{name}: {row.case_id}: no case declaration.")
                rows.extend((name, row) for row in measured)
                target_rows.append([shard["name"], name, features.get(name, "unplanned"), selector.get("cases") or "all",
                                    seconds(record.get("elapsed_seconds")),
                                    f"{len(measured)}/{len(selected) if selected is not None else '?'}", outcome_text(record)])
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
    status = assembly.get("outcome", {}).get("status", "incomplete")
    if outcome != "success" or errors:
        status = status if status in ("failed", "incomplete") else "incomplete"
    qualified = qualification.get("outcome", {}) if isinstance(qualification, dict) else {}
    lines = ["# Performance report", "", f"Native outcome: **{cell(status.upper())}**. CI assembly step: **{cell(outcome)}**.", "",
             "| Context | Value |", "| --- | --- |"]
    lines.extend(f"| {cell(key)} | {cell(value)} |" for key, value in context.items())
    lines += ["", f"Full-suite observations: **{len(rows)} cases**. Native comparison results: **{len(comparisons)} cases**.",
              "", "Each comparison runs alone on its runner, with adjacent reference-before, candidate and reference-after trials. Its thresholds and controls are unchanged.",
              "", "Full-suite observations retain authored sampling settings and run one measurement at a time on their shard's runner. Compare them only across runs on matching runner hardware, with the same case, features and profile. They apply no regression thresholds.",
              "", "All estimates are in **µs**. Category durations are not summed.", "", "## Qualification", "",
              f"Result: **{'passed' if qualified.get('status') == 'passed' else 'not passed'}**. {cell(qualified.get('detail') or '')}",
              f"Slowdown control: **{cell(controls.get('slowdown', {}).get('outcome', 'unavailable'))}**; missing-reader output rejected: **{cell(controls.get('missing_work_rejected_by_completion_oracle', 'unavailable'))}**; missing-Studio output rejected: **{cell(controls.get('missing_studio_projection_output_rejected', 'unavailable'))}**.",
              "", "## Stages", "", "In CI each stage runs on its own runner, so stage durations overlap and must not be summed; a local run executes them serially. The end-to-end wall time is in the context table.", "",
              "| Stage | Name | CPU | Logical CPUs | Image | Seconds | Outcome |", "| --- | --- | --- | ---: | --- | ---: | --- |"]
    lines.extend("| " + " | ".join(cell(value) for value in row) + " |" for row in stage_rows)
    lines += ["", "## Target completion by shard", "", "| Shard | Target | Features | Cases selected | Seconds | Measured | Outcome |",
              "| --- | --- | --- | --- | ---: | ---: | --- |"]
    lines.extend("| " + " | ".join(cell(value) for value in row) + " |" for row in target_rows)
    if errors:
        lines += ["", "## Missing or unreadable evidence", ""]
        lines.extend(f"- {cell(error)}" for error in errors)

    def classify(target, case):
        return declared.get(target, {}).get(case, (None, "No case declaration"))

    for key, (title, note) in [*CATEGORIES.items(), (None, UNCATEGORISED)]:
        selected = [(target, row, classify(target, row.case_id)[1]) for target, row in rows
                    if classify(target, row.case_id)[0] == key]
        gated = [(case, estimates, verdict) for target, case, estimates, verdict in comparisons
                 if classify(target, case)[0] == key]
        if not selected and not gated:
            continue
        lines += ["", f"## {title}", "", note]
        if gated:
            lines += ["", "### Isolated comparison gate", "", "Estimates show median [confidence interval; level]. Decisions come from the native validator.", "",
                      "| Case ID | Reference before (µs) | Candidate (µs) | Reference after (µs) | Decision |",
                      "| --- | --- | --- | --- | --- |"]
            for case, estimates, verdict in gated:
                lines.append("| " + " | ".join(cell(value) for value in [case, *estimates, verdict]) + " |")
        if selected:
            lines += ["", "### Full-suite observations", "", "| Target | Case ID | Timed work | Median (µs) | Median confidence interval (µs) | Samples |",
                      "| --- | --- | --- | ---: | --- | ---: |"]
            for target, row, timed in sorted(selected, key=lambda item: (item[0], item[1].case_id)):
                interval = f"{micros(row.lower_ns)}–{micros(row.upper_ns)} ({row.confidence * 100:g}%)"
                lines.append("| " + " | ".join(cell(value) for value in [target, row.case_id, timed,
                                                                          micros(row.median_ns), interval, row.samples]) + " |")
    return "\n".join(lines) + "\n", errors


def performance_summary(report):
    copyable = copyable_summary(report)
    # Show rendered tables as well as a single copyable Markdown block.
    summary = report + "\n<details><summary>Copy the complete report as Markdown</summary>\n\n" + copyable + "\n</details>\n"
    return summary if len(summary.encode("utf-8")) <= 1_000_000 else copyable


def context_for():
    # Hardware and compiler come from each stage's record and build manifests;
    # the machine formatting this report measured nothing.
    run_url = "unavailable (local run)"
    if os.getenv("GITHUB_RUN_ID") and os.getenv("GITHUB_REPOSITORY"):
        run_url = f"{os.getenv('GITHUB_SERVER_URL', 'https://github.com')}/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']}/attempts/{os.getenv('GITHUB_RUN_ATTEMPT', '1')}"
    context = {
        "Report format": "performance-operations-v4",
        "Measured checkout SHA": "unavailable",
        "Target": "all declared targets",
        "Configured features": "per target below",
        "Profile": "bench (optimised)",
        "Command": "cargo xtask performance <stage>, or cargo xtask test --lane performance serially",
        "Run / attempt": f"{os.getenv('GITHUB_RUN_ID', 'local')} / {os.getenv('GITHUB_RUN_ATTEMPT', '1')}",
        "Run URL": run_url,
    }
    if os.getenv("CRITERION_ARTIFACT_URL"):
        context["Raw Criterion artefact"] = os.environ["CRITERION_ARTIFACT_URL"]
    return context


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--performance-dir", required=True, help="The run's performance evidence directory; empty means planning did not start")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--outcome", choices=("success", "failure", "cancelled", "skipped"), required=True)
    args = parser.parse_args()
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


if __name__ == "__main__":
    raise SystemExit(main())
