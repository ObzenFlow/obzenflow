// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Criterion executes and samples work. This module preserves the reference,
//! checks comparability/completion, and applies the declared acceptance rule.

mod driver;

use super::{failed, plan::Policy, process};
use crate::{error, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ComparisonPolicy {
    version: u32,
    baseline_revision: String,
    hot_path_validity_cases: usize,
    statistic: String,
    sample_size: usize,
    warm_up_ms: u64,
    measurement_ms: u64,
    allowed_regression: f64,
    allowed_control_drift: f64,
    maximum_relative_interval_width: f64,
    cases: Vec<String>,
    boundary_cases: Vec<String>,
}

impl ComparisonPolicy {
    fn read(root: &Path) -> Result<Self> {
        let path = root.join(".config/performance.toml");
        let policy: Self = toml::from_str(&fs::read_to_string(&path).map_err(|failure| {
            error(format!(
                "qualified performance policy unavailable: {failure}"
            ))
        })?)?;
        if policy.version != 3
            || policy.statistic != "median"
            || policy.baseline_revision.len() != 40
            || !policy
                .baseline_revision
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || policy.sample_size < 20
            || policy.hot_path_validity_cases < policy.cases.len()
            || policy.warm_up_ms == 0
            || policy.measurement_ms == 0
            || [
                policy.allowed_regression,
                policy.allowed_control_drift,
                policy.maximum_relative_interval_width,
            ]
            .iter()
            .any(|value| !value.is_finite() || *value <= 0.0 || *value >= 1.0)
            || policy.cases.is_empty()
            || policy.boundary_cases.is_empty()
            || policy
                .cases
                .iter()
                .chain(&policy.boundary_cases)
                .any(|case| case.is_empty())
            || policy
                .cases
                .iter()
                .chain(&policy.boundary_cases)
                .collect::<BTreeSet<_>>()
                .len()
                != policy.cases.len() + policy.boundary_cases.len()
        {
            return Err(error("invalid performance comparison policy"));
        }
        Ok(policy)
    }

    fn filter(&self) -> String {
        let escaped: Vec<_> = self
            .cases
            .iter()
            .map(|case| {
                let mut escaped = String::new();
                for character in case.chars() {
                    if "\\.+*?()|[]{}^$".contains(character) {
                        escaped.push('\\');
                    }
                    escaped.push(character);
                }
                escaped
            })
            .collect();
        format!("^({})$", escaped.join("|"))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Interval {
    confidence_level: f64,
    lower_bound: f64,
    upper_bound: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Estimate {
    confidence_interval: Interval,
    point_estimate: f64,
}

#[derive(Debug, Clone, Serialize)]
struct Measurement {
    estimate: Estimate,
    raw_iterations: Vec<f64>,
    raw_nanoseconds: Vec<f64>,
    work: Value,
}

#[derive(Serialize)]
struct Executable {
    path: PathBuf,
    compiled_manifest_dir: PathBuf,
    sha256: String,
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(tag = "outcome", content = "detail", rename_all = "snake_case")]
enum Decision {
    Passed,
    Regressed(String),
    Inconclusive(String),
}

pub(super) fn run(root: &Path, tools: &Policy, directory: &Path) -> Result<()> {
    let policy = ComparisonPolicy::read(root)?;
    fs::write(
        directory.join("comparison-policy.json"),
        serde_json::to_vec_pretty(&policy)?,
    )?;
    let baseline = directory.join("baseline-source");
    fs::create_dir(&baseline)?;
    let archive = directory.join("baseline.tar");
    let status = process::execute(
        process::command(root, tools, "git")
            .args(["archive", "--format=tar", "--output"])
            .arg(&archive)
            .arg(&policy.baseline_revision),
        directory,
        "baseline-source",
        Duration::from_secs(60),
    )?;
    if !status.success() {
        return Err(error(format!("required baseline {} is unavailable locally; its history is required for a comparable performance run", policy.baseline_revision)));
    }
    let status = process::execute(
        process::command(root, tools, "tar")
            .arg("-xf")
            .arg(&archive)
            .arg("-C")
            .arg(&baseline),
        directory,
        "baseline-unpack",
        Duration::from_secs(60),
    )?;
    if !status.success() {
        return Err(error(
            "could not extract the identified performance baseline",
        ));
    }
    driver::install(root, &baseline, directory)?;
    let (baseline, reference_target) = driver::reference(root, &baseline, directory)?;
    let candidate_target = root.join("target/validation-candidate");
    // Compile both versions before sampling, in separate target directories.
    // Cargo freshness alone cannot distinguish two equal-named workspaces that
    // write the same artifact paths. Preserve and identify every executable.
    let base_binary = build(
        root,
        &baseline,
        &reference_target,
        tools,
        directory,
        "baseline-build",
        ("journal_hot_path", "journal-benchmarks"),
    )?;
    let candidate_binary = build(
        root,
        root,
        &candidate_target,
        tools,
        directory,
        "candidate-build",
        ("journal_hot_path", "journal-benchmarks"),
    )?;
    let base_boundaries = build(
        root,
        &baseline,
        &reference_target,
        tools,
        directory,
        "baseline-boundaries-build",
        ("validation_boundaries", "validation-benchmarks"),
    )?;
    let candidate_boundaries = build(
        root,
        root,
        &candidate_target,
        tools,
        directory,
        "candidate-boundaries-build",
        ("validation_boundaries", "validation-benchmarks"),
    )?;
    for (reference, current) in [
        (&base_binary, &candidate_binary),
        (&base_boundaries, &candidate_boundaries),
    ] {
        if reference.sha256 == current.sha256 {
            return Err(error(
                "reference and candidate resolved to the same executable; comparison is invalid",
            ));
        }
    }
    validate_workloads(
        root,
        tools,
        &policy,
        directory,
        &base_binary,
        &candidate_binary,
    )?;
    let nonce = directory
        .parent()
        .and_then(Path::file_name)
        .ok_or_else(|| error("missing performance run identity"))?
        .to_string_lossy();
    let mut before = BTreeMap::new();
    let mut candidate = BTreeMap::new();
    let mut after = BTreeMap::new();
    // Adjacent per-case trials avoid placing the unchanged reference several
    // whole suites away from its candidate. Both implementations are already
    // built; no compiler work overlaps these measurements.
    for (suite, cases, reference, current) in [
        ("hot-path", &policy.cases, &base_binary, &candidate_binary),
        (
            "boundaries",
            &policy.boundary_cases,
            &base_boundaries,
            &candidate_boundaries,
        ),
    ] {
        for (index, case) in cases.iter().enumerate() {
            let selected = ComparisonPolicy {
                cases: vec![case.clone()],
                ..policy.clone()
            };
            for (phase, binary, results) in [
                ("before", reference, &mut before),
                ("candidate", current, &mut candidate),
                ("after", reference, &mut after),
            ] {
                eprintln!("validation: performance case={case}, phase={phase}");
                results.extend(measure(
                    root,
                    tools,
                    &selected,
                    directory,
                    binary,
                    &format!("{nonce}-{suite}-{index}-{phase}"),
                    None,
                )?);
            }
        }
    }
    let mut decisions = BTreeMap::new();
    for case in policy.cases.iter().chain(&policy.boundary_cases) {
        decisions.insert(
            case.clone(),
            compare(&before[case], &candidate[case], &after[case], &policy),
        );
    }
    fs::write(
        directory.join("comparison.json"),
        serde_json::to_vec_pretty(&json!({
            "baseline_revision": policy.baseline_revision,
            "profile": "bench", "suites": {
                "journal_hot_path": {"features": ["journal-benchmarks"], "cases": policy.cases},
                "validation_boundaries": {"features": ["validation-benchmarks"], "cases": policy.boundary_cases},
            },
            "platform": format!("{}-{}", std::env::consts::ARCH, std::env::consts::OS),
            "method": "adjacent reference before, candidate, unchanged reference after for each case; Criterion median confidence intervals",
            "hot_path_validity_cases_per_implementation": policy.hot_path_validity_cases,
            "decisions": decisions, "before": before, "candidate": candidate, "after": after,
        }))?,
    )?;
    // Rejection controls are independent obligations. A noisy positive case
    // must not prevent their execution or hide a broken completion oracle.
    let controls = qualify_controls(
        root,
        tools,
        &policy,
        directory,
        &candidate_binary,
        &nonce,
        (&before, &after),
    );
    if let Err(failure) = &controls {
        eprintln!("validation: performance rejection controls failed: {failure}");
    }
    if decisions
        .values()
        .any(|decision| matches!(decision, Decision::Regressed(_)))
    {
        return Err(failed(
            "required Criterion comparisons found a regression; see comparison.json",
        ));
    }
    if decisions
        .values()
        .any(|decision| matches!(decision, Decision::Inconclusive(_)))
    {
        return Err(error("performance comparison is inconclusive; incomplete or noisy work cannot certify acceptance; see comparison.json"));
    }
    controls
}

fn validate_workloads(
    root: &Path,
    tools: &Policy,
    policy: &ComparisonPolicy,
    directory: &Path,
    reference: &Executable,
    candidate: &Executable,
) -> Result<()> {
    let mut inventories = Vec::new();
    for (label, binary) in [
        ("reference-validity", reference),
        ("candidate-validity", candidate),
    ] {
        let census = directory.join(format!("{label}.work.json"));
        let status = process::execute(
            process::command(root, tools, &binary.path)
                .env("OBZENFLOW_WORK_CENSUS", &census)
                .env_remove("OBZENFLOW_BENCH_CONTROL")
                .arg("--test"),
            directory,
            label,
            Duration::from_secs(tools.command_watchdog_seconds),
        )?;
        if !status.success() {
            return Err(failed(format!(
                "{label}: a required hot-path workload oracle failed"
            )));
        }
        let rows = read_work(&census, binary)?;
        if rows.len() != policy.hot_path_validity_cases
            || policy.cases.iter().any(|case| !rows.contains_key(case))
        {
            return Err(error(format!(
                "{label}: incomplete hot-path workload inventory"
            )));
        }
        inventories.push(
            rows.into_iter()
                .map(|(case, row)| (case, row["input"].clone()))
                .collect::<BTreeMap<_, _>>(),
        );
    }
    if inventories[0] != inventories[1] {
        return Err(error(
            "reference and candidate hot-path validity inventories or workload dimensions differ",
        ));
    }
    fs::write(
        directory.join("workload-validity.json"),
        serde_json::to_vec_pretty(&json!({
            "outcome": "passed", "cases_per_implementation": policy.hot_path_validity_cases,
            "workloads": inventories[0], "timing_acceptance": false,
        }))?,
    )?;
    Ok(())
}

fn qualify_controls(
    root: &Path,
    tools: &Policy,
    policy: &ComparisonPolicy,
    directory: &Path,
    binary: &Executable,
    nonce: &str,
    (before, after): (
        &BTreeMap<String, Measurement>,
        &BTreeMap<String, Measurement>,
    ),
) -> Result<()> {
    const CASE: &str = "reader_dispatch/full/actual_reader/readers_8";
    if !policy.cases.iter().any(|case| case == CASE) {
        return Err(error(
            "performance policy omits its required complete-reader control",
        ));
    }
    let selected = ComparisonPolicy {
        cases: vec![CASE.into()],
        ..policy.clone()
    };
    let slow = measure(
        root,
        tools,
        &selected,
        directory,
        binary,
        &format!("{nonce}-slow-control"),
        Some("slow-reader"),
    )?;
    let slowdown = compare(&before[CASE], &slow[CASE], &after[CASE], policy);
    let missing_label = format!("{nonce}-missing-work-control");
    let missing = measure(
        root,
        tools,
        &selected,
        directory,
        binary,
        &missing_label,
        Some("missing-reader-output"),
    );
    let result: Value = serde_json::from_slice(&fs::read(
        directory.join(&missing_label).join("criterion.result.json"),
    )?)?;
    let diagnostic =
        fs::read_to_string(directory.join(&missing_label).join("criterion.stderr.log"))?;
    let missing_rejected = missing.is_err()
        && result["exit_code"] == 101
        && diagnostic.contains("actual reader output completeness and order");
    fs::write(
        directory.join("negative-controls.json"),
        serde_json::to_vec_pretty(&json!({
            "case": CASE, "slowdown": slowdown, "slow_measurement": slow,
            "missing_work_rejected_by_completion_oracle": missing_rejected,
            "missing_work_process": result,
            "acceptance_measurements_contain_no_controls": true,
        }))?,
    )?;
    if !matches!(slowdown, Decision::Regressed(_)) || !missing_rejected {
        return Err(error("performance gate failed its live slowdown/missing-work qualification; see negative-controls.json"));
    }
    Ok(())
}

fn build(
    root: &Path,
    source: &Path,
    target_directory: &Path,
    tools: &Policy,
    directory: &Path,
    label: &str,
    (target, features): (&str, &str),
) -> Result<Executable> {
    let status = process::execute(
        process::command(root, tools, "cargo")
            .env("CARGO_TARGET_DIR", target_directory)
            .args([
                "bench",
                "--locked",
                "--no-run",
                "--message-format=json",
                "--manifest-path",
            ])
            .arg(source.join("Cargo.toml"))
            .args([
                "--package",
                "obzenflow_benchmarks",
                "--features",
                features,
                "--bench",
                target,
            ]),
        directory,
        label,
        Duration::from_secs(tools.command_watchdog_seconds),
    )?;
    if !status.success() {
        return Err(failed(format!("{label} failed")));
    }
    let mut executables = BTreeSet::new();
    for line in fs::read_to_string(directory.join(format!("{label}.stdout.log")))?.lines() {
        let message: Value = serde_json::from_str(line)?;
        if message["reason"] == "compiler-artifact" && message["target"]["name"] == target {
            if let Some(path) = message["executable"].as_str() {
                executables.insert(PathBuf::from(path));
            }
        }
    }
    if executables.len() != 1 {
        return Err(error(format!(
            "{label} did not identify exactly one Criterion executable"
        )));
    }
    // A later build must never replace a binary whose samples are retained.
    let preserved = directory.join(format!("{label}.executable"));
    fs::copy(executables.into_iter().next().unwrap(), &preserved)?;
    let executable = Executable {
        sha256: driver::sha256(&fs::read(&preserved)?),
        path: preserved,
        compiled_manifest_dir: source.join("crates/obzenflow_benchmarks"),
    };
    fs::write(
        directory.join(format!("{label}.executable.json")),
        serde_json::to_vec_pretty(&executable)?,
    )?;
    Ok(executable)
}

fn measure(
    root: &Path,
    tools: &Policy,
    policy: &ComparisonPolicy,
    directory: &Path,
    binary: &Executable,
    label: &str,
    control: Option<&str>,
) -> Result<BTreeMap<String, Measurement>> {
    let artifacts = directory.join(label);
    fs::create_dir(&artifacts)?;
    let census = artifacts.join("work.json");
    let mut command = process::command(root, tools, &binary.path);
    command
        .env("OBZENFLOW_WORK_CENSUS", &census)
        .env("CRITERION_HOME", root.join("target/criterion"))
        .env_remove("OBZENFLOW_BENCH_CONTROL")
        .env("RUST_LOG", "warn")
        .args([
            "--bench",
            &policy.filter(),
            "--save-baseline",
            label,
            "--noplot",
            "--sample-size",
            &policy.sample_size.to_string(),
            "--warm-up-time",
            &(policy.warm_up_ms as f64 / 1000.0).to_string(),
            "--measurement-time",
            &(policy.measurement_ms as f64 / 1000.0).to_string(),
            "--confidence-level",
            "0.95",
        ]);
    if let Some(control) = control {
        command.env("OBZENFLOW_BENCH_CONTROL", control);
    }
    let status = process::execute(
        &mut command,
        &artifacts,
        "criterion",
        Duration::from_secs(tools.command_watchdog_seconds),
    )?;
    if !status.success() {
        return Err(failed(format!(
            "{label}: Criterion rejected the workload or could not complete it"
        )));
    }
    let mut work = read_work(&census, binary)?;
    let expected: BTreeSet<_> = policy.cases.iter().cloned().collect();
    if work.keys().cloned().collect::<BTreeSet<_>>() != expected {
        return Err(error(
            "work census does not cover the selected Criterion cases exactly",
        ));
    }
    let mut found = BTreeMap::new();
    collect_samples(&root.join("target/criterion"), label, &mut found)?;
    if found.keys().cloned().collect::<BTreeSet<_>>() != expected {
        return Err(error(
            "Criterion samples do not cover the selected cases exactly",
        ));
    }
    let mut measurements = BTreeMap::new();
    for (case, source) in found {
        let samples: Value = serde_json::from_slice(&fs::read(source.join("sample.json"))?)?;
        let estimates: Value = serde_json::from_slice(&fs::read(source.join("estimates.json"))?)?;
        let iterations: Vec<f64> = serde_json::from_value(samples["iters"].clone())?;
        let times: Vec<f64> = serde_json::from_value(samples["times"].clone())?;
        let measurement = Measurement {
            estimate: serde_json::from_value(estimates[&policy.statistic].clone())?,
            raw_iterations: iterations,
            raw_nanoseconds: times,
            work: work.remove(&case).unwrap(),
        };
        validate(&measurement, policy)?;
        let copy = artifacts
            .join("samples")
            .join(measurements.len().to_string());
        fs::create_dir_all(&copy)?;
        for name in ["benchmark.json", "sample.json", "estimates.json"] {
            fs::copy(source.join(name), copy.join(name))?;
        }
        measurements.insert(case, measurement);
    }
    Ok(measurements)
}

fn read_work(path: &Path, binary: &Executable) -> Result<BTreeMap<String, Value>> {
    let census: Value = serde_json::from_slice(&fs::read(path)?)?;
    if census["compiled_manifest_dir"].as_str().map(Path::new)
        != Some(binary.compiled_manifest_dir.as_path())
    {
        return Err(error(format!("{}: benchmark was compiled from a different source tree; refusing Cargo cache aliasing", path.display())));
    }
    let rows = census["cases"]
        .as_array()
        .ok_or_else(|| error("benchmark has no work census"))?;
    let mut work = BTreeMap::new();
    for row in rows {
        if !row["input"].is_object() || !row["work"].is_object() {
            return Err(error(
                "work census has no workload dimensions or completed-work counters",
            ));
        }
        let case = row["case"]
            .as_str()
            .ok_or_else(|| error("unnamed work census"))?
            .to_owned();
        if work.insert(case, row.clone()).is_some() {
            return Err(error("duplicate work census"));
        }
    }
    Ok(work)
}

fn collect_samples(
    directory: &Path,
    label: &str,
    found: &mut BTreeMap<String, PathBuf>,
) -> Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        if entry.file_name() == label {
            let benchmark: Value =
                serde_json::from_slice(&fs::read(entry.path().join("benchmark.json"))?)?;
            let name = benchmark["full_id"]
                .as_str()
                .ok_or_else(|| error("Criterion artifact has no full case identity"))?
                .to_owned();
            if found.insert(name, entry.path()).is_some() {
                return Err(error("duplicate Criterion case identity"));
            }
        } else {
            collect_samples(&entry.path(), label, found)?;
        }
    }
    Ok(())
}

fn validate(measurement: &Measurement, policy: &ComparisonPolicy) -> Result<()> {
    let interval = &measurement.estimate.confidence_interval;
    if measurement.raw_iterations.len() != policy.sample_size
        || measurement.raw_nanoseconds.len() != policy.sample_size
        || measurement
            .raw_iterations
            .iter()
            .chain(&measurement.raw_nanoseconds)
            .any(|value| !value.is_finite() || *value <= 0.0)
        || [
            interval.lower_bound,
            interval.upper_bound,
            measurement.estimate.point_estimate,
        ]
        .iter()
        .any(|value| !value.is_finite() || *value <= 0.0)
        || !interval.confidence_level.is_finite()
        || interval.confidence_level < 0.95
        || interval.confidence_level > 1.0
        || interval.lower_bound > measurement.estimate.point_estimate
        || interval.upper_bound < measurement.estimate.point_estimate
        || !measurement.work["input"].is_object()
        || !measurement.work["work"].is_object()
    {
        return Err(error(
            "invalid or incomplete Criterion samples, estimates or work census",
        ));
    }
    Ok(())
}

fn compare(
    before: &Measurement,
    candidate: &Measurement,
    after: &Measurement,
    policy: &ComparisonPolicy,
) -> Decision {
    for measurement in [before, candidate, after] {
        if let Err(failure) = validate(measurement, policy) {
            return Decision::Inconclusive(failure.to_string());
        }
    }
    if before.work["input"] != candidate.work["input"]
        || before.work["input"] != after.work["input"]
    {
        return Decision::Inconclusive("workload dimensions differ".into());
    }
    let first = &before.estimate.confidence_interval;
    let last = &after.estimate.confidence_interval;
    let drift =
        (last.upper_bound / first.lower_bound).max(first.upper_bound / last.lower_bound) - 1.0;
    if drift > policy.allowed_control_drift {
        return Decision::Inconclusive(format!(
            "unchanged reference drift/uncertainty {drift:.4} exceeds policy"
        ));
    }
    let uncertain = [before, candidate, after].iter().any(|measurement| {
        let interval = &measurement.estimate.confidence_interval;
        (interval.upper_bound - interval.lower_bound) / measurement.estimate.point_estimate
            > policy.maximum_relative_interval_width
    });
    let candidate = &candidate.estimate.confidence_interval;
    let lower = candidate.lower_bound / first.upper_bound.max(last.upper_bound);
    let upper = candidate.upper_bound / first.lower_bound.min(last.lower_bound);
    let allowed = 1.0 + policy.allowed_regression;
    if lower > allowed {
        Decision::Regressed(format!(
            "relative interval [{lower:.4}, {upper:.4}] exceeds {allowed:.4}"
        ))
    } else if uncertain {
        // A wide interval cannot certify acceptance. It can still prove a
        // regression when even its most favourable bound exceeds the limit.
        Decision::Inconclusive(
            "measurement uncertainty exceeds the declared precision limit".into(),
        )
    } else if upper <= allowed {
        Decision::Passed
    } else {
        Decision::Inconclusive(format!(
            "relative interval [{lower:.4}, {upper:.4}] straddles {allowed:.4}"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn comparisons_reject_slow_incomplete_incomparable_and_noisy_work() {
        // Deliberate test thresholds exercise the decision rule. Repository
        // acceptance thresholds must separately be qualified by measurement.
        let policy = ComparisonPolicy {
            version: 3,
            baseline_revision: "a".repeat(40),
            hot_path_validity_cases: 1,
            statistic: "median".into(),
            sample_size: 20,
            warm_up_ms: 300,
            measurement_ms: 1000,
            allowed_regression: 0.2,
            allowed_control_drift: 0.1,
            maximum_relative_interval_width: 0.05,
            cases: vec!["fixture".into()],
            boundary_cases: vec!["boundary-fixture".into()],
        };
        let baseline = Measurement {
            estimate: Estimate {
                point_estimate: 100.0,
                confidence_interval: Interval {
                    confidence_level: 0.95,
                    lower_bound: 99.0,
                    upper_bound: 101.0,
                },
            },
            raw_iterations: vec![1.0; 20],
            raw_nanoseconds: vec![100.0; 20],
            work: json!({"input":{"records":64},"work":{"decoded":64}}),
        };
        assert_eq!(
            compare(&baseline, &baseline, &baseline, &policy),
            Decision::Passed
        );
        let mut slow = baseline.clone();
        slow.estimate.point_estimate *= 2.0;
        slow.estimate.confidence_interval.lower_bound *= 2.0;
        slow.estimate.confidence_interval.upper_bound *= 2.0;
        slow.raw_nanoseconds
            .iter_mut()
            .for_each(|time| *time *= 2.0);
        assert!(matches!(
            compare(&baseline, &slow, &baseline, &policy),
            Decision::Regressed(_)
        ));
        let mut incomplete = baseline.clone();
        incomplete.raw_iterations.pop();
        assert!(matches!(
            compare(&baseline, &incomplete, &baseline, &policy),
            Decision::Inconclusive(_)
        ));
        let mut different = baseline.clone();
        different.work["input"]["records"] = json!(63);
        assert!(matches!(
            compare(&baseline, &different, &baseline, &policy),
            Decision::Inconclusive(_)
        ));
        let mut noisy = baseline.clone();
        noisy.estimate.confidence_interval.upper_bound = 140.0;
        assert!(matches!(
            compare(&baseline, &noisy, &baseline, &policy),
            Decision::Inconclusive(_)
        ));
        // Uncertainty must never hide a decisive slowdown. The entire interval
        // is slower than permitted even though it is too wide to certify a pass.
        slow.estimate.confidence_interval.lower_bound = 150.0;
        slow.estimate.confidence_interval.upper_bound = 260.0;
        assert!(matches!(
            compare(&baseline, &slow, &baseline, &policy),
            Decision::Regressed(_)
        ));
        assert!(matches!(
            compare(&baseline, &baseline, &slow, &policy),
            Decision::Inconclusive(_)
        ));
    }
}
