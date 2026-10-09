// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Repository validation owns selection and acceptance, never test execution.
pub(crate) mod dependencies;
mod launcher;
mod performance;
mod plan;
pub(crate) mod prerequisites;
mod process;
mod report;
pub(crate) mod source;

#[cfg(test)]
mod tests;

use crate::{error, Result};
use plan::{Lane, Options, Policy};
use serde::Serialize;
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    fs,
    io::Write,
    path::Path,
    time::{Duration, Instant},
};

#[derive(Debug, Serialize)]
#[serde(tag = "status", content = "detail", rename_all = "snake_case")]
enum Outcome {
    Pending,
    Running,
    Passed,
    Failed(String),
    Incomplete(String),
}

/// A completed check found a defect. Missing tools, reports and unfinished
/// work instead remain incomplete, so neither can be mistaken for acceptance.
#[derive(Debug)]
struct CheckFailed(String);

impl std::fmt::Display for CheckFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for CheckFailed {}

fn failed(message: impl Into<String>) -> Box<dyn std::error::Error> {
    Box::new(CheckFailed(message.into()))
}

#[derive(Serialize)]
struct LaneResult {
    lane: Lane,
    outcome: Outcome,
    elapsed_seconds: f64,
}

#[derive(Serialize)]
struct RunReport {
    version: u32,
    run_id: String,
    started_at_unix_ms: u128,
    source: source::SourceIdentity,
    #[serde(skip_serializing_if = "Option::is_none")]
    final_source: Option<source::SourceIdentity>,
    platform: String,
    requested_scope: String,
    not_requested: Vec<Lane>,
    policy: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    dependency_preparation: Option<Outcome>,
    lanes: Vec<LaneResult>,
    outcome: Outcome,
}

pub(crate) fn run(root: &Path, args: &[String]) -> Result<()> {
    if args.len() == 1 && crate::is_help(&args[0]) {
        println!("cargo xtask test [--lane <lane>]\n\nLanes: default, production-features, test-support, journal-fixtures, doctest, postgres, performance\nOmitting --lane requests all correctness lanes. Use --lane performance for complete performance qualification. Repeated --lane selects exactly the declared scope.\nReports: target/test-runs/<run-id>/report.json. Failed attempts and incomplete required coverage return nonzero. Unrequested lanes supply no acceptance evidence.");
        return Ok(());
    }
    let options = Options::parse(args)?;
    let policy = Policy::read(root)?;
    let summary_path = std::env::var_os("GITHUB_STEP_SUMMARY").map(std::path::PathBuf::from);
    run_native(root, options, policy, summary_path.as_deref(), run_lane)
}

fn run_native(
    root: &Path,
    options: Options,
    policy: Policy,
    summary_path: Option<&Path>,
    execute_lane: impl FnMut(&Path, &Policy, Lane, &[String], &Path, &launcher::Launcher) -> Result<()>,
) -> Result<()> {
    run_native_with_preparation(
        root,
        options,
        policy,
        summary_path,
        execute_lane,
        dependencies::prepare,
    )
}

fn run_native_with_preparation(
    root: &Path,
    options: Options,
    policy: Policy,
    summary_path: Option<&Path>,
    mut execute_lane: impl FnMut(
        &Path,
        &Policy,
        Lane,
        &[String],
        &Path,
        &launcher::Launcher,
    ) -> Result<()>,
    mut prepare: impl FnMut(&Path, &Policy, &Path) -> Result<()>,
) -> Result<()> {
    let _lock = process::lock(root)?;
    let _signals = process::SignalGuard::install()?;
    let run_id = uuid::Uuid::new_v4().to_string();
    let directory = root.join("target/test-runs").join(&run_id);
    fs::create_dir(&directory)?;
    // The always-run CI formatter must select this attempt exactly, including
    // early prerequisite failures. Never search for the newest cached report.
    if let Some(path) = std::env::var_os("GITHUB_OUTPUT") {
        let mut output = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        writeln!(output, "report_directory={}", directory.display())?;
    }
    let mut report = RunReport {
        version: 4,
        run_id,
        started_at_unix_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
        source: source::identity(root)?,
        final_source: None,
        platform: format!("{}-{}", std::env::consts::ARCH, std::env::consts::OS),
        requested_scope: options.scope().into(),
        not_requested: Lane::ALL
            .into_iter()
            .filter(|lane| !options.lanes.contains(lane))
            .collect(),
        policy: native_policy(&policy),
        dependency_preparation: options
            .lanes
            .iter()
            .any(|lane| {
                matches!(
                    lane,
                    Lane::Default | Lane::ProductionFeatures | Lane::TestSupport
                )
            })
            .then_some(Outcome::Pending),
        lanes: options
            .lanes
            .iter()
            .map(|lane| LaneResult {
                lane: *lane,
                outcome: Outcome::Pending,
                elapsed_seconds: 0.0,
            })
            .collect(),
        outcome: Outcome::Running,
    };
    save(&directory, &report)?;
    eprintln!(
        "validation: source={} scope={} artifacts={}",
        report.source.content_sha256,
        report.requested_scope,
        directory.display()
    );
    if report.not_requested.contains(&Lane::Performance) {
        eprintln!(
            "validation: performance=not-requested; this run supplies no performance qualification"
        );
    }
    let prerequisites = (|| {
        // This must precede any Cargo invocation, including inventory builds.
        let launcher = launcher::Launcher::retain(&directory)?;
        let rust = process::capture(
            root,
            &policy,
            "rustc",
            &["--version"],
            &directory,
            "rust-version",
        )?;
        if rust.split_whitespace().nth(1) != Some(policy.rust.as_str()) {
            return Err(error(format!(
                "required rustc {}, found {}; use the pinned toolchain",
                policy.rust,
                rust.trim()
            )));
        }
        let raw = process::capture(
            root,
            &policy,
            "cargo",
            &["metadata", "--locked", "--no-deps", "--format-version", "1"],
            &directory,
            "cargo-metadata",
        )?;
        let production = plan::production_features(&serde_json::from_str(&raw)?, root)?;
        Ok((production, launcher))
    })();
    let (production, launcher) = match prerequisites {
        Ok(features) => features,
        Err(failure) => {
            report.outcome = Outcome::Incomplete(failure.to_string());
            save(&directory, &report)?;
            summary(&report, summary_path)?;
            return Err(failure);
        }
    };
    if report.dependency_preparation.is_some() {
        report.dependency_preparation = Some(Outcome::Running);
        save(&directory, &report)?;
        report.dependency_preparation = Some(match prepare(root, &policy, &directory) {
            Ok(()) => Outcome::Passed,
            Err(failure) => {
                eprintln!("validation: dependency preparation incomplete: {failure}; continuing independent checks");
                Outcome::Incomplete(failure.to_string())
            }
        });
        save(&directory, &report)?;
    }
    for (index, lane) in options.lanes.iter().copied().enumerate() {
        if process::was_interrupted() {
            break;
        }
        report.lanes[index].outcome = Outcome::Running;
        save(&directory, &report)?;
        let phase = directory.join(lane.name());
        fs::create_dir(&phase)?;
        let started = Instant::now();
        let result = execute_lane(root, &policy, lane, &production, &phase, &launcher);
        report.lanes[index].elapsed_seconds = started.elapsed().as_secs_f64();
        report.lanes[index].outcome = match result {
            Ok(()) => Outcome::Passed,
            Err(failure) => {
                eprintln!("validation: {} did not pass: {failure}", lane.name());
                if failure.is::<CheckFailed>() {
                    Outcome::Failed(failure.to_string())
                } else {
                    Outcome::Incomplete(failure.to_string())
                }
            }
        };
        save(&directory, &report)?;
    }
    let source_check = source::identity(root).and_then(|final_source| {
        let unchanged = report.source.same_contents_as(&final_source);
        report.final_source = Some(final_source);
        if unchanged {
            Ok(())
        } else {
            Err(error(
                "source changed during validation; results do not certify the final checkout",
            ))
        }
    });
    report.outcome = if let Err(failure) = source_check {
        Outcome::Incomplete(failure.to_string())
    } else if report
        .dependency_preparation
        .as_ref()
        .is_some_and(|outcome| !matches!(outcome, Outcome::Passed))
    {
        Outcome::Incomplete(
            "dependency preparation did not complete; lane results remain available".into(),
        )
    } else if report
        .lanes
        .iter()
        .all(|lane| matches!(lane.outcome, Outcome::Passed))
    {
        Outcome::Passed
    } else if process::was_interrupted()
        || report.lanes.iter().any(|lane| {
            matches!(
                lane.outcome,
                Outcome::Pending | Outcome::Running | Outcome::Incomplete(_)
            )
        })
    {
        Outcome::Incomplete(
            "one or more required checks are incomplete; see each lane's artifacts".into(),
        )
    } else {
        Outcome::Failed("one or more required checks failed; see each lane's artifacts".into())
    };
    save(&directory, &report)?;
    summary(&report, summary_path)?;
    eprintln!(
        "validation: {:?}; scope={}; report={}",
        report.outcome,
        report.requested_scope,
        directory.join("report.json").display()
    );
    if matches!(report.outcome, Outcome::Passed) {
        Ok(())
    } else {
        Err(error(format!(
            "validation did not pass; report={}",
            directory.join("report.json").display()
        )))
    }
}

fn native_policy(policy: &Policy) -> Value {
    json!({"profile":policy.profile,"nextest":policy.nextest,"rust":policy.rust,"retries":0,"fail_fast":false,
        "leak_timeout":{"period":plan::LEAK_WAIT,"result":"fail"},
        "status_level":"leak","final_status_level":"fail",
        "build_jobs":policy.build_jobs,"nextest_processes":policy.test_threads,"tokio_default_workers":policy.tokio_workers,
        "explicit_tokio_workers":"retained as authored, including the four-worker Studio proof; source identity pins overrides",
        "cargo_incremental":false,"dev_debug":0,"test_debug":0})
}

fn save(directory: &Path, report: &RunReport) -> Result<()> {
    let pending = directory.join("report.pending.json");
    fs::write(&pending, serde_json::to_vec_pretty(report)?)?;
    fs::rename(pending, directory.join("report.json"))?;
    Ok(())
}

fn summary(report: &RunReport, path: Option<&Path>) -> Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    let mut output = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    writeln!(output, "### Validation: {}\n", report.requested_scope)?;
    writeln!(output, "Outcome: {:?}\n", report.outcome)?;
    writeln!(output, "Source: `{}`\n", report.source.content_sha256)?;
    if let Some(preparation) = &report.dependency_preparation {
        writeln!(output, "Dependency preparation: {preparation:?}\n")?;
    }
    writeln!(output, "| Lane | Outcome |\n|---|---|")?;
    for lane in &report.lanes {
        writeln!(output, "| {} | {:?} |", lane.lane.name(), lane.outcome)?;
    }
    for lane in &report.not_requested {
        writeln!(
            output,
            "| {} | Not requested; no acceptance evidence |",
            lane.name()
        )?;
    }
    writeln!(output)?;
    Ok(())
}

fn run_lane(
    root: &Path,
    policy: &Policy,
    lane: Lane,
    production: &[String],
    directory: &Path,
    launcher: &launcher::Launcher,
) -> Result<()> {
    match lane {
        Lane::Default | Lane::ProductionFeatures | Lane::TestSupport | Lane::JournalFixtures => {
            nextest(root, policy, lane, production, directory, launcher)
        }
        Lane::Doctest => {
            let status = process::execute(
                process::command(root, policy, "cargo").args([
                    "test",
                    "--workspace",
                    "--doc",
                    "--locked",
                ]),
                directory,
                "doctest",
                Duration::from_secs(policy.command_watchdog_seconds),
            )?;
            if status.success() {
                Ok(())
            } else {
                Err(failed("doctests failed"))
            }
        }
        Lane::Postgres => {
            let invocation = uuid::Uuid::new_v4().to_string();
            let source = source::identity(root)?;
            let execution = process::execute(
                process::command(root, policy, launcher.path())
                    .arg("__postgres-test")
                    .arg(directory)
                    .arg(&invocation)
                    .arg(&source.content_sha256),
                directory,
                "postgres",
                Duration::from_secs(policy.command_watchdog_seconds),
            );
            // Even interruption or a failed wrapper can leave completed failed
            // targets. Import those facts before deciding overall acceptance.
            let evidence = crate::postgres::evidence::read_validated(
                directory,
                &invocation,
                &source.content_sha256,
            );
            postgres_acceptance(execution, evidence)
        }
        Lane::Performance => performance::run(root, policy, directory),
    }
}

pub(super) fn postgres_acceptance(
    execution: Result<std::process::ExitStatus>,
    evidence: Result<crate::postgres::evidence::Report>,
) -> Result<()> {
    let report = evidence.map_err(|failure| {
        error(format!(
        "PostgreSQL coordinator evidence unavailable or invalid: {failure}; execution={execution:?}"
    ))
    })?;
    let failures = report.known_failures();
    let mut unfinished = report.unfinished_obligations();
    match execution {
        Ok(status) if status.success() && report.passed() => return Ok(()),
        Ok(status) if !status.success() && report.passed() => {
            unfinished.push(format!(
                "coordinator exited {status} despite passing target evidence"
            ));
        }
        Ok(_) => {}
        Err(failure) => unfinished.push(format!("coordinator execution: {failure}")),
    }
    let detail = format!(
        "PostgreSQL known failures={failures:?}; unfinished obligations={unfinished:?}; see coordinator.json"
    );
    if unfinished.is_empty() && !failures.is_empty() {
        Err(failed(detail))
    } else {
        Err(error(detail))
    }
}

fn nextest(
    root: &Path,
    policy: &Policy,
    lane: Lane,
    production: &[String],
    directory: &Path,
    launcher: &launcher::Launcher,
) -> Result<()> {
    let version = process::capture(
        root,
        policy,
        "cargo",
        &["nextest", "--version"],
        directory,
        "nextest-version",
    )?;
    if version.split_whitespace().nth(1) != Some(policy.nextest.as_str()) {
        return Err(error(format!(
            "required cargo-nextest {}, found {}; install the pinned version",
            policy.nextest,
            version.trim()
        )));
    }
    let mut config: toml::Value =
        toml::from_str(&fs::read_to_string(root.join(".config/nextest.toml"))?)?;
    plan::validate_leak_policy(&config)?;
    let expensive = config["profile"]["default"]["overrides"]
        .as_array()
        .and_then(|overrides| {
            overrides.iter().find(|entry| {
                entry.get("test-group").and_then(toml::Value::as_str)
                    == Some("expensive-journal-proofs")
            })
        })
        .and_then(|entry| entry.get("filter"))
        .and_then(toml::Value::as_str)
        .ok_or_else(|| error("expensive proof group has no declared selector"))?
        .to_owned();
    config["profile"][&policy.profile]["junit"]["path"] =
        toml::Value::String(directory.join("junit.xml").to_string_lossy().into_owned());
    let config_path = directory.join("nextest.toml");
    fs::write(&config_path, toml::to_string(&config)?)?;
    let selection = lane.cargo_selection(production);
    let mut common = vec![
        "--locked".to_owned(),
        "--profile".into(),
        policy.profile.clone(),
        "--config-file".into(),
        config_path.to_string_lossy().into_owned(),
        "--user-config-file".into(),
        "none".into(),
    ];
    common.extend(selection);
    let inventory = list(root, policy, lane, directory, &common, None, "inventory")?;
    let expensive_ids = if lane == Lane::JournalFixtures {
        BTreeSet::new()
    } else {
        list(
            root,
            policy,
            lane,
            directory,
            &common,
            Some(&expensive),
            "expensive-inventory",
        )?
        .selected
    };
    if !expensive_ids.is_subset(&inventory.selected) {
        return Err(error(
            "expensive selector includes tests outside full lane coverage",
        ));
    }
    let ordinary: BTreeSet<_> = inventory
        .selected
        .difference(&expensive_ids)
        .cloned()
        .collect();
    fs::write(
        directory.join("selection.json"),
        serde_json::to_vec_pretty(&json!({
            "features":match lane { Lane::ProductionFeatures => production.to_vec(), Lane::TestSupport => vec!["test-support".to_owned(),"obzenflow_infra/warp-server".to_owned()], _ => vec![] },
            "ordinary": ordinary, "expensive": expensive_ids, "excluded": inventory.excluded,
        }))?,
    )?;
    let case_artifacts = directory.join("cases");
    fs::create_dir_all(&case_artifacts)?;
    let admission = prerequisites::admit(
        root,
        policy,
        lane,
        &inventory.selected,
        directory,
        &case_artifacts,
        launcher,
    )?;
    eprintln!(
        "validation: {} required={} runnable={} blocked={}",
        lane.name(),
        admission.required.len(),
        admission.runnable.len(),
        admission.blocked.len()
    );
    if admission.runnable.is_empty() {
        return admission.finish(directory, Ok(()));
    }
    if let Some(filter) = admission.exclusion_filter() {
        let selected = list(
            root,
            policy,
            lane,
            directory,
            &common,
            Some(&filter),
            "runnable-inventory",
        )?;
        if selected.selected != admission.runnable {
            return admission.finish(
                directory,
                Err(error("prerequisite filter changed the runnable inventory")),
            );
        }
        common.extend(["-E".into(), filter]);
    }
    // One Nextest scheduler executes the disjoint selections with the configured
    // expensive-test group, preserving four total process slots and two proofs.
    let mut command = process::command(root, policy, "cargo");
    command.env("OBZENFLOW_TEST_ARTIFACTS", &case_artifacts);
    command
        .args(["nextest", "run"])
        .args(&common)
        .args(nextest_execution_args(policy));
    let execution = process::execute_nextest(
        &mut command,
        directory,
        Duration::from_secs(policy.command_watchdog_seconds),
    )
    .and_then(|status| evaluate_nextest(directory, &admission.runnable, status.success()));
    admission.finish(directory, execution)
}

fn nextest_execution_args(policy: &Policy) -> Vec<String> {
    [
        "--no-fail-fast",
        "--retries",
        "0",
        "--flaky-result",
        "fail",
        "--status-level",
        "leak",
        "--final-status-level",
        "fail",
        "--test-threads",
        &policy.test_threads.to_string(),
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn evaluate_nextest(
    directory: &Path,
    expected: &BTreeSet<plan::TestId>,
    exit_success: bool,
) -> Result<()> {
    let xml = fs::read_to_string(directory.join("junit.xml"))
        .map_err(|e| error(format!("required JUnit report unavailable: {e}")))?;
    let results = report::inspect(&xml)?;
    fs::write(
        directory.join("outcomes.json"),
        serde_json::to_vec_pretty(&results)?,
    )?;
    report::accept(&results, expected, exit_success)
}

fn list(
    root: &Path,
    policy: &Policy,
    lane: Lane,
    directory: &Path,
    common: &[String],
    filter: Option<&str>,
    label: &str,
) -> Result<plan::Inventory> {
    let mut command = process::command(root, policy, "cargo");
    command
        .args(["nextest", "list", "--message-format", "json"])
        .args(common);
    if let Some(filter) = filter {
        command.args(["-E", filter]);
    }
    let status = process::execute(
        &mut command,
        directory,
        label,
        Duration::from_secs(policy.command_watchdog_seconds),
    )?;
    if !status.success() {
        return Err(failed(format!(
            "{label} did not complete; see compiler/Nextest diagnostics"
        )));
    }
    let raw = fs::read_to_string(directory.join(format!("{label}.stdout.log")))?;
    plan::inventory(&serde_json::from_str(&raw)?, lane, policy, filter.is_some())
}
