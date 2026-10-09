// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Criterion executes and samples work. This module plans a requested run,
//! builds identified executables, applies the declared acceptance rule and
//! assembles the evidence of every stage (FLOWIP-080v B7). CI runs the stages
//! on separate machines; `cargo xtask test --lane performance` runs them serially.

mod assembly;
mod driver;
mod schedule;
mod suite;

const MEASUREMENT_CONTRACT: &str = "public-operations-v1";

use super::{failed, plan::Policy, process, settle, source, Outcome};
use crate::{error, Result};
use schedule::{BuildGroup, RunPlan, Side, BOUNDARIES, HOT_PATH, READER_CONTROL, STUDIO_CONTROL};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

pub(super) use suite::smoke;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ComparisonPolicy {
    version: u32,
    /// A deliberate re-qualification against a fixed commit (080v B10).
    #[serde(default)]
    pin: Option<String>,
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
        if policy.version != 5
            || policy.statistic != "median"
            || policy.pin.as_ref().is_some_and(|pin| {
                pin.len() != 40 || !pin.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
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
    census: Option<Box<Executable>>,
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(tag = "outcome", content = "detail", rename_all = "snake_case")]
enum Decision {
    Passed,
    Regressed(String),
    Inconclusive(String),
}

/// The machine a stage ran on, so report rows identify their hardware.
#[derive(Deserialize, Serialize)]
struct Host {
    cpu: String,
    logical_cpus: usize,
    os: String,
    image: String,
}

fn host() -> Host {
    let mut cpu = fs::read_to_string("/proc/cpuinfo").ok().and_then(|info| {
        info.lines()
            .find_map(|line| line.strip_prefix("model name"))
            .and_then(|line| line.split_once(':'))
            .map(|(_, model)| model.trim().to_owned())
    });
    if cpu.is_none() && cfg!(target_os = "macos") {
        cpu = std::process::Command::new("sysctl")
            .args(["-n", "machdep.cpu.brand_string"])
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned());
    }
    Host {
        cpu: cpu.unwrap_or_else(|| "unknown".into()),
        logical_cpus: std::thread::available_parallelism().map_or(0, usize::from),
        os: format!("{}-{}", std::env::consts::ARCH, std::env::consts::OS),
        image: match (std::env::var("ImageOS"), std::env::var("ImageVersion")) {
            (Ok(image), Ok(version)) => format!("{image} {version}"),
            _ => "local".into(),
        },
    }
}

fn now_unix_ms() -> Result<u128> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis())
}

/// One stage's wide record: what ran, where, on which source, which verified
/// executables it used and its outcome. Assembly settles the run from these.
#[derive(Deserialize, Serialize)]
struct StageRecord {
    stage: String,
    name: String,
    run_id: String,
    source: source::SourceIdentity,
    host: Host,
    started_at_unix_ms: u128,
    elapsed_seconds: f64,
    executables: BTreeMap<String, String>,
    outcome: Outcome,
}

/// Runs one stage and records it, including when the stage fails.
fn stage(
    root: &Path,
    plan: &RunPlan,
    (kind, name): (&str, &str),
    directory: &Path,
    body: impl FnOnce(&mut BTreeMap<String, String>) -> Result<()>,
) -> Result<()> {
    fs::create_dir_all(directory)?;
    let source = source::identity(root)?;
    let started_at_unix_ms = now_unix_ms()?;
    let started = Instant::now();
    let mut executables = BTreeMap::new();
    let result = body(&mut executables);
    if let Err(failure) = &result {
        eprintln!("validation: performance {kind} {name} did not pass: {failure}");
    }
    let record = StageRecord {
        stage: kind.into(),
        name: name.into(),
        run_id: plan.run_id.clone(),
        source,
        host: host(),
        started_at_unix_ms,
        elapsed_seconds: started.elapsed().as_secs_f64(),
        executables,
        outcome: Outcome::of(&result),
    };
    fs::write(
        directory.join("stage.json"),
        serde_json::to_vec_pretty(&record)?,
    )?;
    result
}

/// Executables one build group published, with hashes and the failures that
/// left targets without one.
#[derive(Deserialize, Serialize)]
struct Manifest {
    group: String,
    side: Side,
    rust: String,
    reference: String,
    executables: Vec<ManifestEntry>,
    unavailable: BTreeMap<String, Outcome>,
    invocations: Vec<(String, Outcome)>,
}

#[derive(Clone, Deserialize, Serialize)]
struct ManifestEntry {
    target: String,
    census: bool,
    /// Relative to the run's performance directory.
    file: String,
    sha256: String,
    compiled_manifest_dir: PathBuf,
}

/// Identified executables, every compilation obligation's outcome, and why
/// each target or census without an executable is unavailable.
#[derive(Default)]
struct Builds {
    executables: BTreeMap<String, Executable>,
    unavailable: BTreeMap<String, Outcome>,
    invocations: Vec<(String, Outcome)>,
}

impl Builds {
    fn outcome(&self) -> Outcome {
        Outcome::of(&settle(&self.invocations))
    }

    /// A reference that cannot compile the candidate's benchmark driver is
    /// incomparable rather than a candidate defect (080v B10).
    fn incomparable(mut self, commit: &str) -> Self {
        let diagnose = |outcome: &mut Outcome| {
            if let Outcome::Failed(detail) = outcome {
                *outcome = Outcome::Incomplete(format!(
                    "{detail}; reference {commit} cannot compile the candidate's benchmark driver; if a public API the benchmarks use changed, add a versioned adapter for that reference or pin a reference"
                ));
            }
        };
        self.unavailable.values_mut().for_each(diagnose);
        self.invocations
            .iter_mut()
            .for_each(|(_, outcome)| diagnose(outcome));
        self
    }
}

/// One Cargo invocation's identified executables and its own result.
struct Compiled {
    executables: BTreeMap<String, Executable>,
    outcome: Outcome,
}

/// Plans the run and names its stages, for CI to fan out.
pub(super) fn plan_stage(root: &Path, run_id: &str, directory: &Path) -> Result<Value> {
    let plan = write_plan(root, run_id, directory)?;
    let names = |names: Vec<&String>| json!(names);
    Ok(json!({
        "builds": names(plan.builds.iter().map(|group| &group.name).collect()),
        "qualification": names(plan.qualification.iter().map(|shard| &shard.name).collect()),
        "suite": names(plan.suite.iter().map(|shard| &shard.name).collect()),
    }))
}

fn write_plan(root: &Path, run_id: &str, directory: &Path) -> Result<RunPlan> {
    fs::create_dir_all(directory)?;
    let policy = ComparisonPolicy::read(root)?;
    fs::write(
        directory.join("comparison-policy.json"),
        serde_json::to_vec_pretty(&policy)?,
    )?;
    let plan = schedule::plan(root, run_id, &policy)?;
    fs::write(
        directory.join(schedule::PLAN),
        serde_json::to_vec_pretty(&plan)?,
    )?;
    Ok(plan)
}

pub(super) fn build_stage(
    root: &Path,
    tools: &Policy,
    directory: &Path,
    name: &str,
    jobs: usize,
) -> Result<()> {
    let plan = schedule::read(directory)?;
    let group = plan
        .builds
        .iter()
        .find(|group| group.name == name)
        .ok_or_else(|| error(format!("{name}: not a planned build group")))?;
    let output = directory.join("build").join(&group.name);
    stage(root, &plan, ("build", name), &output, |_| {
        let rust = process::capture(root, tools, "rustc", &["-Vv"], &output, "rust-version")?;
        if rust.split_whitespace().nth(1) != Some(tools.rust.as_str()) {
            return Err(error(format!(
                "required rustc {}, found {}; use the pinned toolchain",
                tools.rust,
                rust.lines().next().unwrap_or_default()
            )));
        }
        let builds = match group.side {
            Side::Reference => {
                match prepare_reference(root, tools, &output, &plan.reference.commit) {
                    Ok((source, target)) => {
                        let workspace = Workspace {
                            source: &source,
                            target: &target,
                            jobs,
                        };
                        build_group(root, tools, &output, &workspace, group)
                            .incomparable(&plan.reference.commit)
                    }
                    Err(failure) => {
                        // Nothing compiled; every executable shares the diagnosis.
                        let outcome = Outcome::of(&Err(failure));
                        Builds {
                            executables: BTreeMap::new(),
                            unavailable: group
                                .targets
                                .iter()
                                .map(|target| (target.clone(), outcome.clone()))
                                .collect(),
                            invocations: vec![("reference-preparation".into(), outcome)],
                        }
                    }
                }
            }
            Side::Candidate => {
                let target = root.join("target/validation-candidate");
                let workspace = Workspace {
                    source: root,
                    target: &target,
                    jobs,
                };
                build_group(root, tools, &output, &workspace, group)
            }
        };
        let mut entries = Vec::new();
        for (target, binary) in &builds.executables {
            for (census, binary) in [(false, Some(binary)), (true, binary.census.as_deref())] {
                if let Some(binary) = binary {
                    entries.push(ManifestEntry {
                        target: target.clone(),
                        census,
                        file: binary
                            .path
                            .strip_prefix(directory)?
                            .to_string_lossy()
                            .into_owned(),
                        sha256: binary.sha256.clone(),
                        compiled_manifest_dir: binary.compiled_manifest_dir.clone(),
                    });
                }
            }
        }
        let manifest = Manifest {
            group: group.name.clone(),
            side: group.side,
            rust: rust.trim().into(),
            reference: plan.reference.commit.clone(),
            executables: entries,
            unavailable: builds.unavailable.clone(),
            invocations: builds.invocations.clone(),
        };
        fs::write(
            output.join("executables.json"),
            serde_json::to_vec_pretty(&manifest)?,
        )?;
        builds.outcome().into_result()
    })
}

pub(super) fn qualify_stage(
    root: &Path,
    tools: &Policy,
    directory: &Path,
    name: &str,
) -> Result<()> {
    let plan = schedule::read(directory)?;
    let shard = plan
        .qualification
        .iter()
        .find(|shard| shard.name == name)
        .ok_or_else(|| error(format!("{name}: not a planned qualification shard")))?;
    let output = directory.join("qualification").join(&shard.name);
    stage(root, &plan, ("qualification", name), &output, |verified| {
        let policy = ComparisonPolicy::read(root)?;
        let mut pairs = Vec::new();
        for (target, gated) in [
            (HOT_PATH, &policy.cases),
            (BOUNDARIES, &policy.boundary_cases),
        ] {
            let cases: Vec<_> = gated
                .iter()
                .filter(|case| shard.cases.contains(case))
                .cloned()
                .collect();
            if cases.is_empty() {
                continue;
            }
            pairs.push(Gated {
                target,
                cases,
                all: gated.clone(),
                reference: load(directory, Side::Reference, target, verified)?,
                candidate: load(directory, Side::Candidate, target, verified)?,
            });
        }
        qualify(root, tools, &output, &policy, &plan, &pairs)
    })
}

pub(super) fn measure_stage(
    root: &Path,
    tools: &Policy,
    directory: &Path,
    name: &str,
) -> Result<()> {
    let plan = schedule::read(directory)?;
    let shard = plan
        .suite
        .iter()
        .find(|shard| shard.name == name)
        .ok_or_else(|| error(format!("{name}: not a planned suite shard")))?;
    let output = directory.join("suite").join(&shard.name);
    stage(root, &plan, ("suite", name), &output, |verified| {
        suite::run_shard(root, tools, directory, &output, &plan, shard, verified)
    })
}

pub(super) fn assemble_stage(directory: &Path) -> Result<()> {
    settle(&assembly::assemble(directory)?)
}

/// Executes the whole plan on one machine: builds overlap, measurements never do.
pub(super) fn run(root: &Path, tools: &Policy, directory: &Path) -> Result<()> {
    let run_id = directory
        .parent()
        .and_then(Path::file_name)
        .ok_or_else(|| error("missing performance run identity"))?
        .to_string_lossy()
        .into_owned();
    let plan = write_plan(root, &run_id, directory)?;
    // Two Cargo processes share the declared compilation budget, never target
    // directories. Join both before any benchmark process can start.
    let jobs = (tools.build_jobs / 2).max(1);
    let report = |kind: &str, name: &str, result: Result<()>| {
        if let Err(failure) = result {
            eprintln!("validation: {kind} {name} recorded as not passed: {failure}");
        }
    };
    std::thread::scope(|scope| {
        let reference = scope.spawn(|| {
            for group in plan.builds.iter().filter(|g| g.side == Side::Reference) {
                if !process::was_interrupted() {
                    report(
                        "build",
                        &group.name,
                        build_stage(root, tools, directory, &group.name, jobs),
                    );
                }
            }
        });
        for group in plan.builds.iter().filter(|g| g.side == Side::Candidate) {
            if !process::was_interrupted() {
                report(
                    "build",
                    &group.name,
                    build_stage(root, tools, directory, &group.name, jobs),
                );
            }
        }
        if reference.join().is_err() {
            eprintln!("validation: reference build thread panicked; its groups stay unreported");
        }
    });
    for shard in &plan.qualification {
        if !process::was_interrupted() {
            report(
                "qualification",
                &shard.name,
                qualify_stage(root, tools, directory, &shard.name),
            );
        }
    }
    for shard in &plan.suite {
        if !process::was_interrupted() {
            report(
                "suite",
                &shard.name,
                measure_stage(root, tools, directory, &shard.name),
            );
        }
    }
    assemble_stage(directory)
}

fn prepare_reference(
    root: &Path,
    tools: &Policy,
    directory: &Path,
    commit: &str,
) -> Result<(PathBuf, PathBuf)> {
    let baseline = directory.join("baseline-source");
    fs::create_dir(&baseline)?;
    let archive = directory.join("baseline.tar");
    let status = process::execute(
        process::command(root, tools, "git")
            .args(["archive", "--format=tar", "--output"])
            .arg(&archive)
            .arg(commit),
        directory,
        "baseline-source",
        Duration::from_secs(60),
    )?;
    if !status.success() {
        return Err(error(format!("reference {commit} is unavailable locally; its history is required for a comparable performance run")));
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
            "could not extract the identified performance reference",
        ));
    }
    driver::install(root, &baseline, directory, commit)?;
    driver::reference(root, &baseline, directory)
}

/// One side's source tree, its own target directory and its compilation budget.
struct Workspace<'a> {
    source: &'a Path,
    target: &'a Path,
    jobs: usize,
}

fn build_group(
    root: &Path,
    tools: &Policy,
    directory: &Path,
    workspace: &Workspace,
    group: &BuildGroup,
) -> Builds {
    build_invocations(
        group,
        |label, names, features| build(root, tools, directory, workspace, label, (names, features)),
        process::was_interrupted,
    )
}

/// Compiles the group, then its separate census, continuing after rejection.
/// Interruption stops new launches; prior executables remain.
fn build_invocations(
    group: &BuildGroup,
    mut compile: impl FnMut(&str, &[String], &str) -> Compiled,
    interrupted: impl Fn() -> bool,
) -> Builds {
    // Do not unify allocation-census into timed binaries, or silently alter a
    // target's declared feature identity.
    let mut census_features = group.features.clone();
    census_features.push("allocation-census".into());
    let mut invocations = vec![(
        format!("{}-build", group.name),
        group.targets.clone(),
        group.features.join(","),
        false,
    )];
    if group.census {
        invocations.push((
            format!("{}-census-build", group.name),
            vec![HOT_PATH.to_owned()],
            census_features.join(","),
            true,
        ));
    }
    let mut builds = Builds::default();
    let mut census = None;
    for (name, names, features, instrumented) in invocations {
        let Compiled {
            mut executables,
            outcome,
        } = if interrupted() {
            Compiled {
                executables: BTreeMap::new(),
                outcome: Outcome::Incomplete("interrupted before compilation".into()),
            }
        } else {
            compile(&name, &names, &features)
        };
        for target in names {
            match executables.remove(&target) {
                Some(binary) if instrumented => census = Some(binary),
                Some(binary) => {
                    builds.executables.insert(target, binary);
                }
                None => {
                    let unavailable = match &outcome {
                        Outcome::Passed => Outcome::Incomplete(format!(
                            "{name}: no unambiguous executable for {target}"
                        )),
                        other => other.clone(),
                    };
                    let key = if instrumented {
                        format!("{target} census")
                    } else {
                        target
                    };
                    builds.unavailable.insert(key, unavailable);
                }
            }
        }
        builds.invocations.push((name, outcome));
    }
    if let (Some(census), Some(binary)) = (census, builds.executables.get_mut(HOT_PATH)) {
        binary.census = Some(Box::new(census));
    }
    builds
}

/// Compiles one feature group with `--keep-going`. Executables Cargo still
/// identifies after a rejection are kept; the rejection itself is not erased.
fn build(
    root: &Path,
    tools: &Policy,
    directory: &Path,
    workspace: &Workspace,
    label: &str,
    (targets, features): (&[String], &str),
) -> Compiled {
    let source = workspace.source;
    let mut command = process::command(root, tools, "cargo");
    command
        .env("CARGO_TARGET_DIR", workspace.target)
        .env("CARGO_BUILD_JOBS", workspace.jobs.to_string())
        .args([
            "build",
            "--profile",
            "bench",
            "--keep-going",
            "--locked",
            "--message-format=json",
            "--manifest-path",
        ])
        .arg(source.join("Cargo.toml"))
        .args(["--package", "obzenflow_benchmarks", "--features", features]);
    for target in targets {
        command.args(["--bench", target]);
    }
    let mut outcome = match process::execute(
        &mut command,
        directory,
        label,
        Duration::from_secs(tools.command_watchdog_seconds),
    ) {
        Ok(status) if status.success() => Outcome::Passed,
        Ok(status) => Outcome::Failed(format!("{label}: compilation rejected ({status})")),
        Err(failure) => Outcome::Incomplete(failure.to_string()),
    };
    let stdout = fs::read_to_string(directory.join(format!("{label}.stdout.log")));
    let found = stdout
        .map_err(Into::into)
        .and_then(|stdout| identified(&stdout, targets, outcome == Outcome::Passed));
    let mut executables = BTreeMap::new();
    match found {
        Ok(found) => {
            for (target, path) in found {
                match preserve(source, directory, label, &target, &path) {
                    Ok(executable) => {
                        executables.insert(target, executable);
                    }
                    Err(failure) if outcome == Outcome::Passed => {
                        outcome = Outcome::Incomplete(format!("{label}: {target}: {failure}"));
                    }
                    Err(_) => {}
                }
            }
        }
        Err(failure) if outcome == Outcome::Passed => {
            outcome = Outcome::Incomplete(format!("{label}: unreadable Cargo output: {failure}"));
        }
        Err(_) => {}
    }
    if outcome == Outcome::Passed {
        if let Some(target) = targets
            .iter()
            .find(|target| !executables.contains_key(*target))
        {
            outcome =
                Outcome::Incomplete(format!("{label}: no unambiguous executable for {target}"));
        }
    }
    Compiled {
        executables,
        outcome,
    }
}

/// Unambiguous executables Cargo reported for the requested targets, including
/// fresh artefacts. Output from a failed or interrupted invocation may end in a
/// truncated line; a successful invocation's output must parse completely.
fn identified(
    stdout: &str,
    targets: &[String],
    complete: bool,
) -> Result<BTreeMap<String, PathBuf>> {
    let mut artifacts: BTreeMap<String, BTreeSet<PathBuf>> = BTreeMap::new();
    for line in stdout.lines() {
        let message: Value = match serde_json::from_str(line) {
            Ok(message) => message,
            Err(failure) if complete => return Err(failure.into()),
            Err(_) => continue,
        };
        if message["reason"] == "compiler-artifact" {
            if let (Some(target), Some(path)) = (
                message["target"]["name"].as_str(),
                message["executable"].as_str(),
            ) {
                if targets.iter().any(|name| name == target) {
                    artifacts
                        .entry(target.into())
                        .or_default()
                        .insert(PathBuf::from(path));
                }
            }
        }
    }
    let mut unique = BTreeMap::new();
    for (target, paths) in artifacts {
        let mut paths = paths.into_iter();
        if let (Some(path), None) = (paths.next(), paths.next()) {
            unique.insert(target, path);
        }
    }
    Ok(unique)
}

/// Copy and hash before a later feature build can replace the output path.
fn preserve(
    source: &Path,
    directory: &Path,
    label: &str,
    target: &str,
    path: &Path,
) -> Result<Executable> {
    let preserved = directory.join(format!("{label}-{target}.executable"));
    fs::copy(path, &preserved)?;
    let executable = Executable {
        census: None,
        sha256: driver::sha256(&fs::read(&preserved)?),
        path: preserved,
        compiled_manifest_dir: source.join("crates/obzenflow_benchmarks"),
    };
    fs::write(
        directory.join(format!("{label}-{target}.executable.json")),
        serde_json::to_vec_pretty(&executable)?,
    )?;
    Ok(executable)
}

fn manifests(directory: &Path) -> Result<Vec<Manifest>> {
    let mut manifests = Vec::new();
    let builds = directory.join("build");
    if !builds.is_dir() {
        return Ok(manifests);
    }
    for entry in fs::read_dir(builds)? {
        let path = entry?.path().join("executables.json");
        if path.is_file() {
            manifests.push(serde_json::from_slice(&fs::read(path)?)?);
        }
    }
    Ok(manifests)
}

/// A published executable may run only if its bytes match its manifest hash.
fn verify(
    directory: &Path,
    entry: &ManifestEntry,
    verified: &mut BTreeMap<String, String>,
) -> Result<Executable> {
    let path = directory.join(&entry.file);
    let bytes = fs::read(&path)
        .map_err(|failure| error(format!("{}: executable unavailable: {failure}", entry.file)))?;
    let sha256 = driver::sha256(&bytes);
    if sha256 != entry.sha256 {
        return Err(failed(format!(
            "{}: executable hash differs from its build manifest",
            entry.file
        )));
    }
    #[cfg(unix)]
    {
        // Artefact transport drops modes; the verified bytes are what runs.
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755))?;
    }
    verified.insert(entry.file.clone(), sha256.clone());
    Ok(Executable {
        path,
        compiled_manifest_dir: entry.compiled_manifest_dir.clone(),
        sha256,
        census: None,
    })
}

/// One side's timing executable for `target`, with its census when published.
fn load(
    directory: &Path,
    side: Side,
    target: &str,
    verified: &mut BTreeMap<String, String>,
) -> Result<Executable> {
    let mut timing = None;
    let mut census = None;
    let mut unavailable = None;
    for manifest in manifests(directory)?.into_iter().filter(|m| m.side == side) {
        for entry in manifest.executables.iter().filter(|e| e.target == target) {
            let executable = verify(directory, entry, verified)?;
            if entry.census {
                census = Some(executable);
            } else {
                timing = Some(executable);
            }
        }
        unavailable = unavailable.or_else(|| manifest.unavailable.get(target).cloned());
    }
    let Some(mut timing) = timing else {
        let side = format!("{side:?}").to_lowercase();
        return Err(match unavailable {
            Some(Outcome::Failed(detail)) => failed(format!("{side} {target}: {detail}")),
            Some(other) => error(format!("{side} {target} unavailable: {other:?}")),
            None => error(format!(
                "no build manifest provides the {side} {target} executable"
            )),
        });
    };
    timing.census = census.map(Box::new);
    Ok(timing)
}

/// A gated target's cases in this shard and both implementations.
struct Gated {
    target: &'static str,
    cases: Vec<String>,
    all: Vec<String>,
    reference: Executable,
    candidate: Executable,
}

fn qualify(
    root: &Path,
    tools: &Policy,
    directory: &Path,
    policy: &ComparisonPolicy,
    plan: &RunPlan,
    pairs: &[Gated],
) -> Result<()> {
    for pair in pairs {
        if pair.reference.sha256 == pair.candidate.sha256 {
            return Err(error(
                "reference and candidate resolved to the same executable; comparison is invalid",
            ));
        }
    }
    let hot = pairs.iter().find(|pair| pair.target == HOT_PATH);
    if let Some(hot) = hot {
        validate_workloads(
            root,
            tools,
            policy,
            directory,
            &hot.reference,
            &hot.candidate,
        )?;
    }
    let nonce = &plan.run_id;
    let mut before = BTreeMap::new();
    let mut candidate = BTreeMap::new();
    let mut after = BTreeMap::new();
    // Adjacent per-case trials avoid placing the unchanged reference several
    // whole suites away from its candidate. Both implementations are already
    // built; no compiler work overlaps these measurements.
    for pair in pairs {
        let suite = if pair.target == HOT_PATH {
            "hot-path"
        } else {
            "boundaries"
        };
        for (index, case) in pair.all.iter().enumerate() {
            if !pair.cases.contains(case) {
                continue;
            }
            let selected = ComparisonPolicy {
                cases: vec![case.clone()],
                ..policy.clone()
            };
            for (phase, binary, results) in [
                ("before", &pair.reference, &mut before),
                ("candidate", &pair.candidate, &mut candidate),
                ("after", &pair.reference, &mut after),
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
    for case in pairs.iter().flat_map(|pair| &pair.cases) {
        decisions.insert(
            case.clone(),
            compare(&before[case], &candidate[case], &after[case], policy),
        );
    }
    let mut suites = BTreeMap::new();
    for pair in pairs {
        let features = &plan.target(pair.target)?.features;
        suites.insert(
            pair.target,
            json!({"features": features, "cases": pair.cases}),
        );
    }
    fs::write(
        directory.join("comparison.json"),
        serde_json::to_vec_pretty(&json!({
            "reference": plan.reference,
            "profile": "bench", "suites": suites,
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
        policy,
        directory,
        nonce,
        pairs,
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
        let instrumented = binary
            .census
            .as_deref()
            .ok_or_else(|| error(format!("{label}: allocation census executable unavailable")))?;
        let status = process::execute(
            process::command(root, tools, &instrumented.path)
                .env("OBZENFLOW_WORK_CENSUS", &census)
                .env_remove("OBZENFLOW_BENCH_CONTROL")
                .env_remove(suite::DECLARATIONS)
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

/// Runs the rejection controls belonging to this shard's cases: the slow and
/// missing reader controls reuse the complete-reader case's adjacent trials.
fn qualify_controls(
    root: &Path,
    tools: &Policy,
    policy: &ComparisonPolicy,
    directory: &Path,
    nonce: &str,
    pairs: &[Gated],
    (before, after): (
        &BTreeMap<String, Measurement>,
        &BTreeMap<String, Measurement>,
    ),
) -> Result<()> {
    let mut record = serde_json::Map::new();
    let mut passed = true;
    let rejected = |label: &str, result: &Result<BTreeMap<String, Measurement>>, expected: &str| {
        let process: Value = serde_json::from_slice(&fs::read(
            directory.join(label).join("criterion.result.json"),
        )?)?;
        let diagnostic = fs::read_to_string(directory.join(label).join("criterion.stderr.log"))?;
        Ok::<_, Box<dyn std::error::Error>>((
            result.is_err() && process["exit_code"] == 101 && diagnostic.contains(expected),
            process,
        ))
    };
    if let Some(pair) = pairs
        .iter()
        .find(|pair| pair.cases.iter().any(|case| case == READER_CONTROL))
    {
        let selected = ComparisonPolicy {
            cases: vec![READER_CONTROL.into()],
            ..policy.clone()
        };
        let slow = measure(
            root,
            tools,
            &selected,
            directory,
            &pair.candidate,
            &format!("{nonce}-slow-control"),
            Some("slow-reader"),
        )?;
        let slowdown = compare(
            &before[READER_CONTROL],
            &slow[READER_CONTROL],
            &after[READER_CONTROL],
            policy,
        );
        let label = format!("{nonce}-missing-work-control");
        let missing = measure(
            root,
            tools,
            &selected,
            directory,
            &pair.candidate,
            &label,
            Some("missing-reader-output"),
        );
        let (missing_rejected, process) = rejected(
            &label,
            &missing,
            "actual reader output completeness and order",
        )?;
        passed &= matches!(slowdown, Decision::Regressed(_)) && missing_rejected;
        record.insert("case".into(), json!(READER_CONTROL));
        record.insert("slowdown".into(), json!(slowdown));
        record.insert("slow_measurement".into(), json!(slow));
        record.insert(
            "missing_work_rejected_by_completion_oracle".into(),
            json!(missing_rejected),
        );
        record.insert("missing_work_process".into(), process);
    }
    if let Some(pair) = pairs
        .iter()
        .find(|pair| pair.cases.iter().any(|case| case == STUDIO_CONTROL))
    {
        let studio = ComparisonPolicy {
            cases: vec![STUDIO_CONTROL.into()],
            ..policy.clone()
        };
        let label = format!("{nonce}-missing-studio-control");
        let missing = measure(
            root,
            tools,
            &studio,
            directory,
            &pair.candidate,
            &label,
            Some("missing-studio-output"),
        );
        let (studio_rejected, process) = rejected(
            &label,
            &missing,
            "Studio projection output completeness and order",
        )?;
        passed &= studio_rejected;
        record.insert(
            "missing_studio_projection_output_rejected".into(),
            json!(studio_rejected),
        );
        record.insert("missing_studio_process".into(), process);
    }
    if record.is_empty() {
        return Ok(());
    }
    record.insert(
        "acceptance_measurements_contain_no_controls".into(),
        json!(true),
    );
    fs::write(
        directory.join("negative-controls.json"),
        serde_json::to_vec_pretty(&record)?,
    )?;
    if !passed {
        return Err(error("performance gate failed its live slowdown/missing-work qualification; see negative-controls.json"));
    }
    Ok(())
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
    if binary.census.is_none() {
        command.env("OBZENFLOW_WORK_CENSUS", &census);
    } else {
        command.env_remove("OBZENFLOW_WORK_CENSUS");
    }
    command
        .env("CRITERION_HOME", directory.join("criterion-comparison"))
        .env_remove("OBZENFLOW_BENCH_CONTROL")
        .env_remove(suite::DECLARATIONS)
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
    // Allocation atomics are excluded from the timing executable. Qualify the
    // same selected operation separately through its existing output oracle.
    if let Some(instrumented) = &binary.census {
        let mut command = process::command(root, tools, &instrumented.path);
        command
            .env("OBZENFLOW_WORK_CENSUS", &census)
            .env_remove("OBZENFLOW_BENCH_CONTROL")
            .env_remove(suite::DECLARATIONS)
            .args(["--test", &policy.filter()]);
        if let Some(control) = control {
            command.env("OBZENFLOW_BENCH_CONTROL", control);
        }
        let status = process::execute(
            &mut command,
            &artifacts,
            "census",
            Duration::from_secs(tools.command_watchdog_seconds),
        )?;
        if !status.success() {
            return Err(failed(format!(
                "{label}: separate census rejected the workload"
            )));
        }
    }
    let mut work = read_work(&census, binary)?;
    let expected: BTreeSet<_> = policy.cases.iter().cloned().collect();
    if work.keys().cloned().collect::<BTreeSet<_>>() != expected {
        return Err(error(
            "work census does not cover the selected Criterion cases exactly",
        ));
    }
    let mut found = BTreeMap::new();
    collect_samples(&directory.join("criterion-comparison"), label, &mut found)?;
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
        let work = work
            .remove(&case)
            .ok_or_else(|| error(format!("{case}: no work census")))?;
        let measurement = Measurement {
            estimate: serde_json::from_value(estimates[&policy.statistic].clone())?,
            raw_iterations: iterations,
            raw_nanoseconds: times,
            work,
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
    if census["measurement_contract"] != MEASUREMENT_CONTRACT {
        return Err(error(
            "benchmark measurement contract differs; requalification required",
        ));
    }
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
    use crate::validation::CheckFailed;

    fn executable(name: &str) -> Executable {
        Executable {
            path: PathBuf::from(name),
            compiled_manifest_dir: PathBuf::from("/candidate"),
            sha256: name.into(),
            census: None,
        }
    }

    fn group(targets: &[&str], census: bool) -> BuildGroup {
        BuildGroup {
            name: "candidate-journal-benchmarks".into(),
            side: Side::Candidate,
            features: vec!["journal-benchmarks".into()],
            targets: targets.iter().map(|t| (*t).to_owned()).collect(),
            census,
        }
    }

    #[test]
    fn rejected_invocation_keeps_identified_executables_only() {
        let artifact = |target: &str, path: &str, fresh: bool| {
            json!({"reason":"compiler-artifact","target":{"name":target},"executable":path,"fresh":fresh})
                .to_string()
        };
        let targets = [
            "latency".to_owned(),
            "rejected".to_owned(),
            "twice".to_owned(),
        ];
        let output = [
            artifact("latency", "/out/latency-1", true),
            artifact("unrequested", "/out/unrequested-1", false),
            artifact("twice", "/out/twice-1", false),
            artifact("twice", "/out/twice-2", false),
            r#"{"reason":"compiler-message","message":{"rendered":"error"}}"#.to_owned(),
            r#"{"reason":"build-finished","success":fal"#.to_owned(),
        ]
        .join("\n");
        let found = identified(&output, &targets, false).unwrap();
        assert_eq!(
            found,
            BTreeMap::from([("latency".to_owned(), PathBuf::from("/out/latency-1"))]),
            "fresh executables count; ambiguous and absent targets do not"
        );
        assert!(
            identified(&output, &targets, true).is_err(),
            "a successful invocation must emit complete Cargo messages"
        );
    }

    #[test]
    fn census_failure_keeps_the_timing_binary_and_blocks_qualification() {
        let mut launched = Vec::new();
        let builds = build_invocations(
            &group(&[HOT_PATH, "other"], true),
            |label, names, _| {
                launched.push(label.to_owned());
                // The group is rejected yet Cargo still identifies one executable;
                // the census build is rejected outright.
                let mut executables = BTreeMap::new();
                let outcome = if label.ends_with("census-build") {
                    Outcome::Failed(format!("{label}: compilation rejected"))
                } else {
                    executables.insert(names[0].clone(), executable(&names[0]));
                    Outcome::Failed(format!("{label}: compilation rejected"))
                };
                Compiled {
                    executables,
                    outcome,
                }
            },
            || false,
        );
        assert_eq!(launched.len(), 2, "the census build still runs");
        assert!(builds.executables[HOT_PATH].census.is_none());
        assert!(matches!(builds.unavailable["other"], Outcome::Failed(_)));
        assert!(matches!(
            builds.unavailable[&format!("{HOT_PATH} census")],
            Outcome::Failed(_)
        ));
        assert!(matches!(builds.outcome(), Outcome::Failed(_)));
    }

    #[test]
    fn reference_that_cannot_compile_the_driver_is_incomparable() {
        let rejected =
            || Outcome::Failed("reference-journal-benchmarks-build: compilation rejected".into());
        let builds = Builds {
            executables: BTreeMap::new(),
            unavailable: BTreeMap::from([(HOT_PATH.to_owned(), rejected())]),
            invocations: vec![("build".into(), rejected())],
        }
        .incomparable("abc123");
        assert!(
            matches!(&builds.unavailable[HOT_PATH], Outcome::Incomplete(detail) if detail.contains("versioned adapter"))
        );
        assert!(matches!(builds.outcome(), Outcome::Incomplete(_)));
    }

    #[test]
    fn interruption_stops_new_compilation_and_keeps_prior_executables() {
        let interrupted = std::cell::Cell::new(false);
        let builds = build_invocations(
            &group(&[HOT_PATH], true),
            |_, names, _| {
                interrupted.set(true);
                Compiled {
                    executables: names
                        .iter()
                        .map(|name| (name.clone(), executable(name)))
                        .collect(),
                    outcome: Outcome::Passed,
                }
            },
            || interrupted.get(),
        );
        assert!(builds.executables.contains_key(HOT_PATH));
        assert_eq!(
            builds.unavailable[&format!("{HOT_PATH} census")],
            Outcome::Incomplete("interrupted before compilation".into())
        );
        assert!(matches!(builds.outcome(), Outcome::Incomplete(_)));
    }

    #[test]
    fn consumers_verify_published_executables_before_running_them() {
        let directory = tempfile::tempdir().unwrap();
        let group = directory.path().join("build/candidate-journal-benchmarks");
        fs::create_dir_all(&group).unwrap();
        fs::write(group.join("timing.executable"), b"timing").unwrap();
        fs::write(group.join("census.executable"), b"census").unwrap();
        let entry = |census: bool, file: &str, bytes: &[u8]| ManifestEntry {
            target: HOT_PATH.into(),
            census,
            file: format!("build/candidate-journal-benchmarks/{file}"),
            sha256: driver::sha256(bytes),
            compiled_manifest_dir: PathBuf::from("/candidate"),
        };
        let manifest = |entries: Vec<ManifestEntry>| Manifest {
            group: "candidate-journal-benchmarks".into(),
            side: Side::Candidate,
            rust: "rustc".into(),
            reference: "a".repeat(40),
            executables: entries,
            unavailable: BTreeMap::from([("other".into(), Outcome::Failed("rejected".into()))]),
            invocations: Vec::new(),
        };
        let write = |manifest: Manifest| {
            fs::write(
                group.join("executables.json"),
                serde_json::to_vec(&manifest).unwrap(),
            )
            .unwrap()
        };
        write(manifest(vec![
            entry(false, "timing.executable", b"timing"),
            entry(true, "census.executable", b"census"),
        ]));
        let mut verified = BTreeMap::new();
        let loaded = load(directory.path(), Side::Candidate, HOT_PATH, &mut verified).unwrap();
        assert!(loaded.census.is_some());
        assert_eq!(verified.len(), 2);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&loaded.path).unwrap().permissions().mode() & 0o111,
                0o111
            );
        }
        let unavailable = load(directory.path(), Side::Candidate, "other", &mut verified)
            .err()
            .unwrap();
        assert!(
            unavailable.is::<CheckFailed>(),
            "a rejected build stays a failure"
        );
        assert!(load(directory.path(), Side::Reference, HOT_PATH, &mut verified).is_err());
        write(manifest(vec![entry(
            false,
            "timing.executable",
            b"different",
        )]));
        let tampered = load(directory.path(), Side::Candidate, HOT_PATH, &mut verified)
            .err()
            .unwrap();
        assert!(tampered.is::<CheckFailed>());
        assert!(tampered
            .to_string()
            .contains("hash differs from its build manifest"));
    }

    #[test]
    fn census_rejects_previous_contract_and_wrong_source() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("work.json");
        let binary = Executable {
            path: PathBuf::new(),
            compiled_manifest_dir: PathBuf::from("/candidate"),
            census: None,
            sha256: String::new(),
        };
        let mut census = json!({
            "measurement_contract": MEASUREMENT_CONTRACT,
            "compiled_manifest_dir": "/candidate",
            "cases": [{"case":"reader","input":{"records":64},"work":{"complete_records":64}}]
        });
        fs::write(&path, serde_json::to_vec(&census).unwrap()).unwrap();
        assert!(read_work(&path, &binary).is_ok());
        census["measurement_contract"] = json!("instrumented-v3");
        fs::write(&path, serde_json::to_vec(&census).unwrap()).unwrap();
        assert!(read_work(&path, &binary)
            .unwrap_err()
            .to_string()
            .contains("measurement contract"));
        census["measurement_contract"] = json!(MEASUREMENT_CONTRACT);
        census["compiled_manifest_dir"] = json!("/reference");
        fs::write(&path, serde_json::to_vec(&census).unwrap()).unwrap();
        assert!(read_work(&path, &binary)
            .unwrap_err()
            .to_string()
            .contains("different source tree"));
    }

    #[test]
    fn comparisons_reject_slow_incomplete_incomparable_and_noisy_work() {
        // Deliberate test thresholds exercise the decision rule. Repository
        // acceptance thresholds must separately be qualified by measurement.
        let policy = ComparisonPolicy {
            version: 5,
            pin: None,
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
