// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{error, workspace_root, Result};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    io::{BufRead, BufReader},
    path::{Component, Path, PathBuf},
    process::{Command, Stdio},
};

const REGISTRY_PATH: &str = ".config/compile-contracts.toml";
const SCRATCH_ROOT: &str = "target/compile-contracts";
const CONST_EVAL_CODE: &str = "E0080";

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registry {
    schema: u32,
    suite: Vec<SuiteConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct SuiteConfig {
    name: String,
    owner_manifest: String,
    default_features: bool,
    features: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct CargoMetadata {
    packages: Vec<MetadataPackage>,
    workspace_members: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct MetadataPackage {
    id: String,
    name: String,
    manifest_path: PathBuf,
    features: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Debug)]
struct ValidatedSuite {
    config: SuiteConfig,
    owner: MetadataPackage,
    owner_manifest: PathBuf,
    owner_root: PathBuf,
    owner_toml: toml::Value,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ContractMode {
    Fail,
    Pass,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Expectation {
    annotation_line: usize,
    target_line: usize,
    code: Option<String>,
    message: Option<String>,
}

#[derive(Clone, Debug)]
struct Fixture {
    path: PathBuf,
    display_path: String,
    mode: ContractMode,
    suite: String,
    expectations: Vec<Expectation>,
}

#[derive(Clone, Debug, Default)]
struct TargetState {
    artifact: bool,
    diagnostics: Vec<Diagnostic>,
}

#[derive(Clone, Debug, Deserialize)]
struct CargoMessage {
    reason: String,
    #[serde(default)]
    target: Option<CargoTarget>,
    #[serde(default)]
    message: Option<Diagnostic>,
}

#[derive(Clone, Debug, Deserialize)]
struct CargoTarget {
    name: String,
}

#[derive(Clone, Debug, Deserialize)]
struct Diagnostic {
    message: String,
    level: String,
    #[serde(default)]
    code: Option<DiagnosticCode>,
    #[serde(default)]
    spans: Vec<DiagnosticSpan>,
}

#[derive(Clone, Debug, Deserialize)]
struct DiagnosticCode {
    code: String,
}

#[derive(Clone, Debug, Deserialize)]
struct DiagnosticSpan {
    file_name: String,
    line_start: usize,
    #[serde(default)]
    is_primary: bool,
    #[serde(default)]
    expansion: Option<Box<DiagnosticExpansion>>,
}

#[derive(Clone, Debug, Deserialize)]
struct DiagnosticExpansion {
    span: DiagnosticSpan,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DiagnosticOrigin {
    FixtureLine(usize),
    OwnedConstEvaluation,
    Invalid(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActualError {
    origin: DiagnosticOrigin,
    code: Option<String>,
    message: String,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct SuiteCounts {
    passed: usize,
    failed_as_expected: usize,
    rejected: usize,
}

pub(super) fn run(args: &[String]) -> Result<()> {
    match args {
        [] => {}
        [arg] if matches!(arg.as_str(), "-h" | "--help" | "help") => {
            println!("usage: cargo xtask compile-contracts");
            return Ok(());
        }
        _ => {
            return Err(error(format!(
                "compile-contracts takes no options: {}",
                args.join(" ")
            )));
        }
    }

    let root = workspace_root()?;
    let root = canonical(&root)?;
    let registry = load_registry(&root)?;
    let metadata = load_metadata(&root)?;
    let suites = validate_registry(&root, registry, &metadata)?;
    let fixtures = discover_fixtures(&root, &metadata, &suites)?;
    let root_toml = read_toml(&root.join("Cargo.toml"))?;

    let mut errors = Vec::new();
    let mut totals = SuiteCounts::default();
    for suite in &suites {
        let suite_fixtures = fixtures
            .iter()
            .filter(|fixture| fixture.suite == suite.config.name)
            .collect::<Vec<_>>();
        let (counts, mut suite_errors) =
            run_suite(&root, &root_toml, suite, &suite_fixtures)?;
        println!("{}", summary_row(&suite.config.name, counts));
        totals.passed += counts.passed;
        totals.failed_as_expected += counts.failed_as_expected;
        totals.rejected += counts.rejected;
        errors.append(&mut suite_errors);
    }

    println!(
        "compile-contracts: {} passed, {} failed as expected, {} rejected",
        totals.passed, totals.failed_as_expected, totals.rejected
    );

    if errors.is_empty() {
        Ok(())
    } else {
        for message in &errors {
            eprintln!("{message}");
        }
        Err(error(format!(
            "{} compile contract(s) rejected",
            totals.rejected
        )))
    }
}

fn load_registry(root: &Path) -> Result<Registry> {
    let path = root.join(REGISTRY_PATH);
    let source = fs::read_to_string(&path)
        .map_err(|cause| error(format!("failed to read {}: {cause}", path.display())))?;
    let registry = toml::from_str::<Registry>(&source)
        .map_err(|cause| error(format!("invalid {}: {cause}", path.display())))?;
    if registry.schema != 1 {
        return Err(error(format!(
            "{}: unsupported schema {}; expected 1",
            path.display(),
            registry.schema
        )));
    }
    Ok(registry)
}

fn load_metadata(root: &Path) -> Result<CargoMetadata> {
    let output = Command::new(cargo_program())
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .current_dir(root)
        .output()
        .map_err(|cause| error(format!("failed to run cargo metadata: {cause}")))?;
    if !output.status.success() {
        return Err(error(format!(
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    serde_json::from_slice(&output.stdout)
        .map_err(|cause| error(format!("failed to decode cargo metadata: {cause}")))
}

fn validate_registry(
    root: &Path,
    registry: Registry,
    metadata: &CargoMetadata,
) -> Result<Vec<ValidatedSuite>> {
    if registry.suite.is_empty() {
        return Err(error(format!("{REGISTRY_PATH}: at least one suite is required")));
    }

    let members = metadata
        .packages
        .iter()
        .filter(|package| metadata.workspace_members.contains(&package.id))
        .map(|package| canonical(&package.manifest_path).map(|path| (path, package.clone())))
        .collect::<Result<BTreeMap<_, _>>>()?;
    let mut names = BTreeSet::new();
    let mut suites = Vec::new();

    for config in registry.suite {
        validate_suite_name(&config.name)?;
        if !names.insert(config.name.clone()) {
            return Err(error(format!(
                "{REGISTRY_PATH}: duplicate suite name {:?}",
                config.name
            )));
        }
        if config.owner_manifest.is_empty() {
            return Err(error(format!(
                "{REGISTRY_PATH}: suite {} has an empty owner-manifest",
                config.name
            )));
        }
        let owner_manifest = canonical(&root.join(&config.owner_manifest)).map_err(|cause| {
            error(format!(
                "{REGISTRY_PATH}: suite {} owner-manifest {} is invalid: {cause}",
                config.name, config.owner_manifest
            ))
        })?;
        if !owner_manifest.starts_with(root) {
            return Err(error(format!(
                "{REGISTRY_PATH}: suite {} owner-manifest escapes the workspace",
                config.name
            )));
        }
        let Some(owner) = members.get(&owner_manifest).cloned() else {
            return Err(error(format!(
                "{REGISTRY_PATH}: suite {} owner-manifest is not a workspace package: {}",
                config.name,
                owner_manifest.display()
            )));
        };
        let mut feature_names = BTreeSet::new();
        for feature in &config.features {
            if !feature_names.insert(feature) {
                return Err(error(format!(
                    "{REGISTRY_PATH}: suite {} repeats feature {feature:?}",
                    config.name
                )));
            }
            if !owner.features.contains_key(feature) {
                return Err(error(format!(
                    "{REGISTRY_PATH}: suite {} names unknown feature {feature:?} on {}",
                    config.name, owner.name
                )));
            }
        }
        let owner_root = owner_manifest
            .parent()
            .ok_or_else(|| {
                error(format!(
                    "owner manifest has no parent: {}",
                    owner_manifest.display()
                ))
            })?
            .to_path_buf();
        let owner_toml = read_toml(&owner_manifest)?;
        suites.push(ValidatedSuite {
            config,
            owner,
            owner_manifest,
            owner_root,
            owner_toml,
        });
    }
    Ok(suites)
}

fn validate_suite_name(name: &str) -> Result<()> {
    let valid = !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && !name.starts_with('-')
        && !name.ends_with('-');
    if valid {
        Ok(())
    } else {
        Err(error(format!(
            "{REGISTRY_PATH}: invalid suite name {name:?}; use lowercase letters, digits, and interior hyphens"
        )))
    }
}

fn discover_fixtures(
    root: &Path,
    metadata: &CargoMetadata,
    suites: &[ValidatedSuite],
) -> Result<Vec<Fixture>> {
    let suite_by_name = suites
        .iter()
        .map(|suite| (suite.config.name.as_str(), suite))
        .collect::<BTreeMap<_, _>>();
    let members = metadata.workspace_members.iter().collect::<BTreeSet<_>>();
    let mut fixture_paths = BTreeMap::<PathBuf, PathBuf>::new();

    for package in metadata
        .packages
        .iter()
        .filter(|package| members.contains(&package.id))
    {
        let manifest = canonical(&package.manifest_path)?;
        let package_root = manifest
            .parent()
            .ok_or_else(|| error(format!("manifest has no parent: {}", manifest.display())))?;
        for relative in ["tests/compile_fail", "tests/compile_pass", "tests/ui"] {
            let directory = package_root.join(relative);
            if directory.is_dir() {
                discover_rust_files(&directory, &mut |path| {
                    fixture_paths.insert(path, manifest.clone());
                    Ok(())
                })?;
            }
        }
    }

    let mut fixtures = Vec::new();
    for (path, package_manifest) in fixture_paths {
        let source = fs::read_to_string(&path)
            .map_err(|cause| error(format!("failed to read {}: {cause}", path.display())))?;
        let display_path = display_path(root, &path);
        let fixture = parse_fixture(&path, display_path, &source)?;
        let Some(suite) = suite_by_name.get(fixture.suite.as_str()) else {
            return Err(error(format!(
                "{}: suite {:?} is not present in {REGISTRY_PATH}",
                fixture.display_path, fixture.suite
            )));
        };
        if package_manifest != suite.owner_manifest {
            return Err(error(format!(
                "{}: suite {} belongs to {}, not owning manifest {}",
                fixture.display_path,
                fixture.suite,
                suite.owner_manifest.display(),
                package_manifest.display()
            )));
        }
        fixtures.push(fixture);
    }
    fixtures.sort_by(|left, right| left.display_path.cmp(&right.display_path));

    for suite in suites {
        if !fixtures
            .iter()
            .any(|fixture| fixture.suite == suite.config.name)
        {
            return Err(error(format!(
                "{REGISTRY_PATH}: suite {} owns no fixtures",
                suite.config.name
            )));
        }
    }
    Ok(fixtures)
}

fn discover_rust_files(
    directory: &Path,
    visit: &mut impl FnMut(PathBuf) -> Result<()>,
) -> Result<()> {
    let mut entries = fs::read_dir(directory)
        .map_err(|cause| error(format!("failed to read {}: {cause}", directory.display())))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let file_type = entry.file_type()?;
        let path = entry.path();
        if file_type.is_dir() {
            if entry.file_name() != "support" {
                discover_rust_files(&path, visit)?;
            }
        } else if file_type.is_file()
            && path.extension().is_some_and(|extension| extension == "rs")
        {
            visit(canonical(&path)?)?;
        }
    }
    Ok(())
}

fn parse_fixture(path: &Path, display_path: String, source: &str) -> Result<Fixture> {
    let mut contract = None;
    let mut suite = None;
    let mut expectations = Vec::new();

    for (index, line) in source.lines().enumerate() {
        let line_number = index + 1;
        let trimmed = line.trim_start();
        if let Some(directive) = trimmed.strip_prefix("//@") {
            let directive = directive.trim();
            if let Some(value) = directive.strip_prefix("contract:") {
                if contract.is_some() {
                    return Err(fixture_error(
                        &display_path,
                        line_number,
                        "duplicate contract directive",
                    ));
                }
                contract = Some(match value.trim() {
                    "fail" => ContractMode::Fail,
                    "pass" => ContractMode::Pass,
                    other => {
                        return Err(fixture_error(
                            &display_path,
                            line_number,
                            format!("unknown contract value {other:?}"),
                        ));
                    }
                });
            } else if let Some(value) = directive.strip_prefix("suite:") {
                if suite.is_some() {
                    return Err(fixture_error(
                        &display_path,
                        line_number,
                        "duplicate suite directive",
                    ));
                }
                let value = value.trim();
                if value.is_empty() {
                    return Err(fixture_error(
                        &display_path,
                        line_number,
                        "empty suite directive",
                    ));
                }
                suite = Some(value.to_owned());
            } else {
                return Err(fixture_error(
                    &display_path,
                    line_number,
                    format!("unknown directive {directive:?}"),
                ));
            }
        }

        if let Some(marker) = line.find("//~") {
            expectations.push(parse_expectation(
                &display_path,
                line_number,
                &line[marker + 3..],
            )?);
        }
    }

    let mode = contract.ok_or_else(|| {
        fixture_error(
            &display_path,
            1,
            "missing //@ contract: fail or //@ contract: pass directive",
        )
    })?;
    let suite = suite.ok_or_else(|| {
        fixture_error(
            &display_path,
            1,
            "missing //@ suite: <registry-name> directive",
        )
    })?;
    match mode {
        ContractMode::Fail if expectations.is_empty() => {
            return Err(fixture_error(
                &display_path,
                1,
                "failing fixture has no ERROR expectation",
            ));
        }
        ContractMode::Pass if !expectations.is_empty() => {
            return Err(fixture_error(
                &display_path,
                expectations[0].annotation_line,
                "passing fixture cannot contain an ERROR expectation",
            ));
        }
        _ => {}
    }

    Ok(Fixture {
        path: path.to_path_buf(),
        display_path,
        mode,
        suite,
        expectations,
    })
}

fn parse_expectation(display_path: &str, line: usize, tail: &str) -> Result<Expectation> {
    let caret_count = tail.bytes().take_while(|byte| *byte == b'^').count();
    let mut remainder = tail[caret_count..].trim_start();
    let Some(after_level) = remainder.strip_prefix("ERROR") else {
        return Err(fixture_error(
            display_path,
            line,
            "only ERROR expectations are supported",
        ));
    };
    remainder = after_level.trim_start();
    let mut code = None;
    let mut message = None;
    if remainder.starts_with('[') {
        let Some(close) = remainder.find(']') else {
            return Err(fixture_error(
                display_path,
                line,
                "unterminated error code",
            ));
        };
        let value = &remainder[1..close];
        if value.len() != 5
            || !value.starts_with('E')
            || !value[1..].bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(fixture_error(
                display_path,
                line,
                format!("invalid Rust error code {value:?}"),
            ));
        }
        code = Some(value.to_owned());
        remainder = remainder[close + 1..].trim_start();
    }
    if let Some(fragment) = remainder.strip_prefix(':') {
        let fragment = fragment.trim();
        if fragment.is_empty() {
            return Err(fixture_error(
                display_path,
                line,
                "empty message fragment",
            ));
        }
        message = Some(fragment.to_owned());
        remainder = "";
    }
    if !remainder.is_empty() {
        return Err(fixture_error(
            display_path,
            line,
            format!("malformed expectation tail {remainder:?}"),
        ));
    }
    if code.is_none() && message.is_none() {
        return Err(fixture_error(
            display_path,
            line,
            "ERROR expectation requires a code, message fragment, or both",
        ));
    }
    let target_line = line
        .checked_sub(caret_count)
        .filter(|line| *line > 0)
        .ok_or_else(|| {
            fixture_error(
                display_path,
                line,
                "expectation carets address a line before the start of the file",
            )
        })?;
    Ok(Expectation {
        annotation_line: line,
        target_line,
        code,
        message,
    })
}

fn run_suite(
    root: &Path,
    root_toml: &toml::Value,
    suite: &ValidatedSuite,
    fixtures: &[&Fixture],
) -> Result<(SuiteCounts, Vec<String>)> {
    let package = prepare_package(root, root_toml, suite, fixtures)?;
    let target_directory = root.join(SCRATCH_ROOT).join("build");
    fs::create_dir_all(&target_directory)?;
    let mut command = Command::new(cargo_program());
    command
        .args([
            "check",
            "--keep-going",
            "--message-format=json",
            "--bins",
            "--locked",
        ])
        .arg("--manifest-path")
        .arg(package.join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", target_directory)
        .current_dir(&package)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    if !suite.config.default_features {
        command.arg("--no-default-features");
    }
    if !suite.config.features.is_empty() {
        command
            .arg("--features")
            .arg(suite.config.features.join(","));
    }

    let mut child = command.spawn().map_err(|cause| {
        error(format!(
            "failed to launch compile-contract suite {}: {cause}",
            suite.config.name
        ))
    })?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| error("cargo check stdout was not captured"))?;
    let target_map = fixtures
        .iter()
        .enumerate()
        .map(|(index, fixture)| (target_name(index), *fixture))
        .collect::<BTreeMap<_, _>>();
    let mut states = target_map
        .keys()
        .map(|target| (target.clone(), TargetState::default()))
        .collect::<BTreeMap<_, _>>();
    let mut harness_errors = Vec::new();

    for line in BufReader::new(stdout).lines() {
        let line = line?;
        let message = serde_json::from_str::<CargoMessage>(&line).map_err(|cause| {
            error(format!(
                "suite {} emitted invalid Cargo JSON: {cause}: {line}",
                suite.config.name
            ))
        })?;
        let cargo_target = message.target.as_ref().map(|target| target.name.as_str());
        match message.reason.as_str() {
            "compiler-artifact" => {
                if let Some(state) = cargo_target.and_then(|target| states.get_mut(target)) {
                    state.artifact = true;
                }
            }
            "compiler-message" => {
                let Some(diagnostic) = message.message else {
                    continue;
                };
                if diagnostic.level != "error" {
                    continue;
                }
                if let Some(state) = cargo_target.and_then(|target| states.get_mut(target)) {
                    state.diagnostics.push(diagnostic);
                } else {
                    harness_errors.push(format!(
                        "compile-contract error: {REGISTRY_PATH}:1: expected suite {} dependencies to compile; observed error in Cargo target {}: {}",
                        suite.config.name,
                        cargo_target.unwrap_or("<unknown>"),
                        diagnostic_summary(&diagnostic)
                    ));
                }
            }
            _ => {}
        }
    }
    let status = child.wait()?;

    let mut counts = SuiteCounts::default();
    let mut errors = harness_errors;
    for (target, fixture) in target_map {
        let state = states.remove(&target).unwrap_or_default();
        let fixture_errors = evaluate_fixture(root, &package, fixture, &state);
        if fixture_errors.is_empty() {
            match fixture.mode {
                ContractMode::Pass => counts.passed += 1,
                ContractMode::Fail => counts.failed_as_expected += 1,
            }
        } else {
            counts.rejected += 1;
            errors.extend(fixture_errors);
        }
    }
    if !errors.is_empty() && counts.rejected == 0 {
        counts.rejected = 1;
    }
    if status.success()
        && fixtures
            .iter()
            .any(|fixture| fixture.mode == ContractMode::Fail)
        && errors.is_empty()
    {
        counts.rejected += 1;
        errors.push(format!(
            "compile-contract error: {REGISTRY_PATH}:1: expected suite {} to contain rejected targets; observed successful cargo check",
            suite.config.name
        ));
    }
    Ok((counts, errors))
}

fn prepare_package(
    root: &Path,
    root_toml: &toml::Value,
    suite: &ValidatedSuite,
    fixtures: &[&Fixture],
) -> Result<PathBuf> {
    let hash = suite_hash(root, suite, fixtures)?;
    let packages = root.join(SCRATCH_ROOT).join("packages");
    fs::create_dir_all(&packages)?;
    let directory_name = format!("{}-{hash:016x}", suite.config.name);
    retain_current_generation(&packages, &suite.config.name, &directory_name)?;
    let package = packages.join(&directory_name);
    if package.is_dir() {
        return Ok(package);
    }

    fs::create_dir_all(&package)?;
    let manifest = generated_manifest(root, root_toml, suite, fixtures)?;
    fs::write(
        package.join("Cargo.toml"),
        toml::to_string_pretty(&manifest)?,
    )?;
    fs::copy(root.join("Cargo.lock"), package.join("Cargo.lock"))?;

    let status = Command::new(cargo_program())
        .args(["generate-lockfile", "--offline", "--manifest-path"])
        .arg(package.join("Cargo.toml"))
        .current_dir(&package)
        .status()
        .map_err(|cause| error(format!("failed to prepare suite lockfile: {cause}")))?;
    if !status.success() {
        return Err(error(format!(
            "failed to prepare lockfile for compile-contract suite {}",
            suite.config.name
        )));
    }
    Ok(package)
}

fn generated_manifest(
    root: &Path,
    root_toml: &toml::Value,
    suite: &ValidatedSuite,
    fixtures: &[&Fixture],
) -> Result<toml::Value> {
    let mut document = toml::map::Map::new();
    let edition = package_string(&suite.owner_toml, root_toml, "edition")?;
    let rust_version = package_string(&suite.owner_toml, root_toml, "rust-version")?;
    let mut package = toml::map::Map::new();
    package.insert(
        "name".into(),
        toml::Value::String(format!("compile-contracts-{}", suite.config.name)),
    );
    package.insert("version".into(), toml::Value::String("0.0.0".into()));
    package.insert("edition".into(), toml::Value::String(edition));
    package.insert("rust-version".into(), toml::Value::String(rust_version));
    package.insert("publish".into(), toml::Value::Boolean(false));
    package.insert("autobins".into(), toml::Value::Boolean(false));
    document.insert("package".into(), toml::Value::Table(package));

    let mut dependencies = dependency_table(&suite.owner_toml, "dependencies");
    dependencies.extend(dependency_table(&suite.owner_toml, "dev-dependencies"));
    dependencies.remove("trybuild");
    absolutize_dependency_paths(&mut dependencies, &suite.owner_root);
    let mut owner_dependency = toml::map::Map::new();
    owner_dependency.insert(
        "path".into(),
        toml::Value::String(suite.owner_root.to_string_lossy().into_owned()),
    );
    owner_dependency.insert("default-features".into(), toml::Value::Boolean(false));
    dependencies.insert(
        suite.owner.name.clone(),
        toml::Value::Table(owner_dependency),
    );
    document.insert(
        "dependencies".into(),
        toml::Value::Table(dependencies),
    );

    let mut features = toml::map::Map::new();
    if let Some(owner_features) = suite
        .owner_toml
        .get("features")
        .and_then(toml::Value::as_table)
    {
        for (feature, enables) in owner_features {
            let mut generated = enables
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(toml::Value::as_str)
                .filter(|enable| enable.starts_with("dep:"))
                .map(|enable| toml::Value::String(enable.to_owned()))
                .collect::<Vec<_>>();
            generated.insert(
                0,
                toml::Value::String(format!("{}/{}", suite.owner.name, feature)),
            );
            features.insert(feature.clone(), toml::Value::Array(generated));
        }
    }
    if !features.is_empty() {
        document.insert("features".into(), toml::Value::Table(features));
    }

    let bins = fixtures
        .iter()
        .enumerate()
        .map(|(index, fixture)| {
            let mut bin = toml::map::Map::new();
            bin.insert("name".into(), toml::Value::String(target_name(index)));
            bin.insert(
                "path".into(),
                toml::Value::String(fixture.path.to_string_lossy().into_owned()),
            );
            toml::Value::Table(bin)
        })
        .collect::<Vec<_>>();
    document.insert("bin".into(), toml::Value::Array(bins));

    let root_workspace = root_toml
        .get("workspace")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| error("workspace Cargo.toml has no [workspace] table"))?;
    let mut workspace = toml::map::Map::new();
    if let Some(resolver) = root_workspace.get("resolver") {
        workspace.insert("resolver".into(), resolver.clone());
    }
    let mut workspace_dependencies = root_workspace
        .get("dependencies")
        .and_then(toml::Value::as_table)
        .cloned()
        .unwrap_or_default();
    workspace_dependencies.remove("trybuild");
    absolutize_dependency_paths(&mut workspace_dependencies, root);
    workspace.insert(
        "dependencies".into(),
        toml::Value::Table(workspace_dependencies),
    );
    document.insert("workspace".into(), toml::Value::Table(workspace));
    Ok(toml::Value::Table(document))
}

fn package_string(owner: &toml::Value, root: &toml::Value, key: &str) -> Result<String> {
    let owner_value = owner.get("package").and_then(|package| package.get(key));
    if let Some(value) = owner_value.and_then(toml::Value::as_str) {
        return Ok(value.to_owned());
    }
    if owner_value
        .and_then(toml::Value::as_table)
        .and_then(|table| table.get("workspace"))
        .and_then(toml::Value::as_bool)
        == Some(true)
    {
        if let Some(value) = root
            .get("workspace")
            .and_then(|workspace| workspace.get("package"))
            .and_then(|package| package.get(key))
            .and_then(toml::Value::as_str)
        {
            return Ok(value.to_owned());
        }
    }
    Err(error(format!(
        "{} does not resolve package.{key}",
        owner
            .get("package")
            .and_then(|package| package.get("name"))
            .and_then(toml::Value::as_str)
            .unwrap_or("owner manifest")
    )))
}

fn dependency_table(
    document: &toml::Value,
    key: &str,
) -> toml::map::Map<String, toml::Value> {
    document
        .get(key)
        .and_then(toml::Value::as_table)
        .cloned()
        .unwrap_or_default()
}

fn absolutize_dependency_paths(
    dependencies: &mut toml::map::Map<String, toml::Value>,
    base: &Path,
) {
    for (_, dependency) in dependencies.iter_mut() {
        let path = dependency
            .as_table_mut()
            .and_then(|table| table.get_mut("path"))
            .and_then(|value| value.as_str())
            .map(ToOwned::to_owned);
        if let Some(path) = path.filter(|path| Path::new(path).is_relative()) {
            if let Some(table) = dependency.as_table_mut() {
                table.insert(
                    "path".into(),
                    toml::Value::String(base.join(path).to_string_lossy().into_owned()),
                );
            }
        }
    }
}

fn suite_hash(root: &Path, suite: &ValidatedSuite, fixtures: &[&Fixture]) -> Result<u64> {
    let mut hash = StableHash::default();
    hash.update(include_bytes!("compile_contracts.rs"));
    hash.update(format!("{:#?}", suite.config).as_bytes());
    for path in [
        root.join("Cargo.toml"),
        root.join("Cargo.lock"),
        suite.owner_manifest.clone(),
    ] {
        hash.update(path.to_string_lossy().as_bytes());
        hash.update(&fs::read(path)?);
    }
    for fixture in fixtures {
        hash.update(fixture.display_path.as_bytes());
        hash.update(&fs::read(&fixture.path)?);
    }
    Ok(hash.finish())
}

struct StableHash(u64);

impl Default for StableHash {
    fn default() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }
}

impl StableHash {
    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    fn finish(self) -> u64 {
        self.0
    }
}

fn retain_current_generation(packages: &Path, suite: &str, current: &str) -> Result<()> {
    let prefix = format!("{suite}-");
    for entry in fs::read_dir(packages)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if entry.file_type()?.is_dir() && name.starts_with(&prefix) && name != current {
            fs::remove_dir_all(entry.path())?;
        }
    }
    Ok(())
}

fn evaluate_fixture(
    root: &Path,
    scratch: &Path,
    fixture: &Fixture,
    state: &TargetState,
) -> Vec<String> {
    let actuals = state
        .diagnostics
        .iter()
        .map(|diagnostic| classify_diagnostic(root, scratch, fixture, diagnostic))
        .collect::<Vec<_>>();
    let mut errors = Vec::new();

    match fixture.mode {
        ContractMode::Pass => {
            if !state.artifact || !actuals.is_empty() {
                errors.push(contract_error(
                    fixture,
                    1,
                    "successful compile-pass target",
                    &observed_summary(state, &actuals),
                ));
            }
        }
        ContractMode::Fail => {
            if state.artifact || actuals.is_empty() {
                errors.push(contract_error(
                    fixture,
                    1,
                    "compile failure with matched semantic diagnostics",
                    &observed_summary(state, &actuals),
                ));
            }
            for actual in &actuals {
                if let DiagnosticOrigin::Invalid(reason) = &actual.origin {
                    errors.push(contract_error(
                        fixture,
                        1,
                        "fixture-primary error or locked owned const-evaluation error",
                        &format!("{} ({reason})", actual_summary(actual)),
                    ));
                }
            }
            if !expectations_match(&fixture.expectations, &actuals) {
                let expected = fixture
                    .expectations
                    .iter()
                    .map(expectation_summary)
                    .collect::<Vec<_>>()
                    .join("; ");
                let observed = if actuals.is_empty() {
                    "no error diagnostics".to_owned()
                } else {
                    actuals
                        .iter()
                        .map(actual_summary)
                        .collect::<Vec<_>>()
                        .join("; ")
                };
                errors.push(contract_error(fixture, 1, &expected, &observed));
            }
        }
    }
    errors
}

fn classify_diagnostic(
    root: &Path,
    scratch: &Path,
    fixture: &Fixture,
    diagnostic: &Diagnostic,
) -> ActualError {
    let mut fixture_lines = BTreeSet::new();
    let mut owned_product_span = false;
    let mut origins = Vec::new();
    for span in diagnostic.spans.iter().filter(|span| span.is_primary) {
        let mut chain = Vec::new();
        span_chain(span, &mut chain);
        for candidate in chain {
            let path = resolve_reported_path(root, scratch, &candidate.file_name);
            origins.push(path.clone());
            if same_path(&path, &fixture.path) {
                fixture_lines.insert(candidate.line_start);
            }
            if is_product_source(root, &path) {
                owned_product_span = true;
            }
        }
    }
    let code = diagnostic.code.as_ref().map(|code| code.code.clone());
    let origin = if fixture_lines.len() == 1 {
        DiagnosticOrigin::FixtureLine(*fixture_lines.first().expect("one fixture line"))
    } else if fixture_lines.len() > 1 {
        DiagnosticOrigin::Invalid(format!(
            "multiple fixture-primary lines: {}",
            fixture_lines
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ))
    } else if owned_product_span && code.as_deref() == Some(CONST_EVAL_CODE) {
        DiagnosticOrigin::OwnedConstEvaluation
    } else {
        let locations = origins
            .iter()
            .map(|path| display_path(root, path))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join(", ");
        DiagnosticOrigin::Invalid(if locations.is_empty() {
            "diagnostic has no primary span".to_owned()
        } else {
            format!("primary/expansion origin is outside the fixture: {locations}")
        })
    };
    ActualError {
        origin,
        code,
        message: diagnostic.message.clone(),
    }
}

fn span_chain<'a>(span: &'a DiagnosticSpan, chain: &mut Vec<&'a DiagnosticSpan>) {
    chain.push(span);
    if let Some(expansion) = &span.expansion {
        span_chain(&expansion.span, chain);
    }
}

fn expectations_match(expectations: &[Expectation], actuals: &[ActualError]) -> bool {
    if expectations.len() != actuals.len()
        || actuals
            .iter()
            .any(|actual| matches!(actual.origin, DiagnosticOrigin::Invalid(_)))
    {
        return false;
    }
    let mut assigned = vec![None; actuals.len()];
    for expectation_index in 0..expectations.len() {
        let mut visited = vec![false; actuals.len()];
        if !assign_expectation(
            expectation_index,
            expectations,
            actuals,
            &mut assigned,
            &mut visited,
        ) {
            return false;
        }
    }
    true
}

fn assign_expectation(
    expectation_index: usize,
    expectations: &[Expectation],
    actuals: &[ActualError],
    assigned: &mut [Option<usize>],
    visited: &mut [bool],
) -> bool {
    for actual_index in 0..actuals.len() {
        if visited[actual_index]
            || !expectation_matches(&expectations[expectation_index], &actuals[actual_index])
        {
            continue;
        }
        visited[actual_index] = true;
        if assigned[actual_index].is_none_or(|previous| {
            assign_expectation(previous, expectations, actuals, assigned, visited)
        }) {
            assigned[actual_index] = Some(expectation_index);
            return true;
        }
    }
    false
}

fn expectation_matches(expectation: &Expectation, actual: &ActualError) -> bool {
    let origin_matches = match actual.origin {
        DiagnosticOrigin::FixtureLine(line) => line == expectation.target_line,
        DiagnosticOrigin::OwnedConstEvaluation => {
            expectation.code.as_deref() == Some(CONST_EVAL_CODE) && expectation.message.is_some()
        }
        DiagnosticOrigin::Invalid(_) => false,
    };
    origin_matches
        && expectation
            .code
            .as_ref()
            .is_none_or(|code| actual.code.as_ref() == Some(code))
        && expectation
            .message
            .as_ref()
            .is_none_or(|fragment| actual.message.contains(fragment))
}

fn resolve_reported_path(root: &Path, scratch: &Path, reported: &str) -> PathBuf {
    let path = Path::new(reported);
    if path.is_absolute() {
        normalise_path(path)
    } else {
        let scratch_path = scratch.join(path);
        if scratch_path.exists() {
            canonical(&scratch_path).unwrap_or_else(|_| normalise_path(&scratch_path))
        } else {
            let workspace_path = root.join(path);
            canonical(&workspace_path).unwrap_or_else(|_| normalise_path(&workspace_path))
        }
    }
}

fn normalise_path(path: &Path) -> PathBuf {
    let mut normalised = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalised.pop();
            }
            other => normalised.push(other.as_os_str()),
        }
    }
    normalised
}

fn same_path(left: &Path, right: &Path) -> bool {
    canonical(left).unwrap_or_else(|_| normalise_path(left))
        == canonical(right).unwrap_or_else(|_| normalise_path(right))
}

fn is_product_source(root: &Path, path: &Path) -> bool {
    let Ok(relative) = path.strip_prefix(root) else {
        return false;
    };
    let components = relative
        .components()
        .filter_map(|component| component.as_os_str().to_str())
        .collect::<Vec<_>>();
    path.extension().is_some_and(|extension| extension == "rs")
        && components.contains(&"src")
        && !components.contains(&"tests")
        && !components.contains(&"target")
}

fn observed_summary(state: &TargetState, actuals: &[ActualError]) -> String {
    if actuals.is_empty() {
        format!(
            "{} artifact and no error diagnostics",
            if state.artifact { "successful" } else { "no" }
        )
    } else {
        actuals
            .iter()
            .map(actual_summary)
            .collect::<Vec<_>>()
            .join("; ")
    }
}

fn expectation_summary(expectation: &Expectation) -> String {
    let mut summary = format!("line {} ERROR", expectation.target_line);
    if let Some(code) = &expectation.code {
        summary.push_str(&format!(" [{code}]"));
    }
    if let Some(message) = &expectation.message {
        summary.push_str(&format!(": {message}"));
    }
    summary
}

fn actual_summary(actual: &ActualError) -> String {
    let origin = match &actual.origin {
        DiagnosticOrigin::FixtureLine(line) => format!("line {line}"),
        DiagnosticOrigin::OwnedConstEvaluation => "owned const-evaluation origin".to_owned(),
        DiagnosticOrigin::Invalid(reason) => format!("invalid origin ({reason})"),
    };
    let code = actual
        .code
        .as_ref()
        .map(|code| format!(" [{code}]"))
        .unwrap_or_default();
    format!("{origin} ERROR{code}: {}", actual.message)
}

fn diagnostic_summary(diagnostic: &Diagnostic) -> String {
    let code = diagnostic
        .code
        .as_ref()
        .map(|code| format!(" [{}]", code.code))
        .unwrap_or_default();
    format!("ERROR{code}: {}", diagnostic.message)
}

fn contract_error(fixture: &Fixture, line: usize, expected: &str, observed: &str) -> String {
    format!(
        "compile-contract error: {}:{line}: expected {expected}; observed {observed}",
        fixture.display_path
    )
}

fn fixture_error(
    display_path: &str,
    line: usize,
    message: impl AsRef<str>,
) -> Box<dyn std::error::Error> {
    error(format!(
        "compile-contract error: {display_path}:{line}: {}",
        message.as_ref()
    ))
}

fn summary_row(suite: &str, counts: SuiteCounts) -> String {
    format!(
        "{suite}: {} passed, {} failed as expected, {} rejected",
        counts.passed, counts.failed_as_expected, counts.rejected
    )
}

fn target_name(index: usize) -> String {
    format!("compile-contract-{index:04}")
}

fn read_toml(path: &Path) -> Result<toml::Value> {
    let source = fs::read_to_string(path)
        .map_err(|cause| error(format!("failed to read {}: {cause}", path.display())))?;
    source
        .parse::<toml::Value>()
        .map_err(|cause| error(format!("invalid {}: {cause}", path.display())))
}

fn canonical(path: &Path) -> Result<PathBuf> {
    path.canonicalize()
        .map_err(|cause| error(format!("failed to resolve {}: {cause}", path.display())))
}

fn display_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn cargo_program() -> PathBuf {
    env::var_os("CARGO")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("cargo"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TEMP_ID: AtomicUsize = AtomicUsize::new(0);

    struct TempDirectory(PathBuf);

    impl TempDirectory {
        fn new() -> Self {
            let id = TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "obzenflow-compile-contract-tests-{}-{id}",
                std::process::id()
            ));
            if path.exists() {
                fs::remove_dir_all(&path).unwrap();
            }
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn expectation(line: usize, code: Option<&str>, message: Option<&str>) -> Expectation {
        Expectation {
            annotation_line: line + 1,
            target_line: line,
            code: code.map(ToOwned::to_owned),
            message: message.map(ToOwned::to_owned),
        }
    }

    fn actual(origin: DiagnosticOrigin, code: Option<&str>, message: &str) -> ActualError {
        ActualError {
            origin,
            code: code.map(ToOwned::to_owned),
            message: message.to_owned(),
        }
    }

    fn fixture(mode: ContractMode, expectations: Vec<Expectation>) -> Fixture {
        Fixture {
            path: PathBuf::from("/workspace/tests/compile_fail/case.rs"),
            display_path: "tests/compile_fail/case.rs".to_owned(),
            mode,
            suite: "root-default".to_owned(),
            expectations,
        }
    }

    fn span(path: &str, line: usize) -> DiagnosticSpan {
        DiagnosticSpan {
            file_name: path.to_owned(),
            line_start: line,
            is_primary: true,
            expansion: None,
        }
    }

    fn diagnostic(code: Option<&str>, message: &str, spans: Vec<DiagnosticSpan>) -> Diagnostic {
        Diagnostic {
            message: message.to_owned(),
            level: "error".to_owned(),
            code: code.map(|code| DiagnosticCode {
                code: code.to_owned(),
            }),
            spans,
        }
    }

    #[test]
    fn parses_closed_directive_and_expectation_grammar() {
        let source = "//@ contract: fail\n//@ suite: root-default\nlet value = 1;\n//~^ ERROR [E0308]: mismatched\n";
        let parsed = parse_fixture(
            Path::new("/workspace/tests/compile_fail/case.rs"),
            "tests/compile_fail/case.rs".to_owned(),
            source,
        )
        .unwrap();
        assert_eq!(parsed.mode, ContractMode::Fail);
        assert_eq!(parsed.suite, "root-default");
        assert_eq!(
            parsed.expectations,
            vec![expectation(3, Some("E0308"), Some("mismatched"))]
        );
    }

    #[test]
    fn parser_rejects_missing_duplicate_and_unknown_assignments() {
        let path = Path::new("/workspace/tests/compile_fail/case.rs");
        let missing =
            parse_fixture(path, "case.rs".into(), "//@ contract: fail\nfn main() {}\n");
        assert!(missing.is_err());

        let duplicate = parse_fixture(
            path,
            "case.rs".into(),
            "//@ contract: fail\n//@ suite: one\n//@ suite: two\n//~ ERROR [E0308]\n",
        );
        assert!(duplicate.is_err());

        let unknown = parse_fixture(
            path,
            "case.rs".into(),
            "//@ contract: fail\n//@ owner: root\n//@ suite: one\n//~ ERROR [E0308]\n",
        );
        assert!(unknown.is_err());
    }

    #[test]
    fn parser_rejects_missing_and_malformed_semantic_reasons() {
        let path = Path::new("/workspace/tests/compile_fail/case.rs");
        let missing = parse_fixture(
            path,
            "case.rs".into(),
            "//@ contract: fail\n//@ suite: one\nfn main() {}\n",
        );
        assert!(missing.is_err());

        for malformed in [
            "//~ ERROR",
            "//~ ERROR [E30]",
            "//~ WARNING [E0308]",
            "//~ ERROR:",
            "//~ ERROR [E0308] trailing",
        ] {
            let source =
                format!("//@ contract: fail\n//@ suite: one\nfn main() {{}}\n{malformed}\n");
            assert!(parse_fixture(path, "case.rs".into(), &source).is_err());
        }
    }

    #[test]
    fn matcher_rejects_wrong_primary_line() {
        let expectations = vec![expectation(10, Some("E0308"), None)];
        let actuals = vec![actual(
            DiagnosticOrigin::FixtureLine(11),
            Some("E0308"),
            "mismatched types",
        )];
        assert!(!expectations_match(&expectations, &actuals));
    }

    #[test]
    fn matcher_rejects_wrong_code_and_wrong_message() {
        let coded = vec![expectation(10, Some("E0308"), None)];
        let messaged = vec![expectation(10, None, Some("owned wording"))];
        let actuals = vec![actual(
            DiagnosticOrigin::FixtureLine(10),
            Some("E0277"),
            "different wording",
        )];
        assert!(!expectations_match(&coded, &actuals));
        assert!(!expectations_match(&messaged, &actuals));
    }

    #[test]
    fn matcher_rejects_duplicate_match_and_extra_primary_error() {
        let one_actual = vec![actual(
            DiagnosticOrigin::FixtureLine(10),
            Some("E0308"),
            "mismatched types",
        )];
        let duplicate = vec![
            expectation(10, Some("E0308"), None),
            expectation(10, Some("E0308"), None),
        ];
        assert!(!expectations_match(&duplicate, &one_actual));

        let extra_actual = vec![
            one_actual[0].clone(),
            actual(
                DiagnosticOrigin::FixtureLine(12),
                Some("E0277"),
                "extra error",
            ),
        ];
        assert!(!expectations_match(
            &[expectation(10, Some("E0308"), None)],
            &extra_actual
        ));
    }

    #[test]
    fn matcher_preserves_repeated_diagnostic_multiplicity() {
        let expectations = vec![
            expectation(10, Some("E0277"), Some("owned message")),
            expectation(10, Some("E0277"), Some("owned message")),
            expectation(10, Some("E0277"), Some("owned message")),
        ];
        let actuals = vec![
            actual(
                DiagnosticOrigin::FixtureLine(10),
                Some("E0277"),
                "owned message",
            ),
            actual(
                DiagnosticOrigin::FixtureLine(10),
                Some("E0277"),
                "owned message",
            ),
            actual(
                DiagnosticOrigin::FixtureLine(10),
                Some("E0277"),
                "owned message",
            ),
        ];
        assert!(expectations_match(&expectations, &actuals));
    }

    #[test]
    fn verdict_rejects_unexpected_success_and_failed_pass() {
        let failing = fixture(
            ContractMode::Fail,
            vec![expectation(10, Some("E0308"), None)],
        );
        let successful_state = TargetState {
            artifact: true,
            diagnostics: Vec::new(),
        };
        assert!(!evaluate_fixture(
            Path::new("/workspace"),
            Path::new("/workspace/target/scratch"),
            &failing,
            &successful_state
        )
        .is_empty());

        let passing = fixture(ContractMode::Pass, Vec::new());
        assert!(!evaluate_fixture(
            Path::new("/workspace"),
            Path::new("/workspace/target/scratch"),
            &passing,
            &TargetState::default()
        )
        .is_empty());
    }

    #[test]
    fn macro_expansion_resolves_to_outer_fixture_callsite() {
        let fixture = fixture(
            ContractMode::Fail,
            vec![expectation(27, Some("E0277"), None)],
        );
        let mut product_span = span("/workspace/crates/core/src/macros.rs", 40);
        product_span.expansion = Some(Box::new(DiagnosticExpansion {
            span: span("/workspace/tests/compile_fail/case.rs", 27),
        }));
        let classified = classify_diagnostic(
            Path::new("/workspace"),
            Path::new("/workspace/target/scratch"),
            &fixture,
            &diagnostic(Some("E0277"), "trait bound", vec![product_span]),
        );
        assert_eq!(classified.origin, DiagnosticOrigin::FixtureLine(27));
    }

    #[test]
    fn support_and_third_party_origins_fail_closed() {
        let fixture = fixture(
            ContractMode::Fail,
            vec![expectation(10, Some("E0308"), None)],
        );
        for origin in [
            "/workspace/tests/compile_fail/support/helper.rs",
            "/cargo/registry/src/dependency/src/lib.rs",
        ] {
            let classified = classify_diagnostic(
                Path::new("/workspace"),
                Path::new("/workspace/target/scratch"),
                &fixture,
                &diagnostic(Some("E0308"), "unrelated failure", vec![span(origin, 10)]),
            );
            assert!(matches!(classified.origin, DiagnosticOrigin::Invalid(_)));
        }
    }

    #[test]
    fn target_owned_exception_is_closed_to_e0080_with_code_and_message() {
        let fixture = fixture(
            ContractMode::Fail,
            vec![expectation(10, Some("E0080"), Some("duplicate member"))],
        );
        let product_span = span("/workspace/crates/core/src/member_set.rs", 40);
        let invalid = classify_diagnostic(
            Path::new("/workspace"),
            Path::new("/workspace/target/scratch"),
            &fixture,
            &diagnostic(Some("E0277"), "duplicate member", vec![product_span.clone()]),
        );
        assert!(matches!(invalid.origin, DiagnosticOrigin::Invalid(_)));

        let valid = classify_diagnostic(
            Path::new("/workspace"),
            Path::new("/workspace/target/scratch"),
            &fixture,
            &diagnostic(Some("E0080"), "duplicate member", vec![product_span]),
        );
        assert_eq!(valid.origin, DiagnosticOrigin::OwnedConstEvaluation);
        assert!(expectations_match(&fixture.expectations, &[valid.clone()]));
        assert!(!expectations_match(
            &[expectation(10, Some("E0080"), None)],
            &[valid]
        ));
    }

    #[test]
    fn owned_exception_ignores_both_rust_src_location_shapes() {
        let fixture = fixture(
            ContractMode::Fail,
            vec![expectation(10, Some("E0080"), Some("duplicate member"))],
        );
        for rust_source in [
            "/rustc/abc123/library/core/src/result.rs",
            "/toolchain/lib/rustlib/src/rust/library/core/src/result.rs",
        ] {
            let classified = classify_diagnostic(
                Path::new("/workspace"),
                Path::new("/workspace/target/scratch"),
                &fixture,
                &diagnostic(
                    Some("E0080"),
                    "duplicate member",
                    vec![
                        span("/workspace/crates/core/src/member_set.rs", 40),
                        span(rust_source, 1900),
                    ],
                ),
            );
            assert_eq!(classified.origin, DiagnosticOrigin::OwnedConstEvaluation);
            assert!(expectations_match(
                &fixture.expectations,
                &[classified]
            ));
        }
    }

    #[test]
    fn cargo_json_decoding_retains_target_code_and_expansion() {
        let source = r#"{
          "reason":"compiler-message",
          "target":{"name":"compile-contract-0001"},
          "message":{
            "message":"mismatched types",
            "level":"error",
            "code":{"code":"E0308","explanation":null},
            "spans":[{
              "file_name":"src/macros.rs",
              "line_start":4,
              "is_primary":true,
              "expansion":{
                "span":{
                  "file_name":"tests/compile_fail/case.rs",
                  "line_start":9,
                  "is_primary":false,
                  "expansion":null
                },
                "macro_decl_name":"example!",
                "def_site_span":null
              }
            }]
          }
        }"#;
        let decoded: CargoMessage = serde_json::from_str(source).unwrap();
        assert_eq!(decoded.target.unwrap().name, "compile-contract-0001");
        let diagnostic = decoded.message.unwrap();
        assert_eq!(diagnostic.code.unwrap().code, "E0308");
        assert!(diagnostic.spans[0].expansion.is_some());
    }

    #[test]
    fn registry_schema_is_closed_and_duplicate_or_unknown_features_fail() {
        let unknown_key = r#"
schema = 1
unexpected = true
suite = []
"#;
        assert!(toml::from_str::<Registry>(unknown_key).is_err());

        let temp = TempDirectory::new();
        let manifest = temp.0.join("Cargo.toml");
        fs::write(
            &manifest,
            "[package]\nname = \"owner\"\nversion = \"0.0.0\"\nedition = \"2021\"\n",
        )
        .unwrap();
        let canonical_manifest = canonical(&manifest).unwrap();
        let metadata = CargoMetadata {
            packages: vec![MetadataPackage {
                id: "owner 0.0.0".into(),
                name: "owner".into(),
                manifest_path: canonical_manifest,
                features: BTreeMap::from([("known".into(), Vec::new())]),
            }],
            workspace_members: vec!["owner 0.0.0".into()],
        };
        let config = |name: &str, feature: &str| SuiteConfig {
            name: name.into(),
            owner_manifest: "Cargo.toml".into(),
            default_features: true,
            features: if feature.is_empty() {
                Vec::new()
            } else {
                vec![feature.into()]
            },
        };
        assert!(validate_registry(
            &temp.0,
            Registry {
                schema: 1,
                suite: vec![config("same", ""), config("same", "")]
            },
            &metadata
        )
        .is_err());
        assert!(validate_registry(
            &temp.0,
            Registry {
                schema: 1,
                suite: vec![config("one", "unknown")]
            },
            &metadata
        )
        .is_err());
    }

    #[test]
    fn path_normalisation_and_product_ownership_are_lexical_and_stable() {
        assert_eq!(
            normalise_path(Path::new("/workspace/tests/../src/lib.rs")),
            PathBuf::from("/workspace/src/lib.rs")
        );
        assert!(is_product_source(
            Path::new("/workspace"),
            Path::new("/workspace/crates/core/src/lib.rs")
        ));
        assert!(!is_product_source(
            Path::new("/workspace"),
            Path::new("/workspace/crates/core/tests/support.rs")
        ));
    }

    #[test]
    fn stale_content_generations_are_bounded_per_suite() {
        let temp = TempDirectory::new();
        for directory in ["root-default-old", "root-default-current", "root-ai-old"] {
            fs::create_dir(temp.0.join(directory)).unwrap();
        }
        retain_current_generation(&temp.0, "root-default", "root-default-current").unwrap();
        assert!(!temp.0.join("root-default-old").exists());
        assert!(temp.0.join("root-default-current").is_dir());
        assert!(temp.0.join("root-ai-old").is_dir());
    }

    #[test]
    fn summary_and_target_names_are_deterministic() {
        assert_eq!(target_name(12), "compile-contract-0012");
        assert_eq!(
            summary_row(
                "root-default",
                SuiteCounts {
                    passed: 2,
                    failed_as_expected: 160,
                    rejected: 0
                }
            ),
            "root-default: 2 passed, 160 failed as expected, 0 rejected"
        );
    }
}
