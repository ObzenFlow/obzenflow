// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::{error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeSet,
    ffi::{OsStr, OsString},
    fs,
    path::{Path, PathBuf},
};

pub(super) const CODEC_TEST: &str =
    "journal::disk::codec::tests::current_schema_fixtures_preserve_bytes_and_logical_records";

pub(super) const LEAK_WAIT: &str = "200ms";

/// B15 has no per-profile or per-case exceptions. Check every explicit setting
/// so inheritance cannot turn a detected leak back into passing coverage.
pub(super) fn validate_leak_policy(config: &toml::Value) -> Result<()> {
    fn approved(value: &toml::Value) -> bool {
        value.as_table().is_some_and(|table| {
            table.len() == 2
                && table.get("period").and_then(toml::Value::as_str) == Some(LEAK_WAIT)
                && table.get("result").and_then(toml::Value::as_str) == Some("fail")
        })
    }
    fn visit(value: &toml::Value, path: &str) -> Result<()> {
        match value {
            toml::Value::Table(table) => {
                for (key, child) in table {
                    let path = format!("{path}.{key}");
                    if key == "leak-timeout" && !approved(child) {
                        return Err(error(format!("{path}: required leak policy is {{ period = \"200ms\", result = \"fail\" }}; found {child}")));
                    }
                    visit(child, &path)?;
                }
            }
            toml::Value::Array(array) => {
                for (index, child) in array.iter().enumerate() {
                    visit(child, &format!("{path}[{index}]"))?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    let profiles = config
        .get("profile")
        .ok_or_else(|| error("missing Nextest profiles"))?;
    if !profiles
        .get("default")
        .and_then(|p| p.get("leak-timeout"))
        .is_some_and(approved)
    {
        return Err(error("profile.default.leak-timeout must explicitly require { period = \"200ms\", result = \"fail\" }"));
    }
    visit(profiles, "profile")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum Lane {
    Default,
    ProductionFeatures,
    TestSupport,
    JournalFixtures,
    Doctest,
    Postgres,
    Benchmarks,
    Performance,
}

impl Lane {
    pub(super) const ALL: [Self; 8] = [
        Self::Default,
        Self::ProductionFeatures,
        Self::TestSupport,
        Self::JournalFixtures,
        Self::Doctest,
        Self::Postgres,
        Self::Benchmarks,
        Self::Performance,
    ];

    pub(super) fn correctness() -> Vec<Self> {
        Self::ALL
            .into_iter()
            .filter(|lane| *lane != Self::Performance)
            .collect()
    }

    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::ProductionFeatures => "production-features",
            Self::TestSupport => "test-support",
            Self::JournalFixtures => "journal-fixtures",
            Self::Doctest => "doctest",
            Self::Postgres => "postgres",
            Self::Benchmarks => "benchmarks",
            Self::Performance => "performance",
        }
    }

    pub(super) fn description(self) -> &'static str {
        match self {
            Self::Default => "whole workspace, no extra features",
            Self::ProductionFeatures => "whole workspace with every production feature",
            Self::TestSupport => "whole workspace with test-support and warp-server",
            Self::JournalFixtures => "journal codec fixtures",
            Self::Doctest => "documentation tests",
            Self::Postgres => "PostgreSQL tests against their own database service",
            Self::Benchmarks => "every benchmark once, untimed",
            Self::Performance => "Criterion comparison, run serially, only when named",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|lane| lane.name() == value)
            .ok_or_else(|| error(format!("unknown validation lane: {value}")))
    }

    pub(super) fn cargo_selection(self, production: &[String]) -> Vec<String> {
        let mut args = self.build_selection(production);
        if self == Self::JournalFixtures {
            args.extend([
                "--run-ignored".into(),
                "only".into(),
                "-E".into(),
                format!("test(={CODEC_TEST})"),
            ]);
        }
        args
    }

    pub(super) fn build_selection(self, production: &[String]) -> Vec<String> {
        let mut args = vec!["--workspace".to_owned()];
        let features = match self {
            Self::ProductionFeatures => Some(production.join(",")),
            Self::TestSupport => Some("test-support,obzenflow_infra/warp-server".to_owned()),
            _ => None,
        };
        if let Some(features) = features {
            args.extend(["--features".into(), features]);
        }
        // Production features include `tokio-console`, whose instrumentation
        // compiles only with `tokio_unstable`; no other lane carries the cfg.
        if self == Self::ProductionFeatures {
            args.extend([
                "--config".into(),
                "build.rustflags=[\"--cfg\", \"tokio_unstable\"]".into(),
            ]);
        }
        args
    }

    pub(super) fn build_directory(self, root: &Path, configured: Option<&Path>) -> BuildDirectory {
        match (configured, self) {
            (Some(directory), _) => BuildDirectory::Configured(root.join(directory)),
            (None, Self::Default | Self::JournalFixtures | Self::Performance) => {
                BuildDirectory::Shared(root.join("target"))
            }
            (None, lane) => BuildDirectory::Lane(root.join("target/lanes").join(lane.name())),
        }
    }

    pub(super) fn incremental(self, configured: Option<&OsStr>) -> Incremental {
        match self {
            Self::Default | Self::JournalFixtures if configured != Some(OsStr::new("0")) => {
                Incremental::Enabled
            }
            _ => Incremental::Disabled,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum BuildDirectory {
    Shared(PathBuf),
    Lane(PathBuf),
    Configured(PathBuf),
}

impl BuildDirectory {
    pub(super) fn path(&self) -> &Path {
        match self {
            Self::Shared(path) | Self::Lane(path) | Self::Configured(path) => path,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Incremental {
    Enabled,
    Disabled,
}

impl Incremental {
    pub(super) fn token(self) -> &'static str {
        match self {
            Self::Enabled => "1",
            Self::Disabled => "0",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Options {
    pub(super) lanes: Vec<Lane>,
}

enum Selection {
    Default,
    Correctness,
    Lanes(BTreeSet<Lane>),
}

impl Options {
    pub(super) fn parse(args: &[String]) -> Result<Self> {
        let mut selection = Selection::Default;
        let mut args = args.iter();
        while let Some(arg) = args.next() {
            selection = match (arg.as_str(), selection) {
                ("--correctness", Selection::Default) => Selection::Correctness,
                ("--correctness", Selection::Correctness) => {
                    return Err(error("duplicate option: --correctness"))
                }
                ("--lane", Selection::Correctness) | ("--correctness", Selection::Lanes(_)) => {
                    return Err(error("--correctness cannot be combined with --lane"))
                }
                ("--lane", selection) => {
                    let name = args
                        .next()
                        .ok_or_else(|| error("--lane requires a lane name"))?;
                    let lane = Lane::parse(name)?;
                    let mut lanes = match selection {
                        Selection::Lanes(lanes) => lanes,
                        Selection::Default | Selection::Correctness => BTreeSet::new(),
                    };
                    if !lanes.insert(lane) {
                        return Err(error(format!("duplicate lane: {name}")));
                    }
                    Selection::Lanes(lanes)
                }
                _ => return Err(error(format!("unsupported validation option: {arg}"))),
            };
        }
        Ok(Self {
            lanes: match selection {
                Selection::Default => vec![Lane::Default],
                Selection::Correctness => Lane::correctness(),
                Selection::Lanes(lanes) => lanes.into_iter().collect(),
            },
        })
    }

    pub(super) fn scope(&self) -> &'static str {
        if self.lanes == Lane::ALL {
            "correctness-and-performance"
        } else if self.lanes == Lane::correctness() {
            "correctness"
        } else if self.lanes == [Lane::Default] {
            "default"
        } else if self.lanes == [Lane::Performance] {
            "performance"
        } else {
            "partial"
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) struct TestId {
    pub(super) binary: String,
    pub(super) test: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Ignored {
    pub(super) binary: String,
    pub(super) test: String,
    pub(super) owner: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Policy {
    pub(super) version: u32,
    pub(super) rust: String,
    pub(super) nextest: String,
    pub(super) profile: String,
    pub(super) test_threads: usize,
    pub(super) tokio_workers: usize,
    pub(super) command_watchdog_seconds: u64,
    pub(super) minimum_free_gigabytes: u64,
    pub(super) build_directory_gigabytes: u64,
    pub(super) test_program_build_jobs: usize,
    pub(super) ignored: Vec<Ignored>,
    #[serde(default)]
    pub(super) prerequisites: Vec<super::prerequisites::Requirement>,
    #[serde(skip)]
    pub(super) environment: Environment,
}

impl Policy {
    pub(super) fn read(root: &Path) -> Result<Self> {
        let mut policy: Self =
            toml::from_str(&fs::read_to_string(root.join(".config/validation.toml"))?)?;
        if policy.version != 3
            || policy.profile != "ci-fast"
            || policy.test_threads == 0
            || policy.tokio_workers == 0
            || policy.command_watchdog_seconds == 0
            || policy.minimum_free_gigabytes == 0
            || policy.build_directory_gigabytes == 0
            || policy.test_program_build_jobs == 0
        {
            return Err(error(
                "invalid validation policy version, profile or resource limit",
            ));
        }
        let unique: BTreeSet<_> = policy
            .ignored
            .iter()
            .map(|item| (&item.binary, &item.test))
            .collect();
        if unique.len() != policy.ignored.len()
            || policy.ignored.iter().any(|item| item.owner.is_empty())
        {
            return Err(error(
                "ignored test policy must have unique identities and explicit owners",
            ));
        }
        super::prerequisites::validate(&policy.prerequisites)?;
        policy.environment = Environment::read()?;
        Ok(policy)
    }

    pub(super) fn test_program_jobs(&self) -> usize {
        self.environment
            .build_jobs
            .min(self.test_program_build_jobs)
    }
}

#[derive(Debug, Default)]
pub(super) struct Environment {
    pub(super) build_jobs: usize,
    pub(super) target_dir: Option<PathBuf>,
    pub(super) incremental: Option<OsString>,
}

impl Environment {
    fn read() -> Result<Self> {
        Ok(Self {
            build_jobs: build_jobs(std::env::var_os("CARGO_BUILD_JOBS").as_deref())?,
            target_dir: std::env::var_os("CARGO_TARGET_DIR")
                .filter(|directory| !directory.is_empty())
                .map(PathBuf::from),
            incremental: std::env::var_os("CARGO_INCREMENTAL"),
        })
    }
}

pub(super) fn build_jobs(configured: Option<&OsStr>) -> Result<usize> {
    match configured {
        None => Ok(std::thread::available_parallelism()?.get()),
        Some(value) => value
            .to_str()
            .and_then(|text| text.parse::<usize>().ok())
            .filter(|jobs| *jobs > 0)
            .ok_or_else(|| error("CARGO_BUILD_JOBS must be a whole number above zero")),
    }
}

pub(super) fn production_features(metadata: &Value, root: &Path) -> Result<Vec<String>> {
    let manifest = root.join("Cargo.toml");
    let packages = metadata["packages"]
        .as_array()
        .ok_or_else(|| error("Cargo metadata has no packages"))?;
    let package = packages
        .iter()
        .find(|p| p["manifest_path"].as_str().map(Path::new) == Some(manifest.as_path()))
        .ok_or_else(|| error("Cargo metadata did not identify the workspace root package"))?;
    let features = package["features"]
        .as_object()
        .ok_or_else(|| error("root package has no feature inventory"))?;
    let result: Vec<_> = features
        .keys()
        .filter(|name| !matches!(name.as_str(), "test-support" | "e2e" | "default"))
        .cloned()
        .collect();
    if result.is_empty() {
        return Err(error("production feature selection is empty"));
    }
    Ok(result)
}

#[derive(Debug, Serialize)]
pub(super) struct Exclusion {
    pub(super) id: TestId,
    pub(super) reason: String,
}

#[derive(Debug, Serialize)]
pub(super) struct Inventory {
    pub(super) selected: BTreeSet<TestId>,
    pub(super) excluded: Vec<Exclusion>,
}

pub(super) fn inventory(
    json: &Value,
    lane: Lane,
    policy: &Policy,
    filtered: bool,
) -> Result<Inventory> {
    let suites = json["rust-suites"]
        .as_object()
        .ok_or_else(|| error("Nextest inventory has no rust-suites"))?;
    let mut result = Inventory {
        selected: BTreeSet::new(),
        excluded: Vec::new(),
    };
    for (binary, suite) in suites {
        // Nextest can skip listing an entire binary when a binary-level filter
        // proves that none of its tests can match. The unfiltered inventory
        // still has to list it and owns the complete coverage check.
        if filtered && suite["status"] == "skipped" {
            continue;
        }
        if suite["status"] != "listed" {
            return Err(error(format!("binary was not listed: {binary}")));
        }
        let cases = suite["testcases"]
            .as_object()
            .ok_or_else(|| error(format!("missing testcases: {binary}")))?;
        for (test, case) in cases {
            let id = TestId {
                binary: binary.clone(),
                test: test.clone(),
            };
            let ignored = case["ignored"]
                .as_bool()
                .ok_or_else(|| error("missing ignored classification"))?;
            let status = case["filter-match"]["status"]
                .as_str()
                .ok_or_else(|| error("missing filter classification"))?;
            if !matches!(status, "matches" | "mismatch") {
                return Err(error("unknown Nextest filter status"));
            }
            if status == "mismatch" && (filtered || lane == Lane::JournalFixtures) {
                continue;
            }
            if ignored && lane != Lane::JournalFixtures {
                let owner = policy
                    .ignored
                    .iter()
                    .find(|item| item.binary == *binary && item.test == *test)
                    .ok_or_else(|| {
                        error(format!(
                            "unexpected ignored test: {binary}::{test}; declare its coverage owner"
                        ))
                    })?;
                result.excluded.push(Exclusion {
                    id,
                    reason: owner.owner.clone(),
                });
            } else if status != "matches" {
                return Err(error(format!(
                    "required test excluded by Nextest filter: {binary}::{test}"
                )));
            } else {
                result.selected.insert(id);
            }
        }
    }
    if result.selected.is_empty() && !filtered {
        return Err(error("required test selection is empty"));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn arguments(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn only_declared_lanes_and_execution_flags_are_accepted() {
        assert_eq!(Options::parse(&[]).unwrap().lanes, [Lane::Default]);
        for args in [
            vec!["--profile", "ci-full"],
            vec!["--lane"],
            vec!["--lane", "typo"],
            vec!["--retries", "3"],
            vec!["--linux"],
        ] {
            assert!(
                Options::parse(&args.into_iter().map(str::to_owned).collect::<Vec<_>>()).is_err()
            );
        }
        assert_eq!(
            Options::parse(&["--lane".into(), "test-support".into()])
                .unwrap()
                .lanes,
            [Lane::TestSupport]
        );
    }
    #[test]
    fn correctness_flag_selects_correctness_lanes() {
        let options = Options::parse(&arguments(&["--correctness"])).unwrap();
        assert_eq!(options.lanes, Lane::correctness());
        assert_eq!(options.scope(), "correctness");
    }

    #[test]
    fn correctness_flag_with_lane_refused() {
        for args in [
            ["--correctness", "--lane", "default"],
            ["--lane", "default", "--correctness"],
        ] {
            assert_eq!(
                Options::parse(&arguments(&args)).unwrap_err().to_string(),
                "--correctness cannot be combined with --lane"
            );
        }
    }

    #[test]
    fn bare_command_reports_default_scope() {
        assert_eq!(Options::parse(&[]).unwrap().scope(), "default");
    }

    #[test]
    fn build_jobs_unset_uses_logical_cpus() {
        assert_eq!(
            build_jobs(None).unwrap(),
            std::thread::available_parallelism().unwrap().get()
        );
    }

    #[test]
    fn build_jobs_set_uses_environment_value() {
        assert_eq!(build_jobs(Some(OsStr::new("6"))).unwrap(), 6);
    }

    #[test]
    fn build_jobs_zero_refused() {
        for value in ["0", "-2", "default", ""] {
            assert_eq!(
                build_jobs(Some(OsStr::new(value))).unwrap_err().to_string(),
                "CARGO_BUILD_JOBS must be a whole number above zero",
                "{value}"
            );
        }
    }

    #[test]
    fn incremental_default_lane_enabled() {
        assert_eq!(Lane::Default.incremental(None), Incremental::Enabled);
    }

    #[test]
    fn incremental_named_lane_disabled() {
        for lane in Lane::ALL
            .into_iter()
            .filter(|lane| !matches!(lane, Lane::Default | Lane::JournalFixtures))
        {
            assert_eq!(
                lane.incremental(None),
                Incremental::Disabled,
                "{}",
                lane.name()
            );
        }
    }

    #[test]
    fn incremental_environment_zero_disabled() {
        for lane in [Lane::Default, Lane::JournalFixtures] {
            assert_eq!(
                lane.incremental(Some(OsStr::new("0"))),
                Incremental::Disabled
            );
        }
    }

    #[test]
    fn incremental_journal_fixtures_lane_enabled() {
        assert_eq!(
            Lane::JournalFixtures.incremental(None),
            Incremental::Enabled
        );
    }

    #[test]
    fn journal_fixtures_selection_matches_default_lane() {
        let default = Lane::Default.cargo_selection(&[]);
        let fixtures = Lane::JournalFixtures.cargo_selection(&[]);
        let filter = format!("test(={CODEC_TEST})");
        assert_eq!(fixtures[..default.len()], default[..]);
        assert_eq!(
            fixtures[default.len()..],
            ["--run-ignored", "only", "-E", filter.as_str()][..]
        );
    }

    #[test]
    fn named_lane_builds_in_own_directory() {
        assert_eq!(
            Lane::TestSupport.build_directory(Path::new("/repo"), None),
            BuildDirectory::Lane(PathBuf::from("/repo/target/lanes/test-support"))
        );
    }

    #[test]
    fn default_lane_builds_in_target() {
        assert_eq!(
            Lane::Default.build_directory(Path::new("/repo"), None),
            BuildDirectory::Shared(PathBuf::from("/repo/target"))
        );
    }

    #[test]
    fn production_feature_discovery_includes_new_features_and_cli() {
        let json = serde_json::json!({"packages":[{"manifest_path":"/repo/Cargo.toml","features":{"cli":[],"new-production-feature":[],"test-support":[],"e2e":[]}}]});
        assert_eq!(
            production_features(&json, Path::new("/repo")).unwrap(),
            ["cli", "new-production-feature"]
        );
    }
}
