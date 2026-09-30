// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::{error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeSet, fs, path::Path};

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
    Performance,
}

impl Lane {
    pub(super) const ALL: [Self; 7] = [
        Self::Default,
        Self::ProductionFeatures,
        Self::TestSupport,
        Self::JournalFixtures,
        Self::Doctest,
        Self::Postgres,
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
            Self::Performance => "performance",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|lane| lane.name() == value)
            .ok_or_else(|| error(format!("unknown validation lane: {value}")))
    }

    pub(super) fn cargo_selection(self, production: &[String]) -> Vec<String> {
        let args = match self {
            Self::JournalFixtures => vec![
                "--package",
                "obzenflow_infra",
                "--lib",
                "--run-ignored",
                "only",
                "-E",
                CODEC_TEST,
            ],
            _ => vec!["--workspace"],
        };
        let mut args: Vec<_> = args.into_iter().map(str::to_owned).collect();
        if self == Self::JournalFixtures {
            *args.last_mut().unwrap() = format!("test(={CODEC_TEST})");
        }
        let features = match self {
            Self::ProductionFeatures => Some(production.join(",")),
            Self::TestSupport => Some("test-support,obzenflow_infra/warp-server".to_owned()),
            _ => None,
        };
        if let Some(features) = features {
            args.extend(["--features".into(), features]);
        }
        args
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Options {
    pub(super) lanes: Vec<Lane>,
}

impl Options {
    pub(super) fn parse(args: &[String]) -> Result<Self> {
        let mut lanes = BTreeSet::new();
        let mut args = args.iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--lane" => {
                    let name = args
                        .next()
                        .ok_or_else(|| error("--lane requires a lane name"))?;
                    let lane = Lane::parse(name)?;
                    if !lanes.insert(lane) {
                        return Err(error(format!("duplicate lane: {name}")));
                    }
                }
                _ => return Err(error(format!("unsupported validation option: {arg}"))),
            }
        }
        Ok(Self {
            lanes: if lanes.is_empty() {
                Lane::correctness()
            } else {
                lanes.into_iter().collect()
            },
        })
    }

    pub(super) fn scope(&self) -> &'static str {
        if self.lanes == Lane::ALL {
            "correctness-and-performance"
        } else if self.lanes == Lane::correctness() {
            "correctness"
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
    pub(super) build_jobs: usize,
    pub(super) test_threads: usize,
    pub(super) tokio_workers: usize,
    pub(super) command_watchdog_seconds: u64,
    pub(super) ignored: Vec<Ignored>,
    #[serde(default)]
    pub(super) prerequisites: Vec<super::prerequisites::Requirement>,
}

impl Policy {
    pub(super) fn read(root: &Path) -> Result<Self> {
        let policy: Self =
            toml::from_str(&fs::read_to_string(root.join(".config/validation.toml"))?)?;
        if policy.version != 2
            || policy.profile != "ci-fast"
            || policy.build_jobs == 0
            || policy.test_threads == 0
            || policy.tokio_workers == 0
            || policy.command_watchdog_seconds == 0
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
        Ok(policy)
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
    #[test]
    fn only_declared_lanes_and_execution_flags_are_accepted() {
        assert_eq!(Options::parse(&[]).unwrap().lanes, Lane::correctness());
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
    fn production_feature_discovery_includes_new_features_and_cli() {
        let json = serde_json::json!({"packages":[{"manifest_path":"/repo/Cargo.toml","features":{"cli":[],"new-production-feature":[],"test-support":[],"e2e":[]}}]});
        assert_eq!(
            production_features(&json, Path::new("/repo")).unwrap(),
            ["cli", "new-production-feature"]
        );
    }
}
