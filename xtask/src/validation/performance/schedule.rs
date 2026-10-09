// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The run plan: build groups, qualification shards and suite shards from
//! reviewed configuration, and the reference chosen from history (080v B7, B10).
//! Every stage of one run reads the same plan.

use super::*;
use std::process::Command;

pub(super) const PLAN: &str = "plan.json";
pub(super) const HOT_PATH: &str = "journal_hot_path";
pub(super) const BOUNDARIES: &str = "validation_boundaries";
pub(super) const READER_CONTROL: &str = "reader_dispatch/full/actual_reader/readers_8";
pub(super) const STUDIO_CONTROL: &str = "studio_validation/project_and_snapshot/inputs_64";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct Target {
    pub(super) name: String,
    #[serde(default, rename = "required-features")]
    pub(super) features: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    version: u32,
    qualification: Vec<QualificationShard>,
    suite: Vec<SuiteShard>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct QualificationShard {
    pub(super) name: String,
    pub(super) cases: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SuiteShard {
    pub(super) name: String,
    pub(super) select: Vec<Selector>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Selector {
    pub(super) target: String,
    /// Anchored Criterion filter over full case IDs; absent selects the target.
    #[serde(default)]
    pub(super) cases: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(super) enum Side {
    Reference,
    Candidate,
}

/// One Cargo feature set compiled once, with the allocation census beside its
/// timing binary when the group holds the hot path.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct BuildGroup {
    pub(super) name: String,
    pub(super) side: Side,
    pub(super) features: Vec<String>,
    pub(super) targets: Vec<String>,
    pub(super) census: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(super) enum Rule {
    /// The measured commit is not on `main`: compare with where it branched.
    MergeBase,
    /// The measured commit is on `main`: compare with the commit before it.
    FirstParent,
    /// A deliberate re-qualification against a fixed commit.
    Pin,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct Reference {
    pub(super) commit: String,
    pub(super) rule: Rule,
    pub(super) main: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct RunPlan {
    pub(super) run_id: String,
    pub(super) created_at_unix_ms: u128,
    pub(super) source: source::SourceIdentity,
    pub(super) reference: Reference,
    pub(super) targets: Vec<Target>,
    pub(super) builds: Vec<BuildGroup>,
    pub(super) qualification: Vec<QualificationShard>,
    pub(super) suite: Vec<SuiteShard>,
}

impl RunPlan {
    pub(super) fn target(&self, name: &str) -> Result<&Target> {
        self.targets
            .iter()
            .find(|target| target.name == name)
            .ok_or_else(|| error(format!("{name}: not a planned benchmark target")))
    }
}

pub(super) fn inventory(root: &Path) -> Result<Vec<Target>> {
    #[derive(Deserialize)]
    struct Manifest {
        bench: Vec<Target>,
    }
    let manifest: Manifest = toml::from_str(&fs::read_to_string(
        root.join("crates/obzenflow_benchmarks/Cargo.toml"),
    )?)?;
    Ok(manifest.bench)
}

pub(super) fn plan(root: &Path, run_id: &str, policy: &ComparisonPolicy) -> Result<RunPlan> {
    let targets = inventory(root)?;
    let config: Config = toml::from_str(&fs::read_to_string(
        root.join(".config/performance-suite.toml"),
    )?)?;
    validate(&config, &targets, policy)?;
    Ok(RunPlan {
        run_id: run_id.into(),
        created_at_unix_ms: now_unix_ms()?,
        source: source::identity(root)?,
        reference: reference(root, policy)?,
        builds: build_groups(&targets),
        targets,
        qualification: config.qualification,
        suite: config.suite,
    })
}

pub(super) fn read(directory: &Path) -> Result<RunPlan> {
    let bytes = fs::read(directory.join(PLAN))
        .map_err(|failure| error(format!("run plan unavailable: {failure}")))?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn named(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

/// Configuration defects fail the plan before anything is built or measured.
fn validate(config: &Config, targets: &[Target], policy: &ComparisonPolicy) -> Result<()> {
    let mut problems = Vec::new();
    if config.version != 2 {
        problems.push(format!(
            "unsupported suite configuration version {}",
            config.version
        ));
    }
    let gated: BTreeSet<_> = policy.cases.iter().chain(&policy.boundary_cases).collect();
    for control in [READER_CONTROL, STUDIO_CONTROL] {
        if !gated.contains(&control.to_owned()) {
            problems.push(format!("gated cases omit rejection control case {control}"));
        }
    }
    let mut placed: BTreeMap<&String, &str> = BTreeMap::new();
    let mut names = BTreeSet::new();
    for shard in &config.qualification {
        if !named(&shard.name) || !names.insert(&shard.name) {
            problems.push(format!(
                "qualification shard {:?}: invalid or duplicate name",
                shard.name
            ));
        }
        if shard.cases.is_empty() {
            problems.push(format!("qualification shard {}: no cases", shard.name));
        }
        for case in &shard.cases {
            if !gated.contains(case) {
                problems.push(format!(
                    "qualification shard {}: {case} is not a gated case",
                    shard.name
                ));
            } else if let Some(other) = placed.insert(case, &shard.name) {
                problems.push(format!(
                    "{case} is in qualification shards {other} and {}",
                    shard.name
                ));
            }
        }
    }
    for case in gated.iter().filter(|case| !placed.contains_key(*case)) {
        problems.push(format!("{case} is in no qualification shard"));
    }
    let manifest: BTreeSet<_> = targets.iter().map(|target| &target.name).collect();
    let mut whole = BTreeSet::new();
    let mut filtered = BTreeSet::new();
    let mut names = BTreeSet::new();
    for shard in &config.suite {
        if !named(&shard.name) || !names.insert(&shard.name) {
            problems.push(format!(
                "suite shard {:?}: invalid or duplicate name",
                shard.name
            ));
        }
        if shard.select.is_empty() {
            problems.push(format!("suite shard {}: no selections", shard.name));
        }
        let mut seen = BTreeSet::new();
        for selector in &shard.select {
            if !manifest.contains(&selector.target) {
                problems.push(format!(
                    "suite shard {}: unknown target {}",
                    shard.name, selector.target
                ));
            }
            if !seen.insert(&selector.target) {
                problems.push(format!(
                    "suite shard {}: selects {} twice",
                    shard.name, selector.target
                ));
            }
            match &selector.cases {
                None if !whole.insert(&selector.target) || filtered.contains(&selector.target) => {
                    problems.push(format!("{} is selected whole and again", selector.target));
                }
                None => {}
                Some(filter) if !filter.starts_with('^') => {
                    problems.push(format!(
                        "suite shard {}: filter {filter:?} is not anchored",
                        shard.name
                    ));
                }
                Some(_) if whole.contains(&selector.target) => {
                    problems.push(format!("{} is selected whole and again", selector.target));
                }
                Some(_) => {
                    filtered.insert(&selector.target);
                }
            }
        }
    }
    for target in manifest
        .iter()
        .filter(|t| !whole.contains(*t) && !filtered.contains(*t))
    {
        problems.push(format!("benchmark target {target} is in no suite shard"));
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(failed(format!(
            "invalid performance suite configuration: {}",
            problems.join("; ")
        )))
    }
}

/// One group per side and feature set; the reference builds only gated targets.
fn build_groups(targets: &[Target]) -> Vec<BuildGroup> {
    let mut groups = Vec::new();
    for side in [Side::Reference, Side::Candidate] {
        let mut by_features: BTreeMap<Vec<String>, Vec<String>> = BTreeMap::new();
        for target in targets {
            if side == Side::Candidate || [HOT_PATH, BOUNDARIES].contains(&target.name.as_str()) {
                by_features
                    .entry(target.features.clone())
                    .or_default()
                    .push(target.name.clone());
            }
        }
        for (features, targets) in by_features {
            let label = if features.is_empty() {
                "default".to_owned()
            } else {
                features.join("+")
            };
            groups.push(BuildGroup {
                name: format!(
                    "{}-{label}",
                    match side {
                        Side::Reference => "reference",
                        Side::Candidate => "candidate",
                    }
                ),
                side,
                census: targets.iter().any(|target| target == HOT_PATH),
                features,
                targets,
            });
        }
    }
    groups
}

fn git(root: &Path, args: &[&str]) -> Result<std::process::Output> {
    Ok(Command::new("git").current_dir(root).args(args).output()?)
}

fn commit(root: &Path, spec: &str) -> Result<Option<String>> {
    let output = git(
        root,
        &[
            "rev-parse",
            "--verify",
            "--quiet",
            &format!("{spec}^{{commit}}"),
        ],
    )?;
    Ok(output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned()))
}

/// Compare with where the measured change started (080v B10).
pub(super) fn reference(root: &Path, policy: &ComparisonPolicy) -> Result<Reference> {
    if let Some(pin) = &policy.pin {
        let commit = commit(root, pin)?.ok_or_else(|| {
            error(format!(
                "pinned reference {pin} is unavailable locally; its history is required"
            ))
        })?;
        return Ok(Reference {
            commit,
            rule: Rule::Pin,
            main: None,
        });
    }
    let mut main = None;
    for name in ["refs/remotes/origin/main", "refs/heads/main"] {
        if commit(root, name)?.is_some() {
            main = Some(name);
            break;
        }
    }
    let main = main.ok_or_else(|| error("no main branch is available to select the reference"))?;
    let contained = git(root, &["merge-base", "--is-ancestor", "HEAD", main])?;
    let (spec, rule) = match contained.status.code() {
        Some(0) => ("HEAD^1".to_owned(), Rule::FirstParent),
        Some(1) => {
            let base = git(root, &["merge-base", "HEAD", main])?;
            if !base.status.success() {
                return Err(error(format!(
                    "the measured commit shares no history with {main}"
                )));
            }
            (
                String::from_utf8_lossy(&base.stdout).trim().to_owned(),
                Rule::MergeBase,
            )
        }
        _ => {
            return Err(error(format!(
                "could not compare the measured commit with {main}: {}",
                String::from_utf8_lossy(&contained.stderr).trim()
            )))
        }
    };
    let commit = commit(root, &spec)?
        .ok_or_else(|| error("the measured commit has no parent to compare against"))?;
    Ok(Reference {
        commit,
        rule,
        main: Some(main.into()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> ComparisonPolicy {
        ComparisonPolicy::read(Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()).unwrap()
    }

    fn target(name: &str) -> Target {
        Target {
            name: name.into(),
            features: Vec::new(),
        }
    }

    fn config(text: &str) -> Config {
        toml::from_str(text).unwrap()
    }

    #[test]
    fn repository_configuration_covers_every_target_and_gated_case() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let targets = inventory(root).unwrap();
        assert_eq!(targets.len(), 8);
        assert!(targets
            .iter()
            .all(|t| !t.features.iter().any(|f| f == "allocation-census")));
        let config: Config = toml::from_str(
            &fs::read_to_string(root.join(".config/performance-suite.toml")).unwrap(),
        )
        .unwrap();
        validate(&config, &targets, &policy()).unwrap();
        let groups = build_groups(&targets);
        assert_eq!(
            groups
                .iter()
                .map(|group| group.name.as_str())
                .collect::<Vec<_>>(),
            [
                "reference-journal-benchmarks",
                "reference-validation-benchmarks",
                "candidate-default",
                "candidate-components",
                "candidate-journal-benchmarks",
                "candidate-validation-benchmarks",
            ]
        );
        assert!(groups
            .iter()
            .filter(|group| group.census)
            .all(|group| group.targets == [HOT_PATH]));
    }

    #[test]
    fn plan_validation_rejects_gaps_overlaps_and_unknown_names() {
        let policy = policy();
        let gated: Vec<_> = policy.cases.iter().chain(&policy.boundary_cases).collect();
        let all = format!("{gated:?}");
        let rest = format!("{:?}", &gated[1..]);
        let targets = [target("a"), target("b")];
        let suite = "[[suite]]\nname = \"s\"\nselect = [{ target = \"a\" }, { target = \"b\" }]\n";
        let valid = format!("version = 2\n[[qualification]]\nname = \"q\"\ncases = {all}\n{suite}");
        validate(&config(&valid), &targets, &policy).unwrap();
        for (text, expected) in [
            (format!("version = 2\n[[qualification]]\nname = \"q\"\ncases = {rest}\n{suite}"), "is in no qualification shard"),
            (format!("version = 2\n[[qualification]]\nname = \"q\"\ncases = {all}\n[[qualification]]\nname = \"r\"\ncases = [{:?}]\n{suite}", gated[0]), "is in qualification shards q and r"),
            (format!("version = 2\n[[qualification]]\nname = \"q\"\ncases = {all}\n[[suite]]\nname = \"s\"\nselect = [{{ target = \"a\" }}, {{ target = \"c\" }}]\n"), "unknown target c"),
            (format!("version = 2\n[[qualification]]\nname = \"q\"\ncases = {all}\n[[suite]]\nname = \"s\"\nselect = [{{ target = \"a\" }}]\n"), "target b is in no suite shard"),
            (format!("version = 2\n[[qualification]]\nname = \"q\"\ncases = {all}\n{suite}[[suite]]\nname = \"s\"\nselect = [{{ target = \"a\", cases = \"^x/\" }}]\n"), "invalid or duplicate name"),
            (format!("version = 2\n[[qualification]]\nname = \"q\"\ncases = {all}\n{suite}[[suite]]\nname = \"t\"\nselect = [{{ target = \"a\", cases = \"^x/\" }}]\n"), "a is selected whole and again"),
            (format!("version = 2\n[[qualification]]\nname = \"q\"\ncases = {all}\n[[suite]]\nname = \"s\"\nselect = [{{ target = \"a\", cases = \"x/\" }}, {{ target = \"b\" }}]\n"), "is not anchored"),
            (format!("version = 1\n[[qualification]]\nname = \"q\"\ncases = {all}\n{suite}"), "unsupported suite configuration version"),
        ] {
            let failure = validate(&config(&text), &targets, &policy).unwrap_err();
            assert!(failure.is::<crate::validation::CheckFailed>(), "{failure}");
            assert!(failure.to_string().contains(expected), "{expected}: {failure}");
        }
    }

    #[test]
    fn reference_is_where_the_change_started() {
        let repository = tempfile::tempdir().unwrap();
        let root = repository.path();
        let run = |args: &[&str]| {
            let output = Command::new("git")
                .current_dir(root)
                .args([
                    "-c",
                    "user.name=fixture",
                    "-c",
                    "user.email=fixture@invalid",
                ])
                .args(args)
                .output()
                .unwrap();
            assert!(output.status.success(), "{output:?}");
            String::from_utf8(output.stdout).unwrap().trim().to_owned()
        };
        run(&["init", "-q", "-b", "main"]);
        run(&["commit", "-q", "--allow-empty", "-m", "first"]);
        let first = run(&["rev-parse", "HEAD"]);
        run(&["commit", "-q", "--allow-empty", "-m", "second"]);
        let second = run(&["rev-parse", "HEAD"]);
        let mut policy = policy();
        policy.pin = None;
        let on_main = reference(root, &policy).unwrap();
        assert_eq!(
            (on_main.commit.as_str(), on_main.rule),
            (first.as_str(), Rule::FirstParent)
        );
        run(&["checkout", "-q", "-b", "topic"]);
        run(&["commit", "-q", "--allow-empty", "-m", "change"]);
        run(&["commit", "-q", "--allow-empty", "-m", "more"]);
        let branch = reference(root, &policy).unwrap();
        assert_eq!(
            (branch.commit.as_str(), branch.rule),
            (second.as_str(), Rule::MergeBase)
        );
        policy.pin = Some(first.clone());
        let pinned = reference(root, &policy).unwrap();
        assert_eq!(
            (pinned.commit.as_str(), pinned.rule),
            (first.as_str(), Rule::Pin)
        );
        policy.pin = Some("f".repeat(40));
        assert!(reference(root, &policy)
            .unwrap_err()
            .to_string()
            .contains("unavailable locally"));
    }
}
