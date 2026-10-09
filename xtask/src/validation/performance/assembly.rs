// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Accepts a fanned-out run only when every planned stage reported on the
//! planned source with the published executables, and every gated and listed
//! suite case appears exactly once (080v B7). Partial evidence is kept.

use super::*;
use serde_json::Map;

pub(super) const ASSEMBLY: &str = "assembly.json";

#[derive(Serialize)]
struct StageRow {
    stage: String,
    name: String,
    host: Host,
    started_at_unix_ms: u128,
    elapsed_seconds: f64,
    outcome: Outcome,
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Option<T> {
    serde_json::from_slice(&fs::read(path).ok()?).ok()
}

/// Settles every obligation of the run and writes the merged evidence.
pub(super) fn assemble(directory: &Path) -> Result<Vec<(String, Outcome)>> {
    let plan = schedule::read(directory)?;
    let manifests = manifests(directory)?;
    let published: BTreeMap<&String, &String> = manifests
        .iter()
        .flat_map(|manifest| &manifest.executables)
        .map(|entry| (&entry.file, &entry.sha256))
        .collect();
    let planned = plan
        .builds
        .iter()
        .map(|group| ("build", &group.name))
        .chain(
            plan.qualification
                .iter()
                .map(|shard| ("qualification", &shard.name)),
        )
        .chain(plan.suite.iter().map(|shard| ("suite", &shard.name)));
    let mut obligations = Vec::new();
    let mut stages = Vec::new();
    for (kind, name) in planned {
        let path = directory.join(kind).join(name).join("stage.json");
        let outcome = match read_json::<StageRecord>(&path) {
            None => Outcome::Incomplete("no stage record".into()),
            Some(record) => {
                let outcome = if record.run_id != plan.run_id
                    || record.stage != kind
                    || record.name != *name
                    || !record.source.same_contents_as(&plan.source)
                {
                    Outcome::Failed(format!(
                        "recorded run {} on source {}, not the planned run and source",
                        record.run_id, record.source.content_sha256
                    ))
                } else if let Some((file, _)) = record
                    .executables
                    .iter()
                    .find(|(file, sha256)| published.get(file) != Some(sha256))
                {
                    Outcome::Failed(format!(
                        "{file}: executable differs from its build manifest"
                    ))
                } else {
                    record.outcome.clone()
                };
                stages.push(StageRow {
                    stage: record.stage,
                    name: record.name,
                    host: record.host,
                    started_at_unix_ms: record.started_at_unix_ms,
                    elapsed_seconds: record.elapsed_seconds,
                    outcome: outcome.clone(),
                });
                outcome
            }
        };
        obligations.push((format!("{kind} {name}"), outcome));
    }
    let qualification = merge_qualification(directory, &plan)?;
    obligations.push(("gated coverage".into(), qualification.clone()));
    for (target, outcome) in suite_coverage(directory, &plan) {
        obligations.push((format!("suite coverage {target}"), outcome));
    }
    let qualified: Vec<_> = obligations
        .iter()
        .filter(|(name, _)| name.starts_with("qualification ") || name == "gated coverage")
        .cloned()
        .collect();
    fs::write(
        directory.join("qualification.json"),
        serde_json::to_vec_pretty(&json!({"outcome": Outcome::of(&settle(&qualified))}))?,
    )?;
    let finished = stages
        .iter()
        .map(|row| row.started_at_unix_ms + (row.elapsed_seconds * 1000.0) as u128)
        .max();
    let rust: BTreeSet<_> = manifests.iter().map(|manifest| &manifest.rust).collect();
    let adapters: BTreeMap<_, _> = plan
        .builds
        .iter()
        .filter(|group| group.side == Side::Reference)
        .filter_map(|group| {
            let driver: Value = read_json(
                &directory
                    .join("build")
                    .join(&group.name)
                    .join("measurement-driver.json"),
            )?;
            Some((&group.name, driver["reference_public_api_adapter"].clone()))
        })
        .collect();
    fs::write(
        directory.join(ASSEMBLY),
        serde_json::to_vec_pretty(&json!({
            "run_id": plan.run_id, "source": plan.source, "reference": plan.reference,
            "end_to_end_seconds": finished.map(|end| end.saturating_sub(plan.created_at_unix_ms) as f64 / 1000.0),
            "rust": rust, "reference_public_api_adapters": adapters,
            "stages": stages, "obligations": obligations,
            "outcome": Outcome::of(&settle(&obligations)),
        }))?,
    )?;
    Ok(obligations)
}

/// Merges each shard's comparison and controls; every gated case must have
/// been decided by exactly one shard.
fn merge_qualification(directory: &Path, plan: &RunPlan) -> Result<Outcome> {
    // The policy the plan recorded, not whatever the assembling checkout holds.
    let policy: ComparisonPolicy =
        serde_json::from_slice(&fs::read(directory.join("comparison-policy.json"))?)?;
    let mut merged = Map::new();
    let mut controls = Map::new();
    let mut decided: BTreeMap<String, Vec<&str>> = BTreeMap::new();
    for shard in &plan.qualification {
        let path = directory.join("qualification").join(&shard.name);
        if let Some(Value::Object(comparison)) = read_json(&path.join("comparison.json")) {
            for (key, value) in comparison {
                match (key.as_str(), value) {
                    ("decisions" | "before" | "candidate" | "after", Value::Object(cases)) => {
                        if key == "decisions" {
                            for case in cases.keys() {
                                decided.entry(case.clone()).or_default().push(&shard.name);
                            }
                        }
                        let entry = merged.entry(key).or_insert_with(|| json!({}));
                        if let Value::Object(entry) = entry {
                            entry.extend(cases);
                        }
                    }
                    ("suites", Value::Object(suites)) => {
                        let entry = merged.entry(key).or_insert_with(|| json!({}));
                        for (target, mut suite) in suites {
                            let mut cases = entry[&target]["cases"]
                                .as_array()
                                .cloned()
                                .unwrap_or_default();
                            cases.extend(suite["cases"].as_array().cloned().unwrap_or_default());
                            suite["cases"] = Value::Array(cases);
                            entry[&target] = suite;
                        }
                    }
                    (_, value) => {
                        merged.insert(key, value);
                    }
                }
            }
        }
        if let Some(Value::Object(shard_controls)) = read_json(&path.join("negative-controls.json"))
        {
            controls.extend(shard_controls);
        }
    }
    merged.insert("shards".into(), serde_json::to_value(&decided)?);
    fs::write(
        directory.join("comparison.json"),
        serde_json::to_vec_pretty(&merged)?,
    )?;
    if !controls.is_empty() {
        fs::write(
            directory.join("negative-controls.json"),
            serde_json::to_vec_pretty(&controls)?,
        )?;
    }
    let gated: BTreeSet<_> = policy.cases.iter().chain(&policy.boundary_cases).collect();
    let mut problems: Vec<_> = gated
        .iter()
        .filter(|case| !decided.contains_key(**case))
        .map(|case| format!("{case} has no decision"))
        .collect();
    problems.extend(decided.iter().filter_map(|(case, shards)| {
        if !gated.contains(case) {
            Some(format!("{case} is not a gated case"))
        } else {
            (shards.len() > 1).then(|| format!("{case} decided by {}", shards.join(" and ")))
        }
    }));
    Ok(if problems.is_empty() {
        Outcome::Passed
    } else {
        Outcome::Failed(problems.join("; "))
    })
}

/// Every listed case of every target selected exactly once across shards,
/// from inventories that agree. Selections whose own stage did not pass are
/// already represented by that stage's outcome.
fn suite_coverage(directory: &Path, plan: &RunPlan) -> Vec<(String, Outcome)> {
    let mut results = Vec::new();
    for target in &plan.targets {
        let mut inventories = Vec::new();
        let mut selections = Vec::new();
        let mut missing = Vec::new();
        for shard in &plan.suite {
            if !shard
                .select
                .iter()
                .any(|selector| selector.target == target.name)
            {
                continue;
            }
            let phase = directory.join("suite").join(&shard.name).join(&target.name);
            match (
                read_json::<BTreeSet<String>>(&phase.join("cases.json")),
                read_json::<BTreeSet<String>>(&phase.join("selected.json")),
            ) {
                (Some(inventory), Some(selected)) => {
                    inventories.push(inventory);
                    selections.push((shard.name.as_str(), selected));
                }
                _ => missing.push(shard.name.as_str()),
            }
        }
        let outcome = if !missing.is_empty() {
            Outcome::Failed(format!("no case selection from {}", missing.join(", ")))
        } else if inventories.windows(2).any(|pair| pair[0] != pair[1]) {
            Outcome::Failed("shards listed different case inventories".into())
        } else {
            match inventories.first() {
                None => Outcome::Failed("no shard selects this target".into()),
                Some(inventory) => match suite::exact_cover(inventory, &selections) {
                    Ok(()) => Outcome::Passed,
                    Err(problem) => Outcome::Failed(problem),
                },
            }
        };
        results.push((target.name.clone(), outcome));
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use schedule::{BuildGroup, QualificationShard, Reference, Rule, Selector, SuiteShard, Target};

    struct Fixture {
        directory: tempfile::TempDir,
        plan: RunPlan,
        gated: Vec<String>,
    }

    fn repository() -> &'static Path {
        Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()
    }

    fn write(path: &Path, value: &impl Serialize) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
    }

    fn record(fixture: &Fixture, stage: &str, name: &str, executables: &[(&str, &str)]) {
        let record = StageRecord {
            stage: stage.into(),
            name: name.into(),
            run_id: fixture.plan.run_id.clone(),
            source: source::identity(repository()).unwrap(),
            host: host(),
            started_at_unix_ms: fixture.plan.created_at_unix_ms + 1000,
            elapsed_seconds: 2.0,
            executables: executables
                .iter()
                .map(|(file, sha)| ((*file).into(), (*sha).into()))
                .collect(),
            outcome: Outcome::Passed,
        };
        write(
            &fixture
                .directory
                .path()
                .join(stage)
                .join(name)
                .join("stage.json"),
            &record,
        );
    }

    /// A complete two-shard run over one filtered target.
    fn complete() -> Fixture {
        let directory = tempfile::tempdir().unwrap();
        let policy = ComparisonPolicy::read(repository()).unwrap();
        let gated: Vec<String> = policy
            .cases
            .iter()
            .chain(&policy.boundary_cases)
            .cloned()
            .collect();
        fs::write(
            directory.path().join("comparison-policy.json"),
            serde_json::to_vec(&policy).unwrap(),
        )
        .unwrap();
        let selector = |cases: &str| Selector {
            target: "t".into(),
            cases: Some(cases.into()),
        };
        let plan = RunPlan {
            run_id: "fixture".into(),
            created_at_unix_ms: 1_000_000,
            source: source::identity(repository()).unwrap(),
            reference: Reference {
                commit: "a".repeat(40),
                rule: Rule::MergeBase,
                main: Some("refs/heads/main".into()),
            },
            targets: vec![Target {
                name: "t".into(),
                features: Vec::new(),
            }],
            builds: vec![BuildGroup {
                name: "candidate-default".into(),
                side: Side::Candidate,
                features: Vec::new(),
                targets: vec!["t".into()],
                census: false,
            }],
            qualification: vec![QualificationShard {
                name: "gate".into(),
                cases: gated.clone(),
            }],
            suite: vec![
                SuiteShard {
                    name: "one".into(),
                    select: vec![selector("^a/")],
                },
                SuiteShard {
                    name: "two".into(),
                    select: vec![selector("^b/")],
                },
            ],
        };
        write(&directory.path().join(schedule::PLAN), &plan);
        let fixture = Fixture {
            directory,
            plan,
            gated,
        };
        let root = fixture.directory.path();
        write(
            &root.join("build/candidate-default/executables.json"),
            &Manifest {
                group: "candidate-default".into(),
                side: Side::Candidate,
                rust: "rustc 1.93.0".into(),
                reference: "a".repeat(40),
                executables: vec![ManifestEntry {
                    target: "t".into(),
                    census: false,
                    file: "build/candidate-default/t.executable".into(),
                    sha256: "published".into(),
                    compiled_manifest_dir: PathBuf::from("/candidate"),
                }],
                unavailable: BTreeMap::new(),
                invocations: Vec::new(),
            },
        );
        record(&fixture, "build", "candidate-default", &[]);
        record(&fixture, "qualification", "gate", &[]);
        let decisions: Map<String, Value> = fixture
            .gated
            .iter()
            .map(|case| (case.clone(), json!({"outcome": "passed"})))
            .collect();
        write(
            &root.join("qualification/gate/comparison.json"),
            &json!({"decisions": decisions, "suites": {"journal_hot_path": {"cases": []}}}),
        );
        let inventory = ["a/1", "a/2", "b/1"];
        for (shard, selected) in [("one", vec!["a/1", "a/2"]), ("two", vec!["b/1"])] {
            record(
                &fixture,
                "suite",
                shard,
                &[("build/candidate-default/t.executable", "published")],
            );
            write(
                &root.join("suite").join(shard).join("t/cases.json"),
                &inventory,
            );
            write(
                &root.join("suite").join(shard).join("t/selected.json"),
                &selected,
            );
        }
        fixture
    }

    fn settled(fixture: &Fixture) -> (Result<()>, Value) {
        let obligations = assemble(fixture.directory.path()).unwrap();
        let assembly: Value = read_json(&fixture.directory.path().join(ASSEMBLY)).unwrap();
        (settle(&obligations), assembly)
    }

    #[test]
    fn complete_run_passes_and_records_end_to_end_time() {
        let fixture = complete();
        let (result, assembly) = settled(&fixture);
        result.unwrap();
        assert_eq!(assembly["outcome"]["status"], "passed");
        assert_eq!(assembly["end_to_end_seconds"], 3.0);
        assert_eq!(assembly["stages"].as_array().unwrap().len(), 4);
        let comparison: Value =
            read_json(&fixture.directory.path().join("comparison.json")).unwrap();
        assert_eq!(
            comparison["decisions"].as_object().unwrap().len(),
            fixture.gated.len()
        );
    }

    #[test]
    fn missing_stage_is_incomplete_and_keeps_partial_evidence() {
        let fixture = complete();
        fs::remove_file(fixture.directory.path().join("suite/two/stage.json")).unwrap();
        let (result, assembly) = settled(&fixture);
        let failure = result.unwrap_err();
        assert!(!failure.is::<crate::validation::CheckFailed>(), "{failure}");
        assert!(failure.to_string().contains("suite two: no stage record"));
        assert_eq!(assembly["stages"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn overlapping_or_missing_selections_fail() {
        for (selected, expected) in [
            (vec!["a/2", "b/1"], "a/2 selected by one and two"),
            (vec![], "no shard selected [\"b/1\"]"),
        ] {
            let fixture = complete();
            write(
                &fixture.directory.path().join("suite/two/t/selected.json"),
                &selected,
            );
            let failure = settled(&fixture).0.unwrap_err();
            assert!(failure.is::<crate::validation::CheckFailed>(), "{failure}");
            assert!(failure.to_string().contains(expected), "{failure}");
        }
    }

    #[test]
    fn gated_cases_need_exactly_one_decision() {
        let fixture = complete();
        write(
            &fixture
                .directory
                .path()
                .join("qualification/gate/comparison.json"),
            &json!({"decisions": {}}),
        );
        let failure = settled(&fixture).0.unwrap_err();
        assert!(failure.is::<crate::validation::CheckFailed>(), "{failure}");
        assert!(failure.to_string().contains("has no decision"), "{failure}");
        let qualification: Value =
            read_json(&fixture.directory.path().join("qualification.json")).unwrap();
        assert_eq!(qualification["outcome"]["status"], "failed");
    }

    #[test]
    fn differing_source_or_executable_hash_fails() {
        let fixture = complete();
        let path = fixture.directory.path().join("suite/one/stage.json");
        let mut stage: Value = read_json(&path).unwrap();
        stage["source"]["content_sha256"] = json!("other");
        write(&path, &stage);
        let failure = settled(&fixture).0.unwrap_err();
        assert!(failure.is::<crate::validation::CheckFailed>(), "{failure}");
        assert!(
            failure
                .to_string()
                .contains("not the planned run and source"),
            "{failure}"
        );

        let fixture = complete();
        record(
            &fixture,
            "suite",
            "two",
            &[("build/candidate-default/t.executable", "substituted")],
        );
        let failure = settled(&fixture).0.unwrap_err();
        assert!(failure.is::<crate::validation::CheckFailed>(), "{failure}");
        assert!(
            failure
                .to_string()
                .contains("differs from its build manifest"),
            "{failure}"
        );
    }
}
