// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Full-suite observations, one shard per machine and one measurement at a
//! time (080v B7), and the per-PR benchmark smoke that executes every case
//! once without timing it (080v B9).

use super::*;
use schedule::{Selector, SuiteShard};
use std::process::Command;

/// Benchmarks write case declarations here while listing; see
/// `obzenflow_benchmarks::case`.
pub(super) const DECLARATIONS: &str = "OBZENFLOW_CASE_DECLARATIONS";

fn benchmark_command(root: &Path, tools: &Policy, binary: &Path, home: &Path) -> Command {
    let mut command = process::command(root, tools, binary);
    command
        .env("CRITERION_HOME", home)
        .env_remove("OBZENFLOW_WORK_CENSUS")
        .env_remove("OBZENFLOW_BENCH_CONTROL")
        .env_remove(DECLARATIONS)
        .env("RUST_LOG", "warn");
    command
}

fn listed_cases(output: &str) -> Result<BTreeSet<String>> {
    let cases: Vec<_> = output
        .lines()
        .filter_map(|line| line.strip_suffix(": benchmark"))
        .map(str::to_owned)
        .collect();
    let unique: BTreeSet<_> = cases.iter().cloned().collect();
    if unique.is_empty() || unique.len() != cases.len() {
        return Err(error("Criterion listed no cases or duplicate IDs"));
    }
    Ok(unique)
}

/// Each case declares its category and timed boundary exactly once.
fn declared_cases(text: &str) -> Result<BTreeMap<String, Value>> {
    let mut declared = BTreeMap::new();
    for line in text.lines() {
        let declaration: Value = serde_json::from_str(line)?;
        let case = declaration["case"]
            .as_str()
            .filter(|case| !case.is_empty())
            .ok_or_else(|| failed("case declaration has no case ID"))?
            .to_owned();
        for field in ["category", "timed"] {
            if declaration[field].as_str().is_none_or(str::is_empty) {
                return Err(failed(format!("{case}: declaration has no {field}")));
            }
        }
        if declared.insert(case.clone(), declaration).is_some() {
            return Err(failed(format!("{case}: declared more than once")));
        }
    }
    Ok(declared)
}

/// Lists cases and collects their declarations before any measurement.
fn declarations(
    name: &str,
    listed: &BTreeSet<String>,
    text: &str,
) -> Result<BTreeMap<String, Value>> {
    let declared = declared_cases(text)?;
    let keys: BTreeSet<_> = declared.keys().cloned().collect();
    if &keys != listed {
        let undeclared: Vec<_> = listed.difference(&keys).cloned().collect();
        let unlisted: Vec<_> = keys.difference(listed).cloned().collect();
        return Err(failed(format!(
            "{name}: case declarations differ from listed cases; undeclared {undeclared:?}, unlisted {unlisted:?}"
        )));
    }
    Ok(declared)
}

/// The executable's complete inventory, checked against its declarations.
fn inventory(
    root: &Path,
    tools: &Policy,
    directory: &Path,
    name: &str,
    binary: &Path,
) -> Result<BTreeSet<String>> {
    let declared = directory.join("declarations.jsonl");
    let listed = process::execute(
        benchmark_command(root, tools, binary, &directory.join("criterion"))
            .env(DECLARATIONS, &declared)
            .arg("--list"),
        directory,
        "inventory",
        Duration::from_secs(tools.command_watchdog_seconds),
    )?;
    if !listed.success() {
        return Err(failed(format!("{name}: Criterion inventory failed")));
    }
    let cases = listed_cases(&fs::read_to_string(directory.join("inventory.stdout.log"))?)?;
    fs::write(
        directory.join("cases.json"),
        serde_json::to_vec_pretty(&cases)?,
    )?;
    let text = match fs::read_to_string(&declared) {
        Ok(text) => text,
        Err(failure) if failure.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(failure) => return Err(failure.into()),
    };
    let declared = declarations(name, &cases, &text)?;
    fs::write(
        directory.join("declarations.json"),
        serde_json::to_vec_pretty(&declared.into_values().collect::<Vec<_>>())?,
    )?;
    Ok(cases)
}

/// Lists, then measures, one selection with the target's authored sample sizes
/// and measurement budgets. No shortened CI profile and no retries.
fn measure_selection(
    root: &Path,
    tools: &Policy,
    directory: &Path,
    selector: &Selector,
    binary: &Executable,
) -> Result<()> {
    let name = &selector.target;
    let home = directory.join("criterion");
    let inventory = inventory(root, tools, directory, name, &binary.path)?;
    let selected = match &selector.cases {
        None => inventory,
        Some(filter) => {
            let listed = process::execute(
                benchmark_command(root, tools, &binary.path, &home).args(["--list", filter]),
                directory,
                "selection",
                Duration::from_secs(tools.command_watchdog_seconds),
            )?;
            if !listed.success() {
                return Err(failed(format!("{name}: Criterion selection failed")));
            }
            let selected =
                listed_cases(&fs::read_to_string(directory.join("selection.stdout.log"))?)?;
            if !selected.is_subset(&inventory) {
                return Err(error(format!("{name}: selection lists unknown cases")));
            }
            selected
        }
    };
    fs::write(
        directory.join("selected.json"),
        serde_json::to_vec_pretty(&selected)?,
    )?;
    let mut command = benchmark_command(root, tools, &binary.path, &home);
    command.arg("--bench");
    if let Some(filter) = &selector.cases {
        command.arg(filter);
    }
    let status = process::execute(
        &mut command,
        directory,
        "measurement",
        Duration::from_secs(tools.command_watchdog_seconds),
    )?;
    if !status.success() {
        return Err(failed(format!("{name}: Criterion failed")));
    }
    let mut found = BTreeMap::new();
    collect_samples(&home, "new", &mut found)?;
    if found.keys().cloned().collect::<BTreeSet<_>>() != selected {
        return Err(error(format!(
            "{name}: measurements differ from the selected cases"
        )));
    }
    Ok(())
}

/// Measures every selection in the shard serially; a selection without a
/// verified executable records why and no samples. Missing coverage cannot pass.
pub(super) fn run_shard(
    root: &Path,
    tools: &Policy,
    directory: &Path,
    output: &Path,
    plan: &RunPlan,
    shard: &SuiteShard,
    verified: &mut BTreeMap<String, String>,
) -> Result<()> {
    let mut obligations = Vec::new();
    for selector in &shard.select {
        let phase = output.join(&selector.target);
        let started = Instant::now();
        let mut sha256 = None;
        let result = (|| {
            fs::create_dir_all(&phase)?;
            if process::was_interrupted() {
                return Err(error("interrupted before benchmark execution"));
            }
            plan.target(&selector.target)?;
            let binary = load(directory, Side::Candidate, &selector.target, verified)?;
            sha256 = Some(binary.sha256.clone());
            measure_selection(root, tools, &phase, selector, &binary)
        })();
        let outcome = Outcome::of(&result);
        let record = json!({
            "target": selector.target, "cases": selector.cases,
            "features": plan.target(&selector.target).ok().map(|target| &target.features),
            "outcome": outcome, "elapsed_seconds": started.elapsed().as_secs_f64(),
            "executable_sha256": sha256,
        });
        let written: Result<()> = fs::create_dir_all(&phase)
            .map_err(Into::into)
            .and_then(|()| {
                Ok(fs::write(
                    phase.join("outcome.json"),
                    serde_json::to_vec_pretty(&record)?,
                )?)
            });
        let outcome = match written {
            Ok(()) => outcome,
            Err(failure) => Outcome::Incomplete(format!("could not record outcome: {failure}")),
        };
        obligations.push((selector.target.clone(), outcome));
    }
    settle(&obligations)
}

/// Builds every benchmark target in the test profile, checks its declarations
/// against its listed cases and runs every case once. Nothing is timed.
pub(in crate::validation) fn smoke(root: &Path, tools: &Policy, directory: &Path) -> Result<()> {
    let targets = schedule::inventory(root)?;
    let mut groups: BTreeMap<Vec<String>, Vec<String>> = BTreeMap::new();
    for target in &targets {
        groups
            .entry(target.features.clone())
            .or_default()
            .push(target.name.clone());
    }
    let mut obligations = Vec::new();
    for (index, (features, names)) in groups.into_iter().enumerate() {
        let label = format!("build-{index}");
        let compiled = if process::was_interrupted() {
            Compiled {
                executables: BTreeMap::new(),
                outcome: Outcome::Incomplete("interrupted before compilation".into()),
            }
        } else {
            compile_tests(
                root,
                tools,
                directory,
                &label,
                (&names, &features.join(",")),
            )
        };
        obligations.push((label, compiled.outcome.clone()));
        for name in names {
            let phase = directory.join(&name);
            let result = match compiled.executables.get(&name) {
                Some(binary) => exercise(root, tools, &phase, &name, &binary.path),
                None => Err(error(format!("{name}: no test-profile executable"))),
            };
            if let Err(failure) = &result {
                eprintln!("validation: benchmark smoke {name} did not pass: {failure}");
            }
            obligations.push((name, Outcome::of(&result)));
        }
    }
    fs::write(
        directory.join("smoke.json"),
        serde_json::to_vec_pretty(&json!({"targets": targets, "obligations": obligations}))?,
    )?;
    settle(&obligations)
}

fn compile_tests(
    root: &Path,
    tools: &Policy,
    directory: &Path,
    label: &str,
    (targets, features): (&[String], &str),
) -> Compiled {
    let mut command = process::command(root, tools, "cargo");
    command.args([
        "test",
        "--no-run",
        "--locked",
        "--message-format=json",
        "--package",
        "obzenflow_benchmarks",
        "--features",
        features,
    ]);
    for target in targets {
        command.args(["--bench", target]);
    }
    let outcome = match process::execute(
        &mut command,
        directory,
        label,
        Duration::from_secs(tools.command_watchdog_seconds),
    ) {
        Ok(status) if status.success() => Outcome::Passed,
        Ok(status) => Outcome::Failed(format!("{label}: compilation rejected ({status})")),
        Err(failure) => Outcome::Incomplete(failure.to_string()),
    };
    let found = fs::read_to_string(directory.join(format!("{label}.stdout.log")))
        .map_err(Into::into)
        .and_then(|stdout| identified(&stdout, targets, outcome == Outcome::Passed));
    let executables = match found {
        Ok(found) => found
            .into_iter()
            .map(|(target, path)| {
                let executable = Executable {
                    path,
                    compiled_manifest_dir: root.join("crates/obzenflow_benchmarks"),
                    sha256: String::new(),
                    census: None,
                };
                (target, executable)
            })
            .collect(),
        Err(failure) => {
            return Compiled {
                executables: BTreeMap::new(),
                outcome: Outcome::Incomplete(format!(
                    "{label}: unreadable Cargo output: {failure}"
                )),
            }
        }
    };
    Compiled {
        executables,
        outcome,
    }
}

fn exercise(
    root: &Path,
    tools: &Policy,
    directory: &Path,
    name: &str,
    binary: &Path,
) -> Result<()> {
    fs::create_dir_all(directory)?;
    let cases = inventory(root, tools, directory, name, binary)?;
    let status = process::execute(
        benchmark_command(root, tools, binary, &directory.join("criterion")).arg("--test"),
        directory,
        "test",
        Duration::from_secs(tools.command_watchdog_seconds),
    )?;
    if !status.success() {
        return Err(failed(format!("{name}: a case failed when run once")));
    }
    let passed = tested(&fs::read_to_string(directory.join("test.stdout.log"))?);
    if passed != cases {
        let missing: Vec<_> = cases.difference(&passed).collect();
        return Err(error(format!(
            "{name}: listed cases did not report success: {missing:?}"
        )));
    }
    Ok(())
}

/// Cases Criterion's test mode reported as run to completion.
fn tested(output: &str) -> BTreeSet<String> {
    let mut passed = BTreeSet::new();
    let mut current = None;
    for line in output.lines() {
        if let Some(case) = line.strip_prefix("Testing ") {
            current = Some(case.to_owned());
        } else if line == "Success" {
            passed.extend(current.take());
        }
    }
    passed
}

/// Selections must name each of a target's listed cases exactly once across
/// every shard; a target selected whole is complete by construction.
pub(super) fn exact_cover(
    inventory: &BTreeSet<String>,
    selections: &[(&str, BTreeSet<String>)],
) -> std::result::Result<(), String> {
    let mut seen: BTreeMap<&String, &str> = BTreeMap::new();
    for (shard, cases) in selections {
        for case in cases {
            if !inventory.contains(case) {
                return Err(format!("{shard} selected unknown case {case}"));
            }
            if let Some(other) = seen.insert(case, shard) {
                return Err(format!("{case} selected by {other} and {shard}"));
            }
        }
    }
    let missing: Vec<_> = inventory
        .iter()
        .filter(|case| !seen.contains_key(case))
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!("no shard selected {missing:?}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validation::CheckFailed;
    use schedule::Target;

    #[test]
    fn test_mode_success_requires_each_case_to_finish() {
        let output = "Testing a/1\nSuccess\nTesting a/2\npanicked\nTesting b/1\nnoise\nSuccess\n";
        assert_eq!(tested(output), BTreeSet::from(["a/1".into(), "b/1".into()]));
    }

    #[test]
    fn case_inventory_retains_full_ids_and_rejects_empty_or_duplicate_output() {
        assert_eq!(
            listed_cases("diagnostic\nreader/8: benchmark\nreader/32: benchmark\n").unwrap(),
            BTreeSet::from(["reader/8".into(), "reader/32".into()])
        );
        assert!(listed_cases("no measurements").is_err());
        assert!(listed_cases("reader/8: benchmark\nreader/8: benchmark").is_err());
    }

    #[test]
    fn declarations_must_match_listed_cases_exactly() {
        let line = |case: &str| {
            format!(r#"{{"case":"{case}","category":"read","timed":"Read 64 records"}}"#)
        };
        let listed = BTreeSet::from(["reader/8".to_owned(), "reader/32".to_owned()]);
        let both = format!("{}\n{}\n", line("reader/8"), line("reader/32"));
        assert_eq!(declarations("t", &listed, &both).unwrap().len(), 2);
        for (text, expected) in [
            (line("reader/8"), "undeclared [\"reader/32\"]"),
            (
                format!("{both}{}", line("reader/1")),
                "unlisted [\"reader/1\"]",
            ),
            (
                format!("{both}{}", line("reader/8")),
                "declared more than once",
            ),
            (String::new(), "undeclared"),
            (
                r#"{"case":"reader/8","category":"","timed":"x"}"#.into(),
                "no category",
            ),
        ] {
            let failure = declarations("t", &listed, &text).unwrap_err();
            assert!(failure.is::<CheckFailed>(), "{failure}");
            assert!(failure.to_string().contains(expected), "{failure}");
        }
    }

    #[test]
    fn shard_selections_cover_each_listed_case_exactly_once() {
        let inventory: BTreeSet<String> = ["a/1", "a/2", "b/1"].map(String::from).into();
        let set = |cases: &[&str]| cases.iter().map(|case| (*case).to_owned()).collect();
        assert!(exact_cover(
            &inventory,
            &[("x", set(&["a/1", "b/1"])), ("y", set(&["a/2"]))]
        )
        .is_ok());
        for (selections, expected) in [
            (
                vec![("x", set(&["a/1", "b/1"]))],
                "no shard selected [\"a/2\"]",
            ),
            (
                vec![("x", set(&["a/1", "a/2", "b/1"])), ("y", set(&["a/2"]))],
                "a/2 selected by x and y",
            ),
            (
                vec![("x", set(&["a/1", "a/2", "b/1", "c/1"]))],
                "x selected unknown case c/1",
            ),
        ] {
            assert_eq!(exact_cover(&inventory, &selections).unwrap_err(), expected);
        }
    }

    #[test]
    fn unavailable_targets_record_their_build_outcome_without_samples() {
        let repository = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let tools = Policy::read(repository).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let build = directory.path().join("build/candidate-default");
        fs::create_dir_all(&build).unwrap();
        fs::write(
            build.join("executables.json"),
            serde_json::to_vec(&json!({
                "group": "candidate-default", "side": "candidate", "rust": "rustc",
                "reference": "a".repeat(40), "executables": [],
                "unavailable": {
                    "rejected": {"status": "failed", "detail": "candidate-default-build: compilation rejected"},
                    "interrupted": {"status": "incomplete", "detail": "interrupted before compilation"},
                },
                "invocations": [],
            }))
            .unwrap(),
        )
        .unwrap();
        let target = |name: &str| Target {
            name: name.into(),
            features: Vec::new(),
        };
        let shard = SuiteShard {
            name: "s".into(),
            select: ["rejected", "interrupted"]
                .map(|name| Selector {
                    target: name.into(),
                    cases: None,
                })
                .into(),
        };
        let plan = RunPlan {
            run_id: "fixture".into(),
            created_at_unix_ms: 0,
            source: source::identity(repository).unwrap(),
            reference: schedule::Reference {
                commit: "a".repeat(40),
                rule: schedule::Rule::Pin,
                main: None,
            },
            targets: vec![target("rejected"), target("interrupted")],
            builds: Vec::new(),
            qualification: Vec::new(),
            suite: vec![shard.clone()],
        };
        let output = directory.path().join("suite/s");
        let failure = run_shard(
            repository,
            &tools,
            directory.path(),
            &output,
            &plan,
            &shard,
            &mut BTreeMap::new(),
        )
        .unwrap_err();
        assert!(
            !failure.is::<CheckFailed>(),
            "incomplete dominates: {failure}"
        );
        for (name, status) in [("rejected", "failed"), ("interrupted", "incomplete")] {
            let record: Value =
                serde_json::from_slice(&fs::read(output.join(name).join("outcome.json")).unwrap())
                    .unwrap();
            assert_eq!(record["outcome"]["status"], status, "{record}");
            assert!(record["executable_sha256"].is_null());
            assert!(!output.join(name).join("criterion").exists());
        }
    }
}
