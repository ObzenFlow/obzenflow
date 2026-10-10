// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use std::process::Command;

#[cfg(unix)]
mod leaks;

#[test]
fn leak_policy_rejects_missing_defaults_and_weakened_inheritance_or_overrides() {
    let config: toml::Value =
        toml::from_str(include_str!("../../../.config/nextest.toml")).unwrap();
    plan::validate_leak_policy(&config).unwrap();
    for profile in ["default", "ci-fast", "ci-full"] {
        for replacement in [
            "leak-timeout = '200ms'",
            "leak-timeout = { period = '200ms' }",
            "leak-timeout = { period = '200ms', result = 'pass' }",
            "leak-timeout = { period = '1s', result = 'fail' }",
        ] {
            let replacement: toml::Value = toml::from_str(replacement).unwrap();
            for in_override in [false, true] {
                let mut changed = config.clone();
                let target = if in_override {
                    &mut changed["profile"][profile]["overrides"][0]
                } else {
                    &mut changed["profile"][profile]
                };
                target
                    .as_table_mut()
                    .unwrap()
                    .insert("leak-timeout".into(), replacement["leak-timeout"].clone());
                let failure = plan::validate_leak_policy(&changed)
                    .unwrap_err()
                    .to_string();
                assert!(failure.contains(&format!("profile.{profile}")), "{failure}");
            }
        }
    }
    let mut missing = config.clone();
    missing["profile"]["default"]
        .as_table_mut()
        .unwrap()
        .remove("leak-timeout");
    assert!(plan::validate_leak_policy(&missing).is_err());
    let mut inherited = config.clone();
    let profile: toml::Value = toml::from_str(
        "inherits = 'ci-fast'\nleak-timeout = { period = '200ms', result = 'pass' }",
    )
    .unwrap();
    inherited["profile"]
        .as_table_mut()
        .unwrap()
        .insert("derived".into(), profile);
    assert!(plan::validate_leak_policy(&inherited)
        .unwrap_err()
        .to_string()
        .contains("profile.derived.leak-timeout"));
    let mut equivalent = config.clone();
    equivalent["profile"]["ci-fast"]
        .as_table_mut()
        .unwrap()
        .insert(
            "leak-timeout".into(),
            config["profile"]["default"]["leak-timeout"].clone(),
        );
    plan::validate_leak_policy(&equivalent).unwrap();
}

#[test]
fn retained_launcher_survives_replacement_of_its_running_image() {
    const CHILD: &str = "OBZENFLOW_VALIDATION_LAUNCHER_CHILD";
    const TEST: &str =
        "validation::tests::retained_launcher_survives_replacement_of_its_running_image";
    if let Some(directory) = std::env::var_os(CHILD) {
        let directory = std::path::PathBuf::from(directory);
        let original = std::env::current_exe().unwrap();
        let retained = launcher::Launcher::retain(&directory).unwrap();
        let replacement = directory.join("replacement");
        fs::copy("/usr/bin/false", &replacement).unwrap();
        fs::rename(replacement, &original).unwrap();
        // This is the old mechanism after the same unlink/replace operation
        // performed by Cargo. Linux returns the deleted link target; macOS
        // resolves the replacement. Neither retains the executing image.
        let rediscovered = std::env::current_exe().unwrap();
        let old = Command::new(&rediscovered).arg("--list").output();
        #[cfg(target_os = "linux")]
        assert_eq!(old.as_ref().unwrap_err().raw_os_error(), Some(libc::ENOENT));
        assert!(old.is_err() || !old.unwrap().status.success());
        let repaired = Command::new(retained.path())
            .arg("--list")
            .output()
            .unwrap();
        assert!(repaired.status.success(), "{repaired:?}");
        assert!(String::from_utf8_lossy(&repaired.stdout).contains(TEST));
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let policy = Policy::read(root).unwrap();
    let image = launcher::Launcher::retain(directory.path()).unwrap();
    let mut command = process::command(root, &policy, image.path());
    command
        .args(["--exact", TEST, "--nocapture"])
        .env(CHILD, directory.path());
    let status = process::execute(
        &mut command,
        directory.path(),
        "replace-running-image",
        Duration::from_secs(30),
    )
    .unwrap();
    assert!(status.success());
}

/// A sibling's inherited write descriptor makes a fresh copy ETXTBSY until it
/// closes; the spawn waits for that instead of failing the owning check.
#[cfg(target_os = "linux")]
#[test]
fn spawning_a_copy_still_held_open_for_writing_waits_for_release() {
    let directory = tempfile::tempdir().unwrap();
    let executable = directory.path().join("true");
    fs::copy("/bin/true", &executable).unwrap();
    let writer = fs::OpenOptions::new()
        .write(true)
        .open(&executable)
        .unwrap();
    let release = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        drop(writer);
    });
    let status = process::execute(
        &mut Command::new(&executable),
        directory.path(),
        "busy-executable",
        Duration::from_secs(5),
    )
    .unwrap();
    release.join().unwrap();
    assert!(status.success());
}

const PASS: &str = r#"<testsuites tests="1" failures="0" errors="0"><testsuite name="fixture" tests="1" failures="0" errors="0"><testcase classname="fixture" name="work_completed"/></testsuite></testsuites>"#;
const EARLY_FAILURE: &str = r#"<testsuites tests="2" failures="1" errors="0"><testsuite name="fixture@stress-0" tests="1" failures="1" errors="0"><testcase classname="fixture" name="work_completed"><failure>lost durable output</failure></testcase></testsuite><testsuite name="fixture@stress-1" tests="1" failures="0" errors="0"><testcase classname="fixture" name="work_completed"/></testsuite></testsuites>"#;

fn git(root: &Path, args: &[&str]) {
    let output = Command::new("git")
        .current_dir(root)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn commit(root: &Path) {
    git(
        root,
        &[
            "-c",
            "user.name=Validation fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--quiet",
            "--allow-empty",
            "-m",
            "fixture",
        ],
    );
}

fn fixture() -> tempfile::TempDir {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    fs::create_dir(root.join(".config")).unwrap();
    fs::write(root.join(".gitignore"), "/target/\n").unwrap();
    fs::write(root.join("source.txt"), "before\n").unwrap();
    fs::write(
        root.join("Cargo.toml"),
        "[package]\nname = 'validation-fixture'\nversion = '0.0.0'\nedition = '2024'\n[lib]\npath = 'lib.rs'\n[features]\ncli = []\n",
    )
    .unwrap();
    fs::write(root.join("lib.rs"), "").unwrap();
    fs::write(
        root.join("Cargo.lock"),
        "version = 4\n[[package]]\nname = 'validation-fixture'\nversion = '0.0.0'\n",
    )
    .unwrap();
    fs::write(
        root.join(".config/validation.toml"),
        include_str!("../../../.config/validation.toml"),
    )
    .unwrap();
    // This disposable repository exercises dirty-checkout identity through the
    // same entry point as the real command, without touching the user's Git data.
    git(root, &["init", "--quiet"]);
    commit(root);
    directory
}

#[test]
fn native_entry_preserves_failures_continues_lanes_and_certifies_only_executed_source_and_scope() {
    // These controls enter the actual native plan owner. Only the lane's tool
    // execution is substituted; selection, process status, report parsing,
    // aggregation, source checks and the persisted final result are production.
    for scenario in [
        "early-failure",
        "independent-failures",
        "missing-report",
        "missing-test",
        "contradictory-summary",
        "edited-source",
        "deleted-source",
        "renamed-source",
        "committed-source",
        "committed-edited-source",
        "committed-deleted-source",
        "committed-renamed-source",
        "partial-pass",
    ] {
        let directory = fixture();
        let root = directory.path().canonicalize().unwrap();
        if matches!(
            scenario,
            "committed-deleted-source" | "committed-renamed-source"
        ) {
            git(&root, &["add", "."]);
            commit(&root);
            if scenario == "committed-deleted-source" {
                fs::remove_file(root.join("source.txt")).unwrap();
            } else {
                fs::rename(root.join("source.txt"), root.join("renamed.txt")).unwrap();
            }
        }
        let policy = Policy::read(&root).unwrap();
        let options = Options::parse(&[
            "--lane".into(),
            "default".into(),
            "--lane".into(),
            "journal-fixtures".into(),
        ])
        .unwrap();
        let mut completed = Vec::new();
        let result = run_native(
            &root,
            options,
            policy,
            None,
            |root, policy, lane, _, output, _| {
                completed.push(lane);
                let mut expected = BTreeSet::from([plan::TestId {
                    binary: "fixture".into(),
                    test: "work_completed".into(),
                }]);
                let first = lane == Lane::Default;
                if scenario != "missing-report" || !first {
                    let xml = if scenario == "independent-failures"
                        || (scenario == "early-failure" && first)
                    {
                        EARLY_FAILURE.to_owned()
                    } else if scenario == "contradictory-summary" && first {
                        PASS.replace(
                            "name=\"fixture\" tests=\"1\" failures=\"0\"",
                            "name=\"fixture\" tests=\"1\" failures=\"1\"",
                        )
                    } else {
                        PASS.to_owned()
                    };
                    fs::write(output.join("junit.xml"), xml)?;
                }
                if scenario == "missing-test" && first {
                    expected.insert(plan::TestId {
                        binary: "fixture".into(),
                        test: "required_but_unexecuted".into(),
                    });
                }
                if scenario.starts_with("committed-") && first {
                    git(root, &["add", "."]);
                    commit(root);
                }
                if matches!(scenario, "edited-source" | "committed-edited-source") && first {
                    fs::write(root.join("source.txt"), "edited during execution\n")?;
                }
                if scenario == "deleted-source" && first {
                    fs::remove_file(root.join("source.txt"))?;
                }
                if scenario == "renamed-source" && first {
                    fs::rename(root.join("source.txt"), root.join("renamed.txt"))?;
                }
                // Reproduce the incident's zero child status independently from its
                // failed report. A later successful lane must not overwrite it.
                let status = process::execute(
                    &mut process::command(root, policy, "true"),
                    output,
                    "zero-exit-child",
                    Duration::from_secs(5),
                )?;
                assert!(status.success());
                evaluate_nextest(output, &expected, status.success())
            },
        );
        assert_eq!(
            completed,
            [Lane::Default, Lane::JournalFixtures],
            "{scenario}"
        );
        assert_eq!(
            result.is_ok(),
            matches!(
                scenario,
                "partial-pass"
                    | "committed-source"
                    | "committed-deleted-source"
                    | "committed-renamed-source"
            ),
            "{scenario}: {result:?}"
        );
        let run = fs::read_dir(root.join("target/test-runs"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.is_dir())
            .unwrap();
        let report: Value =
            serde_json::from_slice(&fs::read(run.join("report.json")).unwrap()).unwrap();
        assert_eq!(report["requested_scope"], "partial");
        let expected_status = match scenario {
            "early-failure" | "independent-failures" => "failed",
            "partial-pass"
            | "committed-source"
            | "committed-deleted-source"
            | "committed-renamed-source" => "passed",
            _ => "incomplete",
        };
        assert_eq!(
            report["outcome"]["status"], expected_status,
            "{scenario}: {report}"
        );
        assert_eq!(
            report["source"]["content_sha256"] == report["final_source"]["content_sha256"],
            !matches!(
                scenario,
                "edited-source" | "deleted-source" | "renamed-source" | "committed-edited-source"
            ),
            "{scenario}: both checkout identities must explain the result"
        );
        if scenario.starts_with("committed-") {
            assert_ne!(report["source"]["commit"], report["final_source"]["commit"]);
        }
        if scenario == "independent-failures" {
            assert!(report["lanes"]
                .as_array()
                .unwrap()
                .iter()
                .all(|lane| lane["outcome"]["status"] == "failed"));
        }
    }
}

#[test]
fn failure_feedback_arrives_before_independent_work_finishes() {
    let directory = tempfile::tempdir().unwrap();
    let release = directory.path().join("release");
    let settled = directory.path().join("settled");
    let mut command = Command::new("sh");
    command.args([
        "-c",
        "printf '%s\\n' 'FAIL fixture::archive phase=decode' >&2; while [ ! -f \"$1\" ]; do sleep 0.02; done; printf done > \"$2\"; exit 1",
        "feedback-control",
    ]).arg(&release).arg(&settled);
    let mut observed = false;
    let status = process::execute_observed(
        &mut command,
        directory.path(),
        "feedback-control",
        Duration::from_secs(5),
        Some(&mut |bytes| {
            if String::from_utf8_lossy(bytes).contains("FAIL fixture::archive phase=decode") {
                assert!(
                    !settled.exists(),
                    "failure must surface before remaining work finishes"
                );
                observed = true;
                fs::write(&release, "continue").unwrap();
            }
        }),
    )
    .unwrap();
    assert!(observed);
    assert!(!status.success());
    assert!(settled.is_file());
    assert!(
        fs::read_to_string(directory.path().join("feedback-control.stderr.log"))
            .unwrap()
            .contains("phase=decode")
    );
}

#[test]
fn watchdog_expiry_is_incomplete_even_when_the_child_handles_termination_with_success() {
    let directory = tempfile::tempdir().unwrap();
    let mut command = Command::new("sh");
    command.args(["-c", "trap 'exit 0' TERM; while :; do sleep 1; done"]);
    let failure = process::execute(
        &mut command,
        directory.path(),
        "watchdog-control",
        Duration::from_millis(100),
    )
    .unwrap_err();
    assert!(failure
        .to_string()
        .contains("infrastructure watchdog expired"));
    let report: Value = serde_json::from_slice(
        &fs::read(directory.path().join("watchdog-control.result.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(report["incomplete"], "infrastructure watchdog expired");
    assert_eq!(
        report["success"], true,
        "the child deliberately returns success; the owner must still reject it"
    );
}

#[test]
fn dependency_preparation_preserves_failures_without_suppressing_independent_execution() {
    for (name, lanes, preparation_required, preparation_succeeds) in [
        (
            "failed-preparation",
            vec![
                Lane::Default,
                Lane::ProductionFeatures,
                Lane::TestSupport,
                Lane::JournalFixtures,
            ],
            true,
            false,
        ),
        (
            "failed-preparation-and-lane",
            vec![Lane::Default, Lane::JournalFixtures],
            true,
            false,
        ),
        (
            "successful-preparation",
            vec![
                Lane::Default,
                Lane::ProductionFeatures,
                Lane::TestSupport,
                Lane::JournalFixtures,
            ],
            true,
            true,
        ),
        ("journal-only", vec![Lane::JournalFixtures], false, false),
        ("doctest-only", vec![Lane::Doctest], false, false),
    ] {
        let known_failure = name == "failed-preparation-and-lane";
        let fixture = fixture();
        let root = fixture.path().canonicalize().unwrap();
        let policy = Policy::read(&root).unwrap();
        let arguments: Vec<String> = lanes
            .iter()
            .flat_map(|lane| ["--lane".into(), lane.name().into()])
            .collect();
        let preparation_calls = std::cell::Cell::new(0);
        let mut executed = Vec::new();
        let summary_path = root.join("target/summary.md");
        let result = run_native_with_preparation(
            &root,
            Options::parse(&arguments).unwrap(),
            policy,
            Some(&summary_path),
            |root, policy, lane, _, output, _| {
                assert_eq!(
                    preparation_calls.get(),
                    usize::from(preparation_required),
                    "{name}: preparation must precede execution and run only once"
                );
                executed.push(lane);
                // Exercise the existing result consumer after a real child
                // completes. Dependency failure cannot suppress this evidence.
                fs::write(
                    output.join("junit.xml"),
                    if known_failure && lane == Lane::Default {
                        EARLY_FAILURE
                    } else {
                        PASS
                    },
                )?;
                let status = process::execute(
                    &mut process::command(root, policy, "true"),
                    output,
                    "completed-independent-work",
                    Duration::from_secs(5),
                )?;
                evaluate_nextest(
                    output,
                    &BTreeSet::from([plan::TestId {
                        binary: "fixture".into(),
                        test: "work_completed".into(),
                    }]),
                    status.success(),
                )
            },
            |_, _, _| {
                assert!(preparation_required, "{name}: preparation is unrequested");
                preparation_calls.set(preparation_calls.get() + 1);
                if preparation_succeeds {
                    Ok(())
                } else {
                    Err(error("locked dependency unavailable during preparation"))
                }
            },
        );
        let preparation_failed = preparation_required && !preparation_succeeds;
        assert_eq!(result.is_err(), preparation_failed, "{name}: {result:?}");
        assert_eq!(executed, lanes, "{name}");
        assert_eq!(
            preparation_calls.get(),
            usize::from(preparation_required),
            "{name}"
        );
        let run = fs::read_dir(root.join("target/test-runs"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.is_dir())
            .unwrap();
        let report: Value =
            serde_json::from_slice(&fs::read(run.join("report.json")).unwrap()).unwrap();
        assert_eq!(report["version"], 4, "{name}");
        assert_eq!(
            report["outcome"]["status"],
            if preparation_failed {
                "incomplete"
            } else {
                "passed"
            },
            "{name}: {report}"
        );
        for lane in report["lanes"].as_array().unwrap() {
            assert_eq!(
                lane["outcome"]["status"],
                if known_failure && lane["lane"] == "default" {
                    "failed"
                } else {
                    "passed"
                },
                "{name}: {report}"
            );
        }
        let summary = fs::read_to_string(summary_path).unwrap();
        if known_failure {
            assert!(report["lanes"][0]["outcome"]["detail"]
                .as_str()
                .unwrap()
                .contains("work_completed"));
            assert!(summary.contains("| default | Failed("));
            assert!(summary.contains("work_completed"));
            assert!(summary.contains("| journal-fixtures | Passed |"));
        }
        if preparation_required {
            assert_eq!(
                report["dependency_preparation"]["status"],
                if preparation_failed {
                    "incomplete"
                } else {
                    "passed"
                },
                "{name}: {report}"
            );
            assert!(summary.contains(if preparation_failed {
                "Dependency preparation: Incomplete"
            } else {
                "Dependency preparation: Passed"
            }));
            if preparation_failed {
                assert!(report["dependency_preparation"]["detail"]
                    .as_str()
                    .unwrap()
                    .contains("locked dependency unavailable"));
                assert!(summary.contains("locked dependency unavailable"));
            }
        } else {
            assert!(
                report["dependency_preparation"].is_null(),
                "{name}: {report}"
            );
        }
    }
}

#[test]
fn default_and_explicit_performance_report_only_their_requested_scope() {
    for (arguments, scope, expected) in [
        (vec![], "correctness", Lane::correctness()),
        (
            vec!["--lane".into(), "performance".into()],
            "performance",
            vec![Lane::Performance],
        ),
        (
            vec![
                "--lane".into(),
                "default".into(),
                "--lane".into(),
                "performance".into(),
            ],
            "partial",
            vec![Lane::Default, Lane::Performance],
        ),
    ] {
        let fixture = fixture();
        let root = fixture.path().canonicalize().unwrap();
        let policy = Policy::read(&root).unwrap();
        let mut executed = Vec::new();
        // A simulated run owns its summary destination, even inside GitHub CI.
        let summary_path = root.join("target/summary.md");
        run_native(
            &root,
            Options::parse(&arguments).unwrap(),
            policy,
            Some(&summary_path),
            |_, _, lane, _, _, _| {
                executed.push(lane);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(executed, expected);
        let run = fs::read_dir(root.join("target/test-runs"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.is_dir())
            .unwrap();
        let report: Value =
            serde_json::from_slice(&fs::read(run.join("report.json")).unwrap()).unwrap();
        assert_eq!(report["version"], 4);
        assert_eq!(report["requested_scope"], scope);
        let unrequested: Vec<Lane> =
            serde_json::from_value(report["not_requested"].clone()).unwrap();
        let summary = fs::read_to_string(summary_path).unwrap();
        assert!(summary.contains(&format!("### Validation: {scope}\n")));
        assert!(summary.contains("Outcome: Passed\n"));
        for lane in Lane::ALL {
            assert_ne!(executed.contains(&lane), unrequested.contains(&lane));
            let outcome = if executed.contains(&lane) {
                "Passed"
            } else {
                "Not requested; no acceptance evidence"
            };
            assert!(summary.contains(&format!("| {} | {outcome} |", lane.name())));
        }
        assert_eq!(report["outcome"]["status"], "passed");
    }
}

#[test]
fn help_advertises_every_lane_for_capability_detection() {
    let text = help();
    let lanes = text
        .lines()
        .find_map(|line| line.strip_prefix("Lanes: "))
        .unwrap();
    let advertised: Vec<_> = lanes.split(", ").collect();
    let declared: Vec<_> = Lane::ALL.iter().map(|lane| lane.name()).collect();
    assert_eq!(advertised, declared);
    assert!(advertised.contains(&"performance"));
}

#[test]
fn incomplete_obligations_dominate_failures_and_keep_their_class() {
    let named = |outcome: Outcome| ("check".to_owned(), outcome);
    assert!(settle(&[named(Outcome::Passed)]).is_ok());
    let failure = settle(&[
        named(Outcome::Failed("rejected".into())),
        named(Outcome::Passed),
    ])
    .unwrap_err();
    assert!(failure.is::<CheckFailed>());
    assert_eq!(
        Outcome::of(&Err(failure)),
        Outcome::Failed("check: rejected".into())
    );
    let incomplete = settle(&[
        named(Outcome::Failed("rejected".into())),
        named(Outcome::Incomplete("interrupted".into())),
    ])
    .unwrap_err();
    assert!(!incomplete.is::<CheckFailed>());
    assert!(incomplete.to_string().contains("interrupted"));
    assert!(Outcome::Failed("x".into())
        .into_result()
        .unwrap_err()
        .is::<CheckFailed>());
}
