// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use std::process::Command;

const PASS: &str = r#"<testsuites tests="1" failures="0" errors="0"><testsuite name="fixture" tests="1" failures="0" errors="0"><testcase classname="fixture" name="work_completed"/></testsuite></testsuites>"#;
const EARLY_FAILURE: &str = r#"<testsuites tests="2" failures="1" errors="0"><testsuite name="fixture@stress-0" tests="1" failures="1" errors="0"><testcase classname="fixture" name="work_completed"><failure>lost durable output</failure></testcase></testsuite><testsuite name="fixture@stress-1" tests="1" failures="0" errors="0"><testcase classname="fixture" name="work_completed"/></testsuite></testsuites>"#;

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
    for args in [
        vec!["init", "--quiet"],
        vec![
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
    ] {
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
        "partial-pass",
    ] {
        let directory = fixture();
        let root = directory.path().canonicalize().unwrap();
        let policy = Policy::read(&root).unwrap();
        let options = Options::parse(&[
            "--lane".into(),
            "default".into(),
            "--lane".into(),
            "journal-fixtures".into(),
        ])
        .unwrap();
        let mut completed = Vec::new();
        let result = run_native(&root, options, policy, |root, policy, lane, _, output| {
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
            if scenario == "edited-source" && first {
                fs::write(root.join("source.txt"), "edited during execution\n")?;
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
        });
        assert_eq!(
            completed,
            [Lane::Default, Lane::JournalFixtures],
            "{scenario}"
        );
        assert_eq!(
            result.is_ok(),
            scenario == "partial-pass",
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
            "partial-pass" => "passed",
            _ => "incomplete",
        };
        assert_eq!(
            report["outcome"]["status"], expected_status,
            "{scenario}: {report}"
        );
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
