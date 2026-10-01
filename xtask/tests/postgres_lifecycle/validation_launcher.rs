// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../../src/validation/launcher.rs"]
mod launcher;

use std::{fs, process::Command};

#[test]
fn real_probe_and_postgres_entry_survive_build_output_replacement() {
    let directory = tempfile::tempdir().unwrap();
    let build_output = directory.path().join("xtask");
    // Cargo has built the ordinary executable for this integration target.
    // Exercise both first materialisation and replacement of existing output.
    for existing in [false, true] {
        assert_eq!(build_output.exists(), existing);
        let rebuilt = directory.path().join("rebuilt-xtask");
        fs::copy(env!("CARGO_BIN_EXE_xtask"), &rebuilt).unwrap();
        fs::rename(rebuilt, &build_output).unwrap();
        let retained = launcher::Launcher::copy(&build_output, directory.path()).unwrap();
        let rejected = directory.path().join("different-executable");
        fs::copy("/usr/bin/false", &rejected).unwrap();
        fs::rename(rejected, &build_output).unwrap();
        assert!(!Command::new(&build_output)
            .arg("--help")
            .status()
            .unwrap()
            .success());
        let probe = Command::new(retained.path())
            .args(["__test-prerequisite", "artifact-files"])
            .arg(directory.path())
            .output()
            .unwrap();
        assert!(probe.status.success(), "{probe:?}");
        let result: Option<serde_json::Value> = serde_json::from_slice(&probe.stdout).unwrap();
        assert!(result.is_none(), "real filesystem probe: {result:?}");
        let denied = Command::new(retained.path())
            .args(["__test-prerequisite", "artifact-files"])
            .arg(&build_output)
            .output()
            .unwrap();
        assert!(
            denied.status.success(),
            "a completed probe reports denial as data: {denied:?}"
        );
        let denial: serde_json::Value = serde_json::from_slice(&denied.stdout).unwrap();
        assert_eq!(denial["kind"], "NotADirectory");
        assert!(denial["message"]
            .as_str()
            .unwrap()
            .contains(build_output.to_str().unwrap()));
        // tempfile adds path context but does not expose the wrapped raw errno
        // through Error::source; preserve its kind/message without inventing a code.
        // This is the same retained executable and real dispatch as the service
        // coordinator, without making launcher ownership depend on a database.
        let postgres = Command::new(retained.path())
            .args(["postgres", "--help"])
            .output()
            .unwrap();
        assert!(postgres.status.success(), "{postgres:?}");
        assert!(String::from_utf8_lossy(&postgres.stdout).contains("postgres"));
        let path = retained.path().to_owned();
        drop(retained);
        assert!(!path.exists());
    }
    // Also compile/exercise capture of a running image in this integration
    // target, so the shared module has no test-only alternate implementation.
    let current = launcher::Launcher::retain(directory.path()).unwrap();
    assert!(current.path().is_file());
}
