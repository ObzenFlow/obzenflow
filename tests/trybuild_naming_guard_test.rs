// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Executable form of the `.config/nextest.toml` rule: every test binary that
//! uses trybuild must match `binary(/compile_fail/)`, or it escapes the
//! serialized trybuild group and times out under build-dir lock contention.

use std::path::{Path, PathBuf};

#[test]
fn trybuild_binaries_carry_the_compile_fail_name() {
    // Built at runtime so this guard's own source does not match the needle.
    let needle = ["trybuild::", "TestCases"].concat();

    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut test_dirs = vec![workspace.join("tests")];
    let crates = workspace.join("crates");
    for entry in std::fs::read_dir(&crates).expect("crates directory should be readable") {
        let dir = entry
            .expect("crate directory entry should be readable")
            .path()
            .join("tests");
        if dir.is_dir() {
            test_dirs.push(dir);
        }
    }

    let mut offenders: Vec<PathBuf> = Vec::new();
    for dir in test_dirs {
        for entry in std::fs::read_dir(&dir).expect("test directory should be readable") {
            let path = entry.expect("test file entry should be readable").path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("rs") {
                continue;
            }
            let stem = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or_default();
            let source =
                std::fs::read_to_string(&path).expect("test source should be readable");
            if source.contains(&needle) && !stem.contains("compile_fail") {
                offenders.push(path);
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "every test binary using trybuild must have `compile_fail` in its file name so it \
         matches the nextest binary(/compile_fail/) filter; without it the binary escapes \
         the serialized trybuild-compile-fail group and its 180s budget, and times out \
         under build-dir lock contention: {offenders:?}"
    );
}
