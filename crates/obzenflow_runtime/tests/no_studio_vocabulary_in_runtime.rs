// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::fs;
use std::path::{Path, PathBuf};

const FORBIDDEN: &[&str] = &[
    "Studio",
    "studiod",
    "phonebook",
    "registry badge",
    "PipelinePhase",
];

#[test]
fn runtime_and_core_lifecycle_layers_do_not_name_studio_adapters() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let scan_roots = [
        manifest_dir.join("../obzenflow_core/src/event"),
        manifest_dir.join("src/pipeline"),
        manifest_dir.join("src/stages"),
        manifest_dir.join("src/metrics"),
    ];

    let mut violations = Vec::new();
    for root in scan_roots {
        scan_dir(&root, &mut violations);
    }

    assert!(
        violations.is_empty(),
        "Studio adapter vocabulary must stay out of core/runtime lifecycle layers:\n{}",
        violations.join("\n")
    );
}

fn scan_dir(path: &Path, violations: &mut Vec<String>) {
    let entries = fs::read_dir(path).unwrap_or_else(|err| {
        panic!("failed to read {}: {err}", path.display());
    });

    for entry in entries {
        let entry = entry.expect("read_dir entry");
        let path = entry.path();
        if path.is_dir() {
            scan_dir(&path, violations);
            continue;
        }

        if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
            continue;
        }

        let contents = fs::read_to_string(&path).unwrap_or_else(|err| {
            panic!("failed to read {}: {err}", path.display());
        });
        for forbidden in FORBIDDEN {
            if contents.contains(forbidden) {
                violations.push(format!("{} contains `{forbidden}`", path.display()));
            }
        }
    }
}
