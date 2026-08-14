// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115m Part 2 guard: ordinary interception points must not quietly
//! regrow the retired publication, logging, measurement, or commit APIs.

use std::fs;
use std::path::{Path, PathBuf};

const FORBIDDEN: &[&str] = &[
    "ObserverReport",
    "ObserverDiagnostic",
    "ObserverEvidence",
    "ObserverDeterminism",
    "OutputCommitObserver",
    "ObserverCommitResult",
    "ObserverCommitError",
    "DiagnosticProvenance",
    "LatencyMeasurement",
    "IndicatorSample",
    "log_event(",
    "indicator()",
    "trace_mirror",
    "observation_file",
];

#[test]
fn production_and_examples_exclude_retired_observer_product_vocabulary() {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut roots = vec![workspace.join("src"), workspace.join("examples")];
    for entry in fs::read_dir(workspace.join("crates")).expect("read workspace crates") {
        let path = entry.expect("crate directory entry").path().join("src");
        if path.is_dir() {
            roots.push(path);
        }
    }

    let mut violations = Vec::new();
    for root in roots {
        scan_dir(&root, &mut violations);
    }
    assert!(
        violations.is_empty(),
        "retired observer product vocabulary returned to production code:\n{}",
        violations.join("\n")
    );
}

fn scan_dir(path: &Path, violations: &mut Vec<String>) {
    for entry in fs::read_dir(path).unwrap_or_else(|error| {
        panic!("failed to read {}: {error}", path.display());
    }) {
        let path = entry.expect("directory entry").path();
        if path.is_dir() {
            scan_dir(&path, violations);
            continue;
        }
        if !matches!(
            path.extension().and_then(|extension| extension.to_str()),
            Some("rs" | "md" | "toml")
        ) {
            continue;
        }

        let contents = fs::read_to_string(&path).unwrap_or_else(|error| {
            panic!("failed to read {}: {error}", path.display());
        });
        for forbidden in FORBIDDEN {
            if contents.contains(forbidden) {
                violations.push(format!("{} contains `{forbidden}`", path.display()));
            }
        }
    }
}
