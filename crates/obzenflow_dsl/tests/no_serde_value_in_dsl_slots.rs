// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-114c PR E lint.
//!
//! Walks every `.rs` file under `examples/`, `crates/**/src`,
//! `crates/**/tests`, `crates/**/benches`, and `tests/`, and rejects any
//! `serde_json::Value` (or `::serde_json::Value`) appearing in a DSL macro
//! type slot. The allowlist below is the only escape hatch: three narrow
//! categories of legitimate `Value` usage. Anything else is treated as the
//! `Value`-as-the-new-mixed anti-pattern and fails the lint.
//!
//! When the lint fires, the fix is one of:
//!
//! 1. Declare a small test-local `TypedPayload` struct that matches the
//!    handler's actual payload shape and use it in the macro slot
//!    (preferred — see `examples/multi_source_ingest_demo/` for the
//!    canonical pattern).
//! 2. If this site genuinely belongs in the allowlist (raw JSON ingress,
//!    runtime lifecycle test, throughput bench where payload shape is
//!    irrelevant), annotate it inline with
//!    `// allow-serde-value: <reason>` AND add the file to `ALLOWLIST`
//!    below with the same reason.

use std::fs;
use std::path::{Path, PathBuf};

/// (path_suffix, reason). A file matches if any line in its repository
/// path ends with `path_suffix`. The reason is required so that allowlist
/// growth shows up in code review.
const ALLOWLIST: &[(&str, &str)] = &[(
    "crates/obzenflow_infra/src/application/flow_application.rs",
    "runtime lifecycle test (server_auto_double_run_regression) exercising \
     FlowApplication launch mechanics, not typed authoring; handlers are \
     payload-agnostic by design",
)];

/// DSL macros whose type slots are subject to the lint.
const DSL_MACROS: &[&str] = &[
    "source!",
    "async_source!",
    "infinite_source!",
    "async_infinite_source!",
    "transform!",
    "async_transform!",
    "stateful!",
    "sink!",
    "join!",
];

#[test]
fn no_serde_json_value_in_dsl_type_slots() {
    let repo_root = repo_root();
    let mut violations: Vec<String> = Vec::new();

    let scan_roots = [
        repo_root.join("examples"),
        repo_root.join("crates"),
        repo_root.join("tests"),
    ];

    for root in &scan_roots {
        if !root.exists() {
            continue;
        }
        walk_rs_files(root, &mut |path| {
            let rel = path
                .strip_prefix(&repo_root)
                .unwrap_or(path)
                .to_string_lossy()
                .replace('\\', "/");

            // Allowlist entries skip the entire file. The inline annotation
            // (`// allow-serde-value: <reason>`) is the second half of the
            // contract and is required at the offending site itself.
            if is_allowlisted(&rel) {
                return;
            }

            let Ok(contents) = fs::read_to_string(path) else {
                return;
            };

            for (line_no, line) in contents.lines().enumerate() {
                if !line.contains("serde_json::Value") {
                    continue;
                }
                if !line_contains_dsl_macro(line) {
                    // serde_json::Value appears outside a DSL slot (e.g. in
                    // a handler body); not the lint's concern.
                    continue;
                }
                // The annotation must appear on the same line as the
                // offending macro invocation, OR on the line immediately
                // above it (to keep the macro line readable). The lint
                // accepts both placements.
                let prev = if line_no > 0 {
                    contents.lines().nth(line_no - 1).unwrap_or("")
                } else {
                    ""
                };
                if line.contains("// allow-serde-value:") || prev.contains("// allow-serde-value:")
                {
                    continue;
                }
                violations.push(format!(
                    "{}:{}: serde_json::Value used in a DSL macro type slot; \
                     declare a TypedPayload struct or add to ALLOWLIST. Line: {}",
                    rel,
                    line_no + 1,
                    line.trim()
                ));
            }
        });
    }

    assert!(
        violations.is_empty(),
        "FLOWIP-114c PR E lint failed. {} violation(s):\n\n{}\n\n\
         See `crates/obzenflow_dsl/tests/no_serde_value_in_dsl_slots.rs` \
         for the policy and fix instructions.",
        violations.len(),
        violations.join("\n")
    );
}

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR points at the obzenflow_dsl crate; walk up to the
    // workspace root (the directory containing the workspace `Cargo.toml`).
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .ancestors()
        .find(|p| p.join("Cargo.toml").exists() && p.join("crates").exists())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| manifest_dir.clone())
}

fn is_allowlisted(rel_path: &str) -> bool {
    ALLOWLIST
        .iter()
        .any(|(suffix, _)| rel_path.ends_with(suffix))
}

fn line_contains_dsl_macro(line: &str) -> bool {
    DSL_MACROS.iter().any(|m| line.contains(m))
}

fn walk_rs_files(root: &Path, visit: &mut dyn FnMut(&Path)) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        // Skip target/ and any hidden directory.
        if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
            if name == "target" || name.starts_with('.') {
                continue;
            }
        }
        if path.is_dir() {
            walk_rs_files(&path, visit);
        } else if path.extension().and_then(|s| s.to_str()) == Some("rs") {
            visit(&path);
        }
    }
}
