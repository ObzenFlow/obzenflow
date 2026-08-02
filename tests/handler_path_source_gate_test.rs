// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-133a supported-surface gate for exported lowering helpers.

use std::fs;
use std::path::{Path, PathBuf};

const LOWERING_IMPLEMENTATION: &str = "crates/obzenflow_dsl/src/dsl/stage_macros.rs";
const FLOW_IMPLEMENTATION: &str = "crates/obzenflow_dsl/src/dsl/dsl.rs";
const FOCUSED_HELPER_TESTS: &str =
    "crates/obzenflow_dsl/src/dsl/tests/typed_decoration_matrix_test.rs";

fn collect_rust_files(directory: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()))
    {
        let path = entry.expect("failed to read source-tree entry").path();
        if path.is_dir() {
            collect_rust_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

fn exported_helper_invocation_lines(source: &str) -> Vec<usize> {
    let prefix = concat!("__obzenflow", "_");
    let bytes = source.as_bytes();
    let mut search_from = 0;
    let mut lines = Vec::new();

    while let Some(relative_offset) = source[search_from..].find(prefix) {
        let offset = search_from + relative_offset;
        let mut cursor = offset + prefix.len();

        while cursor < bytes.len()
            && (bytes[cursor].is_ascii_alphanumeric() || bytes[cursor] == b'_')
        {
            cursor += 1;
        }
        while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }

        if bytes.get(cursor) == Some(&b'!') {
            lines.push(
                source[..offset]
                    .bytes()
                    .filter(|byte| *byte == b'\n')
                    .count()
                    + 1,
            );
        }

        search_from = offset + prefix.len();
    }

    lines
}

fn authored_flow_invocation_lines(source: &str) -> Vec<usize> {
    let needle = "flow! {";
    let bytes = source.as_bytes();
    let mut search_from = 0;
    let mut lines = Vec::new();

    while let Some(relative_offset) = source[search_from..].find(needle) {
        let offset = search_from + relative_offset;
        let previous = offset.checked_sub(1).and_then(|index| bytes.get(index));
        let is_part_of_a_longer_identifier = previous
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'!'));

        if !is_part_of_a_longer_identifier {
            lines.push(
                source[..offset]
                    .bytes()
                    .filter(|byte| *byte == b'\n')
                    .count()
                    + 1,
            );
        }

        search_from = offset + needle.len();
    }

    lines
}

#[test]
fn first_party_declarations_do_not_call_exported_lowering_helpers_directly() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut rust_files = Vec::new();
    for source_root in ["crates", "examples", "tests"] {
        collect_rust_files(&repository.join(source_root), &mut rust_files);
    }

    let allowed = [
        LOWERING_IMPLEMENTATION,
        FLOW_IMPLEMENTATION,
        FOCUSED_HELPER_TESTS,
    ];
    let mut violations = Vec::new();

    for path in rust_files {
        let relative = path
            .strip_prefix(&repository)
            .expect("source path must be inside the repository");
        if allowed.iter().any(|allowed| relative == Path::new(allowed)) {
            continue;
        }

        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        for line in exported_helper_invocation_lines(&source) {
            violations.push(format!("{}:{line}", relative.display()));
        }
    }

    assert!(
        violations.is_empty(),
        "supported author declarations must not call exported lowering helpers directly:\n{}",
        violations.join("\n")
    );
}

#[test]
fn compiling_first_party_flow_files_name_the_deferred_materialisation_boundary() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut rust_files = Vec::new();
    for source_root in ["crates", "examples", "src", "tests"] {
        collect_rust_files(&repository.join(source_root), &mut rust_files);
    }

    let mut violations = Vec::new();
    for path in rust_files {
        let relative = path
            .strip_prefix(&repository)
            .expect("source path must be inside the repository");
        if relative.starts_with("tests/compile_fail") {
            continue;
        }

        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        if source.contains("#![cfg(any())]") {
            continue;
        }

        let invocations = authored_flow_invocation_lines(&source);
        if !invocations.is_empty() && !source.contains("FlowDefinition::materialize") {
            violations.extend(
                invocations
                    .into_iter()
                    .map(|line| format!("{}:{line}", relative.display())),
            );
        }
    }

    assert!(
        violations.is_empty(),
        "compiling first-party files with authored `flow!` declarations must name \
         `FlowDefinition::materialize` (negative fixtures and cfg-disabled legacy harnesses are \
         excluded):\n{}",
        violations.join("\n")
    );
}
