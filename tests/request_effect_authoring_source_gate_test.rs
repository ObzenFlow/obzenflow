// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::fs;
use std::path::{Path, PathBuf};

fn rust_files(directory: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()))
    {
        let path = entry.expect("failed to read an example entry").path();
        if path.is_dir() {
            rust_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

#[test]
fn examples_perform_requests_without_binding_plumbing() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    rust_files(&root.join("examples"), &mut files);
    assert!(!files.is_empty(), "the examples hold Rust sources");

    let plumbing = [
        "project_named_effect",
        "EffectBindingUse",
        "EffectRegistrationBuilder",
        "BoundedBindingEvidence",
        "EffectPortSlot",
        "impl NamedEffect",
    ];
    let violations = files
        .iter()
        .flat_map(|file| {
            let source = fs::read_to_string(file).expect("read an example source");
            let shown = file.strip_prefix(&root).unwrap_or(file).display().to_string();
            plumbing
                .iter()
                .filter(move |name| source.contains(**name))
                .map(move |name| format!("{shown} names {name}"))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[test]
fn the_request_examples_perform_their_requests_with_fx_call() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for (relative, requests) in [
        ("examples/aml_overnight_monitoring/stages.rs", 2),
        ("examples/flash_sale_allocation/allocation.rs", 2),
    ] {
        let source = fs::read_to_string(root.join(relative))
            .expect("read a request example")
            .split_whitespace()
            .collect::<String>();
        assert!(
            source.matches("fx.call(").count() >= requests,
            "{relative} performs its requests with `fx.call`"
        );
        assert!(
            !source.contains("fx.perform("),
            "{relative} performs no request with `fx.perform`"
        );
    }
}
