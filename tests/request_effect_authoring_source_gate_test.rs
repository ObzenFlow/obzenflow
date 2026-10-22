// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use proc_macro2::{TokenStream, TokenTree};

const RETIRED_NAMES: &[&str] = &[
    "BoundedBindingEvidence",
    "CHAT_CLIENT",
    "EMBEDDING_CLIENT",
    "EffectBindingUse",
    "EffectPortSlot",
    "EffectPortSlotSet",
    "EffectRegistrationBuilder",
    "Named",
    "Portless",
    "TransactionalEffectPort",
    "project_named_effect",
    "transactional_effect_port_slot",
];

const RETIRED_TRAITS: &[&str] = &["Effect", "NamedEffect"];

const RETIRED_METHODS: &[&str] = &["perform", "project_named_effect"];

fn rust_files(directory: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()))
    {
        let path = entry.expect("failed to read a directory entry").path();
        if path.is_dir() {
            rust_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

fn retired_surface(tokens: TokenStream, found: &mut BTreeSet<String>) {
    let mut previous: Option<TokenTree> = None;
    for token in tokens {
        match &token {
            TokenTree::Group(group) => retired_surface(group.stream(), found),
            TokenTree::Ident(ident) => {
                let name = ident.to_string();
                if RETIRED_NAMES.contains(&name.as_str()) {
                    found.insert(format!("names {name}"));
                }
                match &previous {
                    Some(TokenTree::Ident(trait_name))
                        if name == "for"
                            && RETIRED_TRAITS.contains(&trait_name.to_string().as_str()) =>
                    {
                        found.insert(format!("implements {trait_name}"));
                    }
                    Some(TokenTree::Punct(punct))
                        if punct.as_char() == '.' && RETIRED_METHODS.contains(&name.as_str()) =>
                    {
                        found.insert(format!("calls .{name}"));
                    }
                    _ => {}
                }
            }
            _ => {}
        }
        previous = Some(token);
    }
}

#[test]
fn examples_tests_and_benchmarks_declare_effects_only_as_requests() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    for directory in ["examples", "tests", "crates/obzenflow_benchmarks"] {
        let before = files.len();
        rust_files(&root.join(directory), &mut files);
        assert!(files.len() > before, "{directory} holds Rust sources");
    }

    let violations = files
        .iter()
        .flat_map(|file| {
            let shown = file
                .strip_prefix(&root)
                .unwrap_or(file)
                .display()
                .to_string();
            let source = fs::read_to_string(file)
                .unwrap_or_else(|error| panic!("failed to read {shown}: {error}"));
            let tokens = TokenStream::from_str(&source)
                .unwrap_or_else(|error| panic!("failed to tokenize {shown}: {error}"));
            let mut found = BTreeSet::new();
            retired_surface(tokens, &mut found);
            found.into_iter().map(move |what| format!("{shown} {what}"))
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
    }
}
