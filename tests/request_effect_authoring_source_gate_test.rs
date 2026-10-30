// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use proc_macro2::{TokenStream, TokenTree};

const RETIRED_NAMES: &[&str] = &[
    "AtLeastOnce",
    "BoundedBindingEvidence",
    "CHAT_CLIENT",
    "Call",
    "CallRequest",
    "CallSafety",
    "EMBEDDING_CLIENT",
    "EffectBindingUse",
    "EffectPortSlot",
    "EffectPortSlotSet",
    "EffectRegistrationBuilder",
    "KeyRule",
    "Named",
    "Portless",
    "RequiresKey",
    "TransactionalEffectPort",
    "project_named_effect",
    "transactional_effect_port_slot",
];

const RETIRED_TRAITS: &[&str] = &["Effect", "NamedEffect"];

const REMOVED_SPELLINGS: &[&str] = &["Call<", "type Safety", "safety =", "impl CallRequest"];

const RETIRED_METHODS: &[&str] = &["perform", "project_named_effect", "transactional"];

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

fn single_member_sets(tokens: TokenStream, found: &mut usize) {
    let tokens = tokens.into_iter().collect::<Vec<_>>();
    for (position, token) in tokens.iter().enumerate() {
        if let TokenTree::Group(group) = token {
            single_member_sets(group.stream(), found);
        }
        let invocation = match &tokens[position..] {
            [TokenTree::Ident(name), TokenTree::Punct(bang), TokenTree::Group(members), ..]
                if name == "effect_set" && bang.as_char() == '!' =>
            {
                members
            }
            _ => continue,
        };
        let members = invocation.stream().into_iter().collect::<Vec<_>>();
        let separated = members
            .iter()
            .any(|member| matches!(member, TokenTree::Punct(comma) if comma.as_char() == ','));
        if !members.is_empty() && !separated {
            *found += 1;
        }
    }
}

#[test]
fn an_example_handler_with_one_effect_names_its_request_alone() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    rust_files(&root.join("examples"), &mut files);
    let wrapped = files
        .iter()
        .filter(|file| {
            let source = fs::read_to_string(file).expect("read an example source");
            let tokens = TokenStream::from_str(&source).expect("tokenize an example source");
            let mut found = 0;
            single_member_sets(tokens, &mut found);
            found > 0
        })
        .map(|file| {
            file.strip_prefix(&root)
                .unwrap_or(file)
                .display()
                .to_string()
        })
        .collect::<Vec<_>>();
    assert!(
        wrapped.is_empty(),
        "write `type AllowedEffects = Request;` for one effect: {}",
        wrapped.join(", ")
    );
}

fn is_doc(attribute: &proc_macro2::Group) -> bool {
    matches!(
        attribute.stream().into_iter().next(),
        Some(TokenTree::Ident(name)) if name == "doc"
    )
}

fn wraps_one_member(text: &str) -> bool {
    text.split("effect_set![").skip(1).any(|rest| {
        let members = rest.split(']').next().unwrap_or_default();
        !members.trim().is_empty() && !members.contains(',')
    })
}

fn removed_spellings(tokens: TokenStream, found: &mut Vec<String>) {
    let tokens = tokens.into_iter().collect::<Vec<_>>();
    let mut position = 0;
    while position < tokens.len() {
        match &tokens[position..] {
            [TokenTree::Punct(hash), TokenTree::Group(attribute), ..]
                if hash.as_char() == '#' && is_doc(attribute) =>
            {
                position += 2;
                continue;
            }
            [TokenTree::Punct(hash), TokenTree::Punct(bang), TokenTree::Group(attribute), ..]
                if hash.as_char() == '#' && bang.as_char() == '!' && is_doc(attribute) =>
            {
                position += 3;
                continue;
            }
            [TokenTree::Ident(keyword), TokenTree::Ident(name), TokenTree::Group(_), ..]
                if keyword == "mod" && name == "tests" =>
            {
                position += 3;
                continue;
            }
            [TokenTree::Group(group), ..] => removed_spellings(group.stream(), found),
            [TokenTree::Literal(literal), ..] => {
                let text = literal.to_string();
                if REMOVED_SPELLINGS
                    .iter()
                    .any(|spelling| text.contains(spelling))
                    || wraps_one_member(&text)
                {
                    found.push(text);
                }
            }
            _ => {}
        }
        position += 1;
    }
}

#[test]
fn framework_diagnostics_teach_no_removed_spelling() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    for entry in fs::read_dir(root.join("crates")).expect("read the framework crates") {
        let source = entry.expect("read a framework crate").path().join("src");
        if source.is_dir() {
            rust_files(&source, &mut files);
        }
    }
    let violations = files
        .iter()
        .filter(|file| {
            !file
                .components()
                .any(|component| component.as_os_str() == "tests")
                && file.file_name().is_some_and(|name| name != "tests.rs")
        })
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
            let mut found = Vec::new();
            removed_spellings(tokens, &mut found);
            found
                .into_iter()
                .map(move |text| format!("{shown}: {text}"))
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
