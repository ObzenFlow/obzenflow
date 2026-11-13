// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use proc_macro2::{TokenStream, TokenTree};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use syn::visit::Visit;

const RETIRED_IDENTIFIERS: &[&str] = &[
    "AdmissionSeq",
    "admission_seq",
    "set_admission_seq",
    "seed_admission_from_archive",
    "max_recorded_admission_seq",
    "admission_sequencer",
    "with_admission_sequencer",
    "seq_ordered",
    "with_seq_ordered",
    "seq_ordered_fan_in",
    "with_seq_ordered_fan_ins",
    "derive_seq_ordered_fan_ins",
    "ordered_delivery",
];

const RUNTIME_ONLY_IDENTIFIERS: &[&str] = &[
    "RecordedOrder",
    "AfterRecord",
    "ArchivedSelection",
    "RecordedSubscription",
    "InputSelection",
    "SelectedInput",
    "input_selected_event",
    "load_selection_record",
    "with_recorded_order",
];

fn rust_sources_under(path: &Path, output: &mut Vec<PathBuf>) {
    if path.is_file() {
        output.push(path.to_path_buf());
        return;
    }
    if !path.is_dir() {
        return;
    }
    for entry in fs::read_dir(path).expect("read source directory") {
        let path = entry.expect("read source entry").path();
        if path.is_dir() {
            rust_sources_under(&path, output);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            output.push(path);
        }
    }
}

#[derive(Default)]
struct Identifiers(BTreeSet<String>);

impl Identifiers {
    fn record_tokens(&mut self, tokens: TokenStream) {
        for token in tokens {
            match token {
                TokenTree::Group(group) => self.record_tokens(group.stream()),
                TokenTree::Ident(ident) => {
                    self.0.insert(ident.to_string());
                }
                TokenTree::Punct(_) | TokenTree::Literal(_) => {}
            }
        }
    }
}

impl<'ast> Visit<'ast> for Identifiers {
    fn visit_ident(&mut self, ident: &'ast proc_macro2::Ident) {
        self.0.insert(ident.to_string());
    }

    fn visit_macro(&mut self, mac: &'ast syn::Macro) {
        self.record_tokens(mac.tokens.clone());
        syn::visit::visit_macro(self, mac);
    }
}

fn uses(paths: &[PathBuf], listed: &[&str]) -> Vec<String> {
    let mut found = Vec::new();
    for path in paths {
        let source = fs::read_to_string(path).expect("read Rust source");
        let file = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));
        let mut identifiers = Identifiers::default();
        identifiers.visit_file(&file);
        for identifier in listed {
            if identifiers.0.contains(*identifier) {
                found.push(format!("{}: {identifier}", path.display()));
            }
        }
    }
    found
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn the_admission_counter_is_retired_everywhere() {
    let root = root();
    let mut sources = Vec::new();
    for relative in ["src", "examples", "tests", "benches", "xtask/src"] {
        rust_sources_under(&root.join(relative), &mut sources);
    }
    for crate_entry in fs::read_dir(root.join("crates")).expect("read crates directory") {
        let crate_root = crate_entry.expect("read crate entry").path();
        for relative in ["src", "tests", "benches", "examples"] {
            rust_sources_under(&crate_root.join(relative), &mut sources);
        }
    }
    let this_guard = root.join(file!());
    sources.retain(|path| path != &this_guard && !path.ends_with(file!()));
    assert_eq!(uses(&sources, RETIRED_IDENTIFIERS), Vec::<String>::new());
}

#[test]
fn recorded_order_adds_no_authoring_surface() {
    let root = root();
    let mut authoring = Vec::new();
    for relative in [
        "src/flow.rs",
        "src/prelude.rs",
        "src/stages",
        "crates/obzenflow_dsl/src/dsl/stage_macros.rs",
        "crates/obzenflow_dsl/src/dsl/dsl.rs",
        "crates/obzenflow_dsl/src/dsl/topology.rs",
    ] {
        rust_sources_under(&root.join(relative), &mut authoring);
    }
    assert!(authoring.len() > 3, "the authoring sources were found");
    assert_eq!(
        uses(&authoring, RUNTIME_ONLY_IDENTIFIERS),
        Vec::<String>::new()
    );
}
