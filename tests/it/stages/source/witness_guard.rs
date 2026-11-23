// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use proc_macro2::{TokenStream, TokenTree};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use syn::visit::Visit;

const RETIRED_IDENTIFIERS: &[&str] = &[
    "__obzenflow_source_untyped",
    "__obzenflow_async_source_untyped",
    "__obzenflow_infinite_source_untyped",
    "__obzenflow_async_infinite_source_untyped",
];

fn rust_sources_under(path: &Path, output: &mut Vec<PathBuf>) {
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

fn workspace_rust_sources(root: &Path) -> Vec<PathBuf> {
    let mut sources = Vec::new();
    for relative in ["src", "examples", "tests", "benches"] {
        rust_sources_under(&root.join(relative), &mut sources);
    }
    for crate_entry in fs::read_dir(root.join("crates")).expect("read crates directory") {
        let crate_root = crate_entry.expect("read crate entry").path();
        for relative in ["src", "tests", "benches", "examples"] {
            rust_sources_under(&crate_root.join(relative), &mut sources);
        }
    }
    sources.sort();
    sources
}

#[derive(Default)]
struct SurfaceVisitor {
    retired: Vec<String>,
}

impl SurfaceVisitor {
    fn record_ident(&mut self, ident: String) {
        if RETIRED_IDENTIFIERS.contains(&ident.as_str()) {
            self.retired.push(ident);
        }
    }

    fn visit_macro_tokens(&mut self, tokens: TokenStream) {
        for token in tokens {
            match token {
                TokenTree::Group(group) => self.visit_macro_tokens(group.stream()),
                TokenTree::Ident(ident) => self.record_ident(ident.to_string()),
                TokenTree::Punct(_) | TokenTree::Literal(_) => {}
            }
        }
    }
}

impl<'ast> Visit<'ast> for SurfaceVisitor {
    fn visit_ident(&mut self, ident: &'ast syn::Ident) {
        self.record_ident(ident.to_string());
    }

    fn visit_token_stream(&mut self, tokens: &'ast TokenStream) {
        self.visit_macro_tokens(tokens.clone());
    }
}

#[test]
fn retired_source_compatibility_macros_stay_absent() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut violations = Vec::new();

    for source_path in workspace_rust_sources(&root) {
        let source = fs::read_to_string(&source_path).expect("read Rust source");
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("parse {}: {error}", source_path.display()));
        let mut visitor = SurfaceVisitor::default();
        visitor.visit_file(&syntax);
        let relative = source_path
            .strip_prefix(&root)
            .expect("workspace-relative source")
            .display();
        violations.extend(
            visitor
                .retired
                .into_iter()
                .map(|identifier| format!("{relative}: {identifier}")),
        );
    }

    assert!(
        violations.is_empty(),
        "retired source compatibility identifiers resurfaced:\n{}",
        violations.join("\n")
    );
}

/// Finds production expressions naming `ReattachGuarantee::SenderRedelivers`;
/// matching it as a pattern reads evidence and records nothing.
#[derive(Default)]
struct SenderRedeliversVisitor {
    records: bool,
}

fn is_test_only(attributes: &[syn::Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        attribute.path().is_ident("test")
            || (attribute.path().is_ident("cfg")
                && attribute
                    .meta
                    .require_list()
                    .is_ok_and(|list| list.tokens.to_string() == "test"))
    })
}

impl<'ast> Visit<'ast> for SenderRedeliversVisitor {
    fn visit_item_mod(&mut self, module: &'ast syn::ItemMod) {
        if !is_test_only(&module.attrs) {
            syn::visit::visit_item_mod(self, module);
        }
    }

    fn visit_item_fn(&mut self, function: &'ast syn::ItemFn) {
        if !is_test_only(&function.attrs) {
            syn::visit::visit_item_fn(self, function);
        }
    }

    // syn visits a path pattern as an expression path.
    fn visit_pat(&mut self, _pattern: &'ast syn::Pat) {}

    fn visit_expr_path(&mut self, expression: &'ast syn::ExprPath) {
        let segments: Vec<String> = expression
            .path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect();
        if segments.ends_with(&["ReattachGuarantee".into(), "SenderRedelivers".into()]) {
            self.records = true;
        }
        syn::visit::visit_expr_path(self, expression);
    }
}

/// FLOWIP-122i B3: only Runtime's hosted-ingress admission records that a
/// sender redelivers. No connector, adapter, DSL or application code can
/// declare it, because only the source's single writer acknowledges after
/// commit.
#[test]
fn only_hosted_ingress_records_that_its_sender_redelivers() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut recorders = BTreeSet::new();

    for source_path in workspace_rust_sources(&root) {
        let relative = source_path
            .strip_prefix(&root)
            .expect("workspace-relative source")
            .to_string_lossy()
            .replace('\\', "/");
        if relative.split('/').any(|part| part == "tests") {
            continue;
        }
        let source = fs::read_to_string(&source_path).expect("read Rust source");
        if !source.contains("SenderRedelivers") {
            continue;
        }
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("parse {}: {error}", source_path.display()));
        let mut visitor = SenderRedeliversVisitor::default();
        visitor.visit_file(&syntax);
        if visitor.records {
            recorders.insert(relative);
        }
    }

    assert_eq!(
        recorders,
        BTreeSet::from([
            "crates/obzenflow_runtime/src/stages/common/handlers/source/hosted.rs".to_string()
        ])
    );
}

#[test]
fn erased_source_descriptors_remain_crate_private() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let descriptor =
        fs::read_to_string(root.join("crates/obzenflow_dsl/src/dsl/stage_descriptor.rs"))
            .expect("read source descriptor module");

    for name in [
        "AsyncFiniteSourceDescriptor",
        "AsyncInfiniteSourceDescriptor",
    ] {
        assert!(
            descriptor.contains(&format!("pub(crate) struct {name}")),
            "{name} must remain crate-private so raw erasure cannot become an authoring path"
        );
    }
}
