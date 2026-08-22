// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Architecture guard for FLOWIP-134a's retired async-transform family.

use proc_macro2::{TokenStream, TokenTree};
use std::fs;
use std::path::{Path, PathBuf};
use syn::visit::Visit;

const RETIRED_IDENTIFIERS: &[&str] = &[
    "AsyncTransformHandler",
    "AsyncTransformHandlerAdapter",
    "AsyncTransformDescriptor",
    "AsyncTransformBuilder",
    "PlaceholderAsyncTransform",
    "BoundAsyncTransform",
    "AsyncMap",
    "AsyncMapTyped",
    "AsyncTryMapWith",
    "AsyncTryMapWithTyped",
    "async_map",
    "async_try_map_with",
    "async_transform",
    "__obzenflow_async_transform_untyped",
    "__obzenflow_async_transform_typed",
    "__obzenflow_async_transform_exact_contract",
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

#[derive(Default)]
struct RetiredIdentifierVisitor {
    found: Vec<String>,
}

impl RetiredIdentifierVisitor {
    fn record_ident(&mut self, ident: String) {
        if RETIRED_IDENTIFIERS.contains(&ident.as_str()) {
            self.found.push(ident);
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

impl<'ast> Visit<'ast> for RetiredIdentifierVisitor {
    fn visit_ident(&mut self, ident: &'ast syn::Ident) {
        self.record_ident(ident.to_string());
    }

    fn visit_token_stream(&mut self, tokens: &'ast TokenStream) {
        self.visit_macro_tokens(tokens.clone());
    }
}

#[test]
fn retirement_guard_descends_into_macro_tokens() {
    let syntax = syn::parse_file(
        r#"
        fn fixture() {
            outer! {
                async_transform!(Input -> Output => handler);
                async_map(handler);
                async_try_map_with(handler);
            }
        }
        "#,
    )
    .expect("parse macro-token regression fixture");
    let mut visitor = RetiredIdentifierVisitor::default();
    visitor.visit_file(&syntax);

    for retired in ["async_transform", "async_map", "async_try_map_with"] {
        assert!(
            visitor.found.iter().any(|found| found == retired),
            "retired identifier {retired:?} inside macro tokens was not visited"
        );
    }
}

#[test]
fn dedicated_async_transform_family_stays_absent() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
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

    let mut violations = Vec::new();
    for source_path in sources {
        let source = fs::read_to_string(&source_path).expect("read Rust source");
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("parse {}: {error}", source_path.display()));
        let mut visitor = RetiredIdentifierVisitor::default();
        visitor.visit_file(&syntax);
        for ident in visitor.found {
            violations.push(format!("{}: {ident}", source_path.display()));
        }
    }

    assert!(
        violations.is_empty(),
        "retired async-transform identifiers resurfaced:\n{}",
        violations.join("\n")
    );
}
