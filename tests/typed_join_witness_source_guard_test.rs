// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Architecture and erased-boundary guard for FLOWIP-134f.

use proc_macro2::{TokenStream, TokenTree};
use std::fs;
use std::path::{Path, PathBuf};
use syn::visit::Visit;

const RETIRED_IDENTIFIERS: &[&str] = &[
    "JoinHandler",
    "JoinTyping",
    "JoinStrategyOutput",
    "__obzenflow_join_untyped",
];

const ERASED_IMPL_ALLOWLIST: &str = include_str!("fixtures/flowip_134f_erased_join_allowlist.txt");

fn erased_impl_allowlist() -> Vec<(String, String)> {
    ERASED_IMPL_ALLOWLIST
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let fields = line.split('|').map(str::trim).collect::<Vec<_>>();
            assert_eq!(fields.len(), 3, "invalid FLOWIP-134f allowlist row: {line}");
            assert_eq!(
                fields[2], "framework_erasure",
                "unknown FLOWIP-134f erased-join classification in: {line}"
            );
            Some((fields[0].to_string(), fields[1].to_string()))
        })
        .collect()
}

fn rust_sources_under(path: &Path, output: &mut Vec<PathBuf>) {
    if !path.is_dir() {
        return;
    }
    for entry in fs::read_dir(path).expect("read source directory") {
        let path = entry.expect("read source entry").path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "compile_fail") {
                continue;
            }
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
    for entry in fs::read_dir(root.join("crates")).expect("read crates directory") {
        let crate_root = entry.expect("read crate entry").path();
        for relative in ["src", "tests", "benches", "examples"] {
            rust_sources_under(&crate_root.join(relative), &mut sources);
        }
    }
    sources.sort();
    sources
}

fn implementing_type_name(ty: &syn::Type) -> String {
    match ty {
        syn::Type::Path(path) => path
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
            .unwrap_or_else(|| "<empty-path>".to_string()),
        _ => "<non-path-type>".to_string(),
    }
}

#[derive(Default)]
struct SurfaceVisitor {
    retired: Vec<String>,
    erased_implementors: Vec<String>,
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

    fn visit_item_impl(&mut self, implementation: &'ast syn::ItemImpl) {
        let implements_erased_join = implementation
            .trait_
            .as_ref()
            .and_then(|(_, path, _)| path.segments.last())
            .is_some_and(|segment| segment.ident == "UnifiedJoinHandler");
        if implements_erased_join {
            self.erased_implementors
                .push(implementing_type_name(&implementation.self_ty));
        }
        syn::visit::visit_item_impl(self, implementation);
    }
}

#[test]
fn typed_join_is_the_only_authored_surface_and_erasure_has_one_adapter() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut retired_hits = Vec::new();
    let mut erased_impls = Vec::new();

    for path in workspace_rust_sources(&root) {
        let source = fs::read_to_string(&path).expect("read Rust source");
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));
        let mut visitor = SurfaceVisitor::default();
        visitor.visit_file(&syntax);
        let relative = path
            .strip_prefix(&root)
            .expect("workspace-relative source")
            .to_string_lossy()
            .replace('\\', "/");
        retired_hits.extend(
            visitor
                .retired
                .into_iter()
                .map(|identifier| format!("{relative}: {identifier}")),
        );
        erased_impls.extend(
            visitor
                .erased_implementors
                .into_iter()
                .map(|implementor| (relative.clone(), implementor)),
        );
    }

    assert!(
        retired_hits.is_empty(),
        "retired join authoring identifiers reappeared:\n{}",
        retired_hits.join("\n")
    );

    let expected = erased_impl_allowlist();
    assert_eq!(
        erased_impls, expected,
        "the sealed erased join implementation census changed; review any new adapter explicitly"
    );

    let descriptor =
        fs::read_to_string(root.join("crates/obzenflow_dsl/src/dsl/stage_descriptor.rs"))
            .expect("read join descriptor source");
    assert!(
        descriptor.contains("pub(crate) struct JoinDescriptor"),
        "raw JoinDescriptor construction must remain crate-private"
    );
}
