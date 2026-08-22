// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Architecture and erasure-boundary guards for FLOWIP-134b.

use proc_macro2::{TokenStream, TokenTree};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use syn::visit::Visit;

const RETIRED_IDENTIFIERS: &[&str] = &[
    "BoundTransform",
    "TryMap",
    "TryMapWith",
    "TryMapWithTyped",
    "ErrorStrategy",
    "try_map_with",
    "on_error_journal",
    "on_error_emit",
    "on_error_emit_with",
    "on_error_drop",
    "on_error_with",
    "__obzenflow_transform_untyped",
];

const RAW_ALLOWLIST: &str = include_str!("fixtures/raw_transform_handler_allowlist.txt");

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
    raw_implementors: Vec<String>,
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

impl<'ast> Visit<'ast> for SurfaceVisitor {
    fn visit_ident(&mut self, ident: &'ast syn::Ident) {
        self.record_ident(ident.to_string());
    }

    fn visit_token_stream(&mut self, tokens: &'ast TokenStream) {
        self.visit_macro_tokens(tokens.clone());
    }

    fn visit_item_impl(&mut self, implementation: &'ast syn::ItemImpl) {
        let implements_raw_transform = implementation
            .trait_
            .as_ref()
            .and_then(|(_, path, _)| path.segments.last())
            .is_some_and(|segment| segment.ident == "TransformHandler");
        if implements_raw_transform {
            self.raw_implementors
                .push(implementing_type_name(&implementation.self_ty));
        }
        syn::visit::visit_item_impl(self, implementation);
    }
}

fn parse_raw_allowlist() -> BTreeMap<(String, String), String> {
    let mut entries = BTreeMap::new();
    for (line_index, line) in RAW_ALLOWLIST.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields = line.split('|').map(str::trim).collect::<Vec<_>>();
        assert_eq!(
            fields.len(),
            3,
            "invalid raw-handler allowlist line {}: {line}",
            line_index + 1
        );
        assert!(
            matches!(
                fields[0],
                "framework_erasure" | "structural_adapter" | "test_harness"
            ),
            "invalid raw-handler category on line {}: {}",
            line_index + 1,
            fields[0]
        );
        let key = (fields[1].to_string(), fields[2].to_string());
        assert!(
            entries.insert(key.clone(), fields[0].to_string()).is_none(),
            "duplicate raw-handler allowlist entry: {}|{}",
            key.0,
            key.1
        );
    }
    entries
}

#[test]
fn retired_synchronous_transform_surface_stays_absent() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut violations = Vec::new();

    for source_path in workspace_rust_sources(&root) {
        let source = fs::read_to_string(&source_path).expect("read Rust source");
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("parse {}: {error}", source_path.display()));
        let mut visitor = SurfaceVisitor::default();
        visitor.visit_file(&syntax);
        for ident in visitor.retired {
            let relative = source_path
                .strip_prefix(&root)
                .expect("workspace source has relative path")
                .display();
            violations.push(format!("{relative}: {ident}"));
        }
    }

    assert!(
        violations.is_empty(),
        "retired synchronous-transform identifiers resurfaced:\n{}",
        violations.join("\n")
    );
}

#[test]
fn raw_transform_implementations_match_the_checked_in_allowlist() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let expected = parse_raw_allowlist();
    let mut actual = BTreeSet::new();

    for source_path in workspace_rust_sources(&root) {
        let source = fs::read_to_string(&source_path).expect("read Rust source");
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("parse {}: {error}", source_path.display()));
        let mut visitor = SurfaceVisitor::default();
        visitor.visit_file(&syntax);
        let relative = source_path
            .strip_prefix(&root)
            .expect("workspace source has relative path")
            .to_string_lossy()
            .replace('\\', "/");
        for implementor in visitor.raw_implementors {
            assert!(
                actual.insert((relative.clone(), implementor.clone())),
                "duplicate raw TransformHandler implementation: {relative}|{implementor}"
            );
        }
    }

    let expected_keys = expected.keys().cloned().collect::<BTreeSet<_>>();
    let unexpected = actual.difference(&expected_keys).collect::<Vec<_>>();
    let stale = expected_keys.difference(&actual).collect::<Vec<_>>();
    assert!(
        unexpected.is_empty() && stale.is_empty(),
        "raw TransformHandler allowlist drifted\nunexpected: {unexpected:#?}\nstale: {stale:#?}"
    );
}
