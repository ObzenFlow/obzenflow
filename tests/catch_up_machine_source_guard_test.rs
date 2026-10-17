// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use proc_macro2::{TokenStream, TokenTree};
use std::fs;
use std::path::{Path, PathBuf};
use syn::visit::Visit;

const MACHINE: &str = "crates/obzenflow_runtime/src/stages/common/supervision/catch_up.rs";
const MARKER_FACTORY: &str = "crates/obzenflow_core/src/event/chain_event/factory/control.rs";
const CROSSING_CALLS: &[&str] = &["catch_up_complete_event", "record_generation_boundary"];

fn rust_sources_under(path: &Path, output: &mut Vec<PathBuf>) {
    if !path.is_dir() {
        return;
    }
    for entry in fs::read_dir(path).expect("read source directory") {
        let path = entry.expect("read source entry").path();
        if path.is_dir() {
            if !path.ends_with("tests") {
                rust_sources_under(&path, output);
            }
        } else if path.extension().is_some_and(|extension| extension == "rs")
            && !path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .is_some_and(|stem| stem == "tests" || stem.ends_with("_tests"))
        {
            output.push(path);
        }
    }
}

fn is_test_only(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path()
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "test")
            || (attr.path().is_ident("cfg")
                && attr
                    .parse_args::<syn::Ident>()
                    .is_ok_and(|ident| ident == "test"))
    })
}

#[derive(Default)]
struct CrossingWrites(Vec<String>);

impl CrossingWrites {
    fn record_tokens(&mut self, tokens: TokenStream) {
        for token in tokens {
            match token {
                TokenTree::Group(group) => self.record_tokens(group.stream()),
                TokenTree::Ident(ident) if CROSSING_CALLS.iter().any(|call| ident == call) => {
                    self.0.push(ident.to_string());
                }
                TokenTree::Ident(_) | TokenTree::Punct(_) | TokenTree::Literal(_) => {}
            }
        }
    }
}

impl<'ast> Visit<'ast> for CrossingWrites {
    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        if !is_test_only(&item.attrs) {
            syn::visit::visit_item_mod(self, item);
        }
    }

    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        if !is_test_only(&item.attrs) {
            syn::visit::visit_item_fn(self, item);
        }
    }

    fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
        if !is_test_only(&item.attrs) {
            syn::visit::visit_item_impl(self, item);
        }
    }

    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        if !is_test_only(&item.attrs) {
            syn::visit::visit_impl_item_fn(self, item);
        }
    }

    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        if CROSSING_CALLS.iter().any(|name| call.method == name) {
            self.0.push(call.method.to_string());
        }
        syn::visit::visit_expr_method_call(self, call);
    }

    fn visit_expr_path(&mut self, expr: &'ast syn::ExprPath) {
        if let Some(segment) = expr.path.segments.last() {
            if CROSSING_CALLS.iter().any(|name| segment.ident == name) {
                self.0.push(segment.ident.to_string());
            }
        }
        syn::visit::visit_expr_path(self, expr);
    }

    fn visit_expr_struct(&mut self, expr: &'ast syn::ExprStruct) {
        if expr
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "CatchUpComplete")
        {
            self.0.push("CatchUpComplete".to_string());
        }
        syn::visit::visit_expr_struct(self, expr);
    }

    fn visit_macro(&mut self, mac: &'ast syn::Macro) {
        self.record_tokens(mac.tokens.clone());
        syn::visit::visit_macro(self, mac);
    }
}

fn crossing_writes(path: &Path) -> Vec<String> {
    let source = fs::read_to_string(path).expect("read Rust source");
    let file = syn::parse_file(&source)
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));
    let mut writes = CrossingWrites::default();
    writes.visit_file(&file);
    writes.0
}

#[test]
fn only_the_catch_up_machine_writes_markers_or_records_boundaries() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut sources = Vec::new();
    rust_sources_under(&root.join("src"), &mut sources);
    for crate_entry in fs::read_dir(root.join("crates")).expect("read crates directory") {
        rust_sources_under(
            &crate_entry.expect("read crate entry").path().join("src"),
            &mut sources,
        );
    }
    assert!(sources.len() > 100, "the production sources were found");

    let mut elsewhere = Vec::new();
    for path in &sources {
        let allowed = path.ends_with(MACHINE);
        for write in crossing_writes(path) {
            let factory_builds_marker =
                write == "CatchUpComplete" && path.ends_with(MARKER_FACTORY);
            if !allowed && !factory_builds_marker {
                elsewhere.push(format!(
                    "{}: {write}",
                    path.strip_prefix(&root).unwrap_or(path).display()
                ));
            }
        }
    }
    assert_eq!(elsewhere, Vec::<String>::new());

    let machine = crossing_writes(&root.join(MACHINE));
    for call in CROSSING_CALLS {
        assert!(
            machine.iter().any(|write| write == call),
            "the machine calls {call}"
        );
    }
}
