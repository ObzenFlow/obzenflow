// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-133f lightweight root-facade source-shape tripwire.

use std::fs;
use std::path::{Path, PathBuf};

fn rust_sources_under(path: &Path, output: &mut Vec<PathBuf>) {
    if !path.is_dir() {
        return;
    }

    for entry in fs::read_dir(path).expect("read root source directory") {
        let path = entry.expect("read root source entry").path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "bin") {
                continue;
            }
            rust_sources_under(&path, output);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            output.push(path);
        }
    }
}

fn item_kind(item: &syn::Item) -> &'static str {
    match item {
        syn::Item::Const(_) => "constant",
        syn::Item::Enum(_) => "enum",
        syn::Item::ExternCrate(_) => "extern crate",
        syn::Item::Fn(_) => "function",
        syn::Item::ForeignMod(_) => "foreign module",
        syn::Item::Impl(_) => "implementation",
        syn::Item::Macro(_) => "item macro",
        syn::Item::Static(_) => "static",
        syn::Item::Struct(_) => "struct",
        syn::Item::Trait(_) => "trait",
        syn::Item::TraitAlias(_) => "trait alias",
        syn::Item::Type(_) => "type alias",
        syn::Item::Union(_) => "union",
        syn::Item::Verbatim(_) => "unparsed item",
        syn::Item::Mod(_) | syn::Item::Use(_) => "allowed facade item",
        _ => "unsupported item",
    }
}

fn collect_violations(items: &[syn::Item], path: &Path, violations: &mut Vec<String>) {
    for item in items {
        match item {
            syn::Item::Use(_) => {}
            syn::Item::Mod(module) => {
                if let Some((_, items)) = &module.content {
                    collect_violations(items, path, violations);
                }
            }
            forbidden => violations.push(format!("{}: {}", path.display(), item_kind(forbidden))),
        }
    }
}

#[test]
fn root_library_source_contains_only_facade_items() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let typed_root = root.join("src/typed");
    assert!(
        !typed_root.exists(),
        "retired root namespace exists: {}",
        typed_root.display()
    );

    let mut sources = Vec::new();
    rust_sources_under(&root.join("src"), &mut sources);
    sources.sort();

    let mut violations = Vec::new();
    for source_path in sources {
        let source = fs::read_to_string(&source_path).expect("read root library source");
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("parse {}: {error}", source_path.display()));
        let relative = source_path
            .strip_prefix(&root)
            .expect("root library source has relative path");
        collect_violations(&syntax.items, relative, &mut violations);
    }

    assert!(
        violations.is_empty(),
        "root library source contains implementation-bearing items:\n{}",
        violations.join("\n")
    );
}
