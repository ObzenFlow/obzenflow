// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-128a D5/D9 source guards (the drift-guard pattern).
//!
//! The `is_composite`/`try_lower_composite` hook was deleted with no
//! transitional adapter; this guard fails the build if either method
//! definition reappears anywhere in the crate. The composition module stays
//! `#[doc(hidden)]`: public for macro reachability, not a stability surface,
//! until a plugin FLOWIP de-hides it deliberately.

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    fn walk_rs_files(dir: &Path, visit: &mut dyn FnMut(&Path, &str)) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk_rs_files(&path, visit);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                if let Ok(contents) = std::fs::read_to_string(&path) {
                    visit(&path, &contents);
                }
            }
        }
    }

    #[test]
    fn composite_hook_methods_do_not_reappear() {
        const FORBIDDEN: &[&str] = &["fn is_composite", "fn try_lower_composite"];

        let src_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut violations: Vec<String> = Vec::new();
        walk_rs_files(&src_root, &mut |path, contents| {
            if path
                .file_name()
                .is_some_and(|name| name == "composite_source_guards_test.rs")
            {
                return;
            }
            for pattern in FORBIDDEN {
                if contents.contains(pattern) {
                    violations.push(format!(
                        "{} reintroduces `{pattern}`; composites are FlowMember::Composite \
                         (FLOWIP-128a D5), never a StageDescriptor hook",
                        path.display()
                    ));
                }
            }
        });
        assert!(violations.is_empty(), "{}", violations.join("\n"));
    }

    #[test]
    fn composition_module_stays_doc_hidden() {
        let mod_rs = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/dsl/mod.rs");
        let contents = std::fs::read_to_string(&mod_rs).expect("dsl/mod.rs readable");
        assert!(
            contents.contains("#[doc(hidden)]\npub mod composition;"),
            "the composition module must remain #[doc(hidden)] (FLOWIP-128a D9); \
             a plugin FLOWIP de-hides it deliberately"
        );
    }
}
