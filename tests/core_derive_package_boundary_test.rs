// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use serde_json::Value;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

const CORE: &str = "obzenflow_core";
const SATELLITE: &str = "obzenflow_core_derive";
const RETIRED_SATELLITE: &str = "obzenflow_derive";

#[test]
fn core_derive_is_a_core_owned_compiler_satellite() {
    let metadata = cargo_metadata();
    let packages = metadata["packages"].as_array().expect("metadata packages");
    let workspace_member_ids = metadata["workspace_members"]
        .as_array()
        .expect("workspace member ids")
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let workspace_packages = packages
        .iter()
        .filter(|package| {
            package["id"]
                .as_str()
                .is_some_and(|id| workspace_member_ids.contains(id))
        })
        .collect::<Vec<_>>();

    assert!(
        workspace_packages
            .iter()
            .all(|package| package["name"] != RETIRED_SATELLITE),
        "the retired package name must not remain in workspace metadata"
    );

    let core = workspace_package(&workspace_packages, CORE);
    let satellite = workspace_package(&workspace_packages, SATELLITE);

    let first_party_names = workspace_packages
        .iter()
        .filter_map(|package| package["name"].as_str())
        .collect::<BTreeSet<_>>();
    let satellite_first_party_dependencies = dependencies(satellite)
        .iter()
        .filter_map(|dependency| dependency["name"].as_str())
        .filter(|name| first_party_names.contains(name))
        .collect::<BTreeSet<_>>();
    assert!(
        satellite_first_party_dependencies.is_empty(),
        "the Core compiler satellite must not depend on first-party packages: {satellite_first_party_dependencies:?}"
    );

    let reverse_dependencies = workspace_packages
        .iter()
        .copied()
        .filter(|package| {
            dependencies(package)
                .iter()
                .any(|dependency| dependency["name"] == SATELLITE)
        })
        .filter_map(|package| package["name"].as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        reverse_dependencies,
        BTreeSet::from([CORE]),
        "Core must be the satellite's sole first-party reverse dependency across all dependency kinds and targets"
    );

    let core_satellite_edges = dependencies(core)
        .iter()
        .filter(|dependency| dependency["name"] == SATELLITE)
        .collect::<Vec<_>>();
    assert_eq!(
        core_satellite_edges.len(),
        1,
        "Core must declare exactly one dependency on its compiler satellite"
    );
    let edge = core_satellite_edges[0];
    assert!(
        edge["kind"].is_null() && edge["target"].is_null() && edge["optional"] == false,
        "Core's compiler satellite must be one unconditional normal dependency"
    );

    assert_codegen_names_only_core_contracts();
}

fn cargo_metadata() -> Value {
    let output = Command::new(env!("CARGO"))
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("cargo metadata launches");
    assert!(
        output.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("metadata is JSON")
}

fn workspace_package<'a>(packages: &[&'a Value], name: &str) -> &'a Value {
    packages
        .iter()
        .copied()
        .find(|package| package["name"] == name)
        .unwrap_or_else(|| panic!("workspace package {name} exists"))
}

fn dependencies(package: &Value) -> &[Value] {
    package["dependencies"]
        .as_array()
        .expect("package dependencies")
}

fn assert_codegen_names_only_core_contracts() {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("crates")
        .join(SATELLITE)
        .join("src");
    let mut sources = Vec::new();
    collect_rust_sources(&source_root, &mut sources);
    sources.sort();
    assert!(!sources.is_empty(), "compiler satellite has Rust source");

    for source_path in sources {
        let source = std::fs::read_to_string(&source_path)
            .unwrap_or_else(|error| panic!("read {}: {error}", source_path.display()));
        for forbidden in [
            "obzenflow_runtime",
            "obzenflow_adapters",
            "obzenflow_dsl",
            "obzenflow_infra",
            "obzenflow::",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} must not name outer crate root `{forbidden}`",
                source_path.display()
            );
        }
    }
}

fn collect_rust_sources(directory: &Path, sources: &mut Vec<PathBuf>) {
    let entries = std::fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("read source directory {}: {error}", directory.display()));
    for entry in entries {
        let entry = entry.expect("read source directory entry");
        let path = entry.path();
        if path.is_dir() {
            collect_rust_sources(&path, sources);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            sources.push(path);
        }
    }
}
