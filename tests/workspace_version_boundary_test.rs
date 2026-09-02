// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use serde_json::Value as JsonValue;
use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;
use toml::Value as TomlValue;

#[test]
fn workspace_members_and_internal_dependencies_use_one_exact_version() {
    let metadata = cargo_metadata();
    let workspace_packages = workspace_packages(&metadata);
    let workspace_manifest =
        parse_manifest(&Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"));
    let workspace_version = workspace_manifest
        .get("workspace")
        .and_then(TomlValue::as_table)
        .and_then(|workspace| workspace.get("package"))
        .and_then(TomlValue::as_table)
        .and_then(|package| package.get("version"))
        .and_then(TomlValue::as_str)
        .expect("workspace.package.version is a string");
    let exact_requirement = format!("={workspace_version}");
    let workspace_names = workspace_packages
        .iter()
        .filter_map(|package| package["name"].as_str())
        .collect::<BTreeSet<_>>();

    for package in &workspace_packages {
        let package_name = package["name"].as_str().expect("package name is a string");
        assert_eq!(
            package["version"].as_str(),
            Some(workspace_version),
            "workspace member {package_name} must resolve to workspace version {workspace_version}"
        );

        let manifest_path = package["manifest_path"]
            .as_str()
            .expect("manifest path is a string");
        let manifest = parse_manifest(Path::new(manifest_path));
        let inherits_workspace_version = manifest
            .get("package")
            .and_then(TomlValue::as_table)
            .and_then(|package| package.get("version"))
            .and_then(TomlValue::as_table)
            .and_then(|version| version.get("workspace"))
            .and_then(TomlValue::as_bool);
        assert_eq!(
            inherits_workspace_version,
            Some(true),
            "workspace member {package_name} must declare `version.workspace = true`"
        );

        for dependency in dependencies(package) {
            let dependency_name = dependency["name"]
                .as_str()
                .expect("dependency name is a string");
            if workspace_names.contains(dependency_name) {
                assert_eq!(
                    dependency["req"].as_str(),
                    Some(exact_requirement.as_str()),
                    "internal dependency {package_name} -> {dependency_name} must require exactly {workspace_version}"
                );
            }
        }
    }

    let workspace_dependencies = workspace_manifest
        .get("workspace")
        .and_then(TomlValue::as_table)
        .and_then(|workspace| workspace.get("dependencies"))
        .and_then(TomlValue::as_table)
        .expect("workspace.dependencies is a table");
    for (dependency_key, specification) in workspace_dependencies {
        let package_name = specification
            .as_table()
            .and_then(|table| table.get("package"))
            .and_then(TomlValue::as_str)
            .unwrap_or(dependency_key);
        if workspace_names.contains(package_name) {
            assert_eq!(
                dependency_requirement(specification),
                Some(exact_requirement.as_str()),
                "workspace dependency {dependency_key} must require exactly {workspace_version}"
            );
        }
    }
}

fn cargo_metadata() -> JsonValue {
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

fn workspace_packages(metadata: &JsonValue) -> Vec<&JsonValue> {
    let workspace_member_ids = metadata["workspace_members"]
        .as_array()
        .expect("workspace member ids")
        .iter()
        .filter_map(JsonValue::as_str)
        .collect::<BTreeSet<_>>();
    metadata["packages"]
        .as_array()
        .expect("metadata packages")
        .iter()
        .filter(|package| {
            package["id"]
                .as_str()
                .is_some_and(|id| workspace_member_ids.contains(id))
        })
        .collect()
}

fn dependencies(package: &JsonValue) -> &[JsonValue] {
    package["dependencies"]
        .as_array()
        .expect("package dependencies")
}

fn dependency_requirement(specification: &TomlValue) -> Option<&str> {
    specification.as_str().or_else(|| {
        specification
            .as_table()
            .and_then(|table| table.get("version"))
            .and_then(TomlValue::as_str)
    })
}

fn parse_manifest(path: &Path) -> TomlValue {
    let source = std::fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    toml::from_str(&source).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}
