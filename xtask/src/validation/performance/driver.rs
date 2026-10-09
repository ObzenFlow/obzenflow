// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Install an identified outer benchmark driver against the preserved reference.
//! No framework source, private adapter or production feature is changed.

use crate::{error, Result};
use ring::digest::{digest, SHA256};
use serde_json::json;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

const LEGACY_REFERENCE: &str = "854c04bab261477ad4bec813369244e84f2ffb20";

pub(super) fn install(
    root: &Path,
    baseline: &Path,
    artifacts: &Path,
    revision: &str,
) -> Result<()> {
    let mut applied = BTreeMap::new();
    let crate_path = Path::new("crates/obzenflow_benchmarks");
    // Copy an entire, coherent outer crate. Source layout and framework internals
    // are not patch points. A future incompatible product API needs an explicitly
    // versioned outer adapter, never a compatibility API inside the framework.
    let mut files = vec![PathBuf::from("Cargo.toml")];
    for directory in ["src", "benches"] {
        files.extend(
            tree(&root.join(crate_path).join(directory))?
                .into_keys()
                .map(|path| Path::new(directory).join(path)),
        );
    }
    for relative in files {
        let source = root.join(crate_path).join(&relative);
        if source.is_symlink() {
            return Err(error("measurement driver must not contain symlinks"));
        }
        let path = crate_path.join(relative);
        write(
            baseline,
            path.to_str().unwrap(),
            &fs::read(source)?,
            &mut applied,
        )?;
    }
    // A versioned outer adapter bridges this known public constructor change.
    // Payloads and workloads stay the same; the preserved framework is untouched.
    let adapter = (revision == LEGACY_REFERENCE).then_some("src/support/reference_854c04");
    if let Some(adapter) = adapter {
        for name in ["data_event.rs", "validation_sink.rs"] {
            write(
                baseline,
                &format!("crates/obzenflow_benchmarks/src/support/{name}"),
                &fs::read(root.join(crate_path).join(adapter).join(name))?,
                &mut applied,
            )?;
        }
    }
    // The driver owns its dependency declaration. Synchronise only its package's
    // lock entry, preserving the reference's resolved production dependencies.
    let candidate: toml::Value = toml::from_str(&fs::read_to_string(root.join("Cargo.lock"))?)?;
    let package = candidate["package"]
        .as_array()
        .and_then(|packages| {
            packages
                .iter()
                .find(|p| p["name"].as_str() == Some("obzenflow_benchmarks"))
        })
        .ok_or_else(|| error("candidate benchmark lock entry unavailable"))?;
    let mut reference: toml::Value =
        toml::from_str(&fs::read_to_string(baseline.join("Cargo.lock"))?)?;
    let destination = reference["package"]
        .as_array_mut()
        .and_then(|packages| {
            packages
                .iter_mut()
                .find(|p| p["name"].as_str() == Some("obzenflow_benchmarks"))
        })
        .ok_or_else(|| error("reference benchmark lock entry unavailable"))?;
    // The copied outer crate inherits the reference workspace's version. A
    // candidate release bump must not replace that resolved package identity.
    let reference_version = destination["version"].clone();
    *destination = package.clone();
    destination["version"] = reference_version;
    write(
        baseline,
        "Cargo.lock",
        toml::to_string(&reference)?.as_bytes(),
        &mut applied,
    )?;
    fs::write(
        artifacts.join("measurement-driver.json"),
        serde_json::to_vec_pretty(&json!({
            "measurement_contract": super::MEASUREMENT_CONTRACT,
            "purpose": "identical public-operation benchmark driver for reference and candidate",
            "production_source_replaced": false,
            "framework_files_modified": [],
            "reference_public_api_adapter": adapter,
            "applied_files_sha256": applied,
        }))?,
    )?;
    Ok(())
}

/// Cargo can consider equal-named packages fresh after another workspace has
/// replaced their shared output. Keep a content-identified reference source
/// and target tree separate from the candidate. Stable paths preserve warm
/// builds; verify every file before reusing this development cache.
pub(super) fn reference(
    root: &Path,
    extracted: &Path,
    artifacts: &Path,
) -> Result<(PathBuf, PathBuf)> {
    let expected = tree(extracted)?;
    let identity = sha256(&serde_json::to_vec(&expected)?);
    let cache = root.join("target/validation-reference").join(&identity);
    let source = cache.join("source");
    let target = cache.join("target");
    fs::create_dir_all(&cache)?;
    if source.exists() {
        if tree(&source)? != expected {
            return Err(error(format!(
                "preserved reference source changed: {}; cannot reuse its build",
                source.display()
            )));
        }
    } else {
        fs::rename(extracted, &source)?;
    }
    fs::write(
        artifacts.join("reference-build.json"),
        serde_json::to_vec_pretty(&json!({
            "source_tree_sha256": identity, "source": source, "target": target,
            "files_verified": expected.len(),
        }))?,
    )?;
    Ok((source, target))
}

fn tree(root: &Path) -> Result<BTreeMap<String, (String, u32)>> {
    fn visit(
        root: &Path,
        directory: &Path,
        files: &mut BTreeMap<String, (String, u32)>,
    ) -> Result<()> {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            let kind = entry.file_type()?;
            if kind.is_dir() {
                visit(root, &path, files)?;
                continue;
            }
            let relative = path
                .strip_prefix(root)?
                .to_str()
                .ok_or_else(|| error("reference filename is not UTF-8"))?
                .to_owned();
            let value = if kind.is_symlink() {
                (
                    format!(
                        "symlink:{}",
                        fs::read_link(&path)?
                            .to_str()
                            .ok_or_else(|| error("reference symlink is not UTF-8"))?
                    ),
                    0,
                )
            } else if kind.is_file() {
                #[cfg(unix)]
                let mode = {
                    use std::os::unix::fs::PermissionsExt;
                    entry.metadata()?.permissions().mode() & 0o111
                };
                #[cfg(not(unix))]
                let mode = 0;
                (sha256(&fs::read(&path)?), mode)
            } else {
                return Err(error("unsupported reference file type"));
            };
            files.insert(relative, value);
        }
        Ok(())
    }
    let mut files = BTreeMap::new();
    visit(root, root, &mut files)?;
    Ok(files)
}

pub(super) fn sha256(bytes: &[u8]) -> String {
    digest(&SHA256, bytes)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn write(
    root: &Path,
    path: &str,
    bytes: &[u8],
    applied: &mut BTreeMap<String, String>,
) -> Result<()> {
    let target = root.join(path);
    fs::create_dir_all(target.parent().unwrap())?;
    fs::write(target, bytes)?;
    applied.insert(path.into(), sha256(bytes));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intact_reference_cache_is_reusable_but_pruned_source_is_rejected() {
        let workspace = tempfile::tempdir().unwrap();
        let artifacts = tempfile::tempdir().unwrap();
        let extracted = workspace.path().join("extracted");
        fs::create_dir(&extracted).unwrap();
        fs::write(extracted.join("source.rs"), "reference").unwrap();
        let (source, target) = reference(workspace.path(), &extracted, artifacts.path()).unwrap();
        fs::create_dir(&extracted).unwrap();
        fs::write(extracted.join("source.rs"), "reference").unwrap();
        assert_eq!(
            reference(workspace.path(), &extracted, artifacts.path()).unwrap(),
            (source.clone(), target)
        );
        fs::remove_file(source.join("source.rs")).unwrap();
        assert!(reference(workspace.path(), &extracted, artifacts.path())
            .unwrap_err()
            .to_string()
            .contains("preserved reference source changed"));
    }

    #[test]
    fn public_driver_changes_only_its_outer_crate_and_lock_entry() {
        let candidate = tempfile::tempdir().unwrap();
        let reference = tempfile::tempdir().unwrap();
        let artifacts = tempfile::tempdir().unwrap();
        for root in [candidate.path(), reference.path()] {
            fs::create_dir_all(root.join("crates/obzenflow_benchmarks/src")).unwrap();
            fs::create_dir_all(root.join("crates/obzenflow_benchmarks/benches")).unwrap();
            fs::create_dir_all(root.join("crates/obzenflow_core/src")).unwrap();
            fs::write(
                root.join("crates/obzenflow_core/src/lib.rs"),
                "private implementation",
            )
            .unwrap();
            fs::write(
                root.join("crates/obzenflow_benchmarks/Cargo.toml"),
                "[package]\nname = 'obzenflow_benchmarks'\n",
            )
            .unwrap();
        }
        fs::write(candidate.path().join("Cargo.lock"), "version = 4\n[[package]]\nname = 'obzenflow_benchmarks'\nversion = '2.0.0'\ndependencies = ['reqwest']\n[[package]]\nname = 'production'\nversion = '2.0.0'\n").unwrap();
        fs::write(reference.path().join("Cargo.lock"), "version = 4\n[[package]]\nname = 'obzenflow_benchmarks'\nversion = '1.0.0'\n[[package]]\nname = 'production'\nversion = '1.0.0'\n").unwrap();
        // Neither source-layout marker required by the former driver is present.
        fs::write(
            candidate
                .path()
                .join("crates/obzenflow_benchmarks/src/lib.rs"),
            "pub fn public_driver() {}",
        )
        .unwrap();
        fs::write(
            candidate
                .path()
                .join("crates/obzenflow_benchmarks/benches/operation.rs"),
            "fn main() {}",
        )
        .unwrap();
        let before = tree(reference.path()).unwrap();
        let adapter = candidate
            .path()
            .join("crates/obzenflow_benchmarks/src/support/reference_854c04/data_event.rs");
        fs::create_dir_all(adapter.parent().unwrap()).unwrap();
        fs::write(&adapter, "versioned public constructor adapter").unwrap();
        fs::write(
            adapter.with_file_name("validation_sink.rs"),
            "versioned public sink adapter",
        )
        .unwrap();
        install(
            candidate.path(),
            reference.path(),
            artifacts.path(),
            LEGACY_REFERENCE,
        )
        .unwrap();
        let after = tree(reference.path()).unwrap();
        for (path, identity) in &after {
            if before.get(path) != Some(identity) {
                assert!(path == "Cargo.lock" || path.starts_with("crates/obzenflow_benchmarks/"));
            }
        }
        assert_eq!(
            fs::read(reference.path().join("crates/obzenflow_core/src/lib.rs")).unwrap(),
            b"private implementation"
        );
        let lock: toml::Value =
            toml::from_str(&fs::read_to_string(reference.path().join("Cargo.lock")).unwrap())
                .unwrap();
        assert_eq!(lock["package"][1]["version"].as_str(), Some("1.0.0"));
        assert_eq!(lock["package"][0]["version"].as_str(), Some("1.0.0"));
        assert_eq!(
            lock["package"][0]["dependencies"][0].as_str(),
            Some("reqwest")
        );
        let record: serde_json::Value = serde_json::from_slice(
            &fs::read(artifacts.path().join("measurement-driver.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(record["framework_files_modified"], json!([]));
        assert_eq!(
            record["reference_public_api_adapter"],
            "src/support/reference_854c04"
        );
        assert_eq!(
            fs::read_to_string(
                reference
                    .path()
                    .join("crates/obzenflow_benchmarks/src/support/data_event.rs")
            )
            .unwrap(),
            "versioned public constructor adapter"
        );
        assert_eq!(
            record["measurement_contract"],
            super::super::MEASUREMENT_CONTRACT
        );
    }
}
