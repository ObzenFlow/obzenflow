// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Apply the new measurement driver to the preserved production reference.
//! Only benchmark files and a test-support endpoint adapter are added. The
//! reference's production implementation, codecs and lifecycle code stay intact.

use crate::{error, Result};
use ring::digest::{digest, SHA256};
use serde_json::json;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

pub(super) fn install(root: &Path, baseline: &Path, artifacts: &Path) -> Result<()> {
    let mut applied = BTreeMap::new();
    for path in [
        "crates/obzenflow_benchmarks/benches/validation_boundaries.rs",
        "crates/obzenflow_benchmarks/src/support/validation.rs",
        "crates/obzenflow_infra/src/testing/studio.rs",
    ] {
        write(baseline, path, &fs::read(root.join(path))?, &mut applied)?;
    }
    let support = fs::read_to_string(root.join("crates/obzenflow_benchmarks/src/support/mod.rs"))?;
    let (_, runtime) = support
        .split_once("pub const DEADLINE")
        .ok_or_else(|| error("shared benchmark runtime boundary unavailable"))?;
    write(baseline, "crates/obzenflow_benchmarks/src/support/mod.rs",
        format!("#[cfg(feature = \"validation-benchmarks\")]\npub mod validation;\npub const DEADLINE{runtime}").as_bytes(), &mut applied)?;
    for (path, declaration) in [
        (
            "crates/obzenflow_benchmarks/src/lib.rs",
            "\n#[cfg(feature = \"components\")]\npub mod support;\n",
        ),
        (
            "crates/obzenflow_infra/src/testing/mod.rs",
            "\n#[cfg(feature = \"warp-server\")]\npub mod studio;\n",
        ),
    ] {
        let mut contents = fs::read_to_string(baseline.join(path))?;
        contents.push_str(declaration);
        write(baseline, path, contents.as_bytes(), &mut applied)?;
    }
    // The original workload is retained. Add only the same build-identity
    // metadata emitted by the candidate, outside every measured operation.
    let path = "crates/obzenflow_benchmarks/benches/journal_hot_path/main.rs";
    let contents = fs::read_to_string(baseline.join(path))?;
    let original = "serde_json::to_vec_pretty(&censuses)";
    if contents.matches(original).count() != 1 {
        return Err(error("reference census output boundary changed"));
    }
    let contents = contents.replace(original, "serde_json::to_vec_pretty(&serde_json::json!({\"compiled_manifest_dir\": env!(\"CARGO_MANIFEST_DIR\"), \"cases\": censuses}))");
    write(baseline, path, contents.as_bytes(), &mut applied)?;
    let path = "crates/obzenflow_benchmarks/Cargo.toml";
    let mut manifest: toml::Value = toml::from_str(&fs::read_to_string(baseline.join(path))?)?;
    let futures = manifest["dev-dependencies"]
        .as_table_mut()
        .ok_or_else(|| error("reference benchmark manifest has no dev-dependencies"))?
        .remove("futures")
        .ok_or_else(|| error("reference benchmark manifest has no futures dependency"))?;
    manifest["dependencies"]
        .as_table_mut()
        .unwrap()
        .insert("futures".into(), futures);
    manifest["features"].as_table_mut().unwrap().insert(
        "validation-benchmarks".into(),
        toml::Value::Array(vec![
            "components".into(),
            "obzenflow_infra/warp-server".into(),
        ]),
    );
    let target: toml::Value = toml::from_str("name = 'validation_boundaries'\npath = 'benches/validation_boundaries.rs'\nharness = false\nrequired-features = ['validation-benchmarks']\n")?;
    manifest["bench"].as_array_mut().unwrap().push(target);
    write(
        baseline,
        path,
        toml::to_string(&manifest)?.as_bytes(),
        &mut applied,
    )?;
    fs::write(
        artifacts.join("measurement-driver.json"),
        serde_json::to_vec_pretty(&json!({
            "purpose": "identical new measurement driver against the pinned production reference and candidate",
            "production_source_replaced": false,
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
