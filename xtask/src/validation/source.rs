// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::{error, Result};
use ring::digest::{Context, SHA256};
use serde::Serialize;
use std::{fs, io::Read, path::Path, process::Command};

#[derive(Debug, PartialEq, Eq, Serialize)]
pub(super) struct SourceIdentity {
    pub(super) commit: String,
    pub(super) content_sha256: String,
    pub(super) files: usize,
}

pub(super) fn identity(root: &Path) -> Result<SourceIdentity> {
    let head = Command::new("git")
        .current_dir(root)
        .args(["rev-parse", "HEAD"])
        .output()?;
    let files = Command::new("git")
        .current_dir(root)
        .args([
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
        ])
        .output()?;
    if !head.status.success() || !files.status.success() {
        return Err(error("could not establish checkout source identity"));
    }
    let mut names: Vec<_> = files
        .stdout
        .split(|b| *b == 0)
        .filter(|name| !name.is_empty())
        .collect();
    names.sort();
    names.dedup();
    let mut digest = Context::new(&SHA256);
    digest.update(b"obzenflow-validation-source-v1\0");
    for name in &names {
        let name = std::str::from_utf8(name)?;
        digest.update(&(name.len() as u64).to_le_bytes());
        digest.update(name.as_bytes());
        let path = root.join(name);
        match fs::symlink_metadata(&path) {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => digest.update(b"deleted\0"),
            Err(err) => return Err(err.into()),
            Ok(meta) if meta.file_type().is_symlink() => {
                digest.update(b"symlink\0");
                let target = fs::read_link(path)?;
                let bytes = target.as_os_str().as_encoded_bytes();
                digest.update(&(bytes.len() as u64).to_le_bytes());
                digest.update(bytes);
            }
            Ok(meta) if meta.is_file() => {
                digest.update(b"file\0");
                digest.update(&meta.len().to_le_bytes());
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    digest.update(&(meta.permissions().mode() & 0o111).to_le_bytes());
                }
                let mut file = fs::File::open(path)?;
                let mut buffer = [0; 64 * 1024];
                loop {
                    let n = file.read(&mut buffer)?;
                    if n == 0 {
                        break;
                    }
                    digest.update(&buffer[..n]);
                }
            }
            Ok(_) => return Err(error(format!("unsupported source entry: {name}"))),
        }
    }
    Ok(SourceIdentity {
        commit: String::from_utf8(head.stdout)?.trim().into(),
        content_sha256: digest
            .finish()
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect(),
        files: names.len(),
    })
}
