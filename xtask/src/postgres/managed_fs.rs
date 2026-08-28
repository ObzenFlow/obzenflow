// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Structural checks for xtask-owned PostgreSQL state and secret artefacts.
//!
//! Effective filesystem access remains the host's responsibility. The Unix mode
//! handling here preserves the workflow's existing private-file and pgpass
//! requirements; it is not a cross-platform permission policy.

use crate::{error, Result};
use std::{
    fs::{self, File, OpenOptions},
    path::Path,
};

pub(super) fn create_directory(path: &Path) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    require_directory(path, "managed PostgreSQL directory")
}

pub(super) fn secret_file_create_new(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}

pub(super) fn set_secret_file_permissions(path: &Path, description: &str) -> Result<()> {
    require_regular_file(path, description)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    require_secret_file(path, description)
}

pub(super) fn require_directory(path: &Path, description: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|failure| error(format!("{description} is unavailable: {failure}")))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(error(format!("{description} is not a regular directory")));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o777 != 0o700 {
            return Err(error(format!("{description} permissions must be 0700")));
        }
    }
    Ok(())
}

pub(super) fn require_secret_file(path: &Path, description: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|failure| error(format!("{description} is unavailable: {failure}")))?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(error(format!("{description} is not a regular file")));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o777 != 0o600 {
            return Err(error(format!("{description} permissions must be 0600")));
        }
    }
    Ok(())
}

pub(super) fn require_regular_file(path: &Path, description: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|failure| error(format!("{description} is unavailable: {failure}")))?;
    if metadata.is_file() && !metadata.file_type().is_symlink() {
        Ok(())
    } else {
        Err(error(format!("{description} is not a regular file")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{env, io::Write, path::PathBuf};

    fn fixture_root(label: &str) -> PathBuf {
        let root = env::temp_dir().join(format!(
            "obzenflow-managed-fs-{label}-{}",
            super::super::state::unique_run_id()
        ));
        fs::create_dir_all(&root).expect("create managed filesystem fixture root");
        root
    }

    #[test]
    fn managed_secret_nodes_are_created_and_verified() {
        let root = fixture_root("create");
        let directory = root.join("session");
        create_directory(&directory).expect("create managed directory");
        let file_path = directory.join("secret");
        let mut file = secret_file_create_new(&file_path).expect("create managed secret file");
        file.write_all(b"secret")
            .expect("write managed secret file");
        file.sync_all().expect("sync managed secret file");

        require_directory(&directory, "fixture directory").expect("verify managed directory");
        require_secret_file(&file_path, "fixture file").expect("verify managed secret file");
        fs::remove_dir_all(root).expect("remove managed filesystem fixture");
    }

    #[cfg(unix)]
    #[test]
    fn weakened_modes_and_symlinks_fail_closed() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let root = fixture_root("fail-closed");
        let directory = root.join("session");
        create_directory(&directory).expect("create managed directory");
        let file_path = directory.join("secret");
        drop(secret_file_create_new(&file_path).expect("create managed secret file"));

        fs::set_permissions(&file_path, fs::Permissions::from_mode(0o640))
            .expect("weaken private file");
        assert!(require_secret_file(&file_path, "fixture file").is_err());
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o750))
            .expect("weaken private directory");
        assert!(require_directory(&directory, "fixture directory").is_err());

        let link = root.join("linked-session");
        symlink(&directory, &link).expect("create directory symlink");
        assert!(require_directory(&link, "fixture link").is_err());
        fs::remove_dir_all(root).expect("remove managed filesystem fixture");
    }
}
