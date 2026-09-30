// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::{
    fs, io,
    path::{Path, PathBuf},
};

/// Cargo may replace its own caller while building the workspace inventory.
/// A private copy owns the image, not merely the mutable build pathname. Its
/// directory outlives every delegated child and is removed only after joining.
pub(crate) struct Launcher {
    _directory: tempfile::TempDir,
    executable: PathBuf,
}

impl Launcher {
    pub(crate) fn retain(directory: &Path) -> io::Result<Self> {
        // Read through the Linux proc link, not its possibly " (deleted)" target.
        #[cfg(target_os = "linux")]
        let image = PathBuf::from("/proc/self/exe");
        #[cfg(not(target_os = "linux"))]
        let image = std::env::current_exe()?;
        Self::copy(&image, directory)
    }

    pub(crate) fn copy(image: &Path, directory: &Path) -> io::Result<Self> {
        let directory = tempfile::Builder::new()
            .prefix("launcher-")
            .tempdir_in(directory)?;
        let executable = directory.path().join("xtask");
        fs::copy(image, &executable)?;
        Ok(Self {
            _directory: directory,
            executable,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.executable
    }
}
