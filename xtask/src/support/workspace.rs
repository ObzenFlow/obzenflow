// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{error, Result};
use std::{env, fs, path::PathBuf};

pub(crate) fn workspace_root() -> Result<PathBuf> {
    let mut dir = env::current_dir()?;
    loop {
        let manifest = dir.join("Cargo.toml");
        if manifest.is_file() {
            let manifest_text = fs::read_to_string(&manifest)?;
            if manifest_text.contains("[workspace]")
                && manifest_text.contains("name = \"obzenflow\"")
            {
                return Ok(dir);
            }
        }
        if !dir.pop() {
            return Err(error("run this xtask from inside the obzenflow workspace"));
        }
    }
}
