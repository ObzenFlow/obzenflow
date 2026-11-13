// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod error;
mod workspace;

pub(crate) use error::{error, Result};
pub(crate) use workspace::workspace_root;

pub(crate) fn is_help(arg: &str) -> bool {
    matches!(arg, "-h" | "--help" | "help")
}
