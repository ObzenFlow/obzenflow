// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{plan::Policy, process};
use crate::Result;
use std::path::Path;

// Offline metadata needs the locked dependencies for all platforms, not only
// the dependencies compiled on this host. Keep Cargo's network configuration.
// Other coordinators share the command but retain their own process ownership.
pub(crate) const FETCH_ARGS: &[&str] = &["fetch", "--locked"];

/// Prepare dependency inputs before timed correctness execution. This does not
/// compile tests, and a restored cache can only avoid redundant downloads.
pub(super) fn prepare(root: &Path, policy: &Policy, directory: &Path) -> Result<()> {
    process::capture(
        root,
        policy,
        "cargo",
        FETCH_ARGS,
        directory,
        "dependency-preparation",
    )
    .map(|_| ())
}
