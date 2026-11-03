// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_infra::testing::journal;
use std::path::Path;

/// Rewrite only optional attachments in a fixed, completed current-schema
/// archive. The physical frames, full provenance, payloads and member order
/// remain the oracle; only length/checksum framing is recomputed.
pub fn omit_observations(run: &Path, keep: impl Fn(usize) -> bool) -> usize {
    journal::omit_observations(run, keep).expect("rewrite current-format fixture")
}
