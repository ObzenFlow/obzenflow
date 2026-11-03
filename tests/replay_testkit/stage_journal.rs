// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::journal::read_journal_envelopes_appended;
use super::manifest::archive_manifest;
use obzenflow_core::event::{ChainEvent, ChainPayload, JournalRecord};
use std::path::Path;

/// Read a stage's data journal in physical append order, the order a
/// downstream subscription's sequential reader delivers.
///
/// `read_stage_envelopes` orders by event id, which matches append order for
/// a stage's own outputs (a single writer mints ids in sequence) but NOT for
/// forwarded control rows, which retain their original upstream ids. Any
/// position-sensitive comparison involving control rows must use this reader.
pub async fn read_stage_envelopes_appended(
    run_dir: &Path,
    stage_key: &str,
) -> Vec<JournalRecord<ChainPayload>> {
    let manifest = archive_manifest(run_dir);
    let stage_journal = manifest["stages"][stage_key]["data_journal_file"]
        .as_str()
        .unwrap_or_else(|| panic!("manifest should contain data journal for '{stage_key}'"));
    read_journal_envelopes_appended::<ChainEvent>(&run_dir.join(stage_journal)).await
}
