// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::journal::read_journal_envelopes_appended;
use super::manifest::archive_manifest;
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::payloads::stage_processing_failed_payload::StageProcessingFailed;
use obzenflow_core::event::{ChainEvent, ChainPayload, JournalRecord};
use std::path::Path;

pub async fn read_stage_error_envelopes(
    run_dir: &Path,
    stage_key: &str,
) -> Vec<JournalRecord<ChainPayload>> {
    let manifest = archive_manifest(run_dir);
    let error_journal = manifest["stages"][stage_key]["error_journal_file"]
        .as_str()
        .unwrap_or_else(|| panic!("manifest should contain error journal for '{stage_key}'"));
    read_journal_envelopes_appended::<ChainEvent>(&run_dir.join(error_journal)).await
}

pub async fn processing_failures(
    run_dir: &Path,
    stage_key: &str,
) -> Vec<(JournalRecord<ChainPayload>, StageProcessingFailed)> {
    read_stage_error_envelopes(run_dir, stage_key)
        .await
        .into_iter()
        .filter_map(|record| match &record.payload {
            ChainPayload::Execution(ExecutionPayload::StageProcessingFailed(failure)) => {
                let failure = failure.clone();
                Some((record, failure))
            }
            _ => None,
        })
        .collect()
}
