// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::stage_journal::read_stage_envelopes_appended;
use obzenflow_core::event::vector_clock::CausalOrderingService;
use obzenflow_core::event::{ChainPayload, JournalRecord};
use std::path::Path;

/// Read a stage's data journal (causally ordered envelopes) through the run
/// manifest.
pub async fn read_stage_envelopes(
    run_dir: &Path,
    stage_key: &str,
) -> Vec<JournalRecord<ChainPayload>> {
    CausalOrderingService::order_envelopes_by_event_id(
        read_stage_envelopes_appended(run_dir, stage_key).await,
    )
    .expect("stage journal should order causally")
}
