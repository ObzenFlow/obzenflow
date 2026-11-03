// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::ordered_stage_journal::read_stage_envelopes;
use obzenflow_core::event::ChainPayload;
use std::path::Path;

/// Count reader-telemetry flow-control rows in a stage's journal
/// (FLOWIP-095d). Witness tests use this to prove a run actually crossed the
/// contract tick and emitted telemetry mid-stream, so a determinism assertion
/// downstream of the telemetry filter is exercising the condition it pins.
pub async fn count_reader_telemetry_rows(run_dir: &Path, stage_key: &str) -> usize {
    read_stage_envelopes(run_dir, stage_key)
        .await
        .iter()
        .filter(|envelope| {
            matches!(
                &envelope.payload,
                ChainPayload::FlowControl(payload)
                    if payload.is_reporting_telemetry()
            )
        })
        .count()
}
