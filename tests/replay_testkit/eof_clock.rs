// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::stage_journal::read_stage_envelopes_appended;
use super::stage_writer::stage_writer;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::ChainPayload;
use obzenflow_core::WriterId;
use std::path::Path;

pub async fn assert_eof_carries_the_ending_clock(run_dir: &Path, stage_key: &str) {
    let stage = stage_writer(run_dir, stage_key);
    let eofs: Vec<(WriterId, Option<VectorClock>)> =
        read_stage_envelopes_appended(run_dir, stage_key)
            .await
            .iter()
            .filter_map(|record| match &record.payload {
                ChainPayload::FlowControl(FlowControlPayload::Eof {
                    writer_id,
                    vector_clock,
                    ..
                }) => Some((
                    writer_id.unwrap_or(record.envelope.provenance.event.writer_id),
                    vector_clock.clone(),
                )),
                _ => None,
            })
            .collect();
    let own = eofs
        .iter()
        .position(|(writer, _)| *writer == stage)
        .unwrap_or_else(|| panic!("'{stage_key}' authors its own EOF: {eofs:?}"));
    let ending = own
        .checked_sub(1)
        .map(|index| &eofs[index])
        .unwrap_or_else(|| panic!("an input EOF ended the reading of '{stage_key}': {eofs:?}"));
    assert_eq!(
        eofs[own].1, ending.1,
        "the EOF of '{stage_key}' carries the clock of the EOF that ended its reading"
    );
}
