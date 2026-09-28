// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Common helper to forward control events downstream.

use obzenflow_core::event::context::StageType;
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::ChainPayload;
use obzenflow_core::journal::AppendOptions;
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, JournalRecord, StageId};
use std::sync::Arc;

pub(crate) async fn forward_control_event(
    envelope: &JournalRecord<ChainPayload>,
    stage_id: StageId,
    stage_name: &str,
    stage_type: StageType,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
) -> Result<JournalRecord<ChainPayload>, Box<dyn std::error::Error + Send + Sync>> {
    // Re-stamp flow and runtime context so metrics remain local to the
    // forwarding stage even when forwarding control events.
    let mut forward_event = envelope.authored();

    let flow_name = forward_event.flow_context.flow_name.clone();
    let flow_id = forward_event.flow_context.flow_id.clone();
    forward_event = forward_event.with_flow_context(FlowContext {
        flow_name,
        flow_id,
        stage_name: stage_name.to_string(),
        stage_id,
        stage_type,
    });

    // RuntimeProvenance will be refreshed by instrumentation when this stage
    // emits observability events; forwarded control events themselves may
    // omit runtime_context to avoid leaking upstream snapshots.
    forward_event.runtime = None;

    let written = crate::supervised_base::publication::append(
        data_journal,
        forward_event,
        AppendOptions::from_record(Some(envelope))?,
    )
    .await
    .map_err(|e| format!("Failed to forward control event: {e}"))?;

    Ok(written)
}
