// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Common helper to forward control events downstream.

use obzenflow_core::event::context::{FlowContext, StageType};
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, EventEnvelope, StageId};
use std::sync::Arc;

pub(crate) async fn forward_control_event(
    envelope: &EventEnvelope<ChainEvent>,
    stage_id: StageId,
    stage_name: &str,
    stage_type: StageType,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
) -> Result<EventEnvelope<ChainEvent>, Box<dyn std::error::Error + Send + Sync>> {
    // Re-stamp flow and runtime context so metrics remain local to the
    // forwarding stage even when forwarding control events.
    let mut forward_event = envelope.event.clone();

    let flow_name = forward_event.flow_context.flow_name.clone();
    let flow_id = forward_event.flow_context.flow_id.clone();
    forward_event = forward_event.with_flow_context(FlowContext {
        flow_name,
        flow_id,
        stage_name: stage_name.to_string(),
        stage_id,
        stage_type,
    });

    // RuntimeContext will be refreshed by instrumentation when this stage
    // emits observability events; forwarded control events themselves may
    // omit runtime_context to avoid leaking upstream snapshots.
    forward_event.runtime_context = None;

    let written = data_journal
        .append(forward_event, Some(envelope))
        .await
        .map_err(|e| format!("Failed to forward control event: {e}"))?;

    Ok(written)
}
