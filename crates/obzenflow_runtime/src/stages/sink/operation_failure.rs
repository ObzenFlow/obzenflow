// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Correctness-bearing lifecycle operation failure sequence.

use crate::metrics::instrumentation::{snapshot_stage_metrics, StageInstrumentation};
use crate::stages::common::handlers::SinkOperationError;
use obzenflow_core::event::context::{FlowContext, StageType};
use obzenflow_core::event::{
    ChainEventFactory, SinkOperationFailed, SinkOperationPhase, SystemEvent,
};
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, EventEnvelope, EventId, StageId, TypedPayload, WriterId};
use std::sync::Arc;

#[doc(hidden)]
pub struct SinkLifecycleFailureCommit<'a> {
    pub stage_id: StageId,
    pub stage_key: &'a str,
    pub flow_id: &'a str,
    pub flow_name: &'a str,
    pub logical_destination: &'a str,
    pub phase: SinkOperationPhase,
    pub error: &'a SinkOperationError,
    pub error_journal: &'a Arc<dyn Journal<ChainEvent>>,
    pub system_journal: &'a Arc<dyn Journal<SystemEvent>>,
    pub instrumentation: &'a StageInstrumentation,
}

#[doc(hidden)]
pub struct SinkLifecycleFailureRecorded {
    pub operation: EventEnvelope<ChainEvent>,
    pub lifecycle_event_id: EventId,
}

#[doc(hidden)]
pub async fn record_sink_lifecycle_operation_failure(
    commit: SinkLifecycleFailureCommit<'_>,
) -> Result<SinkLifecycleFailureRecorded, Box<dyn std::error::Error + Send + Sync>> {
    debug_assert!(!matches!(commit.phase, SinkOperationPhase::Write(_)));
    let payload = SinkOperationFailed {
        stage_id: commit.stage_id,
        stage_key: commit.stage_key.to_string(),
        logical_destination: commit.logical_destination.to_string(),
        causal_event_id: None,
        input_position: None,
        failed_delivery_event_id: None,
        operation_subject_event_id: commit.error.operation_subject_event_id(),
        phase: commit.phase,
        kind: commit.error.kind(),
        destination_error_code: commit.error.destination_error_code().cloned(),
        detail: commit.error.detail(),
    };
    let event = ChainEventFactory::data_event(
        WriterId::from(commit.stage_id),
        SinkOperationFailed::versioned_event_type(),
        serde_json::to_value(payload)?,
    )
    .with_flow_context(FlowContext {
        flow_name: commit.flow_name.to_string(),
        flow_id: commit.flow_id.to_string(),
        stage_name: commit.stage_key.to_string(),
        stage_id: commit.stage_id,
        stage_type: StageType::Sink,
    })
    .mark_as_error(commit.error.detail(), commit.error.kind())
    .with_runtime_context(commit.instrumentation.snapshot_with_control());
    commit
        .instrumentation
        .record_error_journal_output_event(&event);
    let operation = commit.error_journal.append(event, None).await?;

    let lifecycle = SystemEvent::stage_failed_with_metrics_causal(
        commit.stage_id,
        commit.error.detail(),
        false,
        snapshot_stage_metrics(commit.instrumentation),
        operation.event.id,
    );
    let lifecycle = commit.system_journal.append(lifecycle, None).await?;
    Ok(SinkLifecycleFailureRecorded {
        operation,
        lifecycle_event_id: lifecycle.event.id,
    })
}
