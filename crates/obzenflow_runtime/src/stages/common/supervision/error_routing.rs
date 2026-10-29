// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Error journal routing logic
//!
//! Every error-marked row a stage writes goes to its error journal, never its
//! data journal.

use crate::stages::common::handler_error::HandlerError;
use obzenflow_core::config::LineagePolicy;
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::payloads::stage_processing_failed_payload::{
    FailedInput, StageProcessingFailed,
};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{ChainEvent, MiddlewareExecutionScope, WriterId};

pub(crate) fn error_row_in_scope(
    mut event: ChainEvent,
    scope: MiddlewareExecutionScope,
) -> ChainEvent {
    if matches!(event.processing.status, ProcessingStatus::Error { .. })
        && !scope.is_deterministic_replay()
    {
        event.replay_context = None;
    }
    event
}

/// Returns `true` if `event` belongs in the stage's error journal.
pub(crate) fn route_to_error_journal(event: &ChainEvent) -> bool {
    matches!(event.processing.status, ProcessingStatus::Error { .. })
}

pub(crate) fn processing_failed(
    writer_id: WriterId,
    flow_context: &FlowContext,
    input: &ChainEvent,
    error: &HandlerError,
    lineage: LineagePolicy,
) -> ChainEvent {
    processing_failure_record(
        writer_id,
        flow_context,
        input,
        error.kind(),
        error.message(),
        lineage,
    )
}

pub(crate) fn processing_failure_record(
    writer_id: WriterId,
    flow_context: &FlowContext,
    input: &ChainEvent,
    kind: ErrorKind,
    detail: String,
    lineage: LineagePolicy,
) -> ChainEvent {
    let record = StageProcessingFailed {
        kind: kind.clone(),
        detail: detail.clone(),
        input: FailedInput::of(input),
    };
    ChainEventFactory::derived_execution_event(
        writer_id,
        input,
        ExecutionPayload::StageProcessingFailed(record),
        lineage,
    )
    .with_flow_context(flow_context.clone())
    .mark_as_error(detail, kind)
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::provenance::ReplayContext;
    use obzenflow_core::event::ChainPayload;
    use obzenflow_core::{EventId, StageId};
    use std::num::NonZeroU32;

    fn recorded_input() -> ChainEvent {
        let mut input = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.input",
            NonZeroU32::MIN,
            serde_json::json!({ "value": 5 }),
        );
        input.replay_context = Some(ReplayContext {
            original_event_id: EventId::new(),
            original_flow_id: "recorded-flow".into(),
            original_stage_id: StageId::new(),
        });
        input
    }

    #[test]
    fn a_live_error_row_drops_its_inputs_replay_provenance() {
        let error = || recorded_input().mark_as_error("failed", ErrorKind::PermanentFailure);
        assert!(
            error_row_in_scope(error(), MiddlewareExecutionScope::LiveHandler)
                .replay_context
                .is_none()
        );
        for reconstructing in [
            MiddlewareExecutionScope::ResumeHandler,
            MiddlewareExecutionScope::StrictReplayHandler,
        ] {
            assert!(
                error_row_in_scope(error(), reconstructing)
                    .replay_context
                    .is_some(),
                "{reconstructing:?}"
            );
        }
        assert!(
            error_row_in_scope(recorded_input(), MiddlewareExecutionScope::LiveHandler)
                .replay_context
                .is_some(),
            "only error rows"
        );
    }

    #[test]
    fn a_failed_input_is_recorded_by_the_stage_with_its_error() {
        let input = recorded_input();
        let stage = WriterId::from(StageId::new());
        let error = HandlerError::Validation("order 2 has no customer".into());
        let flow_context = FlowContext {
            stage_name: "check".into(),
            ..FlowContext::default()
        };
        let record = processing_failed(
            stage,
            &flow_context,
            &input,
            &error,
            LineagePolicy::default(),
        );

        assert_eq!(record.writer_id, stage);
        assert_eq!(record.flow_context.stage_name, "check");
        assert_eq!(record.event_type(), "runtime.stage.processing_failed");
        assert_eq!(record.causality.parent_ids, vec![input.id]);
        let ChainPayload::Execution(ExecutionPayload::StageProcessingFailed(failure)) =
            &record.payload
        else {
            panic!("expected a processing failure, got {:?}", record.payload);
        };
        assert_eq!(failure.kind, ErrorKind::Validation);
        assert_eq!(failure.detail, "order 2 has no customer");
        assert_eq!(failure.input.event_type.0, "test.input");
        assert_eq!(failure.input.payload, serde_json::json!({ "value": 5 }));
        assert!(matches!(
            &record.processing.status,
            ProcessingStatus::Error { message, kind: Some(ErrorKind::Validation) }
                if message == "order 2 has no customer"
        ));
        assert!(route_to_error_journal(&record));
    }
}
