// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Error journal routing logic
//!
//! Determines whether an error-marked event should be routed to the stage's
//! error journal (isolation from downstream transport contracts) or kept in
//! the normal output pipeline.

use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::{ChainEvent, MiddlewareExecutionScope};

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

/// Returns `true` if `event` should be written to the error journal instead of
/// the stage's data journal.
///
/// Infrastructure errors (Timeout, Remote, RateLimited, PermanentFailure,
/// Deserialization, Unknown) go to the error journal. Business errors
/// (Validation, Domain) stay in the main pipeline so downstream stages can
/// observe and potentially handle them.
pub(crate) fn route_to_error_journal(event: &ChainEvent) -> bool {
    match &event.processing.status {
        ProcessingStatus::Error { kind, .. } => match kind {
            Some(ErrorKind::Timeout)
            | Some(ErrorKind::Remote)
            | Some(ErrorKind::RateLimited)
            | Some(ErrorKind::PermanentFailure)
            | Some(ErrorKind::Deserialization) => true,
            Some(ErrorKind::Validation) | Some(ErrorKind::Domain) => false,
            None | Some(ErrorKind::Unknown) => true,
        },
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::provenance::ReplayContext;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{EventId, StageId, WriterId};
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
}
