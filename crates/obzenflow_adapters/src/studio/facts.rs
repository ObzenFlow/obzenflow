// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Converts system journal entries into the message types in `messages.rs`.
//! `stages.rs` also uses `stage_message` to build snapshots with the same JSON.

use super::messages::{ContractEdge, MetricsUpdate, Observation, StudioMessage};
use super::{middleware::MiddlewareView, ContractBoundaryAliases};
use obzenflow_core::event::{MetricsCoordinationEvent, SupervisorRecord, SystemPayload};
use obzenflow_core::web::SseFrame;

pub(super) fn stage_message(envelope: &SupervisorRecord) -> Option<StudioMessage<'_>> {
    let SystemPayload::StageLifecycle { stage_id, event } = &envelope.payload else {
        return None;
    };
    Some(StudioMessage::StageLifecycle {
        stage_id: *stage_id,
        event,
        at: observation(envelope),
    })
}

pub(super) fn frame(
    envelope: &SupervisorRecord,
    middleware: &MiddlewareView,
    aliases: &ContractBoundaryAliases,
) -> Option<SseFrame> {
    let at = observation(envelope);
    let message = match &envelope.payload {
        SystemPayload::SupervisorRegistered { descriptor } => StudioMessage::SupervisorRegistered {
            writer_id: envelope.writer_id(),
            descriptor,
            at,
        },
        SystemPayload::StageLifecycle { .. } => stage_message(envelope)?,
        SystemPayload::PipelineLifecycle(event) => StudioMessage::FlowLifecycle { event, at },
        SystemPayload::ReplayLifecycle(event) => StudioMessage::ReplayLifecycle {
            stage_id: envelope.writer_id().as_stage().map(|id| id.to_string()),
            event,
            at,
        },
        SystemPayload::SupervisorCommandDiscarded {
            supervisor,
            terminal_state,
            command,
            disposition,
            error,
        } => StudioMessage::SupervisorCommandDiscarded {
            stage_id: envelope.writer_id().as_stage().map(|id| id.to_string()),
            supervisor,
            terminal_state,
            command,
            disposition: *disposition,
            error: error.as_deref(),
            at,
        },
        SystemPayload::SourceCleanupFailed {
            stage_id,
            stage_name,
            error,
        } => StudioMessage::SourceCleanupFailed {
            stage_id: *stage_id,
            stage_name,
            error,
            at,
        },
        SystemPayload::MiddlewareLifecycle {
            stage_id,
            stage_name,
            flow_id,
            flow_name,
            origin,
            middleware: event,
        } => {
            let Some(update) = middleware.message(*stage_id, event) else {
                return Some(SseFrame::comment("unsupported_middleware_event_skipped"));
            };
            StudioMessage::MiddlewareLifecycle {
                stage_id: *stage_id,
                stage_name: stage_name.as_deref(),
                flow_id: flow_id.as_deref(),
                flow_name: flow_name.as_deref(),
                origin,
                revision: origin.seq,
                update,
                at,
            }
        }
        SystemPayload::ContractStatus {
            upstream,
            reader,
            selected_event_type,
            feed_role,
            pass,
            reader_seq,
            advertised_writer_seq,
            reason,
        } => StudioMessage::ContractStatus {
            edge: ContractEdge {
                upstream_stage_id: *upstream,
                reader_stage_id: *reader,
                selected_event_type: selected_event_type.as_ref(),
                feed_role: *feed_role,
                reader_seq: *reader_seq,
                advertised_writer_seq: *advertised_writer_seq,
                composite_boundaries: aliases.for_edge(*upstream, *reader),
            },
            pass: *pass,
            reason: reason.as_ref(),
            at,
        },
        SystemPayload::ContractResult {
            upstream,
            reader,
            selected_event_type,
            feed_role,
            contract_name,
            status,
            cause,
            reader_seq,
            advertised_writer_seq,
        } => StudioMessage::ContractResult {
            edge: ContractEdge {
                upstream_stage_id: *upstream,
                reader_stage_id: *reader,
                selected_event_type: selected_event_type.as_ref(),
                feed_role: *feed_role,
                reader_seq: *reader_seq,
                advertised_writer_seq: *advertised_writer_seq,
                composite_boundaries: aliases.for_edge(*upstream, *reader),
            },
            contract_name,
            status,
            cause: cause.as_deref(),
            at,
        },
        SystemPayload::MetricsCoordination(MetricsCoordinationEvent::Exported { watermark }) => {
            StudioMessage::MetricsWatermark {
                watermark,
                export_id: *envelope.id(),
                at,
            }
        }
        SystemPayload::MetricsCoordination(event) => StudioMessage::MetricsCoordination {
            event_type: match event {
                MetricsCoordinationEvent::Ready => MetricsUpdate::Ready,
                MetricsCoordinationEvent::DrainRequested => MetricsUpdate::DrainRequested,
                MetricsCoordinationEvent::Drained => MetricsUpdate::Drained,
                MetricsCoordinationEvent::Shutdown => MetricsUpdate::Shutdown,
                MetricsCoordinationEvent::Exported { .. } => unreachable!("handled above"),
            },
            at,
        },
        SystemPayload::IngressRefusal { .. } => return None,
    };
    Some(message.frame(Some(*envelope.id())))
}

fn observation(envelope: &SupervisorRecord) -> Observation<'_> {
    Observation {
        commitment: Some(
            envelope
                .commitment()
                .expect("admitted system commitment")
                .reference,
        ),
        timestamp_ms: envelope.timestamp(),
        vector_clock: Some(&envelope.journal().vector_clock),
        capture: None,
    }
}
