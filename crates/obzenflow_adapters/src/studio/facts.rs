// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Select the Studio message for a committed fact. Encoding never changes state.

use super::messages::{ContractEdge, MetricsUpdate, Observation, StudioMessage};
use super::{middleware::MiddlewareView, ContractBoundaryAliases};
use obzenflow_core::event::{
    event_envelope::SystemEventEnvelope, MetricsCoordinationEvent, SystemEventType,
};
use obzenflow_core::web::SseFrame;

pub(super) fn stage_message(envelope: &SystemEventEnvelope) -> Option<StudioMessage<'_>> {
    let SystemEventType::StageLifecycle { stage_id, event } = &envelope.event.event else {
        return None;
    };
    Some(StudioMessage::StageLifecycle {
        stage_id: *stage_id,
        event,
        at: observation(envelope),
    })
}

pub(super) fn frame(
    envelope: &SystemEventEnvelope,
    middleware: &MiddlewareView,
    aliases: &ContractBoundaryAliases,
) -> Option<SseFrame> {
    let at = observation(envelope);
    let message = match &envelope.event.event {
        SystemEventType::StageLifecycle { .. } => stage_message(envelope)?,
        SystemEventType::PipelineLifecycle(event) => StudioMessage::FlowLifecycle { event, at },
        SystemEventType::ReplayLifecycle(event) => StudioMessage::ReplayLifecycle {
            stage_id: envelope.event.writer_id.as_stage().map(|id| id.to_string()),
            event,
            at,
        },
        SystemEventType::SourceCleanupFailed {
            stage_id,
            stage_name,
            error,
        } => StudioMessage::SourceCleanupFailed {
            stage_id: *stage_id,
            stage_name,
            error,
            at,
        },
        SystemEventType::MiddlewareLifecycle {
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
        SystemEventType::ContractStatus {
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
        SystemEventType::ContractResult {
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
        SystemEventType::EdgeLiveness {
            upstream,
            reader,
            state,
            idle_ms,
            last_reader_seq,
            last_event_id,
        } => StudioMessage::EdgeLiveness {
            upstream_stage_id: *upstream,
            reader_stage_id: *reader,
            state: *state,
            idle_ms: *idle_ms,
            last_reader_seq: *last_reader_seq,
            last_event_id: *last_event_id,
            at,
        },
        SystemEventType::MetricsCoordination(MetricsCoordinationEvent::Exported { watermark }) => {
            StudioMessage::MetricsWatermark {
                watermark,
                export_id: envelope.event.id,
                at,
            }
        }
        SystemEventType::MetricsCoordination(event) => StudioMessage::MetricsCoordination {
            event_type: match event {
                MetricsCoordinationEvent::Ready => MetricsUpdate::Ready,
                MetricsCoordinationEvent::DrainRequested => MetricsUpdate::DrainRequested,
                MetricsCoordinationEvent::Drained => MetricsUpdate::Drained,
                MetricsCoordinationEvent::Shutdown => MetricsUpdate::Shutdown,
                MetricsCoordinationEvent::Exported { .. } => unreachable!("handled above"),
            },
            at,
        },
        // High-volume telemetry and ingress refusals belong to the metrics view.
        SystemEventType::StageHeartbeat { .. }
        | SystemEventType::HttpSurfaceSnapshot { .. }
        | SystemEventType::IngressRefusal { .. } => return None,
    };
    Some(message.frame(Some(envelope.event.id)))
}

fn observation(envelope: &SystemEventEnvelope) -> Observation<'_> {
    Observation {
        timestamp_ms: envelope.event.timestamp,
        vector_clock: &envelope.vector_clock,
    }
}
