// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Projects original owned journal facts into the messages in `messages.rs`.
//! `stages.rs` also uses `stage_message` to build snapshots with the same JSON.

use super::messages::{ContractEdge, MetricsUpdate, Observation, StudioMessage};
use super::{middleware::MiddlewareView, ContractBoundaryAliases};
use obzenflow_core::event::journal_record::{ChainJournalRecord, SystemJournalRecord};
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::{ChainPayload, MetricsCoordinationEvent, SystemPayload};
use obzenflow_core::journal::read::RunRecordData;
use obzenflow_core::web::SseFrame;

pub(super) fn stage_message(envelope: &ChainJournalRecord) -> Option<StudioMessage<'_>> {
    let ChainPayload::Execution(ExecutionPayload::StageLifecycle(event)) = &envelope.payload else {
        return None;
    };
    Some(StudioMessage::StageLifecycle {
        stage_id: event.stage_id(),
        event,
        at: observation(
            envelope,
            envelope.envelope.provenance.event.processing.event_time,
        ),
    })
}

pub(super) fn frame(
    record: &RunRecordData,
    middleware: &MiddlewareView,
    aliases: &ContractBoundaryAliases,
) -> Option<SseFrame> {
    match record {
        RunRecordData::Chain(record) => stage_frame(record, middleware, aliases),
        RunRecordData::System(record) => system_frame(record),
    }
}

fn stage_frame(
    envelope: &ChainJournalRecord,
    middleware: &MiddlewareView,
    aliases: &ContractBoundaryAliases,
) -> Option<SseFrame> {
    let ChainPayload::Execution(payload) = &envelope.payload else {
        return None;
    };
    let at = observation(
        envelope,
        envelope.envelope.provenance.event.processing.event_time,
    );
    let message = match payload {
        ExecutionPayload::SupervisorRegistered { descriptor } => {
            StudioMessage::SupervisorRegistered {
                writer_id: envelope.writer_id(),
                descriptor,
                at,
            }
        }
        ExecutionPayload::StageLifecycle(_) => stage_message(envelope)?,
        ExecutionPayload::ReplayLifecycle(event) => StudioMessage::ReplayLifecycle {
            stage_id: envelope.writer_id().as_stage().map(|id| id.to_string()),
            event,
            at,
        },
        ExecutionPayload::SupervisorCommandDiscarded {
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
        ExecutionPayload::SourceCleanupFailed {
            stage_id,
            stage_name,
            error,
        } => StudioMessage::SourceCleanupFailed {
            stage_id: *stage_id,
            stage_name,
            error,
            at,
        },
        ExecutionPayload::CircuitBreaker(_) | ExecutionPayload::RateLimiter(_) => {
            let context = &envelope.envelope.provenance.event.flow_context;
            let update = middleware.message(context.stage_id, payload)?;
            StudioMessage::MiddlewareLifecycle {
                stage_id: context.stage_id,
                stage_name: Some(&context.stage_name),
                flow_id: Some(&context.flow_id),
                flow_name: Some(&context.flow_name),
                origin: super::messages::MiddlewareEventOrigin {
                    event_id: *envelope.id(),
                    writer_key: envelope.writer_id().to_string(),
                    seq: obzenflow_core::event::types::SeqNo(envelope.local_sequence()),
                },
                revision: obzenflow_core::event::types::SeqNo(envelope.local_sequence()),
                update,
                at,
            }
        }
        ExecutionPayload::ContractStatus {
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
        ExecutionPayload::ContractResult {
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
        _ => return None,
    };
    Some(message.frame(Some(*envelope.id())))
}

fn system_frame(envelope: &SystemJournalRecord) -> Option<SseFrame> {
    let at = observation(envelope, envelope.envelope.provenance.event.timestamp);
    let message = match &envelope.payload {
        SystemPayload::SupervisorRegistered { descriptor } => StudioMessage::SupervisorRegistered {
            writer_id: envelope.writer_id(),
            descriptor,
            at,
        },
        SystemPayload::PipelineLifecycle(event) => StudioMessage::FlowLifecycle { event, at },
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
        _ => return None,
    };
    Some(message.frame(Some(*envelope.id())))
}

fn observation<P: obzenflow_core::JournalPayload>(
    envelope: &obzenflow_core::JournalRecord<P>,
    timestamp_ms: u64,
) -> Observation<'_> {
    Observation {
        commitment: Some(envelope.commitment()),
        timestamp_ms,
        vector_clock: Some(&envelope.envelope.provenance.journal.vector_clock),
        capture: None,
    }
}
