// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{current_timestamp, ChainEventFactory};
use crate::event::chain_event::{
    ChainEvent, ChainEventContent, ConsumptionFinalEventParams, ConsumptionProgressEventParams,
    SourceContractEventParams,
};
use crate::event::payloads::flow_control_payload::FlowControlPayload;
use crate::event::types::{SeqNo, ViolationCause, WriterId};
use crate::id::StageId;
use serde_json::Value;

impl ChainEventFactory {
    /// Create an EOF signal
    pub fn eof_event(writer_id: WriterId, natural: bool) -> ChainEvent {
        Self::create_event(
            writer_id,
            ChainEventContent::FlowControl(FlowControlPayload::Eof {
                natural,
                timestamp: current_timestamp(),
                writer_id: Some(writer_id),
                writer_seq: None,
                vector_clock: None,
                last_event_id: None,
            }),
        )
    }

    /// Create a watermark signal
    pub fn watermark_event(
        writer_id: WriterId,
        timestamp: u64,
        stage_id: Option<String>,
    ) -> ChainEvent {
        Self::create_event(
            writer_id,
            ChainEventContent::FlowControl(FlowControlPayload::Watermark {
                timestamp,
                stage_id,
            }),
        )
    }

    /// Create a checkpoint signal
    pub fn checkpoint_event(
        writer_id: WriterId,
        id: String,
        metadata: Option<Value>,
    ) -> ChainEvent {
        Self::create_event(
            writer_id,
            ChainEventContent::FlowControl(FlowControlPayload::Checkpoint { id, metadata }),
        )
    }

    /// Create a drain signal
    pub fn drain_event(writer_id: WriterId) -> ChainEvent {
        Self::create_event(
            writer_id,
            ChainEventContent::FlowControl(FlowControlPayload::Drain),
        )
    }

    /// Create a pipeline abort signal
    pub fn pipeline_abort_event(
        writer_id: WriterId,
        reason: ViolationCause,
        upstream: Option<crate::StageId>,
    ) -> ChainEvent {
        Self::create_event(
            writer_id,
            ChainEventContent::FlowControl(FlowControlPayload::PipelineAbort { reason, upstream }),
        )
    }

    /// Create a source contract event
    pub fn source_contract_event(
        writer_id: WriterId,
        params: SourceContractEventParams,
    ) -> ChainEvent {
        let SourceContractEventParams {
            expected_count,
            source_id,
            route,
            journal_path,
            journal_index,
            writer_seq,
            vector_clock,
        } = params;
        Self::create_event(
            writer_id,
            ChainEventContent::FlowControl(FlowControlPayload::SourceContract {
                expected_count,
                source_id,
                route,
                journal_path,
                journal_index,
                writer_seq,
                vector_clock,
            }),
        )
    }

    /// Create a consumption progress event
    pub fn consumption_progress_event(
        writer_id: WriterId,
        params: ConsumptionProgressEventParams,
    ) -> ChainEvent {
        let ConsumptionProgressEventParams {
            reader_seq,
            last_event_id,
            vector_clock,
            eof_seen,
            reader_path,
            reader_index,
            advertised_writer_seq,
            advertised_vector_clock,
            stalled_since,
        } = params;
        Self::create_event(
            writer_id,
            ChainEventContent::FlowControl(FlowControlPayload::ConsumptionProgress {
                reader_seq,
                last_event_id,
                vector_clock,
                eof_seen,
                reader_path,
                reader_index,
                advertised_writer_seq,
                advertised_vector_clock,
                stalled_since,
            }),
        )
    }

    /// Create a consumption gap event
    pub fn consumption_gap_event(
        writer_id: WriterId,
        from_seq: SeqNo,
        to_seq: SeqNo,
        upstream: StageId,
    ) -> ChainEvent {
        Self::create_event(
            writer_id,
            ChainEventContent::FlowControl(FlowControlPayload::ConsumptionGap {
                from_seq,
                to_seq,
                upstream,
            }),
        )
    }

    /// Create a consumption final event
    pub fn consumption_final_event(
        writer_id: WriterId,
        params: ConsumptionFinalEventParams,
    ) -> ChainEvent {
        let ConsumptionFinalEventParams {
            pass,
            consumed_count,
            expected_count,
            eof_seen,
            last_event_id,
            reader_seq,
            advertised_writer_seq,
            advertised_vector_clock,
            failure_reason,
        } = params;
        Self::create_event(
            writer_id,
            ChainEventContent::FlowControl(FlowControlPayload::ConsumptionFinal {
                pass,
                consumed_count,
                expected_count,
                eof_seen,
                last_event_id,
                reader_seq,
                advertised_writer_seq,
                advertised_vector_clock,
                failure_reason,
            }),
        )
    }

    /// Create a reader stalled event
    pub fn reader_stalled_event(
        writer_id: WriterId,
        upstream: crate::StageId,
        stalled_since: crate::event::types::DurationMs,
    ) -> ChainEvent {
        Self::create_event(
            writer_id,
            ChainEventContent::FlowControl(FlowControlPayload::ReaderStalled {
                upstream,
                stalled_since,
            }),
        )
    }

    /// Create an at-least-once violation event
    pub fn at_least_once_violation_event(
        writer_id: WriterId,
        upstream: crate::StageId,
        reason: crate::event::types::ViolationCause,
        reader_seq: crate::event::types::SeqNo,
        advertised_writer_seq: Option<crate::event::types::SeqNo>,
    ) -> ChainEvent {
        Self::create_event(
            writer_id,
            ChainEventContent::FlowControl(FlowControlPayload::AtLeastOnceViolation {
                upstream,
                reason,
                reader_seq,
                advertised_writer_seq,
            }),
        )
    }
}
