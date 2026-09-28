// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::ChainEventFactory;
use crate::event::payloads::execution_payload::{
    ExecutionPayload, MetricsCoordinationFact, StageLifecycleFact,
};
use crate::event::provenance::ExecutionAccounting;
use crate::event::{ChainEvent, ChainPayload};
use crate::{StageId, WriterId};

impl ChainEventFactory {
    pub fn execution_event(writer_id: WriterId, payload: ExecutionPayload) -> ChainEvent {
        Self::create_event(writer_id, ChainPayload::Execution(payload))
    }

    pub fn stage_running(writer_id: WriterId, stage_id: StageId) -> ChainEvent {
        Self::execution_event(
            writer_id,
            ExecutionPayload::StageLifecycle(StageLifecycleFact::Running { stage_id }),
        )
    }
    pub fn stage_draining(
        writer_id: WriterId,
        stage_id: StageId,
        reason: Option<String>,
    ) -> ChainEvent {
        Self::execution_event(
            writer_id,
            ExecutionPayload::StageLifecycle(StageLifecycleFact::Draining {
                stage_id,
                reason,
                accounting: None,
            }),
        )
    }
    pub fn stage_drained(
        writer_id: WriterId,
        stage_id: StageId,
        events_processed: Option<u64>,
    ) -> ChainEvent {
        Self::execution_event(
            writer_id,
            ExecutionPayload::StageLifecycle(StageLifecycleFact::Drained {
                stage_id,
                events_processed,
            }),
        )
    }
    pub fn stage_completed(
        writer_id: WriterId,
        stage_id: StageId,
        accounting: ExecutionAccounting,
    ) -> ChainEvent {
        Self::execution_event(
            writer_id,
            ExecutionPayload::StageLifecycle(StageLifecycleFact::Completed {
                stage_id,
                accounting: Some(accounting),
            }),
        )
    }
    pub fn stage_failed(
        writer_id: WriterId,
        stage_id: StageId,
        error: String,
        recoverable: Option<bool>,
    ) -> ChainEvent {
        Self::execution_event(
            writer_id,
            ExecutionPayload::StageLifecycle(StageLifecycleFact::Failed {
                stage_id,
                error,
                recoverable,
                accounting: None,
                causal_event_id: None,
            }),
        )
    }
    pub fn metrics_ready(writer_id: WriterId) -> ChainEvent {
        Self::execution_event(
            writer_id,
            ExecutionPayload::MetricsCoordination(MetricsCoordinationFact::Ready {
                exporter_count: None,
            }),
        )
    }
    pub fn metrics_drain_requested(writer_id: WriterId) -> ChainEvent {
        Self::execution_event(
            writer_id,
            ExecutionPayload::MetricsCoordination(MetricsCoordinationFact::DrainRequested),
        )
    }
    pub fn metrics_drained(writer_id: WriterId) -> ChainEvent {
        Self::execution_event(
            writer_id,
            ExecutionPayload::MetricsCoordination(MetricsCoordinationFact::Drained {
                final_flush_count: None,
            }),
        )
    }
}
