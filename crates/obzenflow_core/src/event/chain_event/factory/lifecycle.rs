// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::ChainEventFactory;
use crate::event::chain_event::{ChainEvent, ChainEventContent};
use crate::event::payloads::observability_payload::{
    MetricsLifecycle, ObservabilityPayload, StageLifecycle,
};
use crate::event::types::WriterId;
use crate::id::StageId;
use serde_json::Value;

impl ChainEventFactory {
    /// Create a lifecycle event
    pub fn observability_event(writer_id: WriterId, payload: ObservabilityPayload) -> ChainEvent {
        Self::create_event(writer_id, ChainEventContent::Observability(payload))
    }

    /// Create a stage running event
    pub fn stage_running(writer_id: WriterId, stage_id: StageId) -> ChainEvent {
        Self::observability_event(
            writer_id,
            ObservabilityPayload::Stage(StageLifecycle::Running {
                stage_id,
                metadata: None,
            }),
        )
    }

    /// Create a stage draining event
    pub fn stage_draining(
        writer_id: WriterId,
        stage_id: StageId,
        reason: Option<String>,
    ) -> ChainEvent {
        Self::observability_event(
            writer_id,
            ObservabilityPayload::Stage(StageLifecycle::Draining { stage_id, reason }),
        )
    }

    /// Create a stage drained event
    pub fn stage_drained(
        writer_id: WriterId,
        stage_id: StageId,
        events_processed: Option<u64>,
    ) -> ChainEvent {
        Self::observability_event(
            writer_id,
            ObservabilityPayload::Stage(StageLifecycle::Drained {
                stage_id,
                events_processed,
            }),
        )
    }

    /// Create a stage completed event
    pub fn stage_completed(writer_id: WriterId, stage_id: StageId) -> ChainEvent {
        Self::observability_event(
            writer_id,
            ObservabilityPayload::Stage(StageLifecycle::Completed {
                stage_id,
                final_metrics: None,
            }),
        )
    }

    /// Create a stage failed event
    pub fn stage_failed(
        writer_id: WriterId,
        stage_id: StageId,
        error: String,
        recoverable: Option<bool>,
    ) -> ChainEvent {
        Self::observability_event(
            writer_id,
            ObservabilityPayload::Stage(StageLifecycle::Failed {
                stage_id,
                error,
                recoverable,
            }),
        )
    }

    /// Create a metrics ready event
    pub fn metrics_ready(writer_id: WriterId) -> ChainEvent {
        Self::observability_event(
            writer_id,
            ObservabilityPayload::Metrics(MetricsLifecycle::Ready {
                exporter_count: None,
            }),
        )
    }

    /// Create a metrics state snapshot event
    pub fn metrics_state_snapshot(writer_id: WriterId, metrics: Value) -> ChainEvent {
        Self::observability_event(
            writer_id,
            ObservabilityPayload::Metrics(MetricsLifecycle::StateSnapshot {
                metrics,
                window_duration_ms: None,
            }),
        )
    }

    /// Create a metrics drain requested event
    pub fn metrics_drain_requested(writer_id: WriterId) -> ChainEvent {
        Self::observability_event(
            writer_id,
            ObservabilityPayload::Metrics(MetricsLifecycle::DrainRequested),
        )
    }

    /// Create a metrics drained event
    pub fn metrics_drained(writer_id: WriterId) -> ChainEvent {
        Self::observability_event(
            writer_id,
            ObservabilityPayload::Metrics(MetricsLifecycle::Drained {
                final_flush_count: None,
            }),
        )
    }

    /// Create a windowing count metrics event
    pub fn windowing_count_event(
        writer_id: WriterId,
        count: usize,
        window_end_ms: u128,
    ) -> ChainEvent {
        Self::observability_event(
            writer_id,
            ObservabilityPayload::Metrics(MetricsLifecycle::Custom {
                name: "windowing.count".to_string(),
                value: serde_json::json!({
                    "count": count,
                    "window_end_ms": window_end_ms,
                }),
                tags: None,
            }),
        )
    }

    /// Create a windowing sum metrics event
    pub fn windowing_sum_event(
        writer_id: WriterId,
        sum: f64,
        field: String,
        value_count: usize,
        event_count: usize,
        window_end_ms: u128,
    ) -> ChainEvent {
        Self::observability_event(
            writer_id,
            ObservabilityPayload::Metrics(MetricsLifecycle::Custom {
                name: "windowing.sum".to_string(),
                value: serde_json::json!({
                    "sum": sum,
                    "field": field,
                    "value_count": value_count,
                    "event_count": event_count,
                    "window_end_ms": window_end_ms,
                }),
                tags: None,
            }),
        )
    }

    /// Create a windowing average metrics event
    pub fn windowing_average_event(
        writer_id: WriterId,
        average: f64,
        field: String,
        value_count: usize,
        event_count: usize,
        window_end_ms: u128,
    ) -> ChainEvent {
        Self::observability_event(
            writer_id,
            ObservabilityPayload::Metrics(MetricsLifecycle::Custom {
                name: "windowing.average".to_string(),
                value: serde_json::json!({
                    "average": average,
                    "field": field,
                    "value_count": value_count,
                    "event_count": event_count,
                    "window_end_ms": window_end_ms,
                }),
                tags: None,
            }),
        )
    }
}
