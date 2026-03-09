// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::factory::ChainEventFactory;
use crate::event::context::causality_context::CausalityContext;
use crate::event::context::observability_context::ObservabilityContext;
use crate::event::context::{
    FlowContext, IntentContext, ProcessingContext, ReplayContext, RuntimeContext,
};
use crate::event::payloads::correlation_payload::CorrelationPayload;
use crate::event::payloads::delivery_payload::DeliveryPayload;
use crate::event::payloads::flow_control_payload::FlowControlPayload;
use crate::event::payloads::observability_payload::{
    MetricsLifecycle, MiddlewareLifecycle, ObservabilityPayload, StageLifecycle,
};
use crate::event::status::processing_status::{ErrorKind, ProcessingStatus};
use crate::event::types::{CorrelationId, EventId, WriterId};
use crate::id::{CycleDepth, SccId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The definitive event structure for ObzenFlow
/// Lives inside EventEnvelope.data as serialized bytes
/// Focuses on application concerns, NOT infrastructure concerns
/// Designed to support CHAIN maturity model levels 1-4
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainEvent {
    // === Identity (Application Level) ===
    /// Unique event identifier (for application-level references)
    pub id: EventId,

    /// Which stage/service created this event (application identity)
    pub writer_id: WriterId,

    // === Core Event Content ===
    /// The actual event content - what kind of event this is
    pub content: ChainEventContent,

    // === Integration Layer (FLOWIP-007) ===
    /// Causality tracking
    pub causality: CausalityContext,

    /// Flow and stage context
    pub flow_context: FlowContext,

    /// Processing and monitoring metadata
    pub processing_info: ProcessingContext,

    // === CHAIN Maturity Support ===
    /// Explicit intent (I1 maturity minimum)
    pub intent: Option<IntentContext>,

    // === Flow-Level Correlation (FLOWIP-054d) ===
    /// Correlation ID that flows through all derived events
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<CorrelationId>,

    /// Metadata about when/where this correlation entered the flow
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_payload: Option<CorrelationPayload>,

    /// Provenance for replayed events (FLOWIP-095a).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replay_context: Option<ReplayContext>,

    // === Cycle Iteration Tracking (FLOWIP-051p) ===
    /// Per-event cycle depth counter. Incremented at the SCC entry point
    /// on each round trip. None for events that have never entered a cycle.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cycle_depth: Option<CycleDepth>,

    /// SCC identifier that `cycle_depth` belongs to. When an event enters
    /// an SCC with a different ID, the depth is reset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cycle_scc_id: Option<SccId>,

    // === Runtime Instrumentation (FLOWIP-056c) ===
    /// Runtime snapshot at event creation time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_context: Option<RuntimeContext>,

    // === Wide Events: Observability Data ===
    /// Can be attached to ANY event type (Data, FlowSignal, or Delivery)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observability: Option<ObservabilityContext>,
}

/// The core event content - what kind of event this is
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "content_type", rename_all = "snake_case")]
pub enum ChainEventContent {
    /// Application data events
    #[serde(rename = "data")]
    Data {
        event_type: String, // Keep as String for user-defined domain events
        payload: Value,
    },

    /// Flow control signals
    #[serde(rename = "flow_signal")]
    FlowControl(FlowControlPayload),

    /// Sink delivery facts
    #[serde(rename = "delivery")]
    Delivery(DeliveryPayload),

    /// Stage lifecycle and observability events
    #[serde(rename = "lifecycle")]
    Observability(ObservabilityPayload),
}

impl ChainEvent {
    /// Attach observability context to any event (wide events pattern)
    pub fn with_observability_context(mut self, observability: ObservabilityContext) -> Self {
        self.observability = Some(observability);
        self
    }

    pub fn with_runtime_context(mut self, ctx: RuntimeContext) -> Self {
        self.runtime_context = Some(ctx);
        self
    }

    /// Replace the flow-context block and return the updated event.
    pub fn with_flow_context(mut self, ctx: FlowContext) -> Self {
        self.flow_context = ctx;
        self
    }

    /// Set causality information for this event
    pub fn with_causality(mut self, causality: CausalityContext) -> Self {
        self.causality = causality;
        self
    }

    /// Check event type helpers
    pub fn is_eof(&self) -> bool {
        matches!(
            self.content,
            ChainEventContent::FlowControl(FlowControlPayload::Eof { .. })
        )
    }

    pub fn is_control(&self) -> bool {
        matches!(self.content, ChainEventContent::FlowControl(_))
    }

    pub fn is_system(&self) -> bool {
        // ChainEvent never contains system events - those are SystemEvent type
        false
    }

    pub fn is_data(&self) -> bool {
        matches!(self.content, ChainEventContent::Data { .. })
    }

    pub fn is_delivery(&self) -> bool {
        matches!(self.content, ChainEventContent::Delivery(_))
    }

    pub fn is_lifecycle(&self) -> bool {
        matches!(self.content, ChainEventContent::Observability(_))
    }

    /// Whether this event should be replayed from an archive (FLOWIP-095a).
    pub fn is_replayable(&self) -> bool {
        matches!(
            &self.content,
            ChainEventContent::Data { .. }
                | ChainEventContent::FlowControl(FlowControlPayload::Watermark { .. })
        )
    }

    /// Mark this event as an error with a structured ErrorKind.
    ///
    /// This sets `processing_info.status` to `ProcessingStatus::Error` with
    /// the provided message and kind, and primes `error_hops_remaining` so
    /// stage supervisors can route the event according to FLOWIP-082e/082g.
    pub fn mark_as_error(mut self, reason: impl Into<String>, kind: ErrorKind) -> Self {
        self.processing_info.status = ProcessingStatus::error_with_kind(reason.into(), Some(kind));
        self.processing_info.error_hops_remaining = Some(1);
        self
    }

    /// Convenience: mark this event as a domain/validation error.
    pub fn mark_as_validation_error(self, reason: impl Into<String>) -> Self {
        self.mark_as_error(reason, ErrorKind::Validation)
    }

    /// Convenience: mark this event as an infra/remote error.
    pub fn mark_as_infra_error(self, reason: impl Into<String>) -> Self {
        self.mark_as_error(reason, ErrorKind::Remote)
    }

    /// Create a derived error event from this event.
    ///
    /// This helper combines `ChainEventFactory::derived_data_event` with
    /// `mark_as_error`, preserving causality/correlation while marking the
    /// new event as an error with the provided `ErrorKind`.
    pub fn derive_error_event(
        &self,
        event_type: impl Into<String>,
        payload: Value,
        reason: impl Into<String>,
        kind: ErrorKind,
    ) -> ChainEvent {
        let reason_str = reason.into();
        ChainEventFactory::derived_data_event(self.writer_id, self, event_type, payload)
            .mark_as_error(reason_str, kind)
    }

    /// Return a concise "category.kind" string for logging & metrics.
    pub fn event_type(&self) -> String {
        match &self.content {
            ChainEventContent::Data { event_type, .. } => event_type.clone(),

            ChainEventContent::FlowControl(signal) => match signal {
                FlowControlPayload::Eof { .. } => "control.eof".into(),
                FlowControlPayload::Watermark { .. } => "control.watermark".into(),
                FlowControlPayload::Checkpoint { .. } => "control.checkpoint".into(),
                FlowControlPayload::Drain => "control.drain".into(),
                FlowControlPayload::PipelineAbort { .. } => "control.pipeline_abort".into(),
                FlowControlPayload::SourceContract { .. } => "control.source_contract".into(),
                FlowControlPayload::ConsumptionProgress { .. } => {
                    "control.consumption_progress".into()
                }
                FlowControlPayload::ConsumptionGap { .. } => "control.consumption_gap".into(),
                FlowControlPayload::ConsumptionFinal { .. } => "control.consumption_final".into(),
                FlowControlPayload::ReaderStalled { .. } => "control.reader_stalled".into(),
                FlowControlPayload::AtLeastOnceViolation { .. } => {
                    "control.at_least_once_violation".into()
                }
            },

            ChainEventContent::Delivery(_) => "sink.delivery".into(),

            ChainEventContent::Observability(obs) => match obs {
                ObservabilityPayload::Stage(stage) => match stage {
                    StageLifecycle::Running { .. } => "lifecycle.stage.running".into(),
                    StageLifecycle::Draining { .. } => "lifecycle.stage.draining".into(),
                    StageLifecycle::Drained { .. } => "lifecycle.stage.drained".into(),
                    StageLifecycle::Completed { .. } => "lifecycle.stage.completed".into(),
                    StageLifecycle::Failed { .. } => "lifecycle.stage.failed".into(),
                },
                ObservabilityPayload::Metrics(metrics) => match metrics {
                    MetricsLifecycle::Ready { .. } => "lifecycle.metrics.ready".into(),
                    MetricsLifecycle::StateSnapshot { .. } => "lifecycle.metrics.state".into(),
                    MetricsLifecycle::ResourceUsage { .. } => "lifecycle.metrics.resource".into(),
                    MetricsLifecycle::Custom { .. } => "lifecycle.metrics.custom".into(),
                    MetricsLifecycle::DrainRequested => "lifecycle.metrics.drain".into(),
                    MetricsLifecycle::Drained { .. } => "lifecycle.metrics.drained".into(),
                },
                ObservabilityPayload::Middleware(mw) => match mw {
                    MiddlewareLifecycle::CircuitBreaker(_) => {
                        "lifecycle.middleware.circuit_breaker".into()
                    }
                    MiddlewareLifecycle::RateLimiter(_) => {
                        "lifecycle.middleware.rate_limiter".into()
                    }
                    MiddlewareLifecycle::Backpressure(_) => {
                        "lifecycle.middleware.backpressure".into()
                    }
                    MiddlewareLifecycle::Retry(_) => "lifecycle.middleware.retry".into(),
                    MiddlewareLifecycle::Sli(_) => "lifecycle.middleware.sli".into(),
                },
            },
        }
    }

    /// Get payload as JSON value
    pub fn payload(&self) -> Value {
        match &self.content {
            ChainEventContent::Data { payload, .. } => payload.clone(),
            ChainEventContent::FlowControl(signal) => {
                serde_json::to_value(signal).unwrap_or_default()
            }
            ChainEventContent::Delivery(delivery) => {
                serde_json::to_value(delivery).unwrap_or_default()
            }
            ChainEventContent::Observability(lifecycle) => {
                serde_json::to_value(lifecycle).unwrap_or_default()
            }
        }
    }
}
