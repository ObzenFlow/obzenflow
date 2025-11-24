//! ChainEvent - the enhanced event structure for FlowState RS
//! This is the application-level event that lives inside EventEnvelope.data

use crate::event::context::causality_context::CausalityContext;
use crate::event::context::observability_context::ObservabilityContext;
use crate::event::context::{FlowContext, IntentContext, ProcessingContext, RuntimeContext};
use crate::event::types::{CorrelationId, EventId, WriterId};
use crate::id::StageId;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The definitive event structure for FlowState RS
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
    /// Create a new ChainEvent
    fn new(id: EventId, writer_id: WriterId, content: ChainEventContent) -> Self {
        ChainEvent {
            id,
            writer_id,
            content,
            causality: CausalityContext::new(),
            flow_context: FlowContext::default(),
            processing_info: ProcessingContext::default(),
            intent: None,
            correlation_id: None,
            correlation_payload: None,
            runtime_context: None,
            observability: None,
        }
    }

    /// Create a data event
    fn data(
        id: EventId,
        writer_id: WriterId,
        event_type: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self::new(
            id,
            writer_id,
            ChainEventContent::Data {
                event_type: event_type.into(),
                payload,
            },
        )
    }

    /// Create an EOF signal
    fn eof(id: EventId, writer_id: WriterId, natural: bool) -> Self {
        Self::new(
            id,
            writer_id.clone(),
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

    /// Create a sink delivery event
    fn delivery(id: EventId, writer_id: WriterId, payload: DeliveryPayload) -> Self {
        Self::new(id, writer_id, ChainEventContent::Delivery(payload))
    }

    /// Create an observability event
    fn observability(id: EventId, writer_id: WriterId, payload: ObservabilityPayload) -> Self {
        Self::new(id, writer_id, ChainEventContent::Observability(payload))
    }

    /// Attach observability context to any event (wide events pattern)
    pub fn with_observability_context(mut self, observability: ObservabilityContext) -> Self {
        self.observability = Some(observability);
        self
    }

    pub fn with_runtime_context(mut self, ctx: RuntimeContext) -> Self {
        self.runtime_context = Some(ctx);
        self
    }

    /// Replace the flow‑context block and return the updated event.
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

    /// Return a concise “category.kind” string for logging & metrics.
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
                // Stage lifecycle
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

                // Middleware lifecycle
                ObservabilityPayload::Middleware(mw) => match mw {
                    MiddlewareLifecycle::CircuitBreaker(_) => {
                        "lifecycle.middleware.circuit_breaker".into()
                    }
                    MiddlewareLifecycle::RateLimiter(_) => {
                        "lifecycle.middleware.rate_limiter".into()
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

    // === Correlation Support (FLOWIP-054d) ===

    /// Create correlation for a source event (flow entry)
    pub fn with_new_correlation(mut self, stage_name: impl Into<String>) -> Self {
        let correlation_id = CorrelationId::new();
        self.correlation_id = Some(correlation_id);
        self.correlation_payload = Some(CorrelationPayload::new(stage_name, self.id));
        self
    }

    /// Propagate correlation from parent event to derived event
    pub fn with_correlation_from(mut self, parent: &ChainEvent) -> Self {
        self.correlation_id = parent.correlation_id;
        self.correlation_payload = parent.correlation_payload.clone();
        self
    }

    /// Check if this event has correlation info
    pub fn has_correlation(&self) -> bool {
        self.correlation_id.is_some()
    }

    /// Calculate latency if this event has correlation payload
    pub fn correlation_latency(&self) -> Option<std::time::Duration> {
        self.correlation_payload
            .as_ref()
            .map(|p| p.calculate_latency())
    }
}

/// Get current timestamp in milliseconds since epoch
fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// Implement JournalEvent for ChainEvent
use crate::event::journal_event::{JournalEvent, Sealed};
use crate::event::payloads::correlation_payload::CorrelationPayload;
use crate::event::payloads::delivery_payload::DeliveryPayload;
use crate::event::payloads::flow_control_payload::FlowControlPayload;
use crate::event::payloads::observability_payload::{
    CircuitBreakerEvent, MetricsLifecycle, MiddlewareLifecycle, ObservabilityPayload, RetryEvent,
    StageLifecycle,
};

// Implement the sealed trait first
impl Sealed for ChainEvent {}

impl JournalEvent for ChainEvent {
    fn id(&self) -> &EventId {
        &self.id
    }

    fn writer_id(&self) -> &WriterId {
        &self.writer_id
    }

    /// Zero‑alloc category string for metrics & fast logs.
    /// Falls back to generic labels when the name is dynamic (e.g. custom metrics).
    fn event_type_name(&self) -> &'static str {
        match &self.content {
            ChainEventContent::Data { .. } => "data",
            ChainEventContent::FlowControl(sig) => match sig {
                FlowControlPayload::Eof { .. } => "control.eof",
                FlowControlPayload::Watermark { .. } => "control.watermark",
                FlowControlPayload::Checkpoint { .. } => "control.checkpoint",
                FlowControlPayload::Drain => "control.drain",
                FlowControlPayload::PipelineAbort { .. } => "control.pipeline_abort",
                FlowControlPayload::SourceContract { .. } => "control.source_contract",
                FlowControlPayload::ConsumptionProgress { .. } => "control.consumption_progress",
                FlowControlPayload::ConsumptionGap { .. } => "control.consumption_gap",
                FlowControlPayload::ConsumptionFinal { .. } => "control.consumption_final",
                FlowControlPayload::ReaderStalled { .. } => "control.reader_stalled",
                FlowControlPayload::AtLeastOnceViolation { .. } => {
                    "control.at_least_once_violation"
                }
            },
            ChainEventContent::Delivery(_) => "sink.delivery",
            ChainEventContent::Observability(obs) => match obs {
                ObservabilityPayload::Stage(_) => "lifecycle.stage",
                ObservabilityPayload::Metrics(m) => match m {
                    MetricsLifecycle::Custom { .. } => "lifecycle.metrics.custom",
                    _ => "lifecycle.metrics",
                },
                ObservabilityPayload::Middleware(_) => "lifecycle.middleware",
            },
        }
    }
}

/// Stateless factory for creating ChainEvents with consistent patterns
pub struct ChainEventFactory;

impl ChainEventFactory {
    /// Create a data event
    pub fn data_event(
        writer_id: WriterId,
        event_type: impl Into<String>,
        payload: Value,
    ) -> ChainEvent {
        Self::create_event(
            writer_id,
            ChainEventContent::Data {
                event_type: event_type.into(),
                payload,
            },
        )
    }

    /// Create a data event from a serializable struct
    pub fn data_event_from<T: serde::Serialize>(
        writer_id: WriterId,
        event_type: impl Into<String>,
        data: &T,
    ) -> Result<ChainEvent, serde_json::Error> {
        let payload = serde_json::to_value(data)?;
        Ok(Self::data_event(writer_id, event_type, payload))
    }

    /// Create an EOF signal
    pub fn eof_event(writer_id: WriterId, natural: bool) -> ChainEvent {
        Self::create_event(
            writer_id.clone(),
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
        reason: crate::event::types::ViolationCause,
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
        expected_count: Option<crate::event::types::Count>,
        source_id: crate::StageId,
        route: Option<crate::event::types::RouteKey>,
        journal_path: crate::event::types::JournalPath,
        journal_index: crate::event::types::JournalIndex,
        writer_seq: Option<crate::event::types::SeqNo>,
        vector_clock: Option<crate::event::vector_clock::VectorClock>,
    ) -> ChainEvent {
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
        reader_seq: crate::event::types::SeqNo,
        last_event_id: Option<crate::event::types::EventId>,
        vector_clock: Option<crate::event::vector_clock::VectorClock>,
        eof_seen: bool,
        reader_path: crate::event::types::JournalPath,
        reader_index: crate::event::types::JournalIndex,
        advertised_writer_seq: Option<crate::event::types::SeqNo>,
        advertised_vector_clock: Option<crate::event::vector_clock::VectorClock>,
        stalled_since: Option<crate::event::types::DurationMs>,
    ) -> ChainEvent {
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
        from_seq: crate::event::types::SeqNo,
        to_seq: crate::event::types::SeqNo,
        upstream: crate::StageId,
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
        pass: bool,
        consumed_count: crate::event::types::Count,
        expected_count: Option<crate::event::types::Count>,
        eof_seen: bool,
        last_event_id: Option<crate::event::types::EventId>,
        reader_seq: crate::event::types::SeqNo,
        advertised_writer_seq: Option<crate::event::types::SeqNo>,
        advertised_vector_clock: Option<crate::event::vector_clock::VectorClock>,
        failure_reason: Option<crate::event::types::ViolationCause>,
    ) -> ChainEvent {
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

    /// Create a delivery event
    pub fn delivery_event(writer_id: WriterId, payload: DeliveryPayload) -> ChainEvent {
        Self::create_event(writer_id, ChainEventContent::Delivery(payload))
    }

    // === Lifecycle Events ===

    /// Create a lifecycle event
    pub fn observability_event(writer_id: WriterId, payload: ObservabilityPayload) -> ChainEvent {
        Self::create_event(writer_id, ChainEventContent::Observability(payload))
    }

    // Stage lifecycle helpers

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

    // Metrics lifecycle helpers

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

    // Middleware lifecycle helpers

    /// Create a circuit breaker opened event
    pub fn circuit_breaker_opened(
        writer_id: WriterId,
        error_rate: f64,
        failure_count: u64,
    ) -> ChainEvent {
        Self::observability_event(
            writer_id,
            ObservabilityPayload::Middleware(MiddlewareLifecycle::CircuitBreaker(
                CircuitBreakerEvent::Opened {
                    error_rate,
                    failure_count,
                    last_error: None,
                },
            )),
        )
    }

    /// Create a circuit breaker summary event
    pub fn circuit_breaker_summary(
        writer_id: WriterId,
        window_duration_s: u64,
        requests_processed: u64,
        requests_rejected: u64,
        state: String,
        consecutive_failures: usize,
        rejection_rate: f64,
    ) -> ChainEvent {
        Self::observability_event(
            writer_id,
            ObservabilityPayload::Middleware(MiddlewareLifecycle::CircuitBreaker(
                CircuitBreakerEvent::Summary {
                    window_duration_s,
                    requests_processed,
                    requests_rejected,
                    state,
                    consecutive_failures,
                    rejection_rate,
                },
            )),
        )
    }

    /// Create a retry exhausted event
    pub fn retry_exhausted(
        writer_id: WriterId,
        total_attempts: u32,
        last_error: String,
        total_duration_ms: u64,
    ) -> ChainEvent {
        Self::observability_event(
            writer_id,
            ObservabilityPayload::Middleware(MiddlewareLifecycle::Retry(RetryEvent::Exhausted {
                total_attempts,
                last_error,
                total_duration_ms,
            })),
        )
    }

    // Windowing metrics events

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

    /// Create a derived event from a parent event (propagates correlation)
    pub fn derived_event(
        writer_id: WriterId,
        parent: &ChainEvent,
        content: ChainEventContent,
    ) -> ChainEvent {
        let mut event = Self::create_event(writer_id, content);

        // Propagate correlation
        event.correlation_id = parent.correlation_id;
        event.correlation_payload = parent.correlation_payload.clone();

        // FIX (FLOWIP-082): Propagate full lineage with depth limit
        const DEFAULT_MAX_LINEAGE_DEPTH: usize = 100;

        // Add immediate parent
        event.causality = CausalityContext::with_parent(parent.id);

        // Get max depth from environment or use default
        let max_depth = std::env::var("OBZENFLOW_MAX_LINEAGE_DEPTH")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_MAX_LINEAGE_DEPTH);

        // Propagate ancestors up to depth limit
        let ancestors_to_add = parent
            .causality
            .parent_ids
            .iter()
            .take(max_depth.saturating_sub(1));

        for ancestor in ancestors_to_add {
            event.causality = event.causality.add_parent(*ancestor);
        }

        // TODO: Log warning if we're truncating (requires tracing dependency)
        // Currently we silently truncate at max_depth
        #[allow(unused_variables)]
        let truncated = parent.causality.parent_ids.len() >= max_depth;

        event
    }

    /// Create a derived data event from a parent
    pub fn derived_data_event(
        writer_id: WriterId,
        parent: &ChainEvent,
        event_type: impl Into<String>,
        payload: Value,
    ) -> ChainEvent {
        Self::derived_event(
            writer_id,
            parent,
            ChainEventContent::Data {
                event_type: event_type.into(),
                payload,
            },
        )
    }

    /// Create an event for a source (flow entry point) with new correlation
    pub fn source_event(
        writer_id: WriterId,
        stage_name: impl Into<String>,
        content: ChainEventContent,
    ) -> ChainEvent {
        let mut event = Self::create_event(writer_id, content);
        event = event.with_new_correlation(stage_name);
        event
    }

    /// Internal helper to create base event
    fn create_event(writer_id: WriterId, content: ChainEventContent) -> ChainEvent {
        let mut event = ChainEvent {
            id: EventId::new(),
            writer_id,
            content,
            causality: CausalityContext::new(),
            flow_context: FlowContext::default(),
            processing_info: ProcessingContext::default(),
            intent: None,
            correlation_id: None,
            correlation_payload: None,
            runtime_context: None,
            observability: None,
        };

        // Update processing info timestamp
        event.processing_info.event_time = chrono::Utc::now().timestamp_millis() as u64;

        event
    }

    /// Create a data event from system component
    pub fn system_data_event(
        writer_id: WriterId,
        event_type: impl Into<String>,
        payload: Value,
    ) -> ChainEvent {
        Self::create_event(
            writer_id,
            ChainEventContent::Data {
                event_type: event_type.into(),
                payload,
            },
        )
    }

    /// Create an event with flow context
    pub fn create_with_context(
        writer_id: WriterId,
        content: ChainEventContent,
        flow_context: FlowContext,
    ) -> ChainEvent {
        let mut event = Self::create_event(writer_id, content);
        event.flow_context = flow_context;
        event
    }

    /// Create an event with observability context
    pub fn create_with_observability(
        writer_id: WriterId,
        content: ChainEventContent,
        observability: ObservabilityContext,
    ) -> ChainEvent {
        let mut event = Self::create_event(writer_id, content);
        event.observability = Some(observability);
        event
    }

    /// Create an event with intent
    pub fn create_with_intent(
        writer_id: WriterId,
        content: ChainEventContent,
        intent: IntentContext,
    ) -> ChainEvent {
        let mut event = Self::create_event(writer_id, content);
        event.intent = Some(intent);
        event
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_factory_creation() {
        let writer_id = WriterId::from(StageId::new());
        let event = ChainEventFactory::data_event(writer_id, "test.event", json!({"key": "value"}));

        assert_eq!(event.writer_id, writer_id);
        assert!(event.is_data());
        assert_eq!(event.event_type(), "test.event");
    }

    #[test]
    fn test_derived_event() {
        let writer_id = WriterId::from(StageId::new());
        let parent = ChainEventFactory::source_event(
            writer_id,
            "test_stage",
            ChainEventContent::Data {
                event_type: "parent.event".to_string(),
                payload: json!({"data": "parent"}),
            },
        );

        let child = ChainEventFactory::derived_data_event(
            writer_id,
            &parent,
            "child.event",
            json!({"data": "child"}),
        );

        assert_eq!(child.correlation_id, parent.correlation_id);
        assert_eq!(child.causality.parent_ids, vec![parent.id]);
    }

    #[test]
    fn test_flow_signals() {
        let writer_id = WriterId::from(StageId::new());

        let eof = ChainEventFactory::eof_event(writer_id, true);
        assert!(eof.is_eof());
        assert!(eof.is_control());

        let drain = ChainEventFactory::drain_event(writer_id);
        assert!(drain.is_control());
        assert_eq!(drain.event_type(), "control.drain");
    }
}
