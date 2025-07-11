// ChainEvent - the enhanced event structure for FlowState RS
// This is the application-level event that lives inside EventEnvelope.data

use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::event::event_id::EventId;
use crate::journal::writer_id::WriterId;
use crate::event::causality::CausalityInfo;
use crate::event::correlation::{CorrelationId, CorrelationPayload};
use crate::event::flow_context::FlowContext;
use crate::event::intent::Intent;
use crate::event::processing_info::ProcessingInfo;

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
    /// This identifies the logical stage, including worker suffix if parallel
    pub writer_id: WriterId,

    // === Application Layer ===
    /// Event type (e.g., "MarketDataFetched", "PriceAnalyzed")
    pub event_type: String,

    /// Event content (application-specific data)
    pub payload: Value,

    // === Integration Layer (FLOWIP-007) ===
    /// Causality tracking
    pub causality: CausalityInfo,

    /// Flow and stage context
    pub flow_context: FlowContext,

    /// Processing and monitoring metadata
    pub processing_info: ProcessingInfo,

    // === CHAIN Maturity Support ===
    /// Explicit intent (I1 maturity minimum)
    pub intent: Option<Intent>,
    
    // === Flow-Level Correlation (FLOWIP-054d) ===
    /// Correlation ID that flows through all derived events
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<CorrelationId>,
    
    /// Metadata about when/where this correlation entered the flow
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_payload: Option<CorrelationPayload>,
}

impl ChainEvent {
    // Control event constants
    /// Prefix for all control event types
    pub const CONTROL_EVENT_PREFIX: &'static str = "control.";
    
    // System event constants
    /// Prefix for all system event types
    pub const SYSTEM_EVENT_PREFIX: &'static str = "system.";
    
    // System event types for stage lifecycle
    /// Stage has started running
    pub const SYSTEM_STAGE_RUNNING: &'static str = "system.stage.running";
    /// Stage is draining
    pub const SYSTEM_STAGE_DRAINING: &'static str = "system.stage.draining";
    /// Stage has drained
    pub const SYSTEM_STAGE_DRAINED: &'static str = "system.stage.drained";
    /// Stage has completed successfully
    pub const SYSTEM_STAGE_COMPLETED: &'static str = "system.stage.completed";
    /// Stage has failed
    pub const SYSTEM_STAGE_FAILED: &'static str = "system.stage.failed";
    
    // System event types for pipeline lifecycle
    /// All stages in pipeline have completed
    pub const SYSTEM_PIPELINE_ALL_STAGES_COMPLETED: &'static str = "system.pipeline.all_stages_completed";
    /// Pipeline is starting to drain
    pub const SYSTEM_PIPELINE_DRAIN: &'static str = "system.pipeline.drain";
    /// Pipeline has completed
    pub const SYSTEM_PIPELINE_COMPLETED: &'static str = "system.pipeline.completed";
    
    // System event types for metrics
    /// Metrics aggregator is ready
    pub const SYSTEM_METRICS_READY: &'static str = "system.metrics.ready";
    /// Metrics aggregator should drain
    pub const SYSTEM_METRICS_DRAIN: &'static str = "system.metrics.drain";
    /// Metrics aggregator has drained
    pub const SYSTEM_METRICS_DRAINED: &'static str = "system.metrics.drained";
    
    // System event types for errors
    /// Generic system error
    pub const SYSTEM_ERROR: &'static str = "system.error";
    
    // Control event types that flow through the journal
    /// Standard event type for EOF events
    pub const EOF_EVENT_TYPE: &'static str = "control.eof";
    /// Standard event type for watermark events (future)
    pub const WATERMARK_EVENT_TYPE: &'static str = "control.watermark";
    /// Standard event type for checkpoint events
    pub const CHECKPOINT_EVENT_TYPE: &'static str = "control.checkpoint";
    /// Standard event type for drain events
    pub const DRAIN_EVENT_TYPE: &'static str = "control.drain";
    
    // Middleware control events (FLOWIP-056-666)
    /// Middleware state transitions (e.g., circuit breaker open/closed)
    pub const CONTROL_MIDDLEWARE_STATE: &'static str = "control.middleware.state";
    /// Middleware periodic summaries (aggregated stats)
    pub const CONTROL_MIDDLEWARE_SUMMARY: &'static str = "control.middleware.summary";
    /// Middleware anomaly detection events
    pub const CONTROL_MIDDLEWARE_ANOMALY: &'static str = "control.middleware.anomaly";
    
    // Metrics control events (FLOWIP-056-666)
    /// Stage state metrics (saturation: queue depth, in-flight)
    pub const CONTROL_METRICS_STATE: &'static str = "control.metrics.state";
    /// Stage resource metrics (utilization: CPU, memory)
    pub const CONTROL_METRICS_RESOURCE: &'static str = "control.metrics.resource";
    /// Custom metrics from user code
    /// TODO(FLOWIP-053a): Reserved for future custom metrics implementation
    /// See docs/flowip-proposals/FLOWIP-053a-custom-metrics.md for design
    pub const CONTROL_METRICS_CUSTOM: &'static str = "control.metrics.custom";
    /// Anomaly detection events
    pub const CONTROL_METRICS_ANOMALY: &'static str = "control.metrics.anomaly";
    
    /// Create a new ChainEvent with minimal fields (for backward compatibility)
    /// Note: This creates an incomplete event - you should use StageWriter for proper events
    pub fn new(
        id: EventId,
        writer_id: WriterId,
        event_type: &str, 
        payload: Value
    ) -> Self {
        ChainEvent {
            id,
            writer_id,
            event_type: event_type.to_string(),
            payload,
            causality: CausalityInfo::new(),
            flow_context: FlowContext::default(),
            processing_info: ProcessingInfo::default(),
            intent: None,
            correlation_id: None,
            correlation_payload: None,
        }
    }

    /// Create an EOF event - used by ALL stages when they're done processing
    /// WriterId already identifies the stage/worker, no need for stage_id
    pub fn eof(
        id: EventId,
        writer_id: WriterId,
        natural: bool
    ) -> Self {
        Self::new(
            id,
            writer_id,
            Self::EOF_EVENT_TYPE,
            serde_json::json!({
                "natural": natural,  // true = natural completion, false = drain/error
                "timestamp": Self::current_timestamp(),
            })
        )
    }
    
    /// Create a control event with the given type and payload
    /// This is used by middleware and stages to emit metrics/state events
    pub fn control(
        event_type: &str,
        payload: Value
    ) -> Self {
        // Generate IDs - these will be replaced by StageWriter when actually emitted
        let id = EventId::new();
        let writer_id = WriterId::new();
        
        Self::new(
            id,
            writer_id,
            event_type,
            payload
        )
    }
    
    /// Check if this event is an EOF event
    pub fn is_eof(&self) -> bool {
        self.event_type == Self::EOF_EVENT_TYPE
    }
    
    /// Check if this is a control event (vs data event)
    pub fn is_control(&self) -> bool {
        self.event_type.starts_with(Self::CONTROL_EVENT_PREFIX)
    }
    
    /// Check if this is a system event
    pub fn is_system(&self) -> bool {
        self.event_type.starts_with(Self::SYSTEM_EVENT_PREFIX)
    }
    
    /// Helper for stage processing loops - returns control event type if this is a control event
    pub fn as_control_type(&self) -> Option<&str> {
        if self.is_control() {
            Some(&self.event_type)
        } else {
            None
        }
    }
    
    /// Helper for system event processing - returns system event type if this is a system event
    pub fn as_system_type(&self) -> Option<&str> {
        if self.is_system() {
            Some(&self.event_type)
        } else {
            None
        }
    }
    
    /// Get current timestamp in milliseconds since epoch
    fn current_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
    
    // === Correlation Support (FLOWIP-054d) ===
    
    /// Create correlation for a source event (flow entry)
    pub fn with_new_correlation(mut self, stage_name: impl Into<String>) -> Self {
        use crate::event::correlation::new_correlation_id;
        
        let correlation_id = new_correlation_id();
        self.correlation_id = Some(correlation_id);
        self.correlation_payload = Some(CorrelationPayload::new(stage_name, self.id));
        self
    }
    
    /// Propagate correlation from parent event to derived event
    pub fn with_correlation_from(mut self, parent: &ChainEvent) -> Self {
        self.correlation_id = parent.correlation_id.clone();
        self.correlation_payload = parent.correlation_payload.clone();
        self
    }
    
    /// Check if this event has correlation info
    pub fn has_correlation(&self) -> bool {
        self.correlation_id.is_some()
    }
    
    /// Calculate latency if this event has correlation payload
    pub fn correlation_latency(&self) -> Option<std::time::Duration> {
        self.correlation_payload.as_ref().map(|p| p.calculate_latency())
    }
}
