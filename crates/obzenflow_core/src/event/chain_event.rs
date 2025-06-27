// ChainEvent - the enhanced event structure for FlowState RS
// This is the application-level event that lives inside EventEnvelope.data

use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::event::event_id::EventId;
use crate::journal::writer_id::WriterId;
use crate::event::causality::CausalityInfo;
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
}

impl ChainEvent {
    // Control event types that flow through the journal
    /// Standard event type for EOF events
    pub const EOF_EVENT_TYPE: &'static str = "flowstate.eof";
    /// Standard event type for watermark events (future)
    pub const WATERMARK_EVENT_TYPE: &'static str = "flowstate.watermark";
    /// Standard event type for barrier events (future)
    pub const BARRIER_EVENT_TYPE: &'static str = "flowstate.barrier";
    
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
    
    /// Check if this event is an EOF event
    pub fn is_eof(&self) -> bool {
        self.event_type == Self::EOF_EVENT_TYPE
    }
    
    /// Check if this is a control event (vs data event)
    pub fn is_control(&self) -> bool {
        self.event_type.starts_with("flowstate.")
    }
    
    /// Helper for stage processing loops - returns control event type if this is a control event
    pub fn as_control_type(&self) -> Option<&str> {
        if self.is_control() {
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
}
