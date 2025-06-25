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

    /// Create a source completion event
    /// This is emitted by sources when they have no more data to produce
    pub fn source_complete(
        id: EventId,
        writer_id: WriterId,
        stage_id: &str, 
        natural: bool
    ) -> Self {
        Self::new(
            id,
            writer_id,
            "flowstate.source.complete",
            serde_json::json!({
                "stage_id": stage_id,
                "natural": natural,
                "timestamp": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                "final": true,  // Indicates no more events will follow
                "message": if natural {
                    "Source naturally completed"
                } else {
                    "Source stopped due to shutdown"
                }
            })
        )
    }
}
