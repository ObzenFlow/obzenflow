// ChainEvent - the enhanced event structure for FlowState RS
// This is the application-level event that lives inside EventEnvelope.data

use serde::{Deserialize, Serialize};
use serde_json::Value;
use ulid::Ulid;

use crate::event_store::WriterId;
use crate::event_types::{
    CausalityInfo, EventExtensions, EventType, FlowContext, Intent, ProcessingInfo,
};

/// The definitive event structure for FlowState RS
/// Lives inside EventEnvelope.data as serialized bytes
/// Focuses on application concerns, NOT infrastructure concerns
/// Designed to support CHAIN maturity model levels 1-4
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainEvent {
    // === Identity (Application Level) ===
    /// Unique event identifier (for application-level references)
    pub ulid: Ulid,
    
    /// Which stage/service created this event (application identity)
    /// This identifies the logical stage, including worker suffix if parallel
    pub writer_id: WriterId,
    
    // === Application Layer ===
    /// Event type (e.g., "MarketDataFetched", "PriceAnalyzed")
    pub event_type: EventType,
    
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
    
    /// Extensions for future maturity levels (C3-4, H3-4, A2-4, I2-4)
    #[serde(default, skip_serializing_if = "EventExtensions::is_empty")]
    pub extensions: EventExtensions,
}

impl ChainEvent {
    /// Create a new ChainEvent with minimal fields (for backward compatibility)
    /// Note: This creates an incomplete event - you should use StageWriter for proper events
    pub fn new(event_type: &str, payload: Value) -> Self {
        ChainEvent {
            ulid: Ulid::new(),
            writer_id: WriterId::new(crate::topology::StageId::from_u32(0)),
            event_type: event_type.to_string(),
            payload,
            causality: CausalityInfo {
                parent_ids: Vec::new(),
            },
            flow_context: FlowContext {
                flow_name: "unknown".to_string(),
                flow_id: "unknown".to_string(),
                stage_name: "unknown".to_string(),
                stage_type: crate::event_types::StageType::Transform,
            },
            processing_info: ProcessingInfo {
                processed_by: "unknown".to_string(),
                processing_time_ms: 0,
                taxonomy: None,
                event_time_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                stage_position: None,
                outcome: crate::event_types::ProcessingOutcome::success(),
                is_boundary_event: None,
                correlation_id: None,
            },
            intent: None,
            extensions: EventExtensions::default(),
        }
    }
    
    /// Create a source completion event
    /// This is emitted by sources when they have no more data to produce
    pub fn source_complete(stage_id: crate::topology::StageId, natural: bool) -> Self {
        let now = chrono::Utc::now();
        Self::new(
            "flowstate.source.complete",
            serde_json::json!({
                "stage_id": stage_id.to_string(),
                "natural": natural,
                "timestamp": now.to_rfc3339(),
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