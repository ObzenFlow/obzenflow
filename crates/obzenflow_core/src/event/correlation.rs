//! Event correlation for flow-level metrics
//!
//! Enables tracking individual events from source entry to sink exit
//! for accurate end-to-end latency and flow metrics.

use serde::{Deserialize, Serialize};
use ulid::Ulid;
use crate::event::event_id::EventId;

/// Unique identifier for correlating events through a flow
pub type CorrelationId = Ulid;

/// Generate a new correlation ID
pub fn new_correlation_id() -> CorrelationId {
    Ulid::new()
}

/// Payload that carries entry metadata through the flow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrelationPayload {
    /// Timestamp when event entered the flow (nanos since epoch)
    pub entry_time_ns: u64,
    
    /// Stage where event entered the flow
    pub entry_stage: String,
    
    /// Original event ID at flow entry
    pub entry_event_id: EventId,
    
    /// Optional metadata for future extensibility
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl CorrelationPayload {
    /// Create a new correlation payload for flow entry
    pub fn new(entry_stage: impl Into<String>, entry_event_id: EventId) -> Self {
        Self {
            entry_time_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64,
            entry_stage: entry_stage.into(),
            entry_event_id,
            metadata: None,
        }
    }
    
    /// Calculate latency from entry time to now
    pub fn calculate_latency(&self) -> std::time::Duration {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        
        let latency_ns = now_ns.saturating_sub(self.entry_time_ns);
        std::time::Duration::from_nanos(latency_ns)
    }
    
    /// Get entry time as SystemTime
    pub fn entry_time(&self) -> std::time::SystemTime {
        std::time::UNIX_EPOCH + std::time::Duration::from_nanos(self.entry_time_ns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_correlation_payload_creation() {
        let event_id = EventId::new();
        let payload = CorrelationPayload::new("http_source", event_id);
        
        assert_eq!(payload.entry_stage, "http_source");
        assert_eq!(payload.entry_event_id, event_id);
        assert!(payload.entry_time_ns > 0);
        assert!(payload.metadata.is_none());
    }
    
    #[test]
    fn test_latency_calculation() {
        let event_id = EventId::new();
        let payload = CorrelationPayload::new("test_source", event_id);
        
        // Sleep a bit to ensure measurable latency
        std::thread::sleep(std::time::Duration::from_millis(10));
        
        let latency = payload.calculate_latency();
        assert!(latency.as_millis() >= 10);
    }
}