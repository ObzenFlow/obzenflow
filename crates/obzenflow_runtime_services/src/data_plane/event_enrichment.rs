//! Event enrichment utilities
//! 
//! Runtime layer adds metadata to events for tracking and debugging

use obzenflow_core::{ChainEvent, WriterId, EventId};
use serde_json::json;
use std::time::Instant;

/// Runtime event enricher - adds metadata without coupling to stages
pub struct EventEnricher {
    flow_name: String,
    stage_name: String,
    writer_id: WriterId,
}

impl EventEnricher {
    pub fn new(
        flow_name: String,
        stage_name: String,
        writer_id: WriterId,
    ) -> Self {
        Self {
            flow_name,
            stage_name,
            writer_id,
        }
    }
    
    /// Enrich an event with runtime metadata
    pub fn enrich(&self, mut event: ChainEvent) -> ChainEvent {
        // Add flow context
        // Ensure payload is an object
        if !event.payload.is_object() {
            event.payload = json!({"value": event.payload});
        }
        
        event.payload["_meta"] = json!({
            "flow_name": self.flow_name,
            "stage_name": self.stage_name,
            "writer_id": self.writer_id.to_string(),
            "enriched_at": chrono::Utc::now().to_rfc3339(),
        });
        
        event
    }
    
    /// Add processing info to an event
    pub fn add_processing_info(&self, event: &mut ChainEvent, start_time: Instant) {
        let duration_ms = start_time.elapsed().as_millis() as u64;
        
        if let Some(meta) = event.payload.get_mut("_meta").and_then(|v| v.as_object_mut()) {
            meta.insert("processing_duration_ms".to_string(), json!(duration_ms));
        }
    }
    
    /// Mark an event as a boundary event (start/end of flow)
    pub fn mark_boundary(&self, event: &mut ChainEvent, boundary_type: BoundaryType) {
        if let Some(meta) = event.payload.get_mut("_meta").and_then(|v| v.as_object_mut()) {
            meta.insert("boundary".to_string(), json!(boundary_type));
        }
    }
}

/// Types of boundary events
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BoundaryType {
    Start,
    End,
    Error,
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_topology_services::stages::StageId;
    
    #[test]
    fn test_event_enrichment() {
        let enricher = EventEnricher::new(
            "test_flow".to_string(),
            "test_stage".to_string(),
            WriterId::new(),
        );
        
        let event = ChainEvent::new(EventId::new(), WriterId::new(), "test_event", json!({"value": 42}));
        let enriched = enricher.enrich(event);
        
        assert!(enriched.payload["_meta"].is_object());
        assert_eq!(enriched.payload["_meta"]["flow_name"], "test_flow");
        assert_eq!(enriched.payload["_meta"]["stage_name"], "test_stage");
    }
}