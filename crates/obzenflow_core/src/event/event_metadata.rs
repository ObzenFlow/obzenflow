//! Core event metadata types
//!
//! These are fundamental domain concepts that describe events
//! in the system. They belong in core because they define the
//! essential properties of events, not how they're stored.

use serde::{Deserialize, Serialize};
use ulid::Ulid;

// ===== Core identification =====
pub type EventId = Ulid;
pub type WriterId = Ulid;
pub type CorrelationId = String;

/// Generate a new correlation ID
pub fn new_correlation_id() -> CorrelationId {
    Ulid::new().to_string()
}

// ===== Causality tracking =====

/// Tracks causal relationships between events
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CausalityInfo {
    /// Parent events that caused this one
    pub parent_ids: Vec<EventId>,
}

// ===== Processing metadata =====

/// Core processing information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingInfo {
    /// Which component processed this event
    pub processed_by: String,
    
    /// When this event was created
    pub event_time_ms: u64,
    
    /// Processing duration in milliseconds
    pub processing_time_ms: u64,
    
    /// Correlation ID for distributed tracing
    pub correlation_id: Option<CorrelationId>,
}

impl Default for ProcessingInfo {
    fn default() -> Self {
        Self {
            processed_by: String::new(),
            event_time_ms: 0,
            processing_time_ms: 0,
            correlation_id: None,
        }
    }
}

// ===== Flow context =====

/// Identifies where in the flow this event belongs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowContext {
    /// Name of the flow
    pub flow_name: String,
    
    /// Instance ID of this flow execution
    pub flow_id: String,
    
    /// Current stage name
    pub stage_name: String,
}

impl Default for FlowContext {
    fn default() -> Self {
        Self {
            flow_name: String::new(),
            flow_id: String::new(),
            stage_name: String::new(),
        }
    }
}