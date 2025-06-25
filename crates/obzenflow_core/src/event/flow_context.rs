//! Flow context types
//!
//! Tracks which flow and stage an event belongs to.

use serde::{Deserialize, Serialize};

/// Context about which flow and stage processed an event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowContext {
    /// Name of the flow (e.g., "price-analysis")
    pub flow_name: String,
    
    /// Instance ID of the flow run
    pub flow_id: String,
    
    /// Name of the stage that processed this event
    pub stage_name: String,
    
    /// Type of the stage
    pub stage_type: StageType,
}

/// Simple stage type classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StageType {
    Source,
    Transform,
    Sink,
}

impl Default for FlowContext {
    fn default() -> Self {
        Self {
            flow_name: "unknown".to_string(),
            flow_id: "unknown".to_string(),
            stage_name: "unknown".to_string(),
            stage_type: StageType::Transform,
        }
    }
}