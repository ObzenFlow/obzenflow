//! Flow context types
//!
//! Tracks which flow and stage an event belongs to.

use serde::{Deserialize, Serialize};
use crate::StageId;
use super::stage_type::StageType;

/// Context about which flow and stage processed an event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowContext {
    /// Name of the flow (e.g., "price-analysis")
    pub flow_name: String,
    
    /// Instance ID of the flow run
    pub flow_id: String,
    
    /// Name of the stage that processed this event
    pub stage_name: String,

    /// ID of the stage that processed this event
    pub stage_id: StageId,
    
    /// Type of the stage
    pub stage_type: StageType,
}


impl FlowContext {
    /// Create a new FlowContext with stage name
    pub fn new(stage_name: impl Into<String>, stage_id: StageId) -> Self {
        Self {
            flow_name: "unknown".to_string(),
            flow_id: "unknown".to_string(),
            stage_name: stage_name.into(),
            stage_id,
            stage_type: StageType::Transform,
        }
    }
}

impl Default for FlowContext {
    fn default() -> Self {
        Self {
            flow_name: "unknown".to_string(),
            flow_id: "unknown".to_string(),
            stage_name: "unknown".to_string(),
            stage_id: StageId::default(),
            stage_type: StageType::Transform,
        }
    }
}