//! Configuration for transform stages

use obzenflow_topology_services::stages::StageId;
use crate::stages::common::control_strategies::ControlEventStrategy;
use std::sync::Arc;

/// Configuration for a transform stage
#[derive(Clone)]
pub struct TransformConfig {
    /// Stage ID
    pub stage_id: StageId,
    
    /// Human-readable stage name
    pub stage_name: String,
    
    /// Flow name this transform belongs to
    pub flow_name: String,
    
    /// IDs of upstream stages this transform reads from
    pub upstream_stages: Vec<StageId>,
    
    /// Control event handling strategy (defaults to JonestownStrategy if not specified)
    pub control_strategy: Option<Arc<dyn ControlEventStrategy>>,
}