//! Configuration for stateful stages

use crate::stages::common::control_strategies::ControlEventStrategy;
use obzenflow_core::StageId;
use std::sync::Arc;

/// Configuration for a stateful stage
#[derive(Clone)]
pub struct StatefulConfig {
    /// Stage ID
    pub stage_id: StageId,

    /// Human-readable stage name
    pub stage_name: String,

    /// Flow name this stateful stage belongs to
    pub flow_name: String,

    /// IDs of upstream stages this stateful stage reads from
    pub upstream_stages: Vec<StageId>,

    /// Control event handling strategy (defaults to JonestownStrategy if not specified)
    pub control_strategy: Option<Arc<dyn ControlEventStrategy>>,
}
