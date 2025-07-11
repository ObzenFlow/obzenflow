//! Configuration for infinite source stages

use obzenflow_topology_services::stages::StageId;

/// Configuration for an infinite source stage
#[derive(Clone, Debug)]
pub struct InfiniteSourceConfig {
    /// Stage ID
    pub stage_id: StageId,
    
    /// Human-readable stage name
    pub stage_name: String,
    
    /// Flow name this source belongs to
    pub flow_name: String,
}