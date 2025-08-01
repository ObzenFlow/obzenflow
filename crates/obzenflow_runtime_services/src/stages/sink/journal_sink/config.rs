//! Configuration for journal sink stages

use obzenflow_core::StageId;

/// Configuration for a journal sink stage
#[derive(Clone, Debug)]
pub struct JournalSinkConfig {
    /// Stage ID
    pub stage_id: StageId,
    
    /// Human-readable stage name
    pub stage_name: String,
    
    /// Flow name this sink belongs to
    pub flow_name: String,
    
    /// IDs of upstream stages this sink consumes from
    pub upstream_stages: Vec<StageId>,
    
    /// Buffer size for event batching (optional optimization)
    pub buffer_size: Option<usize>,
    
    /// Flush interval in milliseconds (optional optimization)
    pub flush_interval_ms: Option<u64>,
}