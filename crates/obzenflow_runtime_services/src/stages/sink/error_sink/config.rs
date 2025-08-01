//! Configuration for error sink stage

use obzenflow_core::StageId;

/// Configuration for the error sink stage
#[derive(Clone, Debug)]
pub struct ErrorSinkConfig {
    /// Stage ID
    pub stage_id: StageId,
    
    /// Human-readable stage name
    pub stage_name: String,
    
    /// Flow name this sink belongs to
    pub flow_name: String,
    
    /// All stage IDs in the flow (error sink subscribes to all error journals)
    pub all_stage_ids: Vec<StageId>,
    
    /// Deduplication window in seconds (default: 300s)
    pub dedupe_window_secs: u64,
    
    /// Maximum events to process in a single batch
    pub batch_size: usize,
    
    /// Rate limit for error events (events per second, 0 = unlimited)
    pub rate_limit: f64,
}