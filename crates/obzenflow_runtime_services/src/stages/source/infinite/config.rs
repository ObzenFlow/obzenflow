//! Configuration for infinite source stages

use crate::stages::source::strategies::SourceControlStrategy;
use obzenflow_core::StageId;
use std::sync::Arc;

/// Configuration for an infinite source stage
#[derive(Clone, Debug)]
pub struct InfiniteSourceConfig {
    /// Stage ID
    pub stage_id: StageId,

    /// Human-readable stage name
    pub stage_name: String,

    /// Flow name this source belongs to
    pub flow_name: String,

    /// Optional source control strategy for shutdown / FlowControl decisions.
    /// When not provided, the builder will default to JonestownSourceStrategy.
    pub control_strategy: Option<Arc<dyn SourceControlStrategy>>,
}
