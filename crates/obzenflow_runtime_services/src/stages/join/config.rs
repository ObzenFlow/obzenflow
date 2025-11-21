//! Join stage configuration

use crate::stages::common::control_strategies::ControlEventStrategy;
use obzenflow_core::StageId;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Configuration for a join stage
#[derive(Clone, Serialize, Deserialize)]
pub struct JoinConfig {
    /// Stage ID for this join
    pub stage_id: StageId,

    /// Unique name for this join stage
    pub stage_name: String,

    /// Flow name
    pub flow_name: String,

    /// Stage ID for the reference upstream
    pub reference_source_id: StageId,

    /// Stage ID for the stream upstream
    pub stream_source_id: StageId,

    /// Control strategy for handling control events
    #[serde(skip)]
    pub control_strategy: Option<Arc<dyn ControlEventStrategy>>,

    /// List of upstream stage IDs
    pub upstream_stages: Vec<StageId>,
}

impl JoinConfig {
    pub fn new(
        stage_id: StageId,
        stage_name: impl Into<String>,
        flow_name: impl Into<String>,
        reference_source_id: StageId,
        stream_source_id: StageId,
    ) -> Self {
        Self {
            stage_id,
            stage_name: stage_name.into(),
            flow_name: flow_name.into(),
            reference_source_id,
            stream_source_id,
            control_strategy: None,
            upstream_stages: vec![reference_source_id, stream_source_id],
        }
    }
}
