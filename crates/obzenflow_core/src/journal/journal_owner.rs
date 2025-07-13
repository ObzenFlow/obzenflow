//! Journal ownership types
//!
//! Every journal must have an owner for identification and debugging

use crate::id::{StageId, PipelineId, MetricsId};
use serde::{Deserialize, Serialize};

/// Identifies who owns a journal
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JournalOwner {
    /// Pipeline owns this journal (typically the control journal)
    Pipeline { 
        pipeline_id: PipelineId,
    },
    
    /// Stage owns this journal (for stage-local data events)
    Stage { 
        stage_id: StageId,
    },
    
    /// Metrics aggregator owns this journal
    Metrics {
        metrics_id: MetricsId,
    },
}

impl JournalOwner {
    /// Create a pipeline owner
    pub fn pipeline(pipeline_id: PipelineId) -> Self {
        Self::Pipeline { pipeline_id }
    }
    
    /// Create a stage owner
    pub fn stage(stage_id: StageId) -> Self {
        Self::Stage { stage_id }
    }
    
    /// Create a metrics owner
    pub fn metrics(metrics_id: MetricsId) -> Self {
        Self::Metrics { metrics_id }
    }
    
    /// Get a unique string representation for file/directory naming
    pub fn as_path_component(&self) -> String {
        match self {
            Self::Pipeline { pipeline_id } => format!("pipeline_{}", pipeline_id),
            Self::Stage { stage_id } => format!("stage_{}", stage_id.as_u64()),
            Self::Metrics { metrics_id } => format!("metrics_{}", metrics_id),
        }
    }
}

impl std::fmt::Display for JournalOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pipeline { pipeline_id } => write!(f, "Pipeline({})", pipeline_id),
            Self::Stage { stage_id } => write!(f, "Stage({})", stage_id),
            Self::Metrics { metrics_id } => write!(f, "Metrics({})", metrics_id),
        }
    }
}