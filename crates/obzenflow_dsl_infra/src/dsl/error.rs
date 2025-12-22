use thiserror::Error;

use obzenflow_topology::TopologyError;

/// Structured error type for failures during flow construction
#[derive(Debug, Error)]
pub enum FlowBuildError {
    #[error("Topology validation failed: {0}")]
    TopologyValidationFailed(#[source] TopologyError),

    #[error("Journal factory failed: {0}")]
    JournalFactoryFailed(String),

    #[error("Stage resources build failed: {0}")]
    StageResourcesFailed(String),

    #[error("Failed to create stage '{stage_name}': {message}")]
    StageCreationFailed { stage_name: String, message: String },

    #[error("Pipeline build failed: {0}")]
    PipelineBuildFailed(String),
}

impl From<FlowBuildError> for String {
    fn from(err: FlowBuildError) -> Self {
        err.to_string()
    }
}
