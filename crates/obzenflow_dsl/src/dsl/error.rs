// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use thiserror::Error;

use obzenflow_adapters::middleware::MiddlewareFactoryError;
use obzenflow_topology::TopologyError;

#[derive(Debug, Error)]
pub enum StageCreationError {
    #[error(transparent)]
    MiddlewareFactory(#[from] MiddlewareFactoryError),

    #[error("{0}")]
    Message(String),
}

pub type StageCreationResult<T> = Result<T, StageCreationError>;

impl From<String> for StageCreationError {
    fn from(message: String) -> Self {
        Self::Message(message)
    }
}

impl From<&str> for StageCreationError {
    fn from(message: &str) -> Self {
        Self::Message(message.to_string())
    }
}

/// Structured error type for failures during flow construction
#[derive(Debug, Error)]
pub enum FlowBuildError {
    #[error("Topology validation failed: {0}")]
    TopologyValidationFailed(#[source] TopologyError),

    #[error("Unsupported cycle topology: {0}")]
    UnsupportedCycleTopology(String),

    #[error("Journal factory failed: {0}")]
    JournalFactoryFailed(String),

    #[error("Stage resources build failed: {0}")]
    StageResourcesFailed(String),

    #[error("Failed to create stage '{stage_name}': {source}")]
    StageCreationFailed {
        stage_name: String,
        #[source]
        source: StageCreationError,
    },

    #[error("Pipeline build failed: {0}")]
    PipelineBuildFailed(String),

    #[error("Duplicate stage descriptor name '{name}' (used by '{first_var}' and '{second_var}')")]
    DuplicateStageName {
        name: String,
        first_var: String,
        second_var: String,
    },
}

impl From<FlowBuildError> for String {
    fn from(err: FlowBuildError) -> Self {
        err.to_string()
    }
}
