//! Error types for runtime services
//!
//! Provides detailed error information with context for better debugging.

use thiserror::Error;
use obzenflow_topology_services::stages::StageId;

/// Errors that can occur in the message bus
#[derive(Error, Debug)]
pub enum MessageBusError {
    #[error("Failed to send stage command - no stages are listening (are stages initialized?)")]
    NoStageReceivers,
}

/// Errors that can occur in the pipeline supervisor
#[derive(Error, Debug)]
pub enum PipelineSupervisorError {
    #[error("Failed to initialize pipeline FSM")]
    FsmInitFailed(#[source] Box<dyn std::error::Error + Send + Sync>),
    
    #[error("Failed to create stage {stage_id}")]
    StageCreationFailed {
        stage_id: StageId,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    
    #[error("Message bus error during {operation}")]
    MessageBus {
        operation: String,
        #[source]
        source: MessageBusError,
    },
    
    #[error("Pipeline FSM error: {message}")]
    PipelineFsmError {
        message: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    
    #[error("Journal error: {0}")]
    JournalError(String),
}

/// Errors that can occur during flow execution
#[derive(Error, Debug)]
pub enum FlowError {
    #[error("Failed to materialize pipeline")]
    MaterializationFailed(#[from] PipelineSupervisorError),
    
    #[error("Failed to run pipeline - supervisor not initialized")]
    SupervisorNotInitialized,
    
    #[error("Failed to send command to pipeline FSM")]
    CommandSendFailed(#[from] MessageBusError),
    
    #[error("Pipeline execution failed")]
    ExecutionFailed(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Result type alias for runtime operations
pub type RuntimeResult<T> = Result<T, FlowError>;