//! Stage handle trait for pipeline coordination
//!
//! This trait defines the interface that all stage supervisors must implement
//! so the Pipeline FSM can coordinate them properly.

use obzenflow_core::{event::context::StageType, StageId};
use std::fmt;

/// Error type for stage operations
#[derive(Debug, Clone)]
pub enum StageError {
    /// Failed to initialize stage
    InitializationFailed(String),
    /// Failed to send event to stage
    EventSendFailed(String),
    /// Stage is in invalid state for operation
    InvalidState(String),
    /// Operation timed out
    Timeout,
    /// Handler-level failure that the supervisor has deemed stage-fatal.
    ///
    /// This wraps a `HandlerError` from stage logic so the pipeline FSM can
    /// distinguish handler failures from other coordination errors.
    HandlerFailure(crate::stages::common::handler_error::HandlerError),
    /// Generic error
    Other(String),
}

impl fmt::Display for StageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StageError::InitializationFailed(msg) => {
                write!(f, "Stage initialization failed: {}", msg)
            }
            StageError::EventSendFailed(msg) => write!(f, "Failed to send event to stage: {}", msg),
            StageError::InvalidState(msg) => write!(f, "Invalid stage state: {}", msg),
            StageError::Timeout => write!(f, "Stage operation timed out"),
            StageError::HandlerFailure(err) => {
                write!(f, "Stage handler failure: {:?}", err)
            }
            StageError::Other(msg) => write!(f, "Stage error: {}", msg),
        }
    }
}

impl std::error::Error for StageError {}

impl From<String> for StageError {
    fn from(s: String) -> Self {
        StageError::Other(s)
    }
}

impl From<&str> for StageError {
    fn from(s: &str) -> Self {
        StageError::Other(s.to_string())
    }
}

impl StageError {
    /// Helper to construct a handler failure variant from a HandlerError.
    pub fn handler_failure(
        err: crate::stages::common::handler_error::HandlerError,
    ) -> Self {
        StageError::HandlerFailure(err)
    }
}

/// Generic stage events that Pipeline uses for coordination
#[derive(Debug, Clone)]
pub enum StageEvent {
    Initialize,
    Ready,
    Start,
    BeginDrain,
    ForceShutdown,
    Shutdown,
    Error(String),
}

/// A handle to a stage that the Pipeline FSM can use for coordination
///
/// This trait exposes exactly what the Pipeline needs:
/// - Identity (stage_id, name)
/// - Lifecycle control (initialize, start, drain)
/// - State queries (is_ready, is_drained)
#[async_trait::async_trait]
pub trait StageHandle: Send + Sync {
    /// Get the stage ID
    fn stage_id(&self) -> StageId;

    /// Get the stage name
    fn stage_name(&self) -> &str;

    /// Get the stage type (for pipeline decisions)
    fn stage_type(&self) -> StageType;

    /// Initialize the stage (allocate resources, create subscriptions)
    async fn initialize(&self) -> Result<(), StageError>;

    /// Move the stage into a ready state (sources: WaitingForGun; others may no-op)
    async fn ready(&self) -> Result<(), StageError>;

    /// Start the stage (only sources need this, others can no-op)
    async fn start(&self) -> Result<(), StageError>;

    /// Send an event to the stage FSM
    async fn send_event(&self, event: StageEvent) -> Result<(), StageError>;

    /// Begin draining the stage
    async fn begin_drain(&self) -> Result<(), StageError>;

    /// Check if the stage is ready
    fn is_ready(&self) -> bool;

    /// Check if the stage is drained
    fn is_drained(&self) -> bool;

    /// Force shutdown
    async fn force_shutdown(&self) -> Result<(), StageError>;

    /// Wait for the stage to complete its work and reach a terminal state.
    ///
    /// Implementations should typically:
    /// - Observe the underlying supervisor state
    /// - Treat terminal states (e.g., Drained/Failed) as completion
    /// - Respect the same shutdown timeout used by the pipeline cleanup path
    async fn wait_for_completion(&self) -> Result<(), StageError>;
}

/// Type-erased stage handle for pipeline storage
pub type BoxedStageHandle = Box<dyn StageHandle>;
