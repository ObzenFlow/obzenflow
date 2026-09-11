// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stage handle trait for pipeline coordination
//!
//! This trait defines the interface that all stage supervisors must implement
//! so the Pipeline FSM can coordinate them properly.

use obzenflow_core::{event::context::StageType, StageId};
use std::fmt;

/// Canonical error message used when the pipeline requests an immediate stage shutdown.
///
/// Stage supervisors treat this as an *intentional cancellation* signal (not a "failed" error),
/// and should emit `system.stage.cancelled` lifecycle events for observability/UI correctness.
pub const FORCE_SHUTDOWN_MESSAGE: &str = "Force shutdown requested";

/// Stable stop/cancel reason labels used across lifecycle events.
pub const STOP_REASON_USER_STOP: &str = "user_stop";
pub const STOP_REASON_TIMEOUT: &str = "stop_timeout";

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
    /// Execution was explicitly aborted.
    Aborted,
    /// Retained execution/publication failure with its original source.
    Execution(std::sync::Arc<dyn std::error::Error + Send + Sync>),
    /// Generic error
    Other(String),
}

impl fmt::Display for StageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StageError::InitializationFailed(msg) => {
                write!(f, "Stage initialization failed: {msg}")
            }
            StageError::EventSendFailed(msg) => {
                write!(f, "Failed to send event to stage: {msg}")
            }
            StageError::InvalidState(msg) => write!(f, "Invalid stage state: {msg}"),
            StageError::Timeout => write!(f, "Stage operation timed out"),
            StageError::HandlerFailure(err) => {
                write!(f, "Stage handler failure: {err:?}")
            }
            StageError::Aborted => write!(f, "Stage execution was aborted"),
            StageError::Execution(error) => write!(f, "Stage execution failed: {error}"),
            StageError::Other(msg) => write!(f, "Stage error: {msg}"),
        }
    }
}

impl std::error::Error for StageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Execution(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

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
    pub fn handler_failure(err: crate::stages::common::handler_error::HandlerError) -> Self {
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
///
/// Command methods retain a pending mailbox send until acceptance. Dropping an
/// unaccepted command future cancels that send; an accepted message belongs to
/// the receiving stage. Returning from a command does not certify the requested
/// lifecycle transition: the pipeline observes its committed system-journal fact.
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

    /// Wait for the stage task and every accepted publication to settle.
    /// A state observation or timeout does not establish resource completion.
    async fn wait_for_completion(&self) -> Result<(), StageError>;

    /// Abort the underlying supervisor task and join it deterministically.
    async fn abort_and_join(&self) -> Result<(), StageError>;

    /// Request immediate supervisor cancellation without waiting for its join.
    /// Used by Runtime lifetime guards; this does not claim stage completion.
    /// Implementations must be idempotent and non-blocking.
    #[doc(hidden)]
    fn request_abort(&self);

    #[doc(hidden)]
    fn publish_pipeline_control(
        &self,
        _journal: std::sync::Arc<
            dyn obzenflow_core::journal::Journal<obzenflow_core::event::ChainEvent>,
        >,
        _event: obzenflow_core::event::ChainEvent,
    ) -> Result<(), StageError> {
        Err(StageError::InvalidState(
            "stage has no retained publication writer".into(),
        ))
    }
}

/// Type-erased stage handle for pipeline storage
pub type BoxedStageHandle = Box<dyn StageHandle>;
