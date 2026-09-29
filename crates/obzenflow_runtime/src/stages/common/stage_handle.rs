// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stage handle trait for pipeline coordination
//!
//! This trait defines the interface that all stage supervisors must implement
//! so the Pipeline FSM can coordinate them properly.

pub use super::stage_lifecycle::StageMilestone;
use super::stage_lifecycle::{LifecycleExit, LifecycleFailure, MilestoneAck, StageSnapshot};

#[derive(Clone, Debug)]
pub struct StageAck {
    pub stage_id: obzenflow_core::StageId,
    pub milestone: StageMilestone,
    pub snapshot: StageSnapshot,
}

impl StageAck {
    pub fn from_result(stage_id: obzenflow_core::StageId, result: MilestoneAck) -> Self {
        Self {
            stage_id,
            milestone: result.milestone,
            snapshot: result.snapshot,
        }
    }
}

#[derive(Clone, Debug)]
pub struct StageFailure {
    pub stage_id: obzenflow_core::StageId,
    pub cause: StageError,
    pub snapshot: StageSnapshot,
}

impl StageFailure {
    pub fn from_result(stage_id: obzenflow_core::StageId, result: LifecycleFailure) -> Self {
        Self {
            stage_id,
            cause: result.cause,
            snapshot: result.snapshot,
        }
    }
}

#[derive(Clone, Debug)]
pub struct StageExit {
    pub stage_id: obzenflow_core::StageId,
    pub outcome: LifecycleExit,
}

use obzenflow_core::{event::context::StageType, StageId};
use std::fmt;

/// Canonical error message used when the pipeline requests an immediate stage shutdown.
///
/// Stage supervisors treat this as an *intentional cancellation* signal (not a "failed" error),
/// and should author a canonical `StageLifecycleFact::Cancelled` for observers.
pub const FORCE_SHUTDOWN_MESSAGE: &str = "Force shutdown requested";

/// Stable stop/cancel reason labels used across lifecycle events.
pub const STOP_REASON_USER_STOP: &str = "user_stop";
pub const STOP_REASON_TIMEOUT: &str = "stop_timeout";

/// Preserve the error text while distinguishing the existing cancellation
/// protocol from an unexpected failure delivered after stage termination.
pub(crate) fn discarded_control_details(
    error: Option<&str>,
) -> (
    obzenflow_core::event::CommandDiscardDisposition,
    Option<String>,
) {
    use obzenflow_core::event::CommandDiscardDisposition;
    let disposition = match error {
        None | Some(FORCE_SHUTDOWN_MESSAGE | STOP_REASON_USER_STOP | STOP_REASON_TIMEOUT) => {
            CommandDiscardDisposition::ObsoleteControl
        }
        Some(_) => CommandDiscardDisposition::UnexpectedError,
    };
    (disposition, error.map(str::to_owned))
}

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
    /// The consuming stage's effective contract failure, including its input
    /// edge. Parents propagate this decision without parsing error messages or
    /// applying contract policy again.
    pub fn contract_failure(
        &self,
    ) -> Option<&crate::messaging::upstream_subscription::ContractFailure> {
        let mut error: &(dyn std::error::Error + 'static) = self;
        loop {
            if let Some(failure) =
                error.downcast_ref::<crate::messaging::upstream_subscription::ContractFailure>()
            {
                return Some(failure);
            }
            error = error.source()?;
        }
    }

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
/// lifecycle transition. Achieved transitions are observed separately through
/// retained FSM acknowledgements; observation never cancels child work.
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

    /// Observe a retained achieved transition. Dropping this wait leaves work alone.
    async fn wait_for_milestone(&self, milestone: StageMilestone) -> Result<StageAck, StageError>;

    /// Observe the original failure as soon as the FSM selects its failure path.
    async fn wait_for_failure(&self) -> Option<StageFailure>;

    /// Wait for physical task termination and every accepted publication to settle.
    async fn wait_for_completion(&self) -> StageExit;

    /// Abort the underlying supervisor task and join it deterministically.
    async fn abort_and_join(&self) -> Result<(), StageError>;

    /// Request immediate supervisor cancellation without waiting for its join.
    /// Used by Runtime lifetime guards; this does not claim stage completion.
    /// Implementations must be idempotent and non-blocking.
    #[doc(hidden)]
    fn request_abort(&self);
}

/// Type-erased stage handle for pipeline storage
pub type BoxedStageHandle = Box<dyn StageHandle>;
