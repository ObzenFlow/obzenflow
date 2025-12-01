//! Handler-level error type for stage logic (FLOWIP-082h).
//!
//! This represents failures that occur inside handlers (sources, transforms,
//! joins, stateful handlers, sinks, observers). Pipeline coordination and
//! FSM control continue to use `StageError`; we provide `From<HandlerError>`
//! so supervisors can map handler failures into stage-level errors when
//! needed.

use crate::stages::common::stage_handle::StageError;
use obzenflow_core::event::status::processing_status::ErrorKind;

/// Error type for handler-level failures.
#[derive(Debug, Clone)]
pub enum HandlerError {
    /// Timeout talking to a remote dependency.
    Timeout(String),
    /// Remote/transport failures (HTTP 5xx, connection refused, etc.).
    Remote(String),
    /// Unable to deserialize/parse the input payload into the expected type.
    Deserialization(String),
    /// Business validation or rule violation.
    Validation(String),
    /// Broader domain logic failure.
    Domain(String),
    /// Generic or unclassified error.
    Other(String),
}

impl HandlerError {
    /// Convenience constructor for generic errors.
    pub fn other(msg: impl Into<String>) -> Self {
        HandlerError::Other(msg.into())
    }

    /// Map this handler error to a structured ErrorKind.
    pub fn kind(&self) -> ErrorKind {
        match self {
            HandlerError::Timeout(_) => ErrorKind::Timeout,
            HandlerError::Remote(_) => ErrorKind::Remote,
            HandlerError::Deserialization(_) => ErrorKind::Deserialization,
            HandlerError::Validation(_) => ErrorKind::Validation,
            HandlerError::Domain(_) => ErrorKind::Domain,
            HandlerError::Other(_) => ErrorKind::Unknown,
        }
    }
}

impl From<HandlerError> for StageError {
    fn from(err: HandlerError) -> Self {
        match err {
            HandlerError::Timeout(msg) => {
                StageError::Other(format!("Timeout: {}", msg))
            }
            HandlerError::Remote(msg) => StageError::Other(format!("Remote error: {}", msg)),
            HandlerError::Deserialization(msg) => {
                StageError::Other(format!("Deserialization error: {}", msg))
            }
            HandlerError::Validation(msg) => {
                StageError::Other(format!("Validation error: {}", msg))
            }
            HandlerError::Domain(msg) => StageError::Other(format!("Domain error: {}", msg)),
            HandlerError::Other(msg) => StageError::Other(msg),
        }
    }
}
