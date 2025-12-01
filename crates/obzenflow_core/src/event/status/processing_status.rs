//! Processing outcome types
//!
//! Defines the possible outcomes of processing an event, including
//! structured error classification via `ErrorKind` (FLOWIP-082h).

use serde::{Deserialize, Serialize};

/// Structured classification for processing errors (FLOWIP-082h).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ErrorKind {
    /// Timeout talking to a remote dependency.
    Timeout,
    /// Remote/transport failures (HTTP 5xx, connection refused, etc.).
    Remote,
    /// Unable to deserialize/parse the input payload.
    Deserialization,
    /// Business rule violation (invalid input, out-of-range value, etc.).
    Validation,
    /// Broader domain logic failure.
    Domain,
    /// Unclassified error; treated conservatively by default.
    Unknown,
}

/// The outcome of processing an event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProcessingStatus {
    /// Event was processed successfully
    Success,

    /// Event was filtered out (intentionally not processed)
    Filtered,

    /// Processing failed with an error
    Error {
        /// Human-readable error message
        message: String,
        /// Structured classification for this error (optional until fully wired)
        #[serde(skip_serializing_if = "Option::is_none")]
        kind: Option<ErrorKind>,
    },

    /// Event should be retried
    Retry { attempt: u32 },
}

impl ProcessingStatus {
    /// Create a success outcome
    pub fn success() -> Self {
        ProcessingStatus::Success
    }

    /// Create a generic error outcome with no specific ErrorKind.
    pub fn error(msg: impl Into<String>) -> Self {
        ProcessingStatus::Error {
            message: msg.into(),
            kind: None,
        }
    }

    /// Create an error outcome with an explicit ErrorKind.
    pub fn error_with_kind(msg: impl Into<String>, kind: Option<ErrorKind>) -> Self {
        ProcessingStatus::Error {
            message: msg.into(),
            kind,
        }
    }

    /// Access the ErrorKind, if present.
    pub fn kind(&self) -> Option<&ErrorKind> {
        match self {
            ProcessingStatus::Error { kind, .. } => kind.as_ref(),
            _ => None,
        }
    }

    /// Check if this outcome is terminal (no more processing needed)
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Success | Self::Filtered | Self::Error { .. })
    }

    /// Check if the event should be retried
    pub fn should_retry(&self) -> bool {
        matches!(self, Self::Retry { .. })
    }

    /// Check if processing was successful
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success)
    }
}

impl Default for ProcessingStatus {
    fn default() -> Self {
        Self::Success
    }
}
