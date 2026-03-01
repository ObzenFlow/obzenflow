// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handler-level error type for stage logic (FLOWIP-082h).
//!
//! This represents failures that occur inside handlers (sources, transforms,
//! joins, stateful handlers, sinks, observers). Pipeline coordination and
//! FSM control continue to use `StageError`; we provide `From<HandlerError>`
//! so supervisors can map handler failures into stage-level errors when
//! needed.

use crate::stages::common::stage_handle::StageError;
use obzenflow_core::event::status::processing_status::ErrorKind;
use std::fmt;
use std::time::Duration;

/// Error type for handler-level failures.
#[derive(Debug, Clone)]
pub enum HandlerError {
    /// Timeout talking to a remote dependency.
    Timeout(String),
    /// Remote/transport failures (HTTP 5xx, connection refused, etc.).
    Remote(String),
    /// Request was rate limited and may be retried after an optional delay.
    RateLimited {
        message: String,
        retry_after: Option<Duration>,
    },
    /// Permanent failure where retry is not expected to help (auth, bad credentials, etc.).
    PermanentFailure(String),
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
            HandlerError::RateLimited { .. } => ErrorKind::RateLimited,
            HandlerError::PermanentFailure(_) => ErrorKind::PermanentFailure,
            HandlerError::Deserialization(_) => ErrorKind::Deserialization,
            HandlerError::Validation(_) => ErrorKind::Validation,
            HandlerError::Domain(_) => ErrorKind::Domain,
            HandlerError::Other(_) => ErrorKind::Unknown,
        }
    }
}

impl fmt::Display for HandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HandlerError::Timeout(msg) => write!(f, "Timeout: {msg}"),
            HandlerError::Remote(msg) => write!(f, "Remote error: {msg}"),
            HandlerError::RateLimited {
                message,
                retry_after,
            } => match retry_after {
                Some(wait) => write!(
                    f,
                    "Rate limited: {message} (retry_after_ms={})",
                    wait.as_millis()
                ),
                None => write!(f, "Rate limited: {message}"),
            },
            HandlerError::PermanentFailure(msg) => write!(f, "Permanent failure: {msg}"),
            HandlerError::Deserialization(msg) => {
                write!(f, "Deserialization error: {msg}")
            }
            HandlerError::Validation(msg) => write!(f, "Validation error: {msg}"),
            HandlerError::Domain(msg) => write!(f, "Domain error: {msg}"),
            HandlerError::Other(msg) => write!(f, "Handler error: {msg}"),
        }
    }
}

impl std::error::Error for HandlerError {}

impl From<HandlerError> for StageError {
    fn from(err: HandlerError) -> Self {
        StageError::handler_failure(err)
    }
}
