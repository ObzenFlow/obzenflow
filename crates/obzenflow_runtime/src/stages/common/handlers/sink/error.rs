// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Narrow connector-author error authority.

use super::typed::SinkWriteReport;
use crate::stages::common::handler_error::HandlerError;
use obzenflow_core::event::status::processing_status::ErrorKind;
pub use obzenflow_core::event::{SinkDestinationErrorCode, SinkWritePhase};
use std::fmt;
use std::time::Duration;

const MAX_SINK_OPERATION_DETAIL_BYTES: usize = 512;

/// An operational connector error. Framework and protocol fatality variants
/// cannot be constructed through this type.
#[derive(Debug, Clone)]
pub struct SinkOperationError {
    error: HandlerError,
    destination_error_code: Option<SinkDestinationErrorCode>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkOperationErrorConversionError;

impl fmt::Display for SinkOperationErrorConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("handler error is not an authored sink operation error")
    }
}

impl std::error::Error for SinkOperationErrorConversionError {}

fn bounded_detail(value: &str) -> String {
    let mut value = value
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect::<String>();
    if value.len() > MAX_SINK_OPERATION_DETAIL_BYTES {
        let mut boundary = MAX_SINK_OPERATION_DETAIL_BYTES;
        while !value.is_char_boundary(boundary) {
            boundary -= 1;
        }
        value.truncate(boundary);
    }
    value
}

impl SinkOperationError {
    pub fn timeout(detail: impl Into<String>) -> Self {
        Self::from_operational(HandlerError::Timeout(bounded_detail(&detail.into())))
    }

    pub fn remote(detail: impl Into<String>) -> Self {
        Self::from_operational(HandlerError::Remote(bounded_detail(&detail.into())))
    }

    pub fn rate_limited(detail: impl Into<String>, retry_after: Option<Duration>) -> Self {
        Self::from_operational(HandlerError::RateLimited {
            message: bounded_detail(&detail.into()),
            retry_after,
        })
    }

    pub fn permanent(detail: impl Into<String>) -> Self {
        Self::from_operational(HandlerError::PermanentFailure(bounded_detail(
            &detail.into(),
        )))
    }

    pub fn deserialization(detail: impl Into<String>) -> Self {
        Self::from_operational(HandlerError::Deserialization(bounded_detail(
            &detail.into(),
        )))
    }

    pub fn validation(detail: impl Into<String>) -> Self {
        Self::from_operational(HandlerError::Validation(bounded_detail(&detail.into())))
    }

    pub fn domain(detail: impl Into<String>) -> Self {
        Self::from_operational(HandlerError::Domain(bounded_detail(&detail.into())))
    }

    pub fn other(detail: impl Into<String>) -> Self {
        Self::from_operational(HandlerError::Other(bounded_detail(&detail.into())))
    }

    fn from_operational(error: HandlerError) -> Self {
        Self {
            error,
            destination_error_code: None,
        }
    }

    pub fn with_destination_error_code(mut self, code: SinkDestinationErrorCode) -> Self {
        self.destination_error_code = Some(code);
        self
    }

    pub fn kind(&self) -> ErrorKind {
        self.error.kind()
    }

    pub fn detail(&self) -> String {
        bounded_detail(&self.error.to_string())
    }

    pub fn retry_after(&self) -> Option<Duration> {
        match &self.error {
            HandlerError::RateLimited { retry_after, .. } => *retry_after,
            _ => None,
        }
    }

    pub fn destination_error_code(&self) -> Option<&SinkDestinationErrorCode> {
        self.destination_error_code.as_ref()
    }
}

impl TryFrom<HandlerError> for SinkOperationError {
    type Error = SinkOperationErrorConversionError;

    fn try_from(error: HandlerError) -> Result<Self, Self::Error> {
        match error {
            HandlerError::Timeout(detail) => Ok(Self::timeout(detail)),
            HandlerError::Remote(detail) => Ok(Self::remote(detail)),
            HandlerError::RateLimited {
                message,
                retry_after,
            } => Ok(Self::rate_limited(message, retry_after)),
            HandlerError::PermanentFailure(detail) => Ok(Self::permanent(detail)),
            HandlerError::Deserialization(detail) => Ok(Self::deserialization(detail)),
            HandlerError::Validation(detail) => Ok(Self::validation(detail)),
            HandlerError::Domain(detail) => Ok(Self::domain(detail)),
            HandlerError::Other(detail) => Ok(Self::other(detail)),
            HandlerError::Fatal(_)
            | HandlerError::AiMapReducePlanning(_)
            | HandlerError::ContractViolation(_)
            | HandlerError::SinkOperation(_)
            | HandlerError::SinkWrite(_) => Err(SinkOperationErrorConversionError),
        }
    }
}

impl fmt::Display for SinkOperationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.detail())
    }
}

impl std::error::Error for SinkOperationError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkWriteFailureDisposition {
    CurrentOnly,
    ConfirmedRollback,
    Poisoned,
}

#[derive(Debug, Clone)]
pub struct SinkWriteFailure {
    disposition: SinkWriteFailureDisposition,
    phase: SinkWritePhase,
    error: SinkOperationError,
}

impl SinkWriteFailure {
    /// Fail only the current input. Earlier deferred inputs remain byte-for-byte
    /// and capability-for-capability intact.
    pub fn current_only(phase: SinkWritePhase, error: SinkOperationError) -> Self {
        Self::new(SinkWriteFailureDisposition::CurrentOnly, phase, error)
    }

    /// Fail the current input after positive proof that no attempted batch work
    /// committed. Every earlier deferred input remains available to a later call.
    pub fn confirmed_rollback(phase: SinkWritePhase, error: SinkOperationError) -> Self {
        Self::new(SinkWriteFailureDisposition::ConfirmedRollback, phase, error)
    }

    /// Stop the materialisation because destination commit state or an earlier
    /// deferred capability can no longer be settled safely.
    pub fn poisoned(phase: SinkWritePhase, error: SinkOperationError) -> Self {
        Self::new(SinkWriteFailureDisposition::Poisoned, phase, error)
    }

    fn new(
        disposition: SinkWriteFailureDisposition,
        phase: SinkWritePhase,
        error: SinkOperationError,
    ) -> Self {
        Self {
            disposition,
            phase,
            error,
        }
    }

    pub fn disposition(&self) -> SinkWriteFailureDisposition {
        self.disposition
    }

    pub fn phase(&self) -> SinkWritePhase {
        self.phase
    }

    pub fn error(&self) -> &SinkOperationError {
        &self.error
    }
}

impl fmt::Display for SinkWriteFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "sink write {:?}/{:?}: {}",
            self.phase, self.disposition, self.error
        )
    }
}

impl std::error::Error for SinkWriteFailure {}

pub type SinkOperationResult<T> = Result<T, SinkOperationError>;
pub type SinkWriteResult = Result<SinkWriteReport, SinkWriteFailure>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::common::handler_error::StageFatal;
    use obzenflow_core::event::{StageFatalCode, StageFatalReason};

    #[test]
    fn conversion_rejects_framework_authority() {
        let fatal = HandlerError::Fatal(StageFatal::new(
            StageFatalCode::Protocol,
            StageFatalReason::ProtocolInputIntegrity,
            "forged",
        ));
        assert!(SinkOperationError::try_from(fatal).is_err());
        assert!(
            SinkOperationError::try_from(HandlerError::ContractViolation("forged".into())).is_err()
        );
    }
}
