// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Durable, runtime-authored evidence for connector operation failures.

use crate::event::schema::TypedPayload;
use crate::event::status::processing_status::ErrorKind;
use crate::{EventId, StageId};
use serde::{Deserialize, Serialize};
use std::fmt;

pub const MAX_SINK_DESTINATION_ERROR_NAMESPACE_BYTES: usize = 48;
pub const MAX_SINK_DESTINATION_ERROR_VALUE_BYTES: usize = 64;

/// The closed set of phases a sink writer may report for a per-input write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SinkWritePhase {
    Encode,
    Acquire,
    Execute,
    Commit,
}

/// Framework-stamped phase for a connector operation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "operation", content = "write_phase", rename_all = "snake_case")]
pub enum SinkOperationPhase {
    Open,
    Write(SinkWritePhase),
    Flush,
    Drain,
}

/// A bounded destination-native diagnostic code.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SinkDestinationErrorCode {
    namespace: String,
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkDestinationErrorCodeError {
    InvalidNamespace,
    InvalidValue,
}

impl fmt::Display for SinkDestinationErrorCodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidNamespace => {
                f.write_str("sink destination error namespace must be 1..=48 bytes of [a-z0-9._-]")
            }
            Self::InvalidValue => {
                f.write_str("sink destination error value must be 1..=64 bytes of [A-Za-z0-9._:-]")
            }
        }
    }
}

impl std::error::Error for SinkDestinationErrorCodeError {}

impl SinkDestinationErrorCode {
    pub fn try_new(
        namespace: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, SinkDestinationErrorCodeError> {
        let namespace = namespace.into();
        let value = value.into();

        if namespace.is_empty()
            || namespace.len() > MAX_SINK_DESTINATION_ERROR_NAMESPACE_BYTES
            || !namespace.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
            })
        {
            return Err(SinkDestinationErrorCodeError::InvalidNamespace);
        }
        if value.is_empty()
            || value.len() > MAX_SINK_DESTINATION_ERROR_VALUE_BYTES
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
        {
            return Err(SinkDestinationErrorCodeError::InvalidValue);
        }

        Ok(Self { namespace, value })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

/// Durable evidence for one connector-authored operation error return.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkOperationFailed {
    pub stage_id: StageId,
    pub stage_key: String,
    pub logical_destination: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causal_event_id: Option<EventId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_position: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_delivery_event_id: Option<EventId>,
    /// The earlier deferred input whose bind or execution was the actual
    /// subject of this failure. This is diagnostic evidence, not settlement
    /// authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_subject_event_id: Option<EventId>,
    pub phase: SinkOperationPhase,
    pub kind: ErrorKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_error_code: Option<SinkDestinationErrorCode>,
    pub detail: String,
}

impl TypedPayload for SinkOperationFailed {
    const EVENT_TYPE: &'static str = "obzenflow.sink_operation_failed";
    const SCHEMA_VERSION: u32 = 1;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn destination_error_code_enforces_its_grammar() {
        let code = SinkDestinationErrorCode::try_new("postgresql.sqlstate", "23505")
            .expect("valid SQLSTATE code");
        assert_eq!(code.namespace(), "postgresql.sqlstate");
        assert_eq!(code.value(), "23505");

        assert_eq!(
            SinkDestinationErrorCode::try_new("PostgreSQL", "23505"),
            Err(SinkDestinationErrorCodeError::InvalidNamespace)
        );
        assert_eq!(
            SinkDestinationErrorCode::try_new("postgresql.sqlstate", "not allowed"),
            Err(SinkDestinationErrorCodeError::InvalidValue)
        );

        assert_eq!(
            SinkDestinationErrorCode::try_new("a".repeat(49), "23505"),
            Err(SinkDestinationErrorCodeError::InvalidNamespace)
        );
        assert_eq!(
            SinkDestinationErrorCode::try_new("postgresql.sqlstate", "x".repeat(65)),
            Err(SinkDestinationErrorCodeError::InvalidValue)
        );
    }

    fn operation_failure(code: Option<SinkDestinationErrorCode>) -> SinkOperationFailed {
        SinkOperationFailed {
            stage_id: StageId::new(),
            stage_key: "payments".to_string(),
            logical_destination: "postgres.public.payments".to_string(),
            causal_event_id: Some(EventId::new()),
            input_position: Some(7),
            failed_delivery_event_id: Some(EventId::new()),
            operation_subject_event_id: None,
            phase: SinkOperationPhase::Write(SinkWritePhase::Execute),
            kind: ErrorKind::Remote,
            destination_error_code: code,
            detail: "redacted failure".to_string(),
        }
    }

    #[test]
    fn destination_code_is_absent_for_none_and_round_trips_when_present() {
        let without = serde_json::to_value(operation_failure(None)).unwrap();
        assert!(without.get("destination_error_code").is_none());
        assert!(without.get("operation_subject_event_id").is_none());

        let expected = SinkDestinationErrorCode::try_new("postgresql.sqlstate", "08007").unwrap();
        let subject = EventId::new();
        let mut failure = operation_failure(Some(expected.clone()));
        failure.operation_subject_event_id = Some(subject);
        let encoded = serde_json::to_value(failure).unwrap();
        let decoded: SinkOperationFailed = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.destination_error_code, Some(expected));
        assert_eq!(decoded.operation_subject_event_id, Some(subject));
    }
}
