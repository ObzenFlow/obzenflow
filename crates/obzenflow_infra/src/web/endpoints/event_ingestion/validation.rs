// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::schema::TypedPayload;
use obzenflow_core::event::{SourceDiagnostic, SourceDiagnosticReason, SourceErrorCode};
use obzenflow_core::ingress::EventSubmission;
use obzenflow_core::EventType;
use obzenflow_runtime::stages::{IngressDecodeError, IngressRecord};
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::sync::Arc;

/// A schema check run in addition to the decoder (FLOWIP-084l B4); it never
/// replaces it. Hosted ingress admits one event type, the decoder's output.
#[derive(Debug, Clone)]
pub enum ValidationConfig {
    /// Every submission must match this schema.
    Single { validator: Arc<dyn SchemaValidator> },
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ValidationError {
    #[error("expected event_type '{expected}', got '{actual}'")]
    EventTypeMismatch { expected: String, actual: String },
    #[error("validation failed for '{event_type}': {diagnostic}")]
    ValidationFailed {
        event_type: String,
        diagnostic: Box<SourceDiagnostic>,
    },
}

impl ValidationError {
    pub fn to_message(&self) -> String {
        self.to_string()
    }

    /// The journalled form; event type strings stay in the HTTP response.
    pub fn diagnostic(&self) -> SourceDiagnostic {
        match self {
            Self::EventTypeMismatch { .. } => {
                let diagnostic = SourceDiagnostic::new(SourceDiagnosticReason::InvalidRecord);
                match SourceErrorCode::try_new("ingress", "event_type") {
                    Ok(code) => diagnostic.code(code),
                    Err(_) => diagnostic,
                }
            }
            Self::ValidationFailed { diagnostic, .. } => diagnostic.as_ref().clone(),
        }
    }
}

pub trait SchemaValidator: Send + Sync + std::fmt::Debug {
    fn event_type(&self) -> EventType;
    fn validate(&self, payload: &serde_json::Value) -> Result<(), SourceDiagnostic>;
}

#[derive(Debug)]
pub struct TypedValidator<T: TypedPayload> {
    _phantom: PhantomData<T>,
}

impl<T: TypedPayload> Default for TypedValidator<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: TypedPayload> TypedValidator<T> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T: TypedPayload + DeserializeOwned + Send + Sync + std::fmt::Debug> SchemaValidator
    for TypedValidator<T>
{
    fn event_type(&self) -> EventType {
        EventType::from(T::EVENT_TYPE)
    }

    fn validate(&self, payload: &serde_json::Value) -> Result<(), SourceDiagnostic> {
        IngressRecord::new(payload)
            .deserialize::<T>()
            .map(|_| ())
            .map_err(IngressDecodeError::into_diagnostic)
    }
}

pub fn validate_submission(
    submission: &EventSubmission,
    config: &ValidationConfig,
) -> Result<(), ValidationError> {
    let ValidationConfig::Single { validator } = config;
    if submission.event_type != validator.event_type() {
        return Err(ValidationError::EventTypeMismatch {
            expected: validator.event_type().to_string(),
            actual: submission.event_type.to_string(),
        });
    }

    validator
        .validate(&submission.data)
        .map_err(|diagnostic| ValidationError::ValidationFailed {
            event_type: submission.event_type.to_string(),
            diagnostic: Box::new(diagnostic),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestPayload {
        required: String,
    }

    impl TypedPayload for TestPayload {
        const EVENT_TYPE: &'static str = "test.event";
    }

    #[test]
    fn typed_validator_rejects_missing_field() {
        let validator = TypedValidator::<TestPayload>::new();
        let submission = EventSubmission {
            event_type: "test.event".into(),
            data: serde_json::json!({}),
            metadata: None,
        };
        let config = ValidationConfig::Single {
            validator: Arc::new(validator),
        };
        let err = validate_submission(&submission, &config).unwrap_err();
        assert!(matches!(err, ValidationError::ValidationFailed { .. }));
        let diagnostic = err.diagnostic();
        assert_eq!(diagnostic.reason(), SourceDiagnosticReason::MissingField);
        assert_eq!(
            diagnostic.location().field_path(),
            [obzenflow_core::event::FieldSegment::Field(
                obzenflow_core::event::FieldName::new("required")
            )]
        );
    }

    #[test]
    fn event_type_refusals_journal_a_code_without_the_submitted_name() {
        let config = ValidationConfig::Single {
            validator: Arc::new(TypedValidator::<TestPayload>::new()),
        };
        let submission = EventSubmission {
            event_type: "SECRET.event".into(),
            data: serde_json::json!({"required": "x"}),
            metadata: None,
        };
        let err = validate_submission(&submission, &config).unwrap_err();
        let diagnostic = err.diagnostic();
        assert_eq!(diagnostic.reason(), SourceDiagnosticReason::InvalidRecord);
        let code = diagnostic.error_code().expect("event type code");
        assert_eq!((code.namespace(), code.value()), ("ingress", "event_type"));
        assert!(!format!("{diagnostic} {diagnostic:?}").contains("SECRET"));
    }
}
