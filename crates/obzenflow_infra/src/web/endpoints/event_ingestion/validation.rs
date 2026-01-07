use obzenflow_core::event::ingestion::EventSubmission;
use obzenflow_core::event::schema::TypedPayload;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum ValidationConfig {
    /// Single event type - all submissions must match this schema.
    Single {
        validator: Arc<dyn SchemaValidator>,
    },
    /// Multiple event types - lookup by event_type field (exact match).
    Registry {
        validators: HashMap<String, Arc<dyn SchemaValidator>>,
        reject_unknown: bool,
    },
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ValidationError {
    #[error("expected event_type '{expected}', got '{actual}'")]
    EventTypeMismatch { expected: String, actual: String },
    #[error("unknown event_type: '{event_type}'")]
    UnknownEventType { event_type: String },
    #[error("validation failed for '{event_type}': {message}")]
    ValidationFailed { event_type: String, message: String },
}

impl ValidationError {
    pub fn to_message(&self) -> String {
        self.to_string()
    }
}

pub trait SchemaValidator: Send + Sync + std::fmt::Debug {
    fn event_type(&self) -> &str;
    fn validate(&self, payload: &serde_json::Value) -> Result<(), String>;
}

#[derive(Debug)]
pub struct TypedValidator<T: TypedPayload> {
    _phantom: PhantomData<T>,
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
    fn event_type(&self) -> &str {
        T::EVENT_TYPE
    }

    fn validate(&self, payload: &serde_json::Value) -> Result<(), String> {
        serde_json::from_value::<T>(payload.clone())
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

pub fn validate_submission(
    submission: &EventSubmission,
    config: &ValidationConfig,
) -> Result<(), ValidationError> {
    let validator = match config {
        ValidationConfig::Single { validator } => {
            if submission.event_type != validator.event_type() {
                return Err(ValidationError::EventTypeMismatch {
                    expected: validator.event_type().to_string(),
                    actual: submission.event_type.clone(),
                });
            }
            validator
        }
        ValidationConfig::Registry {
            validators,
            reject_unknown,
        } => match validators.get(&submission.event_type) {
            Some(v) => v,
            None if *reject_unknown => {
                return Err(ValidationError::UnknownEventType {
                    event_type: submission.event_type.clone(),
                });
            }
            None => return Ok(()),
        },
    };

    validator.validate(&submission.data).map_err(|message| {
        ValidationError::ValidationFailed {
            event_type: submission.event_type.clone(),
            message,
        }
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
            event_type: "test.event".to_string(),
            data: serde_json::json!({}),
            metadata: None,
        };
        let config = ValidationConfig::Single {
            validator: Arc::new(validator),
        };
        let err = validate_submission(&submission, &config).unwrap_err();
        assert!(matches!(err, ValidationError::ValidationFailed { .. }));
    }
}
