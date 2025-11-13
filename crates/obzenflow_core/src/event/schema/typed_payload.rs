//! TypedPayload trait for type-safe event handling

use crate::event::chain_event::{ChainEvent, ChainEventContent, ChainEventFactory};
use crate::event::types::WriterId;
use serde::{de::DeserializeOwned, Serialize};

/// Trait for strongly-typed event payloads
///
/// Implementing this trait allows type-safe extraction and creation of events,
/// associating a Rust type with a specific event_type string.
///
/// # Example
///
/// ```rust
/// use serde::{Deserialize, Serialize};
/// use obzenflow_core::event::schema::TypedPayload;
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// struct OrderCreated {
///     pub order_id: String,
///     pub customer_id: String,
///     pub total_amount: f64,
/// }
///
/// impl TypedPayload for OrderCreated {
///     const EVENT_TYPE: &'static str = "order.created";
/// }
/// ```
pub trait TypedPayload: Serialize + DeserializeOwned + Sized {
    /// Semantic event type name (e.g., "order.created", "flight.record.imported")
    ///
    /// Should describe WHAT happened (the business fact), not include version numbers.
    /// Version suffixes like ".v1" create coupling and don't belong in event names.
    ///
    /// Use semantic, stable names that represent the domain event.
    const EVENT_TYPE: &'static str;

    /// Schema version for compatibility tracking
    ///
    /// Increment this when making backward-compatible changes:
    /// - Adding optional fields
    /// - Adding enum variants (with #[non_exhaustive])
    /// - Relaxing validation
    ///
    /// Breaking changes require a new event type with a semantic name change.
    ///
    /// Default is 1 for new event types.
    const SCHEMA_VERSION: u32 = 1;

    /// Extract typed payload from ChainEvent if the event type matches
    ///
    /// Returns `Some(Self)` if:
    /// - The event content is `Data`
    /// - The event_type matches `Self::EVENT_TYPE`
    /// - The payload can be deserialized to `Self`
    ///
    /// Returns `None` otherwise.
    fn from_event(event: &ChainEvent) -> Option<Self> {
        match &event.content {
            ChainEventContent::Data {
                event_type,
                payload,
            } if event_type == Self::EVENT_TYPE => serde_json::from_value(payload.clone()).ok(),
            _ => None,
        }
    }

    /// Convert typed payload to ChainEvent
    ///
    /// # Panics
    ///
    /// Panics if serialization fails (should never happen for valid Serialize implementations)
    fn to_event(self, writer_id: WriterId) -> ChainEvent {
        ChainEventFactory::data_event(
            writer_id,
            Self::EVENT_TYPE,
            serde_json::to_value(self).expect("Serialization should not fail"),
        )
    }

    /// Try to extract typed payload from ChainEvent, returning Result
    ///
    /// This is similar to `from_event` but provides more detailed error information.
    fn try_from_event(event: &ChainEvent) -> Result<Self, TypedPayloadError> {
        match &event.content {
            ChainEventContent::Data {
                event_type,
                payload,
            } => {
                if event_type != Self::EVENT_TYPE {
                    return Err(TypedPayloadError::TypeMismatch {
                        expected: Self::EVENT_TYPE,
                        actual: event_type.clone(),
                    });
                }

                serde_json::from_value(payload.clone()).map_err(|e| {
                    TypedPayloadError::DeserializationFailed {
                        event_type: Self::EVENT_TYPE,
                        error: e.to_string(),
                    }
                })
            }
            ChainEventContent::FlowControl(_) => {
                Err(TypedPayloadError::WrongContentType("flow_signal"))
            }
            ChainEventContent::Delivery(_) => {
                Err(TypedPayloadError::WrongContentType("delivery"))
            }
            ChainEventContent::Observability(_) => {
                Err(TypedPayloadError::WrongContentType("lifecycle"))
            }
        }
    }
}

/// Errors that can occur when extracting typed payloads from events
#[derive(Debug, Clone, thiserror::Error)]
pub enum TypedPayloadError {
    /// Event type doesn't match expected type
    #[error("Event type mismatch: expected '{expected}', got '{actual}'")]
    TypeMismatch {
        expected: &'static str,
        actual: String,
    },

    /// Payload deserialization failed
    #[error("Failed to deserialize event type '{event_type}': {error}")]
    DeserializationFailed {
        event_type: &'static str,
        error: String,
    },

    /// Event has wrong content type (not Data)
    #[error("Event has wrong content type: {0} (expected 'data')")]
    WrongContentType(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::StageId;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestEvent {
        message: String,
        count: u32,
    }

    impl TypedPayload for TestEvent {
        const EVENT_TYPE: &'static str = "test.event";
    }

    #[test]
    fn test_to_event() {
        let payload = TestEvent {
            message: "hello".to_string(),
            count: 42,
        };
        let writer_id = WriterId::from(StageId::new());
        let event = payload.clone().to_event(writer_id);

        match event.content {
            ChainEventContent::Data {
                event_type,
                payload: json_payload,
            } => {
                assert_eq!(event_type, "test.event.v1");
                let extracted: TestEvent = serde_json::from_value(json_payload).unwrap();
                assert_eq!(extracted, payload);
            }
            _ => panic!("Expected Data content"),
        }
    }

    #[test]
    fn test_from_event_success() {
        let payload = TestEvent {
            message: "hello".to_string(),
            count: 42,
        };
        let writer_id = WriterId::from(StageId::new());
        let event = payload.clone().to_event(writer_id);

        let extracted = TestEvent::from_event(&event);
        assert_eq!(extracted, Some(payload));
    }

    #[test]
    fn test_from_event_wrong_type() {
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "different.type",
            serde_json::json!({"message": "hello", "count": 42}),
        );

        let extracted = TestEvent::from_event(&event);
        assert_eq!(extracted, None);
    }

    #[test]
    fn test_try_from_event_success() {
        let payload = TestEvent {
            message: "hello".to_string(),
            count: 42,
        };
        let writer_id = WriterId::from(StageId::new());
        let event = payload.clone().to_event(writer_id);

        let extracted = TestEvent::try_from_event(&event).unwrap();
        assert_eq!(extracted, payload);
    }

    #[test]
    fn test_try_from_event_type_mismatch() {
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "different.type",
            serde_json::json!({"message": "hello", "count": 42}),
        );

        let result = TestEvent::try_from_event(&event);
        assert!(matches!(
            result,
            Err(TypedPayloadError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn test_try_from_event_deserialization_failed() {
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.event.v1",
            serde_json::json!({"wrong_field": "value"}),
        );

        let result = TestEvent::try_from_event(&event);
        assert!(matches!(
            result,
            Err(TypedPayloadError::DeserializationFailed { .. })
        ));
    }
}
