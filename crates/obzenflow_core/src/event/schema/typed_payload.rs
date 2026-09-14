// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! TypedPayload trait for type-safe event handling

use crate::event::chain_event::{ChainEvent, ChainEventFactory, ChainPayload};
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
    /// - Adding enum variants (with `#[non_exhaustive]`)
    /// - Relaxing validation
    ///
    /// Breaking changes require a new event type with a semantic name change.
    ///
    /// Default is 1 for new event types.
    const SCHEMA_VERSION: u32 = 1;

    /// Extract typed payload from ChainEvent if the event type matches
    ///
    /// Returns `Some(Self)` if:
    /// - The payload belongs to this type's declared family
    /// - The event_type matches `Self::EVENT_TYPE`
    /// - The payload can be deserialized to `Self`
    ///
    /// Returns `None` otherwise.
    fn from_event(event: &ChainEvent) -> Option<Self> {
        Self::try_from_event(event).ok()
    }

    /// User payloads are application facts. Built-in protocol implementations
    /// override this with their closed typed carrier or execution constructor.
    fn into_chain_payload(self) -> Result<ChainPayload, serde_json::Error> {
        serde_json::to_value(self).map(ChainPayload::Fact)
    }

    fn accepts_payload(payload: &ChainPayload) -> bool {
        matches!(payload, ChainPayload::Fact(_))
    }

    fn to_event(self, writer_id: WriterId) -> ChainEvent {
        let mut event = ChainEventFactory::create_event(
            writer_id,
            self.into_chain_payload()
                .expect("typed payload serialization"),
        );
        event.envelope.provenance.event.event_type = Self::versioned_event_type();
        event
    }

    fn try_from_event(event: &ChainEvent) -> Result<Self, TypedPayloadError> {
        if !Self::accepts_payload(&event.payload) {
            return Err(TypedPayloadError::WrongContentType(
                event.payload.kind().as_str(),
            ));
        }
        if !Self::event_type_matches(&event.envelope.provenance.event.event_type) {
            return Err(TypedPayloadError::TypeMismatch {
                expected: Self::EVENT_TYPE,
                actual: event.event_type(),
            });
        }
        // Execution history has a typed inner body as well as the outer tag.
        let value = event.payload.contract_body().map_err(|error| {
            TypedPayloadError::DeserializationFailed {
                event_type: Self::EVENT_TYPE,
                error: error.to_string(),
            }
        })?;
        serde_json::from_value(value).map_err(|error| TypedPayloadError::DeserializationFailed {
            event_type: Self::EVENT_TYPE,
            error: error.to_string(),
        })
    }

    /// Fully qualified event type including schema version (e.g., "event.v1")
    fn versioned_event_type() -> String {
        format!("{}.v{}", Self::EVENT_TYPE, Self::SCHEMA_VERSION)
    }

    /// Accept current or legacy event type strings
    fn event_type_matches(event_type: &str) -> bool {
        event_type == Self::EVENT_TYPE || event_type == Self::versioned_event_type()
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

    /// Event belongs to a different payload family
    #[error("Event has wrong payload family: {0}")]
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

        assert_eq!(event.event_type(), "test.event.v1");
        match event.payload {
            ChainPayload::Fact(json_payload) => {
                let extracted: TestEvent = serde_json::from_value(json_payload).unwrap();
                assert_eq!(extracted, payload);
            }
            _ => panic!("Expected application fact"),
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
