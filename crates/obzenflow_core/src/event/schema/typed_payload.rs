// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! TypedPayload trait for type-safe event handling

use crate::event::chain_event::{ChainEvent, ChainEventFactory, ChainPayload};
use crate::event::payloads::chain_payload::EventKind;
use crate::event::types::WriterId;
use serde::{de::DeserializeOwned, Serialize};
use std::num::NonZeroU32;

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

    const EVENT_KIND: EventKind = EventKind::Fact;

    /// Exact payload representation version, independent of semantic identity.
    /// A changed representation increments this version; changed meaning may
    /// require a new event name. Typed decoding accepts only this exact version.
    /// Instantiating a zero-version declaration is a compile error:
    ///
    /// ```compile_fail
    /// use obzenflow_core::TypedPayload;
    /// #[derive(serde::Serialize, serde::Deserialize)]
    /// struct Invalid;
    /// impl TypedPayload for Invalid {
    ///     const EVENT_TYPE: &'static str = "invalid";
    ///     const SCHEMA_VERSION: u32 = 0;
    /// }
    /// let _ = Invalid::payload_schema_version();
    /// ```
    const SCHEMA_VERSION: u32 = 1;

    fn descriptor() -> crate::EventDescriptor {
        crate::EventDescriptor {
            event_kind: Self::EVENT_KIND,
            event_type: Self::EVENT_TYPE.into(),
            payload_schema_version: Self::payload_schema_version(),
        }
    }

    fn payload_schema_version() -> NonZeroU32 {
        const {
            match NonZeroU32::new(Self::SCHEMA_VERSION) {
                Some(version) => version,
                None => panic!("TypedPayload::SCHEMA_VERSION must be positive"),
            }
        }
    }

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
        ChainEventFactory::create_event(
            writer_id,
            self.into_chain_payload()
                .expect("typed payload serialization"),
            Self::EVENT_TYPE,
            Self::payload_schema_version(),
        )
    }

    fn try_from_event(event: &ChainEvent) -> Result<Self, TypedPayloadError> {
        if event.event_kind != Self::EVENT_KIND || !Self::accepts_payload(&event.payload) {
            return Err(TypedPayloadError::WrongContentType(
                event.payload.kind().as_str(),
            ));
        }
        if !Self::matches_event_type(&event.envelope.provenance.event.event_type) {
            return Err(TypedPayloadError::TypeMismatch {
                expected: Self::EVENT_TYPE,
                actual: event.event_type(),
            });
        }
        if event.payload_schema_version != Self::payload_schema_version() {
            return Err(TypedPayloadError::SchemaVersionMismatch {
                event_type: Self::EVENT_TYPE,
                expected: Self::payload_schema_version(),
                actual: event.payload_schema_version,
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

    /// Semantic event name. The payload version is separate provenance.
    fn event_type_name() -> String {
        Self::EVENT_TYPE.to_string()
    }

    /// Name-only classification. Typed decoding also checks kind and version.
    fn matches_event_type(event_type: &str) -> bool {
        event_type == Self::EVENT_TYPE
    }

    fn matches_fact(fact: &super::typed_fact_set::TypedFact) -> bool {
        fact.payload.kind() == Self::EVENT_KIND
            && Self::accepts_payload(&fact.payload)
            && fact.event_type.as_str() == Self::EVENT_TYPE
            && fact.payload_schema_version == Self::payload_schema_version()
    }
}

/// Errors that can occur when extracting typed payloads from events
#[derive(Debug, Clone, thiserror::Error)]
pub enum TypedPayloadError {
    #[error(
        "Payload schema version mismatch for '{event_type}': expected {expected}, got {actual}"
    )]
    SchemaVersionMismatch {
        event_type: &'static str,
        expected: NonZeroU32,
        actual: NonZeroU32,
    },
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

        assert_eq!(event.event_type(), "test.event");
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
            std::num::NonZeroU32::MIN,
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
            std::num::NonZeroU32::MIN,
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
            "test.event",
            std::num::NonZeroU32::MIN,
            serde_json::json!({"wrong_field": "value"}),
        );

        let result = TestEvent::try_from_event(&event);
        assert!(matches!(
            result,
            Err(TypedPayloadError::DeserializationFailed { .. })
        ));
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct VersionTwo {
        value: u64,
    }

    impl TypedPayload for VersionTwo {
        const EVENT_TYPE: &'static str = "test.versioned";
        const SCHEMA_VERSION: u32 = 2;
    }

    #[test]
    fn raw_typed_and_derived_outputs_keep_independent_versions() {
        let writer = WriterId::from(StageId::new());
        let typed = VersionTwo { value: 7 }.to_event(writer);
        let raw = ChainEventFactory::data_event_from(
            writer,
            VersionTwo::EVENT_TYPE,
            VersionTwo::payload_schema_version(),
            &VersionTwo { value: 7 },
        )
        .unwrap();
        assert_eq!(raw.descriptor(), typed.descriptor());
        assert_eq!(
            VersionTwo::try_from_event(&raw).unwrap(),
            VersionTwo { value: 7 }
        );
        let forwarded = raw.clone();
        assert_eq!(forwarded.descriptor(), typed.descriptor());
        let derived = ChainEventFactory::derived_data_event(
            writer,
            &raw,
            "test.child",
            NonZeroU32::new(3).unwrap(),
            serde_json::json!({}),
            Default::default(),
        );
        assert_eq!(derived.payload_schema_version.get(), 3);
        assert_eq!(raw.payload_schema_version.get(), 2);
    }

    #[test]
    fn typed_mismatch_is_rejected_before_body_decoding() {
        let writer = WriterId::from(StageId::new());
        for body in [
            serde_json::json!({"value": 7}),
            serde_json::json!("invalid body"),
        ] {
            let mut event = ChainEventFactory::data_event(
                writer,
                VersionTwo::EVENT_TYPE,
                NonZeroU32::MIN,
                body,
            );
            assert!(matches!(
                VersionTwo::try_from_event(&event),
                Err(TypedPayloadError::SchemaVersionMismatch { .. })
            ));
            event.payload_schema_version = VersionTwo::payload_schema_version();
            event.event_kind = EventKind::Execution;
            assert!(matches!(
                VersionTwo::try_from_event(&event),
                Err(TypedPayloadError::WrongContentType(_))
            ));
        }
        let mut event = VersionTwo { value: 7 }.to_event(writer);
        event.event_type = "test.versioned.v2".into();
        assert!(matches!(
            VersionTwo::try_from_event(&event),
            Err(TypedPayloadError::TypeMismatch { .. })
        ));
    }
}
