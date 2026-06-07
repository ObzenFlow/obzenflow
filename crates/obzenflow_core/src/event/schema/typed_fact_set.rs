// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed fact-set authoring support.

use crate::event::chain_event::{ChainEvent, ChainEventContent};
use crate::event::schema::typed_payload::TypedPayload;
use serde_json::Value;
use std::any::{type_name, TypeId};

/// Type metadata for one member of a typed fact set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypedFactType {
    pub type_id: TypeId,
    pub display_name: String,
    pub event_type: String,
    pub schema_version: u32,
}

impl TypedFactType {
    pub fn of<T>() -> Self
    where
        T: TypedPayload + 'static,
    {
        Self {
            type_id: TypeId::of::<T>(),
            display_name: type_name::<T>().to_string(),
            event_type: T::versioned_event_type(),
            schema_version: T::SCHEMA_VERSION,
        }
    }
}

/// Serialized fact ready for conversion into a `ChainEvent::Data` value.
#[derive(Clone, Debug, PartialEq)]
pub struct TypedFact {
    pub event_type: String,
    pub payload: Value,
}

impl TypedFact {
    pub fn from_payload<T>(payload: T) -> Result<Self, TypedFactSetError>
    where
        T: TypedPayload,
    {
        Ok(Self {
            event_type: T::versioned_event_type(),
            payload: serde_json::to_value(payload)
                .map_err(|e| TypedFactSetError::SerializationFailed(e.to_string()))?,
        })
    }

    pub fn from_event(event: &ChainEvent) -> Option<Self> {
        match &event.content {
            ChainEventContent::Data {
                event_type,
                payload,
            } => Some(Self {
                event_type: event_type.clone(),
                payload: payload.clone(),
            }),
            _ => None,
        }
    }
}

/// Error returned while serializing a typed fact set.
#[derive(Debug, Clone, thiserror::Error)]
pub enum TypedFactSetError {
    #[error("failed to serialize typed fact: {0}")]
    SerializationFailed(String),

    #[error("missing typed fact `{event_type}`")]
    MissingFact { event_type: String },

    #[error("duplicate typed fact `{event_type}`")]
    DuplicateFact { event_type: String },

    #[error("failed to deserialize typed fact `{event_type}`: {error}")]
    DeserializationFailed { event_type: String, error: String },
}

/// A typed marshalling bridge for code paths that still produce a typed value
/// before it is converted into `Data` facts.
///
/// The blanket `TypedPayload -> TypedFactSet` implementation keeps scalar
/// stateful outputs working while FLOWIP-120b moves effectful authoring to
/// explicit `fx.emit` calls and flat output contracts.
pub trait TypedFactSet: Send + Sync + 'static {
    fn fact_types() -> Vec<TypedFactType>;

    fn into_facts(self) -> Result<Vec<TypedFact>, TypedFactSetError>;

    fn try_from_facts(facts: &[TypedFact]) -> Result<Self, TypedFactSetError>
    where
        Self: Sized;
}

impl<T> TypedFactSet for T
where
    T: TypedPayload + Send + Sync + 'static,
{
    fn fact_types() -> Vec<TypedFactType> {
        vec![TypedFactType::of::<T>()]
    }

    fn into_facts(self) -> Result<Vec<TypedFact>, TypedFactSetError> {
        Ok(vec![TypedFact::from_payload(self)?])
    }

    fn try_from_facts(facts: &[TypedFact]) -> Result<Self, TypedFactSetError> {
        decode_fact::<T>(facts)
    }
}

fn decode_fact<T>(facts: &[TypedFact]) -> Result<T, TypedFactSetError>
where
    T: TypedPayload,
{
    let mut matches = facts
        .iter()
        .filter(|fact| T::event_type_matches(&fact.event_type));
    let fact = matches
        .next()
        .ok_or_else(|| TypedFactSetError::MissingFact {
            event_type: T::versioned_event_type(),
        })?;
    if matches.next().is_some() {
        return Err(TypedFactSetError::DuplicateFact {
            event_type: T::versioned_event_type(),
        });
    }
    serde_json::from_value(fact.payload.clone()).map_err(|e| {
        TypedFactSetError::DeserializationFailed {
            event_type: fact.event_type.clone(),
            error: e.to_string(),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct First {
        value: u32,
    }

    impl TypedPayload for First {
        const EVENT_TYPE: &'static str = "fact.first";
    }

    #[test]
    fn scalar_typed_payload_is_single_fact_set() {
        let facts = First { value: 7 }
            .into_facts()
            .expect("scalar fact set serializes");

        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].event_type, "fact.first.v1");
        assert_eq!(facts[0].payload, serde_json::json!({ "value": 7 }));
    }

    #[test]
    fn scalar_fact_set_reconstructs_from_member_fact_by_event_type() {
        let facts = vec![TypedFact::from_payload(First { value: 7 }).expect("first fact")];

        let reconstructed = First::try_from_facts(&facts).expect("reconstruct scalar");

        assert_eq!(reconstructed, First { value: 7 });
    }
}
