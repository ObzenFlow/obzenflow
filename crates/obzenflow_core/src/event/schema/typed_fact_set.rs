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

/// A handler output that can author zero or more typed `Data` facts.
///
/// Existing scalar outputs continue to work through the blanket
/// `TypedPayload -> TypedFactSet` implementation. Product carriers such as
/// `Facts2` are the additive surface for authoring multiple facts from one
/// handler return value.
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

/// Product fact-set carrier for two typed facts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Facts2<A, B>(pub A, pub B);

impl<A, B> TypedFactSet for Facts2<A, B>
where
    A: TypedPayload + Send + Sync + 'static,
    B: TypedPayload + Send + Sync + 'static,
{
    fn fact_types() -> Vec<TypedFactType> {
        vec![TypedFactType::of::<A>(), TypedFactType::of::<B>()]
    }

    fn into_facts(self) -> Result<Vec<TypedFact>, TypedFactSetError> {
        Ok(vec![
            TypedFact::from_payload(self.0)?,
            TypedFact::from_payload(self.1)?,
        ])
    }

    fn try_from_facts(facts: &[TypedFact]) -> Result<Self, TypedFactSetError> {
        Ok(Self(decode_fact::<A>(facts)?, decode_fact::<B>(facts)?))
    }
}

/// Product fact-set carrier for three typed facts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Facts3<A, B, C>(pub A, pub B, pub C);

impl<A, B, C> TypedFactSet for Facts3<A, B, C>
where
    A: TypedPayload + Send + Sync + 'static,
    B: TypedPayload + Send + Sync + 'static,
    C: TypedPayload + Send + Sync + 'static,
{
    fn fact_types() -> Vec<TypedFactType> {
        vec![
            TypedFactType::of::<A>(),
            TypedFactType::of::<B>(),
            TypedFactType::of::<C>(),
        ]
    }

    fn into_facts(self) -> Result<Vec<TypedFact>, TypedFactSetError> {
        Ok(vec![
            TypedFact::from_payload(self.0)?,
            TypedFact::from_payload(self.1)?,
            TypedFact::from_payload(self.2)?,
        ])
    }

    fn try_from_facts(facts: &[TypedFact]) -> Result<Self, TypedFactSetError> {
        Ok(Self(
            decode_fact::<A>(facts)?,
            decode_fact::<B>(facts)?,
            decode_fact::<C>(facts)?,
        ))
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

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Second {
        value: String,
    }

    impl TypedPayload for Second {
        const EVENT_TYPE: &'static str = "fact.second";
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
    fn product_fact_set_preserves_member_order_and_types() {
        let fact_types = Facts2::<First, Second>::fact_types();
        assert_eq!(fact_types.len(), 2);
        assert_eq!(fact_types[0].event_type, "fact.first.v1");
        assert_eq!(fact_types[1].event_type, "fact.second.v1");

        let facts = Facts2(
            First { value: 7 },
            Second {
                value: "ok".to_string(),
            },
        )
        .into_facts()
        .expect("product fact set serializes");

        assert_eq!(facts.len(), 2);
        assert_eq!(facts[0].event_type, "fact.first.v1");
        assert_eq!(facts[1].event_type, "fact.second.v1");
    }

    #[test]
    fn product_fact_set_reconstructs_from_member_facts_by_event_type() {
        let facts = vec![
            TypedFact::from_payload(Second {
                value: "ok".to_string(),
            })
            .expect("second fact"),
            TypedFact::from_payload(First { value: 7 }).expect("first fact"),
        ];

        let reconstructed =
            Facts2::<First, Second>::try_from_facts(&facts).expect("reconstruct product");

        assert_eq!(
            reconstructed,
            Facts2(
                First { value: 7 },
                Second {
                    value: "ok".to_string()
                }
            )
        );
    }

    #[test]
    fn product_fact_set_reconstruction_rejects_missing_member() {
        let facts = vec![TypedFact::from_payload(First { value: 7 }).expect("first fact")];

        let err = Facts2::<First, Second>::try_from_facts(&facts)
            .expect_err("missing second fact should fail");

        assert!(matches!(
            err,
            TypedFactSetError::MissingFact { event_type } if event_type == "fact.second.v1"
        ));
    }
}
