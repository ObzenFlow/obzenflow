// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed fact-set authoring support.

use crate::event::chain_event::{ChainEvent, ChainEventContent};
use crate::event::schema::typed_payload::TypedPayload;
use crate::event::types::EventType;
use serde_json::Value;
use std::any::{type_name, TypeId};

/// Type metadata for one member of a typed fact set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypedFactType {
    pub type_id: TypeId,
    pub display_name: String,
    pub event_type: EventType,
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
            event_type: EventType::from(T::versioned_event_type()),
            schema_version: T::SCHEMA_VERSION,
        }
    }
}

/// Serialized fact ready for conversion into a `ChainEvent::Data` value.
#[derive(Clone, Debug, PartialEq)]
pub struct TypedFact {
    pub event_type: EventType,
    pub payload: Value,
}

impl TypedFact {
    pub fn from_payload<T>(payload: T) -> Result<Self, TypedFactSetError>
    where
        T: TypedPayload,
    {
        Ok(Self {
            event_type: EventType::from(T::versioned_event_type()),
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
                event_type: EventType::from(event_type.clone()),
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
    MissingFact { event_type: EventType },

    #[error("duplicate typed fact `{event_type}`")]
    DuplicateFact { event_type: EventType },

    #[error("failed to deserialize typed fact `{event_type}`: {error}")]
    DeserializationFailed {
        event_type: EventType,
        error: String,
    },

    #[error("unexpected typed fact `{event_type}` in the recorded fact group")]
    UnexpectedFact { event_type: EventType },
}

/// Build the `MissingFact` error for an empty recorded group, naming every
/// declared member type. `#[doc(hidden)]`: an `effect_outcome!` expansion
/// detail (FLOWIP-120m).
#[doc(hidden)]
pub fn missing_fact_group_error(declared: &[TypedFactType]) -> TypedFactSetError {
    TypedFactSetError::MissingFact {
        event_type: EventType::from(
            declared
                .iter()
                .map(|fact| fact.event_type.as_str())
                .collect::<Vec<_>>()
                .join(" | "),
        ),
    }
}

/// Classify a multi-fact group decoded as a sum carrier, whose cardinality is
/// exactly one fact: a repeat of the first fact's type is `DuplicateFact`,
/// and a second declared-but-different member is `UnexpectedFact` (mixed sum
/// variants). `#[doc(hidden)]`: an `effect_outcome!` expansion detail
/// (FLOWIP-120m).
#[doc(hidden)]
pub fn sum_group_arity_error(first: &TypedFact, rest: &[TypedFact]) -> TypedFactSetError {
    if rest.iter().any(|fact| fact.event_type == first.event_type) {
        TypedFactSetError::DuplicateFact {
            event_type: first.event_type.clone(),
        }
    } else {
        TypedFactSetError::UnexpectedFact {
            event_type: rest
                .first()
                .map(|fact| fact.event_type.clone())
                .unwrap_or_else(|| first.event_type.clone()),
        }
    }
}

/// A typed marshalling bridge for code paths that still produce a typed value
/// before it is converted into `Data` facts.
///
/// The blanket `TypedPayload -> TypedFactSet` implementation keeps scalar
/// stateful outputs working while FLOWIP-120b moves effectful authoring to
/// explicit `fx.emit` calls and flat output contracts.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not lower to named facts",
    note = "a closed set of named facts is written as an enum or named-field struct with \
            #[derive(EffectOutcomeFacts)]; a single fact is its `TypedPayload` type directly \
            (FLOWIP-120m)"
)]
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

/// Decode the recorded group as exactly one `T` fact, failing closed.
///
/// Exactness rule (FLOWIP-120m): any fact in the group that does not match
/// `T` is `UnexpectedFact`; an all-matching group of zero is `MissingFact`
/// and of more than one is `DuplicateFact`. The recorded group is the
/// carrier's facts and nothing else, so a foreign fact is corruption, never
/// something to skip.
fn decode_fact<T>(facts: &[TypedFact]) -> Result<T, TypedFactSetError>
where
    T: TypedPayload,
{
    if let Some(unexpected) = facts
        .iter()
        .find(|fact| !T::event_type_matches(fact.event_type.as_str()))
    {
        return Err(TypedFactSetError::UnexpectedFact {
            event_type: unexpected.event_type.clone(),
        });
    }
    decode_member_fact::<T>(facts)
}

/// Decode exactly one `T` fact from a group, ignoring facts of other types.
///
/// Carrier-internal building block (FLOWIP-120m): a multi-member carrier's
/// `try_from_facts` scans the whole group for undeclared facts itself, then
/// decodes each declared member with this helper, which must therefore
/// tolerate the sibling members it is not looking for. Whole-group exactness
/// is the carrier's job, not this helper's; use the strict scalar decode
/// through `TypedFactSet::try_from_facts` everywhere else.
#[doc(hidden)]
pub fn decode_member_fact<T>(facts: &[TypedFact]) -> Result<T, TypedFactSetError>
where
    T: TypedPayload,
{
    let mut matches = facts
        .iter()
        .filter(|fact| T::event_type_matches(fact.event_type.as_str()));
    let fact = matches
        .next()
        .ok_or_else(|| TypedFactSetError::MissingFact {
            event_type: EventType::from(T::versioned_event_type()),
        })?;
    if matches.next().is_some() {
        return Err(TypedFactSetError::DuplicateFact {
            event_type: EventType::from(T::versioned_event_type()),
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

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Second {
        label: String,
    }

    impl TypedPayload for Second {
        const EVENT_TYPE: &'static str = "fact.second";
    }

    #[test]
    fn scalar_decode_rejects_group_with_unexpected_fact() {
        let facts = vec![
            TypedFact::from_payload(First { value: 7 }).expect("first fact"),
            TypedFact::from_payload(Second {
                label: "stray".to_string(),
            })
            .expect("second fact"),
        ];

        let err = First::try_from_facts(&facts).expect_err("foreign fact must fail closed");

        assert!(matches!(
            err,
            TypedFactSetError::UnexpectedFact { ref event_type }
                if event_type.as_str() == "fact.second.v1"
        ));
    }

    #[test]
    fn scalar_decode_rejects_duplicate_matching_facts() {
        let facts = vec![
            TypedFact::from_payload(First { value: 1 }).expect("first fact"),
            TypedFact::from_payload(First { value: 2 }).expect("duplicate fact"),
        ];

        let err = First::try_from_facts(&facts).expect_err("duplicate must fail closed");

        assert!(matches!(err, TypedFactSetError::DuplicateFact { .. }));
    }

    #[test]
    fn decode_member_fact_ignores_sibling_member_facts() {
        let facts = vec![
            TypedFact::from_payload(First { value: 7 }).expect("first fact"),
            TypedFact::from_payload(Second {
                label: "sibling".to_string(),
            })
            .expect("second fact"),
        ];

        let first = decode_member_fact::<First>(&facts).expect("member decode tolerates siblings");
        let second = decode_member_fact::<Second>(&facts).expect("member decode finds sibling");

        assert_eq!(first, First { value: 7 });
        assert_eq!(
            second,
            Second {
                label: "sibling".to_string()
            }
        );
    }

    /// Hand-written sum carrier: the manual-carrier doctrine for FLOWIP-120m.
    /// One persisted fact per variant, exact fail-closed reconstruction. The
    /// `effect_outcome!` macro generates this shape; the hand-written form
    /// stays here as the documented pattern.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum HandWrittenSum {
        First(First),
        Second(Second),
    }

    impl TypedFactSet for HandWrittenSum {
        fn fact_types() -> Vec<TypedFactType> {
            vec![TypedFactType::of::<First>(), TypedFactType::of::<Second>()]
        }

        fn into_facts(self) -> Result<Vec<TypedFact>, TypedFactSetError> {
            match self {
                Self::First(fact) => Ok(vec![TypedFact::from_payload(fact)?]),
                Self::Second(fact) => Ok(vec![TypedFact::from_payload(fact)?]),
            }
        }

        fn try_from_facts(facts: &[TypedFact]) -> Result<Self, TypedFactSetError> {
            if let Some(unexpected) = facts.iter().find(|fact| {
                !First::event_type_matches(fact.event_type.as_str())
                    && !Second::event_type_matches(fact.event_type.as_str())
            }) {
                return Err(TypedFactSetError::UnexpectedFact {
                    event_type: unexpected.event_type.clone(),
                });
            }
            match facts {
                [single] => {
                    if First::event_type_matches(single.event_type.as_str()) {
                        return Ok(Self::First(decode_member_fact::<First>(facts)?));
                    }
                    Ok(Self::Second(decode_member_fact::<Second>(facts)?))
                }
                [] => Err(TypedFactSetError::MissingFact {
                    event_type: EventType::from(format!(
                        "{} | {}",
                        First::versioned_event_type(),
                        Second::versioned_event_type()
                    )),
                }),
                [first, ..] => Err(TypedFactSetError::DuplicateFact {
                    event_type: first.event_type.clone(),
                }),
            }
        }
    }

    #[test]
    fn hand_written_sum_carrier_is_exact() {
        for carrier in [
            HandWrittenSum::First(First { value: 3 }),
            HandWrittenSum::Second(Second {
                label: "declined".to_string(),
            }),
        ] {
            let facts = carrier.clone().into_facts().expect("variant serializes");
            assert_eq!(facts.len(), 1);
            let reconstructed =
                HandWrittenSum::try_from_facts(&facts).expect("variant reconstructs");
            assert_eq!(reconstructed, carrier);
        }

        let empty: Vec<TypedFact> = Vec::new();
        assert!(matches!(
            HandWrittenSum::try_from_facts(&empty),
            Err(TypedFactSetError::MissingFact { .. })
        ));

        let mixed = vec![
            TypedFact::from_payload(First { value: 1 }).expect("first"),
            TypedFact::from_payload(Second {
                label: "mixed".to_string(),
            })
            .expect("second"),
        ];
        assert!(matches!(
            HandWrittenSum::try_from_facts(&mixed),
            Err(TypedFactSetError::DuplicateFact { .. })
        ));

        let unknown = vec![TypedFact {
            event_type: EventType::from("fact.unknown.v1"),
            payload: serde_json::json!({}),
        }];
        assert!(matches!(
            HandWrittenSum::try_from_facts(&unknown),
            Err(TypedFactSetError::UnexpectedFact { .. })
        ));
    }

    /// Hand-written product carrier: exact multiset reconstruction.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct HandWrittenProduct {
        first: First,
        second: Second,
    }

    impl TypedFactSet for HandWrittenProduct {
        fn fact_types() -> Vec<TypedFactType> {
            vec![TypedFactType::of::<First>(), TypedFactType::of::<Second>()]
        }

        fn into_facts(self) -> Result<Vec<TypedFact>, TypedFactSetError> {
            Ok(vec![
                TypedFact::from_payload(self.first)?,
                TypedFact::from_payload(self.second)?,
            ])
        }

        fn try_from_facts(facts: &[TypedFact]) -> Result<Self, TypedFactSetError> {
            if let Some(unexpected) = facts.iter().find(|fact| {
                !First::event_type_matches(fact.event_type.as_str())
                    && !Second::event_type_matches(fact.event_type.as_str())
            }) {
                return Err(TypedFactSetError::UnexpectedFact {
                    event_type: unexpected.event_type.clone(),
                });
            }
            Ok(Self {
                first: decode_member_fact::<First>(facts)?,
                second: decode_member_fact::<Second>(facts)?,
            })
        }
    }

    #[test]
    fn hand_written_product_carrier_requires_multiset_equality() {
        let carrier = HandWrittenProduct {
            first: First { value: 9 },
            second: Second {
                label: "audit".to_string(),
            },
        };
        let facts = carrier.clone().into_facts().expect("product serializes");
        assert_eq!(facts.len(), 2);
        assert_eq!(
            HandWrittenProduct::try_from_facts(&facts).expect("product reconstructs"),
            carrier
        );

        let missing = vec![TypedFact::from_payload(First { value: 9 }).expect("first")];
        assert!(matches!(
            HandWrittenProduct::try_from_facts(&missing),
            Err(TypedFactSetError::MissingFact { .. })
        ));

        let duplicated = vec![
            TypedFact::from_payload(First { value: 9 }).expect("first"),
            TypedFact::from_payload(First { value: 10 }).expect("duplicate"),
            TypedFact::from_payload(Second {
                label: "audit".to_string(),
            })
            .expect("second"),
        ];
        assert!(matches!(
            HandWrittenProduct::try_from_facts(&duplicated),
            Err(TypedFactSetError::DuplicateFact { .. })
        ));

        let mut with_unknown = carrier.into_facts().expect("product serializes");
        with_unknown.push(TypedFact {
            event_type: EventType::from("fact.unknown.v1"),
            payload: serde_json::json!({}),
        });
        assert!(matches!(
            HandWrittenProduct::try_from_facts(&with_unknown),
            Err(TypedFactSetError::UnexpectedFact { .. })
        ));
    }
}
