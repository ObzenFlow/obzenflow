// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed fact-set authoring support.

use crate::config::LineagePolicy;
use crate::event::chain_event::{ChainEvent, ChainPayload};
use crate::event::schema::typed_payload::TypedPayload;
use crate::event::types::EventType;
use crate::event::ChainEventFactory;
use crate::WriterId;
use std::any::{type_name, TypeId};

/// Type metadata for one fact type of an outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypedFactType {
    pub type_id: TypeId,
    pub display_name: String,
    pub event_type: EventType,
    pub event_kind: crate::event::payloads::chain_payload::EventKind,
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
            event_type: EventType::from(T::event_type_name()),
            event_kind: T::EVENT_KIND,
            schema_version: T::payload_schema_version().get(),
        }
    }
}

/// Serialized fact ready for conversion into a `ChainEvent::Data` value.
#[derive(Clone, Debug)]
pub struct TypedFact {
    pub event_type: EventType,
    pub payload_schema_version: std::num::NonZeroU32,
    pub payload: ChainPayload,
}

impl TypedFact {
    pub fn descriptor(&self) -> crate::event::types::EventDescriptor {
        crate::event::types::EventDescriptor {
            event_kind: self.payload.kind(),
            event_type: self.event_type.clone(),
            payload_schema_version: self.payload_schema_version,
        }
    }

    pub fn from_payload<T>(payload: T) -> Result<Self, OutcomeError>
    where
        T: TypedPayload,
    {
        Ok(Self {
            event_type: EventType::from(T::event_type_name()),
            payload_schema_version: T::payload_schema_version(),
            payload: payload
                .into_chain_payload()
                .map_err(|e| OutcomeError::SerializationFailed(e.to_string()))?,
        })
    }

    pub fn from_event(event: &ChainEvent) -> Option<Self> {
        event.is_typed_input().then(|| Self {
            event_type: EventType::from(event.event_type()),
            payload_schema_version: event.payload_schema_version,
            payload: event.payload.clone(),
        })
    }

    pub fn into_derived_event(
        self,
        writer_id: WriterId,
        parent: &ChainEvent,
        lineage: LineagePolicy,
    ) -> ChainEvent {
        ChainEventFactory::derived_event(
            writer_id,
            parent,
            self.payload,
            self.event_type,
            self.payload_schema_version,
            lineage,
        )
    }

    pub fn into_event(self, writer_id: WriterId) -> ChainEvent {
        ChainEventFactory::create_event(
            writer_id,
            self.payload,
            self.event_type,
            self.payload_schema_version,
        )
    }
}

/// Error returned while turning an outcome into facts or back.
#[derive(Debug, Clone, thiserror::Error)]
pub enum OutcomeError {
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

/// Build the `MissingFact` error for a recorded group that matches no
/// outcome, naming every declared member type.
#[doc(hidden)]
pub fn missing_fact_group_error(declared: &[TypedFactType]) -> OutcomeError {
    OutcomeError::MissingFact {
        event_type: EventType::from(
            declared
                .iter()
                .map(|fact| fact.event_type.as_str())
                .collect::<Vec<_>>()
                .join(" | "),
        ),
    }
}

/// A typed marshalling bridge between a value and the `Data` facts it lowers
/// to: one fact, an `outcome![...]` list or `Outcomes<T>`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not lower to named facts",
    note = "write one fact, or `outcome![A | (B, C)]` for exactly one of several outcomes"
)]
pub trait OutcomeFacts: Send + Sync + 'static {
    fn fact_types() -> Vec<TypedFactType>;

    fn into_facts(self) -> Result<Vec<TypedFact>, OutcomeError>;

    fn try_from_facts(facts: &[TypedFact]) -> Result<Self, OutcomeError>
    where
        Self: Sized;
}

impl<T> OutcomeFacts for T
where
    T: TypedPayload + Send + Sync + 'static,
{
    fn fact_types() -> Vec<TypedFactType> {
        vec![TypedFactType::of::<T>()]
    }

    fn into_facts(self) -> Result<Vec<TypedFact>, OutcomeError> {
        Ok(vec![TypedFact::from_payload(self)?])
    }

    fn try_from_facts(facts: &[TypedFact]) -> Result<Self, OutcomeError> {
        decode_fact::<T>(facts)
    }
}

/// Decode the recorded group as exactly one `T` fact, failing closed.
///
/// Exactness rule (FLOWIP-120m): any fact in the group that does not match
/// `T` is `UnexpectedFact`; an all-matching group of zero is `MissingFact`
/// and of more than one is `DuplicateFact`. The recorded group is the
/// outcome's facts and nothing else, so a foreign fact is corruption, never
/// something to skip.
fn decode_fact<T>(facts: &[TypedFact]) -> Result<T, OutcomeError>
where
    T: TypedPayload,
{
    if let Some(unexpected) = facts.iter().find(|fact| !T::matches_fact(fact)) {
        return Err(OutcomeError::UnexpectedFact {
            event_type: unexpected.event_type.clone(),
        });
    }
    decode_member_fact::<T>(facts)
}

/// Decode exactly one `T` fact from a group, ignoring facts of other types.
///
/// A multi-fact outcome's `try_from_facts` scans the whole group for
/// undeclared facts itself, then decodes each member with this helper, which
/// therefore tolerates the sibling members it is not looking for. Use the
/// strict scalar decode through `OutcomeFacts::try_from_facts` everywhere
/// else.
#[doc(hidden)]
pub fn decode_member_fact<T>(facts: &[TypedFact]) -> Result<T, OutcomeError>
where
    T: TypedPayload,
{
    let mut matches = facts.iter().filter(|fact| T::matches_fact(fact));
    let fact = matches.next().ok_or_else(|| OutcomeError::MissingFact {
        event_type: EventType::from(T::event_type_name()),
    })?;
    if matches.next().is_some() {
        return Err(OutcomeError::DuplicateFact {
            event_type: EventType::from(T::event_type_name()),
        });
    }
    serde_json::from_value(
        fact.payload
            .contract_body()
            .map_err(|e| OutcomeError::SerializationFailed(e.to_string()))?,
    )
    .map_err(|e| OutcomeError::DeserializationFailed {
        event_type: fact.event_type.clone(),
        error: e.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::ChainPayload;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct First {
        value: u32,
    }

    impl TypedPayload for First {
        const EVENT_TYPE: &'static str = "fact.first";
    }

    #[test]
    fn scalar_outcome_serializes_one_fact() {
        let facts = First { value: 7 }
            .into_facts()
            .expect("scalar outcome serializes");

        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].event_type, "fact.first");
        assert_eq!(
            serde_json::to_value(&facts[0].payload).unwrap(),
            serde_json::json!({ "value": 7 })
        );
    }

    #[test]
    fn scalar_outcome_reconstructs_by_event_type() {
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
            OutcomeError::UnexpectedFact { ref event_type }
                if event_type.as_str() == "fact.second"
        ));
    }

    #[test]
    fn scalar_decode_rejects_duplicate_matching_facts() {
        let facts = vec![
            TypedFact::from_payload(First { value: 1 }).expect("first fact"),
            TypedFact::from_payload(First { value: 2 }).expect("duplicate fact"),
        ];

        let err = First::try_from_facts(&facts).expect_err("duplicate must fail closed");

        assert!(matches!(err, OutcomeError::DuplicateFact { .. }));
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

    /// Hand-written sum outcome: the manual outcome pattern for FLOWIP-120m.
    /// One persisted fact per variant, exact fail-closed reconstruction.
    /// `outcome![...]` generates this shape; the hand-written form stays here
    /// as the documented pattern.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum HandWrittenSum {
        First(First),
        Second(Second),
    }

    impl OutcomeFacts for HandWrittenSum {
        fn fact_types() -> Vec<TypedFactType> {
            vec![TypedFactType::of::<First>(), TypedFactType::of::<Second>()]
        }

        fn into_facts(self) -> Result<Vec<TypedFact>, OutcomeError> {
            match self {
                Self::First(fact) => Ok(vec![TypedFact::from_payload(fact)?]),
                Self::Second(fact) => Ok(vec![TypedFact::from_payload(fact)?]),
            }
        }

        fn try_from_facts(facts: &[TypedFact]) -> Result<Self, OutcomeError> {
            if let Some(unexpected) = facts
                .iter()
                .find(|fact| !First::matches_fact(fact) && !Second::matches_fact(fact))
            {
                return Err(OutcomeError::UnexpectedFact {
                    event_type: unexpected.event_type.clone(),
                });
            }
            match facts {
                [single] => {
                    if First::matches_fact(single) {
                        return Ok(Self::First(decode_member_fact::<First>(facts)?));
                    }
                    Ok(Self::Second(decode_member_fact::<Second>(facts)?))
                }
                [] => Err(OutcomeError::MissingFact {
                    event_type: EventType::from(format!(
                        "{} | {}",
                        First::event_type_name(),
                        Second::event_type_name()
                    )),
                }),
                [first, ..] => Err(OutcomeError::DuplicateFact {
                    event_type: first.event_type.clone(),
                }),
            }
        }
    }

    #[test]
    fn hand_written_sum_outcome_round_trips() {
        for outcome in [
            HandWrittenSum::First(First { value: 3 }),
            HandWrittenSum::Second(Second {
                label: "declined".to_string(),
            }),
        ] {
            let facts = outcome.clone().into_facts().expect("variant serializes");
            assert_eq!(facts.len(), 1);
            let reconstructed =
                HandWrittenSum::try_from_facts(&facts).expect("variant reconstructs");
            assert_eq!(reconstructed, outcome);
        }

        let empty: Vec<TypedFact> = Vec::new();
        assert!(matches!(
            HandWrittenSum::try_from_facts(&empty),
            Err(OutcomeError::MissingFact { .. })
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
            Err(OutcomeError::DuplicateFact { .. })
        ));

        let unknown = vec![TypedFact {
            payload_schema_version: std::num::NonZeroU32::MIN,
            event_type: EventType::from("fact.unknown"),
            payload: ChainPayload::Fact(serde_json::json!({})),
        }];
        assert!(matches!(
            HandWrittenSum::try_from_facts(&unknown),
            Err(OutcomeError::UnexpectedFact { .. })
        ));
    }

    /// Hand-written product outcome: exact multiset reconstruction.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct HandWrittenProduct {
        first: First,
        second: Second,
    }

    impl OutcomeFacts for HandWrittenProduct {
        fn fact_types() -> Vec<TypedFactType> {
            vec![TypedFactType::of::<First>(), TypedFactType::of::<Second>()]
        }

        fn into_facts(self) -> Result<Vec<TypedFact>, OutcomeError> {
            Ok(vec![
                TypedFact::from_payload(self.first)?,
                TypedFact::from_payload(self.second)?,
            ])
        }

        fn try_from_facts(facts: &[TypedFact]) -> Result<Self, OutcomeError> {
            if let Some(unexpected) = facts
                .iter()
                .find(|fact| !First::matches_fact(fact) && !Second::matches_fact(fact))
            {
                return Err(OutcomeError::UnexpectedFact {
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
    fn product_outcome_multiset_mismatch_refused() {
        let outcome = HandWrittenProduct {
            first: First { value: 9 },
            second: Second {
                label: "audit".to_string(),
            },
        };
        let facts = outcome.clone().into_facts().expect("product serializes");
        assert_eq!(facts.len(), 2);
        assert_eq!(
            HandWrittenProduct::try_from_facts(&facts).expect("product reconstructs"),
            outcome
        );

        let missing = vec![TypedFact::from_payload(First { value: 9 }).expect("first")];
        assert!(matches!(
            HandWrittenProduct::try_from_facts(&missing),
            Err(OutcomeError::MissingFact { .. })
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
            Err(OutcomeError::DuplicateFact { .. })
        ));

        let mut with_unknown = outcome.into_facts().expect("product serializes");
        with_unknown.push(TypedFact {
            payload_schema_version: std::num::NonZeroU32::MIN,
            event_type: EventType::from("fact.unknown"),
            payload: ChainPayload::Fact(serde_json::json!({})),
        });
        assert!(matches!(
            HandWrittenProduct::try_from_facts(&with_unknown),
            Err(OutcomeError::UnexpectedFact { .. })
        ));
    }
}
