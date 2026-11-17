// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Outcomes.
//!
//! A typed transform returns one fact, an `outcome![...]` list or
//! [`Outcomes`], each of which lowers to flat named facts through the
//! shared [`OutcomeFacts`] marshalling and [`Outcome`] membership
//! machinery. The returned value is never persisted.

use super::member_set::{EmptySet, WithMember};
use super::outcome::Outcome;
use super::outcome_facts::{OutcomeError, OutcomeFacts, TypedFact, TypedFactType};
use super::outcome_list::{OutcomeOr, SingleFactCases};
use super::typed_payload::TypedPayload;

/// Lowering-only outcome of zero or more facts of one payload type.
///
/// `Outcomes<T>` supplies dynamic cardinality without turning `Vec<T>` into
/// framework vocabulary. The outcome is never persisted: each contained value
/// lowers directly to a `T` fact in insertion order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outcomes<T>(Vec<T>);

impl<T: TypedPayload> Outcomes<T> {
    /// Return an outcome that lowers to no facts.
    pub fn none() -> Self {
        Self(Vec::new())
    }

    /// Return an outcome that lowers to one fact.
    pub fn one(value: T) -> Self {
        Self(vec![value])
    }

    /// Return an outcome that lowers all supplied values in iteration order.
    pub fn many(values: impl IntoIterator<Item = T>) -> Self {
        Self(values.into_iter().collect())
    }
}

impl<T> Outcome for Outcomes<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    type Members = WithMember<T, EmptySet>;

    fn member_fact_types() -> Vec<TypedFactType> {
        vec![TypedFactType::of::<T>()]
    }
}

impl<T> OutcomeFacts for Outcomes<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    fn fact_types() -> Vec<TypedFactType> {
        vec![TypedFactType::of::<T>()]
    }

    fn into_facts(self) -> Result<Vec<TypedFact>, OutcomeError> {
        self.0.into_iter().map(TypedFact::from_payload).collect()
    }

    fn try_from_facts(facts: &[TypedFact]) -> Result<Self, OutcomeError> {
        if let Some(unexpected) = facts.iter().find(|fact| !T::matches_fact(fact)) {
            return Err(OutcomeError::UnexpectedFact {
                event_type: unexpected.event_type.clone(),
            });
        }

        facts
            .iter()
            .map(|fact| {
                serde_json::from_value(
                    fact.payload
                        .contract_body()
                        .map_err(|e| OutcomeError::SerializationFailed(e.to_string()))?,
                )
                .map_err(|error| OutcomeError::DeserializationFailed {
                    event_type: fact.event_type.clone(),
                    error: error.to_string(),
                })
            })
            .collect::<Result<Vec<T>, _>>()
            .map(Self)
    }
}

/// The `Output` bound of a typed transform and an AI role's `Out`: one fact,
/// an `outcome![...]` list or [`Outcomes`].
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be a transform's `Output` or an AI role's `Out`",
    label = "a transform returns one fact, `outcome![...]` or `Outcomes<T>`",
    note = "a transform returns exactly one outcome per input, listed as `outcome![A | (B, C)]`, \
            or any number of facts of one type as `Outcomes<T>`"
)]
pub trait TransformOutcome: OutcomeFacts + Outcome {}

impl<T: TypedPayload + Send + Sync + 'static> TransformOutcome for T {}

impl<Head, Rest> TransformOutcome for OutcomeOr<Head, Rest> where Self: OutcomeFacts + Outcome {}

impl<T: TypedPayload + Send + Sync + 'static> TransformOutcome for Outcomes<T> {}

/// The `Output` bound of a stateful stage, a source and a join, each of whose
/// values lowers to exactly one fact: one fact, or an `outcome![...]` list
/// whose outcomes are single facts.
///
/// # Contract
///
/// Every value must lower to exactly one fact, and every member fact type
/// must reconstruct when passed alone to [`OutcomeFacts::try_from_facts`]. A
/// manual implementation is a trusted semantic assertion; if a lowered value
/// contradicts it, the runtime treats the contradiction as a fatal
/// stage-contract violation.
///
/// A list with a tuple outcome is refused:
///
/// ```compile_fail
/// # use obzenflow_core::{outcome, OneFactOutcome, TypedPayload};
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Valid;
/// # impl TypedPayload for Valid { const EVENT_TYPE: &'static str = "doc.valid"; }
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Invalid;
/// # impl TypedPayload for Invalid { const EVENT_TYPE: &'static str = "doc.invalid"; }
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Cancelled;
/// # impl TypedPayload for Cancelled { const EVENT_TYPE: &'static str = "doc.cancelled"; }
/// fn source_output<O: OneFactOutcome>() {}
/// source_output::<outcome![Valid | (Invalid, Cancelled)]>();
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be a stateful stage's, a source's or a join's `Output`",
    label = "these stages write one fact per value, so one fact or `outcome![A | B]` without a \
             tuple",
    note = "facts written together as a tuple belong in a transform's `outcome![...]`"
)]
pub trait OneFactOutcome: TransformOutcome {}

impl<T: TypedPayload + Send + Sync + 'static> OneFactOutcome for T {}

impl<Head, Rest> OneFactOutcome for OutcomeOr<Head, Rest> where
    Self: TransformOutcome + SingleFactCases
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::schema::{EffectOutcome, EffectfulOutcome, OutcomeError, TransformOutcome};
    use serde::{Deserialize, Serialize, Serializer};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Solo {
        value: u32,
    }

    impl TypedPayload for Solo {
        const EVENT_TYPE: &'static str = "outcomes.solo";
    }

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    struct SerializationFails;

    impl Serialize for SerializationFails {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            Err(serde::ser::Error::custom(
                "deliberate serialization failure",
            ))
        }
    }

    impl TypedPayload for SerializationFails {
        const EVENT_TYPE: &'static str = "outcomes.serialization_fails";
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Valid {
        value: u32,
    }

    impl TypedPayload for Valid {
        const EVENT_TYPE: &'static str = "outcomes.valid";
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Invalid {
        reason: String,
    }

    impl TypedPayload for Invalid {
        const EVENT_TYPE: &'static str = "outcomes.invalid";
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Cancelled {
        reason: String,
    }

    impl TypedPayload for Cancelled {
        const EVENT_TYPE: &'static str = "outcomes.cancelled";
    }

    fn is_transform_outcome<T: TransformOutcome>() {}
    fn is_one_fact_outcome<T: OneFactOutcome>() {}
    fn is_effect_outcome<T: EffectOutcome>() {}
    fn is_effectful_outcome<T: EffectfulOutcome>() {}

    #[test]
    fn scalar_payload_satisfies_outcome_bounds() {
        is_transform_outcome::<Solo>();
        is_one_fact_outcome::<Solo>();
    }

    #[test]
    fn position_bounds_accept_listed_forms() {
        is_transform_outcome::<crate::outcome![Valid | (Invalid, Cancelled)]>();
        is_effect_outcome::<crate::outcome![Valid | (Invalid, Cancelled)]>();
        is_one_fact_outcome::<crate::outcome![Valid | Invalid]>();
        is_transform_outcome::<Outcomes<Solo>>();
        is_effectful_outcome::<crate::outcome![Valid | Invalid]>();
        is_effectful_outcome::<Solo>();
    }

    #[test]
    fn dynamic_homogeneous_outputs_lower_zero_one_and_many_in_order() {
        assert!(Outcomes::<Solo>::none()
            .into_facts()
            .expect("empty output lowers")
            .is_empty());

        let one = Outcomes::one(Solo { value: 4 })
            .into_facts()
            .expect("one output lowers");
        assert_eq!(one.len(), 1);
        assert_eq!(
            serde_json::to_value(&one[0].payload).unwrap(),
            serde_json::json!({ "value": 4 })
        );

        let many = Outcomes::many([Solo { value: 3 }, Solo { value: 7 }])
            .into_facts()
            .expect("many outputs lower");
        assert_eq!(many.len(), 2);
        assert_eq!(
            serde_json::to_value(&many[0].payload).unwrap(),
            serde_json::json!({ "value": 3 })
        );
        assert_eq!(
            serde_json::to_value(&many[1].payload).unwrap(),
            serde_json::json!({ "value": 7 })
        );
        assert_eq!(
            <Outcomes<Solo> as Outcome>::member_fact_types(),
            <Solo as Outcome>::member_fact_types()
        );
    }

    #[test]
    fn dynamic_homogeneous_outputs_reconstruct_repeated_facts() {
        let original = Outcomes::many([Solo { value: 2 }, Solo { value: 2 }]);
        let facts = original.clone().into_facts().expect("outputs lower");
        assert_eq!(
            Outcomes::<Solo>::try_from_facts(&facts).expect("outputs reconstruct"),
            original
        );
    }

    #[test]
    fn dynamic_homogeneous_outputs_propagate_serialization_failure() {
        assert!(matches!(
            Outcomes::one(SerializationFails).into_facts(),
            Err(OutcomeError::SerializationFailed(message))
                if message.contains("deliberate serialization failure")
        ));
    }
}
