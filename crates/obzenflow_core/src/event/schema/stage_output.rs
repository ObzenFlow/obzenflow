// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stage outputs.
//!
//! A typed transform returns one fact, an `outcome![...]` list or
//! [`StageOutputs`], each of which lowers to flat named facts through the
//! shared [`TypedFactSet`] marshalling and [`StageFactSet`] membership
//! machinery. The returned value is never persisted.

use super::member_set::{EmptySet, WithMember};
use super::outcome_list::{OutcomeOr, SingleFactOutcomes};
use super::stage_fact_set::StageFactSet;
use super::typed_fact_set::{TypedFact, TypedFactSet, TypedFactSetError, TypedFactType};
use super::typed_payload::TypedPayload;

/// Lowering-only carrier for zero or more facts of one payload type.
///
/// `StageOutputs<T>` supplies dynamic cardinality without turning `Vec<T>` into
/// framework vocabulary. The carrier is never persisted: each contained value
/// lowers directly to a `T` fact in insertion order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageOutputs<T>(Vec<T>);

impl<T: TypedPayload> StageOutputs<T> {
    /// Return a carrier that lowers to no facts.
    pub fn none() -> Self {
        Self(Vec::new())
    }

    /// Return a carrier that lowers to one fact.
    pub fn one(value: T) -> Self {
        Self(vec![value])
    }

    /// Return a carrier that lowers all supplied values in iteration order.
    pub fn many(values: impl IntoIterator<Item = T>) -> Self {
        Self(values.into_iter().collect())
    }
}

impl<T> StageFactSet for StageOutputs<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    type Members = WithMember<T, EmptySet>;

    fn member_fact_types() -> Vec<TypedFactType> {
        vec![TypedFactType::of::<T>()]
    }
}

impl<T> TypedFactSet for StageOutputs<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    fn fact_types() -> Vec<TypedFactType> {
        vec![TypedFactType::of::<T>()]
    }

    fn into_facts(self) -> Result<Vec<TypedFact>, TypedFactSetError> {
        self.0.into_iter().map(TypedFact::from_payload).collect()
    }

    fn try_from_facts(facts: &[TypedFact]) -> Result<Self, TypedFactSetError> {
        if let Some(unexpected) = facts.iter().find(|fact| !T::matches_fact(fact)) {
            return Err(TypedFactSetError::UnexpectedFact {
                event_type: unexpected.event_type.clone(),
            });
        }

        facts
            .iter()
            .map(|fact| {
                serde_json::from_value(
                    fact.payload
                        .contract_body()
                        .map_err(|e| TypedFactSetError::SerializationFailed(e.to_string()))?,
                )
                .map_err(|error| TypedFactSetError::DeserializationFailed {
                    event_type: fact.event_type.clone(),
                    error: error.to_string(),
                })
            })
            .collect::<Result<Vec<T>, _>>()
            .map(Self)
    }
}

/// The `Output` bound of a typed transform and an AI role's `Out`: one fact,
/// an `outcome![...]` list or [`StageOutputs`].
///
/// `stage_fact_set![...]` is refused:
///
/// ```compile_fail
/// # use obzenflow_core::{stage_fact_set, StageOutputFacts, TypedPayload};
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Valid;
/// # impl TypedPayload for Valid { const EVENT_TYPE: &'static str = "doc.valid"; }
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Invalid;
/// # impl TypedPayload for Invalid { const EVENT_TYPE: &'static str = "doc.invalid"; }
/// fn transform_output<O: StageOutputFacts>() {}
/// transform_output::<stage_fact_set![Valid, Invalid]>();
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be a transform's `Output` or an AI role's `Out`",
    label = "a transform returns one fact, `outcome![...]` or `StageOutputs<T>`",
    note = "a transform returns exactly one outcome per input, listed as `outcome![A | (B, C)]`, \
            or any number of facts of one type as `StageOutputs<T>`; `stage_fact_set![...]` is \
            the set an effectful handler may emit"
)]
pub trait StageOutputFacts: TypedFactSet + StageFactSet {}

impl<T: TypedPayload + Send + Sync + 'static> StageOutputFacts for T {}

impl<Head, Rest> StageOutputFacts for OutcomeOr<Head, Rest> where Self: TypedFactSet + StageFactSet {}

impl<T: TypedPayload + Send + Sync + 'static> StageOutputFacts for StageOutputs<T> {}

/// The `Output` bound of a stateful stage, a source and a join, each of whose
/// values lowers to exactly one fact: one fact, or an `outcome![...]` list
/// whose outcomes are single facts.
///
/// # Contract
///
/// Every value must lower to exactly one fact, and every member fact type
/// must reconstruct when passed alone to [`TypedFactSet::try_from_facts`]. A
/// manual implementation is a trusted semantic assertion; if a lowered value
/// contradicts it, the runtime treats the contradiction as a fatal
/// stage-contract violation.
///
/// A list with a tuple outcome is refused:
///
/// ```compile_fail
/// # use obzenflow_core::{outcome, OneFactStageOutput, TypedPayload};
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Valid;
/// # impl TypedPayload for Valid { const EVENT_TYPE: &'static str = "doc.valid"; }
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Invalid;
/// # impl TypedPayload for Invalid { const EVENT_TYPE: &'static str = "doc.invalid"; }
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Cancelled;
/// # impl TypedPayload for Cancelled { const EVENT_TYPE: &'static str = "doc.cancelled"; }
/// fn source_output<O: OneFactStageOutput>() {}
/// source_output::<outcome![Valid | (Invalid, Cancelled)]>();
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be a stateful stage's, a source's or a join's `Output`",
    label = "these stages write one fact per value, so one fact or `outcome![A | B]` without a \
             tuple",
    note = "facts written together as a tuple belong in a transform's `outcome![...]`"
)]
pub trait OneFactStageOutput: StageOutputFacts {}

impl<T: TypedPayload + Send + Sync + 'static> OneFactStageOutput for T {}

impl<Head, Rest> OneFactStageOutput for OutcomeOr<Head, Rest> where
    Self: StageOutputFacts + SingleFactOutcomes
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::schema::{
        EffectOutcomeFacts, EffectfulStageOutput, StageOutputFacts, TypedFactSetError,
    };
    use serde::{Deserialize, Serialize, Serializer};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Solo {
        value: u32,
    }

    impl TypedPayload for Solo {
        const EVENT_TYPE: &'static str = "stage_output.solo";
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
        const EVENT_TYPE: &'static str = "stage_output.serialization_fails";
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Valid {
        value: u32,
    }

    impl TypedPayload for Valid {
        const EVENT_TYPE: &'static str = "stage_output.valid";
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Invalid {
        reason: String,
    }

    impl TypedPayload for Invalid {
        const EVENT_TYPE: &'static str = "stage_output.invalid";
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Cancelled {
        reason: String,
    }

    impl TypedPayload for Cancelled {
        const EVENT_TYPE: &'static str = "stage_output.cancelled";
    }

    fn is_stage_output<T: StageOutputFacts>() {}
    fn is_one_fact_output<T: OneFactStageOutput>() {}
    fn is_effect_outcome<T: EffectOutcomeFacts>() {}
    fn is_effectful_output<T: EffectfulStageOutput>() {}

    #[test]
    fn scalar_payload_is_a_one_fact_stage_output() {
        is_stage_output::<Solo>();
        is_one_fact_output::<Solo>();
    }

    #[test]
    fn position_bounds_accept_listed_forms() {
        is_stage_output::<crate::outcome![Valid | (Invalid, Cancelled)]>();
        is_effect_outcome::<crate::outcome![Valid | (Invalid, Cancelled)]>();
        is_one_fact_output::<crate::outcome![Valid | Invalid]>();
        is_stage_output::<StageOutputs<Solo>>();
        is_effectful_output::<crate::stage_fact_set![Valid, Invalid]>();
        is_effectful_output::<Solo>();
    }

    #[test]
    fn dynamic_homogeneous_outputs_lower_zero_one_and_many_in_order() {
        assert!(StageOutputs::<Solo>::none()
            .into_facts()
            .expect("empty output lowers")
            .is_empty());

        let one = StageOutputs::one(Solo { value: 4 })
            .into_facts()
            .expect("one output lowers");
        assert_eq!(one.len(), 1);
        assert_eq!(
            serde_json::to_value(&one[0].payload).unwrap(),
            serde_json::json!({ "value": 4 })
        );

        let many = StageOutputs::many([Solo { value: 3 }, Solo { value: 7 }])
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
            <StageOutputs<Solo> as StageFactSet>::member_fact_types(),
            <Solo as StageFactSet>::member_fact_types()
        );
    }

    #[test]
    fn dynamic_homogeneous_outputs_reconstruct_repeated_facts() {
        let original = StageOutputs::many([Solo { value: 2 }, Solo { value: 2 }]);
        let facts = original.clone().into_facts().expect("outputs lower");
        assert_eq!(
            StageOutputs::<Solo>::try_from_facts(&facts).expect("outputs reconstruct"),
            original
        );
    }

    #[test]
    fn dynamic_homogeneous_outputs_propagate_serialization_failure() {
        assert!(matches!(
            StageOutputs::one(SerializationFails).into_facts(),
            Err(TypedFactSetError::SerializationFailed(message))
                if message.contains("deliberate serialization failure")
        ));
    }
}
