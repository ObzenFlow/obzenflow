// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Outcome lists, written `outcome![A | (B, C)]`.
//!
//! A list names a stage's facts directly. A value holds exactly one listed
//! outcome, which is one fact or a tuple of facts written together, and
//! lowers to those facts in the order written. The list itself is never
//! persisted.

use super::fold::FoldStart;
use super::member_set::{
    AppendList, EmptySet, FactList, FactOr, MemberAfter, MemberFound, MemberList, NoOtherFact,
    WithMember,
};
use super::stage_fact_set::{
    assert_distinct_stage_fact_set, DecodeCommitted, EffectfulStageOutput, StageFactSet,
};
use super::typed_fact_set::{
    decode_member_fact, missing_fact_group_error, TypedFact, TypedFactSet, TypedFactSetError,
    TypedFactType,
};
use super::typed_payload::TypedPayload;
use std::fmt;

/// A list of outcomes of which a value holds exactly one, written with
/// `outcome![...]`.
pub struct OutcomeOr<Head, Rest = NoOtherOutcome>(Case<Head, Rest>);

enum Case<Head, Rest> {
    Head(Head),
    Rest(Rest),
}

/// The end of an outcome list; it has no value.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NoOtherOutcome {}

/// One outcome of a list: a fact, or a tuple of up to twelve facts written
/// together.
#[doc(hidden)]
pub trait OutcomeFacts: Sized + Send + Sync + 'static {
    type Members: MemberList;

    fn append_fact_types(out: &mut Vec<TypedFactType>);

    fn declares(fact: &TypedFact) -> bool;

    fn lower(self, out: &mut Vec<TypedFact>) -> Result<(), TypedFactSetError>;

    fn matches_group(facts: &[TypedFact]) -> bool;

    fn decode(facts: &[TypedFact]) -> Result<Self, TypedFactSetError>;
}

impl<T> OutcomeFacts for T
where
    T: TypedPayload + Send + Sync + 'static,
{
    type Members = WithMember<T, EmptySet>;

    fn append_fact_types(out: &mut Vec<TypedFactType>) {
        out.push(TypedFactType::of::<T>());
    }

    fn declares(fact: &TypedFact) -> bool {
        T::matches_fact(fact)
    }

    fn lower(self, out: &mut Vec<TypedFact>) -> Result<(), TypedFactSetError> {
        out.push(TypedFact::from_payload(self)?);
        Ok(())
    }

    fn matches_group(facts: &[TypedFact]) -> bool {
        matches!(facts, [fact] if T::matches_fact(fact))
    }

    fn decode(facts: &[TypedFact]) -> Result<Self, TypedFactSetError> {
        decode_member_fact::<T>(facts)
    }
}

macro_rules! tuple_outcome_members {
    () => { EmptySet };
    ($head:ident $($rest:ident)*) => { WithMember<$head, tuple_outcome_members!($($rest)*)> };
}

macro_rules! tuple_outcome {
    ($arity:literal: $($fact:ident . $index:tt),+) => {
        impl<$($fact),+> OutcomeFacts for ($($fact,)+)
        where
            $($fact: TypedPayload + Send + Sync + 'static,)+
        {
            type Members = tuple_outcome_members!($($fact)+);

            fn append_fact_types(out: &mut Vec<TypedFactType>) {
                $(out.push(TypedFactType::of::<$fact>());)+
            }

            fn declares(fact: &TypedFact) -> bool {
                false $(|| $fact::matches_fact(fact))+
            }

            fn lower(self, out: &mut Vec<TypedFact>) -> Result<(), TypedFactSetError> {
                $(out.push(TypedFact::from_payload(self.$index)?);)+
                Ok(())
            }

            fn matches_group(facts: &[TypedFact]) -> bool {
                facts.len() == $arity
                    $(&& facts.iter().any(|fact| $fact::matches_fact(fact)))+
            }

            fn decode(facts: &[TypedFact]) -> Result<Self, TypedFactSetError> {
                Ok(($(decode_member_fact::<$fact>(facts)?,)+))
            }
        }
    };
}

tuple_outcome!(2: A.0, B.1);
tuple_outcome!(3: A.0, B.1, C.2);
tuple_outcome!(4: A.0, B.1, C.2, D.3);
tuple_outcome!(5: A.0, B.1, C.2, D.3, E.4);
tuple_outcome!(6: A.0, B.1, C.2, D.3, E.4, F.5);
tuple_outcome!(7: A.0, B.1, C.2, D.3, E.4, F.5, G.6);
tuple_outcome!(8: A.0, B.1, C.2, D.3, E.4, F.5, G.6, H.7);
tuple_outcome!(9: A.0, B.1, C.2, D.3, E.4, F.5, G.6, H.7, I.8);
tuple_outcome!(10: A.0, B.1, C.2, D.3, E.4, F.5, G.6, H.7, I.8, J.9);
tuple_outcome!(11: A.0, B.1, C.2, D.3, E.4, F.5, G.6, H.7, I.8, J.9, K.10);
tuple_outcome!(12: A.0, B.1, C.2, D.3, E.4, F.5, G.6, H.7, I.8, J.9, K.10, L.11);

/// The outcomes of a list, from one position to its end.
#[doc(hidden)]
pub trait OutcomeList: Sized + Send + Sync + 'static {
    type Members: MemberList;

    fn append_fact_types(out: &mut Vec<TypedFactType>);

    fn declares(fact: &TypedFact) -> bool;

    fn lower(self) -> Result<Vec<TypedFact>, TypedFactSetError>;

    fn decode_group(facts: &[TypedFact]) -> Option<Result<Self, TypedFactSetError>>;
}

impl OutcomeList for NoOtherOutcome {
    type Members = EmptySet;

    fn append_fact_types(_out: &mut Vec<TypedFactType>) {}

    fn declares(_fact: &TypedFact) -> bool {
        false
    }

    fn lower(self) -> Result<Vec<TypedFact>, TypedFactSetError> {
        match self {}
    }

    fn decode_group(_facts: &[TypedFact]) -> Option<Result<Self, TypedFactSetError>> {
        None
    }
}

impl<Head, Rest> OutcomeList for OutcomeOr<Head, Rest>
where
    Head: OutcomeFacts,
    Rest: OutcomeList,
    Head::Members: AppendList<Rest::Members>,
{
    type Members = <Head::Members as AppendList<Rest::Members>>::Output;

    fn append_fact_types(out: &mut Vec<TypedFactType>) {
        Head::append_fact_types(out);
        Rest::append_fact_types(out);
    }

    fn declares(fact: &TypedFact) -> bool {
        Head::declares(fact) || Rest::declares(fact)
    }

    fn lower(self) -> Result<Vec<TypedFact>, TypedFactSetError> {
        match self.0 {
            Case::Head(outcome) => {
                let mut facts = Vec::new();
                outcome.lower(&mut facts)?;
                Ok(facts)
            }
            Case::Rest(rest) => rest.lower(),
        }
    }

    fn decode_group(facts: &[TypedFact]) -> Option<Result<Self, TypedFactSetError>> {
        if Head::matches_group(facts) {
            return Some(Head::decode(facts).map(|outcome| Self(Case::Head(outcome))));
        }
        Rest::decode_group(facts).map(|rest| rest.map(|rest| Self(Case::Rest(rest))))
    }
}

impl<Head, Rest> TypedFactSet for OutcomeOr<Head, Rest>
where
    Self: OutcomeList,
    <Self as OutcomeList>::Members: FactList,
{
    fn fact_types() -> Vec<TypedFactType> {
        assert_distinct_stage_fact_set::<Self>();
        let mut out = Vec::new();
        <Self as OutcomeList>::append_fact_types(&mut out);
        out
    }

    fn into_facts(self) -> Result<Vec<TypedFact>, TypedFactSetError> {
        assert_distinct_stage_fact_set::<Self>();
        self.lower()
    }

    fn try_from_facts(facts: &[TypedFact]) -> Result<Self, TypedFactSetError> {
        assert_distinct_stage_fact_set::<Self>();
        if let Some(undeclared) = facts.iter().find(|fact| !Self::declares(fact)) {
            return Err(TypedFactSetError::UnexpectedFact {
                event_type: undeclared.event_type.clone(),
            });
        }
        for (index, fact) in facts.iter().enumerate() {
            if facts[index + 1..]
                .iter()
                .any(|later| later.descriptor() == fact.descriptor())
            {
                return Err(TypedFactSetError::DuplicateFact {
                    event_type: fact.event_type.clone(),
                });
            }
        }
        Self::decode_group(facts)
            .unwrap_or_else(|| Err(missing_fact_group_error(&Self::fact_types())))
    }
}

impl<Head, Rest> StageFactSet for OutcomeOr<Head, Rest>
where
    Self: OutcomeList,
    <Self as OutcomeList>::Members: FactList,
{
    type Members = <Self as OutcomeList>::Members;

    fn member_fact_types() -> Vec<TypedFactType> {
        let mut out = Vec::new();
        <Self as OutcomeList>::append_fact_types(&mut out);
        out
    }

    const MEMBERS_DISTINCT: () = <<Self as OutcomeList>::Members as FactList>::DISTINCT_EVENT_TYPES;
}

/// A list `outcome!` refused, kept as written so its refusal is the only
/// error.
#[doc(hidden)]
pub trait Refused<const REFUSAL: bool> {
    type List;
}

impl<List, const REFUSAL: bool> Refused<REFUSAL> for List {
    type List = List;
}

/// Lists where every outcome is one fact.
#[doc(hidden)]
pub trait SingleFactOutcomes {}

impl SingleFactOutcomes for NoOtherOutcome {}

impl<Head: TypedPayload, Rest: SingleFactOutcomes> SingleFactOutcomes for OutcomeOr<Head, Rest> {}

impl<Head, Rest> EffectfulStageOutput for OutcomeOr<Head, Rest> where
    Self: StageFactSet + SingleFactOutcomes
{
}

impl<Head, Rest> DecodeCommitted for OutcomeOr<Head, Rest>
where
    Self: TypedFactSet + StageFactSet + SingleFactOutcomes,
{
    fn decode_committed(fact: &TypedFact) -> Result<Self, TypedFactSetError> {
        Self::try_from_facts(std::slice::from_ref(fact))
    }
}

impl FoldStart for NoOtherOutcome {
    type Remaining = NoOtherFact;

    fn fold_start(self) -> NoOtherFact {
        match self {}
    }
}

impl<Head: TypedPayload, Rest: FoldStart> FoldStart for OutcomeOr<Head, Rest> {
    type Remaining = FactOr<Head, Rest::Remaining>;

    fn fold_start(self) -> Self::Remaining {
        match self.0 {
            Case::Head(fact) => FactOr::Head(fact),
            Case::Rest(rest) => FactOr::Rest(rest.fold_start()),
        }
    }
}

/// The list holds `Outcome` at `Position`, which the compiler infers.
#[diagnostic::on_unimplemented(
    message = "`{Outcome}` is not one of the outcomes listed in `outcome![...]`",
    label = "`{Outcome}` is not a listed outcome",
    note = "an outcome is one fact, or a tuple of the facts written together, in the order the \
            list writes them; return exactly one listed outcome"
)]
pub trait ListsOutcome<Outcome, Position> {
    #[doc(hidden)]
    fn listing(outcome: Outcome) -> Self;
}

impl<Head, Rest> ListsOutcome<Head, MemberFound> for OutcomeOr<Head, Rest> {
    fn listing(outcome: Head) -> Self {
        Self(Case::Head(outcome))
    }
}

#[diagnostic::do_not_recommend]
impl<Head, Rest, Outcome, Position> ListsOutcome<Outcome, MemberAfter<Position>>
    for OutcomeOr<Head, Rest>
where
    Rest: ListsOutcome<Outcome, Position>,
{
    fn listing(outcome: Outcome) -> Self {
        Self(Case::Rest(Rest::listing(outcome)))
    }
}

/// Build a value of an `outcome![...]` list from one of its outcomes.
///
/// A value the list doesn't hold is refused, whether it is an unlisted fact,
/// half of a listed pair, or a pair written in another order than the
/// list's:
///
/// ```compile_fail
/// # use obzenflow_core::{outcome, TypedPayload};
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Valid;
/// # impl TypedPayload for Valid { const EVENT_TYPE: &'static str = "doc.valid"; }
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Invalid;
/// # impl TypedPayload for Invalid { const EVENT_TYPE: &'static str = "doc.invalid"; }
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Cancelled;
/// # impl TypedPayload for Cancelled { const EVENT_TYPE: &'static str = "doc.cancelled"; }
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Unlisted;
/// # impl TypedPayload for Unlisted { const EVENT_TYPE: &'static str = "doc.unlisted"; }
/// let unlisted: outcome![Valid | (Invalid, Cancelled)] = outcome(Unlisted);
/// ```
///
/// ```compile_fail
/// # use obzenflow_core::{outcome, TypedPayload};
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Valid;
/// # impl TypedPayload for Valid { const EVENT_TYPE: &'static str = "doc.valid"; }
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Invalid;
/// # impl TypedPayload for Invalid { const EVENT_TYPE: &'static str = "doc.invalid"; }
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Cancelled;
/// # impl TypedPayload for Cancelled { const EVENT_TYPE: &'static str = "doc.cancelled"; }
/// let half: outcome![Valid | (Invalid, Cancelled)] = outcome(Invalid);
/// ```
///
/// ```compile_fail
/// # use obzenflow_core::{outcome, TypedPayload};
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Valid;
/// # impl TypedPayload for Valid { const EVENT_TYPE: &'static str = "doc.valid"; }
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Invalid;
/// # impl TypedPayload for Invalid { const EVENT_TYPE: &'static str = "doc.invalid"; }
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Cancelled;
/// # impl TypedPayload for Cancelled { const EVENT_TYPE: &'static str = "doc.cancelled"; }
/// let reordered: outcome![Valid | (Invalid, Cancelled)] = outcome((Cancelled, Invalid));
/// ```
pub fn outcome<Outcome, Output, Position>(outcome: Outcome) -> Output
where
    Output: ListsOutcome<Outcome, Position>,
{
    Output::listing(outcome)
}

impl<Head: fmt::Debug, Rest: fmt::Debug> fmt::Debug for OutcomeOr<Head, Rest> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Case::Head(outcome) => outcome.fmt(formatter),
            Case::Rest(rest) => rest.fmt(formatter),
        }
    }
}

impl<Head: Clone, Rest: Clone> Clone for OutcomeOr<Head, Rest> {
    fn clone(&self) -> Self {
        match &self.0 {
            Case::Head(outcome) => Self(Case::Head(outcome.clone())),
            Case::Rest(rest) => Self(Case::Rest(rest.clone())),
        }
    }
}

impl<Head: PartialEq, Rest: PartialEq> PartialEq for OutcomeOr<Head, Rest> {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Case::Head(left), Case::Head(right)) => left == right,
            (Case::Rest(left), Case::Rest(right)) => left == right,
            _ => false,
        }
    }
}

impl<Head: Eq, Rest: Eq> Eq for OutcomeOr<Head, Rest> {}

/// Declare a list of outcomes in type position, of which a value holds
/// exactly one: `outcome![ValidatedOrder | (InvalidOrder, CancelledOrder)]`.
///
/// `|` separates outcomes, and a tuple writes facts that commit together in
/// the order written. The outcomes may be listed in any order. Build a value
/// with [`outcome`](crate::event::schema::outcome).
///
/// ```
/// use obzenflow_core::event::schema::TypedFactSet;
/// use obzenflow_core::{outcome, TypedPayload};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// pub struct ValidatedOrder { pub order_id: String }
/// impl TypedPayload for ValidatedOrder {
///     const EVENT_TYPE: &'static str = "order.validated";
/// }
///
/// #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// pub struct InvalidOrder { pub order_id: String }
/// impl TypedPayload for InvalidOrder {
///     const EVENT_TYPE: &'static str = "order.invalid";
/// }
///
/// #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// pub struct CancelledOrder { pub order_id: String }
/// impl TypedPayload for CancelledOrder {
///     const EVENT_TYPE: &'static str = "order.cancelled";
/// }
///
/// let refused: outcome![ValidatedOrder | (InvalidOrder, CancelledOrder)] = outcome((
///     InvalidOrder { order_id: "o-1".into() },
///     CancelledOrder { order_id: "o-1".into() },
/// ));
/// let facts = refused.into_facts().unwrap();
/// assert_eq!(facts[0].event_type.as_str(), "order.invalid");
/// assert_eq!(facts[1].event_type.as_str(), "order.cancelled");
/// ```
///
/// A fact in two outcomes, and commas between outcomes, are refused:
///
/// ```compile_fail
/// # use obzenflow_core::{outcome, TypedPayload};
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Approved;
/// # impl TypedPayload for Approved { const EVENT_TYPE: &'static str = "doc.approved"; }
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Audited;
/// # impl TypedPayload for Audited { const EVENT_TYPE: &'static str = "doc.audited"; }
/// let answer: outcome![Approved | (Approved, Audited)] = outcome(Approved);
/// ```
///
/// ```compile_fail
/// # use obzenflow_core::{outcome, TypedPayload};
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Approved;
/// # impl TypedPayload for Approved { const EVENT_TYPE: &'static str = "doc.approved"; }
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Refused;
/// # impl TypedPayload for Refused { const EVENT_TYPE: &'static str = "doc.refused"; }
/// let answer: outcome![Approved, Refused] = outcome(Approved);
/// ```
#[macro_export]
macro_rules! outcome {
    [$($outcomes:tt)*] => {
        $crate::event::schema::__private::sorted_outcomes!($crate; $($outcomes)*)
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::types::EventType;
    use crate::event::ChainPayload;
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Valid {
        value: u32,
    }

    impl TypedPayload for Valid {
        const EVENT_TYPE: &'static str = "outcome_list.valid";
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Invalid {
        reason: String,
    }

    impl TypedPayload for Invalid {
        const EVENT_TYPE: &'static str = "outcome_list.invalid";
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Cancelled {
        reason: String,
    }

    impl TypedPayload for Cancelled {
        const EVENT_TYPE: &'static str = "outcome_list.cancelled";
    }

    mod facts {
        pub(super) use super::Cancelled;
    }

    fn refused() -> crate::outcome![Valid | (Invalid, Cancelled)] {
        outcome((
            Invalid {
                reason: "zero".to_string(),
            },
            Cancelled {
                reason: "invalid".to_string(),
            },
        ))
    }

    fn recorded(facts: &[TypedFact]) -> Vec<(String, serde_json::Value)> {
        facts
            .iter()
            .map(|fact| {
                (
                    fact.event_type.to_string(),
                    serde_json::to_value(&fact.payload).expect("payload serializes"),
                )
            })
            .collect()
    }

    #[test]
    fn outcome_list_tuple_lowers_written_order() {
        let accepted: crate::outcome![Valid | (Invalid, Cancelled)] = outcome(Valid { value: 4 });
        assert_eq!(
            recorded(&accepted.into_facts().expect("outcome lowers")),
            vec![("outcome_list.valid".to_string(), json!({ "value": 4 }))]
        );
        assert_eq!(
            recorded(&refused().into_facts().expect("outcome lowers")),
            vec![
                (
                    "outcome_list.invalid".to_string(),
                    json!({ "reason": "zero" })
                ),
                (
                    "outcome_list.cancelled".to_string(),
                    json!({ "reason": "invalid" })
                ),
            ]
        );
    }

    #[test]
    fn outcome_list_rebuild_ignores_fact_order() {
        let facts = refused().into_facts().expect("outcome lowers");
        let reversed = vec![facts[1].clone(), facts[0].clone()];
        assert_eq!(
            <crate::outcome![Valid | (Invalid, Cancelled)]>::try_from_facts(&facts)
                .expect("outcome rebuilds"),
            refused()
        );
        assert_eq!(
            <crate::outcome![Valid | (Invalid, Cancelled)]>::try_from_facts(&reversed)
                .expect("outcome rebuilds"),
            refused()
        );
    }

    #[test]
    fn outcome_list_malformed_group_refused() {
        let foreign = vec![TypedFact {
            payload_schema_version: std::num::NonZeroU32::MIN,
            event_type: EventType::from("outcome_list.unknown"),
            payload: ChainPayload::Fact(json!({})),
        }];
        assert!(matches!(
            <crate::outcome![Valid | (Invalid, Cancelled)]>::try_from_facts(&foreign),
            Err(TypedFactSetError::UnexpectedFact { .. })
        ));

        let repeated = vec![
            TypedFact::from_payload(Valid { value: 1 }).expect("valid"),
            TypedFact::from_payload(Valid { value: 2 }).expect("valid again"),
        ];
        assert!(matches!(
            <crate::outcome![Valid | (Invalid, Cancelled)]>::try_from_facts(&repeated),
            Err(TypedFactSetError::DuplicateFact { .. })
        ));

        let unmatched = vec![
            TypedFact::from_payload(Valid { value: 1 }).expect("valid"),
            TypedFact::from_payload(Invalid {
                reason: "zero".to_string(),
            })
            .expect("invalid"),
        ];
        assert!(matches!(
            <crate::outcome![Valid | (Invalid, Cancelled)]>::try_from_facts(&unmatched),
            Err(TypedFactSetError::MissingFact { .. })
        ));

        assert!(matches!(
            <crate::outcome![Valid | (Invalid, Cancelled)]>::try_from_facts(&[]),
            Err(TypedFactSetError::MissingFact { .. })
        ));
    }

    #[test]
    fn outcome_list_reordered_unifies() {
        let reordered: crate::outcome![(Invalid, Cancelled) | Valid] = refused();
        let qualified: crate::outcome![Valid | (Invalid, facts::Cancelled)] = refused();
        assert_eq!(qualified, reordered);
        assert_eq!(
            recorded(&reordered.into_facts().expect("outcome lowers"))
                .into_iter()
                .map(|(event_type, _)| event_type)
                .collect::<Vec<_>>(),
            vec!["outcome_list.invalid", "outcome_list.cancelled"]
        );
    }

    #[test]
    fn outcome_list_debug_prints_held_outcome() {
        let accepted: crate::outcome![Valid | (Invalid, Cancelled)] = outcome(Valid { value: 4 });
        assert_eq!(format!("{accepted:?}"), "Valid { value: 4 }");
    }

    fn command_output<O: EffectfulStageOutput + DecodeCommitted>() {}

    #[test]
    fn fact_list_command_output_accepted() {
        command_output::<crate::outcome![Valid | Invalid]>();
        command_output::<Valid>();
    }

    #[test]
    fn committed_fact_decodes_into_list() {
        let invalid = Invalid {
            reason: "empty".to_string(),
        };
        let fact = TypedFact::from_payload(invalid.clone()).unwrap();
        let decoded =
            <crate::outcome![Valid | Invalid] as DecodeCommitted>::decode_committed(&fact).unwrap();
        assert_eq!(decoded, outcome(invalid));
    }

    #[test]
    fn foreign_fact_decode_refused() {
        let fact = TypedFact::from_payload(Cancelled {
            reason: "late".to_string(),
        })
        .unwrap();
        assert!(
            <crate::outcome![Valid | Invalid] as DecodeCommitted>::decode_committed(&fact).is_err()
        );
    }

    #[test]
    fn list_outcome_folds_by_fact() {
        fn label(fact: crate::outcome![Valid | Invalid]) -> &'static str {
            crate::fold!(match fact {
                Valid { .. } => {
                    "valid"
                }
                Invalid { .. } => {
                    "invalid"
                }
            })
        }
        assert_eq!(label(outcome(Valid { value: 1 })), "valid");
        assert_eq!(
            label(outcome(Invalid {
                reason: "empty".to_string()
            })),
            "invalid"
        );
    }
}
