// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stage fact sets.
//!
//! A stage fact set is the exhaustive, compiler-visible set of flat fact
//! types one stage may author across direct emissions, effect outcomes, and
//! typed policy branches: an `outcome![...]` list, or a scalar `TypedPayload`
//! through the one-member blanket. Sets are never persisted; the journal
//! records only the flat named facts themselves.

use super::member_set::{EmptySet, FactList, MemberList, WithMember};
use super::typed_fact_set::{TypedFact, TypedFactSet, TypedFactSetError, TypedFactType};
use super::typed_payload::TypedPayload;
use std::marker::PhantomData;

/// Membership authority for one stage's authorable fact set.
///
/// Deliberately independent of `TypedFactSet`: a member list names facts and
/// does not lower to them, while an `outcome![...]` list implements both. The
/// hidden `Members` projection is what `Effects` bounds and DSL equality
/// assertions consume.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a stage fact set",
    note = "declare one fact, or `outcome![A | (B, C)]` where a value is exactly one outcome"
)]
pub trait StageFactSet: Send + Sync + 'static {
    /// Hidden type-level member projection; proof machinery, not public
    /// vocabulary.
    #[doc(hidden)]
    type Members: MemberList;

    /// Value-level member metadata for descriptors and runtime defence in
    /// depth.
    fn member_fact_types() -> Vec<TypedFactType>;

    /// Const duplicate guard over member `EVENT_TYPE`s. Explicit sets panic
    /// at const-eval on duplicates; the default covers shapes whose members
    /// are distinct by construction.
    #[doc(hidden)]
    const MEMBERS_DISTINCT: () = ();
}

/// Decoding of one committed fact into an effectful stateful handler's
/// `Output`.
#[doc(hidden)]
pub trait DecodeCommitted: StageFactSet + Sized {
    fn decode_committed(fact: &TypedFact) -> Result<Self, TypedFactSetError>;
}

impl<T: TypedPayload + Send + Sync + 'static> DecodeCommitted for T {
    fn decode_committed(fact: &TypedFact) -> Result<Self, TypedFactSetError> {
        T::try_from_facts(std::slice::from_ref(fact))
    }
}

/// The `Output` bound of an effectful transform and an effectful stateful
/// handler: one fact, or an `outcome![...]` list whose outcomes are single
/// facts.
///
/// A list with a tuple outcome and `StageOutputs<T>` are refused:
///
/// ```compile_fail
/// # use obzenflow_core::{outcome, EffectfulStageOutput, TypedPayload};
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Authorized;
/// # impl TypedPayload for Authorized { const EVENT_TYPE: &'static str = "doc.authorized"; }
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Declined;
/// # impl TypedPayload for Declined { const EVENT_TYPE: &'static str = "doc.declined"; }
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Cancelled;
/// # impl TypedPayload for Cancelled { const EVENT_TYPE: &'static str = "doc.cancelled"; }
/// fn effectful_output<O: EffectfulStageOutput>() {}
/// effectful_output::<outcome![Authorized | (Declined, Cancelled)]>();
/// ```
///
/// ```compile_fail
/// # use obzenflow_core::{EffectfulStageOutput, StageOutputs, TypedPayload};
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Authorized;
/// # impl TypedPayload for Authorized { const EVENT_TYPE: &'static str = "doc.authorized"; }
/// fn effectful_output<O: EffectfulStageOutput>() {}
/// effectful_output::<StageOutputs<Authorized>>();
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be an effectful handler's `Output`",
    label = "an effectful handler writes one fact at a time, so one fact or `outcome![A | B]` \
             without a tuple",
    note = "facts written together as a tuple belong in a transform's or a port's `outcome![...]`"
)]
pub trait EffectfulStageOutput: StageFactSet {}

impl<T: TypedPayload + Send + Sync + 'static> EffectfulStageOutput for T {}

impl<O: StageFactSet> EffectfulStageOutput for FactsOf<O> {}

/// The facts of `O` as a set, for a handler whose `Output` is taken from a
/// port's answer or an AI role's output. It holds no value.
#[doc(hidden)]
pub struct FactsOf<O>(PhantomData<fn() -> O>);

impl<O: StageFactSet> StageFactSet for FactsOf<O> {
    type Members = O::Members;

    fn member_fact_types() -> Vec<TypedFactType> {
        O::member_fact_types()
    }

    const MEMBERS_DISTINCT: () = O::MEMBERS_DISTINCT;
}

impl StageFactSet for EmptySet {
    type Members = EmptySet;

    fn member_fact_types() -> Vec<TypedFactType> {
        Vec::new()
    }
}

impl<Head, Rest> StageFactSet for WithMember<Head, Rest>
where
    Self: FactList,
{
    type Members = Self;

    fn member_fact_types() -> Vec<TypedFactType> {
        let mut out = Vec::new();
        Self::append_fact_types(&mut out);
        out
    }

    const MEMBERS_DISTINCT: () = <Self as FactList>::DISTINCT_EVENT_TYPES;
}

/// Scalar blanket: one `TypedPayload` is a one-member fact set.
impl<T: TypedPayload + Send + Sync + 'static> StageFactSet for T {
    type Members = WithMember<T, EmptySet>;

    fn member_fact_types() -> Vec<TypedFactType> {
        vec![TypedFactType::of::<T>()]
    }
}

/// Check-time hook forcing a set's duplicate guard: the DSL emits
/// `const _: () = assert_distinct_stage_fact_set::<S>();` per stage so a
/// duplicated member panics at const-eval with the stage declaration span.
#[doc(hidden)]
pub const fn assert_distinct_stage_fact_set<S: StageFactSet>() {
    S::MEMBERS_DISTINCT
}

#[cfg(test)]
mod tests {
    use super::super::member_set::{Member, SubsetOf};
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Alpha {
        value: u32,
    }

    impl TypedPayload for Alpha {
        const EVENT_TYPE: &'static str = "stage_fact_set.alpha";
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Beta {
        value: u32,
    }

    impl TypedPayload for Beta {
        const EVENT_TYPE: &'static str = "stage_fact_set.beta";
    }

    type AlphaBeta = WithMember<Alpha, WithMember<Beta, EmptySet>>;
    type BetaAlpha = WithMember<Beta, WithMember<Alpha, EmptySet>>;

    fn member_holds<Set, T, Index>()
    where
        Set: StageFactSet,
        Set::Members: Member<T, Index>,
    {
    }

    fn subset_holds<Sub, Super, Proof>()
    where
        Sub: StageFactSet,
        Super: StageFactSet,
        Sub::Members: SubsetOf<Super::Members, Proof>,
    {
    }

    #[test]
    fn member_list_projects_declared_order() {
        let names: Vec<String> = <AlphaBeta as StageFactSet>::member_fact_types()
            .iter()
            .map(|t| t.event_type.to_string())
            .collect();
        assert_eq!(names, vec!["stage_fact_set.alpha", "stage_fact_set.beta"]);
    }

    #[test]
    fn empty_list_declares_no_facts() {
        assert!(<EmptySet as StageFactSet>::member_fact_types().is_empty());
        subset_holds::<EmptySet, WithMember<Alpha, EmptySet>, _>();
    }

    #[test]
    fn scalar_payload_is_a_one_member_set() {
        let members = <Alpha as StageFactSet>::member_fact_types();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].event_type.as_str(), "stage_fact_set.alpha");
        member_holds::<Alpha, Alpha, _>();
        subset_holds::<Alpha, BetaAlpha, _>();
    }

    #[test]
    fn member_list_membership_ignores_order() {
        member_holds::<AlphaBeta, Beta, _>();
        member_holds::<BetaAlpha, Beta, _>();
        subset_holds::<AlphaBeta, BetaAlpha, _>();
        subset_holds::<BetaAlpha, AlphaBeta, _>();
    }

    #[test]
    fn distinct_member_guard_evaluates_for_distinct_sets() {
        const _: () = assert_distinct_stage_fact_set::<AlphaBeta>();
        const _: () = assert_distinct_stage_fact_set::<EmptySet>();
        const _: () = assert_distinct_stage_fact_set::<Alpha>();
    }
}
