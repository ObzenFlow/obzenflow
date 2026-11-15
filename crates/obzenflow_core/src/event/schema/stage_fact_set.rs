// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stage fact sets.
//!
//! A stage fact set is the exhaustive, compiler-visible set of flat fact
//! types one stage may author across direct emissions, effect outcomes, and
//! typed policy branches: a `stage_fact_set![...]` declaration, an
//! `outcome![...]` list, or a scalar `TypedPayload` through the one-member
//! blanket. Sets are never persisted; the journal records only the flat named
//! facts themselves.

use super::member_set::{EmptySet, FactList, ListsEventType, MemberList, WithMember};
use super::typed_fact_set::{TypedFact, TypedFactSet, TypedFactSetError, TypedFactType};
use super::typed_payload::TypedPayload;
use std::fmt;
use std::marker::PhantomData;

/// Membership authority for one stage's authorable fact set.
///
/// Deliberately independent of `TypedFactSet`: a declared set lists the facts
/// a handler may emit and does not lower to facts, while an `outcome![...]`
/// list implements both. The hidden `Members` projection is what `Effects`
/// bounds and DSL equality assertions consume.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a stage fact set",
    note = "declare one fact, `stage_fact_set![A, B]` as an effectful handler's `Output`, or \
            `outcome![A | (B, C)]` where a value is exactly one outcome"
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

/// Expansion target of `stage_fact_set![...]`. A value is one committed fact
/// of the set, which only the effectful stateful adapter and a test's
/// `fact.into()` build; it is never persisted.
pub struct DeclaredStageFactSet<L: FactList>(L::Committed);

impl<L: FactList> fmt::Debug for DeclaredStageFactSet<L>
where
    L::Committed: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl<L: FactList> Clone for DeclaredStageFactSet<L>
where
    L::Committed: Clone,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<L: FactList> PartialEq for DeclaredStageFactSet<L>
where
    L::Committed: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<L: FactList> Eq for DeclaredStageFactSet<L> where L::Committed: Eq {}

impl<L, F> From<F> for DeclaredStageFactSet<L>
where
    L: FactList + ListsEventType<F>,
    F: TypedPayload + Send + Sync + 'static,
{
    fn from(fact: F) -> Self {
        const {
            assert!(
                <L as ListsEventType<F>>::LISTED,
                "a fact converted into a `stage_fact_set![...]` value must be one of the facts \
                 the set lists"
            )
        };
        Self(L::committed_from(Box::new(fact)))
    }
}

/// The committed fact `fold!` takes apart.
#[doc(hidden)]
pub fn fold_start<L: FactList>(set: DeclaredStageFactSet<L>) -> L::Committed {
    set.0
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

impl<L: FactList> DecodeCommitted for DeclaredStageFactSet<L> {
    fn decode_committed(fact: &TypedFact) -> Result<Self, TypedFactSetError> {
        L::decode_committed(fact).map(Self)
    }
}

/// The `Output` bound of an effectful transform and an effectful stateful
/// handler: one fact, or the set of facts it may emit for one input.
///
/// An `outcome![...]` list and `StageOutputs<T>` are refused:
///
/// ```compile_fail
/// # use obzenflow_core::{outcome, EffectfulStageOutput, TypedPayload};
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Authorized;
/// # impl TypedPayload for Authorized { const EVENT_TYPE: &'static str = "doc.authorized"; }
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Declined;
/// # impl TypedPayload for Declined { const EVENT_TYPE: &'static str = "doc.declined"; }
/// fn effectful_output<O: EffectfulStageOutput>() {}
/// effectful_output::<outcome![Authorized | Declined]>();
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
    label = "an effectful handler declares one fact or `stage_fact_set![...]`",
    note = "an effectful handler may emit any number of its facts for one input, so its \
            `Output` is a set written with commas; `|` lists outcomes of which exactly one is \
            returned"
)]
pub trait EffectfulStageOutput: StageFactSet {}

impl<T: TypedPayload + Send + Sync + 'static> EffectfulStageOutput for T {}

impl<L: FactList> EffectfulStageOutput for DeclaredStageFactSet<L> {}

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

impl<L: FactList> StageFactSet for DeclaredStageFactSet<L> {
    type Members = L;

    fn member_fact_types() -> Vec<TypedFactType> {
        let mut out = Vec::new();
        L::append_fact_types(&mut out);
        out
    }

    const MEMBERS_DISTINCT: () = L::DISTINCT_EVENT_TYPES;
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

/// Declare the facts an effectful handler may emit, in type position:
///
/// ```
/// use obzenflow_core::{stage_fact_set, StageFactSet, TypedPayload};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// pub struct PaymentAuthorized { pub authorization_id: String }
/// impl TypedPayload for PaymentAuthorized {
///     const EVENT_TYPE: &'static str = "payment.authorized";
/// }
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// pub struct PaymentDeclined { pub reason: String }
/// impl TypedPayload for PaymentDeclined {
///     const EVENT_TYPE: &'static str = "payment.declined";
/// }
///
/// type Output = stage_fact_set![PaymentAuthorized, PaymentDeclined];
/// assert_eq!(<Output as StageFactSet>::member_fact_types().len(), 2);
/// ```
///
/// Duplicate members are compile errors (a const-eval panic when the set is
/// used). Set equality elsewhere is semantic, not order-sensitive; source
/// order is rendering order only. A value of the set is one committed fact,
/// which an effectful stateful handler's `apply` receives and takes apart
/// with `fold!`.
#[macro_export]
macro_rules! stage_fact_set {
    [$($member:ty),* $(,)?] => {
        $crate::event::schema::DeclaredStageFactSet<
            $crate::stage_fact_set!(@list $($member),*)
        >
    };
    (@list) => { $crate::event::schema::EmptySet };
    (@list $head:ty $(, $rest:ty)*) => {
        $crate::event::schema::WithMember<$head, $crate::stage_fact_set!(@list $($rest),*)>
    };
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
    fn declared_set_projects_members_in_declared_order() {
        type Set = stage_fact_set![Alpha, Beta];
        let names: Vec<String> = <Set as StageFactSet>::member_fact_types()
            .iter()
            .map(|t| t.event_type.to_string())
            .collect();
        assert_eq!(names, vec!["stage_fact_set.alpha", "stage_fact_set.beta"]);
    }

    #[test]
    fn empty_set_is_a_valid_declaration() {
        type Empty = stage_fact_set![];
        assert!(<Empty as StageFactSet>::member_fact_types().is_empty());
        subset_holds::<Empty, stage_fact_set![Alpha], _>();
    }

    #[test]
    fn scalar_payload_is_a_one_member_set() {
        let members = <Alpha as StageFactSet>::member_fact_types();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].event_type.as_str(), "stage_fact_set.alpha");
        member_holds::<Alpha, Alpha, _>();
        subset_holds::<Alpha, stage_fact_set![Beta, Alpha], _>();
    }

    #[test]
    fn declared_set_membership_and_equality_are_order_insensitive() {
        type Forward = stage_fact_set![Alpha, Beta];
        type Backward = stage_fact_set![Beta, Alpha];

        member_holds::<Forward, Beta, _>();
        member_holds::<Backward, Beta, _>();
        subset_holds::<Forward, Backward, _>();
        subset_holds::<Backward, Forward, _>();
    }

    #[test]
    fn distinct_member_guard_evaluates_for_distinct_sets() {
        const _: () = assert_distinct_stage_fact_set::<stage_fact_set![Alpha, Beta]>();
        const _: () = assert_distinct_stage_fact_set::<stage_fact_set![]>();
        const _: () = assert_distinct_stage_fact_set::<Alpha>();
    }
}
