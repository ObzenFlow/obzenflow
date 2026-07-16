// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stage fact sets (FLOWIP-120z).
//!
//! A stage fact set is the exhaustive, compiler-visible set of flat fact
//! types one stage may author across direct emissions, effect outcomes, and
//! typed policy branches. A set may be zero-sized (a `stage_fact_set![...]`
//! declaration, ergonomic sugar over a hidden type list) or inhabited (a
//! carrier deriving `StageOutputFacts`, or a scalar `TypedPayload` through
//! the one-member blanket). Sets are never persisted; the journal records
//! only the flat named facts themselves.

use super::member_set::{EmptySet, FactList, MemberList, WithMember};
use super::typed_fact_set::TypedFactType;
use super::typed_payload::TypedPayload;
use std::marker::PhantomData;

/// Membership authority for one stage's authorable fact set.
///
/// Deliberately independent of `TypedFactSet`: a zero-sized declared set is
/// a set, not a carrier (it cannot lower to facts), while inhabited carriers
/// implement both. The hidden `Members` projection is what `Effects` bounds
/// and DSL equality assertions consume.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a stage fact set",
    note = "declare an effectful handler's output as `stage_fact_set![FactA, FactB]`, a \
            carrier deriving `StageOutputFacts`, or a single `TypedPayload` fact type \
            (FLOWIP-120z)"
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

/// Zero-sized expansion target of `stage_fact_set![...]`. Never constructed
/// and never persisted; it exists only as a type-level declaration.
pub struct DeclaredStageFactSet<L>(PhantomData<fn() -> L>);

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
///
/// Coherence keeps this disjoint from declared sets and derived carriers by
/// the same mutual-exclusion mechanism FLOWIP-120m established: neither
/// `DeclaredStageFactSet` nor a carrier may implement `TypedPayload`.
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

/// Declare a stage fact set in type position (FLOWIP-120z):
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
/// order is rendering order only.
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
        assert_eq!(
            names,
            vec!["stage_fact_set.alpha.v1", "stage_fact_set.beta.v1"]
        );
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
        assert_eq!(members[0].event_type.as_str(), "stage_fact_set.alpha.v1");
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
