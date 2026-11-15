// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Type-level membership proofs for declared stage sets (FLOWIP-120z).
//!
//! A stage's authorable fact set and permitted effect set are static,
//! compiler-visible sets. This module holds the shared proof vocabulary: the
//! hidden type-level member list, the indexed membership witness, and the
//! recursive subset witness. `Effects::{emit, perform}` and the DSL equality
//! assertions consume these bounds; user code never names them.
//!
//! The names are domain-flavoured rather than the textbook `Cons`/`Here`/
//! `There`, because rustc's secondary notes can render hidden types even
//! under `#[diagnostic::on_unimplemented]`.

use super::typed_fact_set::{decode_member_fact, TypedFact, TypedFactSetError, TypedFactType};
use super::typed_payload::TypedPayload;
use std::any::Any;
use std::fmt;
use std::marker::PhantomData;

mod sealed {
    pub trait Sealed {}

    pub trait Member<T, Index> {}

    pub trait SubsetOf<Super, Proof> {}
}

/// Type-level empty member list.
#[doc(hidden)]
pub struct EmptySet;

/// Type-level member list node: `Head` followed by the `Rest` of the list.
#[doc(hidden)]
pub struct WithMember<Head, Rest>(PhantomData<fn() -> (Head, Rest)>);

/// Sealed marker for the two list shapes; nothing outside this module can
/// add a third, so every declared set is one of these.
#[doc(hidden)]
pub trait MemberList: sealed::Sealed + Send + Sync + 'static {}

impl sealed::Sealed for EmptySet {}
impl MemberList for EmptySet {}
impl<Head: 'static, Rest: MemberList> sealed::Sealed for WithMember<Head, Rest> {}
impl<Head: 'static, Rest: MemberList> MemberList for WithMember<Head, Rest> {}

/// Index witness: the member is the head of the list.
#[doc(hidden)]
pub struct MemberFound;

/// Index witness: the member is found after skipping the head.
#[doc(hidden)]
pub struct MemberAfter<Index>(PhantomData<fn() -> Index>);

/// Compiler-inferred membership witness: the list contains `T` at `Index`.
///
/// This trait appears in `Effects::{emit, perform}` bounds and in DSL
/// assertion helpers; the index parameter is always inferred. It is proof
/// machinery, never implemented or named by user code.
#[diagnostic::on_unimplemented(
    message = "`{T}` is not a declared member of this stage's set",
    label = "`{T}` is missing from the declared set",
    note = "`emit` requires the fact type in the handler's `Output` set and `perform` \
            requires the effect type in its `AllowedEffects` set; add the member to the \
            handler declaration and the matching DSL clause, or remove this call \
            (FLOWIP-120z)"
)]
pub trait Member<T, Index>: sealed::Member<T, Index> {}

impl<T, Rest> sealed::Member<T, MemberFound> for WithMember<T, Rest> {}
impl<T, Rest> Member<T, MemberFound> for WithMember<T, Rest> {}

#[diagnostic::do_not_recommend]
impl<T, Head, Rest, Index> sealed::Member<T, MemberAfter<Index>> for WithMember<Head, Rest> where
    Rest: Member<T, Index>
{
}

#[diagnostic::do_not_recommend]
impl<T, Head, Rest, Index> Member<T, MemberAfter<Index>> for WithMember<Head, Rest> where
    Rest: Member<T, Index>
{
}

/// Subset proof terminator: the empty set is a subset of anything.
#[doc(hidden)]
pub struct SubsetProofEnd;

/// Subset proof step: the head's membership index plus the rest's proof.
#[doc(hidden)]
pub struct SubsetProofStep<AtIndex, RestProof>(PhantomData<fn() -> (AtIndex, RestProof)>);

/// Compiler-inferred subset witness: every member of this list is a member
/// of `Super`.
///
/// Proof machinery, never implemented or named by user code. The proof
/// parameter is always inferred.
#[diagnostic::on_unimplemented(
    message = "not every member of this fact set is declared by the enclosing set",
    note = "every fact an allowed effect may author must be a member of the handler's \
            `Output` set, and a stage arrow must equal the handler's declared set in both \
            directions (FLOWIP-120z)"
)]
pub trait SubsetOf<Super, Proof>: sealed::SubsetOf<Super, Proof> {}

impl<Super> sealed::SubsetOf<Super, SubsetProofEnd> for EmptySet {}
impl<Super> SubsetOf<Super, SubsetProofEnd> for EmptySet {}

#[diagnostic::do_not_recommend]
impl<Head, Rest, Super, At, RestProof> sealed::SubsetOf<Super, SubsetProofStep<At, RestProof>>
    for WithMember<Head, Rest>
where
    Super: Member<Head, At>,
    Rest: SubsetOf<Super, RestProof>,
{
}

#[diagnostic::do_not_recommend]
impl<Head, Rest, Super, At, RestProof> SubsetOf<Super, SubsetProofStep<At, RestProof>>
    for WithMember<Head, Rest>
where
    Super: Member<Head, At>,
    Rest: SubsetOf<Super, RestProof>,
{
}

/// Const string equality; stable-Rust building block for the duplicate
/// member guards.
#[doc(hidden)]
#[must_use]
pub const fn const_str_eq(a: &str, b: &str) -> bool {
    let a = a.as_bytes();
    let b = b.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

/// Force two `()` proofs in one const initializer without a path statement.
#[doc(hidden)]
pub const fn require_distinct(_head_disjoint: (), _rest_distinct: ()) {}

/// The list `Self` followed by `Tail`.
#[doc(hidden)]
pub trait AppendList<Tail> {
    type Output: MemberList;
}

impl<Tail: MemberList> AppendList<Tail> for EmptySet {
    type Output = Tail;
}

impl<Head: 'static, Rest: AppendList<Tail>, Tail> AppendList<Tail> for WithMember<Head, Rest> {
    type Output = WithMember<Head, Rest::Output>;
}

/// One committed fact of a fact list: its head fact or one of the rest.
#[doc(hidden)]
pub enum FactOr<Head, Rest> {
    Head(Head),
    Rest(Rest),
}

impl<Head: fmt::Debug, Rest: fmt::Debug> fmt::Debug for FactOr<Head, Rest> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Head(fact) => fact.fmt(formatter),
            Self::Rest(rest) => rest.fmt(formatter),
        }
    }
}

impl<Head: Clone, Rest: Clone> Clone for FactOr<Head, Rest> {
    fn clone(&self) -> Self {
        match self {
            Self::Head(fact) => Self::Head(fact.clone()),
            Self::Rest(rest) => Self::Rest(rest.clone()),
        }
    }
}

impl<Head: PartialEq, Rest: PartialEq> PartialEq for FactOr<Head, Rest> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Head(left), Self::Head(right)) => left == right,
            (Self::Rest(left), Self::Rest(right)) => left == right,
            _ => false,
        }
    }
}

impl<Head: Eq, Rest: Eq> Eq for FactOr<Head, Rest> {}

/// What is left of a committed fact once every listed fact has been taken
/// out; it has no value.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NoOtherFact {}

/// A member list whose heads are fact types, with a const duplicate guard
/// over `EVENT_TYPE` and a value-level projection for descriptors and
/// runtime defence in depth.
#[doc(hidden)]
pub trait FactList: MemberList {
    /// Const-evaluates to `()`, or panics at const-eval when two members
    /// share an `EVENT_TYPE`. Forced by `Effects` construction and by the
    /// DSL's per-stage assertion hook.
    const DISTINCT_EVENT_TYPES: ();

    /// One committed fact of the list.
    type Committed: Send + Sync + 'static;

    fn append_fact_types(out: &mut Vec<TypedFactType>);

    fn decode_committed(fact: &TypedFact) -> Result<Self::Committed, TypedFactSetError>;

    fn committed_from(fact: Box<dyn Any>) -> Self::Committed;
}

impl FactList for EmptySet {
    const DISTINCT_EVENT_TYPES: () = ();

    type Committed = NoOtherFact;

    fn append_fact_types(_out: &mut Vec<TypedFactType>) {}

    fn decode_committed(fact: &TypedFact) -> Result<NoOtherFact, TypedFactSetError> {
        Err(TypedFactSetError::UnexpectedFact {
            event_type: fact.event_type.clone(),
        })
    }

    fn committed_from(_fact: Box<dyn Any>) -> NoOtherFact {
        unreachable!("a fact whose event type the list declares is one of the list's types")
    }
}

impl<Head, Rest> FactList for WithMember<Head, Rest>
where
    Head: TypedPayload + Send + Sync + 'static,
    Rest: FactList + FactNameDisjoint<Head>,
{
    const DISTINCT_EVENT_TYPES: () = require_distinct(
        <Rest as FactNameDisjoint<Head>>::OK,
        Rest::DISTINCT_EVENT_TYPES,
    );

    type Committed = FactOr<Head, Rest::Committed>;

    fn append_fact_types(out: &mut Vec<TypedFactType>) {
        out.push(TypedFactType::of::<Head>());
        Rest::append_fact_types(out);
    }

    fn decode_committed(fact: &TypedFact) -> Result<Self::Committed, TypedFactSetError> {
        if Head::matches_fact(fact) {
            decode_member_fact::<Head>(std::slice::from_ref(fact)).map(FactOr::Head)
        } else {
            Rest::decode_committed(fact).map(FactOr::Rest)
        }
    }

    fn committed_from(fact: Box<dyn Any>) -> Self::Committed {
        match fact.downcast::<Head>() {
            Ok(fact) => FactOr::Head(*fact),
            Err(other) => FactOr::Rest(Rest::committed_from(other)),
        }
    }
}

/// Whether the list declares a fact with `F`'s event type, evaluated at
/// compile time.
#[doc(hidden)]
pub trait ListsEventType<F> {
    const LISTED: bool;
}

impl<F> ListsEventType<F> for EmptySet {
    const LISTED: bool = false;
}

impl<F, Head, Rest> ListsEventType<F> for WithMember<Head, Rest>
where
    F: TypedPayload,
    Head: TypedPayload,
    Rest: ListsEventType<F>,
{
    const LISTED: bool = const_str_eq(F::EVENT_TYPE, Head::EVENT_TYPE) || Rest::LISTED;
}

/// Pairwise `EVENT_TYPE` disjointness between one fact type and every member
/// of the list; combined with the recursion in [`FactList`], this is the
/// O(n^2) duplicate guard over a declared set.
#[doc(hidden)]
pub trait FactNameDisjoint<X> {
    const OK: ();
}

impl<X> FactNameDisjoint<X> for EmptySet {
    const OK: () = ();
}

impl<X, Head, Rest> FactNameDisjoint<X> for WithMember<Head, Rest>
where
    X: TypedPayload,
    Head: TypedPayload,
    Rest: FactNameDisjoint<X>,
{
    const OK: () = {
        assert!(
            !const_str_eq(X::EVENT_TYPE, Head::EVENT_TYPE),
            "duplicate member in a declared stage fact set: two members share an event \
             type; each fact type appears exactly once (FLOWIP-120z)"
        );
        Rest::OK
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct First {
        value: u32,
    }

    impl TypedPayload for First {
        const EVENT_TYPE: &'static str = "member_set.first";
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Second {
        value: u32,
    }

    impl TypedPayload for Second {
        const EVENT_TYPE: &'static str = "member_set.second";
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Third {
        value: u32,
    }

    impl TypedPayload for Third {
        const EVENT_TYPE: &'static str = "member_set.third";
    }

    type Pair = WithMember<First, WithMember<Second, EmptySet>>;
    type Triple = WithMember<First, WithMember<Second, WithMember<Third, EmptySet>>>;

    fn member_holds<List, T, Index>()
    where
        List: Member<T, Index>,
    {
    }

    fn subset_holds<Sub, Super, Proof>()
    where
        Sub: SubsetOf<Super, Proof>,
    {
    }

    #[test]
    fn membership_witnesses_resolve_at_any_position() {
        member_holds::<Pair, First, _>();
        member_holds::<Pair, Second, _>();
        member_holds::<Triple, Third, _>();
    }

    #[test]
    fn subset_witnesses_resolve_order_insensitively() {
        type Reversed = WithMember<Second, WithMember<First, EmptySet>>;

        subset_holds::<EmptySet, Pair, _>();
        subset_holds::<Pair, Triple, _>();
        subset_holds::<Pair, Reversed, _>();
        subset_holds::<Reversed, Pair, _>();
    }

    #[test]
    fn const_str_eq_compares_bytewise() {
        assert!(const_str_eq("payment.authorized", "payment.authorized"));
        assert!(!const_str_eq("payment.authorized", "payment.declined"));
        assert!(!const_str_eq("payment", "payment.authorized"));
    }

    #[test]
    fn fact_list_projects_member_types_in_declared_order() {
        let mut out = Vec::new();
        Triple::append_fact_types(&mut out);
        let names: Vec<&str> = out.iter().map(|t| t.event_type.as_str()).collect();
        assert_eq!(
            names,
            vec!["member_set.first", "member_set.second", "member_set.third"]
        );
    }

    #[test]
    fn distinct_event_types_proof_evaluates_for_distinct_sets() {
        const _: () = Triple::DISTINCT_EVENT_TYPES;
    }
}
