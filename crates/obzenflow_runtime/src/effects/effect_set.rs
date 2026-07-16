// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Effect capability sets (FLOWIP-120z).
//!
//! An effect set is the exhaustive, compiler-visible set of effect request
//! types one handler may pass to `perform`. Membership is proved directly
//! against the concrete `Effect` type; there is no second associated effect
//! identity, and the stable runtime and archive identity remains
//! `EFFECT_TYPE`. Sets are zero-sized declarations, never persisted.

use super::declaration::Effect;
use obzenflow_core::event::schema::{
    const_str_eq, require_distinct, EmptySet, MemberList, WithMember,
};
use std::marker::PhantomData;

/// Capability authority for one handler's permitted effect identities.
///
/// A deliberately effect-free handler declares `effect_set![]`; the hidden
/// `Members` projection is what `Effects::perform` bounds and the DSL
/// manifest equality assertions consume.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an effect capability set",
    note = "declare the handler's permitted effects as `effect_set![EffectA, EffectB]`; a \
            deliberately effect-free handler declares `effect_set![]` (FLOWIP-120z)"
)]
pub trait EffectSet: Send + Sync + 'static {
    /// Hidden type-level member projection; proof machinery, not public
    /// vocabulary.
    #[doc(hidden)]
    type Members: MemberList;

    /// Declared effect identities for diagnostics and runtime defence in
    /// depth.
    fn effect_types() -> Vec<&'static str>;

    /// Const duplicate guard over member `EFFECT_TYPE`s; explicit sets panic
    /// at const-eval on duplicates.
    #[doc(hidden)]
    const MEMBERS_DISTINCT: () = ();
}

/// Zero-sized expansion target of `effect_set![...]`. Never constructed and
/// never persisted; it exists only as a type-level declaration.
pub struct DeclaredEffectSet<L>(PhantomData<fn() -> L>);

impl<L: EffectList> EffectSet for DeclaredEffectSet<L> {
    type Members = L;

    fn effect_types() -> Vec<&'static str> {
        let mut out = Vec::new();
        L::append_effect_types(&mut out);
        out
    }

    const MEMBERS_DISTINCT: () = L::DISTINCT_EFFECT_TYPES;
}

/// A member list whose heads are effect request types, with a const
/// duplicate guard over `EFFECT_TYPE` and a value-level identity
/// projection.
#[doc(hidden)]
pub trait EffectList: MemberList {
    const DISTINCT_EFFECT_TYPES: ();
    fn append_effect_types(out: &mut Vec<&'static str>);
}

impl EffectList for EmptySet {
    const DISTINCT_EFFECT_TYPES: () = ();

    fn append_effect_types(_out: &mut Vec<&'static str>) {}
}

impl<Head, Rest> EffectList for WithMember<Head, Rest>
where
    Head: Effect,
    Rest: EffectList + EffectTypeDisjoint<Head>,
{
    const DISTINCT_EFFECT_TYPES: () = require_distinct(
        <Rest as EffectTypeDisjoint<Head>>::OK,
        Rest::DISTINCT_EFFECT_TYPES,
    );

    fn append_effect_types(out: &mut Vec<&'static str>) {
        out.push(Head::EFFECT_TYPE);
        Rest::append_effect_types(out);
    }
}

/// Pairwise `EFFECT_TYPE` disjointness between one effect type and every
/// member of the list.
#[doc(hidden)]
pub trait EffectTypeDisjoint<X> {
    const OK: ();
}

impl<X> EffectTypeDisjoint<X> for EmptySet {
    const OK: () = ();
}

impl<X, Head, Rest> EffectTypeDisjoint<X> for WithMember<Head, Rest>
where
    X: Effect,
    Head: Effect,
    Rest: EffectTypeDisjoint<X>,
{
    const OK: () = {
        assert!(
            !const_str_eq(X::EFFECT_TYPE, Head::EFFECT_TYPE),
            "duplicate member in a declared effect capability set: two members share an \
             effect type; each effect appears exactly once (FLOWIP-120z)"
        );
        Rest::OK
    };
}

/// Check-time hook forcing a set's duplicate guard, mirroring
/// `assert_distinct_stage_fact_set` for the effect side.
#[doc(hidden)]
pub const fn assert_distinct_effect_set<S: EffectSet>() {
    S::MEMBERS_DISTINCT
}

/// Declare an effect capability set in type position (FLOWIP-120z):
///
/// ```ignore
/// type AllowedEffects = effect_set![AuthorizePayment];
/// type NoEffects = effect_set![];
/// ```
///
/// Duplicate members are compile errors (a const-eval panic when the set is
/// used). Set equality elsewhere is semantic, not order-sensitive.
#[macro_export]
macro_rules! effect_set {
    [$($member:ty),* $(,)?] => {
        $crate::effects::DeclaredEffectSet<
            $crate::effect_set!(@list $($member),*)
        >
    };
    (@list) => { $crate::obzenflow_core::event::schema::EmptySet };
    (@list $head:ty $(, $rest:ty)*) => {
        $crate::obzenflow_core::event::schema::WithMember<
            $head,
            $crate::effect_set!(@list $($rest),*)
        >
    };
}

#[cfg(test)]
mod tests {
    use super::super::declaration::EffectSafety;
    use super::super::error::EffectError;
    use super::super::EffectContext;
    use super::*;
    use async_trait::async_trait;
    use obzenflow_core::event::schema::Member;
    use obzenflow_core::TypedPayload;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct PingValue {
        value: u32,
    }

    impl TypedPayload for PingValue {
        const EVENT_TYPE: &'static str = "effect_set.ping_value";
    }

    #[derive(Debug, Clone)]
    struct Ping;

    #[async_trait]
    impl Effect for Ping {
        const EFFECT_TYPE: &'static str = "effect_set.ping";
        const SCHEMA_VERSION: u32 = 1;
        const SAFETY: EffectSafety = EffectSafety::Idempotent;

        type Outcome = PingValue;

        fn label(&self) -> &str {
            "ping"
        }

        fn canonical_input(&self) -> serde_json::Value {
            serde_json::json!({})
        }

        async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
            Ok(PingValue { value: 1 })
        }
    }

    #[derive(Debug, Clone)]
    struct Pong;

    #[async_trait]
    impl Effect for Pong {
        const EFFECT_TYPE: &'static str = "effect_set.pong";
        const SCHEMA_VERSION: u32 = 1;
        const SAFETY: EffectSafety = EffectSafety::Idempotent;

        type Outcome = PingValue;

        fn label(&self) -> &str {
            "pong"
        }

        fn canonical_input(&self) -> serde_json::Value {
            serde_json::json!({})
        }

        async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
            Ok(PingValue { value: 2 })
        }
    }

    fn member_holds<Set, E, Index>()
    where
        Set: EffectSet,
        Set::Members: Member<E, Index>,
    {
    }

    #[test]
    fn declared_effect_set_projects_identities_in_declared_order() {
        type Allowed = crate::effect_set![Ping, Pong];
        assert_eq!(
            <Allowed as EffectSet>::effect_types(),
            vec!["effect_set.ping", "effect_set.pong"]
        );
    }

    #[test]
    fn empty_effect_set_is_a_valid_declaration() {
        type NoEffects = crate::effect_set![];
        assert!(<NoEffects as EffectSet>::effect_types().is_empty());
    }

    #[test]
    fn membership_witnesses_resolve_against_concrete_effects() {
        type Allowed = crate::effect_set![Ping, Pong];
        member_holds::<Allowed, Ping, _>();
        member_holds::<Allowed, Pong, _>();
    }

    #[test]
    fn distinct_member_guard_evaluates_for_distinct_sets() {
        const _: () = assert_distinct_effect_set::<crate::effect_set![Ping, Pong]>();
        const _: () = assert_distinct_effect_set::<crate::effect_set![]>();
    }
}
