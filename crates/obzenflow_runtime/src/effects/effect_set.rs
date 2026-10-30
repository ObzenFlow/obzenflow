// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Effect capability sets.
//!
//! An effect set is the exhaustive, compiler-visible set of effects one
//! handler may perform. `#[derive(EffectHandler)]` builds it from the
//! handler's `#[system]` fields: each field allows every effect its port
//! declares, or the requests its `#[system(...)]` lists. [`EffectName`] maps
//! each member to its effect; the stable runtime and archive identity remains
//! `EFFECT_TYPE`. Sets are zero-sized declarations, never persisted.

use super::call::{Call, CallRequest};
use super::declaration::Effect;
use obzenflow_core::event::schema::{
    const_str_eq, require_distinct, EmptySet, MemberList, WithMember,
};
use std::marker::PhantomData;

/// A name written where an effect is named: a request type, which names the
/// effect that performs it, or an effect type, which names itself.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a declared request",
    label = "no `#[effect]` port method takes `{Self}`",
    note = "declare it with `#[effect(type = \"...\", exactly_once)]` or `at_least_once` on the port method that takes it"
)]
pub trait EffectName: 'static {
    type Effect: Effect;
}

impl<R: CallRequest> EffectName for R {
    type Effect = Call<R>;
}

impl<R: CallRequest> EffectName for Call<R> {
    type Effect = Call<R>;
}

/// Capability authority for one handler's permitted effect identities.
///
/// A handler with no `#[system]` field allows no effect. The hidden `Members`
/// projection is what `Effects::perform` bounds and the DSL manifest equality
/// assertions consume.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an effect capability set",
    note = "a handler's effects come from its `#[system]` fields; derive `EffectHandler` and \
            mark each field that holds a system `#[system]`"
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

/// The zero-sized set of the members `L` lists, which `#[derive(EffectHandler)]`
/// and the framework's own handlers name. Never constructed and never
/// persisted; it exists only as a type-level declaration.
#[doc(hidden)]
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

/// Names written as nested pairs, `(A, (B, ()))`, read as set members.
#[doc(hidden)]
pub trait RequestList {
    type Members;
}

impl RequestList for () {
    type Members = EmptySet;
}

impl<Head, Rest: RequestList> RequestList for (Head, Rest) {
    type Members = WithMember<Head, Rest::Members>;
}

/// A member list whose heads are effect names, with a const duplicate guard
/// over their `EFFECT_TYPE` and a value-level identity projection.
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
    Head: EffectName,
    Rest: EffectList + EffectTypeDisjoint<Head>,
{
    const DISTINCT_EFFECT_TYPES: () = require_distinct(
        <Rest as EffectTypeDisjoint<Head>>::OK,
        Rest::DISTINCT_EFFECT_TYPES,
    );

    fn append_effect_types(out: &mut Vec<&'static str>) {
        out.push(<Head::Effect as Effect>::EFFECT_TYPE);
        Rest::append_effect_types(out);
    }
}

/// Pairwise `EFFECT_TYPE` disjointness between one effect name and every
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
    X: EffectName,
    Head: EffectName,
    Rest: EffectTypeDisjoint<X>,
{
    const OK: () = {
        assert!(
            !const_str_eq(
                <X::Effect as Effect>::EFFECT_TYPE,
                <Head::Effect as Effect>::EFFECT_TYPE
            ),
            "two effects this handler allows share an effect type; each effect type is \
             allowed once"
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

#[cfg(test)]
mod tests {
    use super::super::error::EffectError;
    use super::super::{EffectContext, Idempotent};
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

    #[derive(Debug, Clone, Serialize)]
    struct PingRequest;

    #[async_trait]
    impl CallRequest for PingRequest {
        const EFFECT_TYPE: &'static str = "effect_set.ping";
        type Safety = Idempotent;
        type Port = ();
        type Outcome = PingValue;

        async fn call(&self, _: &(), _: &mut EffectContext) -> Result<PingValue, EffectError> {
            Ok(PingValue { value: 1 })
        }
    }

    #[derive(Debug, Clone, Serialize)]
    struct PongRequest;

    #[async_trait]
    impl CallRequest for PongRequest {
        const EFFECT_TYPE: &'static str = "effect_set.pong";
        type Safety = Idempotent;
        type Port = ();
        type Outcome = PingValue;

        async fn call(&self, _: &(), _: &mut EffectContext) -> Result<PingValue, EffectError> {
            Ok(PingValue { value: 2 })
        }
    }

    type PingPong = DeclaredEffectSet<<(PingRequest, (PongRequest, ())) as RequestList>::Members>;
    type NoEffects = DeclaredEffectSet<<() as RequestList>::Members>;

    fn member_holds<Set, E, Index>()
    where
        Set: EffectSet,
        Set::Members: Member<E, Index>,
    {
    }

    #[test]
    fn declared_effect_set_projects_identities_in_declared_order() {
        assert_eq!(
            <PingPong as EffectSet>::effect_types(),
            vec!["effect_set.ping", "effect_set.pong"]
        );
    }

    #[test]
    fn empty_effect_set_is_a_valid_declaration() {
        assert!(<NoEffects as EffectSet>::effect_types().is_empty());
    }

    #[test]
    fn membership_witnesses_resolve_against_written_names() {
        member_holds::<PingPong, PingRequest, _>();
        member_holds::<PingPong, PongRequest, _>();
    }

    #[test]
    fn distinct_member_guard_evaluates_for_distinct_sets() {
        const _: () = assert_distinct_effect_set::<PingPong>();
        const _: () = assert_distinct_effect_set::<NoEffects>();
    }
}
