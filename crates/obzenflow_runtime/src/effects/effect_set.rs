// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Effect capability sets (FLOWIP-120z).
//!
//! An effect set is the exhaustive, compiler-visible set of effects one
//! handler may perform. Its members are the names written in it, a request
//! type or an effect type, and [`EffectName`] maps each to its effect; the
//! stable runtime and archive identity remains `EFFECT_TYPE`. Sets are
//! zero-sized declarations, never persisted.

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

    /// The name itself. `effect_set!` writes each member through it, so a
    /// member that names no effect is reported where it is written.
    type Name;
}

impl<R: CallRequest> EffectName for R {
    type Effect = Call<R>;
    type Name = R;
}

impl<R: CallRequest> EffectName for Call<R> {
    type Effect = Call<R>;
    type Name = Call<R>;
}

/// Capability authority for one handler's permitted effect identities.
///
/// A deliberately effect-free handler declares `effect_set![]`; the hidden
/// `Members` projection is what `Effects::perform` bounds and the DSL
/// manifest equality assertions consume.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an effect capability set",
    note = "name one request declared with `#[effect]`, `effect_set![RequestA, RequestB]` for \
            several, or `effect_set![]` for none"
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

/// One request is a set whose only member is that request.
#[diagnostic::do_not_recommend]
impl<R: CallRequest> EffectSet for R {
    type Members = WithMember<R, EmptySet>;

    fn effect_types() -> Vec<&'static str> {
        vec![R::EFFECT_TYPE]
    }
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
/// type AllowedEffects = effect_set![StockReservation, StockRelease];
/// type NoEffects = effect_set![];
/// ```
///
/// A handler with one effect names its request alone, as
/// `type AllowedEffects = AuthorizeRequest;`. Duplicate members are compile
/// errors (a const-eval panic when the set is used). Set equality elsewhere
/// is semantic, not order-sensitive.
#[macro_export]
macro_rules! effect_set {
    [$($member:ty),* $(,)?] => {
        $crate::effects::DeclaredEffectSet<
            $crate::effect_set!(@list $($member),*)
        >
    };
    (@list) => { $crate::__private::EmptySet };
    (@list $head:ty $(, $rest:ty)*) => {
        $crate::__private::WithMember<
            <$head as $crate::effects::EffectName>::Name,
            $crate::effect_set!(@list $($rest),*)
        >
    };
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

    fn member_holds<Set, E, Index>()
    where
        Set: EffectSet,
        Set::Members: Member<E, Index>,
    {
    }

    #[test]
    fn declared_effect_set_projects_identities_in_declared_order() {
        type Allowed = crate::effect_set![PingRequest, PongRequest];
        assert_eq!(
            <Allowed as EffectSet>::effect_types(),
            vec!["effect_set.ping", "effect_set.pong"]
        );
    }

    #[test]
    fn one_request_is_a_set_of_one() {
        assert_eq!(
            <PingRequest as EffectSet>::effect_types(),
            vec!["effect_set.ping"]
        );
        member_holds::<PingRequest, PingRequest, _>();
    }

    #[test]
    fn empty_effect_set_is_a_valid_declaration() {
        type NoEffects = crate::effect_set![];
        assert!(<NoEffects as EffectSet>::effect_types().is_empty());
    }

    #[test]
    fn membership_witnesses_resolve_against_written_names() {
        type Allowed = crate::effect_set![PingRequest, PongRequest];
        member_holds::<Allowed, PingRequest, _>();
        member_holds::<Allowed, PongRequest, _>();
    }

    #[test]
    fn distinct_member_guard_evaluates_for_distinct_sets() {
        const _: () = assert_distinct_effect_set::<crate::effect_set![PingRequest, PongRequest]>();
        const _: () = assert_distinct_effect_set::<PingRequest>();
        const _: () = assert_distinct_effect_set::<crate::effect_set![]>();
    }
}
