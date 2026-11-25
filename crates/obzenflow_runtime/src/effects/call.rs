// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{
    BoundEffect, Effect, EffectBindingUse, EffectContext, EffectError, EffectPortSlot,
    EffectPortSlotSet, EffectSafety, IdempotencyKey, Named,
};
use crate::commands::CallRequest;
use async_trait::async_trait;
use obzenflow_core::SystemBindingEvidence;

/// The idempotency key of a request whose port method takes one, derived from
/// the request's business identity. The system deduplicates repeats by it.
///
/// ```
/// # use async_trait::async_trait;
/// # use obzenflow_runtime::effects::{EffectError, IdempotencyKey, Keyed};
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Scored;
/// # impl obzenflow_core::TypedPayload for Scored { const EVENT_TYPE: &'static str = "docs.keyed.scored"; }
/// #[obzenflow_runtime::effects::port]
/// #[async_trait]
/// trait RiskEnginePort: Send + Sync {
///     #[effect(type = "docs.keyed.score", exactly_once)]
///     async fn score(
///         &self,
///         request: &ScoreRequest,
///         key: &IdempotencyKey,
///     ) -> Result<Scored, EffectError>;
/// }
///
/// #[derive(Clone, Debug, serde::Serialize)]
/// struct ScoreRequest {
///     txn_id: u64,
/// }
///
/// impl Keyed for ScoreRequest {
///     fn idempotency_key(&self) -> IdempotencyKey {
///         IdempotencyKey(format!("score:{}", self.txn_id))
///     }
/// }
/// ```
///
/// A method that takes a key does not compile while its request lacks one:
///
/// ```compile_fail,E0277
/// # use async_trait::async_trait;
/// # use obzenflow_runtime::effects::{EffectError, IdempotencyKey};
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Scored;
/// # impl obzenflow_core::TypedPayload for Scored { const EVENT_TYPE: &'static str = "docs.keyed.scored"; }
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct ScoreRequest { txn_id: u64 }
/// #[obzenflow_runtime::effects::port]
/// #[async_trait]
/// trait RiskEnginePort: Send + Sync {
///     #[effect(type = "docs.keyed.score", exactly_once)]
///     async fn score(
///         &self,
///         request: &ScoreRequest,
///         key: &IdempotencyKey,
///     ) -> Result<Scored, EffectError>;
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` needs an idempotency key, because its port method takes one",
    label = "`{Self}` has no `Keyed` impl",
    note = "implement `Keyed` for `{Self}`, deriving the key from the request's business identity"
)]
pub trait Keyed {
    fn idempotency_key(&self) -> IdempotencyKey;
}

mod sealed {
    #[diagnostic::on_unimplemented(
        message = "`{Self}` is not a request safety class",
        note = "a request's `Safety` is `Idempotent`, `RequiresKey`, `AtLeastOnce`, `AtMostOnce` or `Transactional`"
    )]
    pub trait CallSafety {}
}

/// A request's safety class, which `#[effect]` selects from the method's
/// guarantee and parameters. No other class can be declared:
///
/// ```compile_fail,E0277
/// # use obzenflow_runtime::effects::{CallSafety, EffectSafety};
/// struct Batched;
///
/// impl CallSafety for Batched {
///     # const SAFETY: EffectSafety = EffectSafety::Idempotent;
/// }
/// ```
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a request safety class",
    note = "a request's `Safety` is `Idempotent`, `RequiresKey`, `AtLeastOnce`, `AtMostOnce` or `Transactional`"
)]
pub trait CallSafety: sealed::CallSafety {
    #[doc(hidden)]
    const SAFETY: EffectSafety;
}

/// An `exactly_once` request whose operation is idempotent.
#[doc(hidden)]
pub struct Idempotent;

/// An `exactly_once` request whose system deduplicates by the key its `Keyed`
/// impl derives.
#[doc(hidden)]
pub struct RequiresKey;

/// An `at_least_once` request whose destination may act again on resume.
#[doc(hidden)]
pub struct AtLeastOnce;

/// An `at_most_once` request, whose call a crash left in doubt is recorded
/// abandoned and never made again.
#[doc(hidden)]
pub struct AtMostOnce;

/// An `exactly_once` request whose port commits its outcome through its
/// `EffectCommitHandle`. The committed record is the outcome.
#[doc(hidden)]
pub struct Transactional;

impl sealed::CallSafety for Idempotent {}
impl sealed::CallSafety for RequiresKey {}
impl sealed::CallSafety for AtLeastOnce {}
impl sealed::CallSafety for AtMostOnce {}
impl sealed::CallSafety for Transactional {}

impl CallSafety for Idempotent {
    const SAFETY: EffectSafety = EffectSafety::Idempotent;
}

impl CallSafety for RequiresKey {
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;
}

impl CallSafety for AtLeastOnce {
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentAtLeastOnce;
}

impl CallSafety for AtMostOnce {
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentAtMostOnce;
}

impl CallSafety for Transactional {
    const SAFETY: EffectSafety = EffectSafety::Transactional;
}

/// The key a request of each class sends. `#[effect]` implements it for
/// `RequiresKey` per request, through the request's `Keyed` impl, so a
/// missing key is reported at the method that takes one.
#[doc(hidden)]
pub trait KeyRule<R> {
    fn key(request: &R) -> Option<IdempotencyKey>;
}

impl<R> KeyRule<R> for Idempotent {
    fn key(_: &R) -> Option<IdempotencyKey> {
        None
    }
}

impl<R> KeyRule<R> for AtLeastOnce {
    fn key(_: &R) -> Option<IdempotencyKey> {
        None
    }
}

impl<R> KeyRule<R> for AtMostOnce {
    fn key(_: &R) -> Option<IdempotencyKey> {
        None
    }
}

impl<R> KeyRule<R> for Transactional {
    fn key(_: &R) -> Option<IdempotencyKey> {
        None
    }
}

/// The named effect that performs one request through the system its handler
/// holds for the request's port.
#[doc(hidden)]
pub struct Call<R: CallRequest> {
    pub(crate) request: R,
    projection: EffectBindingUse<Call<R>>,
}

impl<R: CallRequest> Call<R> {
    pub(crate) fn new(request: R, projection: EffectBindingUse<Call<R>>) -> Self {
        Self {
            request,
            projection,
        }
    }
}

pub(crate) const fn call_port_slot<R: CallRequest>() -> EffectPortSlot<R::Port> {
    EffectPortSlot::new("port")
}

impl<R: CallRequest> Clone for Call<R> {
    fn clone(&self) -> Self {
        Self {
            request: self.request.clone(),
            projection: self.projection.clone(),
        }
    }
}

impl<R: CallRequest> std::fmt::Debug for Call<R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Call")
            .field("effect_type", &R::EFFECT_TYPE)
            .field("request", &self.request)
            .finish()
    }
}

#[async_trait]
impl<R: CallRequest> Effect for Call<R> {
    const EFFECT_TYPE: &'static str = R::EFFECT_TYPE;
    const SCHEMA_VERSION: u32 = R::SCHEMA_VERSION;
    const SAFETY: EffectSafety = <R::Safety as CallSafety>::SAFETY;
    type BindingMode = Named<SystemBindingEvidence>;
    type Outcome = R::Outcome;
    type OutcomeSemantics = R::Records;

    fn label(&self) -> &str {
        R::EFFECT_TYPE
    }

    fn canonical_input(&self) -> serde_json::Value {
        serde_json::json!(self.request)
    }

    async fn execute(&self, ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        let port = ctx.port(call_port_slot::<R>())?;
        self.request.call(&port, ctx).await
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        <R::Safety as KeyRule<R>>::key(&self.request)
    }
}

impl<R: CallRequest> BoundEffect for Call<R> {
    type BindingEvidence = SystemBindingEvidence;

    fn binding_use(&self) -> &EffectBindingUse<Self> {
        &self.projection
    }

    fn required_slots() -> EffectPortSlotSet {
        EffectPortSlotSet::single(call_port_slot::<R>())
    }
}
