// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{
    BoundEffect, DomainFacts, Effect, EffectBindingUse, EffectContext, EffectError,
    EffectPortSlot, EffectPortSlotSet, EffectSafety, IdempotencyKey, Named,
};
use async_trait::async_trait;
use obzenflow_core::event::schema::EffectOutcomeFacts;
use obzenflow_core::SystemBindingEvidence;
use serde::Serialize;

/// A request a system answers, performed by a handler with `fx.call(request)`.
#[async_trait]
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a request",
    label = "`fx.call` performs a request that a system answers",
    note = "declare the effect by implementing `CallRequest` on its request type"
)]
pub trait CallRequest: Clone + std::fmt::Debug + Serialize + Send + Sync + 'static {
    const EFFECT_TYPE: &'static str;
    const SCHEMA_VERSION: u32 = 1;

    /// How a repeated call behaves: `Idempotent`, `RequiresKey`, `AtLeastOnce` or
    /// `Transactional`.
    type Safety: CallSafety + KeyRule<Self>;

    /// The port a system implements to answer this request.
    type Port: ?Sized + Send + Sync + 'static;

    /// The facts a successful call records.
    type Outcome: EffectOutcomeFacts + Clone + Send + Sync + 'static;

    async fn call(
        &self,
        port: &Self::Port,
        ctx: &mut EffectContext,
    ) -> Result<Self::Outcome, EffectError>;
}

/// The idempotency key of a request whose safety class is `RequiresKey`.
///
/// ```
/// # use async_trait::async_trait;
/// # use obzenflow_runtime::effects::{CallRequest, EffectContext, EffectError, IdempotencyKey, Keyed, RequiresKey};
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Scored;
/// # impl obzenflow_core::TypedPayload for Scored { const EVENT_TYPE: &'static str = "docs.keyed.scored"; }
/// #[async_trait]
/// trait RiskEnginePort: Send + Sync {
///     async fn score(&self, request: &ScoreRequest) -> Result<Scored, EffectError>;
/// }
///
/// #[derive(Clone, Debug, serde::Serialize)]
/// struct ScoreRequest {
///     txn_id: u64,
/// }
///
/// #[async_trait]
/// impl CallRequest for ScoreRequest {
///     const EFFECT_TYPE: &'static str = "docs.keyed.score";
///     type Safety = RequiresKey;
///     type Port = dyn RiskEnginePort;
///     type Outcome = Scored;
///
///     async fn call(
///         &self,
///         engine: &dyn RiskEnginePort,
///         _: &mut EffectContext,
///     ) -> Result<Scored, EffectError> {
///         engine.score(self).await
///     }
/// }
///
/// impl Keyed for ScoreRequest {
///     fn idempotency_key(&self) -> IdempotencyKey {
///         IdempotencyKey(format!("score:{}", self.txn_id))
///     }
/// }
/// ```
///
/// A `RequiresKey` request without it does not compile at its own impl:
///
/// ```compile_fail,E0277
/// # use async_trait::async_trait;
/// # use obzenflow_runtime::effects::{CallRequest, EffectContext, EffectError, RequiresKey};
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Scored;
/// # impl obzenflow_core::TypedPayload for Scored { const EVENT_TYPE: &'static str = "docs.keyed.scored"; }
/// # trait RiskEnginePort: Send + Sync {}
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct ScoreRequest { txn_id: u64 }
/// #[async_trait]
/// impl CallRequest for ScoreRequest {
///     const EFFECT_TYPE: &'static str = "docs.keyed.score";
///     type Safety = RequiresKey;
///     type Port = dyn RiskEnginePort;
///     type Outcome = Scored;
///
///     async fn call(
///         &self,
///         _: &dyn RiskEnginePort,
///         _: &mut EffectContext,
///     ) -> Result<Scored, EffectError> {
///         Ok(Scored)
///     }
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not idempotent, so it needs an idempotency key",
    label = "`{Self}` declares `type Safety = RequiresKey`",
    note = "implement `Keyed` for `{Self}`, deriving the key from the request's business identity"
)]
pub trait Keyed {
    fn idempotency_key(&self) -> IdempotencyKey;
}

mod sealed {
    #[diagnostic::on_unimplemented(
        message = "`{Self}` is not a request safety class",
        note = "a request's `Safety` is `Idempotent`, `RequiresKey`, `AtLeastOnce` or `Transactional`"
    )]
    pub trait CallSafety {}
}

/// A request's safety class: [`Idempotent`], [`RequiresKey`], [`AtLeastOnce`]
/// or [`Transactional`]. No other class can be declared:
///
/// ```compile_fail,E0277
/// # use obzenflow_runtime::effects::{CallSafety, EffectSafety};
/// struct Batched;
///
/// impl CallSafety for Batched {
///     # const SAFETY: EffectSafety = EffectSafety::Idempotent;
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a request safety class",
    note = "a request's `Safety` is `Idempotent`, `RequiresKey`, `AtLeastOnce` or `Transactional`"
)]
pub trait CallSafety: sealed::CallSafety {
    #[doc(hidden)]
    const SAFETY: EffectSafety;
}

/// A request a repeated call answers the same way.
pub struct Idempotent;

/// A request a system deduplicates by the key its `Keyed` impl derives.
pub struct RequiresKey;

/// A paid request a repeat may perform again, acknowledged after its stage's
/// handler with `.at_least_once::<E>()`.
pub struct AtLeastOnce;

/// A request whose port commits its outcome through the handle
/// `ctx.transaction(self)` returns, acknowledged after its stage's handler
/// with `.transactional::<E>()`. The committed record is the outcome.
pub struct Transactional;

impl sealed::CallSafety for Idempotent {}
impl sealed::CallSafety for RequiresKey {}
impl sealed::CallSafety for AtLeastOnce {}
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

impl CallSafety for Transactional {
    const SAFETY: EffectSafety = EffectSafety::Transactional;
}

#[doc(hidden)]
pub trait KeyRule<R> {
    fn key(request: &R) -> Option<IdempotencyKey>;
}

impl<R> KeyRule<R> for Idempotent {
    fn key(_: &R) -> Option<IdempotencyKey> {
        None
    }
}

impl<R: Keyed> KeyRule<R> for RequiresKey {
    fn key(request: &R) -> Option<IdempotencyKey> {
        Some(request.idempotency_key())
    }
}

impl<R> KeyRule<R> for AtLeastOnce {
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
pub struct Call<R: CallRequest> {
    request: R,
    projection: EffectBindingUse<Call<R>>,
}

impl<R: CallRequest> Call<R> {
    pub(super) fn new(request: R, projection: EffectBindingUse<Call<R>>) -> Self {
        Self {
            request,
            projection,
        }
    }
}

pub(super) const fn call_port_slot<R: CallRequest>() -> EffectPortSlot<R::Port> {
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
    type OutcomeSemantics = DomainFacts;

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
