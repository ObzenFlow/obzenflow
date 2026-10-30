// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{
    BoundEffect, DomainFacts, Effect, EffectBindingUse, EffectContext, EffectError, EffectPortSlot,
    EffectPortSlotSet, EffectSafety, IdempotencyKey, Named,
};
use async_trait::async_trait;
use obzenflow_core::event::schema::EffectOutcomeFacts;
use obzenflow_core::SystemBindingEvidence;
use serde::Serialize;

/// A request a system answers, performed by a handler with `fx.call(request)`.
/// `#[effect]` on the port method that takes the request implements it, and
/// refuses a method it cannot answer through. A method states its guarantee:
///
/// ```compile_fail
/// # use obzenflow_runtime::effects::EffectError;
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct Reserve;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Reserved;
/// # impl obzenflow_core::TypedPayload for Reserved { const EVENT_TYPE: &'static str = "docs.reserved"; }
/// #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// trait WarehousePort: Send + Sync {
///     #[effect(type = "docs.reserve")]
///     fn reserve(&self, request: &Reserve) -> Result<Reserved, EffectError>;
/// }
/// ```
///
/// and its effect type:
///
/// ```compile_fail
/// # use obzenflow_runtime::effects::EffectError;
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct Reserve;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Reserved;
/// # impl obzenflow_core::TypedPayload for Reserved { const EVENT_TYPE: &'static str = "docs.reserved"; }
/// #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// trait WarehousePort: Send + Sync {
///     #[effect(exactly_once)]
///     fn reserve(&self, request: &Reserve) -> Result<Reserved, EffectError>;
/// }
/// ```
///
/// It takes its request by reference after `&self`:
///
/// ```compile_fail
/// # use obzenflow_runtime::effects::EffectError;
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct Reserve;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Reserved;
/// # impl obzenflow_core::TypedPayload for Reserved { const EVENT_TYPE: &'static str = "docs.reserved"; }
/// #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// trait WarehousePort: Send + Sync {
///     #[effect(type = "docs.reserve", exactly_once)]
///     fn reserve(&self, request: Reserve) -> Result<Reserved, EffectError>;
/// }
/// ```
///
/// and after it, nothing, `&EffectContext`, a key optionally followed by
/// `&EffectContext`, or a commit handle:
///
/// ```compile_fail
/// # use obzenflow_runtime::effects::EffectError;
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct Reserve;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Reserved;
/// # impl obzenflow_core::TypedPayload for Reserved { const EVENT_TYPE: &'static str = "docs.reserved"; }
/// #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// trait WarehousePort: Send + Sync {
///     #[effect(type = "docs.reserve", exactly_once)]
///     fn reserve(&self, request: &Reserve, count: u32) -> Result<Reserved, EffectError>;
/// }
/// ```
///
/// A keyed method is exactly once:
///
/// ```compile_fail
/// # use obzenflow_runtime::effects::{EffectError, IdempotencyKey, Keyed};
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct Authorize;
/// # impl Keyed for Authorize { fn idempotency_key(&self) -> IdempotencyKey { IdempotencyKey("a".into()) } }
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Authorized;
/// # impl obzenflow_core::TypedPayload for Authorized { const EVENT_TYPE: &'static str = "docs.authorized"; }
/// #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// trait GatewayPort: Send + Sync {
///     #[effect(type = "docs.authorize", at_least_once)]
///     fn authorize(&self, request: &Authorize, key: &IdempotencyKey) -> Result<Authorized, EffectError>;
/// }
/// ```
///
/// and so is a committing one:
///
/// ```compile_fail
/// # use obzenflow_runtime::effects::{EffectCommitHandle, EffectError};
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct Post;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Posted;
/// # impl obzenflow_core::TypedPayload for Posted { const EVENT_TYPE: &'static str = "docs.posted"; }
/// #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// trait LedgerPort: Send + Sync {
///     #[effect(type = "docs.post", at_least_once)]
///     fn post(&self, request: &Post, tx: EffectCommitHandle<Posted>) -> Result<Posted, EffectError>;
/// }
/// ```
///
/// A method takes a key or a commit handle, not both:
///
/// ```compile_fail
/// # use obzenflow_runtime::effects::{EffectCommitHandle, EffectError, IdempotencyKey};
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct Post;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Posted;
/// # impl obzenflow_core::TypedPayload for Posted { const EVENT_TYPE: &'static str = "docs.posted"; }
/// #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// trait LedgerPort: Send + Sync {
///     #[effect(type = "docs.post", exactly_once)]
///     fn post(
///         &self,
///         request: &Post,
///         key: &IdempotencyKey,
///         tx: EffectCommitHandle<Posted>,
///     ) -> Result<Posted, EffectError>;
/// }
/// ```
///
/// Each request type answers one effect method:
///
/// ```compile_fail
/// # use obzenflow_runtime::effects::EffectError;
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct Reserve;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Reserved;
/// # impl obzenflow_core::TypedPayload for Reserved { const EVENT_TYPE: &'static str = "docs.reserved"; }
/// #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// trait WarehousePort: Send + Sync {
///     #[effect(type = "docs.reserve", exactly_once)]
///     fn reserve(&self, request: &Reserve) -> Result<Reserved, EffectError>;
///     #[effect(type = "docs.reserve_again", exactly_once)]
///     fn reserve_again(&self, request: &Reserve) -> Result<Reserved, EffectError>;
/// }
/// ```
///
/// and each effect method records under its own effect type:
///
/// ```compile_fail
/// # use obzenflow_runtime::effects::EffectError;
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct Reserve;
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct Release;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Reserved;
/// # impl obzenflow_core::TypedPayload for Reserved { const EVENT_TYPE: &'static str = "docs.reserved"; }
/// #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// trait WarehousePort: Send + Sync {
///     #[effect(type = "docs.reserve", exactly_once)]
///     fn reserve(&self, request: &Reserve) -> Result<Reserved, EffectError>;
///     #[effect(type = "docs.reserve", exactly_once)]
///     fn release(&self, request: &Release) -> Result<Reserved, EffectError>;
/// }
/// ```
///
/// Outside a port, no macro reads `#[effect]`:
///
/// ```compile_fail
/// # use obzenflow_runtime::effects::EffectError;
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct Reserve;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Reserved;
/// # impl obzenflow_core::TypedPayload for Reserved { const EVENT_TYPE: &'static str = "docs.reserved"; }
/// trait WarehousePort: Send + Sync {
///     #[effect(type = "docs.reserve", exactly_once)]
///     fn reserve(&self, request: &Reserve) -> Result<Reserved, EffectError>;
/// }
/// ```
///
/// A struct port is marked on its inherent `impl` block, not on the struct:
///
/// ```compile_fail
/// #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// struct CallCounter;
/// ```
///
/// nor on a trait's implementation:
///
/// ```compile_fail
/// # use obzenflow_runtime::effects::EffectError;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Reserved;
/// # impl obzenflow_core::TypedPayload for Reserved { const EVENT_TYPE: &'static str = "docs.reserved"; }
/// # trait WarehousePort: Send + Sync { fn reserve(&self) -> Result<Reserved, EffectError>; }
/// struct Warehouse;
///
/// #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// impl WarehousePort for Warehouse {
///     fn reserve(&self) -> Result<Reserved, EffectError> {
///         Ok(Reserved)
///     }
/// }
/// ```
///
/// nor on a generic block:
///
/// ```compile_fail
/// # use obzenflow_runtime::effects::EffectError;
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct Reserve;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Reserved;
/// # impl obzenflow_core::TypedPayload for Reserved { const EVENT_TYPE: &'static str = "docs.reserved"; }
/// struct Counter<T>(T);
///
/// #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// impl<T> Counter<T> {
///     #[effect(type = "docs.reserve", exactly_once)]
///     fn reserve(&self, _: &Reserve) -> Result<Reserved, EffectError> {
///         Ok(Reserved)
///     }
/// }
/// ```
///
/// and on one block per struct:
///
/// ```compile_fail,E0119
/// # use obzenflow_runtime::effects::EffectError;
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct Reserve;
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct Release;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Reserved;
/// # impl obzenflow_core::TypedPayload for Reserved { const EVENT_TYPE: &'static str = "docs.reserved"; }
/// struct CallCounter;
///
/// #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// impl CallCounter {
///     #[effect(type = "docs.reserve", exactly_once)]
///     fn reserve(&self, _: &Reserve) -> Result<Reserved, EffectError> {
///         Ok(Reserved)
///     }
/// }
///
/// #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// impl CallCounter {
///     #[effect(type = "docs.release", exactly_once)]
///     fn release(&self, _: &Release) -> Result<Reserved, EffectError> {
///         Ok(Reserved)
///     }
/// }
/// ```
#[doc(hidden)]
#[async_trait]
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a declared request",
    label = "no `#[effect]` port method takes `{Self}`",
    note = "declare it with `#[effect(type = \"...\", exactly_once)]` or `at_least_once` on the port method that takes it"
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

/// The idempotency key of a request whose port method takes one, derived from
/// the request's business identity. The system deduplicates repeats by it.
///
/// ```
/// # use async_trait::async_trait;
/// # use obzenflow_runtime::effects::{EffectError, IdempotencyKey, Keyed};
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Scored;
/// # impl obzenflow_core::TypedPayload for Scored { const EVENT_TYPE: &'static str = "docs.keyed.scored"; }
/// #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
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
/// #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
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
        note = "a request's `Safety` is `Idempotent`, `RequiresKey`, `AtLeastOnce` or `Transactional`"
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
    note = "a request's `Safety` is `Idempotent`, `RequiresKey`, `AtLeastOnce` or `Transactional`"
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

/// An `at_least_once` request, acknowledged after its stage's handler with
/// `.at_least_once::<R>()`.
#[doc(hidden)]
pub struct AtLeastOnce;

/// An `exactly_once` request whose port commits its outcome through its
/// `EffectCommitHandle`. The committed record is the outcome.
#[doc(hidden)]
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

impl<R> KeyRule<R> for Transactional {
    fn key(_: &R) -> Option<IdempotencyKey> {
        None
    }
}

/// The named effect that performs one request through the system its handler
/// holds for the request's port.
#[doc(hidden)]
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
