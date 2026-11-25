// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::effects::{CallSafety, EffectContext, EffectError, EffectOutcomeSemantics, KeyRule};
use async_trait::async_trait;
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
/// #[obzenflow_runtime::effects::port]
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
/// #[obzenflow_runtime::effects::port]
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
/// #[obzenflow_runtime::effects::port]
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
/// #[obzenflow_runtime::effects::port]
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
/// #[obzenflow_runtime::effects::port]
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
/// #[obzenflow_runtime::effects::port]
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
/// #[obzenflow_runtime::effects::port]
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
/// #[obzenflow_runtime::effects::port]
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
/// #[obzenflow_runtime::effects::port]
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
/// #[obzenflow_runtime::effects::port]
/// struct CallCounter;
/// ```
///
/// nor on a system's `impl` of its port:
///
/// ```compile_fail
/// # use obzenflow_runtime::effects::EffectError;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Reserved;
/// # impl obzenflow_core::TypedPayload for Reserved { const EVENT_TYPE: &'static str = "docs.reserved"; }
/// # trait WarehousePort: Send + Sync { fn reserve(&self) -> Result<Reserved, EffectError>; }
/// struct Warehouse;
///
/// #[obzenflow_runtime::effects::port]
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
/// #[obzenflow_runtime::effects::port]
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
/// #[obzenflow_runtime::effects::port]
/// impl CallCounter {
///     #[effect(type = "docs.reserve", exactly_once)]
///     fn reserve(&self, _: &Reserve) -> Result<Reserved, EffectError> {
///         Ok(Reserved)
///     }
/// }
///
/// #[obzenflow_runtime::effects::port]
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
    note = "declare it with `#[effect(type = \"...\", exactly_once)]`, `at_least_once` or `at_most_once` on the port method that takes it"
)]
pub trait CallRequest: Clone + std::fmt::Debug + Serialize + Send + Sync + 'static {
    const EFFECT_TYPE: &'static str;
    const SCHEMA_VERSION: u32 = 1;

    /// How a repeated call behaves: `Idempotent`, `RequiresKey`, `AtLeastOnce`,
    /// `AtMostOnce` or `Transactional`.
    type Safety: CallSafety + KeyRule<Self>;

    /// The port a system implements to answer this request.
    type Port: ?Sized + Send + Sync + 'static;

    /// What a successful call answers: facts, or a reply under `records = reply`.
    type Outcome: Clone + Send + Sync + 'static;

    /// `DomainFacts` when the answer is facts the stage writes, `RecordedReply`
    /// when it is a reply the runtime records and returns.
    type Records: EffectOutcomeSemantics<Self::Outcome>;

    async fn call(
        &self,
        port: &Self::Port,
        ctx: &mut EffectContext,
    ) -> Result<Self::Outcome, EffectError>;
}
