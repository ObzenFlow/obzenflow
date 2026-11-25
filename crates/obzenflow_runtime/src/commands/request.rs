// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::effects::{CallSafety, EffectContext, EffectError, EffectOutcomeSemantics, KeyRule};
use async_trait::async_trait;
use serde::Serialize;

/// An `#[effect]` method without a guarantee does not compile:
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
/// An `#[effect]` method without an effect type does not compile:
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
/// An `#[effect]` method that takes its request by value does not compile:
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
/// After the request, an `#[effect]` method takes nothing, `&EffectContext`, a key, a key and
/// `&EffectContext`, or a commit handle. Other parameters do not compile:
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
/// A keyed `#[effect]` method that is not `exactly_once` does not compile:
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
/// An `#[effect]` method that takes a commit handle and is not `exactly_once` does not compile:
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
/// An `#[effect]` method that takes a key and a commit handle does not compile:
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
/// Two `#[effect]` methods that take the same request type do not compile:
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
/// Two `#[effect]` methods with the same effect type do not compile:
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
/// An `#[effect]` method outside a port does not compile:
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
/// `#[obzenflow::port]` on a struct does not compile:
///
/// ```compile_fail
/// #[obzenflow_runtime::effects::port]
/// struct CallCounter;
/// ```
///
/// `#[obzenflow::port]` on the `impl` of a port trait for a system does not compile:
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
/// `#[obzenflow::port]` on a generic `impl` block does not compile:
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
/// `#[obzenflow::port]` on two `impl` blocks of one struct does not compile:
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

    type Safety: CallSafety + KeyRule<Self>;

    type Port: ?Sized + Send + Sync + 'static;

    type Outcome: Clone + Send + Sync + 'static;

    type Records: EffectOutcomeSemantics<Self::Outcome>;

    async fn call(
        &self,
        port: &Self::Port,
        ctx: &mut EffectContext,
    ) -> Result<Self::Outcome, EffectError>;
}
