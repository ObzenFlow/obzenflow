// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{Events, IngressDecoder};
use crate::stages::common::handlers::source::{
    Continuous, SourceRead, TypedAsyncInfiniteSourceHandler,
};
use crate::systems::{EffectKind, Port, PortKind, QueryKind};
use async_trait::async_trait;
use obzenflow_core::OneFactOutcome;
use std::any::Any;

/// `subscribe` starts the line that builds a push source from its decoder.
/// `.through(port)` ends the line and returns the source handler. The decoder runs once
/// for each event that the system of the port delivers.
///
/// ```
/// # use obzenflow_runtime::events::{source_channel, subscribe, IngressDecoder};
/// # use obzenflow_runtime::systems::Port;
/// #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// struct Tick {
///     id: u64,
/// }
/// # impl obzenflow_core::TypedPayload for Tick { const EVENT_TYPE: &'static str = "docs.tick"; }
///
/// #[derive(Clone)]
/// struct TickIngress;
///
/// impl IngressDecoder for TickIngress {
///     type Output = Tick;
/// }
///
/// let (tx, rx) = source_channel::<Tick>(16);
/// let ticks = subscribe(TickIngress).through(Port::to(rx));
/// ```
pub fn subscribe<D: IngressDecoder>(decoder: D) -> PendingSubscription<D> {
    PendingSubscription { decoder }
}

/// `PendingSubscription` is the `subscribe` line before `.through(port)`. It holds the
/// decoder of the line.
pub struct PendingSubscription<D> {
    decoder: D,
}

impl<D: IngressDecoder> PendingSubscription<D> {
    /// `through` binds the line to the event port of its system and returns the
    /// source handler. A query port or an effect port does not compile:
    ///
    /// ```compile_fail,E0277
    /// # use obzenflow_runtime::events::{subscribe, IngressDecoder, Subscription};
    /// # use obzenflow_runtime::queries::{FiniteRead, QueryContext, QueryError};
    /// # use obzenflow_runtime::systems::Port;
    /// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    /// # struct Tick;
    /// # impl obzenflow_core::TypedPayload for Tick { const EVENT_TYPE: &'static str = "docs.tick"; }
    /// # #[derive(Clone)]
    /// # struct TickIngress;
    /// # impl IngressDecoder for TickIngress { type Output = Tick; }
    /// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    /// # struct Position;
    /// #[obzenflow_runtime::effects::port]
    /// #[async_trait::async_trait]
    /// trait TickFeed: Send + Sync {
    ///     #[query(type = "docs.ticks.read")]
    ///     async fn read(&self, from: &Position, ctx: &QueryContext)
    ///         -> Result<FiniteRead<Tick, Position>, QueryError>;
    /// }
    ///
    /// fn ticks(feed: Port<dyn TickFeed>) -> Subscription<Tick> {
    ///     subscribe(TickIngress).through(feed)
    /// }
    /// ```
    ///
    /// ```compile_fail,E0277
    /// # use obzenflow_runtime::effects::EffectError;
    /// # use obzenflow_runtime::events::{subscribe, IngressDecoder, Subscription};
    /// # use obzenflow_runtime::systems::Port;
    /// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    /// # struct Tick;
    /// # impl obzenflow_core::TypedPayload for Tick { const EVENT_TYPE: &'static str = "docs.tick"; }
    /// # #[derive(Clone)]
    /// # struct TickIngress;
    /// # impl IngressDecoder for TickIngress { type Output = Tick; }
    /// # #[derive(Clone, Debug, serde::Serialize)]
    /// # struct Append;
    /// #[obzenflow_runtime::effects::port]
    /// trait TickLedger: Send + Sync {
    ///     #[effect(type = "docs.ticks.append", exactly_once)]
    ///     fn append(&self, request: &Append) -> Result<Tick, EffectError>;
    /// }
    ///
    /// fn ticks(ledger: Port<dyn TickLedger>) -> Subscription<Tick> {
    ///     subscribe(TickIngress).through(ledger)
    /// }
    /// ```
    pub fn through<P>(self, port: Port<P>) -> Subscription<D::Output>
    where
        P: ?Sized + PortKind + Send + Sync + 'static,
        P::Kind: NotQueryPort<P> + NotEffectPort<P>,
    {
        let port: Box<dyn Any> = Box::new(port);
        let port = *port
            .downcast::<Port<dyn Events>>()
            .expect("only the event port is neither a query port nor an effect port");
        let decoder = self.decoder;
        Subscription {
            read: Box::new(move || SourceRead::events(port.clone(), decoder.clone())),
        }
    }
}

mod kind {
    pub struct EventKind;
}

impl PortKind for dyn Events {
    type Kind = kind::EventKind;
}

#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`subscribe` binds an event port; `Port<{P}>` is a query port"
)]
pub trait NotQueryPort<P: ?Sized> {}

impl<P: ?Sized> NotQueryPort<P> for kind::EventKind {}
impl<P: ?Sized> NotQueryPort<P> for EffectKind {}

#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`subscribe` binds an event port; `Port<{P}>` is an effect port"
)]
pub trait NotEffectPort<P: ?Sized> {}

impl<P: ?Sized> NotEffectPort<P> for kind::EventKind {}
impl<P: ?Sized> NotEffectPort<P> for QueryKind {}

/// `Subscription` is the source handler that a `subscribe` line builds. `source!`
/// takes it.
pub struct Subscription<Output> {
    read: Box<dyn Fn() -> SourceRead<Output, Continuous> + Send + Sync>,
}

impl<Output> std::fmt::Debug for Subscription<Output> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Subscription")
            .field("output", &std::any::type_name::<Output>())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<Output: OneFactOutcome + Send + Sync + 'static> TypedAsyncInfiniteSourceHandler
    for Subscription<Output>
{
    type Output = Output;

    fn read(&self) -> SourceRead<Output, Continuous> {
        (self.read)()
    }
}
