// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Source rows that take a `Port`.
//!
//! Each source family has one port trait, the trait-object form of its
//! connector trait, so a flow builder chooses between connectors with
//! `Port::to` and `Port::deferred`. The port's type repeats the row's output
//! and its connectors' continuation, and every system behind it shares both.
//! A connector's reader crosses the port behind a box and keeps its own poll
//! timeout, drain and observation hook.

use super::continuation::{
    AsyncFiniteContinuation, AsyncInfiniteContinuation, FiniteContinuation, InfiniteContinuation,
    LiveGuarantee, Reattach, Refuse, Seek, SourceContinuation,
};
use super::erased::{
    UnifiedAsyncFiniteSourceHandler, UnifiedAsyncInfiniteSourceHandler, UnifiedFiniteSourceHandler,
    UnifiedInfiniteSourceHandler,
};
use super::positioned::{PositionedFiniteReader, SeekableSource, SourceRecord};
use super::prepared::{sealed as admission_sealed, AdmitSource, PortSource};
use super::typed::{
    SourceObservationSink, TypedAsyncFiniteSourceHandler, TypedAsyncInfiniteSourceHandler,
    TypedFiniteSourceHandler, TypedInfiniteSourceHandler,
};
use super::SourceError;
use crate::effects::{Port, PortImpl, ServesPort, SourceSystem, SourceSystemFault};
use crate::stages::source::{
    AsyncFiniteSourceConnector, AsyncInfiniteSourceConnector, FiniteSourceConnector,
    InfiniteSourceConnector, SourceReaderInitContext,
};
use async_trait::async_trait;
use obzenflow_core::event::SourceDiagnosticReason;
use obzenflow_core::source::{SourceInputIdentity, SourcePosition};
use obzenflow_core::OneFactOutcome;
use std::sync::Arc;
use std::time::Duration;

type Finite = dyn UnifiedFiniteSourceHandler;
type AsyncFinite = dyn UnifiedAsyncFiniteSourceHandler;
type Infinite = dyn UnifiedInfiniteSourceHandler;
type AsyncInfinite = dyn UnifiedAsyncInfiniteSourceHandler;

type PortReader<Family, K, Out> = <K as PortContinuation<Family, Out>>::Reader;

/// The continuations a source port of one family can repeat, each with the
/// boxed reader its connectors open.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a continuation that connectors of this source family declare",
    note = "a source port's type repeats its connectors' continuation: `Refuse`, `Seek<P>` for a synchronous finite source, or `Reattach<G>` for an infinite one"
)]
pub trait PortContinuation<Family: ?Sized, Out>: Send + Sync + 'static {
    type Reader: Send + 'static;
}

/// Boxes a connector's own reader for the port its continuation declares.
#[doc(hidden)]
pub trait BoxesReader<Family: ?Sized, R, Out>: PortContinuation<Family, Out> {
    fn boxed(reader: R) -> Self::Reader;
}

/// A synchronous finite reader behind a port.
#[doc(hidden)]
pub struct PortFiniteReader<Out> {
    reader: Box<dyn TypedFiniteSourceHandler<Output = Out>>,
}

impl<Out> TypedFiniteSourceHandler for PortFiniteReader<Out>
where
    Out: OneFactOutcome + Send + Sync + 'static,
{
    type Output = Out;

    fn next(&mut self) -> Result<Option<Vec<Out>>, SourceError> {
        self.reader.next()
    }
}

/// A positioned synchronous finite reader behind a port.
#[doc(hidden)]
pub struct PortPositionedReader<Out, P> {
    reader: Box<dyn PositionedFiniteReader<Output = Out, Position = P>>,
}

impl<Out, P> SeekableSource for PortPositionedReader<Out, P>
where
    Out: OneFactOutcome + Send + Sync + 'static,
    P: SourcePosition,
{
    type Position = P;

    fn input_identity(&self) -> &SourceInputIdentity {
        self.reader.input_identity()
    }

    fn beginning(&self) -> P {
        self.reader.beginning()
    }

    fn seek(&mut self, position: P) -> Result<(), SourceError> {
        self.reader.seek(position)
    }
}

impl<Out, P> PositionedFiniteReader for PortPositionedReader<Out, P>
where
    Out: OneFactOutcome + Send + Sync + 'static,
    P: SourcePosition,
{
    type Output = Out;

    fn next(&mut self) -> Result<Option<Vec<SourceRecord<Out, P>>>, SourceError> {
        self.reader.next()
    }
}

/// An asynchronous finite reader behind a port.
#[doc(hidden)]
pub struct PortAsyncFiniteReader<Out> {
    reader: Box<dyn TypedAsyncFiniteSourceHandler<Output = Out>>,
}

#[async_trait]
impl<Out> TypedAsyncFiniteSourceHandler for PortAsyncFiniteReader<Out>
where
    Out: OneFactOutcome + Send + Sync + 'static,
{
    type Output = Out;

    fn poll_timeout(&self) -> Option<Duration> {
        self.reader.poll_timeout()
    }

    async fn next(&mut self) -> Result<Option<Vec<Out>>, SourceError> {
        self.reader.next().await
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        self.reader.drain().await
    }

    fn install_source_observation_sink(&mut self, sink: SourceObservationSink) {
        self.reader.install_source_observation_sink(sink);
    }
}

/// A synchronous infinite reader behind a port.
#[doc(hidden)]
pub struct PortInfiniteReader<Out> {
    reader: Box<dyn TypedInfiniteSourceHandler<Output = Out>>,
}

impl<Out> TypedInfiniteSourceHandler for PortInfiniteReader<Out>
where
    Out: OneFactOutcome + Send + Sync + 'static,
{
    type Output = Out;

    fn next(&mut self) -> Result<Vec<Out>, SourceError> {
        self.reader.next()
    }
}

/// An asynchronous infinite reader behind a port.
#[doc(hidden)]
pub struct PortAsyncInfiniteReader<Out> {
    reader: Box<dyn TypedAsyncInfiniteSourceHandler<Output = Out>>,
}

#[async_trait]
impl<Out> TypedAsyncInfiniteSourceHandler for PortAsyncInfiniteReader<Out>
where
    Out: OneFactOutcome + Send + Sync + 'static,
{
    type Output = Out;

    fn poll_timeout(&self) -> Option<Duration> {
        self.reader.poll_timeout()
    }

    async fn next(&mut self) -> Result<Vec<Out>, SourceError> {
        self.reader.next().await
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        self.reader.drain().await
    }

    fn install_source_observation_sink(&mut self, sink: SourceObservationSink) {
        self.reader.install_source_observation_sink(sink);
    }
}

impl<Out> PortContinuation<Finite, Out> for Refuse
where
    Out: OneFactOutcome + Send + Sync + 'static,
{
    type Reader = PortFiniteReader<Out>;
}

impl<R> BoxesReader<Finite, R, R::Output> for Refuse
where
    R: TypedFiniteSourceHandler + 'static,
{
    fn boxed(reader: R) -> PortFiniteReader<R::Output> {
        PortFiniteReader {
            reader: Box::new(reader),
        }
    }
}

impl<Out, P> PortContinuation<Finite, Out> for Seek<P>
where
    Out: OneFactOutcome + Send + Sync + 'static,
    P: SourcePosition,
{
    type Reader = PortPositionedReader<Out, P>;
}

impl<P, R> BoxesReader<Finite, R, R::Output> for Seek<P>
where
    P: SourcePosition,
    R: PositionedFiniteReader<Position = P> + 'static,
{
    fn boxed(reader: R) -> PortPositionedReader<R::Output, P> {
        PortPositionedReader {
            reader: Box::new(reader),
        }
    }
}

impl<Out> PortContinuation<AsyncFinite, Out> for Refuse
where
    Out: OneFactOutcome + Send + Sync + 'static,
{
    type Reader = PortAsyncFiniteReader<Out>;
}

impl<R> BoxesReader<AsyncFinite, R, R::Output> for Refuse
where
    R: TypedAsyncFiniteSourceHandler + 'static,
{
    fn boxed(reader: R) -> PortAsyncFiniteReader<R::Output> {
        PortAsyncFiniteReader {
            reader: Box::new(reader),
        }
    }
}

impl<Out> PortContinuation<Infinite, Out> for Refuse
where
    Out: OneFactOutcome + Send + Sync + 'static,
{
    type Reader = PortInfiniteReader<Out>;
}

impl<R> BoxesReader<Infinite, R, R::Output> for Refuse
where
    R: TypedInfiniteSourceHandler + 'static,
{
    fn boxed(reader: R) -> PortInfiniteReader<R::Output> {
        PortInfiniteReader {
            reader: Box::new(reader),
        }
    }
}

impl<Out, G> PortContinuation<Infinite, Out> for Reattach<G>
where
    Out: OneFactOutcome + Send + Sync + 'static,
    G: LiveGuarantee,
{
    type Reader = PortInfiniteReader<Out>;
}

impl<G, R> BoxesReader<Infinite, R, R::Output> for Reattach<G>
where
    G: LiveGuarantee,
    R: TypedInfiniteSourceHandler + 'static,
{
    fn boxed(reader: R) -> PortInfiniteReader<R::Output> {
        PortInfiniteReader {
            reader: Box::new(reader),
        }
    }
}

impl<Out> PortContinuation<AsyncInfinite, Out> for Refuse
where
    Out: OneFactOutcome + Send + Sync + 'static,
{
    type Reader = PortAsyncInfiniteReader<Out>;
}

impl<R> BoxesReader<AsyncInfinite, R, R::Output> for Refuse
where
    R: TypedAsyncInfiniteSourceHandler + 'static,
{
    fn boxed(reader: R) -> PortAsyncInfiniteReader<R::Output> {
        PortAsyncInfiniteReader {
            reader: Box::new(reader),
        }
    }
}

impl<Out, G> PortContinuation<AsyncInfinite, Out> for Reattach<G>
where
    Out: OneFactOutcome + Send + Sync + 'static,
    G: LiveGuarantee,
{
    type Reader = PortAsyncInfiniteReader<Out>;
}

impl<G, R> BoxesReader<AsyncInfinite, R, R::Output> for Reattach<G>
where
    G: LiveGuarantee,
    R: TypedAsyncInfiniteSourceHandler + 'static,
{
    fn boxed(reader: R) -> PortAsyncInfiniteReader<R::Output> {
        PortAsyncInfiniteReader {
            reader: Box::new(reader),
        }
    }
}

/// The port a `source!` row takes, behind which sits any synchronous finite
/// connector with output `Out` and continuation `K`.
///
/// A flow builder writes this type only in the signature of a helper that
/// returns a source's port. `Port::to` and `Port::deferred` infer it from the
/// connector everywhere else. Two connectors that share an output and a
/// continuation meet in one port, and each binds alone:
///
/// ```
/// # use obzenflow_runtime::effects::Port;
/// # use obzenflow_runtime::stages::source::{
/// #     FiniteSource, FiniteSourceConnector, PositionedFiniteReader, Refuse, Seek,
/// #     SeekableSource, SourceError, SourceInputIdentity, SourcePosition,
/// #     SourceReaderInitContext, SourceRecord, TypedFiniteSourceHandler,
/// # };
/// # #[derive(serde::Serialize, serde::Deserialize)]
/// # struct Ticket;
/// # impl obzenflow_core::TypedPayload for Ticket {
/// #     const EVENT_TYPE: &'static str = "doc.ticket";
/// # }
/// # struct TicketReader;
/// # impl TypedFiniteSourceHandler for TicketReader {
/// #     type Output = Ticket;
/// #     fn next(&mut self) -> Result<Option<Vec<Ticket>>, SourceError> { Ok(None) }
/// # }
/// # #[derive(Clone, serde::Serialize, serde::Deserialize)]
/// # struct Offset(u64);
/// # impl SourcePosition for Offset {}
/// # struct OffsetReader { identity: SourceInputIdentity }
/// # impl SeekableSource for OffsetReader {
/// #     type Position = Offset;
/// #     fn input_identity(&self) -> &SourceInputIdentity { &self.identity }
/// #     fn beginning(&self) -> Offset { Offset(0) }
/// #     fn seek(&mut self, _: Offset) -> Result<(), SourceError> { Ok(()) }
/// # }
/// # impl PositionedFiniteReader for OffsetReader {
/// #     type Output = Ticket;
/// #     fn next(&mut self) -> Result<Option<Vec<SourceRecord<Ticket, Offset>>>, SourceError> {
/// #         Ok(None)
/// #     }
/// # }
/// # fn offset_reader() -> OffsetReader { unimplemented!() }
/// struct ScriptedTickets;
/// #[obzenflow_runtime::effects::system("scripted-tickets")]
/// impl FiniteSourceConnector for ScriptedTickets {
///     type Output = Ticket;
///     type Reader = TicketReader;
///     type Continuation = Refuse;
///     fn open(&self, _: SourceReaderInitContext) -> Result<TicketReader, SourceError> {
///         Ok(TicketReader)
///     }
/// }
///
/// struct ArchivedTickets;
/// #[obzenflow_runtime::effects::system("archived-tickets")]
/// impl FiniteSourceConnector for ArchivedTickets {
///     type Output = Ticket;
///     type Reader = TicketReader;
///     type Continuation = Refuse;
///     fn open(&self, _: SourceReaderInitContext) -> Result<TicketReader, SourceError> {
///         Ok(TicketReader)
///     }
/// }
///
/// struct PositionedTickets;
/// #[obzenflow_runtime::effects::system("positioned-tickets")]
/// impl FiniteSourceConnector for PositionedTickets {
///     type Output = Ticket;
///     type Reader = OffsetReader;
///     type Continuation = Seek<Offset>;
///     fn open(&self, _: SourceReaderInitContext) -> Result<OffsetReader, SourceError> {
///         Ok(offset_reader())
///     }
/// }
///
/// fn tickets(scripted: bool) -> Port<dyn FiniteSource<Ticket, Refuse>> {
///     if scripted {
///         Port::to(ScriptedTickets)
///     } else {
///         Port::to(ArchivedTickets)
///     }
/// }
/// let positioned: Port<dyn FiniteSource<Ticket, Seek<Offset>>> = Port::to(PositionedTickets);
/// ```
///
/// Two connectors whose continuations differ don't fit one port, so joining
/// them does not compile:
///
/// ```compile_fail,E0308
/// # use obzenflow_runtime::effects::Port;
/// # use obzenflow_runtime::stages::source::{
/// #     FiniteSource, FiniteSourceConnector, PositionedFiniteReader, Refuse, Seek,
/// #     SeekableSource, SourceError, SourceInputIdentity, SourcePosition,
/// #     SourceReaderInitContext, SourceRecord, TypedFiniteSourceHandler,
/// # };
/// # #[derive(serde::Serialize, serde::Deserialize)]
/// # struct Ticket;
/// # impl obzenflow_core::TypedPayload for Ticket {
/// #     const EVENT_TYPE: &'static str = "doc.ticket";
/// # }
/// # struct TicketReader;
/// # impl TypedFiniteSourceHandler for TicketReader {
/// #     type Output = Ticket;
/// #     fn next(&mut self) -> Result<Option<Vec<Ticket>>, SourceError> { Ok(None) }
/// # }
/// # #[derive(Clone, serde::Serialize, serde::Deserialize)]
/// # struct Offset(u64);
/// # impl SourcePosition for Offset {}
/// # struct OffsetReader { identity: SourceInputIdentity }
/// # impl SeekableSource for OffsetReader {
/// #     type Position = Offset;
/// #     fn input_identity(&self) -> &SourceInputIdentity { &self.identity }
/// #     fn beginning(&self) -> Offset { Offset(0) }
/// #     fn seek(&mut self, _: Offset) -> Result<(), SourceError> { Ok(()) }
/// # }
/// # impl PositionedFiniteReader for OffsetReader {
/// #     type Output = Ticket;
/// #     fn next(&mut self) -> Result<Option<Vec<SourceRecord<Ticket, Offset>>>, SourceError> {
/// #         Ok(None)
/// #     }
/// # }
/// # fn offset_reader() -> OffsetReader { unimplemented!() }
/// # struct ScriptedTickets;
/// # #[obzenflow_runtime::effects::system("scripted-tickets")]
/// # impl FiniteSourceConnector for ScriptedTickets {
/// #     type Output = Ticket;
/// #     type Reader = TicketReader;
/// #     type Continuation = Refuse;
/// #     fn open(&self, _: SourceReaderInitContext) -> Result<TicketReader, SourceError> {
/// #         Ok(TicketReader)
/// #     }
/// # }
/// # struct PositionedTickets;
/// # #[obzenflow_runtime::effects::system("positioned-tickets")]
/// # impl FiniteSourceConnector for PositionedTickets {
/// #     type Output = Ticket;
/// #     type Reader = OffsetReader;
/// #     type Continuation = Seek<Offset>;
/// #     fn open(&self, _: SourceReaderInitContext) -> Result<OffsetReader, SourceError> {
/// #         Ok(offset_reader())
/// #     }
/// # }
/// # fn main() {
/// # let scripted = true;
/// let tickets = if scripted {
///     Port::to(ScriptedTickets)
/// } else {
///     Port::to(PositionedTickets)
/// };
/// # }
/// ```
pub trait FiniteSource<Out, K>: Send + Sync + 'static
where
    K: PortContinuation<Finite, Out>,
{
    #[doc(hidden)]
    fn open_port_reader(
        &self,
        context: SourceReaderInitContext,
    ) -> Result<PortReader<Finite, K, Out>, SourceError>;
}

/// The port an `async_source!` row takes, behind which sits any asynchronous
/// finite connector with output `Out` and continuation `K`.
#[async_trait]
pub trait AsyncFiniteSource<Out, K>: Send + Sync + 'static
where
    K: PortContinuation<AsyncFinite, Out>,
{
    #[doc(hidden)]
    async fn open_port_reader(
        &self,
        context: SourceReaderInitContext,
    ) -> Result<PortReader<AsyncFinite, K, Out>, SourceError>;
}

/// The port an `infinite_source!` row takes, behind which sits any
/// synchronous infinite connector with output `Out` and continuation `K`.
pub trait InfiniteSource<Out, K>: Send + Sync + 'static
where
    K: PortContinuation<Infinite, Out>,
{
    #[doc(hidden)]
    fn open_port_reader(
        &self,
        context: SourceReaderInitContext,
    ) -> Result<PortReader<Infinite, K, Out>, SourceError>;
}

/// The port an `async_infinite_source!` row takes, behind which sits any
/// asynchronous infinite connector with output `Out` and continuation `K`.
#[async_trait]
pub trait AsyncInfiniteSource<Out, K>: Send + Sync + 'static
where
    K: PortContinuation<AsyncInfinite, Out>,
{
    #[doc(hidden)]
    async fn open_port_reader(
        &self,
        context: SourceReaderInitContext,
    ) -> Result<PortReader<AsyncInfinite, K, Out>, SourceError>;
}

impl<C> FiniteSource<C::Output, C::Continuation> for C
where
    C: FiniteSourceConnector,
    C::Continuation: BoxesReader<Finite, C::Reader, C::Output>,
{
    fn open_port_reader(
        &self,
        context: SourceReaderInitContext,
    ) -> Result<PortReader<Finite, C::Continuation, C::Output>, SourceError> {
        self.open(context)
            .map(<C::Continuation as BoxesReader<Finite, C::Reader, C::Output>>::boxed)
    }
}

#[async_trait]
impl<C> AsyncFiniteSource<C::Output, C::Continuation> for C
where
    C: AsyncFiniteSourceConnector,
    C::Continuation: BoxesReader<AsyncFinite, C::Reader, C::Output>,
{
    async fn open_port_reader(
        &self,
        context: SourceReaderInitContext,
    ) -> Result<PortReader<AsyncFinite, C::Continuation, C::Output>, SourceError> {
        self.open(context)
            .await
            .map(<C::Continuation as BoxesReader<AsyncFinite, C::Reader, C::Output>>::boxed)
    }
}

impl<C> InfiniteSource<C::Output, C::Continuation> for C
where
    C: InfiniteSourceConnector,
    C::Continuation: BoxesReader<Infinite, C::Reader, C::Output>,
{
    fn open_port_reader(
        &self,
        context: SourceReaderInitContext,
    ) -> Result<PortReader<Infinite, C::Continuation, C::Output>, SourceError> {
        self.open(context)
            .map(<C::Continuation as BoxesReader<Infinite, C::Reader, C::Output>>::boxed)
    }
}

#[async_trait]
impl<C> AsyncInfiniteSource<C::Output, C::Continuation> for C
where
    C: AsyncInfiniteSourceConnector,
    C::Continuation: BoxesReader<AsyncInfinite, C::Reader, C::Output>,
{
    async fn open_port_reader(
        &self,
        context: SourceReaderInitContext,
    ) -> Result<PortReader<AsyncInfinite, C::Continuation, C::Output>, SourceError> {
        self.open(context)
            .await
            .map(<C::Continuation as BoxesReader<AsyncInfinite, C::Reader, C::Output>>::boxed)
    }
}

impl<C> ServesPort<dyn FiniteSource<C::Output, C::Continuation>, C> for PortImpl<C>
where
    C: FiniteSourceConnector,
    C::Continuation: BoxesReader<Finite, C::Reader, C::Output>,
{
    fn coerce(port: Arc<C>) -> Arc<dyn FiniteSource<C::Output, C::Continuation>> {
        port
    }
}

impl<C> ServesPort<dyn AsyncFiniteSource<C::Output, C::Continuation>, C> for PortImpl<C>
where
    C: AsyncFiniteSourceConnector,
    C::Continuation: BoxesReader<AsyncFinite, C::Reader, C::Output>,
{
    fn coerce(port: Arc<C>) -> Arc<dyn AsyncFiniteSource<C::Output, C::Continuation>> {
        port
    }
}

impl<C> ServesPort<dyn InfiniteSource<C::Output, C::Continuation>, C> for PortImpl<C>
where
    C: InfiniteSourceConnector,
    C::Continuation: BoxesReader<Infinite, C::Reader, C::Output>,
{
    fn coerce(port: Arc<C>) -> Arc<dyn InfiniteSource<C::Output, C::Continuation>> {
        port
    }
}

impl<C> ServesPort<dyn AsyncInfiniteSource<C::Output, C::Continuation>, C> for PortImpl<C>
where
    C: AsyncInfiniteSourceConnector,
    C::Continuation: BoxesReader<AsyncInfinite, C::Reader, C::Output>,
{
    fn coerce(port: Arc<C>) -> Arc<dyn AsyncInfiniteSource<C::Output, C::Continuation>> {
        port
    }
}

fn unavailable(fault: SourceSystemFault) -> SourceError {
    match fault {
        SourceSystemFault::Refused(_) => {
            SourceError::Transport(SourceDiagnosticReason::InputUnavailable.into())
        }
        SourceSystemFault::Unbuilt => {
            SourceError::Other(SourceDiagnosticReason::Unclassified.into())
        }
    }
}

impl<Out, K> admission_sealed::Admitted<Finite, PortSource> for Port<dyn FiniteSource<Out, K>>
where
    Out: OneFactOutcome + Send + Sync + 'static,
    K: PortContinuation<Finite, Out>,
{
}

impl<Out, K> AdmitSource<Finite, PortSource> for Port<dyn FiniteSource<Out, K>>
where
    Out: OneFactOutcome + Send + Sync + 'static,
    K: PortContinuation<Finite, Out> + FiniteContinuation<PortReader<Finite, K, Out>, Output = Out>,
{
    type Output = Out;
    type Handler = <K as FiniteContinuation<PortReader<Finite, K, Out>>>::Handler;
    const CONTINUATION: SourceContinuation =
        <K as FiniteContinuation<PortReader<Finite, K, Out>>>::KIND;

    fn prepare(self) -> Self::Handler {
        self.prepare_with_system().0
    }

    fn prepare_with_system(self) -> (Self::Handler, Option<SourceSystem>) {
        let system = self.source_system();
        let resolving = system.clone();
        let handler =
            <K as FiniteContinuation<PortReader<Finite, K, Out>>>::cold(Box::new(move |context| {
                resolving
                    .resolve::<dyn FiniteSource<Out, K>>()
                    .map_err(unavailable)?
                    .open_port_reader(context)
            }));
        (handler, Some(system))
    }
}

impl<Out, K> admission_sealed::Admitted<AsyncFinite, PortSource>
    for Port<dyn AsyncFiniteSource<Out, K>>
where
    Out: OneFactOutcome + Send + Sync + 'static,
    K: PortContinuation<AsyncFinite, Out>,
{
}

impl<Out, K> AdmitSource<AsyncFinite, PortSource> for Port<dyn AsyncFiniteSource<Out, K>>
where
    Out: OneFactOutcome + Send + Sync + 'static,
    K: PortContinuation<AsyncFinite, Out>
        + AsyncFiniteContinuation<PortReader<AsyncFinite, K, Out>, Output = Out>,
{
    type Output = Out;
    type Handler = <K as AsyncFiniteContinuation<PortReader<AsyncFinite, K, Out>>>::Handler;
    const CONTINUATION: SourceContinuation =
        <K as AsyncFiniteContinuation<PortReader<AsyncFinite, K, Out>>>::KIND;

    fn prepare(self) -> Self::Handler {
        self.prepare_with_system().0
    }

    fn prepare_with_system(self) -> (Self::Handler, Option<SourceSystem>) {
        let system = self.source_system();
        let resolving = system.clone();
        let handler = <K as AsyncFiniteContinuation<PortReader<AsyncFinite, K, Out>>>::cold(
            Box::new(move |context| {
                Box::pin(async move {
                    resolving
                        .resolve::<dyn AsyncFiniteSource<Out, K>>()
                        .map_err(unavailable)?
                        .open_port_reader(context)
                        .await
                })
            }),
        );
        (handler, Some(system))
    }
}

impl<Out, K> admission_sealed::Admitted<Infinite, PortSource> for Port<dyn InfiniteSource<Out, K>>
where
    Out: OneFactOutcome + Send + Sync + 'static,
    K: PortContinuation<Infinite, Out>,
{
}

impl<Out, K> AdmitSource<Infinite, PortSource> for Port<dyn InfiniteSource<Out, K>>
where
    Out: OneFactOutcome + Send + Sync + 'static,
    K: PortContinuation<Infinite, Out>
        + InfiniteContinuation<PortReader<Infinite, K, Out>, Output = Out>,
{
    type Output = Out;
    type Handler = <K as InfiniteContinuation<PortReader<Infinite, K, Out>>>::Handler;
    const CONTINUATION: SourceContinuation =
        <K as InfiniteContinuation<PortReader<Infinite, K, Out>>>::KIND;

    fn prepare(self) -> Self::Handler {
        self.prepare_with_system().0
    }

    fn prepare_with_system(self) -> (Self::Handler, Option<SourceSystem>) {
        let system = self.source_system();
        let resolving = system.clone();
        let handler = <K as InfiniteContinuation<PortReader<Infinite, K, Out>>>::cold(Box::new(
            move |context| {
                resolving
                    .resolve::<dyn InfiniteSource<Out, K>>()
                    .map_err(unavailable)?
                    .open_port_reader(context)
            },
        ));
        (handler, Some(system))
    }
}

impl<Out, K> admission_sealed::Admitted<AsyncInfinite, PortSource>
    for Port<dyn AsyncInfiniteSource<Out, K>>
where
    Out: OneFactOutcome + Send + Sync + 'static,
    K: PortContinuation<AsyncInfinite, Out>,
{
}

impl<Out, K> AdmitSource<AsyncInfinite, PortSource> for Port<dyn AsyncInfiniteSource<Out, K>>
where
    Out: OneFactOutcome + Send + Sync + 'static,
    K: PortContinuation<AsyncInfinite, Out>
        + AsyncInfiniteContinuation<PortReader<AsyncInfinite, K, Out>, Output = Out>,
{
    type Output = Out;
    type Handler = <K as AsyncInfiniteContinuation<PortReader<AsyncInfinite, K, Out>>>::Handler;
    const CONTINUATION: SourceContinuation =
        <K as AsyncInfiniteContinuation<PortReader<AsyncInfinite, K, Out>>>::KIND;

    fn prepare(self) -> Self::Handler {
        self.prepare_with_system().0
    }

    fn prepare_with_system(self) -> (Self::Handler, Option<SourceSystem>) {
        let system = self.source_system();
        let resolving = system.clone();
        let handler = <K as AsyncInfiniteContinuation<PortReader<AsyncInfinite, K, Out>>>::cold(
            Box::new(move |context| {
                Box::pin(async move {
                    resolving
                        .resolve::<dyn AsyncInfiniteSource<Out, K>>()
                        .map_err(unavailable)?
                        .open_port_reader(context)
                        .await
                })
            }),
        );
        (handler, Some(system))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::effects::EffectPortRegistry;
    use obzenflow_core::{StageId, TypedPayload};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Barrier;

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Ticket;
    impl TypedPayload for Ticket {
        const EVENT_TYPE: &'static str = "source.port.ticket";
    }

    struct TicketReader;
    impl TypedFiniteSourceHandler for TicketReader {
        type Output = Ticket;
        fn next(&mut self) -> Result<Option<Vec<Ticket>>, SourceError> {
            Ok(None)
        }
    }

    struct ScriptedTickets {
        opens: Arc<AtomicUsize>,
    }

    #[crate::effects::system("scripted-tickets")]
    impl FiniteSourceConnector for ScriptedTickets {
        type Output = Ticket;
        type Reader = TicketReader;
        type Continuation = Refuse;
        fn open(&self, _context: SourceReaderInitContext) -> Result<TicketReader, SourceError> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            Ok(TicketReader)
        }
    }

    struct ArchivedTickets;

    #[crate::effects::system("archived-tickets")]
    impl FiniteSourceConnector for ArchivedTickets {
        type Output = Ticket;
        type Reader = TicketReader;
        type Continuation = Refuse;
        fn open(&self, _context: SourceReaderInitContext) -> Result<TicketReader, SourceError> {
            Ok(TicketReader)
        }
    }

    struct SlowReader;

    #[async_trait]
    impl TypedAsyncFiniteSourceHandler for SlowReader {
        type Output = Ticket;
        fn poll_timeout(&self) -> Option<Duration> {
            Some(Duration::from_secs(120))
        }
        async fn next(&mut self) -> Result<Option<Vec<Ticket>>, SourceError> {
            Ok(None)
        }
    }

    struct SlowTickets;

    #[crate::effects::system("slow-tickets")]
    #[async_trait]
    impl AsyncFiniteSourceConnector for SlowTickets {
        type Output = Ticket;
        type Reader = SlowReader;
        type Continuation = Refuse;
        async fn open(&self, _context: SourceReaderInitContext) -> Result<SlowReader, SourceError> {
            Ok(SlowReader)
        }
    }

    enum Input {
        Scripted,
        Archived,
    }

    fn context() -> SourceReaderInitContext {
        SourceReaderInitContext {
            stage_id: StageId::new(),
            stage_name: "tickets".into(),
            flow_name: "ports".into(),
        }
    }

    #[test]
    fn source_port_two_systems_unify() {
        let names: Vec<String> = [Input::Scripted, Input::Archived]
            .into_iter()
            .map(|input| -> Port<dyn FiniteSource<Ticket, Refuse>> {
                match input {
                    Input::Scripted => Port::to(ScriptedTickets {
                        opens: Arc::default(),
                    }),
                    Input::Archived => Port::to(ArchivedTickets),
                }
            })
            .map(|port| port.system_name().to_owned())
            .collect();
        assert_eq!(names, ["scripted-tickets", "archived-tickets"]);
    }

    #[test]
    fn source_port_concurrent_opens_resolve_once() {
        let builds = Arc::new(AtomicUsize::new(0));
        let opens = Arc::new(AtomicUsize::new(0));
        let port: Port<dyn FiniteSource<Ticket, Refuse>> = Port::deferred({
            let builds = builds.clone();
            let opens = opens.clone();
            move || {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok(ScriptedTickets {
                    opens: opens.clone(),
                })
            }
        });
        let mut rows = [port.clone(), port].map(
            <Port<dyn FiniteSource<Ticket, Refuse>> as AdmitSource<Finite, PortSource>>::prepare_with_system,
        );
        let mut registry = EffectPortRegistry::new();
        for (_, system) in &rows {
            registry.install_source_system(system.as_ref().expect("a port row names a system"));
        }
        let run = registry.into_run_registry();
        let start = Barrier::new(rows.len());
        std::thread::scope(|scope| {
            for (handler, system) in &mut rows {
                system
                    .as_ref()
                    .expect("a port row names a system")
                    .attach(&run);
                let start = &start;
                scope.spawn(move || {
                    start.wait();
                    handler.acquire(context()).expect("the shared port opens");
                });
            }
        });
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert_eq!(opens.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn source_port_reader_returns_connector_poll_timeout() {
        let port: Port<dyn AsyncFiniteSource<Ticket, Refuse>> = Port::to(SlowTickets);
        let (mut handler, system) = <Port<dyn AsyncFiniteSource<Ticket, Refuse>> as AdmitSource<
            AsyncFinite,
            PortSource,
        >>::prepare_with_system(port);
        let system = system.expect("a port row names a system");
        let mut registry = EffectPortRegistry::new();
        registry.install_source_system(&system);
        system.attach(&registry.into_run_registry());
        handler
            .acquire(context())
            .await
            .expect("the port opens its reader");
        assert_eq!(handler.poll_timeout(), Some(Duration::from_secs(120)));
    }
}
