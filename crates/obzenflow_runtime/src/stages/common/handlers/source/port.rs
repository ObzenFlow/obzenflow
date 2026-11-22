// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::continuation::{
    AsyncFiniteContinuation, AsyncInfiniteContinuation, FiniteContinuation, InfiniteContinuation,
    LiveGuarantee, Reattach, Refuse, Seek, SourceContinuation,
};
use super::erased::{
    UnifiedAsyncFiniteSourceHandler, UnifiedAsyncInfiniteSourceHandler, UnifiedFiniteSourceHandler,
    UnifiedInfiniteSourceHandler,
};
use super::positioned::{PositionedFiniteReader, SeekableSource, SourceRecord};
use super::prepared::{sealed as admission_sealed, AdmitSource, AdmittedSource};
use super::typed::{
    SourceObservationSink, TypedAsyncFiniteSourceHandler, TypedAsyncInfiniteSourceHandler,
    TypedFiniteSourceHandler, TypedInfiniteSourceHandler,
};
use super::SourceError;
use crate::effects::{Port, PortImpl, ServesPort, SourceSystemFault};
use crate::stages::source::{
    AsyncFiniteSourceConnector, AsyncInfiniteSourceConnector, FiniteSourceConnector,
    InfiniteSourceConnector, SourceReaderInitContext,
};
use async_trait::async_trait;
use obzenflow_core::event::SourceDiagnosticReason;
use obzenflow_core::source::{RefusedBy, SourceInputIdentity, SourcePosition};
use obzenflow_core::OneFactOutcome;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

type Finite = dyn UnifiedFiniteSourceHandler;
type AsyncFinite = dyn UnifiedAsyncFiniteSourceHandler;
type Infinite = dyn UnifiedInfiniteSourceHandler;
type AsyncInfinite = dyn UnifiedAsyncInfiniteSourceHandler;

type PortReader<Family, K, Out> = <K as PortContinuation<Family, Out>>::Reader;

type ReaderPoll<'a, T> = Pin<Box<dyn Future<Output = Result<T, SourceError>> + Send + 'a>>;

mod sealed {
    pub trait RowInput {}
    pub trait Owned<P: ?Sized> {}
}

#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this source family does not allow the continuation `{Self}`",
    note = "the type of a source port carries the continuation of its connectors: `Refuse`, `Seek<P>` for a synchronous finite source, or `Reattach<G>` for an infinite source"
)]
pub trait PortContinuation<Family: ?Sized, Out>: Send + Sync + 'static {
    type Reader: Send + 'static;
}

#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this source stage takes only a `Port`",
    label = "`{Self}` is not a `Port`",
    note = "bind a connector with `Port::to(...)`",
    note = "bind a direct handler with `Port::owned(...)`",
    note = "for hosted ingress, use the port from ingress registration"
)]
pub trait SourceRowInput: sealed::RowInput {
    type Port;

    fn into_port(self) -> Self::Port;
}

impl<P: ?Sized + Send + Sync + 'static> sealed::RowInput for Port<P> {}

impl<P: ?Sized + Send + Sync + 'static> SourceRowInput for Port<P> {
    type Port = Port<P>;

    fn into_port(self) -> Port<P> {
        self
    }
}

#[doc(hidden)]
pub trait BoxesReader<Family: ?Sized, R, Out>: PortContinuation<Family, Out> {
    fn boxed(reader: R) -> Self::Reader;
}

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

#[doc(hidden)]
pub struct PortAsyncFiniteReader<Out> {
    reader: Box<dyn TypedAsyncFiniteSourceHandler<Output = Out>>,
}

impl<Out> TypedAsyncFiniteSourceHandler for PortAsyncFiniteReader<Out>
where
    Out: OneFactOutcome + Send + Sync + 'static,
{
    type Output = Out;

    fn poll_timeout(&self) -> Option<Duration> {
        self.reader.poll_timeout()
    }

    fn next<'life0, 'async_trait>(&'life0 mut self) -> ReaderPoll<'async_trait, Option<Vec<Out>>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        self.reader.next()
    }

    fn drain<'life0, 'async_trait>(&'life0 mut self) -> ReaderPoll<'async_trait, ()>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        self.reader.drain()
    }

    fn install_source_observation_sink(&mut self, sink: SourceObservationSink) {
        self.reader.install_source_observation_sink(sink);
    }
}

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

#[doc(hidden)]
pub struct PortAsyncInfiniteReader<Out> {
    reader: Box<dyn TypedAsyncInfiniteSourceHandler<Output = Out>>,
}

impl<Out> TypedAsyncInfiniteSourceHandler for PortAsyncInfiniteReader<Out>
where
    Out: OneFactOutcome + Send + Sync + 'static,
{
    type Output = Out;

    fn poll_timeout(&self) -> Option<Duration> {
        self.reader.poll_timeout()
    }

    fn next<'life0, 'async_trait>(&'life0 mut self) -> ReaderPoll<'async_trait, Vec<Out>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        self.reader.next()
    }

    fn drain<'life0, 'async_trait>(&'life0 mut self) -> ReaderPoll<'async_trait, ()>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        self.reader.drain()
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

/// `FiniteSource` is the port trait for `source!` stages. Each synchronous finite
/// connector with the output `Out` and the continuation `K` implements it.
///
/// Connectors with the same output and the same continuation share one port type.
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
/// Connectors with different continuations have different port types, so the
/// example below does not compile.
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

/// `AsyncFiniteSource` is the port trait for `async_source!` stages. Each asynchronous
/// finite connector with the output `Out` and the continuation `K` implements it.
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

/// `InfiniteSource` is the port trait for `infinite_source!` stages. Each synchronous
/// infinite connector with the output `Out` and the continuation `K` implements it.
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

/// `AsyncInfiniteSource` is the port trait for `async_infinite_source!` stages. Each
/// asynchronous infinite connector with the output `Out` and the continuation `K`
/// implements it.
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

const HANDLER_REFUSES: SourceContinuation = SourceContinuation::Refuse {
    by: RefusedBy::Handler,
};

#[doc(hidden)]
pub struct OwnedHandler<H> {
    handler: Mutex<Option<H>>,
}

impl<H> OwnedHandler<H> {
    fn new(handler: H) -> Self {
        Self {
            handler: Mutex::new(Some(handler)),
        }
    }

    fn take(&self) -> Result<H, SourceError> {
        self.handler
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .ok_or(SourceError::Other(
                SourceDiagnosticReason::InputUnavailable.into(),
            ))
    }
}

impl<H: TypedFiniteSourceHandler + 'static> FiniteSourceConnector for OwnedHandler<H> {
    type Output = H::Output;
    type Reader = H;
    type Continuation = Refuse;

    fn open(&self, _context: SourceReaderInitContext) -> Result<H, SourceError> {
        self.take()
    }
}

#[async_trait]
impl<H: TypedAsyncFiniteSourceHandler + 'static> AsyncFiniteSourceConnector for OwnedHandler<H> {
    type Output = H::Output;
    type Reader = H;
    type Continuation = Refuse;

    async fn open(&self, _context: SourceReaderInitContext) -> Result<H, SourceError> {
        self.take()
    }
}

impl<H: TypedInfiniteSourceHandler + 'static> InfiniteSourceConnector for OwnedHandler<H> {
    type Output = H::Output;
    type Reader = H;
    type Continuation = Refuse;

    fn open(&self, _context: SourceReaderInitContext) -> Result<H, SourceError> {
        self.take()
    }
}

#[async_trait]
impl<H: TypedAsyncInfiniteSourceHandler + 'static> AsyncInfiniteSourceConnector
    for OwnedHandler<H>
{
    type Output = H::Output;
    type Reader = H;
    type Continuation = Refuse;

    async fn open(&self, _context: SourceReaderInitContext) -> Result<H, SourceError> {
        self.take()
    }
}

#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this port cannot own `{Self}` as a typed source handler",
    label = "`Port::owned` takes a handler of the port's source family by value",
    note = "`Port::owned` binds a `TypedFiniteSourceHandler` to `FiniteSource<_, Refuse>`, a `TypedAsyncFiniteSourceHandler` to `AsyncFiniteSource<_, Refuse>`, a `TypedInfiniteSourceHandler` to `InfiniteSource<_, Refuse>` and a `TypedAsyncInfiniteSourceHandler` to `AsyncInfiniteSource<_, Refuse>`",
    note = "bind a connector with `Port::to(...)`",
    note = "pass the handler itself, not an `Arc` of the handler"
)]
pub trait OwnedSource<P: ?Sized>: sealed::Owned<P> + Send + 'static {
    fn into_system(self) -> Arc<P>;
}

impl<H: TypedFiniteSourceHandler + 'static> sealed::Owned<dyn FiniteSource<H::Output, Refuse>>
    for H
{
}

impl<H: TypedAsyncFiniteSourceHandler + 'static>
    sealed::Owned<dyn AsyncFiniteSource<H::Output, Refuse>> for H
{
}

impl<H: TypedInfiniteSourceHandler + 'static> sealed::Owned<dyn InfiniteSource<H::Output, Refuse>>
    for H
{
}

impl<H: TypedAsyncInfiniteSourceHandler + 'static>
    sealed::Owned<dyn AsyncInfiniteSource<H::Output, Refuse>> for H
{
}

impl<H: TypedFiniteSourceHandler + 'static> OwnedSource<dyn FiniteSource<H::Output, Refuse>> for H {
    fn into_system(self) -> Arc<dyn FiniteSource<H::Output, Refuse>> {
        Arc::new(OwnedHandler::new(self))
    }
}

impl<H: TypedAsyncFiniteSourceHandler + 'static>
    OwnedSource<dyn AsyncFiniteSource<H::Output, Refuse>> for H
{
    fn into_system(self) -> Arc<dyn AsyncFiniteSource<H::Output, Refuse>> {
        Arc::new(OwnedHandler::new(self))
    }
}

impl<H: TypedInfiniteSourceHandler + 'static> OwnedSource<dyn InfiniteSource<H::Output, Refuse>>
    for H
{
    fn into_system(self) -> Arc<dyn InfiniteSource<H::Output, Refuse>> {
        Arc::new(OwnedHandler::new(self))
    }
}

impl<H: TypedAsyncInfiniteSourceHandler + 'static>
    OwnedSource<dyn AsyncInfiniteSource<H::Output, Refuse>> for H
{
    fn into_system(self) -> Arc<dyn AsyncInfiniteSource<H::Output, Refuse>> {
        Arc::new(OwnedHandler::new(self))
    }
}

fn port_continuation<P: ?Sized + Send + Sync + 'static>(
    port: &Port<P>,
    kind: SourceContinuation,
) -> SourceContinuation {
    match (kind, port.refused_by()) {
        (SourceContinuation::Refuse { .. }, RefusedBy::Handler) => HANDLER_REFUSES,
        (kind, _) => kind,
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

impl<Out, K> admission_sealed::Admitted<Finite> for Port<dyn FiniteSource<Out, K>>
where
    Out: OneFactOutcome + Send + Sync + 'static,
    K: PortContinuation<Finite, Out>,
{
}

impl<Out, K> AdmitSource<Finite> for Port<dyn FiniteSource<Out, K>>
where
    Out: OneFactOutcome + Send + Sync + 'static,
    K: PortContinuation<Finite, Out> + FiniteContinuation<PortReader<Finite, K, Out>, Output = Out>,
{
    type Output = Out;
    type Handler = <K as FiniteContinuation<PortReader<Finite, K, Out>>>::Handler;

    fn admit(self) -> AdmittedSource<Self::Handler> {
        let continuation = port_continuation(
            &self,
            <K as FiniteContinuation<PortReader<Finite, K, Out>>>::KIND,
        );
        let system = self.source_system();
        let resolving = system.clone();
        let handler =
            <K as FiniteContinuation<PortReader<Finite, K, Out>>>::cold(Box::new(move |context| {
                resolving
                    .resolve::<dyn FiniteSource<Out, K>>()
                    .map_err(unavailable)?
                    .open_port_reader(context)
            }));
        AdmittedSource {
            handler,
            continuation,
            system,
        }
    }
}

impl<Out, K> admission_sealed::Admitted<AsyncFinite> for Port<dyn AsyncFiniteSource<Out, K>>
where
    Out: OneFactOutcome + Send + Sync + 'static,
    K: PortContinuation<AsyncFinite, Out>,
{
}

impl<Out, K> AdmitSource<AsyncFinite> for Port<dyn AsyncFiniteSource<Out, K>>
where
    Out: OneFactOutcome + Send + Sync + 'static,
    K: PortContinuation<AsyncFinite, Out>
        + AsyncFiniteContinuation<PortReader<AsyncFinite, K, Out>, Output = Out>,
{
    type Output = Out;
    type Handler = <K as AsyncFiniteContinuation<PortReader<AsyncFinite, K, Out>>>::Handler;

    fn admit(self) -> AdmittedSource<Self::Handler> {
        let continuation = port_continuation(
            &self,
            <K as AsyncFiniteContinuation<PortReader<AsyncFinite, K, Out>>>::KIND,
        );
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
        AdmittedSource {
            handler,
            continuation,
            system,
        }
    }
}

impl<Out, K> admission_sealed::Admitted<Infinite> for Port<dyn InfiniteSource<Out, K>>
where
    Out: OneFactOutcome + Send + Sync + 'static,
    K: PortContinuation<Infinite, Out>,
{
}

impl<Out, K> AdmitSource<Infinite> for Port<dyn InfiniteSource<Out, K>>
where
    Out: OneFactOutcome + Send + Sync + 'static,
    K: PortContinuation<Infinite, Out>
        + InfiniteContinuation<PortReader<Infinite, K, Out>, Output = Out>,
{
    type Output = Out;
    type Handler = <K as InfiniteContinuation<PortReader<Infinite, K, Out>>>::Handler;

    fn admit(self) -> AdmittedSource<Self::Handler> {
        let continuation = port_continuation(
            &self,
            <K as InfiniteContinuation<PortReader<Infinite, K, Out>>>::KIND,
        );
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
        AdmittedSource {
            handler,
            continuation,
            system,
        }
    }
}

impl<Out, K> admission_sealed::Admitted<AsyncInfinite> for Port<dyn AsyncInfiniteSource<Out, K>>
where
    Out: OneFactOutcome + Send + Sync + 'static,
    K: PortContinuation<AsyncInfinite, Out>,
{
}

impl<Out, K> AdmitSource<AsyncInfinite> for Port<dyn AsyncInfiniteSource<Out, K>>
where
    Out: OneFactOutcome + Send + Sync + 'static,
    K: PortContinuation<AsyncInfinite, Out>
        + AsyncInfiniteContinuation<PortReader<AsyncInfinite, K, Out>, Output = Out>,
{
    type Output = Out;
    type Handler = <K as AsyncInfiniteContinuation<PortReader<AsyncInfinite, K, Out>>>::Handler;

    fn admit(self) -> AdmittedSource<Self::Handler> {
        let continuation = port_continuation(
            &self,
            <K as AsyncInfiniteContinuation<PortReader<AsyncInfinite, K, Out>>>::KIND,
        );
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
        AdmittedSource {
            handler,
            continuation,
            system,
        }
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

    #[crate::effects::system("ticket-reader")]
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
        let mut rows = [port.clone(), port]
            .map(<Port<dyn FiniteSource<Ticket, Refuse>> as AdmitSource<Finite>>::admit);
        let mut registry = EffectPortRegistry::new();
        for row in &rows {
            registry.install_source_system(&row.system);
        }
        let run = registry.into_run_registry();
        let start = Barrier::new(rows.len());
        std::thread::scope(|scope| {
            for row in &mut rows {
                row.system.attach(&run);
                let handler = &mut row.handler;
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

    #[test]
    fn owned_port_refusal_names_handler() {
        let owned: Port<dyn FiniteSource<Ticket, Refuse>> = Port::owned(TicketReader);
        let connector: Port<dyn FiniteSource<Ticket, Refuse>> = Port::to(ArchivedTickets);
        assert_eq!(owned.system_name(), "ticket-reader");
        assert_eq!(owned.admit().continuation, HANDLER_REFUSES);
        assert_eq!(
            connector.admit().continuation,
            SourceContinuation::Refuse {
                by: RefusedBy::Connector
            }
        );
    }

    #[test]
    fn owned_port_second_claimant_refused() {
        let port: Port<dyn FiniteSource<Ticket, Refuse>> = Port::owned(TicketReader);
        let mut rows = [port.clone(), port]
            .map(<Port<dyn FiniteSource<Ticket, Refuse>> as AdmitSource<Finite>>::admit);
        let mut registry = EffectPortRegistry::new();
        for row in &rows {
            registry.install_source_system(&row.system);
        }
        let run = registry.into_run_registry();
        let claims: Vec<Result<(), SourceError>> = rows
            .iter_mut()
            .map(|row| {
                row.system.attach(&run);
                row.handler.acquire(context())
            })
            .collect();
        assert_eq!(
            claims,
            [
                Ok(()),
                Err(SourceError::Other(
                    SourceDiagnosticReason::InputUnavailable.into()
                ))
            ]
        );
    }

    #[test]
    fn owned_port_unacquired_clone_acquires() {
        let port: Port<dyn FiniteSource<Ticket, Refuse>> = Port::owned(TicketReader);
        let _replayed = port.clone().admit();
        let mut live = port.admit();
        let mut registry = EffectPortRegistry::new();
        registry.install_source_system(&live.system);
        live.system.attach(&registry.into_run_registry());
        assert_eq!(live.handler.acquire(context()), Ok(()));
    }

    #[tokio::test]
    async fn source_port_reader_returns_connector_poll_timeout() {
        let port: Port<dyn AsyncFiniteSource<Ticket, Refuse>> = Port::to(SlowTickets);
        let AdmittedSource {
            mut handler,
            system,
            ..
        } = port.admit();
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
