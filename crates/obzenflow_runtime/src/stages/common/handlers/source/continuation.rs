// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Connector continuation contract (FLOWIP-134i B8, FLOWIP-122i).
//!
//! Each connector declares how its interrupted input continues on resume with
//! one associated type, `Continuation`. A sealed bound ties each declaration to
//! the reader it requires, so an unsupported pairing fails to compile, and
//! admission captures the declaration before any open. Direct handlers carry
//! no declaration and lower to `Refuse`.
//!
//! ```
//! use obzenflow_runtime::stages::source::{
//!     FiniteSourceConnector, Refuse, SourceError, SourceReaderInitContext,
//!     TypedFiniteSourceHandler,
//! };
//! # #[derive(serde::Serialize, serde::Deserialize)]
//! # struct Row;
//! # impl obzenflow_core::TypedPayload for Row {
//! #     const EVENT_TYPE: &'static str = "doc.row";
//! # }
//! # struct RowReader;
//! # impl TypedFiniteSourceHandler for RowReader {
//! #     type Output = Row;
//! #     fn next(&mut self) -> Result<Option<Vec<Row>>, SourceError> { Ok(None) }
//! # }
//! struct Rows;
//! impl FiniteSourceConnector for Rows {
//!     type Output = Row;
//!     type Reader = RowReader;
//!     type Continuation = Refuse;
//!     fn open(&self, _: SourceReaderInitContext) -> Result<RowReader, SourceError> {
//!         Ok(RowReader)
//!     }
//! }
//! ```
//!
//! A connector must state its continuation:
//!
//! ```compile_fail,E0046
//! # use obzenflow_runtime::stages::source::{
//! #     FiniteSourceConnector, SourceError, SourceReaderInitContext, TypedFiniteSourceHandler,
//! # };
//! # #[derive(serde::Serialize, serde::Deserialize)]
//! # struct Row;
//! # impl obzenflow_core::TypedPayload for Row {
//! #     const EVENT_TYPE: &'static str = "doc.row";
//! # }
//! # struct RowReader;
//! # impl TypedFiniteSourceHandler for RowReader {
//! #     type Output = Row;
//! #     fn next(&mut self) -> Result<Option<Vec<Row>>, SourceError> { Ok(None) }
//! # }
//! struct Rows;
//! impl FiniteSourceConnector for Rows {
//!     type Output = Row;
//!     type Reader = RowReader;
//!     fn open(&self, _: SourceReaderInitContext) -> Result<RowReader, SourceError> {
//!         Ok(RowReader)
//!     }
//! }
//! ```
//!
//! An infinite connector that reattaches live names what an interruption can
//! cost its input, `LatestState` or `AtMostOnce` (FLOWIP-122i):
//!
//! ```
//! # use obzenflow_runtime::stages::source::{
//! #     InfiniteSourceConnector, LatestState, Reattach, SourceError, SourceReaderInitContext,
//! #     TypedInfiniteSourceHandler,
//! # };
//! # #[derive(serde::Serialize, serde::Deserialize)]
//! # struct Row;
//! # impl obzenflow_core::TypedPayload for Row {
//! #     const EVENT_TYPE: &'static str = "doc.row";
//! # }
//! # struct Poller;
//! # impl TypedInfiniteSourceHandler for Poller {
//! #     type Output = Row;
//! #     fn next(&mut self) -> Result<Vec<Row>, SourceError> { Ok(Vec::new()) }
//! # }
//! struct Polls;
//! impl InfiniteSourceConnector for Polls {
//!     type Output = Row;
//!     type Reader = Poller;
//!     type Continuation = Reattach<LatestState>;
//!     fn open(&self, _: SourceReaderInitContext) -> Result<Poller, SourceError> {
//!         Ok(Poller)
//!     }
//! }
//! ```
//!
//! A reattachment without a guarantee does not compile:
//!
//! ```compile_fail,E0107
//! # use obzenflow_runtime::stages::source::{
//! #     InfiniteSourceConnector, Reattach, SourceError, SourceReaderInitContext,
//! #     TypedInfiniteSourceHandler,
//! # };
//! # #[derive(serde::Serialize, serde::Deserialize)]
//! # struct Row;
//! # impl obzenflow_core::TypedPayload for Row {
//! #     const EVENT_TYPE: &'static str = "doc.row";
//! # }
//! # struct Poller;
//! # impl TypedInfiniteSourceHandler for Poller {
//! #     type Output = Row;
//! #     fn next(&mut self) -> Result<Vec<Row>, SourceError> { Ok(Vec::new()) }
//! # }
//! struct Polls;
//! impl InfiniteSourceConnector for Polls {
//!     type Output = Row;
//!     type Reader = Poller;
//!     type Continuation = Reattach;
//!     fn open(&self, _: SourceReaderInitContext) -> Result<Poller, SourceError> {
//!         Ok(Poller)
//!     }
//! }
//! ```
//!
//! Guarantees are sealed: only hosted ingress, which acknowledges after its
//! records commit, records that its sender redelivers (FLOWIP-122i B3).
//!
//! ```compile_fail,E0277
//! # use obzenflow_core::event::payloads::execution_payload::ReattachGuarantee;
//! # use obzenflow_runtime::stages::source::LiveGuarantee;
//! struct SenderRedelivers;
//! impl LiveGuarantee for SenderRedelivers {
//!     const GUARANTEE: ReattachGuarantee = ReattachGuarantee::SenderRedelivers;
//! }
//! ```
//!
//! Only infinite connectors can reattach live:
//!
//! ```compile_fail,E0277
//! # use obzenflow_runtime::stages::source::{
//! #     AtMostOnce, FiniteSourceConnector, Reattach, SourceError, SourceReaderInitContext,
//! #     TypedFiniteSourceHandler,
//! # };
//! # #[derive(serde::Serialize, serde::Deserialize)]
//! # struct Row;
//! # impl obzenflow_core::TypedPayload for Row {
//! #     const EVENT_TYPE: &'static str = "doc.row";
//! # }
//! # struct RowReader;
//! # impl TypedFiniteSourceHandler for RowReader {
//! #     type Output = Row;
//! #     fn next(&mut self) -> Result<Option<Vec<Row>>, SourceError> { Ok(None) }
//! # }
//! struct Rows;
//! impl FiniteSourceConnector for Rows {
//!     type Output = Row;
//!     type Reader = RowReader;
//!     type Continuation = Reattach<AtMostOnce>;
//!     fn open(&self, _: SourceReaderInitContext) -> Result<RowReader, SourceError> {
//!         Ok(RowReader)
//!     }
//! }
//! ```
//!
//! `Seek<P>` pairs a synchronous finite connector with a reader positioned
//! for `P`, which returns each record with the position after it:
//!
//! ```
//! # use obzenflow_runtime::stages::source::{
//! #     FiniteSourceConnector, PositionedFiniteReader, Seek, SeekableSource, SourceError,
//! #     SourceInputIdentity, SourcePosition, SourceReaderInitContext, SourceRecord,
//! # };
//! # #[derive(serde::Serialize, serde::Deserialize)]
//! # struct Row;
//! # impl obzenflow_core::TypedPayload for Row {
//! #     const EVENT_TYPE: &'static str = "doc.row";
//! # }
//! #[derive(Clone, serde::Serialize, serde::Deserialize)]
//! struct Offset(u64);
//! impl SourcePosition for Offset {}
//!
//! struct OffsetReader {
//!     identity: SourceInputIdentity,
//! }
//! impl SeekableSource for OffsetReader {
//!     type Position = Offset;
//!     fn input_identity(&self) -> &SourceInputIdentity { &self.identity }
//!     fn beginning(&self) -> Offset { Offset(0) }
//!     fn seek(&mut self, _: Offset) -> Result<(), SourceError> { Ok(()) }
//! }
//! impl PositionedFiniteReader for OffsetReader {
//!     type Output = Row;
//!     fn next(&mut self) -> Result<Option<Vec<SourceRecord<Row, Offset>>>, SourceError> {
//!         Ok(None)
//!     }
//! }
//! # fn reader() -> OffsetReader { unimplemented!() }
//!
//! struct Rows;
//! impl FiniteSourceConnector for Rows {
//!     type Output = Row;
//!     type Reader = OffsetReader;
//!     type Continuation = Seek<Offset>;
//!     fn open(&self, _: SourceReaderInitContext) -> Result<OffsetReader, SourceError> {
//!         Ok(reader())
//!     }
//! }
//! ```
//!
//! A reader that is not positioned for `P` cannot seek:
//!
//! ```compile_fail,E0277
//! # use obzenflow_runtime::stages::source::{
//! #     FiniteSourceConnector, Seek, SourceError, SourcePosition, SourceReaderInitContext,
//! #     TypedFiniteSourceHandler,
//! # };
//! # #[derive(serde::Serialize, serde::Deserialize)]
//! # struct Row;
//! # impl obzenflow_core::TypedPayload for Row {
//! #     const EVENT_TYPE: &'static str = "doc.row";
//! # }
//! # #[derive(Clone, serde::Serialize, serde::Deserialize)]
//! # struct Offset(u64);
//! # impl SourcePosition for Offset {}
//! # struct RowReader;
//! # impl TypedFiniteSourceHandler for RowReader {
//! #     type Output = Row;
//! #     fn next(&mut self) -> Result<Option<Vec<Row>>, SourceError> { Ok(None) }
//! # }
//! struct Rows;
//! impl FiniteSourceConnector for Rows {
//!     type Output = Row;
//!     type Reader = RowReader;
//!     type Continuation = Seek<Offset>;
//!     fn open(&self, _: SourceReaderInitContext) -> Result<RowReader, SourceError> {
//!         Ok(RowReader)
//!     }
//! }
//! ```
//!
//! Every positioned record carries its position:
//!
//! ```compile_fail,E0053
//! # use obzenflow_runtime::stages::source::{
//! #     PositionedFiniteReader, SeekableSource, SourceError, SourceInputIdentity,
//! #     SourcePosition,
//! # };
//! # #[derive(serde::Serialize, serde::Deserialize)]
//! # struct Row;
//! # impl obzenflow_core::TypedPayload for Row {
//! #     const EVENT_TYPE: &'static str = "doc.row";
//! # }
//! # #[derive(Clone, serde::Serialize, serde::Deserialize)]
//! # struct Offset(u64);
//! # impl SourcePosition for Offset {}
//! # struct OffsetReader {
//! #     identity: SourceInputIdentity,
//! # }
//! # impl SeekableSource for OffsetReader {
//! #     type Position = Offset;
//! #     fn input_identity(&self) -> &SourceInputIdentity { &self.identity }
//! #     fn beginning(&self) -> Offset { Offset(0) }
//! #     fn seek(&mut self, _: Offset) -> Result<(), SourceError> { Ok(()) }
//! # }
//! impl PositionedFiniteReader for OffsetReader {
//!     type Output = Row;
//!     fn next(&mut self) -> Result<Option<Vec<Row>>, SourceError> {
//!         Ok(None)
//!     }
//! }
//! ```
//!
//! In this release `Seek<P>` belongs to synchronous finite connectors only
//! (B14). An asynchronous finite connector cannot declare it:
//!
//! ```compile_fail,E0277
//! # use obzenflow_runtime::stages::source::{
//! #     AsyncFiniteSourceConnector, PositionedFiniteReader, Seek, SeekableSource, SourceError,
//! #     SourceInputIdentity, SourcePosition, SourceReaderInitContext, SourceRecord,
//! # };
//! # #[derive(serde::Serialize, serde::Deserialize)]
//! # struct Row;
//! # impl obzenflow_core::TypedPayload for Row {
//! #     const EVENT_TYPE: &'static str = "doc.row";
//! # }
//! # #[derive(Clone, serde::Serialize, serde::Deserialize)]
//! # struct Offset(u64);
//! # impl SourcePosition for Offset {}
//! # struct OffsetReader {
//! #     identity: SourceInputIdentity,
//! # }
//! # impl SeekableSource for OffsetReader {
//! #     type Position = Offset;
//! #     fn input_identity(&self) -> &SourceInputIdentity { &self.identity }
//! #     fn beginning(&self) -> Offset { Offset(0) }
//! #     fn seek(&mut self, _: Offset) -> Result<(), SourceError> { Ok(()) }
//! # }
//! # impl PositionedFiniteReader for OffsetReader {
//! #     type Output = Row;
//! #     fn next(&mut self) -> Result<Option<Vec<SourceRecord<Row, Offset>>>, SourceError> {
//! #         Ok(None)
//! #     }
//! # }
//! # fn reader() -> OffsetReader { unimplemented!() }
//! struct Rows;
//! #[async_trait::async_trait]
//! impl AsyncFiniteSourceConnector for Rows {
//!     type Output = Row;
//!     type Reader = OffsetReader;
//!     type Continuation = Seek<Offset>;
//!     async fn open(&self, _: SourceReaderInitContext) -> Result<OffsetReader, SourceError> {
//!         Ok(reader())
//!     }
//! }
//! ```
//!
//! Nor can an infinite connector:
//!
//! ```compile_fail,E0277
//! # use obzenflow_runtime::stages::source::{
//! #     InfiniteSourceConnector, PositionedFiniteReader, Seek, SeekableSource, SourceError,
//! #     SourceInputIdentity, SourcePosition, SourceReaderInitContext, SourceRecord,
//! # };
//! # #[derive(serde::Serialize, serde::Deserialize)]
//! # struct Row;
//! # impl obzenflow_core::TypedPayload for Row {
//! #     const EVENT_TYPE: &'static str = "doc.row";
//! # }
//! # #[derive(Clone, serde::Serialize, serde::Deserialize)]
//! # struct Offset(u64);
//! # impl SourcePosition for Offset {}
//! # struct OffsetReader {
//! #     identity: SourceInputIdentity,
//! # }
//! # impl SeekableSource for OffsetReader {
//! #     type Position = Offset;
//! #     fn input_identity(&self) -> &SourceInputIdentity { &self.identity }
//! #     fn beginning(&self) -> Offset { Offset(0) }
//! #     fn seek(&mut self, _: Offset) -> Result<(), SourceError> { Ok(()) }
//! # }
//! # impl PositionedFiniteReader for OffsetReader {
//! #     type Output = Row;
//! #     fn next(&mut self) -> Result<Option<Vec<SourceRecord<Row, Offset>>>, SourceError> {
//! #         Ok(None)
//! #     }
//! # }
//! # fn reader() -> OffsetReader { unimplemented!() }
//! struct Rows;
//! impl InfiniteSourceConnector for Rows {
//!     type Output = Row;
//!     type Reader = OffsetReader;
//!     type Continuation = Seek<Offset>;
//!     fn open(&self, _: SourceReaderInitContext) -> Result<OffsetReader, SourceError> {
//!         Ok(reader())
//!     }
//! }
//! ```
//!
//! Nor an asynchronous infinite one:
//!
//! ```compile_fail,E0277
//! # use obzenflow_runtime::stages::source::{
//! #     AsyncInfiniteSourceConnector, PositionedFiniteReader, Seek, SeekableSource, SourceError,
//! #     SourceInputIdentity, SourcePosition, SourceReaderInitContext, SourceRecord,
//! # };
//! # #[derive(serde::Serialize, serde::Deserialize)]
//! # struct Row;
//! # impl obzenflow_core::TypedPayload for Row {
//! #     const EVENT_TYPE: &'static str = "doc.row";
//! # }
//! # #[derive(Clone, serde::Serialize, serde::Deserialize)]
//! # struct Offset(u64);
//! # impl SourcePosition for Offset {}
//! # struct OffsetReader {
//! #     identity: SourceInputIdentity,
//! # }
//! # impl SeekableSource for OffsetReader {
//! #     type Position = Offset;
//! #     fn input_identity(&self) -> &SourceInputIdentity { &self.identity }
//! #     fn beginning(&self) -> Offset { Offset(0) }
//! #     fn seek(&mut self, _: Offset) -> Result<(), SourceError> { Ok(()) }
//! # }
//! # impl PositionedFiniteReader for OffsetReader {
//! #     type Output = Row;
//! #     fn next(&mut self) -> Result<Option<Vec<SourceRecord<Row, Offset>>>, SourceError> {
//! #         Ok(None)
//! #     }
//! # }
//! # fn reader() -> OffsetReader { unimplemented!() }
//! struct Rows;
//! #[async_trait::async_trait]
//! impl AsyncInfiniteSourceConnector for Rows {
//!     type Output = Row;
//!     type Reader = OffsetReader;
//!     type Continuation = Seek<Offset>;
//!     async fn open(&self, _: SourceReaderInitContext) -> Result<OffsetReader, SourceError> {
//!         Ok(reader())
//!     }
//! }
//! ```

use super::erased::{
    UnifiedAsyncFiniteSourceHandler, UnifiedAsyncInfiniteSourceHandler, UnifiedFiniteSourceHandler,
    UnifiedInfiniteSourceHandler,
};
use super::positioned::PositionedFiniteReader;
use super::typed::{
    PositionedFiniteSourceHandlerAdapter, TypedAsyncFiniteSourceHandler,
    TypedAsyncFiniteSourceHandlerAdapter, TypedAsyncInfiniteSourceHandler,
    TypedAsyncInfiniteSourceHandlerAdapter, TypedFiniteSourceHandler,
    TypedFiniteSourceHandlerAdapter, TypedInfiniteSourceHandler, TypedInfiniteSourceHandlerAdapter,
};
use super::SourceError;
use crate::stages::source::SourceReaderInitContext;
use futures::future::BoxFuture;
use obzenflow_core::event::payloads::execution_payload::ReattachGuarantee;
use obzenflow_core::source::{position_version, SourcePosition};
use obzenflow_core::OneFactStageOutput;
use std::marker::PhantomData;

/// Unfinished input cannot be continued; resume refuses.
#[derive(Debug, Clone, Copy, Default)]
pub struct Refuse;

/// The reader is positioned for `P`; resume seeks strictly after the committed
/// position (FLOWIP-134i B1).
pub struct Seek<P>(PhantomData<fn() -> P>);

/// Infinite connectors only; resume reattaches live at the committed handoff
/// and records what the interruption could cost, `G` (FLOWIP-122i).
pub struct Reattach<G: LiveGuarantee>(PhantomData<fn() -> G>);

/// Each observation supersedes the last; re-seen rows are expected.
#[derive(Debug, Clone, Copy, Default)]
pub struct LatestState;

/// No input is admitted twice. Input not committed at an interruption, and
/// input that arrived while no reader was attached, may be lost.
#[derive(Debug, Clone, Copy, Default)]
pub struct AtMostOnce;

/// The guarantees a connector can declare for [`Reattach`]. Sealed: only
/// hosted ingress, which acknowledges after commit, records that its sender
/// redelivers (FLOWIP-122i B3).
pub trait LiveGuarantee: sealed::Sealed + Send + Sync + 'static {
    #[doc(hidden)]
    const GUARANTEE: ReattachGuarantee;
}

impl LiveGuarantee for LatestState {
    const GUARANTEE: ReattachGuarantee = ReattachGuarantee::LatestState;
}

impl LiveGuarantee for AtMostOnce {
    const GUARANTEE: ReattachGuarantee = ReattachGuarantee::AtMostOnce;
}

// Core owns the declarations archive admission reads (FLOWIP-134k B4).
pub use obzenflow_core::source::{RefusedBy, ResumeRefusal, SourceContinuation};

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Refuse {}
    impl<P: obzenflow_core::source::SourcePosition> Sealed for super::Seek<P> {}
    impl<G: super::LiveGuarantee> Sealed for super::Reattach<G> {}
    impl Sealed for super::LatestState {}
    impl Sealed for super::AtMostOnce {}
}

/// Opens a cold synchronous reader at supervised acquisition.
#[doc(hidden)]
pub type SyncReaderOpener<R> =
    Box<dyn FnOnce(SourceReaderInitContext) -> Result<R, SourceError> + Send + Sync>;

/// Opens a cold asynchronous reader at supervised acquisition.
#[doc(hidden)]
pub type AsyncReaderOpener<R> = Box<
    dyn FnOnce(SourceReaderInitContext) -> BoxFuture<'static, Result<R, SourceError>> + Send + Sync,
>;

/// The continuations a synchronous finite connector may declare.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a continuation for synchronous finite connectors whose reader is `{R}`",
    note = "synchronous finite connectors declare `Refuse` with a reader implementing \
            `TypedFiniteSourceHandler`, or `Seek<P>` with a reader implementing \
            `PositionedFiniteReader<Position = P>`; only infinite connectors can reattach live"
)]
pub trait FiniteContinuation<R>: sealed::Sealed + Send + Sync + 'static {
    type Output: OneFactStageOutput + Send + Sync + 'static;
    #[doc(hidden)]
    type Handler: UnifiedFiniteSourceHandler + Send + Sync + 'static;
    #[doc(hidden)]
    const KIND: SourceContinuation;
    #[doc(hidden)]
    fn cold(open: SyncReaderOpener<R>) -> Self::Handler;
}

/// The continuations an asynchronous finite connector may declare.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a continuation for asynchronous finite connectors whose reader is `{R}`",
    note = "in this release `Seek<P>` is available to synchronous finite connectors; \
            asynchronous finite connectors declare `Refuse` with a reader implementing \
            `TypedAsyncFiniteSourceHandler`"
)]
pub trait AsyncFiniteContinuation<R>: sealed::Sealed + Send + Sync + 'static {
    type Output: OneFactStageOutput + Send + Sync + 'static;
    #[doc(hidden)]
    type Handler: UnifiedAsyncFiniteSourceHandler + Send + Sync + 'static;
    #[doc(hidden)]
    const KIND: SourceContinuation;
    #[doc(hidden)]
    fn cold(open: AsyncReaderOpener<R>) -> Self::Handler;
}

/// The continuations a synchronous infinite connector may declare.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a continuation for synchronous infinite connectors whose reader is `{R}`",
    note = "in this release `Seek<P>` is available to synchronous finite connectors; \
            synchronous infinite connectors declare `Refuse`, or `Reattach<LatestState>` or \
            `Reattach<AtMostOnce>`, with a reader implementing `TypedInfiniteSourceHandler`"
)]
pub trait InfiniteContinuation<R>: sealed::Sealed + Send + Sync + 'static {
    type Output: OneFactStageOutput + Send + Sync + 'static;
    #[doc(hidden)]
    type Handler: UnifiedInfiniteSourceHandler + Send + Sync + 'static;
    #[doc(hidden)]
    const KIND: SourceContinuation;
    #[doc(hidden)]
    fn cold(open: SyncReaderOpener<R>) -> Self::Handler;
}

/// The continuations an asynchronous infinite connector may declare.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a continuation for asynchronous infinite connectors whose reader is `{R}`",
    note = "in this release `Seek<P>` is available to synchronous finite connectors; \
            asynchronous infinite connectors declare `Refuse`, or `Reattach<LatestState>` or \
            `Reattach<AtMostOnce>`, with a reader implementing `TypedAsyncInfiniteSourceHandler`"
)]
pub trait AsyncInfiniteContinuation<R>: sealed::Sealed + Send + Sync + 'static {
    type Output: OneFactStageOutput + Send + Sync + 'static;
    #[doc(hidden)]
    type Handler: UnifiedAsyncInfiniteSourceHandler + Send + Sync + 'static;
    #[doc(hidden)]
    const KIND: SourceContinuation;
    #[doc(hidden)]
    fn cold(open: AsyncReaderOpener<R>) -> Self::Handler;
}

/// A connector's declared refusal.
const CONNECTOR_REFUSES: SourceContinuation = SourceContinuation::Refuse {
    by: RefusedBy::Connector,
};

impl<R: TypedFiniteSourceHandler + 'static> FiniteContinuation<R> for Refuse {
    type Output = R::Output;
    type Handler = TypedFiniteSourceHandlerAdapter<R>;
    const KIND: SourceContinuation = CONNECTOR_REFUSES;

    fn cold(open: SyncReaderOpener<R>) -> Self::Handler {
        TypedFiniteSourceHandlerAdapter::cold(open)
    }
}

impl<P, R> FiniteContinuation<R> for Seek<P>
where
    P: SourcePosition,
    R: PositionedFiniteReader<Position = P> + 'static,
{
    type Output = R::Output;
    type Handler = PositionedFiniteSourceHandlerAdapter<R>;
    const KIND: SourceContinuation = SourceContinuation::Seek {
        position_version: position_version::<P>(),
    };

    fn cold(open: SyncReaderOpener<R>) -> Self::Handler {
        PositionedFiniteSourceHandlerAdapter::cold(open)
    }
}

impl<R: TypedAsyncFiniteSourceHandler + 'static> AsyncFiniteContinuation<R> for Refuse {
    type Output = R::Output;
    type Handler = TypedAsyncFiniteSourceHandlerAdapter<R>;
    const KIND: SourceContinuation = CONNECTOR_REFUSES;

    fn cold(open: AsyncReaderOpener<R>) -> Self::Handler {
        TypedAsyncFiniteSourceHandlerAdapter::cold(open)
    }
}

impl<R: TypedInfiniteSourceHandler + 'static> InfiniteContinuation<R> for Refuse {
    type Output = R::Output;
    type Handler = TypedInfiniteSourceHandlerAdapter<R>;
    const KIND: SourceContinuation = CONNECTOR_REFUSES;

    fn cold(open: SyncReaderOpener<R>) -> Self::Handler {
        TypedInfiniteSourceHandlerAdapter::cold(open)
    }
}

impl<G, R> InfiniteContinuation<R> for Reattach<G>
where
    G: LiveGuarantee,
    R: TypedInfiniteSourceHandler + 'static,
{
    type Output = R::Output;
    type Handler = TypedInfiniteSourceHandlerAdapter<R>;
    const KIND: SourceContinuation = SourceContinuation::Reattach {
        guarantee: G::GUARANTEE,
    };

    fn cold(open: SyncReaderOpener<R>) -> Self::Handler {
        TypedInfiniteSourceHandlerAdapter::cold(open)
    }
}

impl<R: TypedAsyncInfiniteSourceHandler + 'static> AsyncInfiniteContinuation<R> for Refuse {
    type Output = R::Output;
    type Handler = TypedAsyncInfiniteSourceHandlerAdapter<R>;
    const KIND: SourceContinuation = CONNECTOR_REFUSES;

    fn cold(open: AsyncReaderOpener<R>) -> Self::Handler {
        TypedAsyncInfiniteSourceHandlerAdapter::cold(open)
    }
}

impl<G, R> AsyncInfiniteContinuation<R> for Reattach<G>
where
    G: LiveGuarantee,
    R: TypedAsyncInfiniteSourceHandler + 'static,
{
    type Output = R::Output;
    type Handler = TypedAsyncInfiniteSourceHandlerAdapter<R>;
    const KIND: SourceContinuation = SourceContinuation::Reattach {
        guarantee: G::GUARANTEE,
    };

    fn cold(open: AsyncReaderOpener<R>) -> Self::Handler {
        TypedAsyncInfiniteSourceHandlerAdapter::cold(open)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FLOWIP-122i: each declared guarantee reaches admission unchanged.
    #[test]
    fn a_declared_guarantee_is_captured_with_the_connector() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Row;
        impl obzenflow_core::TypedPayload for Row {
            const EVENT_TYPE: &'static str = "continuation.row";
        }
        struct Reader;
        impl TypedInfiniteSourceHandler for Reader {
            type Output = Row;
            fn next(&mut self) -> Result<Vec<Row>, SourceError> {
                Ok(Vec::new())
            }
        }
        assert_eq!(
            <Reattach<LatestState> as InfiniteContinuation<Reader>>::KIND,
            SourceContinuation::Reattach {
                guarantee: ReattachGuarantee::LatestState
            }
        );
        assert_eq!(
            <Reattach<AtMostOnce> as InfiniteContinuation<Reader>>::KIND,
            SourceContinuation::Reattach {
                guarantee: ReattachGuarantee::AtMostOnce
            }
        );
        assert_eq!(
            <Refuse as InfiniteContinuation<Reader>>::KIND,
            SourceContinuation::Refuse {
                by: RefusedBy::Connector
            }
        );
    }
}
