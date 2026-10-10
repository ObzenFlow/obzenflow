// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Connector continuation contract (FLOWIP-134i B8).
//!
//! Each connector declares how its interrupted input continues on resume with
//! one associated type, `Continuation`. A sealed bound ties each declaration to
//! the reader it requires, so an unsupported pairing fails to compile, and
//! admission captures the declaration before any open. Direct handlers carry
//! no declaration: a finite handler lowers to `Refuse`, an infinite one to
//! `Reattach`.
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
//! Only infinite connectors can reattach live:
//!
//! ```compile_fail,E0277
//! # use obzenflow_runtime::stages::source::{
//! #     FiniteSourceConnector, Reattach, SourceError, SourceReaderInitContext,
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
//!     type Continuation = Reattach;
//!     fn open(&self, _: SourceReaderInitContext) -> Result<RowReader, SourceError> {
//!         Ok(RowReader)
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
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::journal::archive::ReplayArchive;
use obzenflow_core::source::SourcePosition;
use obzenflow_core::OneFactStageOutput;
use std::marker::PhantomData;

/// Unfinished input cannot be continued; resume refuses.
#[derive(Debug, Clone, Copy, Default)]
pub struct Refuse;

/// The reader is positioned for `P`; resume seeks strictly after the committed
/// position (FLOWIP-134i B1).
pub struct Seek<P>(PhantomData<fn() -> P>);

/// Infinite connectors only; resume reattaches live at the committed handoff.
#[derive(Debug, Clone, Copy, Default)]
pub struct Reattach;

/// The continuation a source stage was admitted with.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceContinuation {
    Refuse,
    Seek,
    Reattach,
}

impl SourceContinuation {
    /// Whether a resume may admit this source given the EOF its archived
    /// journal committed: a `Refuse` source only after natural completion,
    /// which its replay then reproduces. `Poison` and `Truncated` are not
    /// natural completion.
    pub fn admits_resume(self, recorded_eof: Option<EofKind>) -> bool {
        match self {
            Self::Refuse => recorded_eof == Some(EofKind::Natural),
            Self::Seek | Self::Reattach => true,
        }
    }
}

/// The sources a resume of `archive` must refuse, sorted by stage key.
#[doc(hidden)]
pub fn sources_refusing_resume<'a>(
    archive: &dyn ReplayArchive,
    sources: impl IntoIterator<Item = (&'a str, SourceContinuation)>,
) -> Vec<String> {
    let mut refused: Vec<String> = sources
        .into_iter()
        .filter(|(stage_key, continuation)| {
            !continuation.admits_resume(archive.recorded_source_eof(stage_key))
        })
        .map(|(stage_key, _)| stage_key.to_string())
        .collect();
    refused.sort();
    refused
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Refuse {}
    impl<P: obzenflow_core::source::SourcePosition> Sealed for super::Seek<P> {}
    impl Sealed for super::Reattach {}
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
            synchronous infinite connectors declare `Refuse` or `Reattach` with a reader \
            implementing `TypedInfiniteSourceHandler`"
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
            asynchronous infinite connectors declare `Refuse` or `Reattach` with a reader \
            implementing `TypedAsyncInfiniteSourceHandler`"
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

impl<R: TypedFiniteSourceHandler + 'static> FiniteContinuation<R> for Refuse {
    type Output = R::Output;
    type Handler = TypedFiniteSourceHandlerAdapter<R>;
    const KIND: SourceContinuation = SourceContinuation::Refuse;

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
    const KIND: SourceContinuation = SourceContinuation::Seek;

    fn cold(open: SyncReaderOpener<R>) -> Self::Handler {
        PositionedFiniteSourceHandlerAdapter::cold(open)
    }
}

impl<R: TypedAsyncFiniteSourceHandler + 'static> AsyncFiniteContinuation<R> for Refuse {
    type Output = R::Output;
    type Handler = TypedAsyncFiniteSourceHandlerAdapter<R>;
    const KIND: SourceContinuation = SourceContinuation::Refuse;

    fn cold(open: AsyncReaderOpener<R>) -> Self::Handler {
        TypedAsyncFiniteSourceHandlerAdapter::cold(open)
    }
}

impl<R: TypedInfiniteSourceHandler + 'static> InfiniteContinuation<R> for Refuse {
    type Output = R::Output;
    type Handler = TypedInfiniteSourceHandlerAdapter<R>;
    const KIND: SourceContinuation = SourceContinuation::Refuse;

    fn cold(open: SyncReaderOpener<R>) -> Self::Handler {
        TypedInfiniteSourceHandlerAdapter::cold(open)
    }
}

impl<R: TypedInfiniteSourceHandler + 'static> InfiniteContinuation<R> for Reattach {
    type Output = R::Output;
    type Handler = TypedInfiniteSourceHandlerAdapter<R>;
    const KIND: SourceContinuation = SourceContinuation::Reattach;

    fn cold(open: SyncReaderOpener<R>) -> Self::Handler {
        TypedInfiniteSourceHandlerAdapter::cold(open)
    }
}

impl<R: TypedAsyncInfiniteSourceHandler + 'static> AsyncInfiniteContinuation<R> for Refuse {
    type Output = R::Output;
    type Handler = TypedAsyncInfiniteSourceHandlerAdapter<R>;
    const KIND: SourceContinuation = SourceContinuation::Refuse;

    fn cold(open: AsyncReaderOpener<R>) -> Self::Handler {
        TypedAsyncInfiniteSourceHandlerAdapter::cold(open)
    }
}

impl<R: TypedAsyncInfiniteSourceHandler + 'static> AsyncInfiniteContinuation<R> for Reattach {
    type Output = R::Output;
    type Handler = TypedAsyncInfiniteSourceHandlerAdapter<R>;
    const KIND: SourceContinuation = SourceContinuation::Reattach;

    fn cold(open: AsyncReaderOpener<R>) -> Self::Handler {
        TypedAsyncInfiniteSourceHandlerAdapter::cold(open)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuse_admits_resume_only_after_natural_completion() {
        let refuse = SourceContinuation::Refuse;
        assert!(refuse.admits_resume(Some(EofKind::Natural)));
        assert!(
            !refuse.admits_resume(Some(EofKind::Poison)),
            "a Jonestown poison is not natural completion"
        );
        assert!(
            !refuse.admits_resume(Some(EofKind::Truncated)),
            "an operator stop leaves the input unfinished"
        );
        assert!(
            !refuse.admits_resume(None),
            "a torn source committed no completion"
        );
    }

    #[test]
    fn reattach_admits_resume_whatever_its_recorded_completion() {
        for recorded_eof in [
            None,
            Some(EofKind::Natural),
            Some(EofKind::Poison),
            Some(EofKind::Truncated),
        ] {
            assert!(SourceContinuation::Reattach.admits_resume(recorded_eof));
        }
    }
}
