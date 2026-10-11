// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Source stage implementations
//!
//! Sources are divided into two types:
//! - Finite: Sources that eventually complete (files, bounded collections)
//! - Infinite: Sources that run indefinitely (Kafka, WebSocket, etc)
//!
//! ## Connector lifecycle
//!
//! The four connector traits in this module configure reusable integrations.
//! Construction, description and cloning perform no external I/O. Each `open`
//! returns an independent typed reader owning its cursor, buffers and resources.
//! Direct typed handlers instead transfer into one execution without `Clone`.
//!
//! The supervisor acquires a reader only after authorised start, or after resume
//! reconstruction requires live input. Strict replay never opens or polls one.
//! [`SourceReaderInitContext`] supplies identity only; connectors receive no
//! journals, execution modes, policy controls or supervisor handles.
//!
//! Synchronous connectors run on the blocking pool. Asynchronous connector methods
//! run on the async workers and must not block; do blocking I/O through
//! `tokio::task::spawn_blocking` (FLOWIP-134i B18, B21).
//!
//! An asynchronous connector owns partial acquisition until it returns its reader.
//! Errors must release partial resources, and dropping the opening future must
//! leave no detached work. After acquisition the runtime attempts `drain` once on
//! every orderly exit, including before the first poll and after runner errors.
//! Drain errors remain secondary to the original failure or natural completion.
//! `drain` releases resources without reading facts or advancing a cursor. Forced
//! task abort and process death cannot guarantee awaited cleanup; implement `Drop`
//! for resources that require synchronous release.
//!
//! Open, poll and drain errors use the existing durable failure paths. Runtime
//! diagnostics retain the error category and stage identity without logging or
//! persisting the connector's raw error text. Connector-authored diagnostics
//! must also exclude credentials and sensitive response data.
//!
//! ## Continuation
//!
//! Each connector declares how its interrupted input continues on resume
//! (FLOWIP-134i B8): [`Refuse`] when it cannot, so resuming an unfinished source
//! refuses before any run starts, or [`Reattach`], for infinite connectors, to
//! continue live at the committed handoff. Direct handlers declare nothing: a
//! finite handler lowers to `Refuse` and an infinite one to `Reattach`.

pub mod boundary;
mod connector;
pub mod finite;
pub mod infinite;
pub(crate) mod replay_lifecycle;
pub mod strategies;
pub(crate) mod supervision;

pub use boundary::{
    SourceBoundary, SourceBoundaryFuture, SourceBoundaryOutcome, SourceBoundaryReport,
    SourcePollCompletion, SourcePollExecution, SourcePollReport, SourcePollResult,
};

#[doc(hidden)]
pub use crate::stages::common::handlers::source::{
    sources_refusing_resume, ResumeRefusal, SourceContinuation,
};
pub use crate::stages::common::handlers::source::{
    AsyncFiniteContinuation, AsyncInfiniteContinuation, FiniteContinuation, InfiniteContinuation,
    PositionedFiniteReader, Reattach, RecordDeError, Refuse, Seek, SeekableSource, SourceError,
    SourceRecord, TypedAsyncFiniteSourceHandler, TypedAsyncInfiniteSourceHandler,
    TypedFiniteSourceHandler, TypedInfiniteSourceHandler,
};
pub use connector::{
    AsyncFiniteSourceConnector, AsyncInfiniteSourceConnector, FiniteSourceConnector,
    InfiniteSourceConnector, SourceReaderInitContext,
};
pub use obzenflow_core::source::{
    EncodedSourcePosition, SourceIdentityError, SourceInputIdentity, SourcePosition,
    SourcePositionError,
};

#[cfg(test)]
mod lifecycle_tests;
