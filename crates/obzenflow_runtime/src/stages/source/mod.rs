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
//! An asynchronous connector owns partial acquisition until it returns its reader.
//! Errors must release partial resources, and dropping the opening future must
//! leave no detached work. After acquisition the runtime attempts `drain` once on
//! every orderly exit, including before the first poll and after runner errors.
//! Drain errors remain secondary to the original failure or natural completion.
//! `drain` releases resources without reading facts or advancing a cursor. Forced
//! task abort and process death cannot guarantee awaited cleanup; implement `Drop`
//! for resources that require synchronous release.
//!
//! Poll and drain errors use the existing durable failure paths. Their messages
//! must be safe to persist and contain no credentials or sensitive response data.
//! Opening errors are attributed by the runtime using the error category and
//! stage identity, without persisting the connector's raw error text.

pub mod boundary;
mod connector;
pub mod finite;
pub mod infinite;
pub(crate) mod replay_lifecycle;
pub mod strategies;
pub(crate) mod supervision;
pub mod typed;

pub use boundary::{
    SourceBoundary, SourceBoundaryFuture, SourceBoundaryOutcome, SourceBoundaryReport,
    SourcePollCompletion, SourcePollExecution, SourcePollReport, SourcePollResult,
};
pub use typed::{
    AsyncFiniteSourceTyped, AsyncInfiniteSourceTyped, FallibleAsyncFiniteSourceTyped,
    FallibleAsyncInfiniteSourceTyped, FallibleFiniteSourceTyped, FallibleInfiniteSourceTyped,
    FiniteSourceTyped, InfiniteSourceTyped,
};

pub use crate::stages::common::handlers::source::{
    SourceError, TypedAsyncFiniteSourceHandler, TypedAsyncInfiniteSourceHandler,
    TypedFiniteSourceHandler, TypedInfiniteSourceHandler,
};
pub use connector::{
    AsyncFiniteSourceConnector, AsyncInfiniteSourceConnector, FiniteSourceConnector,
    InfiniteSourceConnector, SourceReaderInitContext,
};

#[cfg(test)]
mod lifecycle_tests;
