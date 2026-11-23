// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Source stage implementations
//!
//! Sources are divided into two types:
//! - Finite: Sources that eventually complete (files, bounded collections)
//! - Infinite: Sources that run indefinitely (Kafka, WebSocket, etc)
//!
//! ## Reads
//!
//! Each source handler declares one read through a bound port. The supervisor
//! drives the handler. The effect runtime performs or reconstructs each read.
//! The system behind the port does the physical I/O of each read.

pub mod boundary;
pub mod finite;
pub mod infinite;
pub(crate) mod replay_lifecycle;
pub mod strategies;
pub(crate) mod supervision;

pub use boundary::{
    SourceBoundary, SourceBoundaryFuture, SourceBoundaryOutcome, SourceBoundaryReport,
    SourcePollCompletion, SourcePollExecution, SourcePollReport, SourcePollResult,
};

pub use crate::stages::common::handlers::source::{
    HostedSource, RecordDeError, SourceError, TypedAsyncFiniteSourceHandler,
    TypedAsyncInfiniteSourceHandler, TypedFiniteSourceHandler, TypedInfiniteSourceHandler,
};
#[doc(hidden)]
pub use crate::stages::common::handlers::source::{ResumeRefusal, SourceContinuation};

#[cfg(test)]
mod lifecycle_tests;
