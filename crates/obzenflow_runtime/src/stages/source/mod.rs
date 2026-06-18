// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Source stage implementations
//!
//! Sources are divided into two types:
//! - Finite: Sources that eventually complete (files, bounded collections)
//! - Infinite: Sources that run indefinitely (Kafka, WebSocket, etc)

pub mod boundary;
pub mod finite;
pub mod infinite;
pub(crate) mod replay_lifecycle;
pub mod strategies;
pub(crate) mod supervision;
pub mod typed;

pub use boundary::{
    SourceBoundary, SourceBoundaryFuture, SourceBoundaryOutcome, SourceBoundaryReport,
    SourcePollCompletion, SourcePollExecution, SourcePollReport,
};
pub use typed::{
    AsyncFiniteSourceTyped, AsyncInfiniteSourceTyped, FallibleAsyncFiniteSourceTyped,
    FallibleAsyncInfiniteSourceTyped, FallibleFiniteSourceTyped, FallibleInfiniteSourceTyped,
    FiniteSourceTyped, InfiniteSourceTyped,
};
