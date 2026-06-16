// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Policy-neutral source live-I/O boundary seam (FLOWIP-115a).
//!
//! The runtime owns this seam and the source supervisors drive it around a live
//! poll. Concrete middleware policy composition lives outside the runtime and
//! implements [`SourceBoundaryMiddleware`].

use crate::stages::common::handlers::source::SourceError;
use obzenflow_core::ChainEvent;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// One live source-poll future handed to the boundary.
pub type SourcePollExecution<'a> = Pin<Box<dyn Future<Output = SourcePollReport> + Send + 'a>>;

/// Future returned by the source boundary.
pub type SourceBoundaryFuture<'a> = Pin<Box<dyn Future<Output = SourceBoundaryReport> + Send + 'a>>;

/// Normalized source-poll completion across finite and infinite source kinds.
pub enum SourcePollCompletion {
    /// The poll returned a batch. The batch may be empty.
    Batch(Vec<ChainEvent>),
    /// A finite source completed naturally.
    Eof,
}

/// Result of polling the wrapped source once.
pub struct SourcePollReport {
    /// The poll result.
    pub result: Result<SourcePollCompletion, SourceError>,
    /// Time spent inside the actual source poll, excluding boundary waits.
    pub poll_duration: Duration,
}

/// Source-boundary outcome for one guarded poll.
pub enum SourceBoundaryOutcome {
    /// The boundary admitted and ran the poll.
    Polled(SourcePollReport),
    /// A policy rejected before polling. 115a source policies do not use this
    /// arm, but the neutral seam reserves it for future fail-fast policies.
    Rejected { reason: String },
}

/// Source-boundary report consumed by the runtime supervisor.
pub struct SourceBoundaryReport {
    /// How the guarded poll ended.
    pub outcome: SourceBoundaryOutcome,
    /// Policy observability/control events buffered by the boundary.
    pub control_events: Vec<ChainEvent>,
}

/// Runtime-neutral source boundary interface.
///
/// The supervisor drives this single future and performs lifecycle transitions
/// from the returned report. It does not know which, if any, middleware policies
/// are composed behind the boundary.
pub trait SourceBoundaryMiddleware: Send + Sync {
    fn around_poll<'a>(&'a self, execute: SourcePollExecution<'a>) -> SourceBoundaryFuture<'a>;
}
