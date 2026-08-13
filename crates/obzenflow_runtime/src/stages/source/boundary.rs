// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Policy-neutral source live-I/O boundary seam (FLOWIP-115a).
//!
//! The runtime owns this seam and the source supervisors drive it around a live
//! poll. Concrete middleware policy composition lives outside the runtime and
//! implements [`SourceBoundary`].

use crate::stages::common::handler_error::StageFatal;
use crate::stages::common::handlers::source::{
    ErasedSourceCompletion, ErasedSourceInvocation, ErasedSourceOutcome, SourceError,
};
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

/// Runtime-only result of polling a source once. Handler failures remain
/// visible to dependency policies; framework fatals bypass policy settlement.
pub enum SourcePollResult {
    Completed(SourcePollCompletion),
    HandlerError(SourceError),
    Fatal(StageFatal),
}

/// Result of polling the wrapped source once.
pub struct SourcePollReport {
    /// The poll result.
    pub result: SourcePollResult,
    /// Closed runtime-authored operational observations from this same poll.
    /// They are not source batch facts and never participate in control policy.
    pub operational_events: Vec<ChainEvent>,
    /// Time spent in the raw source poll or timeout only. Error normalization,
    /// boundary policy, output staging, and idle delay are excluded.
    pub poll_duration: Duration,
}

impl SourcePollReport {
    pub(crate) fn from_erased(invocation: ErasedSourceInvocation, poll_duration: Duration) -> Self {
        let (outcome, operational_events) = invocation.into_parts();
        let result = match outcome {
            ErasedSourceOutcome::Completed(ErasedSourceCompletion::Batch(events)) => {
                SourcePollResult::Completed(SourcePollCompletion::Batch(events))
            }
            ErasedSourceOutcome::Completed(ErasedSourceCompletion::Eof) => {
                SourcePollResult::Completed(SourcePollCompletion::Eof)
            }
            ErasedSourceOutcome::HandlerError(error) => SourcePollResult::HandlerError(error),
            ErasedSourceOutcome::Fatal(fatal) => SourcePollResult::Fatal(fatal),
        };
        Self {
            result,
            operational_events,
            poll_duration,
        }
    }

    pub(crate) fn handler_error(error: SourceError, poll_duration: Duration) -> Self {
        Self {
            result: SourcePollResult::HandlerError(error),
            operational_events: Vec::new(),
            poll_duration,
        }
    }
}

/// Source-boundary outcome for one guarded poll.
pub enum SourceBoundaryOutcome {
    /// The boundary admitted and ran the poll.
    Polled(SourcePollReport),
    /// A policy rejected before polling. 115a source policies do not use this
    /// arm, but the neutral seam reserves it for future fail-fast policies.
    Rejected {
        policy: Option<String>,
        reason: String,
    },
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
pub trait SourceBoundary: Send + Sync {
    fn around_poll<'a>(&'a self, execute: SourcePollExecution<'a>) -> SourceBoundaryFuture<'a>;
}
