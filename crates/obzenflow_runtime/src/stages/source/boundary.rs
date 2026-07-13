// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Policy-neutral source live-I/O boundary seam (FLOWIP-115a).
//!
//! The runtime owns this seam and the source supervisors drive it around a live
//! poll. Concrete middleware policy composition lives outside the runtime and
//! implements [`SourceBoundary`].

use crate::stages::common::handlers::source::SourceError;
use crate::stages::common::BoundaryStopReceiver;
use obzenflow_core::ChainEvent;
use std::future::Future;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::time::Duration;

/// One live source-poll future handed to the boundary.
pub type SourcePollExecution<'a> = Pin<Box<dyn Future<Output = SourcePollReport> + Send + 'a>>;

/// Future returned by the source boundary.
pub type SourceBoundaryFuture<'a> = Pin<Box<dyn Future<Output = SourceBoundaryReport> + Send + 'a>>;

/// Re-invokable execution seam for one live async source poll.
///
/// A retry-aware boundary calls `attempt` once for each physical poll. The
/// executor borrows the same handler, so attempts are serialized and no
/// detached poll survives the boundary invocation.
pub trait SourcePollExecutor: Send {
    fn attempt<'a>(&'a mut self, attempt: NonZeroU32) -> SourcePollExecution<'a>;
}

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
pub trait SourceBoundary: Send + Sync {
    fn around_poll<'a>(&'a self, execute: SourcePollExecution<'a>) -> SourceBoundaryFuture<'a>;

    /// Whether graceful drain must keep this boundary invocation alive long
    /// enough for an already-started physical attempt to settle.
    ///
    /// Legacy one-shot boundaries keep the historical source-supervisor
    /// behaviour: graceful drain drops the whole boundary future promptly.
    /// Retry-owning boundaries opt in because they must distinguish a pending
    /// admission/backoff from an active physical attempt.
    #[doc(hidden)]
    fn graceful_drain_settles_active_attempt(&self) -> bool {
        false
    }

    /// Retry-aware entry point. Existing boundary implementations remain
    /// source compatible and execute once through `around_poll`; boundaries
    /// that own retry override this method and may invoke the executor again.
    fn around_retryable_poll<'a>(
        &'a self,
        execute: &'a mut dyn SourcePollExecutor,
        _stop: BoundaryStopReceiver,
    ) -> SourceBoundaryFuture<'a> {
        self.around_poll(execute.attempt(NonZeroU32::MIN))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct CountingExecutor {
        attempts: Arc<Mutex<Vec<u32>>>,
    }

    impl SourcePollExecutor for CountingExecutor {
        fn attempt<'a>(&'a mut self, attempt: NonZeroU32) -> SourcePollExecution<'a> {
            self.attempts.lock().unwrap().push(attempt.get());
            Box::pin(async {
                SourcePollReport {
                    result: Ok(SourcePollCompletion::Batch(Vec::new())),
                    poll_duration: Duration::ZERO,
                }
            })
        }
    }

    struct LegacyBoundary;

    impl SourceBoundary for LegacyBoundary {
        fn around_poll<'a>(&'a self, execute: SourcePollExecution<'a>) -> SourceBoundaryFuture<'a> {
            Box::pin(async move {
                SourceBoundaryReport {
                    outcome: SourceBoundaryOutcome::Polled(execute.await),
                    control_events: Vec::new(),
                }
            })
        }
    }

    #[tokio::test]
    async fn retryable_default_preserves_legacy_one_shot_boundaries() {
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let mut executor = CountingExecutor {
            attempts: attempts.clone(),
        };

        let report = LegacyBoundary
            .around_retryable_poll(&mut executor, BoundaryStopReceiver::default())
            .await;

        assert!(matches!(report.outcome, SourceBoundaryOutcome::Polled(_)));
        assert_eq!(*attempts.lock().unwrap(), vec![1]);
        assert!(!LegacyBoundary.graceful_drain_settles_active_attempt());
    }
}
