// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Framework integration with Runtime-owned stop admission and completion.
//!
//! This is technically public for Infra's cross-crate use, but is not a
//! supported application API. Runtime remains the sole admission and execution
//! outcome authority; observers neither extend deadlines nor select outcomes.

pub use crate::pipeline::fsm::{FlowCancelCause, FlowStopStatus};

use crate::errors::FlowError;
use crate::pipeline::FlowHandle;
use tokio::sync::watch;

/// Latest accepted stop status, with channel mechanics kept inside Runtime.
/// Updates may coalesce; this observes admission state, not every request.
pub struct StopObserver {
    receiver: watch::Receiver<FlowStopStatus>,
}

/// Runtime has closed stop observation and no unseen status remains.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Runtime stop observation closed")]
pub struct StopObservationClosed;

impl StopObserver {
    pub(crate) fn new(receiver: watch::Receiver<FlowStopStatus>) -> Self {
        Self { receiver }
    }

    /// Read the latest accepted status and mark it observed. Admission
    /// timestamps are preserved, including when observation happens late.
    pub fn snapshot(&mut self) -> FlowStopStatus {
        self.receiver.borrow_and_update().clone()
    }

    /// Wait for an unseen status. Cancelling this wait does not mark an update
    /// observed. A final unseen update is delivered before closure is reported.
    /// Closure is not proof of supervisor completion; callers must still join.
    pub async fn changed(&mut self) -> Result<(), StopObservationClosed> {
        self.receiver
            .changed()
            .await
            .map_err(|_| StopObservationClosed)
    }
}

/// Subscribe before sending a stop request: enqueueing is not admission.
pub fn observe_stop(flow: &FlowHandle) -> StopObserver {
    flow.observe_stop()
}

/// Join the supervisor, then interpret its acknowledged execution outcome.
/// Repeated, concurrent and late waits share one physical join. Dropping a
/// wait neither stops execution nor consumes another observer's completion.
pub async fn wait(flow: &FlowHandle) -> Result<(), FlowError> {
    flow.wait_for_execution().await
}

/// Request timeout escalation. Runtime admits it only against an expired
/// graceful stop and preserves the original deadline and selected outcome.
pub async fn cancel_after_timeout(flow: &FlowHandle) -> Result<(), FlowError> {
    flow.cancel_after_timeout().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[tokio::test]
    async fn stop_observer_preserves_deadlines_and_cancelled_waits() {
        let (sender, receiver) = watch::channel(FlowStopStatus::NotRequested);
        let mut observer = StopObserver::new(receiver);
        assert_eq!(observer.snapshot(), FlowStopStatus::NotRequested);
        let deadline = Instant::now() + Duration::from_secs(5);
        sender.send_replace(FlowStopStatus::Graceful { deadline });
        assert_eq!(observer.snapshot(), FlowStopStatus::Graceful { deadline });
        let mut wait = Box::pin(observer.changed());
        assert!(
            futures::poll!(&mut wait).is_pending(),
            "snapshot must mark the update observed"
        );
        drop(wait);

        let cancelling = FlowStopStatus::Cancelling {
            admitted_at: deadline,
            cause: FlowCancelCause::GracefulTimeout,
            graceful_deadline: Some(deadline),
        };
        sender.send_replace(cancelling.clone());
        observer.changed().await.unwrap();
        assert_eq!(observer.snapshot(), cancelling);
        drop(sender);
        assert_eq!(observer.changed().await, Err(StopObservationClosed));
        assert_eq!(
            observer.snapshot(),
            cancelling,
            "closure retains the last admitted status"
        );
    }

    #[tokio::test]
    async fn stop_observer_delivers_latest_unseen_status_before_closure() {
        let (sender, receiver) = watch::channel(FlowStopStatus::NotRequested);
        let mut observer = StopObserver::new(receiver);
        let deadline = Instant::now() + Duration::from_secs(5);
        sender.send_replace(FlowStopStatus::Graceful { deadline });
        let cancelling = FlowStopStatus::Cancelling {
            admitted_at: Instant::now(),
            cause: FlowCancelCause::Requested,
            graceful_deadline: Some(deadline),
        };
        sender.send_replace(cancelling.clone());
        drop(sender);
        observer.changed().await.unwrap();
        assert_eq!(
            observer.snapshot(),
            cancelling,
            "updates may coalesce without losing the deadline"
        );
        assert_eq!(observer.changed().await, Err(StopObservationClosed));
    }
}
