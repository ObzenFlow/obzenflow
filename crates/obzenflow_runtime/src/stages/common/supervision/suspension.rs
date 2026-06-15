// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Bounded suspension and attempt finalization (FLOWIP-115c).
//!
//! A control-strategy gate never awaits. It returns a [`WakeOn`] and the
//! supervisor calls [`suspend_until`], the single place a wake becomes an
//! `await`. The bounded-wake rule lives here: every non-source pause is bounded
//! by a deadline or a runtime-supplied stall cap, so a wedged downstream can
//! never hang the loop.
//!
//! [`AdmittedAttempt`] is the loop-driven finalization shape: a guard that
//! settles its observers once, on every exit path, modelled on the
//! circuit-breaker probe-slot guard. The wrap-the-future shape already exists
//! as the effect boundary and is realigned by 115b.
//!
//! These items are the reusable machinery the supervisor loop and the binding
//! slices consume: `suspend_until` is wired into the signal `Pause` path in a
//! later 115c PR, and `AdmittedAttempt` is consumed by the admission seams in
//! 115a/115b/115d. Until then they have no non-test caller, so the module
//! permits dead code; the allowance is removed as each consumer lands.
#![allow(dead_code)]

use crate::stages::common::control_strategies::{
    AdmissionPosition, AttemptObserver, AttemptOutcome, WakeOn,
};
use std::sync::Arc;
use std::time::Duration;

/// Outcome of a bounded suspension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WakeOutcome {
    /// The wake fired: the deadline elapsed, credit was signalled, or the
    /// re-check was immediate.
    Woke,
    /// The stall cap elapsed before the wake fired. Callers map this to their
    /// stall fact (115e: `backpressure.stalled`).
    BoundElapsed,
}

/// Suspend the current supervisor iteration until `wake` fires or `max_wait`
/// elapses, whichever comes first. Gates never call this; they return a
/// [`WakeOn`] and the supervisor owns the wait.
///
/// `max_wait` is required only for [`WakeOn::Notify`], the one
/// unbounded wake. [`WakeOn::At`] is self-bounded and
/// [`WakeOn::Immediate`] does not wait, so both pass `None`.
pub(crate) async fn suspend_until(wake: &WakeOn, max_wait: Option<Duration>) -> WakeOutcome {
    match wake {
        WakeOn::Immediate => {
            tokio::task::yield_now().await;
            WakeOutcome::Woke
        }
        WakeOn::At(at) => {
            tokio::time::sleep_until(tokio::time::Instant::from_std(*at)).await;
            WakeOutcome::Woke
        }
        WakeOn::Notify(waker) => {
            debug_assert!(
                max_wait.is_some(),
                "a CreditAvailable pause must carry a stall cap"
            );
            let cap = max_wait.unwrap_or(Duration::ZERO);
            let notify = waker.inner().clone();
            tokio::select! {
                biased;
                _ = notify.notified() => WakeOutcome::Woke,
                _ = tokio::time::sleep(cap) => WakeOutcome::BoundElapsed,
            }
        }
    }
}

/// RAII guard for an admitted attempt whose work the supervisor performs in
/// pieces. Guarantees each observer that admitted the attempt is notified of
/// the terminal outcome exactly once, on every exit path.
///
/// Two paths: call [`AdmittedAttempt::settle`] when the loop knows the outcome
/// and durable journal work must happen, or let `Drop` run the synchronous
/// `observe` with a default `Aborted` outcome. `Drop` cannot await, so it never
/// runs the durable `settle`; an attempt ended by a drop is, by the
/// event-sourcing model, simply not recorded as complete and is re-attempted on
/// restart.
pub(crate) struct AdmittedAttempt {
    position: AdmissionPosition,
    observers: Vec<Arc<dyn AttemptObserver>>,
    settled: bool,
}

impl AdmittedAttempt {
    pub(crate) fn new(
        position: AdmissionPosition,
        observers: Vec<Arc<dyn AttemptObserver>>,
    ) -> Self {
        Self {
            position,
            observers,
            settled: false,
        }
    }

    /// Durable settle. Notifies each observer's `observe` then `settle` once;
    /// the subsequent `Drop` is a no-op.
    pub(crate) async fn settle(
        mut self,
        outcome: AttemptOutcome,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.settled = true;
        for observer in &self.observers {
            observer.observe(self.position, &outcome);
            observer.settle(self.position, &outcome).await?;
        }
        Ok(())
    }
}

impl Drop for AdmittedAttempt {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let outcome = AttemptOutcome::Aborted {
            reason: "attempt dropped without explicit settle".to_string(),
        };
        for observer in &self.observers {
            // Drop cannot await, so only the synchronous observe runs here.
            observer.observe(self.position, &outcome);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::common::control_strategies::CreditWaker;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test(start_paused = true)]
    async fn deadline_wake_returns_woke() {
        let wake = WakeOn::At(std::time::Instant::now() + Duration::from_millis(10));
        assert_eq!(suspend_until(&wake, None).await, WakeOutcome::Woke);
    }

    #[tokio::test(start_paused = true)]
    async fn recheck_wake_returns_woke_without_waiting() {
        assert_eq!(
            suspend_until(&WakeOn::Immediate, None).await,
            WakeOutcome::Woke
        );
    }

    #[tokio::test(start_paused = true)]
    async fn credit_wake_woke_when_notified() {
        let waker = CreditWaker::new();
        // A notify with no waiter stores a permit, so the next notified()
        // completes immediately and beats the cap.
        waker.notify();
        let wake = WakeOn::Notify(waker);
        assert_eq!(
            suspend_until(&wake, Some(Duration::from_secs(30))).await,
            WakeOutcome::Woke
        );
    }

    #[tokio::test(start_paused = true)]
    async fn credit_wake_bound_elapsed_when_silent() {
        let wake = WakeOn::Notify(CreditWaker::new());
        assert_eq!(
            suspend_until(&wake, Some(Duration::from_millis(50))).await,
            WakeOutcome::BoundElapsed
        );
    }

    #[cfg(debug_assertions)]
    #[tokio::test(start_paused = true)]
    #[should_panic(expected = "must carry a stall cap")]
    async fn credit_wake_without_cap_panics_in_debug() {
        let wake = WakeOn::Notify(CreditWaker::new());
        let _ = suspend_until(&wake, None).await;
    }

    #[derive(Default)]
    struct CountingObserver {
        observed: Arc<AtomicUsize>,
        settled: Arc<AtomicUsize>,
        last_was_abort: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl AttemptObserver for CountingObserver {
        fn observe(&self, _position: AdmissionPosition, outcome: &AttemptOutcome) {
            self.observed.fetch_add(1, Ordering::SeqCst);
            if matches!(outcome, AttemptOutcome::Aborted { .. }) {
                self.last_was_abort.fetch_add(1, Ordering::SeqCst);
            }
        }

        async fn settle(
            &self,
            _position: AdmissionPosition,
            _outcome: &AttemptOutcome,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.settled.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn settle_runs_observe_and_settle_once() {
        let observer = Arc::new(CountingObserver::default());
        let observed = observer.observed.clone();
        let settled = observer.settled.clone();
        let attempt = AdmittedAttempt::new(AdmissionPosition::BeforeEffect, vec![observer.clone()]);
        attempt
            .settle(AttemptOutcome::Succeeded)
            .await
            .expect("settle should succeed");
        assert_eq!(observed.load(Ordering::SeqCst), 1);
        assert_eq!(settled.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn drop_without_settle_runs_only_sync_observe_with_abort() {
        let observer = Arc::new(CountingObserver::default());
        let observed = observer.observed.clone();
        let settled = observer.settled.clone();
        let aborts = observer.last_was_abort.clone();
        {
            let _attempt = AdmittedAttempt::new(AdmissionPosition::PrePoll, vec![observer.clone()]);
            // dropped here without settle
        }
        assert_eq!(
            observed.load(Ordering::SeqCst),
            1,
            "sync observe ran on drop"
        );
        assert_eq!(
            settled.load(Ordering::SeqCst),
            0,
            "Drop cannot run the async durable settle"
        );
        assert_eq!(aborts.load(Ordering::SeqCst), 1, "drop default is Aborted");
    }
}
