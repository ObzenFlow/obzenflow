// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Bounded suspension for paused supervisors.
//!
//! A control-strategy gate never awaits. It returns a [`WakeOn`] and the
//! supervisor calls [`suspend_until`], the single place a wake becomes an
//! `await`. The bounded-wake rule lives here: every non-source pause is bounded
//! by a deadline or a runtime-supplied stall cap, so a wedged downstream can
//! never hang the loop.

use crate::stages::common::control_strategies::WakeOn;
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
            let cap = max_wait.expect("a Notify pause must carry a stall cap");
            let notify = waker.inner().clone();
            tokio::select! {
                biased;
                _ = notify.notified() => WakeOutcome::Woke,
                _ = tokio::time::sleep(cap) => WakeOutcome::BoundElapsed,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::common::control_strategies::CreditWaker;

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

    #[tokio::test(start_paused = true)]
    #[should_panic(expected = "Notify pause must carry a stall cap")]
    async fn credit_wake_without_cap_panics() {
        let wake = WakeOn::Notify(CreditWaker::new());
        suspend_until(&wake, None).await;
    }
}
