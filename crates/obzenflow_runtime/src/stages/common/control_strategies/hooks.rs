// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Wake sources for paused supervisors.
//!
//! Live I/O policies (source/effect/sink delivery) compose as boundary
//! middleware. The signal hook lives in [`super::core`] and the completion hook
//! in the source FSMs.

use std::sync::Arc;

/// Why and when a paused supervisor should re-check its gate.
///
/// Every wake is bounded: `At` is self-bounding, `Immediate` does not wait, and
/// `Notify` must be paired with a stall cap by the caller. The suspension helper
/// enforces this; 115e supplies the per-edge cap.
#[derive(Debug, Clone)]
pub enum WakeOn {
    /// Re-check no later than this instant.
    At(std::time::Instant),
    /// Re-check when a producer signals that credit may be available.
    Notify(CreditWaker),
    /// Re-check immediately on the next loop iteration (a single yield).
    Immediate,
}

/// A clonable handle a producer notifies to wake a credit-blocked consumer.
///
/// Wraps a `tokio::sync::Notify`. 115c ships it as the reusable primitive;
/// concrete credit accounting is bound by 115e. A wake is a prompt to re-check
/// the readiness condition, never a grant.
#[derive(Clone)]
pub struct CreditWaker(Arc<tokio::sync::Notify>);

impl CreditWaker {
    pub fn new() -> Self {
        Self(Arc::new(tokio::sync::Notify::new()))
    }

    /// Producer side: signal that credit may now be available, after the
    /// readiness condition has been published.
    pub fn notify(&self) {
        self.0.notify_one();
    }

    pub(crate) fn inner(&self) -> &Arc<tokio::sync::Notify> {
        &self.0
    }
}

impl Default for CreditWaker {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for CreditWaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CreditWaker")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloned_credit_wake_shares_its_notify() {
        let waker = CreditWaker::new();
        let WakeOn::Notify(cloned) = WakeOn::Notify(waker.clone()).clone() else {
            panic!("a cloned credit wake stays a credit wake");
        };
        assert!(Arc::ptr_eq(waker.inner(), cloned.inner()));
    }
}
