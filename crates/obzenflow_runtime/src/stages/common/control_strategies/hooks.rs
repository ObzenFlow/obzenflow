// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime control-strategy hooks (FLOWIP-115c).
//!
//! The supervisor consults a small set of typed hooks at named decision
//! points. Every driving hook shares the same shape, `Continue` or `Pause`,
//! and adds its own hook-local stop, so the type system keeps an admission
//! rejection distinct from a suppressed signal or a poison EOF. Observation is
//! a callback that never steers the loop.
//!
//! This slice defines the contract. Concrete policies bind to these hooks in
//! the source (115a), effect (115b), sink (115d), and backpressure (115e)
//! slices, so the admission and observation hooks have no consultation site
//! yet. The signal hook lives in [`super::core`] (adapted in place) and the
//! completion hook in the source FSMs.

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

    // Reached only by the supervisor suspension helper. Signal pauses use
    // deadline wakes; backpressure (115e) consumes credit wakes.
    #[allow(dead_code)]
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

/// The positions at which the admission hook may be consulted. The position is
/// data, so one [`AdmissionGate`] answers across positions and the supervisor
/// passes the position it is at. 115c defines the positions; the binding slices
/// wire the consultation sites.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionPosition {
    /// Before pulling the next unit from a source.
    PrePoll,
    /// After a finite source produced a held unit (FLOWIP-114m). Continue or
    /// Pause only; the held unit cannot be rejected.
    PostAdmit,
    /// Before invoking a declared effect.
    BeforeEffect,
    /// Before committing a produced output downstream.
    BeforeOutputCommit,
}

/// Result of consulting the admission hook at a rejecting position
/// (`PrePoll` / `BeforeEffect` / `BeforeOutputCommit`). `RejectAttempt` and
/// `SynthesizeFallback` are this hook's stop.
#[derive(Debug, Clone)]
pub enum AdmissionDecision {
    /// Proceed with the attempt.
    Continue,
    /// Suspend; the supervisor owns the wait and re-consults after `wake`.
    Pause { wake: WakeOn },
    /// Refuse the attempt outright.
    RejectAttempt { reason: String },
    /// Short-circuit with a synthesized result, carried as an opaque marker;
    /// the consuming seam converts it to its own event shape.
    SynthesizeFallback { source: Option<String> },
}

/// Result at the `PostAdmit` position (FLOWIP-114m). Continue or Pause only:
/// the held unit already exists and the narrower type forbids dropping it.
#[derive(Debug, Clone)]
pub enum PostAdmitDecision {
    Continue,
    Pause { wake: WakeOn },
}

/// How one admitted attempt ended. Handed to every observer that admitted it.
/// Mirrors the effect boundary's outcome arms without importing the effect
/// type, preserving the crate onion.
#[derive(Debug, Clone)]
pub enum AttemptOutcome {
    Succeeded,
    Failed { cause: String },
    Skipped,
    Aborted { reason: String },
}

/// Admission hook. Consulted at an [`AdmissionPosition`] before live work runs.
/// 115c defines the trait; the source / effect / sink / backpressure slices
/// wire the consultation sites.
pub trait AdmissionGate: Send + Sync {
    /// Decide whether an attempt may proceed at a rejecting position.
    fn admit(&self, position: AdmissionPosition) -> AdmissionDecision;

    /// Decide whether an already-produced unit may proceed (`PostAdmit`). The
    /// return type cannot reject, preserving FLOWIP-114m's no-drop rule.
    fn admit_held(&self) -> PostAdmitDecision {
        PostAdmitDecision::Continue
    }
}

/// Attempt-observation hook. Pure observation: it returns nothing and cannot
/// steer the loop. `observe` is synchronous so it can run from a `Drop` guard;
/// `settle` is the explicit async path for durable journal work that `Drop`
/// cannot perform.
#[async_trait::async_trait]
pub trait AttemptObserver: Send + Sync {
    /// Synchronous notification of the terminal outcome.
    fn observe(&self, _position: AdmissionPosition, _outcome: &AttemptOutcome) {}

    /// Durable observation. Called explicitly before the loop drops the attempt
    /// guard, or on the way out of a wrapped attempt.
    async fn settle(
        &self,
        _position: AdmissionPosition,
        _outcome: &AttemptOutcome,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_position_is_copy_and_eq() {
        let p = AdmissionPosition::PostAdmit;
        let q = p; // Copy
        assert_eq!(p, q);
        assert_ne!(AdmissionPosition::PrePoll, AdmissionPosition::BeforeEffect);
    }

    #[test]
    fn wake_source_is_clonable() {
        let waker = CreditWaker::new();
        let wake = WakeOn::Notify(waker.clone());
        // Cloning a credit wake shares the underlying Notify.
        let _again = wake.clone();
        waker.notify(); // smoke: producer side does not panic
        let _deadline = WakeOn::At(std::time::Instant::now());
        let _recheck = WakeOn::Immediate;
    }

    struct FixedGate {
        decision: &'static str,
    }

    impl AdmissionGate for FixedGate {
        fn admit(&self, position: AdmissionPosition) -> AdmissionDecision {
            match self.decision {
                "continue" => AdmissionDecision::Continue,
                "reject" => AdmissionDecision::RejectAttempt {
                    reason: format!("rejected at {position:?}"),
                },
                _ => AdmissionDecision::Pause {
                    wake: WakeOn::Immediate,
                },
            }
        }
    }

    #[test]
    fn admission_gate_answers_per_position() {
        let gate = FixedGate { decision: "reject" };
        match gate.admit(AdmissionPosition::PrePoll) {
            AdmissionDecision::RejectAttempt { reason } => assert!(reason.contains("PrePoll")),
            other => panic!("expected reject, got {other:?}"),
        }
        // `admit_held` cannot reject: its type has only Continue / Pause.
        match gate.admit_held() {
            PostAdmitDecision::Continue => {}
            PostAdmitDecision::Pause { .. } => {}
        }
    }
}
