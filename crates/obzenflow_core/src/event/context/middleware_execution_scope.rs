// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Execution scope for typed policy boundaries and runtime observers.
//!
//! During deterministic replay a stage is reconstructed from recorded events
//! and performs no live external work. Replay-sensitive observers receive the
//! reconstruction variants, while source, effect, and sink policy contexts are
//! constructed only for the corresponding live boundary variants.
//!
//! The placement split makes policy suppression structural: control policy
//! attaches to live I/O units only, so strict replay bypasses it rather than
//! asking it to inspect a flag. Runtime supervisors compute stage scope for
//! observer dispatch and journal evidence; adapter-owned boundaries assign the
//! live scope to their invocation-local typed policy carrier.
//!
//! Stage scope is deliberately not inferred from `event.replay_context`: that
//! field is stamped only on re-injected source events and is nulled across
//! fan-in. The runtime execution phase remains authoritative.

/// The execution context a piece of middleware is running in for one event.
///
/// Defaults to [`LiveHandler`](MiddlewareExecutionScope::LiveHandler) for
/// ordinary live stage and observer work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MiddlewareExecutionScope {
    /// Live handler execution. Middleware runs normally.
    #[default]
    LiveHandler,

    /// Strict-replay handler reconstruction. The stage is being rebuilt from a
    /// complete archive and performs no live external work, so handler-level
    /// control middleware must not mutate state, delay, reject, or emit.
    StrictReplayHandler,

    /// Incomplete-archive resume handler reconstruction. The replayed portion of
    /// a resumed run is reconstructed exactly like strict replay, so handler-level
    /// control middleware is suppressed here too. Live work that resume performs
    /// happens at the effect boundary, under [`LiveEffectBoundary`], not here.
    ResumeHandler,

    /// Live effect-boundary execution. The effect boundary is consulted only when
    /// an effect is executing live (replay returns the recorded outcome first), so
    /// boundary middleware runs and protects the live call. Never suppressed.
    LiveEffectBoundary,

    /// Live source-boundary execution. The source boundary wraps only the live
    /// poll branch; replay bypasses it structurally. Never suppressed.
    LiveSourceBoundary,

    /// Live sink-delivery-boundary execution (FLOWIP-115b). The sink delivery
    /// boundary wraps only the live data-event `consume_report` attempt; replay
    /// reconstructs recorded receipts without consulting it. Never suppressed.
    LiveSinkDeliveryBoundary,
}

impl MiddlewareExecutionScope {
    /// True when the stage is being deterministically reconstructed from recorded
    /// events and handler-level control middleware must suppress all side effects
    /// (state mutation, delay, rejection, lifecycle emission).
    ///
    /// This is the handler-reconstruction predicate only. Live I/O boundaries
    /// report boundary scopes and are intentionally excluded: they only run for
    /// live execution.
    pub fn is_deterministic_replay(&self) -> bool {
        matches!(self, Self::StrictReplayHandler | Self::ResumeHandler)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_live_handler() {
        assert_eq!(
            MiddlewareExecutionScope::default(),
            MiddlewareExecutionScope::LiveHandler
        );
    }

    #[test]
    fn only_handler_reconstruction_scopes_are_deterministic_replay() {
        assert!(!MiddlewareExecutionScope::LiveHandler.is_deterministic_replay());
        assert!(MiddlewareExecutionScope::StrictReplayHandler.is_deterministic_replay());
        assert!(MiddlewareExecutionScope::ResumeHandler.is_deterministic_replay());
        // Live I/O boundaries only run for live execution; never suppress them.
        assert!(!MiddlewareExecutionScope::LiveEffectBoundary.is_deterministic_replay());
        assert!(!MiddlewareExecutionScope::LiveSourceBoundary.is_deterministic_replay());
        assert!(!MiddlewareExecutionScope::LiveSinkDeliveryBoundary.is_deterministic_replay());
    }
}
