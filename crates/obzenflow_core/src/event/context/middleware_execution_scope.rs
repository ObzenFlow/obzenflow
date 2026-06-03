// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Middleware execution scope (FLOWIP-120a).
//!
//! Handler-level control middleware (rate limiters, circuit breakers) observe an
//! unreliable boundary and react to it by sleeping, consuming tokens, mutating
//! breaker state, and emitting lifecycle records. During deterministic replay a
//! stage is reconstructed from recorded events and performs no live external
//! work, so those reactions must be suppressed: a replay that re-paces or
//! re-trips a breaker would change timing and state without changing recorded
//! values, and would re-emit records that already exist in the archive.
//!
//! The scope is the signal the supervisor hands to each per-event
//! [`MiddlewareContext`](crate) so middleware can tell which of four execution
//! contexts it is running in. It is deliberately not inferred from
//! `event.replay_context`: that field is stamped only on re-injected source
//! events and is nulled across fan-in, so it does not identify handler-level
//! replay reconstruction at a downstream stage. The authoritative signal is the
//! stage's replay mode, mapped onto this scope by the runtime.
//!
//! The effect boundary is distinguished from handler reconstruction on purpose.
//! Replay returns a recorded effect outcome before the effect boundary is ever
//! consulted, so when boundary middleware does run, the effect is executing live
//! (including the resume-incomplete case where an input carries replay provenance
//! but its effect record is missing and must execute live). Boundary middleware
//! therefore runs under [`LiveEffectBoundary`](MiddlewareExecutionScope::LiveEffectBoundary)
//! and is never suppressed.

/// The execution context a piece of middleware is running in for one event.
///
/// Defaults to [`LiveHandler`](MiddlewareExecutionScope::LiveHandler) so any
/// context that is not explicitly scoped behaves exactly as it did before
/// FLOWIP-120a (live, no suppression).
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
}

impl MiddlewareExecutionScope {
    /// True when the stage is being deterministically reconstructed from recorded
    /// events and handler-level control middleware must suppress all side effects
    /// (state mutation, delay, rejection, lifecycle emission).
    ///
    /// This is the handler-reconstruction predicate only. The effect boundary
    /// reports [`LiveEffectBoundary`](MiddlewareExecutionScope::LiveEffectBoundary)
    /// and is intentionally excluded: it only runs for live effect execution.
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
        // The effect boundary only runs for live effect execution; never suppress it.
        assert!(!MiddlewareExecutionScope::LiveEffectBoundary.is_deterministic_replay());
    }
}
