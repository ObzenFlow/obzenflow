// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Middleware execution scope (FLOWIP-120a), a transitional bridge under
//! FLOWIP-120c.
//!
//! Handler-level control middleware (rate limiters, circuit breakers) observe an
//! unreliable boundary and react to it by sleeping, consuming tokens, mutating
//! breaker state, and emitting lifecycle records. During deterministic replay a
//! stage is reconstructed from recorded events and performs no live external
//! work, so those reactions must be suppressed: a replay that re-paces or
//! re-trips a breaker would change timing and state without changing recorded
//! values, and would re-emit records that already exist in the archive.
//!
//! FLOWIP-120c's placement split makes that suppression structural: policy
//! middleware attaches to live I/O units only (sources, the effect boundary
//! per effect, sink delivery), so it cannot run during reconstruction at all,
//! the way replay bypasses source middleware through the `ReplayDriver`.
//! This scope remains for the transition, in three roles: it suppresses the
//! handler-level policy chains that still exist until the split retires them
//! (the surface FLOWIP-120f deletes), it labels observation emissions during
//! reconstruction (FLOWIP-120i), and its sink share survives until
//! FLOWIP-095g settles re-delivery suppression. It is computed per dispatched
//! event by the supervisors (FLOWIP-120c H3), which is the seam where
//! FLOWIP-120n's resume phase predicate decides live versus reconstruction
//! per position.
//!
//! The scope is the signal the supervisor hands to each per-event
//! [`MiddlewareContext`](crate) so middleware can tell which of four execution
//! contexts it is running in. It is deliberately not inferred from
//! `event.replay_context`: that field is stamped only on re-injected source
//! events and is nulled across fan-in, so it does not identify handler-level
//! replay reconstruction at a downstream stage. The authoritative signal is the
//! stage's replay mode, mapped onto this scope by the runtime.
//!
//! Live I/O boundaries are distinguished from handler reconstruction on purpose.
//! Replay returns recorded source/effect/sink work before a boundary is ever
//! consulted, so when boundary middleware does run, it is guarding live I/O.
//! Boundary middleware therefore runs under a live boundary scope and is never
//! suppressed.

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

    /// Live source-boundary execution. The source boundary wraps only the live
    /// poll branch; replay bypasses it structurally. Never suppressed.
    LiveSourceBoundary,
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
    }
}
