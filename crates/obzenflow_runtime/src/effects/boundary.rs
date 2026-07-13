// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::stages::common::BoundaryStopReceiver;
use std::num::NonZeroU32;

/// Identity of the effect a boundary policy guards (FLOWIP-120c gap G3).
///
/// Carries the effect cursor, the framework's deterministic identity
/// coordinate for one effect invocation, plus the declaration-level facts a
/// policy keys on. One policy instance guards one protected dependency, so
/// `effect_type` is the routing key for per-effect policy chains.
#[derive(Debug, Clone)]
pub struct EffectIdentity {
    pub effect_type: &'static str,
    pub safety: EffectSafety,
    pub cursor: EffectCursor,
    pub idempotency_key: Option<IdempotencyKey>,
}

/// The live execution of an effect, handed to the boundary as a future so
/// per-effect policies compose as wrappers around it (FLOWIP-120c).
///
/// The future is built by `Effects::perform` and owns everything it needs;
/// the boundary decides whether to poll it. On success it yields
/// observation-grade `derived_data_event` copies of the authored facts, the
/// same shape policy middleware observed through `after_effect` before this
/// seam (FLOWIP-120c gap G7: authored facts commit inside `perform` and the
/// boundary never sees the committed originals).
pub type EffectExecution = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Vec<ChainEvent>, EffectError>> + Send>,
>;

/// Re-invokable execution seam for one live declared-effect invocation.
///
/// Each returned future owns its attempt captures, allowing a retry-aware
/// boundary to execute attempts serially while the outer `Effects::perform`
/// call retains the cursor and terminal commit authority.
pub trait EffectExecutor: Send {
    fn attempt(&mut self, attempt: NonZeroU32) -> EffectExecution;
}

/// Structured, policy-neutral reason carried by a boundary abort so the
/// rejection is recorded under the effect cursor and replays deterministically.
#[derive(Debug, Clone)]
pub struct EffectAbortReason {
    pub cause: EffectFailureCause,
    pub message: String,
    pub retry: RetryDisposition,
}

/// How one guarded effect invocation ended at the boundary.
pub enum EffectBoundaryOutcome {
    /// The boundary admitted the effect and polled it to completion. The
    /// payload is the execution result: observation copies on success, the
    /// effect's own error on failure. `Effects::perform` records the real
    /// outcome from its own state, not from these copies.
    Executed(Result<Vec<ChainEvent>, EffectError>),
    /// A policy short-circuited execution and synthesized fallback results
    /// (or none). `source` labels the synthesizing middleware, recorded as
    /// the outcome group's `EffectFactOrigin` (FLOWIP-120h).
    Skipped {
        results: Vec<ChainEvent>,
        source: Option<String>,
    },
    /// A policy rejected execution outright; recorded as a `Failed` outcome
    /// under the effect cursor so strict replay reproduces the rejection.
    Aborted(EffectAbortReason),
}

/// The boundary's report for one guarded invocation: the outcome plus any
/// control events policies emitted, buffered by `Effects` and joined to the
/// stage's normal output path.
pub struct EffectBoundaryReport {
    pub outcome: EffectBoundaryOutcome,
    pub control_events: Vec<ChainEvent>,
}

/// The effect-boundary seam (FLOWIP-120c phase 2).
///
/// Replaces the `before_effect`/`after_effect` bracket: the boundary wraps
/// the whole execution future, so admission may await (a rate limiter awaits
/// a permit instead of blocking the worker) and finalization is structural,
/// every policy that admitted observes how the attempt ended on the way out,
/// whichever arm ended it.
#[async_trait]
pub trait EffectBoundary: Send + Sync {
    async fn around_effect(
        &self,
        identity: &EffectIdentity,
        event: &ChainEvent,
        execute: EffectExecution,
    ) -> EffectBoundaryReport;

    /// Retry-aware entry point. Existing implementations remain source
    /// compatible and execute once through `around_effect`; retry-owning
    /// boundaries override this method and may call the executor again.
    async fn around_retryable_effect(
        &self,
        identity: &EffectIdentity,
        event: &ChainEvent,
        execute: &mut dyn EffectExecutor,
        _stop: BoundaryStopReceiver,
    ) -> EffectBoundaryReport {
        self.around_effect(identity, event, execute.attempt(NonZeroU32::MIN))
            .await
    }

    /// Whether this boundary has live retry enabled for the protected effect.
    ///
    /// Materialisation is the primary safety gate. This defaulted query gives
    /// the runtime a defensive rejection for transactional effects when a
    /// custom caller bypasses ordinary materialisation.
    fn retry_enabled(&self, _identity: &EffectIdentity) -> bool {
        false
    }
}
