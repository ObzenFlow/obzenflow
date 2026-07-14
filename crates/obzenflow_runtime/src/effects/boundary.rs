// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

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

type EffectCall = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Vec<ChainEvent>, EffectError>> + Send>,
>;

/// A policy-neutral callable for one live physical effect call.
///
/// The runtime constructs one operation per logical invocation. A boundary
/// may call it more than once, but each call requires an exclusive reference,
/// so calls belonging to one invocation cannot overlap. The operation owns
/// the effect value and creates a fresh [`EffectContext`] for every call.
/// Terminal outcome recording remains outside this callable and happens only
/// after the boundary returns.
pub struct EffectOperation {
    call: Box<dyn FnMut() -> EffectCall + Send>,
}

impl EffectOperation {
    /// Construct a neutral physical-call operation.
    ///
    /// Framework runtimes create this around an effect and its pristine call
    /// context. The public constructor also supports trusted custom boundary
    /// implementations and boundary-focused tests.
    pub fn new<F, Fut>(mut call: F) -> Self
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<Vec<ChainEvent>, EffectError>> + Send + 'static,
    {
        Self {
            call: Box::new(move || Box::pin(call())),
        }
    }

    /// Perform one physical call.
    pub async fn execute(&mut self) -> Result<Vec<ChainEvent>, EffectError> {
        (self.call)().await
    }
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
        operation: EffectOperation,
    ) -> EffectBoundaryReport;
}
