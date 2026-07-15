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

/// A policy-neutral callable for repeatable live physical effect calls.
///
/// The runtime constructs one operation per eligible non-transactional
/// logical invocation. A boundary may call it more than once, but each call
/// requires an exclusive reference, so calls belonging to one invocation
/// cannot overlap. The operation owns the effect value and creates a fresh
/// [`EffectContext`] for every call. Terminal outcome recording remains
/// outside this callable and happens only after the boundary returns.
pub struct RepeatableEffectOperation {
    call: Box<dyn FnMut() -> EffectCall + Send>,
}

impl RepeatableEffectOperation {
    /// Construct a repeatable physical-call operation.
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

/// A policy-neutral capability for one live physical effect call.
///
/// This capability is used for transactional effects. Calling [`Self::execute`]
/// consumes it, as do the pre-execution rejection constructors. A boundary
/// therefore cannot execute a transaction and subsequently report that the
/// same operation was skipped or aborted.
pub struct SingleUseEffectOperation {
    call: Box<dyn FnOnce() -> EffectCall + Send>,
    provenance: SingleUseEffectProvenance,
}

impl SingleUseEffectOperation {
    /// Construct a single-use physical-call operation.
    ///
    /// Only the runtime may mint this capability. Public boundary
    /// implementations receive it from [`EffectBoundary`] and can consume it,
    /// but cannot manufacture substitute operations or reports.
    pub(super) fn new<F, Fut>(call: F) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<Vec<ChainEvent>, EffectError>> + Send + 'static,
    {
        Self {
            call: Box::new(move || Box::pin(call())),
            provenance: SingleUseEffectProvenance::new(),
        }
    }

    pub(super) fn provenance(&self) -> SingleUseEffectProvenance {
        self.provenance.clone()
    }

    /// Perform the physical call, consuming the operation.
    ///
    /// The returned receipt is the only way to report an executed single-use
    /// operation to the runtime.
    pub async fn execute(self) -> SingleUseEffectExecution {
        let Self { call, provenance } = self;
        SingleUseEffectExecution {
            result: call().await,
            provenance,
        }
    }

    /// Reject the operation before execution.
    pub fn abort(
        self,
        reason: EffectAbortReason,
        control_events: Vec<ChainEvent>,
    ) -> SingleUseEffectBoundaryReport {
        let Self { call, provenance } = self;
        drop(call);
        SingleUseEffectBoundaryReport {
            outcome: SingleUseEffectBoundaryOutcome::Aborted(reason),
            control_events,
            provenance,
        }
    }

    /// Report an attempted fallback before execution.
    ///
    /// Transactional effects do not accept synthesized fallback results. The
    /// runtime records this report as a deterministic boundary rejection.
    pub fn reject_fallback(
        self,
        source: Option<String>,
        control_events: Vec<ChainEvent>,
    ) -> SingleUseEffectBoundaryReport {
        let Self { call, provenance } = self;
        drop(call);
        SingleUseEffectBoundaryReport {
            outcome: SingleUseEffectBoundaryOutcome::FallbackRejected { source },
            control_events,
            provenance,
        }
    }
}

/// Runtime-held invocation brand for a single-use effect capability.
///
/// Pointer identity is compared while both `Arc`s are alive, so even reports
/// exchanged between concurrent boundary invocations cannot be mistaken for
/// reports about the supplied operation.
#[derive(Clone)]
pub(super) struct SingleUseEffectProvenance(std::sync::Arc<SingleUseEffectProvenanceMarker>);

struct SingleUseEffectProvenanceMarker;

impl SingleUseEffectProvenance {
    fn new() -> Self {
        Self(std::sync::Arc::new(SingleUseEffectProvenanceMarker))
    }

    fn matches(&self, expected: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &expected.0)
    }
}

/// Proof that a [`SingleUseEffectOperation`] was executed.
///
/// Its fields and constructor are private so a custom boundary cannot forge
/// an executed report without consuming the operation.
pub struct SingleUseEffectExecution {
    result: Result<Vec<ChainEvent>, EffectError>,
    provenance: SingleUseEffectProvenance,
}

impl SingleUseEffectExecution {
    /// Observe how the physical call ended before finalizing the boundary
    /// report. Transactional settlement still uses the runtime's committed
    /// outcome slot as its source of truth.
    pub fn result(&self) -> &Result<Vec<ChainEvent>, EffectError> {
        &self.result
    }

    /// Finalize an executed single-use boundary report.
    pub fn into_report(self, control_events: Vec<ChainEvent>) -> SingleUseEffectBoundaryReport {
        let provenance = self.provenance.clone();
        SingleUseEffectBoundaryReport {
            outcome: SingleUseEffectBoundaryOutcome::Executed(self),
            control_events,
            provenance,
        }
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

/// How one guarded single-use invocation ended at the boundary.
///
/// The enum is crate-private because its variants are settlement details.
/// Public boundary implementations create coherent reports only by consuming
/// a [`SingleUseEffectOperation`] or [`SingleUseEffectExecution`].
pub(crate) enum SingleUseEffectBoundaryOutcome {
    Executed(SingleUseEffectExecution),
    FallbackRejected { source: Option<String> },
    Aborted(EffectAbortReason),
}

/// The sealed report for one guarded single-use invocation.
pub struct SingleUseEffectBoundaryReport {
    outcome: SingleUseEffectBoundaryOutcome,
    control_events: Vec<ChainEvent>,
    provenance: SingleUseEffectProvenance,
}

impl SingleUseEffectBoundaryReport {
    pub(super) fn into_parts(
        self,
        expected: &SingleUseEffectProvenance,
    ) -> Result<(SingleUseEffectBoundaryOutcome, Vec<ChainEvent>), EffectError> {
        if !self.provenance.matches(expected) {
            return Err(EffectError::EffectProvenanceMismatch(
                "single-use effect boundary returned a report for a different operation"
                    .to_string(),
            ));
        }
        Ok((self.outcome, self.control_events))
    }
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
    async fn around_repeatable_effect(
        &self,
        identity: &EffectIdentity,
        event: &ChainEvent,
        operation: RepeatableEffectOperation,
    ) -> EffectBoundaryReport;

    /// Guard an effect operation that may be executed at most once.
    ///
    /// Transactional operations use this entry point. The distinct consuming
    /// capability prevents recovery loops and custom boundaries from
    /// attempting the same transaction more than once.
    async fn around_single_use_effect(
        &self,
        identity: &EffectIdentity,
        event: &ChainEvent,
        operation: SingleUseEffectOperation,
    ) -> SingleUseEffectBoundaryReport;
}
