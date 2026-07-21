// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

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

/// Policy-neutral outcome of the physical dependency span.
///
/// This deliberately records only whether `Effect::execute` succeeded. The
/// returned operation result may still fail later while decomposing an
/// otherwise successful outcome into facts, which is not dependency health.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalCallOutcome {
    Succeeded,
    Failed,
}

/// Read-only lifecycle observation for one prepared physical call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalCallObservation {
    Prepared,
    Started {
        dependency_elapsed: Duration,
    },
    Completed {
        outcome: PhysicalCallOutcome,
        dependency_elapsed: Duration,
    },
}

#[derive(Debug)]
enum PhysicalCallState {
    Prepared,
    Started {
        at: Instant,
    },
    Completed {
        outcome: PhysicalCallOutcome,
        dependency_elapsed: Duration,
    },
}

/// Shared, policy-neutral receipt for a physical effect call.
///
/// The runtime owns the state transitions. Boundary adapters may only observe
/// them, including synchronously during cancellation-driven `Drop`.
#[derive(Debug, Clone)]
pub struct PhysicalCallReceipt(Arc<Mutex<PhysicalCallState>>);

impl PhysicalCallReceipt {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(PhysicalCallState::Prepared)))
    }

    pub fn observation(&self) -> PhysicalCallObservation {
        let state = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match *state {
            PhysicalCallState::Prepared => PhysicalCallObservation::Prepared,
            PhysicalCallState::Started { at } => PhysicalCallObservation::Started {
                dependency_elapsed: at.elapsed(),
            },
            PhysicalCallState::Completed {
                outcome,
                dependency_elapsed,
            } => PhysicalCallObservation::Completed {
                outcome,
                dependency_elapsed,
            },
        }
    }
}

/// Runtime-owned lifecycle mutator paired with [`PhysicalCallReceipt`].
#[derive(Clone)]
pub(crate) struct PhysicalCallLifecycle {
    receipt: PhysicalCallReceipt,
}

impl PhysicalCallLifecycle {
    pub(crate) fn mark_started(&self) {
        let mut state = self
            .receipt
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if matches!(*state, PhysicalCallState::Prepared) {
            *state = PhysicalCallState::Started { at: Instant::now() };
        }
    }

    pub(crate) fn mark_completed(&self, outcome: PhysicalCallOutcome) {
        let mut state = self
            .receipt
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let PhysicalCallState::Started { at } = *state else {
            return;
        };
        *state = PhysicalCallState::Completed {
            outcome,
            dependency_elapsed: at.elapsed(),
        };
    }
}

/// One prepared repeatable physical call and its lifecycle receipt.
pub struct PreparedRepeatableEffectCall {
    call: EffectCall,
    receipt: PhysicalCallReceipt,
}

impl PreparedRepeatableEffectCall {
    pub fn receipt(&self) -> PhysicalCallReceipt {
        self.receipt.clone()
    }

    pub async fn execute(self) -> Result<Vec<ChainEvent>, EffectError> {
        self.call.await
    }
}

/// A policy-neutral callable for repeatable live physical effect calls.
///
/// The runtime constructs one operation per eligible non-transactional
/// logical invocation. A boundary may call it more than once, but each call
/// requires an exclusive reference, so calls belonging to one invocation
/// cannot overlap. The operation owns the effect value and creates a fresh
/// [`EffectContext`] for every call. Terminal outcome recording remains
/// outside this callable and happens only after the boundary returns.
pub struct RepeatableEffectOperation {
    call: Box<dyn FnMut(PhysicalCallLifecycle) -> EffectCall + Send>,
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
        Self::new_with_lifecycle(move |lifecycle| {
            let future = call();
            async move {
                lifecycle.mark_started();
                let result = future.await;
                lifecycle.mark_completed(if result.is_ok() {
                    PhysicalCallOutcome::Succeeded
                } else {
                    PhysicalCallOutcome::Failed
                });
                result
            }
        })
    }

    pub(crate) fn new_with_lifecycle<F, Fut>(mut call: F) -> Self
    where
        F: FnMut(PhysicalCallLifecycle) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<Vec<ChainEvent>, EffectError>> + Send + 'static,
    {
        Self {
            call: Box::new(move |lifecycle| Box::pin(call(lifecycle))),
        }
    }

    /// Prepare one physical call without polling it.
    pub fn prepare(&mut self) -> PreparedRepeatableEffectCall {
        let receipt = PhysicalCallReceipt::new();
        let lifecycle = PhysicalCallLifecycle {
            receipt: receipt.clone(),
        };
        PreparedRepeatableEffectCall {
            call: (self.call)(lifecycle),
            receipt,
        }
    }

    /// Perform one physical call.
    pub async fn execute(&mut self) -> Result<Vec<ChainEvent>, EffectError> {
        self.prepare().execute().await
    }
}

/// A policy-neutral capability for one live physical effect call.
///
/// This capability is used for transactional effects. Calling [`Self::execute`]
/// consumes it, as do the pre-execution rejection constructors. A boundary
/// therefore cannot execute a transaction and subsequently report that the
/// same operation was skipped or aborted.
pub struct SingleUseEffectOperation {
    call: Box<dyn FnOnce(PhysicalCallLifecycle) -> EffectCall + Send>,
    provenance: SingleUseEffectProvenance,
}

/// One prepared transactional physical call and its lifecycle receipt.
///
/// Preparing consumes the single-use capability, preserving the invariant
/// that a transactional port can be polled at most once while still allowing
/// an effect boundary to observe the runtime-owned dependency span.
pub struct PreparedSingleUseEffectCall {
    call: EffectCall,
    provenance: SingleUseEffectProvenance,
    receipt: PhysicalCallReceipt,
}

impl PreparedSingleUseEffectCall {
    pub fn receipt(&self) -> PhysicalCallReceipt {
        self.receipt.clone()
    }

    pub async fn execute(self) -> SingleUseEffectExecution {
        let Self {
            call, provenance, ..
        } = self;
        SingleUseEffectExecution {
            result: call.await,
            provenance,
        }
    }
}

impl SingleUseEffectOperation {
    /// Construct a single-use physical-call operation.
    ///
    /// Only the runtime may mint this capability. Public boundary
    /// implementations receive it from [`EffectBoundary`] and can consume it,
    /// but cannot manufacture substitute operations or reports.
    #[cfg(test)]
    pub(super) fn new<F, Fut>(call: F) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<Vec<ChainEvent>, EffectError>> + Send + 'static,
    {
        Self::new_with_lifecycle(move |lifecycle| async move {
            lifecycle.mark_started();
            let result = call().await;
            lifecycle.mark_completed(if result.is_ok() {
                PhysicalCallOutcome::Succeeded
            } else {
                PhysicalCallOutcome::Failed
            });
            result
        })
    }

    pub(super) fn new_with_lifecycle<F, Fut>(call: F) -> Self
    where
        F: FnOnce(PhysicalCallLifecycle) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<Vec<ChainEvent>, EffectError>> + Send + 'static,
    {
        Self {
            call: Box::new(move |lifecycle| Box::pin(call(lifecycle))),
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
        self.prepare().execute().await
    }

    /// Prepare the sole physical call without polling it.
    pub fn prepare(self) -> PreparedSingleUseEffectCall {
        let Self { call, provenance } = self;
        let receipt = PhysicalCallReceipt::new();
        let lifecycle = PhysicalCallLifecycle {
            receipt: receipt.clone(),
        };
        PreparedSingleUseEffectCall {
            call: call(lifecycle),
            provenance,
            receipt,
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
    /// Read the executed physical result, when the operation ran.
    pub fn execution_result(&self) -> Option<&Result<Vec<ChainEvent>, EffectError>> {
        match &self.outcome {
            SingleUseEffectBoundaryOutcome::Executed(execution) => Some(execution.result()),
            SingleUseEffectBoundaryOutcome::FallbackRejected { .. }
            | SingleUseEffectBoundaryOutcome::Aborted(_) => None,
        }
    }

    /// Read a pre-execution abort reason, when admission rejected the call.
    pub fn abort_reason(&self) -> Option<&EffectAbortReason> {
        match &self.outcome {
            SingleUseEffectBoundaryOutcome::Aborted(reason) => Some(reason),
            SingleUseEffectBoundaryOutcome::Executed(_)
            | SingleUseEffectBoundaryOutcome::FallbackRejected { .. } => None,
        }
    }

    /// Read the policy label that attempted an unsupported transactional
    /// fallback, when present.
    pub fn fallback_source(&self) -> Option<Option<&str>> {
        match &self.outcome {
            SingleUseEffectBoundaryOutcome::FallbackRejected { source } => Some(source.as_deref()),
            SingleUseEffectBoundaryOutcome::Executed(_)
            | SingleUseEffectBoundaryOutcome::Aborted(_) => None,
        }
    }

    /// Append buffered control evidence produced by outer observations.
    pub fn extend_control_events(&mut self, events: impl IntoIterator<Item = ChainEvent>) {
        self.control_events.extend(events);
    }

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

#[cfg(test)]
mod lifecycle_tests {
    use super::*;

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn repeatable_receipt_excludes_post_dependency_materialisation() {
        let mut operation = RepeatableEffectOperation::new_with_lifecycle(|lifecycle| async move {
            lifecycle.mark_started();
            tokio::time::sleep(Duration::from_millis(25)).await;
            lifecycle.mark_completed(PhysicalCallOutcome::Succeeded);
            tokio::time::sleep(Duration::from_millis(75)).await;
            Err(EffectError::Serialization(
                "outcome decomposition failed".to_string(),
            ))
        });

        let prepared = operation.prepare();
        let receipt = prepared.receipt();
        assert_eq!(receipt.observation(), PhysicalCallObservation::Prepared);
        assert!(prepared.execute().await.is_err());
        assert_eq!(
            receipt.observation(),
            PhysicalCallObservation::Completed {
                outcome: PhysicalCallOutcome::Succeeded,
                dependency_elapsed: Duration::from_millis(25),
            }
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn transactional_receipt_times_the_whole_single_use_envelope() {
        let operation = SingleUseEffectOperation::new_with_lifecycle(|lifecycle| async move {
            lifecycle.mark_started();
            tokio::time::sleep(Duration::from_millis(40)).await;
            lifecycle.mark_completed(PhysicalCallOutcome::Failed);
            Err(EffectError::Transport("commit failed".to_string()))
        });

        let prepared = operation.prepare();
        let receipt = prepared.receipt();
        let execution = prepared.execute().await;
        assert!(execution.result().is_err());
        assert_eq!(
            receipt.observation(),
            PhysicalCallObservation::Completed {
                outcome: PhysicalCallOutcome::Failed,
                dependency_elapsed: Duration::from_millis(40),
            }
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn dropped_in_flight_call_leaves_an_observable_started_receipt() {
        let mut operation = RepeatableEffectOperation::new_with_lifecycle(|lifecycle| async move {
            lifecycle.mark_started();
            std::future::pending::<Result<Vec<ChainEvent>, EffectError>>().await
        });
        let prepared = operation.prepare();
        let receipt = prepared.receipt();
        let task = tokio::spawn(prepared.execute());
        tokio::task::yield_now().await;

        assert!(matches!(
            receipt.observation(),
            PhysicalCallObservation::Started { .. }
        ));
        task.abort();
        let _ = task.await;
        assert!(matches!(
            receipt.observation(),
            PhysicalCallObservation::Started { .. }
        ));
    }
}
