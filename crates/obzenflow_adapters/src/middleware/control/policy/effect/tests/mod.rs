// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::super::retry::AttemptDisposition;
use super::disposition::effect_attempt_disposition;
use super::*;
use crate::middleware::control::circuit_breaker::CircuitBreakerMiddleware;
use crate::middleware::{
    BoundaryRetryOwner, BoundaryRetryPolicy, EffectSurface, EffectTypeKey, EffectUnitId,
    MiddlewareAbortCause, MiddlewareAttachmentId, MiddlewareAttachmentRequest, MiddlewareContext,
    MiddlewareDeclaration, MiddlewareDeclarationIndex, MiddlewareOrigin, MiddlewareSurface,
    MiddlewareSurfaceKind, ProtectedUnit, ProtectedUnitId,
};
use async_trait::async_trait;
use obzenflow_core::event::payloads::observability_payload::{
    MiddlewareLifecycle, ObservabilityPayload, RetryEvent, RetryExhaustionCause,
};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::{
    ChainEventContent, ChainEventFactory, EffectFailureCode, EffectFailureSource, RetryDisposition,
};
use obzenflow_core::{ChainEvent, MiddlewareContextKey, StageId, Ulid, WriterId};
use obzenflow_runtime::effects::{
    EffectBoundary, EffectBoundaryOutcome, EffectCursor, EffectDescriptorHash, EffectError,
    EffectExecution, EffectExecutor, EffectIdentity, EffectSafety,
};
use obzenflow_runtime::stages::common::{
    control_strategies::BackoffStrategy, BoundaryStopReceiver,
};
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Barrier, Notify};

fn identity_for(effect_type: &'static str) -> EffectIdentity {
    EffectIdentity {
        effect_type,
        safety: EffectSafety::Idempotent,
        cursor: EffectCursor::new("test_flow", "test_stage", 1, 0),
        idempotency_key: None,
    }
}

fn data_event() -> ChainEvent {
    ChainEventFactory::data_event(WriterId::from(StageId::new()), "test.input", json!({}))
}

fn effect_retry_owner(
    max_attempts: u32,
    delay: Duration,
    max_single_delay: Duration,
) -> BoundaryRetryOwner {
    let stage_id = StageId::from_ulid(Ulid::from(0x115_0002_u128));
    let effect_type = EffectTypeKey::from("effect.retry_test");
    let declaration = MiddlewareDeclaration::control(
        "test.effect_retry_owner",
        vec![MiddlewareSurfaceKind::Effect],
    );
    let surface = MiddlewareSurface::Effect(EffectSurface {
        stage_id,
        effect_type: effect_type.clone(),
    });
    let protected_unit = ProtectedUnitId {
        stage_id,
        unit: ProtectedUnit::Effect(EffectUnitId { effect_type }),
    };
    let origin = MiddlewareOrigin::Stage;
    let request = MiddlewareAttachmentRequest {
        surface: &surface,
        protected_unit: &protected_unit,
        origin: &origin,
        declaration_index: MiddlewareDeclarationIndex::effect_policy(0),
    };
    BoundaryRetryOwner {
        attachment_id: MiddlewareAttachmentId::from_declaration_and_request(&declaration, &request),
        stage_id,
        writer_id: WriterId::from(stage_id),
        protected_unit_label: "effect.retry_test".to_string(),
        sink_configured_target_id: None,
        policy: BoundaryRetryPolicy {
            max_attempts: NonZeroU32::new(max_attempts).expect("test attempts are non-zero"),
            backoff: BackoffStrategy::Fixed { delay },
            max_single_delay,
            max_total_wall_time: Duration::from_secs(30),
        },
    }
}

fn retry_event(event: &ChainEvent) -> &RetryEvent {
    match &event.content {
        ChainEventContent::Observability(ObservabilityPayload::Middleware(
            MiddlewareLifecycle::Retry(retry),
        )) => retry,
        other => panic!("expected retry lifecycle row, got {other:?}"),
    }
}

struct EffectRetryContextMarker;

impl MiddlewareContextKey for EffectRetryContextMarker {
    type Value = ();
    const LABEL: &'static str = "effect_retry_test_marker";
}

struct EffectRetryOwnerPolicy {
    owner: BoundaryRetryOwner,
    admissions: Arc<AtomicUsize>,
    log: Arc<Mutex<Vec<String>>>,
    observed: Option<Arc<Notify>>,
}

#[async_trait]
impl EffectPolicy for EffectRetryOwnerPolicy {
    fn label(&self) -> &'static str {
        "effect_retry_owner"
    }

    async fn admit(&self, ctx: &mut MiddlewareContext) -> PolicyAdmission {
        assert!(
            !ctx.contains::<EffectRetryContextMarker>(),
            "each effect attempt must receive a fresh middleware context"
        );
        ctx.insert::<EffectRetryContextMarker>(());
        let attempt = self.admissions.fetch_add(1, Ordering::SeqCst) + 1;
        self.log.lock().unwrap().push(format!("admit:{attempt}"));
        PolicyAdmission::Admit
    }

    fn commit_execution(&self, _ctx: &mut MiddlewareContext) {
        self.log.lock().unwrap().push("commit".to_string());
    }

    fn observe(&self, attempt: &EffectAttemptOutcome<'_>, _ctx: &mut MiddlewareContext) {
        let outcome = match attempt {
            EffectAttemptOutcome::Executed(Ok(_)) => "ok",
            EffectAttemptOutcome::Executed(Err(_)) => "error",
            EffectAttemptOutcome::SkippedBy(_) => "skipped",
            EffectAttemptOutcome::RejectedBy(_) => "rejected",
        };
        self.log.lock().unwrap().push(format!("observe:{outcome}"));
        if let Some(observed) = &self.observed {
            observed.notify_one();
        }
    }

    fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
        Some(self.owner.clone())
    }
}

struct SequenceEffectExecutor {
    outcomes: VecDeque<Result<Vec<ChainEvent>, EffectError>>,
    ordinals: Arc<Mutex<Vec<u32>>>,
}

struct ControlledEffectExecutor {
    outcome: Option<Result<Vec<ChainEvent>, EffectError>>,
    ordinals: Arc<Mutex<Vec<u32>>>,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

struct EffectFutureDropCounter(Arc<AtomicUsize>);

impl Drop for EffectFutureDropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct PendingEffectExecutor {
    ordinals: Arc<Mutex<Vec<u32>>>,
    started: Arc<Notify>,
    dropped: Arc<AtomicUsize>,
}

struct LockstepEffectExecutor {
    ordinals: Arc<Mutex<Vec<u32>>>,
    attempt_barrier: Arc<Barrier>,
}

impl EffectExecutor for PendingEffectExecutor {
    fn attempt(&mut self, attempt: NonZeroU32) -> EffectExecution {
        self.ordinals.lock().unwrap().push(attempt.get());
        let started = self.started.clone();
        let dropped = self.dropped.clone();
        Box::pin(async move {
            let _drop_counter = EffectFutureDropCounter(dropped);
            started.notify_one();
            std::future::pending::<Result<Vec<ChainEvent>, EffectError>>().await
        })
    }
}

impl EffectExecutor for LockstepEffectExecutor {
    fn attempt(&mut self, attempt: NonZeroU32) -> EffectExecution {
        self.ordinals.lock().unwrap().push(attempt.get());
        let attempt_barrier = self.attempt_barrier.clone();
        Box::pin(async move {
            // Both logical invocations must reach each physical ordinal
            // before either can finish it. A sequential implementation
            // deadlocks here, so this is an honest interleaving proof.
            attempt_barrier.wait().await;
            if attempt == NonZeroU32::MIN {
                Err(EffectError::TransientExecution(
                    "lockstep transient".to_string(),
                ))
            } else {
                Ok(Vec::new())
            }
        })
    }
}

impl EffectExecutor for ControlledEffectExecutor {
    fn attempt(&mut self, attempt: NonZeroU32) -> EffectExecution {
        self.ordinals.lock().unwrap().push(attempt.get());
        let outcome = self
            .outcome
            .take()
            .expect("controlled effect executor runs exactly once");
        let started = self.started.clone();
        let release = self.release.clone();
        Box::pin(async move {
            started.notify_one();
            release.notified().await;
            outcome
        })
    }
}

struct DelayedAdmissionEffectPolicy {
    owner: BoundaryRetryOwner,
    admissions: Arc<AtomicUsize>,
    started: Arc<Notify>,
    delay: Duration,
}

#[async_trait]
impl EffectPolicy for DelayedAdmissionEffectPolicy {
    fn label(&self) -> &'static str {
        "delayed_admission_retry_owner"
    }

    async fn admit(&self, _ctx: &mut MiddlewareContext) -> PolicyAdmission {
        self.admissions.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        tokio::time::sleep(self.delay).await;
        PolicyAdmission::Admit
    }

    fn observe(&self, _attempt: &EffectAttemptOutcome<'_>, _ctx: &mut MiddlewareContext) {}

    fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
        Some(self.owner.clone())
    }
}

impl EffectExecutor for SequenceEffectExecutor {
    fn attempt(&mut self, attempt: NonZeroU32) -> EffectExecution {
        self.ordinals.lock().unwrap().push(attempt.get());
        let outcome = self
            .outcomes
            .pop_front()
            .expect("effect test executor has an outcome for every attempt");
        Box::pin(async move { outcome })
    }
}

fn retry_effect_boundary(
    owner: BoundaryRetryOwner,
    admissions: Arc<AtomicUsize>,
    log: Arc<Mutex<Vec<String>>>,
) -> PerEffectPolicyBoundary {
    retry_effect_boundary_with_observer(owner, admissions, log, None)
}

fn retry_effect_boundary_with_observer(
    owner: BoundaryRetryOwner,
    admissions: Arc<AtomicUsize>,
    log: Arc<Mutex<Vec<String>>>,
    observed: Option<Arc<Notify>>,
) -> PerEffectPolicyBoundary {
    let mut chains = HashMap::new();
    chains.insert(
        "effect.retry_test",
        Arc::new(vec![EffectPolicyAttachment::neutral(Arc::new(
            EffectRetryOwnerPolicy {
                owner,
                admissions,
                log,
                observed,
            },
        ))]),
    );
    PerEffectPolicyBoundary::new(chains)
}

fn ok_execute() -> EffectExecution {
    Box::pin(async { Ok(Vec::new()) })
}

fn failing_execute() -> EffectExecution {
    Box::pin(async { Err(EffectError::Execution("simulated_failure".to_string())) })
}

struct RecordingPolicy {
    label: &'static str,
    observed: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl EffectPolicy for RecordingPolicy {
    fn label(&self) -> &'static str {
        self.label
    }

    async fn admit(&self, _ctx: &mut MiddlewareContext) -> PolicyAdmission {
        PolicyAdmission::Admit
    }

    fn observe(&self, attempt: &EffectAttemptOutcome<'_>, _ctx: &mut MiddlewareContext) {
        let kind = match attempt {
            EffectAttemptOutcome::Executed(Ok(_)) => "executed_ok".to_string(),
            EffectAttemptOutcome::Executed(Err(_)) => "executed_err".to_string(),
            EffectAttemptOutcome::SkippedBy(label) => format!("skipped_by:{label}"),
            EffectAttemptOutcome::RejectedBy(cause) => {
                format!("rejected_by:{}", cause.source)
            }
        };
        self.observed.lock().unwrap().push(kind);
    }
}

struct RejectingPolicy;

#[async_trait]
impl EffectPolicy for RejectingPolicy {
    fn label(&self) -> &'static str {
        "test.rejecting"
    }

    async fn admit(&self, _ctx: &mut MiddlewareContext) -> PolicyAdmission {
        PolicyAdmission::Reject(MiddlewareAbortCause {
            source: EffectFailureSource::new("test.rejecting"),
            code: EffectFailureCode::new("rejected"),
            message: "test rejection".to_string(),
            retry: RetryDisposition::NotRetryable,
            event: None,
        })
    }

    fn observe(&self, _attempt: &EffectAttemptOutcome<'_>, _ctx: &mut MiddlewareContext) {}
}

/// FLOWIP-120c gap G8: finalization is structural. A policy that
/// admitted observes the attempt even when a later policy rejects it.
mod disposition;
mod factory;
mod onion;
mod recovery;
mod stop;
