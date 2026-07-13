// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::super::retry::AttemptDisposition;
use super::*;
use crate::middleware::control::circuit_breaker::{
    CircuitBreakerBuilder, CircuitBreakerMiddleware, CircuitBreakerSinkPolicy,
};
use crate::middleware::control::rate_limiter::RateLimiterFactory;
use crate::middleware::control::ControlMiddlewareAggregator;
use crate::middleware::BoundaryRetryOwner;
use crate::middleware::{
    BoundaryRetryPolicy, ControlMiddlewareRole, Middleware, MiddlewareAttachmentId,
    MiddlewareAttachmentRequest, MiddlewareDeclaration, MiddlewareDeclarationIndex,
    MiddlewareFactory, MiddlewareFactoryError, MiddlewareFactoryResult,
    MiddlewareMaterializationContext, MiddlewareOrigin, MiddlewareOverrideKey,
    MiddlewarePlanContribution, MiddlewareSurface, MiddlewareSurfaceAttachment,
    MiddlewareSurfaceKind, ProtectedUnit, ProtectedUnitId, SinkDeliverySurface, SinkDeliveryTarget,
    SinkDeliveryUnitId, TopologyMiddlewareConfigSlot,
};
use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::observability_payload::{
    MiddlewareLifecycle, ObservabilityPayload, RetryEvent, RetryExhaustionCause, RetryInvocation,
    RetryProtectedUnit,
};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::{ChainEventContent, EventId};
use obzenflow_core::{ChainEvent, MiddlewareContextKey, StageId, Ulid, WriterId};
use obzenflow_runtime::control_plane::ControlPlaneProvider;
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::stages::common::control_strategies::BackoffStrategy;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::SinkConsumeReport;
use obzenflow_runtime::stages::common::BoundaryStopReceiver;
use obzenflow_runtime::stages::sink::journal_sink::{
    SinkDeliveryAttemptOutcome, SinkDeliveryBoundary, SinkDeliveryBoundaryOutcome,
    SinkDeliveryExecutor,
};
use std::collections::VecDeque;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

fn sink_retry_owner(
    max_attempts: u32,
    delay: Duration,
    max_single_delay: Duration,
) -> BoundaryRetryOwner {
    let stage_id = StageId::from_ulid(Ulid::from(0x115_0003_u128));
    let declaration = MiddlewareDeclaration::control(
        "test.sink_retry_owner",
        vec![MiddlewareSurfaceKind::SinkDelivery],
    );
    let surface = MiddlewareSurface::SinkDelivery(SinkDeliverySurface {
        stage_id,
        configured_target: None,
    });
    let protected_unit = ProtectedUnitId {
        stage_id,
        unit: ProtectedUnit::SinkDelivery(SinkDeliveryUnitId {
            target: SinkDeliveryTarget::Stage,
        }),
    };
    let origin = MiddlewareOrigin::Stage;
    let request = MiddlewareAttachmentRequest {
        surface: &surface,
        protected_unit: &protected_unit,
        origin: &origin,
        declaration_index: MiddlewareDeclarationIndex::resolved(0),
    };
    BoundaryRetryOwner {
        attachment_id: MiddlewareAttachmentId::from_declaration_and_request(&declaration, &request),
        stage_id,
        writer_id: WriterId::from(stage_id),
        protected_unit_label: "payments_sink".to_string(),
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

struct SinkRetryContextMarker;

impl MiddlewareContextKey for SinkRetryContextMarker {
    type Value = ();
    const LABEL: &'static str = "sink_retry_test_marker";
}

struct SinkRetryOwnerPolicy {
    owner: BoundaryRetryOwner,
    admissions: Arc<AtomicUsize>,
    log: Arc<Mutex<Vec<String>>>,
    guard_drops: Option<Arc<AtomicUsize>>,
    observed: Option<Arc<Notify>>,
}

#[async_trait]
impl SinkPolicy for SinkRetryOwnerPolicy {
    fn label(&self) -> &'static str {
        "sink_retry_owner"
    }

    async fn admit(&self, ctx: &mut SinkPolicyCtx) -> SinkAdmission {
        let middleware = ctx.middleware_context_mut();
        assert!(
            !middleware.contains::<SinkRetryContextMarker>(),
            "each sink attempt must receive a fresh policy context"
        );
        middleware.insert::<SinkRetryContextMarker>(());
        let attempt = self.admissions.fetch_add(1, Ordering::SeqCst) + 1;
        self.log.lock().unwrap().push(format!("admit:{attempt}"));
        let guard = self
            .guard_drops
            .as_ref()
            .map(|drops| Box::new(SinkDropCounter(drops.clone())) as Box<dyn SinkAdmissionGuard>);
        SinkAdmission::Admit(guard)
    }

    fn commit_execution(&self, _ctx: &mut SinkPolicyCtx) {
        self.log.lock().unwrap().push("commit".to_string());
    }

    fn observe(&self, outcome: &SinkDeliveryPolicyOutcome<'_>, _ctx: &mut SinkPolicyCtx) {
        let label = match outcome {
            SinkDeliveryPolicyOutcome::Delivered { .. } => "delivered",
            SinkDeliveryPolicyOutcome::Failed => "failed",
            SinkDeliveryPolicyOutcome::RejectedBy { .. } => "rejected",
        };
        self.log.lock().unwrap().push(format!("observe:{label}"));
        if let Some(observed) = &self.observed {
            observed.notify_one();
        }
    }

    fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
        Some(self.owner.clone())
    }
}

struct SequenceSinkExecutor {
    parent_event_id: EventId,
    outcomes: VecDeque<SinkDeliveryAttemptOutcome>,
    calls: Arc<AtomicUsize>,
}

struct ControlledSinkExecutor {
    parent_event_id: EventId,
    outcome: Option<SinkDeliveryAttemptOutcome>,
    calls: Arc<AtomicUsize>,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

struct PendingSinkExecutor {
    parent_event_id: EventId,
    calls: Arc<AtomicUsize>,
    started: Arc<Notify>,
    dropped: Arc<AtomicUsize>,
}

struct SinkDropCounter(Arc<AtomicUsize>);

/// Test-only decorator around a real surface adapter. The wrapped limiter
/// and breaker still own every admission, commit, settlement, metric, and
/// retry decision; this records only the boundary calls made to them.
struct TracedSinkPolicy {
    inner: Arc<dyn SinkPolicy>,
    trace: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl SinkPolicy for TracedSinkPolicy {
    fn label(&self) -> &'static str {
        self.inner.label()
    }

    async fn admit(&self, ctx: &mut SinkPolicyCtx) -> SinkAdmission {
        self.trace
            .lock()
            .unwrap()
            .push(format!("admit:{}", self.inner.label()));
        self.inner.admit(ctx).await
    }

    fn commit_execution(&self, ctx: &mut SinkPolicyCtx) {
        self.trace
            .lock()
            .unwrap()
            .push(format!("commit:{}", self.inner.label()));
        self.inner.commit_execution(ctx);
    }

    fn observe(&self, outcome: &SinkDeliveryPolicyOutcome<'_>, ctx: &mut SinkPolicyCtx) {
        let outcome_label = match outcome {
            SinkDeliveryPolicyOutcome::Delivered { .. } => "delivered",
            SinkDeliveryPolicyOutcome::Failed => "failed",
            SinkDeliveryPolicyOutcome::RejectedBy { .. } => "not_executed",
        };
        self.trace
            .lock()
            .unwrap()
            .push(format!("observe:{}:{outcome_label}", self.inner.label()));
        self.inner.observe(outcome, ctx);
    }

    fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
        self.inner.retry_owner()
    }

    fn recovery_allowed_after_settlement(&self, ctx: &SinkPolicyCtx) -> bool {
        self.inner.recovery_allowed_after_settlement(ctx)
    }
}

impl Drop for SinkDropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl SinkDeliveryExecutor for PendingSinkExecutor {
    fn parent_event_id(&self) -> EventId {
        self.parent_event_id
    }

    async fn attempt(&mut self) -> SinkDeliveryAttemptOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let _drop_counter = SinkDropCounter(self.dropped.clone());
        self.started.notify_one();
        std::future::pending::<SinkDeliveryAttemptOutcome>().await
    }
}

#[async_trait]
impl SinkDeliveryExecutor for SequenceSinkExecutor {
    fn parent_event_id(&self) -> EventId {
        self.parent_event_id
    }

    async fn attempt(&mut self) -> SinkDeliveryAttemptOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.outcomes
            .pop_front()
            .expect("sink test executor has an outcome for every attempt")
    }
}

#[async_trait]
impl SinkDeliveryExecutor for ControlledSinkExecutor {
    fn parent_event_id(&self) -> EventId {
        self.parent_event_id
    }

    async fn attempt(&mut self) -> SinkDeliveryAttemptOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let outcome = self
            .outcome
            .take()
            .expect("controlled sink executor runs exactly once");
        self.started.notify_one();
        self.release.notified().await;
        outcome
    }
}

fn retry_sink_boundary(
    owner: BoundaryRetryOwner,
    admissions: Arc<AtomicUsize>,
    log: Arc<Mutex<Vec<String>>>,
) -> PerSinkDeliveryPolicyBoundary {
    retry_sink_boundary_with_observer_and_guard(owner, admissions, log, None, None)
}

fn retry_sink_boundary_with_guard(
    owner: BoundaryRetryOwner,
    admissions: Arc<AtomicUsize>,
    log: Arc<Mutex<Vec<String>>>,
    guard_drops: Option<Arc<AtomicUsize>>,
) -> PerSinkDeliveryPolicyBoundary {
    retry_sink_boundary_with_observer_and_guard(owner, admissions, log, None, guard_drops)
}

fn retry_sink_boundary_with_observer(
    owner: BoundaryRetryOwner,
    admissions: Arc<AtomicUsize>,
    log: Arc<Mutex<Vec<String>>>,
    observed: Arc<Notify>,
) -> PerSinkDeliveryPolicyBoundary {
    retry_sink_boundary_with_observer_and_guard(owner, admissions, log, Some(observed), None)
}

fn retry_sink_boundary_with_observer_and_guard(
    owner: BoundaryRetryOwner,
    admissions: Arc<AtomicUsize>,
    log: Arc<Mutex<Vec<String>>>,
    observed: Option<Arc<Notify>>,
    guard_drops: Option<Arc<AtomicUsize>>,
) -> PerSinkDeliveryPolicyBoundary {
    PerSinkDeliveryPolicyBoundary::new(vec![Arc::new(SinkRetryOwnerPolicy {
        owner,
        admissions,
        log,
        guard_drops,
        observed,
    })])
}

fn materialize_sink_policy(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    control: &Arc<ControlMiddlewareAggregator>,
    declaration_index: usize,
) -> Arc<dyn SinkPolicy> {
    let surface = MiddlewareSurface::SinkDelivery(SinkDeliverySurface {
        stage_id: config.stage_id,
        configured_target: None,
    });
    let protected_unit = ProtectedUnitId {
        stage_id: config.stage_id,
        unit: ProtectedUnit::SinkDelivery(SinkDeliveryUnitId {
            target: SinkDeliveryTarget::Stage,
        }),
    };
    let origin = MiddlewareOrigin::Stage;
    let request = MiddlewareAttachmentRequest {
        surface: &surface,
        protected_unit: &protected_unit,
        origin: &origin,
        declaration_index: MiddlewareDeclarationIndex::resolved(declaration_index),
    };
    let materialization = MiddlewareMaterializationContext {
        config,
        control_middleware: control,
        stage_type: obzenflow_core::event::context::StageType::Sink,
    };

    match factory
        .materialize(request, &materialization)
        .expect("real sink policy should materialize")
    {
        MiddlewareSurfaceAttachment::SinkDelivery(policy) => policy,
        _ => panic!("factory declared a sink-delivery surface but returned another surface"),
    }
}

fn traced_sink_policy(
    inner: Arc<dyn SinkPolicy>,
    trace: &Arc<Mutex<Vec<String>>>,
) -> Arc<dyn SinkPolicy> {
    Arc::new(TracedSinkPolicy {
        inner,
        trace: trace.clone(),
    })
}

fn sink_stage_config(stage_id: StageId, name: &str) -> StageConfig {
    StageConfig {
        stage_id,
        name: name.to_string(),
        flow_name: "retry_policy_order_test".to_string(),
        cycle_guard: None,
        lineage: obzenflow_core::config::LineagePolicy::default(),
        resolved_policies: Default::default(),
    }
}

/// A third-party (non-breaker) sink policy that always rejects.
struct AlwaysRejectPolicy;

#[async_trait]
impl SinkPolicy for AlwaysRejectPolicy {
    fn label(&self) -> &'static str {
        "third_party_reject"
    }

    async fn admit(&self, _ctx: &mut SinkPolicyCtx) -> SinkAdmission {
        SinkAdmission::Reject {
            reason: "third party policy".to_string(),
        }
    }

    fn observe(&self, _outcome: &SinkDeliveryPolicyOutcome<'_>, _ctx: &mut SinkPolicyCtx) {}
}

/// An executor that must never run when a policy rejects before delivery.
struct PanicExecutor;

#[async_trait]
impl SinkDeliveryExecutor for PanicExecutor {
    fn parent_event_id(&self) -> obzenflow_core::EventId {
        obzenflow_core::EventId::new()
    }

    async fn attempt(&mut self) -> SinkDeliveryAttemptOutcome {
        panic!("executor must not run when a sink policy rejects");
    }
}

/// Override-key family marker for the third-party test factory.
struct ThirdPartyFamily;

/// A minimal third-party `MiddlewareFactory` that is NOT the circuit breaker
/// (its `control_role` is `None`) yet attaches through the same carrier by
/// declaring a control surface and implementing `materialize`.
struct ThirdPartySinkFactory;

impl MiddlewareFactory for ThirdPartySinkFactory {
    fn label(&self) -> &'static str {
        "third_party_sink"
    }

    fn control_role(&self) -> ControlMiddlewareRole {
        ControlMiddlewareRole::None
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<ThirdPartyFamily>("third_party_sink")
    }

    fn plan_contribution(&self) -> MiddlewarePlanContribution {
        MiddlewarePlanContribution::None
    }

    fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
        None
    }

    fn create(
        &self,
        _config: &StageConfig,
        _control: Arc<ControlMiddlewareAggregator>,
    ) -> MiddlewareFactoryResult<Box<dyn Middleware>> {
        // Hook-bound only: it is placed via `materialize`, never the legacy
        // handler-shell `create` path.
        Err(MiddlewareFactoryError::not_hook_bound(self.label()))
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::control(self.label(), vec![MiddlewareSurfaceKind::SinkDelivery])
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        _ctx: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        match request.surface {
            MiddlewareSurface::SinkDelivery(_) => Ok(MiddlewareSurfaceAttachment::SinkDelivery(
                Arc::new(AlwaysRejectPolicy),
            )),
            other => Err(MiddlewareFactoryError::materialization_failed(
                self.label(),
                "third_party_test",
                std::io::Error::other(format!("unsupported surface {:?}", other.kind())),
            )),
        }
    }
}

mod classification;
mod composition;
mod retry_behavior;
mod signals;
