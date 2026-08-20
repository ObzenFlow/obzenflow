// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Adapter-owned sink-delivery policy boundary (FLOWIP-115b).
//!
//! The runtime sees only `SinkDeliveryBoundary`. This module owns the middleware
//! policy onion hidden behind that seam: admission runs forward, the delivery
//! attempt runs once, and observation runs in reverse over the raw outcome,
//! mirroring the source (FLOWIP-115a) and effect (FLOWIP-120c) boundaries.

use crate::middleware::MiddlewareContext;
use async_trait::async_trait;
use obzenflow_core::event::payloads::observability_payload::{
    MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::{ChainEvent, MiddlewareExecutionScope};
use obzenflow_runtime::stages::common::handlers::SinkConsumeReport;
use obzenflow_runtime::stages::sink::journal_sink::{
    SinkDeliveryAdmission, SinkDeliveryAttemptOutcome, SinkDeliveryBoundary, SinkDeliveryPermit,
    SinkDeliveryRejection, SinkPolicyEvidence, SinkPolicyEvidenceBatch, SinkPolicyEvidenceError,
};
use std::sync::Arc;

/// RAII guard returned by sink-policy admission for a reserved resource (such as
/// a half-open probe slot), held by the boundary across the delivery attempt.
pub trait SinkAdmissionGuard: Send + Sync {}

impl<T: Send + Sync> SinkAdmissionGuard for T {}

/// Admission decision from one sink-delivery policy.
pub enum SinkAdmission {
    /// Admit the delivery, optionally holding a guard across the attempt.
    Admit(Option<Box<dyn SinkAdmissionGuard>>),
    /// Reject before delivery. The supervisor maps this to a failed delivery
    /// receipt, never a successful `Noop`.
    Reject { reason: String },
}

/// Raw sink-delivery outcome shown independently to each admitted policy.
pub enum SinkDeliveryPolicyOutcome<'a> {
    /// The handler ran and returned a consume report.
    Delivered { report: &'a SinkConsumeReport },
    /// The handler errored or panicked.
    Failed,
    /// A later policy rejected before delivery; the protected call never went out.
    RejectedBy {
        policy: &'static str,
        reason: &'a str,
    },
}

/// Sink-shaped policy context. It owns the observability outbox returned by the
/// boundary report and never crosses into the runtime supervisor.
pub struct SinkPolicyCtx {
    middleware_ctx: MiddlewareContext,
    evidence: SinkPolicyEvidenceBatch,
}

impl Default for SinkPolicyCtx {
    fn default() -> Self {
        Self::new()
    }
}

impl SinkPolicyCtx {
    pub fn new() -> Self {
        Self {
            middleware_ctx: MiddlewareContext::with_scope(
                MiddlewareExecutionScope::LiveSinkDeliveryBoundary,
            ),
            evidence: SinkPolicyEvidenceBatch::new(),
        }
    }

    pub fn try_push_evidence(
        &mut self,
        evidence: SinkPolicyEvidence,
    ) -> Result<(), SinkPolicyEvidenceError> {
        self.evidence.try_push(evidence)
    }

    pub(crate) fn write_control_event(&mut self, event: ChainEvent) {
        self.middleware_ctx.write_control_event(event);
    }

    fn capture_internal_events(&mut self) {
        for event in self.middleware_ctx.take_control_events() {
            let evidence = match event.content {
                ChainEventContent::Observability(ObservabilityPayload::Middleware(
                    MiddlewareLifecycle::CircuitBreaker(event),
                )) => SinkPolicyEvidence::circuit_breaker(event),
                ChainEventContent::Observability(ObservabilityPayload::Middleware(
                    MiddlewareLifecycle::RateLimiter(event),
                )) => SinkPolicyEvidence::rate_limiter(event),
                _ => {
                    tracing::warn!(
                        "discarding event outside the closed sink-policy evidence vocabulary"
                    );
                    continue;
                }
            };
            match evidence.and_then(|evidence| self.evidence.try_push(evidence)) {
                Ok(()) => {}
                Err(error) => tracing::warn!(%error, "discarding invalid sink-policy evidence"),
            }
        }
    }

    fn take_evidence(&mut self) -> SinkPolicyEvidenceBatch {
        std::mem::take(&mut self.evidence)
    }

    pub(crate) fn middleware_context_mut(&mut self) -> &mut MiddlewareContext {
        &mut self.middleware_ctx
    }
}

/// A sink-delivery resilience policy behind the adapter-owned boundary.
///
/// The boundary owns typed delivery identity and attempt facts; policies see
/// only the control outcome facts they actually consume. Future sink policies
/// may extend this contract with a same-slice reader and proof.
#[async_trait]
pub trait SinkPolicy: Send + Sync {
    fn label(&self) -> &'static str;

    async fn admit(&self, ctx: &mut SinkPolicyCtx) -> SinkAdmission;

    fn observe(&self, outcome: &SinkDeliveryPolicyOutcome<'_>, ctx: &mut SinkPolicyCtx);
}

/// Sink-delivery boundary backed by a declared-order policy chain.
pub struct PerSinkDeliveryPolicyBoundary {
    policies: Arc<Vec<Arc<dyn SinkPolicy>>>,
}

impl PerSinkDeliveryPolicyBoundary {
    pub fn new(policies: Vec<Arc<dyn SinkPolicy>>) -> Self {
        Self {
            policies: Arc::new(policies),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.policies.is_empty()
    }
}

type SinkAdmitGuard = Option<Box<dyn SinkAdmissionGuard>>;

struct PerSinkDeliveryPermit {
    admitted: Vec<(Arc<dyn SinkPolicy>, SinkAdmitGuard)>,
    ctx: SinkPolicyCtx,
}

impl SinkDeliveryPermit for PerSinkDeliveryPermit {
    fn observe(
        mut self: Box<Self>,
        outcome: &SinkDeliveryAttemptOutcome,
    ) -> SinkPolicyEvidenceBatch {
        let policy_outcome = match outcome {
            SinkDeliveryAttemptOutcome::Delivered(Ok(report)) => {
                SinkDeliveryPolicyOutcome::Delivered { report }
            }
            SinkDeliveryAttemptOutcome::Delivered(Err(_))
            | SinkDeliveryAttemptOutcome::Panicked { .. } => SinkDeliveryPolicyOutcome::Failed,
        };
        for (policy, _) in self.admitted.iter().rev() {
            policy.observe(&policy_outcome, &mut self.ctx);
            self.ctx.capture_internal_events();
        }
        self.ctx.take_evidence()
    }
}

#[async_trait]
impl SinkDeliveryBoundary for PerSinkDeliveryPolicyBoundary {
    async fn admit_sink_delivery(&self) -> SinkDeliveryAdmission {
        let mut ctx = SinkPolicyCtx::new();
        let mut admitted: Vec<(Arc<dyn SinkPolicy>, SinkAdmitGuard)> = Vec::new();

        for policy in self.policies.iter() {
            let admission = policy.admit(&mut ctx).await;
            ctx.capture_internal_events();
            match admission {
                SinkAdmission::Admit(guard) => admitted.push((Arc::clone(policy), guard)),
                SinkAdmission::Reject { reason } => {
                    let outcome = SinkDeliveryPolicyOutcome::RejectedBy {
                        policy: policy.label(),
                        reason: &reason,
                    };
                    for (prior, _) in admitted.iter().rev() {
                        prior.observe(&outcome, &mut ctx);
                        ctx.capture_internal_events();
                    }
                    return SinkDeliveryAdmission::Rejected {
                        rejection: SinkDeliveryRejection::new(policy.label(), reason),
                        evidence: ctx.take_evidence(),
                    };
                }
            }
        }

        SinkDeliveryAdmission::Admitted(Box::new(PerSinkDeliveryPermit { admitted, ctx }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::middleware::control::ControlMiddlewareAggregator;
    use crate::middleware::{
        MiddlewareAttachmentRequest, MiddlewareDeclaration, MiddlewareFactory,
        MiddlewareFactoryError, MiddlewareFactoryResult, MiddlewareMaterializationContext,
        MiddlewareOverrideKey, MiddlewareSurface, MiddlewareSurfaceAttachment,
        MiddlewareSurfaceKind, ProtectedUnit, ProtectedUnitId,
    };
    use obzenflow_core::StageId;
    use obzenflow_runtime::pipeline::config::StageConfig;

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

    /// Override-key family marker for the third-party test factory.
    struct ThirdPartyFamily;

    /// A minimal third-party `MiddlewareFactory` that is NOT the circuit breaker
    /// yet attaches through the same carrier by declaring a control surface and
    /// implementing `materialize`.
    struct ThirdPartySinkFactory;

    impl MiddlewareFactory for ThirdPartySinkFactory {
        fn label(&self) -> &'static str {
            "third_party_sink"
        }

        fn override_key(&self) -> MiddlewareOverrideKey {
            MiddlewareOverrideKey::of::<ThirdPartyFamily>("third_party_sink")
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
                MiddlewareSurface::SinkDelivery(_) => Ok(
                    MiddlewareSurfaceAttachment::sink_delivery(Arc::new(AlwaysRejectPolicy)),
                ),
                other => Err(MiddlewareFactoryError::materialization_failed(
                    self.label(),
                    "third_party_test",
                    std::io::Error::other(format!("unsupported surface {:?}", other.kind())),
                )),
            }
        }
    }

    #[tokio::test]
    async fn third_party_factory_uses_the_sink_carrier_and_rejects() {
        // FLOWIP-115b: the carrier is a general abstraction, not breaker-special.
        // A non-breaker factory routes through the same `materialize` path
        // purely by declaring a control surface, and its
        // policy composes in the same onion the breaker uses, short-circuiting
        // delivery on rejection.
        let factory = ThirdPartySinkFactory;
        assert!(factory.declaration().is_control());
        assert!(factory
            .declaration()
            .supports(MiddlewareSurfaceKind::SinkDelivery));
        let config = StageConfig {
            stage_id: StageId::new(),
            name: "third_party".to_string(),
            flow_name: "test".to_string(),
            cycle_guard: None,
            lineage: obzenflow_core::config::LineagePolicy::default(),
            effective_config: std::sync::Arc::new(
                obzenflow_runtime::runtime_config::FlowEffectiveConfig::default(),
            ),
        };
        let control = Arc::new(ControlMiddlewareAggregator::new());
        let surface = MiddlewareSurface::SinkDelivery(crate::middleware::SinkDeliverySurface {
            stage_id: config.stage_id,
            configured_target: None,
        });
        let unit = ProtectedUnitId {
            stage_id: config.stage_id,
            unit: ProtectedUnit::SinkDelivery(crate::middleware::SinkDeliveryUnitId {
                target: crate::middleware::SinkDeliveryTarget::Stage,
            }),
        };
        let request = MiddlewareAttachmentRequest {
            surface: &surface,
            protected_unit: &unit,
            declaration_index: crate::middleware::MiddlewareDeclarationIndex::sink_with(0),
        };
        let policy = crate::middleware::materialize_factory_checked(
            &factory,
            request,
            &config,
            obzenflow_core::event::context::StageType::Sink,
            &control,
        )
        .expect("third-party factory should materialize a sink policy");
        let policy = policy
            .into_sink_delivery()
            .expect("expected a SinkDelivery attachment from the third-party factory");

        let boundary = PerSinkDeliveryPolicyBoundary::new(vec![policy]);
        let admission = boundary.admit_sink_delivery().await;

        match admission {
            SinkDeliveryAdmission::Rejected { rejection, .. } => {
                assert_eq!(rejection.policy(), "third_party_reject");
                assert_eq!(rejection.reason(), "third party policy");
            }
            _ => panic!("expected the third-party policy to reject delivery"),
        }
    }
}
