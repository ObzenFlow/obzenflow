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
use obzenflow_core::{ChainEvent, MiddlewareExecutionScope};
use obzenflow_runtime::stages::common::handlers::SinkConsumeReport;
use obzenflow_runtime::stages::sink::journal_sink::{
    SinkDeliveryAttemptContext, SinkDeliveryAttemptOutcome, SinkDeliveryBoundary,
    SinkDeliveryBoundaryOutcome, SinkDeliveryBoundaryReport, SinkDeliveryExecutor,
    SinkDeliveryIdentity, SinkDeliveryRejection,
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
        }
    }

    pub fn write_control_event(&mut self, event: ChainEvent) {
        self.middleware_ctx.write_control_event(event);
    }

    pub fn take_control_events(&mut self) -> Vec<ChainEvent> {
        self.middleware_ctx.take_control_events()
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

#[async_trait]
impl SinkDeliveryBoundary for PerSinkDeliveryPolicyBoundary {
    async fn around_sink_delivery(
        &self,
        _: &SinkDeliveryIdentity,
        _: &SinkDeliveryAttemptContext,
        execute: &mut dyn SinkDeliveryExecutor,
    ) -> SinkDeliveryBoundaryReport {
        if self.policies.is_empty() {
            return SinkDeliveryBoundaryReport {
                outcome: SinkDeliveryBoundaryOutcome::Attempted(execute.attempt().await),
                control_events: Vec::new(),
            };
        }

        let mut ctx = SinkPolicyCtx::new();
        let mut admitted: Vec<(&Arc<dyn SinkPolicy>, SinkAdmitGuard)> = Vec::new();

        for policy in self.policies.iter() {
            match policy.admit(&mut ctx).await {
                SinkAdmission::Admit(guard) => admitted.push((policy, guard)),
                SinkAdmission::Reject { reason } => {
                    let outcome = SinkDeliveryPolicyOutcome::RejectedBy {
                        policy: policy.label(),
                        reason: &reason,
                    };
                    for (prior, _) in admitted.iter().rev() {
                        prior.observe(&outcome, &mut ctx);
                    }
                    return SinkDeliveryBoundaryReport {
                        outcome: SinkDeliveryBoundaryOutcome::Rejected(SinkDeliveryRejection {
                            policy: policy.label().to_string(),
                            reason,
                        }),
                        control_events: ctx.take_control_events(),
                    };
                }
            }
        }

        let attempt_outcome = execute.attempt().await;
        let policy_outcome = match &attempt_outcome {
            SinkDeliveryAttemptOutcome::Delivered(Ok(report)) => {
                SinkDeliveryPolicyOutcome::Delivered { report }
            }
            SinkDeliveryAttemptOutcome::Delivered(Err(_))
            | SinkDeliveryAttemptOutcome::Panicked { .. } => SinkDeliveryPolicyOutcome::Failed,
        };
        for (policy, _) in admitted.iter().rev() {
            policy.observe(&policy_outcome, &mut ctx);
        }

        SinkDeliveryBoundaryReport {
            outcome: SinkDeliveryBoundaryOutcome::Attempted(attempt_outcome),
            control_events: ctx.take_control_events(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::middleware::control::ControlMiddlewareAggregator;
    use crate::middleware::{
        ControlMiddlewareRole, Middleware, MiddlewareAttachmentRequest, MiddlewareDeclaration,
        MiddlewareFactory, MiddlewareFactoryError, MiddlewareFactoryResult,
        MiddlewareMaterializationContext, MiddlewareOrigin, MiddlewareOverrideKey,
        MiddlewarePlanContribution, MiddlewareSurface, MiddlewareSurfaceAttachment,
        MiddlewareSurfaceKind, ProtectedUnit, ProtectedUnitId, TopologyMiddlewareConfigSlot,
    };
    use obzenflow_core::event::chain_event::ChainEventFactory;
    use obzenflow_core::{StageId, WriterId};
    use obzenflow_runtime::pipeline::config::StageConfig;
    use obzenflow_runtime::stages::sink::journal_sink::{
        SinkDeliveryAttemptContext, SinkDeliveryIdentity, SinkDeliveryTargetId,
    };

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
                MiddlewareSurface::SinkDelivery(_) => Ok(
                    MiddlewareSurfaceAttachment::SinkDelivery(Arc::new(AlwaysRejectPolicy)),
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
        // A non-breaker factory (control_role None) routes through the same
        // `materialize` path purely by declaring a control surface, and its
        // policy composes in the same onion the breaker uses, short-circuiting
        // delivery on rejection.
        let factory = ThirdPartySinkFactory;
        assert!(factory.declaration().is_control());
        assert!(factory
            .declaration()
            .supports(MiddlewareSurfaceKind::SinkDelivery));
        assert_eq!(factory.control_role(), ControlMiddlewareRole::None);

        let config = StageConfig {
            stage_id: StageId::new(),
            name: "third_party".to_string(),
            flow_name: "test".to_string(),
            cycle_guard: None,
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
        let origin = MiddlewareOrigin::Stage;
        let request = MiddlewareAttachmentRequest {
            surface: &surface,
            protected_unit: &unit,
            origin: &origin,
            declaration_index: crate::middleware::MiddlewareDeclarationIndex::resolved(0),
        };
        let materialization = MiddlewareMaterializationContext {
            config: &config,
            control_middleware: &control,
            stage_type: obzenflow_core::event::context::StageType::Sink,
        };
        let attachment = factory
            .materialize(request, &materialization)
            .expect("third-party factory should materialize a sink policy");
        let policy = match attachment {
            MiddlewareSurfaceAttachment::SinkDelivery(policy) => policy,
            _ => panic!("expected a SinkDelivery attachment from the third-party factory"),
        };

        let boundary = PerSinkDeliveryPolicyBoundary::new(vec![policy]);
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "third_party.event",
            serde_json::json!({}),
        );
        let identity = SinkDeliveryIdentity {
            stage_id: config.stage_id,
            target: SinkDeliveryTargetId::Stage,
        };
        let attempt = SinkDeliveryAttemptContext {
            parent_event_id: event.id,
            upstream_stage: None,
            input_position: None,
        };
        let mut executor = PanicExecutor;
        let report = boundary
            .around_sink_delivery(&identity, &attempt, &mut executor)
            .await;

        match report.outcome {
            SinkDeliveryBoundaryOutcome::Rejected(rejection) => {
                assert_eq!(rejection.policy, "third_party_reject");
                assert_eq!(rejection.reason, "third party policy");
            }
            _ => panic!("expected the third-party policy to reject delivery"),
        }
    }
}
