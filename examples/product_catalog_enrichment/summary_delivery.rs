// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Application-owned protection for the one final analytics dashboard.
//!
//! The summary fold emits at EOF, so its sink needs at most one live delivery
//! attempt. A second admission is a wiring or emission error and must not print
//! another dashboard. This is a per-materialisation attempt budget, not durable
//! deduplication: admission spends it even if delivery fails or is cancelled.
//! Strict replay retains the framework's existing console-delivery behaviour.

use async_trait::async_trait;
use obzenflow_adapters::middleware::{
    validate_attachment_request, MiddlewareAttachmentRequest, MiddlewareDeclaration,
    MiddlewareFactory, MiddlewareFactoryError, MiddlewareFactoryResult,
    MiddlewareMaterializationContext, MiddlewareOverrideKey, MiddlewareSurfaceAttachment,
    MiddlewareSurfaceKind, SinkAdmission, SinkDeliveryPolicyOutcome, SinkPolicy, SinkPolicyCtx,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// A fixed one-attempt budget for an emit-on-EOF summary sink.
///
/// It has no resolver-visible settings. Each checked materialisation creates
/// independent policy state, including when the same factory value is reused.
pub struct SingleSummaryDelivery;

impl MiddlewareFactory for SingleSummaryDelivery {
    fn label(&self) -> &'static str {
        "single_summary_delivery"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<Self>(self.label())
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::control(self.label(), vec![MiddlewareSurfaceKind::SinkDelivery])
            .with_control_intent(MiddlewareSurfaceKind::SinkDelivery)
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        validate_attachment_request(&self.declaration(), &request).map_err(|error| {
            MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                error,
            )
        })?;
        Ok(MiddlewareSurfaceAttachment::sink_delivery(Arc::new(
            SingleSummaryDeliveryPolicy {
                admitted: AtomicBool::new(false),
            },
        )))
    }
}

struct SingleSummaryDeliveryPolicy {
    admitted: AtomicBool,
}

#[async_trait]
impl SinkPolicy for SingleSummaryDeliveryPolicy {
    fn label(&self) -> &'static str {
        "single_summary_delivery"
    }

    async fn admit(&self, _context: &mut SinkPolicyCtx) -> SinkAdmission {
        // This flag is the entire budget; it does not publish any other state.
        if self.admitted.swap(true, Ordering::Relaxed) {
            SinkAdmission::Reject {
                reason: "the final analytics summary permits only one live delivery attempt per materialised sink"
                    .to_string(),
            }
        } else {
            SinkAdmission::Admit(None)
        }
    }

    fn observe(&self, _outcome: &SinkDeliveryPolicyOutcome<'_>, _context: &mut SinkPolicyCtx) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
    use obzenflow_adapters::middleware::{
        materialize_factory_checked, MiddlewareAttachmentSite, MiddlewareSurface, ProtectedUnit,
        ProtectedUnitId, SinkDeliverySurface, SinkDeliveryTarget, SinkDeliveryUnitId,
    };
    use obzenflow_core::event::context::StageType;
    use obzenflow_core::StageId;
    use obzenflow_runtime::pipeline::config::StageConfig;
    use obzenflow_runtime::runtime_config::FlowEffectiveConfig;

    fn materialise(factory: &SingleSummaryDelivery) -> Arc<dyn SinkPolicy> {
        let stage_id = StageId::new();
        let config = StageConfig {
            stage_id,
            name: "summary_printer".to_string(),
            flow_name: "product_catalog_enrichment".to_string(),
            cycle_guard: None,
            lineage: Default::default(),
            effective_config: Arc::new(FlowEffectiveConfig::default()),
        };
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
        materialize_factory_checked(
            factory,
            MiddlewareAttachmentRequest {
                stage_key: &config.name,
                surface: &surface,
                protected_unit: &protected_unit,
                authored_site: MiddlewareAttachmentSite::Implementation,
            },
            &config,
            StageType::Sink,
            &Arc::new(ControlMiddlewareAggregator::new()),
        )
        .expect("summary control should pass checked sink materialisation")
        .into_sink_delivery()
        .expect("summary control must materialise a sink-delivery policy")
    }

    #[tokio::test]
    async fn a_failed_attempt_does_not_replenish_the_summary_budget() {
        let policy = materialise(&SingleSummaryDelivery);
        let mut context = SinkPolicyCtx::new();
        assert!(matches!(
            policy.admit(&mut context).await,
            SinkAdmission::Admit(_)
        ));
        policy.observe(&SinkDeliveryPolicyOutcome::Failed, &mut context);
        match policy.admit(&mut context).await {
            SinkAdmission::Reject { reason } => {
                assert!(reason.contains("only one live delivery attempt"));
            }
            SinkAdmission::Admit(_) => panic!("a second summary attempt must be rejected"),
        }
    }

    #[tokio::test]
    async fn reused_factory_materialises_independent_summary_budgets() {
        let factory = SingleSummaryDelivery;
        let first = materialise(&factory);
        let second = materialise(&factory);
        assert!(matches!(
            first.admit(&mut SinkPolicyCtx::new()).await,
            SinkAdmission::Admit(_)
        ));
        assert!(matches!(
            first.admit(&mut SinkPolicyCtx::new()).await,
            SinkAdmission::Reject { .. }
        ));
        assert!(matches!(
            second.admit(&mut SinkPolicyCtx::new()).await,
            SinkAdmission::Admit(_)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_summary_admissions_share_one_attempt() {
        let policy = materialise(&SingleSummaryDelivery);
        let start = Arc::new(tokio::sync::Barrier::new(3));
        let mut attempts = Vec::new();
        for _ in 0..2 {
            let policy = policy.clone();
            let start = start.clone();
            attempts.push(tokio::spawn(async move {
                start.wait().await;
                matches!(
                    policy.admit(&mut SinkPolicyCtx::new()).await,
                    SinkAdmission::Admit(_)
                )
            }));
        }
        start.wait().await;
        let mut admitted = 0;
        for attempt in attempts {
            admitted += usize::from(attempt.await.expect("admission task must complete"));
        }
        assert_eq!(
            admitted, 1,
            "only one concurrent attempt may print a summary"
        );
    }
}
