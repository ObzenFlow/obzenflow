// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Checked materialisation proofs for the application's summary policy.

use crate::product_catalog_enrichment::summary_delivery::SingleSummaryDelivery;
use obzenflow::middleware::{SinkAdmission, SinkDeliveryPolicyOutcome, SinkPolicy, SinkPolicyCtx};
use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
use obzenflow_adapters::middleware::{
    materialize_factory_checked, MiddlewareAttachmentRequest, MiddlewareAttachmentSite,
    MiddlewareSurface, ProtectedUnit, ProtectedUnitId, SinkDeliverySurface, SinkDeliveryTarget,
    SinkDeliveryUnitId,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::StageId;
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::runtime_config::FlowEffectiveConfig;
use std::sync::Arc;

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
