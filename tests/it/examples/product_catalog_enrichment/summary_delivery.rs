// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Checked materialization and admission proofs for the application's summary
//! policy.

use super::example::summary_delivery::{SingleSummaryDelivery, SingleSummaryDeliveryPolicy};
use obzenflow::effects::EffectError;
use obzenflow::middleware::{
    EffectAttemptOutcome, EffectPolicy, MiddlewareContext, PolicyAdmission,
};
use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
use obzenflow_adapters::middleware::{
    materialize_factory_checked, EffectSurface, EffectTypeKey, EffectUnitId,
    MiddlewareAttachmentRequest, MiddlewareAttachmentSite, MiddlewareSurface, ProtectedUnit,
    ProtectedUnitId, ProtectedUnitOwner,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::{MiddlewareExecutionScope, StageId};
use obzenflow_runtime::effects::EffectSafety;
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::runtime_config::FlowEffectiveConfig;
use std::sync::Arc;

fn context() -> MiddlewareContext {
    MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary)
}

#[test]
fn summary_control_attaches_effect_surface() {
    let stage_id = StageId::new();
    let config = StageConfig {
        stage_id,
        name: "summary_printer".to_string(),
        flow_name: "product_catalog_enrichment".to_string(),
        cycle_guard: None,
        lineage: Default::default(),
        effective_config: Arc::new(FlowEffectiveConfig::default()),
    };
    let effect_type = EffectTypeKey::from("obzenflow.console.output");
    let surface = MiddlewareSurface::Effect(EffectSurface {
        stage_id,
        effect_type: effect_type.clone(),
        safety: EffectSafety::NonIdempotentAtLeastOnce,
    });
    let protected_unit = ProtectedUnitId {
        stage_id,
        owner: ProtectedUnitOwner::Stage,
        unit: ProtectedUnit::Effect(EffectUnitId { effect_type }),
    };
    let attachment = materialize_factory_checked(
        &SingleSummaryDelivery,
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
    .expect("summary control should pass checked sink materialization");
    assert!(
        attachment.into_effect().is_some(),
        "summary control must materialize an effect policy"
    );
}

#[tokio::test]
async fn failed_attempt_keeps_summary_budget_spent() {
    let policy = SingleSummaryDeliveryPolicy::new();
    assert!(matches!(
        policy.admit(&mut context()).await,
        PolicyAdmission::Admit
    ));
    let failed = Err(EffectError::Execution("console unavailable".to_string()));
    policy.observe(&EffectAttemptOutcome::Executed(&failed), &mut context());
    match policy.admit(&mut context()).await {
        PolicyAdmission::Reject(cause) => {
            assert!(cause.message.contains("only one live delivery attempt"));
        }
        PolicyAdmission::Admit => panic!("a second summary attempt must be rejected"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_summary_admissions_share_one_attempt() {
    let policy = Arc::new(SingleSummaryDeliveryPolicy::new());
    let start = Arc::new(tokio::sync::Barrier::new(3));
    let mut attempts = Vec::new();
    for _ in 0..2 {
        let policy = policy.clone();
        let start = start.clone();
        attempts.push(tokio::spawn(async move {
            start.wait().await;
            matches!(policy.admit(&mut context()).await, PolicyAdmission::Admit)
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
