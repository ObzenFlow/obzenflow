// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared fixtures for the effect-boundary tests. Everything here is private
//! to `effect::tests`; production code never sees these helpers.

pub(super) use crate::middleware::control::circuit_breaker::{
    failure_rate, CircuitBreaker, CircuitBreakerMiddleware, FailureClassification, Retry,
};
pub(super) use crate::middleware::control::policy::effect::{
    EffectAttemptOutcome, EffectPolicy, EffectPolicyAttachment, PerEffectPolicyBoundary,
    PolicyAdmission,
};
pub(super) use crate::middleware::control::ControlMiddlewareAggregator;
pub(super) use crate::middleware::{MiddlewareAbortCause, MiddlewareContext};
pub(super) use async_trait::async_trait;
pub(super) use obzenflow_core::event::payloads::observability_payload::{
    CircuitBreakerEvent, CircuitBreakerHealthClassification, CircuitBreakerRetryStopReason,
    MiddlewareLifecycle, ObservabilityPayload,
};
pub(super) use obzenflow_core::event::{
    ChainEventContent, ChainEventFactory, EffectFailureCode, EffectFailureSource, RetryDisposition,
};
pub(super) use obzenflow_core::{ChainEvent, StageId, StageKey, WriterId};
pub(super) use obzenflow_runtime::control_plane::ControlPlaneProvider;
pub(super) use obzenflow_runtime::effects::{
    EffectBoundary, EffectBoundaryOutcome, EffectBoundaryReport, EffectCursor, EffectError,
    EffectIdentity, EffectSafety, RepeatableEffectOperation,
};
pub(super) use std::collections::HashMap;
pub(super) use std::sync::atomic::{AtomicUsize, Ordering};
pub(super) use std::sync::{Arc, Mutex};
pub(super) use std::time::Duration;

use crate::middleware::control::rate_limiter::RateLimiterBuilder;
use crate::middleware::{
    EffectSurface, EffectTypeKey, EffectUnitId, MiddlewareAttachmentRequest,
    MiddlewareDeclarationIndex, MiddlewareFactory, MiddlewareMaterializationContext,
    MiddlewareOrigin, MiddlewareSurface, MiddlewareSurfaceAttachment, ProtectedUnit,
    ProtectedUnitId,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::EffectType;
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::runtime_config::{
    materialize_flow_config, DslCandidates, FlowResolutionContext, ResolvedRuntimeConfig,
};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn identity_for(effect_type: &'static str) -> EffectIdentity {
    EffectIdentity {
        effect_type,
        safety: EffectSafety::Idempotent,
        cursor: EffectCursor::new("test_flow", "test_stage", 1, 0),
        idempotency_key: None,
    }
}

pub(super) fn data_event() -> ChainEvent {
    ChainEventFactory::data_event(WriterId::from(StageId::new()), "test.input", json!({}))
}

pub(super) fn ok_execute() -> RepeatableEffectOperation {
    RepeatableEffectOperation::new(|| async { Ok(Vec::new()) })
}

pub(super) fn failing_execute() -> RepeatableEffectOperation {
    RepeatableEffectOperation::new(|| async {
        Err(EffectError::Execution("simulated_failure".to_string()))
    })
}

pub(super) fn test_stage_config(factory: &dyn MiddlewareFactory) -> StageConfig {
    test_stage_config_for_factories(&[factory])
}

pub(super) fn test_stage_config_for_factories(factories: &[&dyn MiddlewareFactory]) -> StageConfig {
    let stage = StageKey::from("retrying_breaker_test");
    let effect_type = EffectType::from("effect.retry");
    let mut dsl = DslCandidates::default();
    for factory in factories {
        for default in factory.dsl_config_defaults() {
            dsl.declare_for_effect(
                default.key_path,
                stage.clone(),
                effect_type.clone(),
                default.value,
            );
        }
    }
    let effective_config = materialize_flow_config(
        &ResolvedRuntimeConfig::builtin_defaults(),
        FlowResolutionContext {
            flow_name: "retrying_breaker_test_flow".to_string(),
            stages: BTreeSet::from([stage.clone()]),
            edges: BTreeSet::new(),
            declared_effects: BTreeMap::from([(stage, BTreeSet::from([effect_type]))]),
            dsl,
        },
    )
    .expect("middleware defaults should resolve for the test effect");

    StageConfig {
        stage_id: StageId::new(),
        name: "retrying_breaker_test".to_string(),
        flow_name: "retrying_breaker_test_flow".to_string(),
        cycle_guard: None,
        lineage: obzenflow_core::config::LineagePolicy::default(),
        effective_config: Arc::new(effective_config),
    }
}

pub(super) fn materialize_effect_attachment(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    control: &Arc<ControlMiddlewareAggregator>,
    declaration_index: usize,
    safety: EffectSafety,
) -> Result<EffectPolicyAttachment, String> {
    let surface = MiddlewareSurface::Effect(EffectSurface {
        stage_id: config.stage_id,
        effect_type: EffectTypeKey::from("effect.retry"),
        safety,
    });
    let protected_unit = ProtectedUnitId {
        stage_id: config.stage_id,
        unit: ProtectedUnit::Effect(EffectUnitId {
            effect_type: EffectTypeKey::from("effect.retry"),
        }),
    };
    let origin = MiddlewareOrigin::Stage;
    let request = MiddlewareAttachmentRequest {
        surface: &surface,
        protected_unit: &protected_unit,
        origin: &origin,
        declaration_index: MiddlewareDeclarationIndex::effect_policy(declaration_index),
    };
    let context = MiddlewareMaterializationContext {
        config,
        control_middleware: control,
        stage_type: StageType::Transform,
    };
    match factory
        .materialize(request, &context)
        .map_err(|error| error.to_string())?
    {
        MiddlewareSurfaceAttachment::Effect(attachment) => Ok(attachment),
        _ => panic!("circuit breaker should materialize an effect attachment"),
    }
}

pub(super) fn retrying_breaker_fixture(
    factory: Box<dyn MiddlewareFactory>,
) -> (
    EffectPolicyAttachment,
    Arc<ControlMiddlewareAggregator>,
    StageId,
) {
    let config = test_stage_config(factory.as_ref());
    let stage_id = config.stage_id;
    let control = Arc::new(ControlMiddlewareAggregator::new());
    let attachment = materialize_effect_attachment(
        factory.as_ref(),
        &config,
        &control,
        0,
        EffectSafety::Idempotent,
    )
    .expect("retrying breaker should materialize for an idempotent effect");
    (attachment, control, stage_id)
}

pub(super) fn retrying_breaker_attachment(
    factory: Box<dyn MiddlewareFactory>,
) -> EffectPolicyAttachment {
    retrying_breaker_fixture(factory).0
}

pub(super) fn rate_limiter_fixture() -> (
    EffectPolicyAttachment,
    Arc<ControlMiddlewareAggregator>,
    StageId,
) {
    let factory = RateLimiterBuilder::new(1_000.0).with_burst(10.0).build();
    let config = test_stage_config(factory.as_ref());
    let stage_id = config.stage_id;
    let control = Arc::new(ControlMiddlewareAggregator::new());
    let attachment = materialize_effect_attachment(
        factory.as_ref(),
        &config,
        &control,
        1,
        EffectSafety::Idempotent,
    )
    .expect("rate limiter should materialize for an effect");
    (attachment, control, stage_id)
}

pub(super) fn boundary_with_chain(chain: Vec<EffectPolicyAttachment>) -> PerEffectPolicyBoundary {
    let mut chains = HashMap::new();
    chains.insert("effect.retry", Arc::new(chain));
    PerEffectPolicyBoundary::new(chains)
}

pub(super) fn retry_events(report: &EffectBoundaryReport) -> Vec<&CircuitBreakerEvent> {
    report
        .control_events
        .iter()
        .filter_map(|event| match &event.content {
            ChainEventContent::Observability(ObservabilityPayload::Middleware(
                MiddlewareLifecycle::CircuitBreaker(event),
            )) if matches!(
                event,
                CircuitBreakerEvent::RetryScheduled { .. }
                    | CircuitBreakerEvent::RetrySucceeded { .. }
                    | CircuitBreakerEvent::RetryExhausted { .. }
                    | CircuitBreakerEvent::RetryStoppedNonRetryable { .. }
            ) =>
            {
                Some(event)
            }
            _ => None,
        })
        .collect()
}

pub(super) fn scheduled_delays(report: &EffectBoundaryReport) -> Vec<u64> {
    retry_events(report)
        .into_iter()
        .filter_map(|event| match event {
            CircuitBreakerEvent::RetryScheduled { delay_ms, .. } => Some(*delay_ms),
            _ => None,
        })
        .collect()
}

pub(super) fn effect_limiter_events(
    control: &ControlMiddlewareAggregator,
    stage_id: StageId,
) -> u64 {
    let snapshots = control.effect_rate_limiter_snapshotters(&stage_id);
    assert_eq!(
        snapshots.len(),
        1,
        "one effect limiter should be registered"
    );
    snapshots[0].1().events_total
}

pub(super) fn scripted_operation(
    calls: Arc<AtomicUsize>,
    result_for_call: impl Fn(usize) -> Result<Vec<ChainEvent>, EffectError> + Send + Sync + 'static,
) -> RepeatableEffectOperation {
    let result_for_call = Arc::new(result_for_call);
    RepeatableEffectOperation::new(move || {
        let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
        let result_for_call = result_for_call.clone();
        async move { result_for_call(call) }
    })
}
