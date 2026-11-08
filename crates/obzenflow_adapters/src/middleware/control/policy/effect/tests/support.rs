// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared fixtures for the effect-boundary tests. Everything here is private
//! to `effect::tests`; production code never sees these helpers.

pub(super) use crate::middleware::control::policy::effect::{
    EffectAttemptOutcome, EffectPolicy, EffectPolicyAttachment, PerEffectPolicyBoundary,
    PolicyAdmission,
};
pub(super) use crate::middleware::control::resilience::FinalAdmissionTestGate;
pub(super) use crate::middleware::control::ControlMiddlewareAggregator;
pub(super) use crate::middleware::{MiddlewareAbortCause, MiddlewareContext};
pub(super) use async_trait::async_trait;
pub(super) use obzenflow_core::config::{ConfigAddress, ConfigSource};
pub(super) use obzenflow_core::event::payloads::execution_payload::{
    CircuitBreakerFact, ExecutionPayload, RecoveryFact, RetryStopReason,
};
pub(super) use obzenflow_core::event::ChainPayload;
pub(super) use obzenflow_core::event::{
    ChainEventFactory, EffectFailureCode, EffectFailureSource, RetryDisposition,
};
pub(super) use obzenflow_core::{ChainEvent, StageId, StageKey, WriterId};
pub(super) use obzenflow_runtime::control_plane::ControlPlaneProvider;
pub(super) use obzenflow_runtime::effects::{
    EffectBoundary, EffectBoundaryOutcome, EffectBoundaryReport, EffectCursor, EffectError,
    EffectIdentity, EffectSafety, RepeatableEffectOperation,
};
pub(super) use obzenflow_runtime::runtime_config::{
    CandidateSet, ConfigValue, ResolvedRuntimeConfig, ScopedCandidate,
};
pub(super) use std::collections::HashMap;
pub(super) use std::sync::atomic::{AtomicUsize, Ordering};
pub(super) use std::sync::{Arc, Mutex};
pub(super) use std::time::Duration;

pub(super) use crate::middleware::ProtectedUnitOwner;
use crate::middleware::{
    materialize_factory_checked, EffectSurface, EffectTypeKey, EffectUnitId,
    MiddlewareAttachmentRequest, MiddlewareAttachmentSite, MiddlewareFactory, MiddlewareSurface,
    ProtectedUnit, ProtectedUnitId,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::EffectType;
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::runtime_config::{
    materialize_flow_config, ConfigResolveError, DslCandidates, FlowEffectiveConfig,
    FlowResolutionContext,
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
    ChainEventFactory::data_event(
        WriterId::from(StageId::new()),
        "test.input",
        std::num::NonZeroU32::MIN,
        json!({}),
    )
}

pub(super) fn ok_execute() -> RepeatableEffectOperation {
    RepeatableEffectOperation::new(|| async { Ok(Vec::new()) })
}

pub(super) fn test_stage_config(factory: &dyn MiddlewareFactory) -> StageConfig {
    test_stage_config_for_factories(&[factory])
}

pub(super) fn test_stage_config_for_factories(factories: &[&dyn MiddlewareFactory]) -> StageConfig {
    test_stage_config_for_factories_with_snapshot(
        factories,
        &ResolvedRuntimeConfig::builtin_defaults(),
    )
}

pub(super) fn test_stage_config_for_factories_with_snapshot(
    factories: &[&dyn MiddlewareFactory],
    snapshot: &ResolvedRuntimeConfig,
) -> StageConfig {
    let effective_config = test_effective_config_for_factories_with_snapshot(factories, snapshot)
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

pub(super) fn test_effective_config_for_factories_with_snapshot(
    factories: &[&dyn MiddlewareFactory],
    snapshot: &ResolvedRuntimeConfig,
) -> Result<FlowEffectiveConfig, ConfigResolveError> {
    let stage = StageKey::from("retrying_breaker_test");
    let effect_type = EffectType::from("effect.retry");
    let mut dsl = DslCandidates::default();
    for factory in factories {
        for key_path in factory.consumed_config_keys() {
            dsl.declare_effect_consumption(key_path, stage.clone(), effect_type.clone());
        }
        for default in factory.dsl_config_defaults() {
            dsl.declare_for_effect(
                default.key_path,
                stage.clone(),
                effect_type.clone(),
                default.value,
            );
        }
    }
    materialize_flow_config(
        snapshot,
        FlowResolutionContext {
            flow_name: "retrying_breaker_test_flow".to_string(),
            stages: BTreeSet::from([stage.clone()]),
            edges: BTreeSet::new(),
            declared_effects: BTreeMap::from([(stage, BTreeSet::from([effect_type]))]),
            dsl,
        },
    )
}

pub(super) fn materialize_effect_attachment(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    control: &Arc<ControlMiddlewareAggregator>,
    safety: EffectSafety,
) -> Result<EffectPolicyAttachment, String> {
    materialize_owned_effect_attachment(factory, config, control, safety, ProtectedUnitOwner::Stage)
}

pub(super) fn materialize_owned_effect_attachment(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    control: &Arc<ControlMiddlewareAggregator>,
    safety: EffectSafety,
    owner: ProtectedUnitOwner,
) -> Result<EffectPolicyAttachment, String> {
    let surface = MiddlewareSurface::Effect(EffectSurface {
        stage_id: config.stage_id,
        effect_type: EffectTypeKey::from("effect.retry"),
        safety,
    });
    let protected_unit = ProtectedUnitId {
        stage_id: config.stage_id,
        owner,
        unit: ProtectedUnit::Effect(EffectUnitId {
            effect_type: EffectTypeKey::from("effect.retry"),
        }),
    };
    let request = MiddlewareAttachmentRequest {
        stage_key: &config.name,
        surface: &surface,
        protected_unit: &protected_unit,
        authored_site: MiddlewareAttachmentSite::Effect,
    };
    materialize_factory_checked(factory, request, config, StageType::Transform, control)
        .map_err(|error| error.to_string())?
        .into_effect()
        .ok_or_else(|| "circuit breaker should materialize an effect attachment".to_string())
}

pub(super) fn boundary_with_chain(chain: Vec<EffectPolicyAttachment>) -> PerEffectPolicyBoundary {
    let mut chains = HashMap::new();
    chains.insert("effect.retry", Arc::new(chain));
    PerEffectPolicyBoundary::new(chains)
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

pub(super) fn materialized_resilience(
    factory: Box<dyn MiddlewareFactory>,
) -> (
    EffectPolicyAttachment,
    Arc<ControlMiddlewareAggregator>,
    StageId,
) {
    materialized_resilience_with_snapshot(factory, &ResolvedRuntimeConfig::builtin_defaults())
}

pub(super) fn materialized_resilience_with_snapshot(
    factory: Box<dyn MiddlewareFactory>,
    snapshot: &ResolvedRuntimeConfig,
) -> (
    EffectPolicyAttachment,
    Arc<ControlMiddlewareAggregator>,
    StageId,
) {
    let config = test_stage_config_for_factories_with_snapshot(&[factory.as_ref()], snapshot);
    let stage_id = config.stage_id;
    let control = Arc::new(ControlMiddlewareAggregator::new());
    let attachment = materialize_effect_attachment(
        factory.as_ref(),
        &config,
        &control,
        EffectSafety::Idempotent,
    )
    .expect("resilience aggregate should materialize for an idempotent effect");
    (attachment, control, stage_id)
}

pub(super) fn file_effect_snapshot(entries: &[(&str, ConfigValue)]) -> ResolvedRuntimeConfig {
    let mut candidates = CandidateSet::default();
    for (key_path, value) in entries {
        candidates
            .admit(ScopedCandidate {
                key_path: (*key_path).to_string(),
                address: ConfigAddress::effect("retrying_breaker_test", "effect.retry"),
                source: ConfigSource::File,
                value: value.clone(),
            })
            .expect("test file candidate should be valid");
    }
    ResolvedRuntimeConfig::new(candidates)
}

pub(super) fn identity_at(input_seq: u64) -> EffectIdentity {
    let mut identity = identity_for("effect.retry");
    identity.cursor = EffectCursor::new("test_flow", "test_stage", input_seq, 0);
    identity
}

pub(super) fn retry_delays(report: &EffectBoundaryReport) -> Vec<u64> {
    recovery_facts(report, |fact| match fact {
        RecoveryFact::RetryScheduled { delay_ms, .. } => Some(*delay_ms),
        _ => None,
    })
}

pub(super) fn scheduled_attempts(report: &EffectBoundaryReport) -> Vec<u32> {
    recovery_facts(report, |fact| match fact {
        RecoveryFact::RetryScheduled { next_attempt, .. } => Some(*next_attempt),
        _ => None,
    })
}

pub(super) fn retry_exhaustions(report: &EffectBoundaryReport) -> Vec<(u32, RetryStopReason)> {
    recovery_facts(report, |fact| match fact {
        RecoveryFact::RetryExhausted {
            total_attempts,
            reason,
            ..
        } => Some((*total_attempts, *reason)),
        _ => None,
    })
}

fn recovery_facts<T>(
    report: &EffectBoundaryReport,
    select: impl Fn(&RecoveryFact) -> Option<T>,
) -> Vec<T> {
    report
        .control_events
        .iter()
        .filter_map(|event| match &event.payload {
            ChainPayload::Execution(ExecutionPayload::Recovery(fact)) => select(fact),
            _ => None,
        })
        .collect()
}

pub(super) fn settled_attempts(report: &EffectBoundaryReport) -> Vec<u32> {
    report
        .control_events
        .iter()
        .filter_map(|event| match &event.payload {
            ChainPayload::Execution(ExecutionPayload::CircuitBreaker(
                CircuitBreakerFact::AttemptSettled { attempt, .. },
            ))
            | ChainPayload::Execution(ExecutionPayload::Recovery(
                RecoveryFact::AttemptCompleted { attempt, .. },
            )) => Some(*attempt),
            _ => None,
        })
        .collect()
}

pub(super) fn breaker_transitions(
    report: &EffectBoundaryReport,
) -> Vec<(WriterId, &'static str, String)> {
    report
        .control_events
        .iter()
        .filter_map(|event| match &event.payload {
            ChainPayload::Execution(ExecutionPayload::CircuitBreaker(fact)) => {
                let transition = match fact {
                    CircuitBreakerFact::Opened { .. } => "opened",
                    CircuitBreakerFact::HalfOpen { .. } => "half_open",
                    CircuitBreakerFact::Closed { .. } => "closed",
                    CircuitBreakerFact::StateChanged { .. } => "state_changed",
                    CircuitBreakerFact::Rejected { .. }
                    | CircuitBreakerFact::AttemptSettled { .. } => return None,
                };
                Some((event.writer_id, transition, fact.unit().as_str().to_owned()))
            }
            _ => None,
        })
        .collect()
}

pub(super) fn composite_members(
    factory: &dyn MiddlewareFactory,
) -> (
    Arc<ControlMiddlewareAggregator>,
    StageId,
    Arc<PerEffectPolicyBoundary>,
    StageId,
    Arc<PerEffectPolicyBoundary>,
) {
    let control = Arc::new(ControlMiddlewareAggregator::new());
    let owner = ProtectedUnitOwner::Composite(StageKey::from("digest"));
    let member = || {
        let config = test_stage_config(factory);
        let attachment = materialize_owned_effect_attachment(
            factory,
            &config,
            &control,
            EffectSafety::Idempotent,
            owner.clone(),
        )
        .expect("a composite member materializes its share of the unit");
        (
            config.stage_id,
            Arc::new(boundary_with_chain(vec![attachment])),
        )
    };
    let (map, map_boundary) = member();
    let (reduce, reduce_boundary) = member();
    (control, map, map_boundary, reduce, reduce_boundary)
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
    snapshots[0].1()
        .expect("uncontended limiter measurements")
        .events_total
}
