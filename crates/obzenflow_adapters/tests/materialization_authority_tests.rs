// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115n A5: third-party factories cannot launder built-in control
//! authority by forwarding an opaque materialisation context.

use async_trait::async_trait;
use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
use obzenflow_adapters::middleware::{
    materialize_factory_checked, CheckedMiddlewareSurfaceAttachment, CircuitBreaker,
    EffectAttemptOutcome, EffectPolicy, EffectResilience, EffectSurface, EffectTypeKey,
    EffectUnitId, MiddlewareAttachmentRequest, MiddlewareContext, MiddlewareDeclaration,
    MiddlewareDeclarationIndex, MiddlewareFactory, MiddlewareFactoryError, MiddlewareFactoryResult,
    MiddlewareMaterializationContext, MiddlewareOrigin, MiddlewareOverrideKey, MiddlewareSurface,
    MiddlewareSurfaceAttachment, MiddlewareSurfaceKind, PolicyAdmission, ProtectedUnit,
    ProtectedUnitId, RateLimiter, RateLimiterBuilder, Retry,
};
use obzenflow_core::event::{context::StageType, EffectType};
use obzenflow_core::{StageId, StageKey};
use obzenflow_runtime::control_plane::ControlPlaneProvider;
use obzenflow_runtime::effects::EffectSafety;
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::runtime_config::{
    materialize_flow_config, DslCandidates, FlowResolutionContext, ResolvedRuntimeConfig,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const STAGE: &str = "authority_stage";
const FLOW: &str = "authority_flow";
const EFFECT: &str = "authority.effect";
const RETARGET_EFFECT: &str = "authority.other_effect";

struct WrapperFamily;

#[derive(Clone, Copy)]
enum WrapperDeclaration {
    Ordinary,
    Forwarded,
}

#[derive(Clone, Copy)]
enum WrapperResult {
    Forwarded,
    OrdinaryAttachment,
    ErrorAfterDelegation,
    Retargeted,
    ChangedSafety,
}

struct DelegatingFactory {
    label: &'static str,
    inner: Box<dyn MiddlewareFactory>,
    declaration: WrapperDeclaration,
    result: WrapperResult,
}

struct StashingFactory {
    inner: Box<dyn MiddlewareFactory>,
    stored: Mutex<Option<MiddlewareSurfaceAttachment>>,
}

impl MiddlewareFactory for StashingFactory {
    fn label(&self) -> &'static str {
        "stashing_wrapper"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        self.inner.override_key()
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        self.inner.declaration()
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        let mut stored = self.stored.lock().expect("stashed attachment lock");
        if let Some(attachment) = stored.take() {
            return Ok(attachment);
        }
        *stored = Some(self.inner.materialize(request, context)?);
        Err(MiddlewareFactoryError::materialization_failed(
            self.label(),
            &context.config.name,
            std::io::Error::other("stored privileged attachment for a later invocation"),
        ))
    }

    fn dsl_config_defaults(&self) -> Vec<obzenflow_runtime::runtime_config::DslConfigDefault> {
        self.inner.dsl_config_defaults()
    }

    fn consumed_config_keys(&self) -> Vec<&'static str> {
        self.inner.consumed_config_keys()
    }
}

impl DelegatingFactory {
    fn new(
        label: &'static str,
        inner: Box<dyn MiddlewareFactory>,
        declaration: WrapperDeclaration,
        result: WrapperResult,
    ) -> Self {
        Self {
            label,
            inner,
            declaration,
            result,
        }
    }
}

impl MiddlewareFactory for DelegatingFactory {
    fn label(&self) -> &'static str {
        self.label
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<WrapperFamily>(self.label)
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        match self.declaration {
            WrapperDeclaration::Ordinary => {
                MiddlewareDeclaration::control(self.label, vec![MiddlewareSurfaceKind::Effect])
            }
            WrapperDeclaration::Forwarded => self.inner.declaration(),
        }
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        if matches!(self.result, WrapperResult::Retargeted) {
            let surface = MiddlewareSurface::Effect(EffectSurface {
                stage_id: request.protected_unit.stage_id,
                effect_type: EffectTypeKey::from(RETARGET_EFFECT),
                safety: EffectSafety::Idempotent,
            });
            let protected_unit = ProtectedUnitId {
                stage_id: request.protected_unit.stage_id,
                unit: ProtectedUnit::Effect(EffectUnitId {
                    effect_type: EffectTypeKey::from(RETARGET_EFFECT),
                }),
            };
            return self.inner.materialize(
                MiddlewareAttachmentRequest {
                    surface: &surface,
                    protected_unit: &protected_unit,
                    origin: request.origin,
                    declaration_index: request.declaration_index,
                },
                context,
            );
        }
        if matches!(self.result, WrapperResult::ChangedSafety) {
            let surface = MiddlewareSurface::Effect(EffectSurface {
                stage_id: request.protected_unit.stage_id,
                effect_type: EffectTypeKey::from(EFFECT),
                safety: EffectSafety::Idempotent,
            });
            return self.inner.materialize(
                MiddlewareAttachmentRequest {
                    surface: &surface,
                    ..request
                },
                context,
            );
        }
        let attachment = self.inner.materialize(request, context)?;
        match self.result {
            WrapperResult::Forwarded => Ok(attachment),
            WrapperResult::OrdinaryAttachment => Ok(MiddlewareSurfaceAttachment::effect(Arc::new(
                OrdinaryPolicy,
            ))),
            WrapperResult::ErrorAfterDelegation => {
                Err(MiddlewareFactoryError::materialization_failed(
                    self.label,
                    &context.config.name,
                    std::io::Error::other("wrapper failed after delegated construction"),
                ))
            }
            WrapperResult::Retargeted => unreachable!("handled before delegated construction"),
            WrapperResult::ChangedSafety => {
                unreachable!("handled before delegated construction")
            }
        }
    }

    fn dsl_config_defaults(&self) -> Vec<obzenflow_runtime::runtime_config::DslConfigDefault> {
        self.inner.dsl_config_defaults()
    }

    fn consumed_config_keys(&self) -> Vec<&'static str> {
        self.inner.consumed_config_keys()
    }
}

struct OrdinaryPolicy;

#[async_trait]
impl EffectPolicy for OrdinaryPolicy {
    fn label(&self) -> &'static str {
        "ordinary_policy"
    }

    async fn admit(&self, _ctx: &mut MiddlewareContext) -> PolicyAdmission {
        PolicyAdmission::Admit
    }

    fn observe(&self, _attempt: &EffectAttemptOutcome<'_>, _ctx: &mut MiddlewareContext) {}
}

fn breaker_only() -> Box<dyn MiddlewareFactory> {
    EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(2)
            .build()
            .expect("valid breaker"),
    )
    .build()
    .expect("valid breaker-only resilience aggregate")
}

fn breaker_and_limiter() -> Box<dyn MiddlewareFactory> {
    EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(2)
            .build()
            .expect("valid breaker"),
    )
    .retry(Retry::fixed(Duration::from_millis(1)).max_attempts(2))
    .rate_limit_each_attempt(RateLimiter::per_second(10.0).expect("valid limiter"))
    .build()
    .expect("valid resilience aggregate")
}

fn stage_config(factory: &dyn MiddlewareFactory) -> StageConfig {
    stage_config_with_effects(factory, &[EFFECT])
}

fn stage_config_with_effects(factory: &dyn MiddlewareFactory, effects: &[&str]) -> StageConfig {
    let stage = StageKey::from(STAGE);
    let mut dsl = DslCandidates::default();
    let effects: BTreeSet<_> = effects.iter().copied().map(EffectType::from).collect();
    for effect in &effects {
        for key in factory.consumed_config_keys() {
            dsl.declare_effect_consumption(key, stage.clone(), effect.clone());
        }
        for default in factory.dsl_config_defaults() {
            dsl.declare_for_effect(
                default.key_path,
                stage.clone(),
                effect.clone(),
                default.value,
            );
        }
    }
    let effective_config = materialize_flow_config(
        &ResolvedRuntimeConfig::builtin_defaults(),
        FlowResolutionContext {
            flow_name: FLOW.to_string(),
            stages: BTreeSet::from([stage.clone()]),
            edges: BTreeSet::new(),
            declared_effects: BTreeMap::from([(stage, effects)]),
            dsl,
        },
    )
    .expect("factory configuration should resolve");

    StageConfig {
        stage_id: StageId::new(),
        name: STAGE.to_string(),
        flow_name: FLOW.to_string(),
        cycle_guard: None,
        lineage: obzenflow_core::config::LineagePolicy::default(),
        effective_config: Arc::new(effective_config),
    }
}

fn materialize(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    control: &Arc<ControlMiddlewareAggregator>,
) -> Result<CheckedMiddlewareSurfaceAttachment, String> {
    materialize_with_safety(factory, config, control, EffectSafety::Idempotent)
}

fn materialize_with_safety(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    control: &Arc<ControlMiddlewareAggregator>,
    safety: EffectSafety,
) -> Result<CheckedMiddlewareSurfaceAttachment, String> {
    let surface = MiddlewareSurface::Effect(EffectSurface {
        stage_id: config.stage_id,
        effect_type: EffectTypeKey::from(EFFECT),
        safety,
    });
    let protected_unit = ProtectedUnitId {
        stage_id: config.stage_id,
        unit: ProtectedUnit::Effect(EffectUnitId {
            effect_type: EffectTypeKey::from(EFFECT),
        }),
    };
    let origin = MiddlewareOrigin::Stage;
    materialize_factory_checked(
        factory,
        MiddlewareAttachmentRequest {
            surface: &surface,
            protected_unit: &protected_unit,
            origin: &origin,
            declaration_index: MiddlewareDeclarationIndex::effect_policy(0),
        },
        config,
        StageType::Transform,
        control,
    )
    .map_err(|error| error.to_string())
}

fn assert_empty(control: &ControlMiddlewareAggregator, stage_id: StageId) {
    assert!(control
        .effect_circuit_breaker_snapshotters(&stage_id)
        .is_empty());
    assert!(control
        .effect_rate_limiter_snapshotters(&stage_id)
        .is_empty());
}

#[test]
fn ordinary_wrapper_cannot_delegate_privileged_resilience_authority() {
    let wrapper = DelegatingFactory::new(
        "ordinary_wrapper",
        breaker_only(),
        WrapperDeclaration::Ordinary,
        WrapperResult::Forwarded,
    );
    let config = stage_config(&wrapper);
    let control = Arc::new(ControlMiddlewareAggregator::new());

    let error = materialize(&wrapper, &config, &control)
        .err()
        .expect("laundering must fail");
    assert!(error.contains("declared authority 'ordinary'"), "{error}");
    assert!(error.contains("effect_resilience"), "{error}");
    assert_empty(&control, config.stage_id);
}

#[test]
fn returned_ordinary_attachment_rolls_back_staged_builtin_authority() {
    let wrapper = DelegatingFactory::new(
        "wrong_return_claim",
        breaker_only(),
        WrapperDeclaration::Forwarded,
        WrapperResult::OrdinaryAttachment,
    );
    let config = stage_config(&wrapper);
    let control = Arc::new(ControlMiddlewareAggregator::new());

    let error = materialize(&wrapper, &config, &control)
        .err()
        .expect("claim mismatch must fail");
    assert!(
        error.contains("returned attachment authority 'ordinary'"),
        "{error}"
    );
    assert_empty(&control, config.stage_id);
}

#[test]
fn factory_error_after_delegation_rolls_back_staged_builtin_authority() {
    let wrapper = DelegatingFactory::new(
        "error_after_delegation",
        breaker_and_limiter(),
        WrapperDeclaration::Forwarded,
        WrapperResult::ErrorAfterDelegation,
    );
    let config = stage_config(&wrapper);
    let control = Arc::new(ControlMiddlewareAggregator::new());

    let error = materialize(&wrapper, &config, &control)
        .err()
        .expect("factory error must fail");
    assert!(
        error.contains("wrapper failed after delegated construction"),
        "{error}"
    );
    assert_empty(&control, config.stage_id);
}

#[test]
fn honest_transparent_wrapper_preserves_builtin_claim_and_commits() {
    let wrapper = DelegatingFactory::new(
        "transparent_wrapper",
        breaker_and_limiter(),
        WrapperDeclaration::Forwarded,
        WrapperResult::Forwarded,
    );
    let config = stage_config(&wrapper);
    let control = Arc::new(ControlMiddlewareAggregator::new());

    let attachment = materialize(&wrapper, &config, &control)
        .expect("transparent declaration and attachment forwarding should succeed");
    assert!(attachment.into_effect().is_some());
    assert_eq!(
        control
            .effect_circuit_breaker_snapshotters(&config.stage_id)
            .len(),
        1
    );
    assert_eq!(
        control
            .effect_rate_limiter_snapshotters(&config.stage_id)
            .len(),
        1
    );
}

#[test]
fn transparent_wrapper_cannot_retarget_authority_to_a_sibling_effect() {
    let wrapper = DelegatingFactory::new(
        "retargeting_wrapper",
        breaker_only(),
        WrapperDeclaration::Forwarded,
        WrapperResult::Retargeted,
    );
    let config = stage_config_with_effects(&wrapper, &[EFFECT, RETARGET_EFFECT]);
    let control = Arc::new(ControlMiddlewareAggregator::new());

    let error = materialize(&wrapper, &config, &control)
        .err()
        .expect("a wrapper cannot retarget the checked protected unit");
    assert!(
        error.contains("attempted privileged materialisation"),
        "{error}"
    );
    assert_empty(&control, config.stage_id);
}

#[test]
fn transparent_wrapper_cannot_weaken_the_checked_effect_safety() {
    let wrapper = DelegatingFactory::new(
        "safety_changing_wrapper",
        breaker_and_limiter(),
        WrapperDeclaration::Forwarded,
        WrapperResult::ChangedSafety,
    );
    let config = stage_config(&wrapper);
    let control = Arc::new(ControlMiddlewareAggregator::new());

    let error = materialize_with_safety(&wrapper, &config, &control, EffectSafety::Transactional)
        .err()
        .expect("a wrapper cannot weaken transactional safety before delegation");
    assert!(
        error.contains("attempted privileged materialisation"),
        "{error}"
    );
    assert_empty(&control, config.stage_id);
}

#[test]
fn privileged_attachment_cannot_be_replayed_into_a_later_invocation() {
    let wrapper = StashingFactory {
        inner: breaker_only(),
        stored: Mutex::new(None),
    };
    let config = stage_config(&wrapper);
    let control = Arc::new(ControlMiddlewareAggregator::new());

    let first = materialize(&wrapper, &config, &control)
        .err()
        .expect("the first invocation intentionally stores then fails");
    assert!(first.contains("stored privileged attachment"), "{first}");
    assert_empty(&control, config.stage_id);

    let second = materialize(&wrapper, &config, &control)
        .err()
        .expect("the stored attachment token belongs to the prior invocation");
    assert!(
        second.contains("different materialisation invocation"),
        "{second}"
    );
    assert_empty(&control, config.stage_id);
}

#[test]
fn failed_batch_preflight_does_not_partially_commit_other_authority() {
    let limiter = RateLimiterBuilder::new(10.0).build();
    let limiter_config = stage_config(limiter.as_ref());
    let control = Arc::new(ControlMiddlewareAggregator::new());
    materialize(limiter.as_ref(), &limiter_config, &control)
        .expect("standalone limiter should register");

    let aggregate = breaker_and_limiter();
    let mut aggregate_config = stage_config(aggregate.as_ref());
    aggregate_config.stage_id = limiter_config.stage_id;
    let error = materialize(aggregate.as_ref(), &aggregate_config, &control)
        .err()
        .expect("duplicate limiter must reject the complete batch");
    assert!(
        error.contains("rate limiter is already registered"),
        "{error}"
    );
    assert!(control
        .effect_circuit_breaker_snapshotters(&limiter_config.stage_id)
        .is_empty());
    assert_eq!(
        control
            .effect_rate_limiter_snapshotters(&limiter_config.stage_id)
            .len(),
        1
    );
}
