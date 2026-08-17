// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115n A4: exclusive built-in effect-control authority is validated
//! over the complete stage-plus-inline declaration set before materialisation.

use async_trait::async_trait;
use obzenflow_adapters::middleware::{
    validate_attachment_request, CircuitBreaker, EffectAttemptOutcome, EffectPolicy,
    EffectResilience, MiddlewareAttachmentRequest, MiddlewareContext, MiddlewareDeclaration,
    MiddlewareFactory, MiddlewareFactoryResult, MiddlewareHints, MiddlewareMaterializationContext,
    MiddlewareOverrideKey, MiddlewareSafety, MiddlewareSurfaceAttachment, MiddlewareSurfaceKind,
    PolicyAdmission, RateLimiterBuilder, TopologyMiddlewareConfigSlot,
};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::dsl::stage_descriptor::{
    EffectPolicyAttachment, EffectfulTransformDescriptor, StageDescriptor,
};
use obzenflow_dsl::dsl::typing::{wrap_typed_descriptor, StageTypingMetadata, TypeHint};
use obzenflow_dsl::{effectful_transform, flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectDeclaration, EffectError, EffectSafety, Effects,
};
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::runtime_config::DslConfigDefault;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
use obzenflow_runtime::stages::observer::EffectObserver;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CompositionInput;

impl TypedPayload for CompositionInput {
    const EVENT_TYPE: &'static str = "effect_control_composition.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CompositionFact;

impl TypedPayload for CompositionFact {
    const EVENT_TYPE: &'static str = "effect_control_composition.fact";
}

#[derive(Clone, Debug)]
struct EffectA;

#[async_trait]
impl Effect for EffectA {
    const EFFECT_TYPE: &'static str = "effect_control_composition.a";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;
    type BindingMode = obzenflow_runtime::effects::Portless;

    type Outcome = CompositionFact;
    type OutcomeSemantics = obzenflow_runtime::effects::DomainFacts;

    fn label(&self) -> &str {
        "effect-a"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({})
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        Ok(CompositionFact)
    }
}

#[derive(Clone, Debug)]
struct EffectB;

#[async_trait]
impl Effect for EffectB {
    const EFFECT_TYPE: &'static str = "effect_control_composition.b";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;
    type BindingMode = obzenflow_runtime::effects::Portless;

    type Outcome = CompositionFact;
    type OutcomeSemantics = obzenflow_runtime::effects::DomainFacts;

    fn label(&self) -> &str {
        "effect-b"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({})
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        Ok(CompositionFact)
    }
}

#[derive(Clone, Debug)]
struct OneEffectHandler;

#[async_trait]
impl EffectfulTransformHandler for OneEffectHandler {
    type Input = CompositionInput;
    type Output = obzenflow_core::stage_fact_set![CompositionFact];
    type AllowedEffects = obzenflow_runtime::effect_set![EffectA];

    async fn process(
        &self,
        _input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        Ok(fx.complete()?)
    }
}

#[derive(Clone, Debug)]
struct TwoEffectHandler;

#[async_trait]
impl EffectfulTransformHandler for TwoEffectHandler {
    type Input = CompositionInput;
    type Output = obzenflow_core::stage_fact_set![CompositionFact];
    type AllowedEffects = obzenflow_runtime::effect_set![EffectA, EffectB];

    async fn process(
        &self,
        _input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        Ok(fx.complete()?)
    }
}

fn aggregate() -> Box<dyn MiddlewareFactory> {
    EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(2)
            .build()
            .expect("breaker-only aggregate configuration"),
    )
    .build()
    .expect("breaker-only aggregate factory")
}

fn limiter() -> Box<dyn MiddlewareFactory> {
    RateLimiterBuilder::new(10.0).build()
}

struct CountingFactory {
    inner: Box<dyn MiddlewareFactory>,
    materializations: Arc<AtomicUsize>,
}

struct LaunderedResilienceFamily;

/// Models a third-party factory that claims ordinary effect-control semantics
/// while delegating construction to the privileged aggregate.
struct LaunderedResilienceFactory {
    inner: Box<dyn MiddlewareFactory>,
}

impl MiddlewareFactory for LaunderedResilienceFactory {
    fn label(&self) -> &'static str {
        "laundered_resilience"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<LaunderedResilienceFamily>(self.label())
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::control(self.label(), vec![MiddlewareSurfaceKind::Effect])
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        self.inner.materialize(request, context)
    }

    fn dsl_config_defaults(&self) -> Vec<DslConfigDefault> {
        self.inner.dsl_config_defaults()
    }

    fn consumed_config_keys(&self) -> Vec<&'static str> {
        self.inner.consumed_config_keys()
    }
}

fn laundered_aggregate() -> Box<dyn MiddlewareFactory> {
    Box::new(LaunderedResilienceFactory { inner: aggregate() })
}

struct MutableDeclarationFamily;

struct MutableDeclarationFactory {
    inner: Box<dyn MiddlewareFactory>,
    materializing: AtomicBool,
}

impl MiddlewareFactory for MutableDeclarationFactory {
    fn label(&self) -> &'static str {
        "mutable_declaration_wrapper"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<MutableDeclarationFamily>(self.label())
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        if self.materializing.load(Ordering::SeqCst) {
            self.inner.declaration()
        } else {
            MiddlewareDeclaration::control(self.label(), vec![MiddlewareSurfaceKind::Effect])
        }
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        self.materializing.store(true, Ordering::SeqCst);
        self.inner.materialize(request, context)
    }

    fn dsl_config_defaults(&self) -> Vec<DslConfigDefault> {
        self.inner.dsl_config_defaults()
    }

    fn consumed_config_keys(&self) -> Vec<&'static str> {
        self.inner.consumed_config_keys()
    }
}

fn mutable_declaration_aggregate() -> Box<dyn MiddlewareFactory> {
    Box::new(MutableDeclarationFactory {
        inner: aggregate(),
        materializing: AtomicBool::new(false),
    })
}

impl CountingFactory {
    fn boxed(
        inner: Box<dyn MiddlewareFactory>,
        materializations: Arc<AtomicUsize>,
    ) -> Box<dyn MiddlewareFactory> {
        Box::new(Self {
            inner,
            materializations,
        })
    }
}

impl MiddlewareFactory for CountingFactory {
    fn label(&self) -> &'static str {
        self.inner.label()
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
        self.materializations.fetch_add(1, Ordering::SeqCst);
        self.inner.materialize(request, context)
    }

    fn dsl_config_defaults(&self) -> Vec<DslConfigDefault> {
        self.inner.dsl_config_defaults()
    }

    fn consumed_config_keys(&self) -> Vec<&'static str> {
        self.inner.consumed_config_keys()
    }

    fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
        self.inner.topology_config_slot()
    }

    fn supported_stage_types(&self) -> &[obzenflow_core::event::context::StageType] {
        self.inner.supported_stage_types()
    }

    fn safety_level(&self) -> MiddlewareSafety {
        self.inner.safety_level()
    }

    fn hints(&self) -> MiddlewareHints {
        self.inner.hints()
    }

    fn config_snapshot(&self) -> Option<serde_json::Value> {
        self.inner.config_snapshot()
    }
}

struct ProofObserverFamily;

struct ProofObserverFactory {
    materializations: Arc<AtomicUsize>,
}

impl MiddlewareFactory for ProofObserverFactory {
    fn label(&self) -> &'static str {
        "a4_effect_observer"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<ProofObserverFamily>(self.label())
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::observer(self.label(), vec![MiddlewareSurfaceKind::Effect])
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        validate_attachment_request(&self.declaration(), &request).map_err(|error| {
            obzenflow_adapters::middleware::MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                error,
            )
        })?;
        self.materializations.fetch_add(1, Ordering::SeqCst);
        Ok(MiddlewareSurfaceAttachment::effect_observer(Arc::new(
            ProofObserver,
        )))
    }
}

struct ProofObserver;

impl EffectObserver for ProofObserver {}

struct OrdinaryControlFamily;

struct OrdinaryControlFactory {
    materializations: Arc<AtomicUsize>,
}

impl MiddlewareFactory for OrdinaryControlFactory {
    fn label(&self) -> &'static str {
        "a4_ordinary_effect_control"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<OrdinaryControlFamily>(self.label())
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::control(self.label(), vec![MiddlewareSurfaceKind::Effect])
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        validate_attachment_request(&self.declaration(), &request).map_err(|error| {
            obzenflow_adapters::middleware::MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                error,
            )
        })?;
        self.materializations.fetch_add(1, Ordering::SeqCst);
        Ok(MiddlewareSurfaceAttachment::effect(Arc::new(
            OrdinaryControl,
        )))
    }
}

struct OrdinaryControl;

#[async_trait]
impl EffectPolicy for OrdinaryControl {
    fn label(&self) -> &'static str {
        "a4_ordinary_effect_control"
    }

    async fn admit(&self, _ctx: &mut MiddlewareContext) -> PolicyAdmission {
        PolicyAdmission::Admit
    }

    fn observe(&self, _attempt: &EffectAttemptOutcome<'_>, _ctx: &mut MiddlewareContext) {}
}

macro_rules! single_effect_flow {
    (observers: [$($observer:expr),* $(,)?], policy: $policy:expr) => {
        FlowDefinition::materialize(move |_runtime_config| {
            let guarded_handler = OneEffectHandler;

            Ok(flow! {
                name: "effect_control_composition_single",
                journals: memory_journals(),

                stages: {
                    input = source!(CompositionInput => placeholder!());
                    guarded = effectful_transform!(
                        CompositionInput -> CompositionFact uses EffectA with $policy => guarded_handler,
                        observers: [$($observer),*]
                    );
                    output = sink!(CompositionFact => placeholder!());
                },

                topology: {
                    input |> guarded;
                    guarded |> output;
                }
            })
        })
    };
}

macro_rules! two_effect_flow {
    (effect_a: $effect_a:expr, effect_b: $effect_b:expr) => {
        FlowDefinition::materialize(move |_runtime_config| {
            let guarded_handler = TwoEffectHandler;

            Ok(flow! {
                name: "effect_control_composition_two_effects",
                journals: memory_journals(),

                stages: {
                    input = source!(CompositionInput => placeholder!());
                    guarded = effectful_transform!(
                        CompositionInput -> CompositionFact
                        uses {
                            EffectA with $effect_a,
                            EffectB with $effect_b,
                        }
                        => guarded_handler,
                        observers: []
                    );
                    output = sink!(CompositionFact => placeholder!());
                },

                topology: {
                    input |> guarded;
                    guarded |> output;
                }
            })
        })
    };
}

fn malformed_effect_policy_attachment_flow(
    policy_materializations: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let guarded: Box<dyn StageDescriptor> = Box::new(EffectfulTransformDescriptor::new(
            "guarded",
            OneEffectHandler,
            vec![EffectDeclaration::of::<EffectA>()],
            Vec::new(),
            vec![EffectPolicyAttachment {
                effect_type: EffectB::EFFECT_TYPE,
                // No config defaults: this must reach descriptor materialisation
                // instead of failing during configuration extraction.
                factory: Box::new(OrdinaryControlFactory {
                    materializations: policy_materializations.clone(),
                }),
            }],
            None,
        ));
        let guarded = wrap_typed_descriptor(
            guarded,
            StageTypingMetadata::transform(
                TypeHint::exact_payload::<CompositionInput>(),
                TypeHint::exact_payload::<CompositionFact>(),
                false,
                None,
            ),
        );

        Ok(flow! {
            name: "malformed_effect_policy_attachment",
            journals: memory_journals(),

            stages: {
                input = source!(CompositionInput => placeholder!());
                guarded = guarded;
                output = sink!(CompositionFact => placeholder!());
            },

            topology: {
                input |> guarded;
                guarded |> output;
            }
        })
    })
}

fn duplicate_effect_policy_attachment_flow(
    first_materializations: Arc<AtomicUsize>,
    second_materializations: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let guarded: Box<dyn StageDescriptor> = Box::new(EffectfulTransformDescriptor::new(
            "guarded",
            OneEffectHandler,
            vec![EffectDeclaration::of::<EffectA>()],
            Vec::new(),
            vec![
                EffectPolicyAttachment {
                    effect_type: EffectA::EFFECT_TYPE,
                    factory: Box::new(OrdinaryControlFactory {
                        materializations: first_materializations.clone(),
                    }),
                },
                EffectPolicyAttachment {
                    effect_type: EffectA::EFFECT_TYPE,
                    factory: Box::new(OrdinaryControlFactory {
                        materializations: second_materializations.clone(),
                    }),
                },
            ],
            None,
        ));
        let guarded = wrap_typed_descriptor(
            guarded,
            StageTypingMetadata::transform(
                TypeHint::exact_payload::<CompositionInput>(),
                TypeHint::exact_payload::<CompositionFact>(),
                false,
                None,
            ),
        );

        Ok(flow! {
            name: "duplicate_effect_policy_attachment",
            journals: memory_journals(),

            stages: {
                input = source!(CompositionInput => placeholder!());
                guarded = guarded;
                output = sink!(CompositionFact => placeholder!());
            },

            topology: {
                input |> guarded;
                guarded |> output;
            }
        })
    })
}

async fn build(flow: FlowDefinition) -> Result<(), obzenflow_dsl::dsl::FlowBuildFailure> {
    flow.build(FlowBuildContext::for_tests()).await.map(|_| ())
}

#[tokio::test]
async fn undeclared_policy_effect_fails_build_before_materialization() {
    let policy_materializations = Arc::new(AtomicUsize::new(0));

    let error = build(malformed_effect_policy_attachment_flow(
        policy_materializations.clone(),
    ))
    .await
    .expect_err("a policy attachment for an undeclared effect must fail the flow build");
    let rendered = error.to_string();
    assert!(
        rendered.contains("Effectful stage 'guarded'")
            && rendered.contains("attaches policy middleware to undeclared effect")
            && rendered.contains(EffectB::EFFECT_TYPE),
        "unexpected malformed attachment diagnostic: {rendered}"
    );
    assert_eq!(policy_materializations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn bare_singleton_effect_policy_and_passive_observer_materialize_once() {
    let observer_materializations = Arc::new(AtomicUsize::new(0));
    let policy_materializations = Arc::new(AtomicUsize::new(0));
    let observer: Box<dyn MiddlewareFactory> = Box::new(ProofObserverFactory {
        materializations: observer_materializations.clone(),
    });
    let policy = CountingFactory::boxed(aggregate(), policy_materializations.clone());

    build(single_effect_flow!(
        observers: [observer],
        policy: policy
    ))
    .await
    .expect("one bare effect policy and one passive observer must materialize");

    assert_eq!(observer_materializations.load(Ordering::SeqCst), 1);
    assert_eq!(policy_materializations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn control_middleware_in_observers_fails_with_authority_diagnostic() {
    let error = build(single_effect_flow!(
        observers: [limiter()],
        policy: aggregate()
    ))
    .await
    .expect_err("the passive observer lane must not grant control authority");

    assert!(
        error.to_string().contains(
            "'observers:' accepts observer middleware only; attach control middleware 'rate_limiter' in the 'with [...]' clause of the live I/O unit it protects (FLOWIP-115s)"
        ),
        "unexpected authority diagnostic: {error}"
    );
}

#[tokio::test]
async fn aggregate_and_limiter_on_distinct_effects_remain_valid() {
    build(two_effect_flow!(
        effect_a: aggregate(),
        effect_b: limiter()
    ))
    .await
    .expect("exclusive built-in control authority is scoped to the exact effect");
}

#[tokio::test]
async fn descriptor_cannot_construct_two_policies_for_one_bare_effect_position() {
    let first_materializations = Arc::new(AtomicUsize::new(0));
    let second_materializations = Arc::new(AtomicUsize::new(0));
    let error = build(duplicate_effect_policy_attachment_flow(
        first_materializations.clone(),
        second_materializations.clone(),
    ))
    .await
    .expect_err("a descriptor cannot bypass the singleton effect position");
    let rendered = error.to_string();
    assert!(
        rendered.contains("attaches more than one policy"),
        "{rendered}"
    );
    assert!(rendered.contains(EffectA::EFFECT_TYPE), "{rendered}");
    assert_eq!(first_materializations.load(Ordering::SeqCst), 0);
    assert_eq!(second_materializations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn ordinary_wrapper_cannot_launder_privileged_aggregate_authority() {
    let error = build(single_effect_flow!(
        observers: [],
        policy: laundered_aggregate()
    ))
    .await
    .expect_err("an ordinary wrapper must not launder aggregate authority");
    let rendered = error.to_string();
    assert!(
        rendered.contains("declared authority 'ordinary'"),
        "{rendered}"
    );
    assert!(rendered.contains("effect_resilience"), "{rendered}");
}

#[tokio::test]
async fn materialization_cannot_swap_the_declaration_that_passed_structural_validation() {
    let error = build(single_effect_flow!(
        observers: [],
        policy: mutable_declaration_aggregate()
    ))
    .await
    .expect_err("the checked gateway must retain the declaration validated as ordinary");
    let rendered = error.to_string();
    assert!(
        rendered.contains("declared authority 'ordinary'"),
        "{rendered}"
    );
    assert!(rendered.contains("effect_resilience"), "{rendered}");
}
