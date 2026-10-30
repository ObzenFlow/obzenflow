// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115v: contextual effect-control composition and authority are
//! validated over every authored site before materialisation.

use async_trait::async_trait;
use obzenflow_adapters::middleware::{
    circuit_breaker, rate_limit, validate_attachment_request, EffectAttemptOutcome, EffectPolicy,
    MiddlewareAttachmentRequest, MiddlewareContext, MiddlewareDeclaration, MiddlewareFactory,
    MiddlewareFactoryResult, MiddlewareMaterializationContext, MiddlewareOverrideKey,
    MiddlewareSurfaceAttachment, MiddlewareSurfaceKind, PolicyAdmission,
};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::dsl::stage_descriptor::{EffectfulTransformDescriptor, StageDescriptor};
use obzenflow_dsl::dsl::typing::{wrap_typed_descriptor, StageTypingMetadata, TypeHint};
use obzenflow_dsl::{effectful_stateful, effectful_transform, flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::effects::{effectful_transform_effects, EffectError, Effects, System};
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::runtime_config::DslConfigDefault;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulStatefulHandler, EffectfulTransformHandler,
};
use obzenflow_runtime::stages::observer::EffectObserver;
use serde::{Deserialize, Serialize};
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

struct CompositionService;

#[obzenflow::port]
impl CompositionService {
    #[effect(type = "effect_control_composition.a", exactly_once)]
    fn a(&self, _: &RequestA) -> Result<CompositionFact, EffectError> {
        Ok(CompositionFact)
    }

    #[effect(type = "effect_control_composition.b", exactly_once)]
    fn b(&self, _: &RequestB) -> Result<CompositionFact, EffectError> {
        Ok(CompositionFact)
    }

    #[effect(type = "effect_control_composition.paid", at_least_once)]
    fn pay(&self, _: &PaidRequest) -> Result<CompositionFact, EffectError> {
        Ok(CompositionFact)
    }
}

fn composition_service() -> System<CompositionService> {
    System::eager("composition", Arc::new(CompositionService)).expect("a valid system name")
}

#[derive(Clone, Debug, Serialize)]
struct RequestA;

#[derive(Clone, Debug, Serialize)]
struct RequestB;

#[derive(Clone, Debug, obzenflow::effects::EffectHandler)]
struct OneEffectHandler {
    #[system]
    service: System<CompositionService>,
}

impl OneEffectHandler {
    fn new() -> Self {
        Self {
            service: composition_service(),
        }
    }
}

#[async_trait]
impl EffectfulTransformHandler for OneEffectHandler {
    type Input = CompositionInput;
    type Output = obzenflow_core::stage_fact_set![CompositionFact];
    type AllowedEffects = RequestA;

    async fn process(
        &self,
        _input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        Ok(fx.complete()?)
    }
}

#[derive(Clone, Debug, obzenflow::effects::EffectHandler)]
struct TwoEffectHandler {
    #[system]
    service: System<CompositionService>,
}

impl TwoEffectHandler {
    fn new() -> Self {
        Self {
            service: composition_service(),
        }
    }
}

#[async_trait]
impl EffectfulTransformHandler for TwoEffectHandler {
    type Input = CompositionInput;
    type Output = obzenflow_core::stage_fact_set![CompositionFact];
    type AllowedEffects = obzenflow_runtime::effect_set![RequestA, RequestB];

    async fn process(
        &self,
        _input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        Ok(fx.complete()?)
    }
}

#[derive(Clone, Debug, obzenflow::effects::EffectHandler)]
struct StatefulOneEffectHandler {
    #[system]
    service: System<CompositionService>,
}

impl StatefulOneEffectHandler {
    fn new() -> Self {
        Self {
            service: composition_service(),
        }
    }
}

#[async_trait]
impl EffectfulStatefulHandler for StatefulOneEffectHandler {
    type State = ();
    type Input = CompositionInput;
    type Output = CompositionFact;
    type AllowedEffects = RequestA;

    fn initial_state(&self) -> Self::State {}

    async fn decide(
        &mut self,
        _state: &Self::State,
        _input: &Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        Ok(fx.complete_empty()?)
    }

    fn apply(&mut self, _state: &mut Self::State, _fact: Self::Output) -> Result<(), HandlerError> {
        Ok(())
    }
}

fn breaker() -> Box<dyn MiddlewareFactory> {
    Box::new(circuit_breaker().consecutive_failures(2))
}

fn limiter() -> Box<dyn MiddlewareFactory> {
    Box::new(rate_limit(10.0))
}

struct LaunderedResilienceFamily;

/// Models a third-party factory that claims ordinary effect-control semantics
/// while delegating construction to the privileged breaker.
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

fn laundered_breaker() -> Box<dyn MiddlewareFactory> {
    Box::new(LaunderedResilienceFactory { inner: breaker() })
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

fn mutable_declaration_breaker() -> Box<dyn MiddlewareFactory> {
    Box::new(MutableDeclarationFactory {
        inner: breaker(),
        materializing: AtomicBool::new(false),
    })
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
    (observers: [], policy: $policy:expr) => {
        FlowDefinition::materialize(move |_runtime_config| {
            let guarded_handler = OneEffectHandler::new();
            Ok(flow! {
                name: "effect_control_composition_single",
                journals: memory_journals(),
                stages: {
                    input = source!(CompositionInput => placeholder!());
                    guarded = effectful_transform!(CompositionInput -> CompositionFact with $policy => guarded_handler);
                    output = sink!(CompositionFact => placeholder!());
                },
                topology: { input |> guarded; guarded |> output; }
            })
        })
    };
    (observers: [$($observer:expr),* $(,)?], policy: $policy:expr) => {
        FlowDefinition::materialize(move |_runtime_config| {
            let guarded_handler = OneEffectHandler::new();

            Ok(flow! {
                name: "effect_control_composition_single",
                journals: memory_journals(),

                stages: {
                    input = source!(CompositionInput => placeholder!());
                    guarded = effectful_transform!(
                        CompositionInput -> CompositionFact with { $policy, $($observer),* } => guarded_handler);
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
            let guarded_handler = TwoEffectHandler::new();
            let effect_a_policy = $effect_a.for_effect::<RequestA>();
            let effect_b_policy = $effect_b.for_effect::<RequestB>();

            Ok(flow! {
                name: "effect_control_composition_two_effects",
                journals: memory_journals(),

                stages: {
                    input = source!(CompositionInput => placeholder!());
                    guarded = effectful_transform!(
                        CompositionInput -> CompositionFact
                        with { effect_a_policy, effect_b_policy }
                        => guarded_handler);
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

macro_rules! stateful_single_effect_flow {
    (observers: [$($observer:expr),* $(,)?], policy: $policy:expr) => {
        FlowDefinition::materialize(move |_runtime_config| {
            let guarded_handler = StatefulOneEffectHandler::new();

            Ok(flow! {
                name: "effect_control_composition_stateful",
                journals: memory_journals(),

                stages: {
                    input = source!(CompositionInput => placeholder!());
                    guarded = effectful_stateful!(
                        CompositionInput -> CompositionFact
                        with { $policy, $($observer),* }
                        => guarded_handler);
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
        let handler = OneEffectHandler::new();
        let effects = effectful_transform_effects(&handler, &[]);
        let guarded: Box<dyn StageDescriptor> = Box::new(EffectfulTransformDescriptor::new(
            "guarded",
            handler,
            effects,
            vec![Box::new(
                OrdinaryControlFactory {
                    materializations: policy_materializations.clone(),
                }
                .for_effect::<RequestB>(),
            )],
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
        let handler = OneEffectHandler::new();
        let effects = effectful_transform_effects(&handler, &[]);
        let guarded: Box<dyn StageDescriptor> = Box::new(EffectfulTransformDescriptor::new(
            "guarded",
            handler,
            effects,
            vec![
                Box::new(
                    OrdinaryControlFactory {
                        materializations: first_materializations.clone(),
                    }
                    .for_effect::<RequestA>(),
                ),
                Box::new(
                    OrdinaryControlFactory {
                        materializations: second_materializations.clone(),
                    }
                    .for_effect::<RequestA>(),
                ),
            ],
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
        rendered.contains("stage 'guarded'")
            && rendered.contains("undeclared effect")
            && rendered.contains("effect_control_composition.b"),
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
    let policy = Box::new(OrdinaryControlFactory {
        materializations: policy_materializations.clone(),
    });

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
async fn stateful_policy_and_passive_observer_materialize_without_a_wrapper() {
    let observer_materializations = Arc::new(AtomicUsize::new(0));
    let policy_materializations = Arc::new(AtomicUsize::new(0));
    let observer: Box<dyn MiddlewareFactory> = Box::new(ProofObserverFactory {
        materializations: observer_materializations.clone(),
    });
    let policy = Box::new(OrdinaryControlFactory {
        materializations: policy_materializations.clone(),
    });

    build(stateful_single_effect_flow!(
        observers: [observer],
        policy: policy
    ))
    .await
    .expect("stateful boundary policy and typed observer must materialize independently");

    assert_eq!(observer_materializations.load(Ordering::SeqCst), 1);
    assert_eq!(policy_materializations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn controls_on_implementation_compose_with_the_sole_named_effect() {
    build(single_effect_flow!(observers: [limiter()], policy: breaker()))
        .await
        .expect("implementation limiter and effect breaker share one operation");
}

#[tokio::test]
async fn stateful_controls_target_the_sole_effect_without_wrapping_accumulation() {
    build(stateful_single_effect_flow!(observers: [limiter()], policy: breaker()))
        .await
        .expect("stateful controls resolve to the sole declared effect");
}

#[tokio::test]
async fn breaker_and_limiter_on_distinct_effects_remain_valid() {
    build(two_effect_flow!(
        effect_a: breaker(),
        effect_b: limiter()
    ))
    .await
    .expect("exclusive built-in control authority is scoped to the exact effect");
}

#[tokio::test]
async fn independent_custom_controls_without_composition_contract_fail_before_materialization() {
    let first_materializations = Arc::new(AtomicUsize::new(0));
    let second_materializations = Arc::new(AtomicUsize::new(0));
    let error = build(duplicate_effect_policy_attachment_flow(
        first_materializations.clone(),
        second_materializations.clone(),
    ))
    .await
    .expect_err("independent custom controls need a defined composition contract");
    let rendered = error.to_string();
    assert!(
        rendered.contains("control") || rendered.contains("composition"),
        "{rendered}"
    );
    assert!(
        rendered.contains("effect_control_composition.a"),
        "{rendered}"
    );
    assert_eq!(first_materializations.load(Ordering::SeqCst), 0);
    assert_eq!(second_materializations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn ordinary_wrapper_cannot_launder_privileged_control_authority() {
    let error = build(single_effect_flow!(
        observers: [],
        policy: laundered_breaker()
    ))
    .await
    .expect_err("an ordinary wrapper must not launder breaker authority");
    let rendered = error.to_string();
    assert!(
        rendered.contains("declared authority 'ordinary'"),
        "{rendered}"
    );
    assert!(rendered.contains("circuit_breaker"), "{rendered}");
}

#[tokio::test]
async fn materialization_cannot_swap_the_declaration_that_passed_structural_validation() {
    let error = build(single_effect_flow!(
        observers: [],
        policy: mutable_declaration_breaker()
    ))
    .await
    .expect_err("the checked gateway must retain the declaration validated as ordinary");
    let rendered = error.to_string();
    assert!(
        rendered.contains("declared authority 'ordinary'"),
        "{rendered}"
    );
    assert!(rendered.contains("circuit_breaker"), "{rendered}");
}

#[derive(Clone, Debug, Serialize)]
struct PaidRequest;

#[derive(Clone, Debug, obzenflow::effects::EffectHandler)]
struct PaidHandler {
    #[system]
    service: System<CompositionService>,
}

impl PaidHandler {
    fn new() -> Self {
        Self {
            service: composition_service(),
        }
    }
}

#[async_trait]
impl EffectfulTransformHandler for PaidHandler {
    type Input = CompositionInput;
    type Output = obzenflow_core::stage_fact_set![CompositionFact];
    type AllowedEffects = PaidRequest;

    async fn process(
        &self,
        _: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        Ok(fx.complete()?)
    }
}

#[derive(Clone, Debug, obzenflow::effects::EffectHandler)]
struct NoEffectHandler;

#[async_trait]
impl EffectfulTransformHandler for NoEffectHandler {
    type Input = CompositionInput;
    type Output = obzenflow_core::stage_fact_set![CompositionFact];
    type AllowedEffects = obzenflow_runtime::effect_set![];

    async fn process(
        &self,
        _: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        Ok(fx.complete()?)
    }
}

fn row_flow<F>(guarded: F) -> FlowDefinition
where
    F: FnOnce() -> Box<dyn StageDescriptor> + Send + 'static,
{
    FlowDefinition::materialize(move |_| {
        let guarded = guarded();
        Ok(flow! {
            name: "effect_row_build",
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

async fn row_refusal<F>(guarded: F) -> String
where
    F: FnOnce() -> Box<dyn StageDescriptor> + Send + 'static,
{
    build(row_flow(guarded))
        .await
        .expect_err("the row must fail the flow build")
        .to_string()
}

#[tokio::test]
async fn an_acknowledged_paid_effect_builds() {
    build(row_flow(|| {
        let paid = PaidHandler::new();
        effectful_transform!(CompositionInput -> CompositionFact => paid.at_least_once::<PaidRequest>())
    }))
    .await
    .expect("an acknowledged paid effect builds");
}

#[tokio::test]
async fn a_paid_effect_without_its_acknowledgement_fails_build() {
    let rendered = row_refusal(|| {
        let paid = PaidHandler::new();
        effectful_transform!(CompositionInput -> CompositionFact => paid)
    })
    .await;
    assert!(
        rendered.contains(
            "Effectful stage 'guarded' performs at-least-once effect \
             'effect_control_composition.paid' without acknowledging it; add \
             `.at_least_once::<PaidRequest>()` after the handler"
        ),
        "{rendered}"
    );
}

#[tokio::test]
async fn an_acknowledgement_for_an_effect_the_handler_does_not_perform_fails_build() {
    let rendered = row_refusal(|| {
        let guarded_handler = OneEffectHandler::new();
        effectful_transform!(
            CompositionInput -> CompositionFact => guarded_handler.at_least_once::<PaidRequest>()
        )
    })
    .await;
    assert!(
        rendered.contains("which its handler does not perform")
            && rendered.contains("`.at_least_once::<PaidRequest>()`"),
        "{rendered}"
    );
}

#[tokio::test]
async fn an_acknowledgement_of_an_exactly_once_effect_fails_build() {
    let rendered = row_refusal(|| {
        let guarded_handler = OneEffectHandler::new();
        effectful_transform!(
            CompositionInput -> CompositionFact => guarded_handler.at_least_once::<RequestA>()
        )
    })
    .await;
    assert!(
        rendered.contains(
            "acknowledges `effect_control_composition.a` as at-least-once, but its effect method \
             is `exactly_once`"
        ),
        "{rendered}"
    );
}

#[tokio::test]
async fn a_control_naming_no_effect_on_a_stage_with_several_effects_fails_build() {
    let rendered = row_refusal(|| {
        let guarded_handler = TwoEffectHandler::new();
        effectful_transform!(CompositionInput -> CompositionFact with rate_limit(10.0) => guarded_handler)
    })
    .await;
    assert!(
        rendered
            .contains("requires exactly one declared effect; build it with `.for_effect::<E>()`")
            && rendered.contains("effect_control_composition.a")
            && rendered.contains("effect_control_composition.b"),
        "{rendered}"
    );
}

#[tokio::test]
async fn middleware_for_an_effect_the_handler_does_not_perform_fails_build() {
    let rendered = row_refusal(|| {
        let guarded_handler = OneEffectHandler::new();
        let effect_b_limit = rate_limit(10.0).for_effect::<RequestB>();
        effectful_transform!(CompositionInput -> CompositionFact with effect_b_limit => guarded_handler)
    })
    .await;
    assert!(
        rendered.contains(&format!(
            "undeclared effect '{}'; `.for_effect::<E>()` must name an effect its handler performs",
            "effect_control_composition.b"
        )),
        "{rendered}"
    );
}

#[tokio::test]
async fn middleware_for_an_effect_on_a_stage_that_performs_none_fails_build() {
    let rendered = row_refusal(|| {
        let guarded_handler = NoEffectHandler;
        let effect_a_limit = rate_limit(10.0).for_effect::<RequestA>();
        effectful_transform!(CompositionInput -> CompositionFact with effect_a_limit => guarded_handler)
    })
    .await;
    assert!(
        rendered.contains(
            "stage 'guarded' performs no effects, so middleware built with `.for_effect::<E>()` \
             has nothing to guard"
        ),
        "{rendered}"
    );

    let source_limit = rate_limit(10.0).for_effect::<RequestA>();
    let error = build(FlowDefinition::materialize(move |_| {
        Ok(flow! {
            name: "effect_scoped_source",
            journals: memory_journals(),

            stages: {
                input = source!(CompositionInput with source_limit => placeholder!());
                output = sink!(CompositionInput => placeholder!());
            },

            topology: {
                input |> output;
            }
        })
    }))
    .await
    .expect_err("a source performs no effects");
    let rendered = error.to_string();
    assert!(
        rendered.contains(
            "stage 'input' performs no effects, so middleware built with `.for_effect::<E>()` \
             has nothing to guard"
        ),
        "{rendered}"
    );
}
