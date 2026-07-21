// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115n A4: exclusive built-in effect-control authority is validated
//! over the complete stage-plus-inline declaration set before materialisation.

use async_trait::async_trait;
use obzenflow_adapters::middleware::observer::EffectObserver;
use obzenflow_adapters::middleware::{
    validate_attachment_request, CircuitBreaker, EffectAttemptOutcome, EffectPolicy,
    EffectPolicyAttachment, EffectResilience, MiddlewareAttachmentRequest, MiddlewareContext,
    MiddlewareDeclaration, MiddlewareFactory, MiddlewareFactoryResult, MiddlewareHints,
    MiddlewareMaterializationContext, MiddlewareOverrideKey, MiddlewareSafety,
    MiddlewareSurfaceAttachment, MiddlewareSurfaceKind, PolicyAdmission, RateLimiterBuilder,
    TopologyMiddlewareConfigSlot,
};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{effectful_transform, flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::effects::{Effect, EffectContext, EffectError, EffectSafety, Effects};
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::runtime_config::DslConfigDefault;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
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

    type Outcome = CompositionFact;

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

    type Outcome = CompositionFact;

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
        Ok(MiddlewareSurfaceAttachment::EffectObserver(Arc::new(
            ProofObserver,
        )))
    }
}

struct ProofObserver;

impl EffectObserver for ProofObserver {
    fn label(&self) -> &'static str {
        "a4_effect_observer"
    }
}

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
        Ok(MiddlewareSurfaceAttachment::Effect(
            EffectPolicyAttachment::neutral(Arc::new(OrdinaryControl)),
        ))
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
    (middleware: [$($middleware:expr),* $(,)?], policies: [$($policy:expr),* $(,)?]) => {
        flow! {
            name: "effect_control_composition_single",
            journals: memory_journals(),
            middleware: [],

            stages: {
                input = source!(CompositionInput => placeholder!());
                guarded = effectful_transform!(
                    CompositionInput -> CompositionFact => OneEffectHandler,
                    effects: [EffectA with [$($policy),*]],
                    middleware: [$($middleware),*]
                );
                output = sink!(CompositionFact => placeholder!());
            },

            topology: {
                input |> guarded;
                guarded |> output;
            }
        }
    };
}

macro_rules! two_effect_flow {
    (effect_a: [$($effect_a:expr),* $(,)?], effect_b: [$($effect_b:expr),* $(,)?]) => {
        flow! {
            name: "effect_control_composition_two_effects",
            journals: memory_journals(),
            middleware: [],

            stages: {
                input = source!(CompositionInput => placeholder!());
                guarded = effectful_transform!(
                    CompositionInput -> CompositionFact => TwoEffectHandler,
                    effects: [
                        EffectA with [$($effect_a),*],
                        EffectB with [$($effect_b),*]
                    ],
                    middleware: []
                );
                output = sink!(CompositionFact => placeholder!());
            },

            topology: {
                input |> guarded;
                guarded |> output;
            }
        }
    };
}

async fn build(flow: FlowDefinition) -> Result<(), obzenflow_dsl::dsl::FlowBuildFailure> {
    flow.build(FlowBuildContext::for_tests()).await.map(|_| ())
}

fn assert_composition_error(error: &obzenflow_dsl::dsl::FlowBuildFailure, message: &str) {
    let rendered = error.to_string();
    assert!(
        rendered.contains("stage 'guarded'")
            && rendered.contains(EffectA::EFFECT_TYPE)
            && rendered.contains(message),
        "unexpected composition diagnostic: {rendered}"
    );
}

#[tokio::test]
async fn inline_aggregate_and_limiter_reject_in_both_orders_before_any_materialization() {
    for aggregate_first in [true, false] {
        let aggregate_materializations = Arc::new(AtomicUsize::new(0));
        let limiter_materializations = Arc::new(AtomicUsize::new(0));
        let observer_materializations = Arc::new(AtomicUsize::new(0));
        let aggregate = CountingFactory::boxed(aggregate(), aggregate_materializations.clone());
        let limiter = CountingFactory::boxed(limiter(), limiter_materializations.clone());
        let (first, second) = if aggregate_first {
            (aggregate, limiter)
        } else {
            (limiter, aggregate)
        };
        let observer: Box<dyn MiddlewareFactory> = Box::new(ProofObserverFactory {
            materializations: observer_materializations.clone(),
        });

        let error = build(single_effect_flow!(
            middleware: [observer],
            policies: [first, second]
        ))
        .await
        .expect_err("aggregate plus standalone limiter must fail the flow build");
        assert_composition_error(&error, "combines EffectResilience");
        assert!(error.to_string().contains("rate_limiter"));
        assert_eq!(aggregate_materializations.load(Ordering::SeqCst), 0);
        assert_eq!(limiter_materializations.load(Ordering::SeqCst), 0);
        assert_eq!(observer_materializations.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn stage_and_inline_authoring_lanes_reject_aggregate_limiter_composition() {
    for aggregate_at_stage in [true, false] {
        let stage_materializations = Arc::new(AtomicUsize::new(0));
        let inline_materializations = Arc::new(AtomicUsize::new(0));
        let (stage, inline) = if aggregate_at_stage {
            (
                CountingFactory::boxed(aggregate(), stage_materializations.clone()),
                CountingFactory::boxed(limiter(), inline_materializations.clone()),
            )
        } else {
            (
                CountingFactory::boxed(limiter(), stage_materializations.clone()),
                CountingFactory::boxed(aggregate(), inline_materializations.clone()),
            )
        };

        let error = build(single_effect_flow!(
            middleware: [stage],
            policies: [inline]
        ))
        .await
        .expect_err("stage and inline declarations must be validated as one set");
        assert_composition_error(&error, "combines EffectResilience");
        assert_eq!(stage_materializations.load(Ordering::SeqCst), 0);
        assert_eq!(inline_materializations.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn duplicate_aggregates_and_equal_standalone_limiters_are_rejected() {
    let duplicate_aggregate = build(single_effect_flow!(
        middleware: [],
        policies: [aggregate(), aggregate()]
    ))
    .await
    .expect_err("two aggregates must fail the flow build");
    assert_composition_error(
        &duplicate_aggregate,
        "declares EffectResilience more than once",
    );

    let duplicate_limiter = build(single_effect_flow!(
        middleware: [],
        policies: [limiter(), limiter()]
    ))
    .await
    .expect_err("two standalone limiters must fail the flow build");
    assert_composition_error(
        &duplicate_limiter,
        "declares standalone rate_limiter more than once",
    );
}

#[tokio::test]
async fn aggregate_remains_composable_with_an_effect_observer_and_ordinary_control() {
    let observer_materializations = Arc::new(AtomicUsize::new(0));
    let control_materializations = Arc::new(AtomicUsize::new(0));
    let observer: Box<dyn MiddlewareFactory> = Box::new(ProofObserverFactory {
        materializations: observer_materializations.clone(),
    });
    let ordinary: Box<dyn MiddlewareFactory> = Box::new(OrdinaryControlFactory {
        materializations: control_materializations.clone(),
    });

    build(single_effect_flow!(
        middleware: [observer],
        policies: [aggregate(), ordinary]
    ))
    .await
    .expect("observer and ordinary custom control must remain composable with the aggregate");

    assert_eq!(observer_materializations.load(Ordering::SeqCst), 1);
    assert_eq!(control_materializations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn aggregate_and_limiter_on_distinct_effects_remain_valid() {
    build(two_effect_flow!(
        effect_a: [aggregate()],
        effect_b: [limiter()]
    ))
    .await
    .expect("exclusive built-in control authority is scoped to the exact effect");
}
