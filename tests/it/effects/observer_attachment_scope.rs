// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115m Part 2 B1: one effect-observer declaration is one logical
//! attachment across all of its subject-specific materialisations.

use async_trait::async_trait;
use obzenflow::stages::sources::ValuesSource;
use obzenflow_adapters::middleware::{
    effect_observer, MiddlewareAttachmentRequest, MiddlewareDeclaration, MiddlewareFactory,
    MiddlewareFactoryError, MiddlewareFactoryResult, MiddlewareMaterializationContext,
    MiddlewareOverrideKey, MiddlewareSurfaceAttachment, MiddlewareSurfaceKind,
};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{effectful_transform, flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::effects::{perform, EffectError, Effects, Port, StageCompletion};
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
use obzenflow_runtime::stages::observer::{
    EffectObserver, EffectObserverContext, HandlerObserver, HandlerObserverContext,
    StageLifecycleObserver, StageLifecycleObserverContext,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input;

impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "effect_observer_scope.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EffectFact {
    subject: String,
}

impl TypedPayload for EffectFact {
    const EVENT_TYPE: &'static str = "effect_observer_scope.fact";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EffectReply {
    subject: String,
}

impl TypedPayload for EffectReply {
    const EVENT_TYPE: &'static str = "effect_observer_scope.reply";
}

const EFFECT_A: &str = "effect_observer_scope.a";
const EFFECT_B: &str = "effect_observer_scope.b";

struct ScopeService;

#[derive(Clone, Debug, Serialize)]
struct RequestA;

#[derive(Clone, Debug, Serialize)]
struct RequestB;

#[obzenflow::port(system = "scope")]
impl ScopeService {
    #[effect(type = "effect_observer_scope.a", exactly_once)]
    fn a(&self, _: &RequestA) -> Result<EffectReply, EffectError> {
        Ok(EffectReply {
            subject: EFFECT_A.to_string(),
        })
    }

    #[effect(type = "effect_observer_scope.b", exactly_once)]
    fn b(&self, _: &RequestB) -> Result<EffectReply, EffectError> {
        Ok(EffectReply {
            subject: EFFECT_B.to_string(),
        })
    }
}

#[obzenflow::command]
#[derive(Clone, Debug)]
struct PerformsBothEffects {
    #[port]
    service: Port<ScopeService>,
    repetitions: usize,
}

impl PerformsBothEffects {
    fn new(repetitions: usize) -> Self {
        Self {
            service: Port::to(ScopeService),
            repetitions,
        }
    }
}

#[async_trait]
impl EffectfulTransformHandler for PerformsBothEffects {
    type Input = Input;
    type Output = obzenflow_core::outcome![EffectFact | EffectReply];

    async fn process(
        &self,
        _input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        for _ in 0..self.repetitions {
            fx.call(RequestA).await?;
            fx.call(RequestB).await?;
        }
        fx.emit(EffectFact {
            subject: "both-effects-completed".to_string(),
        })
        .await?;
        Ok(fx.complete()?)
    }
}

#[derive(Clone, Debug, Serialize)]
struct Delivered;

struct CountDeliveries {
    deliveries: Arc<AtomicUsize>,
}

#[obzenflow_runtime::effects::port(system = "scope-delivery-counter")]
impl CountDeliveries {
    #[effect(type = "effect_observer_scope.deliver", at_least_once, records = reply)]
    fn deliver(&self, _: &Delivered) -> Result<(), EffectError> {
        self.deliveries.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct RecordsEffects {
    calls: Arc<Mutex<Vec<String>>>,
}

impl EffectObserver for RecordsEffects {
    fn after_effect(&self, ctx: &EffectObserverContext<'_>) {
        self.calls
            .lock()
            .expect("recording observer call lock")
            .push(ctx.effect_type().to_string());
    }
}

struct MultiSurfaceObserver {
    calls: Arc<Mutex<Vec<&'static str>>>,
    fail_at: &'static str,
}

impl MultiSurfaceObserver {
    fn record(&self, surface: &'static str) {
        self.calls
            .lock()
            .expect("multi-surface call lock")
            .push(surface);
        if surface == self.fail_at {
            panic!("intentional observer panic at {surface}");
        }
    }
}

impl HandlerObserver for MultiSurfaceObserver {
    fn before_handle(&self, _ctx: &HandlerObserverContext<'_>) {
        self.record("handler");
    }

    fn after_handle(
        &self,
        _ctx: &HandlerObserverContext<'_>,
        _outputs: &[obzenflow_core::ChainEvent],
    ) {
        self.record("handler-after");
    }
}

impl EffectObserver for MultiSurfaceObserver {
    fn after_effect(&self, _ctx: &EffectObserverContext<'_>) {
        self.record("effect");
    }
}

impl StageLifecycleObserver for MultiSurfaceObserver {
    fn on_stage_lifecycle(&self, _ctx: &StageLifecycleObserverContext<'_>) {
        self.record("lifecycle");
    }
}

struct MultiSurfaceFactory(Arc<MultiSurfaceObserver>);

impl MiddlewareFactory for MultiSurfaceFactory {
    fn label(&self) -> &'static str {
        "shared-label"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<Self>(self.label())
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::observer(
            self.label(),
            vec![
                MiddlewareSurfaceKind::Handler,
                MiddlewareSurfaceKind::Effect,
                MiddlewareSurfaceKind::StageLifecycle,
            ],
        )
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        match request.surface.kind() {
            MiddlewareSurfaceKind::Handler => Ok(MiddlewareSurfaceAttachment::handler_observer(
                self.0.clone(),
            )),
            MiddlewareSurfaceKind::Effect => {
                Ok(MiddlewareSurfaceAttachment::effect_observer(self.0.clone()))
            }
            MiddlewareSurfaceKind::StageLifecycle => Ok(
                MiddlewareSurfaceAttachment::stage_lifecycle_observer(self.0.clone()),
            ),
            other => Err(MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                std::io::Error::other(format!("unexpected observer surface {other:?}")),
            )),
        }
    }
}

#[tokio::test]
async fn one_authored_observer_shares_quarantine_across_handler_effect_and_lifecycle() {
    for fail_at in ["handler", "effect", "lifecycle"] {
        let broad_calls = Arc::new(Mutex::new(Vec::new()));
        let named_calls = Arc::new(Mutex::new(Vec::new()));
        let deliveries = Arc::new(AtomicUsize::new(0));
        let broad = MultiSurfaceFactory(Arc::new(MultiSurfaceObserver {
            calls: broad_calls.clone(),
            fail_at,
        }));
        let named = effect_observer(
            "shared-label",
            RecordsEffects {
                calls: named_calls.clone(),
            },
        )
        .for_effect::<RequestA>();
        let deliveries_for_flow = deliveries.clone();
        let handle = FlowDefinition::materialize(move |_runtime_config| {
            let input_source = ValuesSource::new([Input]);
            let observed_handler = PerformsBothEffects::new(2);
            let output_sink =
                perform(|_: &EffectFact| Delivered).through(Port::to(CountDeliveries {
                    deliveries: deliveries_for_flow,
                }));
            Ok(flow! {
                name: "effect_observer_cross_surface_quarantine",
                journals: memory_journals(),
                stages: {
                    input = source!(Input => input_source);
                    observed = effectful_transform!(
                        Input -> { EffectFact, EffectReply }
                        with { broad, named }
                        => observed_handler);
                    output = sink!(EffectFact => output_sink);
                },
                topology: {
                    input |> observed;
                    observed |> output;
                }
            })
        })
        .build(FlowBuildContext::for_tests())
        .await
        .expect("one multi-surface observer is one valid attachment");
        handle
            .run()
            .await
            .expect("observer panic does not change flow completion");

        let calls = broad_calls.lock().expect("multi-surface assertion lock");
        let first_failure = calls
            .iter()
            .position(|surface| *surface == fail_at)
            .expect("configured failure surface was invoked");
        assert_eq!(first_failure + 1, calls.len(),
            "a failure on {fail_at} must suppress every later callback across all materialized surfaces: {calls:?}");
        assert_eq!(
            *named_calls.lock().unwrap(),
            [EFFECT_A, EFFECT_A],
            "the named observer with the same label is a separate attachment"
        );
        assert_eq!(deliveries.load(Ordering::SeqCst), 1);
    }
}
