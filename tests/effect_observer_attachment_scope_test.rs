// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115m Part 2 B1: one effect-observer declaration is one logical
//! attachment across all of its subject-specific materialisations.

use async_trait::async_trait;
use obzenflow_adapters::middleware::{
    effect_observer, MiddlewareAttachmentRequest, MiddlewareDeclaration, MiddlewareFactory,
    MiddlewareFactoryError, MiddlewareFactoryResult, MiddlewareMaterializationContext,
    MiddlewareOverrideKey, MiddlewareSurfaceAttachment, MiddlewareSurfaceKind,
};
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{effectful_transform, flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::effects::{EffectError, Effects, StageCompletion, System};
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, InlineSink, SinkDescription, SinkWriteFailure,
    TypedFiniteSourceHandler,
};
use obzenflow_runtime::stages::observer::{
    EffectObserver, EffectObserverContext, HandlerObserver, HandlerObserverContext,
    StageLifecycleObserver, StageLifecycleObserverContext,
};
use obzenflow_runtime::stages::SourceError;
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

#[obzenflow::port]
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

#[derive(Clone, Debug)]
struct OneInputSource {
    emitted: bool,
}

impl TypedFiniteSourceHandler for OneInputSource {
    type Output = Input;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.emitted {
            Ok(None)
        } else {
            self.emitted = true;
            Ok(Some(vec![Input]))
        }
    }
}

#[derive(Clone, Debug, obzenflow::effects::EffectHandler)]
struct PerformsBothEffects {
    #[system]
    service: System<ScopeService>,
    repetitions: usize,
}

impl PerformsBothEffects {
    fn new(repetitions: usize) -> Self {
        Self {
            service: System::eager("scope", Arc::new(ScopeService)).expect("a valid system name"),
            repetitions,
        }
    }
}

#[async_trait]
impl EffectfulTransformHandler for PerformsBothEffects {
    type Input = Input;
    type Output = obzenflow_core::stage_fact_set![EffectFact, EffectReply];

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

#[derive(Clone, Debug)]
struct CountingSink {
    deliveries: Arc<AtomicUsize>,
}

#[async_trait]
impl InlineSink for CountingSink {
    type Input = EffectFact;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Custom(
            "effect-observer-scope-test".to_string(),
        ))
    }

    async fn write(&mut self, _event: Self::Input) -> Result<(), SinkWriteFailure> {
        self.deliveries.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct PanicsOnEffectA {
    calls: Arc<Mutex<Vec<String>>>,
}

impl EffectObserver for PanicsOnEffectA {
    fn after_effect(&self, ctx: &EffectObserverContext<'_>) {
        self.calls
            .lock()
            .expect("panicking observer call lock")
            .push(ctx.effect_type().to_string());
        if ctx.effect_type() == EFFECT_A {
            panic!("intentional effect observer panic");
        }
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

#[tokio::test]
async fn public_effect_observer_path_keys_dispatch_and_shares_declaration_quarantine() {
    let panicking_calls = Arc::new(Mutex::new(Vec::new()));
    let sibling_calls = Arc::new(Mutex::new(Vec::new()));
    let deliveries = Arc::new(AtomicUsize::new(0));
    let panicking_calls_for_flow = panicking_calls.clone();
    let sibling_calls_for_flow = sibling_calls.clone();
    let deliveries_for_flow = deliveries.clone();

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let input_source = OneInputSource { emitted: false };
        let observed_handler = PerformsBothEffects::new(1);
        let output_sink = CountingSink {
            deliveries: deliveries_for_flow,
        };

        Ok(flow! {
            name: "effect_observer_attachment_scope",
            journals: memory_journals(),

            stages: {
                input = source!(Input => input_source);
                observed = effectful_transform!(
                    Input -> { EffectFact, EffectReply }
                    with { effect_observer(
                            "panics-on-a",
                            PanicsOnEffectA {
                                calls: panicking_calls_for_flow,
                            }
                        ),
                        effect_observer(
                            "records-effects",
                            RecordsEffects {
                                calls: sibling_calls_for_flow,
                            }
                        ) }
                    => observed_handler);
                output = sink!(EffectFact => output_sink.idempotent());
            },

            topology: {
                input |> observed;
                observed |> output;
            }
        })
    })
    .build(FlowBuildContext::for_tests())
    .await
    .expect("two-effect observer flow builds");

    handle.run().await.expect("two-effect observer flow runs");

    assert_eq!(
        *panicking_calls
            .lock()
            .expect("panicking observer assertion lock"),
        [EFFECT_A],
        "a panic on A must quarantine the declaration before B"
    );
    assert_eq!(
        *sibling_calls
            .lock()
            .expect("recording observer assertion lock"),
        [EFFECT_A, EFFECT_B],
        "one declaration must receive exactly one callback for each matching effect"
    );
    assert_eq!(
        deliveries.load(Ordering::SeqCst),
        1,
        "observer quarantine must not alter either protected effect result"
    );
}

struct ScopedRecorder {
    calls: Arc<Mutex<Vec<String>>>,
    panic_on_a: bool,
}

impl EffectObserver for ScopedRecorder {
    fn after_effect(&self, ctx: &EffectObserverContext<'_>) {
        self.calls
            .lock()
            .expect("scoped observer call lock")
            .push(ctx.effect_type().to_string());
        if self.panic_on_a && ctx.effect_type() == EFFECT_A {
            panic!("intentional scoped effect observer panic");
        }
    }
}

#[tokio::test]
async fn same_label_at_implementation_and_effect_sites_has_independent_quarantine() {
    use obzenflow_topology::{MiddlewareAuthoredSite, MiddlewareOperation};

    let mut stable_keys = None;
    for implementation_panics in [false, true] {
        for reverse_group in [false, true] {
            let implementation_calls = Arc::new(Mutex::new(Vec::new()));
            let effect_calls = Arc::new(Mutex::new(Vec::new()));
            let implementation_sibling_calls = Arc::new(Mutex::new(Vec::new()));
            let effect_sibling_calls = Arc::new(Mutex::new(Vec::new()));
            let deliveries = Arc::new(AtomicUsize::new(0));
            let implementation = effect_observer(
                "shared-label",
                ScopedRecorder {
                    calls: implementation_calls.clone(),
                    panic_on_a: implementation_panics,
                },
            );
            let named = effect_observer(
                "shared-label",
                ScopedRecorder {
                    calls: effect_calls.clone(),
                    panic_on_a: !implementation_panics,
                },
            )
            .for_effect::<RequestA>();
            let implementation_sibling = effect_observer(
                "implementation-sibling",
                ScopedRecorder {
                    calls: implementation_sibling_calls.clone(),
                    panic_on_a: false,
                },
            );
            let named_sibling = effect_observer(
                "effect-sibling",
                ScopedRecorder {
                    calls: effect_sibling_calls.clone(),
                    panic_on_a: false,
                },
            )
            .for_effect::<RequestA>();
            let (implementation_first, implementation_second, named_first, named_second) =
                if reverse_group {
                    (implementation_sibling, implementation, named_sibling, named)
                } else {
                    (implementation, implementation_sibling, named, named_sibling)
                };
            let deliveries_for_flow = deliveries.clone();
            let handle = FlowDefinition::materialize(move |_runtime_config| {
                let input_source = OneInputSource { emitted: false };
                let observed_handler = PerformsBothEffects::new(2);
                let output_sink = CountingSink { deliveries: deliveries_for_flow };
                Ok(flow! {
                    name: "effect_observer_site_quarantine",
                    journals: memory_journals(),
                    stages: {
                        input = source!(Input => input_source);
                        observed = effectful_transform!(
                            Input -> { EffectFact, EffectReply }
                            with { implementation_first, implementation_second, named_first, named_second }
                            => observed_handler);
                        output = sink!(EffectFact => output_sink.idempotent());
                    },
                    topology: {
                        input |> observed;
                        observed |> output;
                    }
                })
            })
            .build(FlowBuildContext::for_tests())
            .await
            .expect("same observer label at distinct authored sites is legal");

            let topology = handle.topology().expect("resolved topology");
            let attachments = &topology
                .stages()
                .find(|stage| stage.name == "observed")
                .expect("observed stage")
                .middleware
                .as_ref()
                .expect("resolved observer membership")
                .attachments;
            let matching = attachments
                .iter()
                .filter(|attachment| {
                    attachment.label == "shared-label"
                        && attachment.operation
                            == MiddlewareOperation::Effect {
                                effect_type: EFFECT_A.to_string(),
                            }
                })
                .collect::<Vec<_>>();
            assert_eq!(
                matching.len(),
                2,
                "both authored sites must survive resolution"
            );
            assert_ne!(matching[0].key, matching[1].key);
            assert!(matching.iter().any(
                |attachment| attachment.authored_site == MiddlewareAuthoredSite::Implementation
            ));
            assert!(matching.iter().any(|attachment| attachment.authored_site
                == MiddlewareAuthoredSite::Effect {
                    effect_type: EFFECT_A.to_string(),
                }));
            let mut keys = attachments
                .iter()
                .map(|attachment| attachment.key)
                .collect::<Vec<_>>();
            keys.sort();
            if let Some(expected) = &stable_keys {
                assert_eq!(
                    &keys, expected,
                    "group permutation and callback health must preserve identity"
                );
            } else {
                stable_keys = Some(keys);
            }

            handle
                .run()
                .await
                .expect("observer quarantine preserves flow completion");
            let every_effect = vec![EFFECT_A, EFFECT_B, EFFECT_A, EFFECT_B];
            let named_effect = vec![EFFECT_A, EFFECT_A];
            assert_eq!(
                *implementation_calls.lock().unwrap(),
                if implementation_panics {
                    vec![EFFECT_A]
                } else {
                    every_effect.clone()
                },
                "named-effect quarantine must not suppress implementation callbacks"
            );
            assert_eq!(
                *effect_calls.lock().unwrap(),
                if implementation_panics {
                    named_effect.clone()
                } else {
                    vec![EFFECT_A]
                },
                "implementation quarantine must not suppress the named attachment"
            );
            assert_eq!(*implementation_sibling_calls.lock().unwrap(), every_effect);
            assert_eq!(*effect_sibling_calls.lock().unwrap(), named_effect);
            assert_eq!(deliveries.load(Ordering::SeqCst), 1);
        }
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
            let input_source = OneInputSource { emitted: false };
            let observed_handler = PerformsBothEffects::new(2);
            let output_sink = CountingSink {
                deliveries: deliveries_for_flow,
            };
            Ok(flow! {
                name: "effect_observer_cross_surface_quarantine",
                journals: memory_journals(),
                stages: {
                    input = source!(Input => input_source);
                    observed = effectful_transform!(
                        Input -> { EffectFact, EffectReply }
                        with { broad, named }
                        => observed_handler);
                    output = sink!(EffectFact => output_sink.idempotent());
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
