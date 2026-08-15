// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115m Part 2 B1: one effect-observer declaration is one logical
//! attachment across all of its subject-specific materialisations.

use async_trait::async_trait;
use obzenflow_adapters::middleware::effect_observer;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{effectful_transform, flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectSafety, Effects, StageCompletion,
};
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext,
    SinkWriteReport, TypedFiniteSourceHandler,
};
use obzenflow_runtime::stages::observer::{EffectObserver, EffectObserverContext};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
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

#[derive(Clone, Debug)]
struct EffectA;

#[async_trait]
impl Effect for EffectA {
    const EFFECT_TYPE: &'static str = "effect_observer_scope.a";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;

    type Outcome = EffectReply;
    type OutcomeSemantics = obzenflow_runtime::effects::RecordedReply;

    fn label(&self) -> &str {
        "effect-a"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({})
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        Ok(EffectReply {
            subject: Self::EFFECT_TYPE.to_string(),
        })
    }
}

#[derive(Clone, Debug)]
struct EffectB;

#[async_trait]
impl Effect for EffectB {
    const EFFECT_TYPE: &'static str = "effect_observer_scope.b";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;

    type Outcome = EffectReply;
    type OutcomeSemantics = obzenflow_runtime::effects::RecordedReply;

    fn label(&self) -> &str {
        "effect-b"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({})
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        Ok(EffectReply {
            subject: Self::EFFECT_TYPE.to_string(),
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

#[derive(Clone, Debug)]
struct PerformsBothEffects;

#[async_trait]
impl EffectfulTransformHandler for PerformsBothEffects {
    type Input = Input;
    type Output = obzenflow_core::stage_fact_set![EffectFact];
    type AllowedEffects = obzenflow_runtime::effect_set![EffectA, EffectB];

    async fn process(
        &self,
        _input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        fx.perform(EffectA).await?;
        fx.perform(EffectB).await?;
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
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        _event: Self::Input,
        _context: SinkWriteContext,
    ) -> Result<SinkWriteReport, HandlerError> {
        self.deliveries.fetch_add(1, Ordering::SeqCst);
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("effect-observer-scope-test".to_string()),
            None,
        )))
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
        if ctx.effect_type() == EffectA::EFFECT_TYPE {
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
        let observed_handler = PerformsBothEffects;
        let output_sink = CountingSink {
            deliveries: deliveries_for_flow,
        };

        Ok(flow! {
            name: "effect_observer_attachment_scope",
            journals: memory_journals(),

            stages: {
                input = source!(Input => input_source);
                observed = effectful_transform!(
                    Input -> EffectFact => observed_handler,
                    effects: [EffectA, EffectB],
                    observers: [
                        effect_observer(
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
                        )
                    ]
                );
                output = sink!(EffectFact => output_sink, delivery: idempotent);
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
        [EffectA::EFFECT_TYPE],
        "a panic on A must quarantine the declaration before B"
    );
    assert_eq!(
        *sibling_calls
            .lock()
            .expect("recording observer assertion lock"),
        [EffectA::EFFECT_TYPE, EffectB::EFFECT_TYPE],
        "one declaration must receive exactly one callback for each matching effect"
    );
    assert_eq!(
        deliveries.load(Ordering::SeqCst),
        1,
        "observer quarantine must not alter either protected effect result"
    );
}
