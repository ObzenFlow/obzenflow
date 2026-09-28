// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::{ChainEvent, ChainPayload, EdgeLivenessState};
use obzenflow_core::journal::Journal;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{effectful_transform, flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::effects::{Effects, StageCompletion};
use obzenflow_runtime::prelude::FlowHandle;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext,
    SinkWriteReport, TypedFiniteSourceHandler,
};
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::supervised_base::SupervisorJournal;
use serde::{Deserialize, Serialize};

/// File-local payloads for the stalled-transition test. The JSON shape is
/// shared, but the source and transform author different fact identities.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProbeEvent {
    value: u64,
}

impl TypedPayload for ProbeEvent {
    const EVENT_TYPE: &'static str = "liveness.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProbeOutputEvent {
    value: u64,
}

impl TypedPayload for ProbeOutputEvent {
    const EVENT_TYPE: &'static str = "liveness.output";
}
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

#[derive(Clone, Debug)]
struct OneEventSource {
    emitted: bool,
}

impl OneEventSource {
    fn new() -> Self {
        Self { emitted: false }
    }
}

impl TypedFiniteSourceHandler for OneEventSource {
    type Output = ProbeEvent;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(vec![ProbeEvent { value: 1 }]))
    }
}

#[derive(Clone, Debug)]
struct StallingTransform;

impl StallingTransform {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl EffectfulTransformHandler for StallingTransform {
    type Input = ProbeEvent;
    type Output = ProbeOutputEvent;
    type AllowedEffects = obzenflow_runtime::effect_set![];

    async fn process(
        &self,
        input: ProbeEvent,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        tokio::time::sleep(Duration::from_secs(130)).await;
        fx.emit(ProbeOutputEvent { value: input.value })
            .await
            .map_err(|error| HandlerError::Other(error.to_string()))?;
        Ok(fx.complete()?)
    }
}

#[derive(Clone, Debug)]
struct NoopSink;

#[async_trait]
impl InlineSink for NoopSink {
    type Input = ProbeOutputEvent;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        _input: ProbeOutputEvent,
        _context: SinkWriteContext,
    ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("Noop".to_string()),
            None,
        )))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn liveness_emits_stalled_transition_without_aborting_pipeline() {
    tokio::time::pause();

    type StageJournals = Vec<Arc<dyn Journal<ChainEvent>>>;
    let stage_journals_slot: Arc<Mutex<Option<StageJournals>>> = Arc::new(Mutex::new(None));
    let stage_journals_slot_hook = stage_journals_slot.clone();
    let mut liveness = liveness_observations::LivenessTrace::default();
    let liveness_source = liveness.source.clone();

    let hook = Box::new(move |handle: &Arc<FlowHandle>| {
        *liveness_source.lock().unwrap() = Some(handle.observations());
        let stage_journals = handle
            .report_journals()
            .into_iter()
            .filter_map(|journal| match journal {
                SupervisorJournal::Stage { journal, .. } => Some(journal),
                SupervisorJournal::System(_) => None,
            })
            .collect();
        *stage_journals_slot_hook
            .lock()
            .expect("stage_journals_slot lock") = Some(stage_journals);
        tokio::spawn(async {})
    });

    let flow_definition = FlowDefinition::materialize(move |_runtime_config| {
        let numbers_handler = OneEventSource::new();
        let slow_handler = StallingTransform::new();
        let sink_handler = NoopSink;

        Ok(flow! {
            name: "liveness_stalled_transition",
            journals: memory_journals(),

            stages: {
                numbers = source!(ProbeEvent => numbers_handler);
                slow = effectful_transform!(
                    ProbeEvent -> ProbeOutputEvent => slow_handler,
                    observers: [],
                );
                sink = sink!(ProbeOutputEvent => sink_handler);
            },

            topology: {
                numbers |> slow;
                slow |> sink;
            }
        })
    });

    let mut run_task = tokio_test::task::spawn(async move {
        FlowApplication::builder()
            .with_cli_args(["obzenflow"])
            .with_flow_handle_hook(hook)
            .run_async(flow_definition)
            .await
    });

    let mut result = None;
    for _ in 0..300 {
        liveness.capture();
        match run_task.poll() {
            Poll::Ready(res) => {
                result = Some(res);
                break;
            }
            Poll::Pending => {
                tokio::time::advance(Duration::from_secs(1)).await;
                tokio::task::yield_now().await;
            }
        }
    }

    liveness.capture();
    result
        .expect("flow did not complete after advancing tokio time")
        .expect("flow should complete successfully");

    let stage_journals = stage_journals_slot
        .lock()
        .expect("stage_journals_slot lock")
        .clone()
        .expect("stage journals captured by hook");

    let mut envelopes = Vec::new();
    for journal in stage_journals {
        envelopes.extend(
            journal
                .read_all_unordered()
                .await
                .expect("read stage journal"),
        );
    }

    let saw_stalled = liveness
        .states
        .iter()
        .any(|(_, _, state)| *state == EdgeLivenessState::Stalled);
    let saw_recovered = liveness
        .states
        .iter()
        .any(|(_, _, state)| *state == EdgeLivenessState::Recovered);
    let mut contracts = 0;
    for envelope in envelopes {
        if let ChainPayload::Execution(ExecutionPayload::ContractStatus { pass, .. }) =
            &envelope.payload
        {
            contracts += 1;
            assert!(
                *pass,
                "unexpected ContractStatus(pass=false) while exercising stalled transition"
            );
        }
    }
    assert!(
        contracts > 0,
        "the owning stage journals contain contract reports"
    );

    assert!(
        saw_stalled,
        "expected EdgeLiveness Stalled during 130s handler call"
    );
    assert!(
        saw_recovered,
        "expected EdgeLiveness Recovered after handler returned and progress resumed"
    );
}

#[path = "support/liveness_observations.rs"]
mod liveness_observations;
