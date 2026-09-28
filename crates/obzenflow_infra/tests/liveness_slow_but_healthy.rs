// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::observability::{ObservationRecord, ObservationSource};
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

/// File-local payloads for the slow-but-healthy test. The JSON shape is shared,
/// but the source and transform author different fact identities.
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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

#[derive(Clone, Debug)]
struct TwoEventSource {
    next_value: u64,
    remaining: usize,
}

impl TwoEventSource {
    fn new() -> Self {
        Self {
            next_value: 1,
            remaining: 2,
        }
    }
}

impl TypedFiniteSourceHandler for TwoEventSource {
    type Output = ProbeEvent;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.remaining == 0 {
            return Ok(None);
        }

        self.remaining = self.remaining.saturating_sub(1);
        let value = self.next_value;
        self.next_value = self.next_value.saturating_add(1);

        Ok(Some(vec![ProbeEvent { value }]))
    }
}

#[derive(Clone, Debug)]
struct SlowAiTransform {
    calls: Arc<AtomicUsize>,
}

impl SlowAiTransform {
    fn new() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl EffectfulTransformHandler for SlowAiTransform {
    type Input = ProbeEvent;
    type Output = ProbeOutputEvent;
    type AllowedEffects = obzenflow_runtime::effect_set![];

    async fn process(
        &self,
        input: ProbeEvent,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        let call_index = self.calls.fetch_add(1, Ordering::SeqCst);
        if call_index == 0 {
            tokio::time::sleep(Duration::from_secs(50)).await;
        } else if call_index == 1 {
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

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
async fn liveness_slow_but_healthy_completes_and_emits_liveness_transitions() {
    tokio::time::pause();

    type StageJournals = Vec<Arc<dyn Journal<ChainEvent>>>;
    let stage_journals_slot: Arc<Mutex<Option<StageJournals>>> = Arc::new(Mutex::new(None));
    let stage_journals_slot_hook = stage_journals_slot.clone();
    let observations: Arc<Mutex<Option<Arc<dyn ObservationSource>>>> = Arc::new(Mutex::new(None));
    let observations_hook = observations.clone();

    let hook = Box::new(move |handle: &Arc<FlowHandle>| {
        *observations_hook.lock().unwrap() = Some(handle.observations());
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
        let numbers_handler = TwoEventSource::new();
        let slow_ai_handler = SlowAiTransform::new();
        let sink_handler = NoopSink;

        Ok(flow! {
            name: "liveness_slow_but_healthy",
            journals: memory_journals(),

            stages: {
                numbers = source!(ProbeEvent => numbers_handler);
                slow_ai = effectful_transform!(
                    ProbeEvent -> ProbeOutputEvent => slow_ai_handler,
                    observers: [],
                );
                sink = sink!(ProbeOutputEvent => sink_handler);
            },

            topology: {
                numbers |> slow_ai;
                slow_ai |> sink;
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
    let mut saw_suspect = false;
    let mut saw_recovered = false;
    for _ in 0..240 {
        if let Some(source) = observations.lock().unwrap().as_ref() {
            for packet in source.snapshot() {
                for record in packet.records {
                    if let ObservationRecord::EdgeLiveness { state, .. } = record {
                        saw_suspect |= state == EdgeLivenessState::Suspect;
                        saw_recovered |= state == EdgeLivenessState::Recovered;
                    }
                }
            }
        }
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

    if result.is_none() {
        if let Poll::Ready(res) = run_task.poll() {
            result = Some(res);
        }
    }

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

    let mut contracts = 0;
    for envelope in envelopes {
        if let ChainPayload::Execution(ExecutionPayload::ContractStatus { pass, .. }) =
            &envelope.payload
        {
            contracts += 1;
            assert!(
                *pass,
                "unexpected ContractStatus(pass=false) while exercising slow-but-healthy handler"
            );
        }
    }
    assert!(
        contracts > 0,
        "the owning stage journals contain contract reports"
    );

    assert!(
        saw_suspect,
        "expected at least one EdgeLiveness Suspect transition during slow handler call"
    );
    assert!(
        saw_recovered,
        "expected EdgeLiveness Recovered after the handler returned and progress resumed"
    );
}
