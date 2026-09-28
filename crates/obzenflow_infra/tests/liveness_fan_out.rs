// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::{ChainEvent, ChainPayload, EdgeLivenessState};
use obzenflow_core::journal::Journal;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{async_source, effectful_transform, flow, sink, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::effects::{Effects, StageCompletion};
use obzenflow_runtime::prelude::FlowHandle;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext,
    SinkWriteReport, TypedAsyncFiniteSourceHandler, TypedTransformHandler,
};
use obzenflow_runtime::stages::LivenessSnapshots;
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::supervised_base::SupervisorJournal;
use serde::{Deserialize, Serialize};

/// File-local payloads for the fan-out test. The JSON shape is shared, but each
/// stage authors a distinct fact type, so the stage contracts mirror the actual
/// event identities that move through the fan-out.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProbeEvent {
    value: u64,
}

impl TypedPayload for ProbeEvent {
    const EVENT_TYPE: &'static str = "liveness.fanout.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SlowProbeEvent {
    value: u64,
}

impl TypedPayload for SlowProbeEvent {
    const EVENT_TYPE: &'static str = "liveness.fanout.slow";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FastProbeEvent {
    value: u64,
}

impl TypedPayload for FastProbeEvent {
    const EVENT_TYPE: &'static str = "liveness.fanout.fast";
}
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

#[derive(Clone, Debug)]
struct DelayedTwoEventSource {
    emitted: usize,
    finish_gate: Arc<tokio::sync::Notify>,
}

impl DelayedTwoEventSource {
    fn new(finish_gate: Arc<tokio::sync::Notify>) -> Self {
        Self {
            emitted: 0,
            finish_gate,
        }
    }
}

#[async_trait]
impl TypedAsyncFiniteSourceHandler for DelayedTwoEventSource {
    type Output = ProbeEvent;

    async fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        match self.emitted {
            0 => {
                self.emitted = 1;
                Ok(Some(vec![ProbeEvent { value: 1 }]))
            }
            1 => {
                self.emitted = 2;
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok(Some(vec![ProbeEvent { value: 2 }]))
            }
            _ => {
                // Keep the resumed edge observable until the test driver sees
                // both recoveries. Immediate EOF can otherwise end a branch
                // before the watcher samples it, depending on unrelated tasks.
                self.finish_gate.notified().await;
                Ok(None)
            }
        }
    }
}

#[derive(Clone, Debug)]
struct SlowTransform {
    calls: Arc<AtomicUsize>,
}

impl SlowTransform {
    fn new() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl EffectfulTransformHandler for SlowTransform {
    type Input = ProbeEvent;
    type Output = SlowProbeEvent;
    type AllowedEffects = obzenflow_runtime::effect_set![];

    async fn process(
        &self,
        input: ProbeEvent,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        let call_index = self.calls.fetch_add(1, Ordering::SeqCst);
        if call_index == 0 {
            tokio::time::sleep(Duration::from_secs(50)).await;
        }
        fx.emit(SlowProbeEvent { value: input.value })
            .await
            .map_err(|error| HandlerError::Other(error.to_string()))?;
        Ok(fx.complete()?)
    }
}

#[derive(Clone, Debug)]
struct FastTransform;

impl FastTransform {
    fn new() -> Self {
        Self
    }
}

impl TypedTransformHandler for FastTransform {
    type Input = ProbeEvent;
    type Output = FastProbeEvent;

    fn process(&self, event: ProbeEvent) -> Result<FastProbeEvent, HandlerError> {
        Ok(FastProbeEvent { value: event.value })
    }
}

#[derive(Debug)]
struct NoopSink<T>(std::marker::PhantomData<fn() -> T>);

impl<T> Clone for NoopSink<T> {
    fn clone(&self) -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> NoopSink<T> {
    fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

#[async_trait]
impl<T> InlineSink for NoopSink<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    type Input = T;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        _input: T,
        _context: SinkWriteContext,
    ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("Noop".to_string()),
            None,
        )))
    }
}

fn stage_id_by_name(registry: &LivenessSnapshots, name: &str) -> obzenflow_core::StageId {
    registry.with_read(|guard| {
        guard
            .iter()
            .find_map(|(stage_id, snapshot)| {
                if snapshot.stage_name == name {
                    Some(*stage_id)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| panic!("expected stage '{name}' in liveness snapshots"))
    })
}

#[tokio::test(flavor = "current_thread")]
async fn liveness_fan_out_produces_independent_liveness_transitions() {
    tokio::time::pause();

    type StageJournals = Vec<Arc<dyn Journal<ChainEvent>>>;
    let stage_journals_slot: Arc<Mutex<Option<StageJournals>>> = Arc::new(Mutex::new(None));
    let registry_slot: Arc<Mutex<Option<LivenessSnapshots>>> = Arc::new(Mutex::new(None));
    let stage_journals_slot_hook = stage_journals_slot.clone();
    let mut liveness = liveness_observations::LivenessTrace::default();
    let liveness_source = liveness.source.clone();
    let registry_slot_hook = registry_slot.clone();
    let finish_gate = Arc::new(tokio::sync::Notify::new());
    let source_finish_gate = finish_gate.clone();

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
        let registry = handle
            .liveness_snapshots()
            .expect("liveness snapshots available");
        *registry_slot_hook.lock().expect("registry_slot lock") = Some(registry);
        tokio::spawn(async {})
    });

    let flow_definition = FlowDefinition::materialize(move |_runtime_config| {
        let numbers_handler = DelayedTwoEventSource::new(source_finish_gate);
        let slow_handler = SlowTransform::new();
        let fast_handler = FastTransform::new();
        let slow_sink_handler = NoopSink::<SlowProbeEvent>::new();
        let fast_sink_handler = NoopSink::<FastProbeEvent>::new();

        Ok(flow! {
            name: "liveness_fan_out",
            journals: memory_journals(),

            stages: {
                numbers = async_source!(ProbeEvent => numbers_handler);
                slow = effectful_transform!(
                    ProbeEvent -> SlowProbeEvent => slow_handler,
                    observers: [],
                );
                fast = transform!(ProbeEvent -> FastProbeEvent => fast_handler);
                sink_slow = sink!(SlowProbeEvent => slow_sink_handler);
                sink_fast = sink!(FastProbeEvent => fast_sink_handler);
            },

            topology: {
                numbers |> slow;
                numbers |> fast;
                slow |> sink_slow;
                fast |> sink_fast;
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
    for _ in 0..240 {
        liveness.capture();
        match run_task.poll() {
            Poll::Ready(res) => {
                result = Some(res);
                break;
            }
            Poll::Pending => {
                tokio::time::advance(Duration::from_secs(1)).await;
                tokio::task::yield_now().await;
                liveness.capture();
                let readers = registry_slot.lock().unwrap().as_ref().map(|registry| {
                    registry.with_read(|entries| {
                        ["fast", "slow"].map(|name| {
                            entries.iter().find_map(|(id, snapshot)| {
                                (snapshot.stage_name == name).then_some(*id)
                            })
                        })
                    })
                });
                if let Some([Some(fast), Some(slow)]) = readers {
                    if [fast, slow].iter().all(|expected_reader| {
                        liveness.states.iter().any(|(_, reader, state)| {
                            *reader == *expected_reader && *state == EdgeLivenessState::Recovered
                        })
                    }) {
                        finish_gate.notify_one();
                    }
                }
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

    let registry = registry_slot
        .lock()
        .expect("registry_slot lock")
        .clone()
        .expect("liveness registry captured by hook");

    let slow_id = stage_id_by_name(&registry, "slow");
    let fast_id = stage_id_by_name(&registry, "fast");

    let mut envelopes = Vec::new();
    for journal in stage_journals {
        envelopes.extend(
            journal
                .read_all_unordered()
                .await
                .expect("read stage journal"),
        );
    }

    let mut edge_states_by_reader: HashMap<obzenflow_core::StageId, Vec<EdgeLivenessState>> =
        HashMap::new();

    for (_, reader, state) in &liveness.states {
        edge_states_by_reader
            .entry(*reader)
            .or_default()
            .push(*state);
    }

    let mut contracts = 0;
    for envelope in envelopes {
        if let ChainPayload::Execution(ExecutionPayload::ContractStatus { pass, .. }) =
            &envelope.payload
        {
            contracts += 1;
            assert!(
                *pass,
                "unexpected ContractStatus(pass=false) while exercising fan-out liveness"
            );
        }
    }
    assert!(
        contracts > 0,
        "the owning stage journals contain contract reports"
    );

    let slow_states = edge_states_by_reader
        .get(&slow_id)
        .cloned()
        .unwrap_or_default();
    let fast_states = edge_states_by_reader
        .get(&fast_id)
        .cloned()
        .unwrap_or_default();

    assert!(
        slow_states.contains(&EdgeLivenessState::Suspect),
        "expected slow consumer to emit Suspect during 50s handler call"
    );
    assert!(
        slow_states.contains(&EdgeLivenessState::Recovered),
        "expected slow consumer to emit Recovered after the handler returned"
    );

    assert!(
        fast_states.contains(&EdgeLivenessState::Idle),
        "expected fast consumer to emit Idle during source delay gap"
    );
    assert!(
        fast_states.contains(&EdgeLivenessState::Recovered),
        "expected fast consumer to emit Recovered after source resumed"
    );

    assert!(
        !fast_states.contains(&EdgeLivenessState::Suspect)
            && !fast_states.contains(&EdgeLivenessState::Stalled),
        "fast consumer should not emit Suspect/Stalled"
    );
}

#[path = "support/liveness_observations.rs"]
mod liveness_observations;
