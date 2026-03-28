// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::{ChainEvent, ChainEventFactory, EdgeLivenessState, SystemEventType};
use obzenflow_core::journal::Journal;
use obzenflow_dsl::{async_source, async_transform, flow, sink};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::prelude::FlowHandle;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    AsyncFiniteSourceHandler, AsyncTransformHandler, SinkHandler,
};
use obzenflow_runtime::stages::LivenessSnapshots;
use obzenflow_runtime::stages::SourceError;
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

#[derive(Clone, Debug)]
struct DelayedTwoEventSource {
    emitted: usize,
    writer_id: Option<obzenflow_core::WriterId>,
}

impl DelayedTwoEventSource {
    fn new() -> Self {
        Self {
            emitted: 0,
            writer_id: None,
        }
    }
}

#[async_trait]
impl AsyncFiniteSourceHandler for DelayedTwoEventSource {
    fn bind_writer_id(&mut self, id: obzenflow_core::WriterId) {
        self.writer_id = Some(id);
    }

    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        let writer_id = self
            .writer_id
            .expect("source writer_id should be bound by runtime");
        match self.emitted {
            0 => {
                self.emitted = 1;
                Ok(Some(vec![ChainEventFactory::data_event(
                    writer_id,
                    "flowip_063e.fanout.input",
                    json!({ "value": 1 }),
                )]))
            }
            1 => {
                self.emitted = 2;
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok(Some(vec![ChainEventFactory::data_event(
                    writer_id,
                    "flowip_063e.fanout.input",
                    json!({ "value": 2 }),
                )]))
            }
            _ => Ok(None),
        }
    }
}

#[derive(Clone, Debug)]
struct SlowTransform {
    writer_id: obzenflow_core::WriterId,
    calls: Arc<AtomicUsize>,
}

impl SlowTransform {
    fn new() -> Self {
        Self {
            writer_id: obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl AsyncTransformHandler for SlowTransform {
    async fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let call_index = self.calls.fetch_add(1, Ordering::SeqCst);
        if call_index == 0 {
            tokio::time::sleep(Duration::from_secs(50)).await;
        }
        Ok(vec![ChainEventFactory::data_event(
            self.writer_id,
            "flowip_063e.fanout.slow",
            event.payload().clone(),
        )])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct FastTransform {
    writer_id: obzenflow_core::WriterId,
}

impl FastTransform {
    fn new() -> Self {
        Self {
            writer_id: obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
        }
    }
}

#[async_trait]
impl AsyncTransformHandler for FastTransform {
    async fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![ChainEventFactory::data_event(
            self.writer_id,
            "flowip_063e.fanout.fast",
            event.payload().clone(),
        )])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct NoopSink;

#[async_trait]
impl SinkHandler for NoopSink {
    async fn consume(&mut self, _event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        Ok(DeliveryPayload::success(
            "noop",
            DeliveryMethod::Custom("Noop".to_string()),
            None,
        ))
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
async fn flowip_063e_fan_out_produces_independent_liveness_transitions() {
    tokio::time::pause();

    let system_journal_slot: Arc<
        Mutex<Option<Arc<dyn Journal<obzenflow_core::event::SystemEvent>>>>,
    > = Arc::new(Mutex::new(None));
    let registry_slot: Arc<Mutex<Option<LivenessSnapshots>>> = Arc::new(Mutex::new(None));
    let system_journal_slot_hook = system_journal_slot.clone();
    let registry_slot_hook = registry_slot.clone();

    let hook = Box::new(move |handle: &Arc<FlowHandle>| {
        let system_journal = handle.system_journal().expect("system journal available");
        *system_journal_slot_hook
            .lock()
            .expect("system_journal_slot lock") = Some(system_journal);
        let registry = handle
            .liveness_snapshots()
            .expect("liveness snapshots available");
        *registry_slot_hook.lock().expect("registry_slot lock") = Some(registry);
        tokio::spawn(async {})
    });

    let flow_definition = flow! {
        name: "flowip_063e_fan_out",
        journals: memory_journals(),
        middleware: [],

        stages: {
            numbers = async_source!(DelayedTwoEventSource::new());
            slow = async_transform!(SlowTransform::new());
            fast = async_transform!(FastTransform::new());
            sink_slow = sink!(NoopSink);
            sink_fast = sink!(NoopSink);
        },

        topology: {
            numbers |> slow;
            numbers |> fast;
            slow |> sink_slow;
            fast |> sink_fast;
        }
    };

    let mut run_task = tokio_test::task::spawn(async move {
        FlowApplication::run_with_web_endpoints_and_hooks(flow_definition, Vec::new(), vec![hook])
            .await
    });

    let mut result = None;
    for _ in 0..240 {
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

    result
        .expect("flow did not complete after advancing tokio time")
        .expect("flow should complete successfully");

    let system_journal = system_journal_slot
        .lock()
        .expect("system_journal_slot lock")
        .clone()
        .expect("system journal captured by hook");

    let registry = registry_slot
        .lock()
        .expect("registry_slot lock")
        .clone()
        .expect("liveness registry captured by hook");

    let slow_id = stage_id_by_name(&registry, "slow");
    let fast_id = stage_id_by_name(&registry, "fast");

    let envelopes = system_journal
        .read_causally_ordered()
        .await
        .expect("read system journal");

    let mut edge_states_by_reader: HashMap<obzenflow_core::StageId, Vec<EdgeLivenessState>> =
        HashMap::new();

    for envelope in envelopes {
        match &envelope.event.event {
            SystemEventType::EdgeLiveness { reader, state, .. } => {
                edge_states_by_reader
                    .entry(*reader)
                    .or_default()
                    .push(*state);
            }
            SystemEventType::ContractStatus { pass, .. } => {
                assert!(
                    *pass,
                    "unexpected ContractStatus(pass=false) while exercising fan-out liveness"
                );
            }
            _ => {}
        }
    }

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
