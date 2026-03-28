// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::{ChainEvent, ChainEventFactory, SystemEvent, SystemEventType};
use obzenflow_core::journal::Journal;
use obzenflow_core::{StageId, WriterId};
use obzenflow_dsl::{async_source, flow, join, sink, source};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::prelude::FlowHandle;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    AsyncFiniteSourceHandler, JoinHandler, SinkHandler,
};
use obzenflow_runtime::stages::LivenessSnapshots;
use obzenflow_runtime::stages::SourceError;
use serde_json::json;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Debug)]
struct OneRefEventSource {
    emitted: bool,
    writer_id: WriterId,
}

impl OneRefEventSource {
    fn new() -> Self {
        Self {
            emitted: false,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl obzenflow_runtime::stages::common::handlers::FiniteSourceHandler for OneRefEventSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            "flowip_063e.join.ref",
            json!({ "kind": "ref", "value": 1 }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct DelayedStreamSource {
    emitted: bool,
    writer_id: Option<WriterId>,
}

impl DelayedStreamSource {
    fn new() -> Self {
        Self {
            emitted: false,
            writer_id: None,
        }
    }
}

#[async_trait]
impl AsyncFiniteSourceHandler for DelayedStreamSource {
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = Some(id);
    }

    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }

        self.emitted = true;
        tokio::time::sleep(Duration::from_secs(1)).await;

        let writer_id = self
            .writer_id
            .expect("stream writer_id should be bound by runtime");
        Ok(Some(vec![ChainEventFactory::data_event(
            writer_id,
            "flowip_063e.join.stream",
            json!({ "kind": "stream", "value": 2 }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct SlowJoin;

#[async_trait]
impl JoinHandler for SlowJoin {
    type State = ();

    fn initial_state(&self) -> Self::State {}

    fn process_event(
        &self,
        _state: &mut Self::State,
        event: ChainEvent,
        _source_id: StageId,
        writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        let is_stream = event
            .payload()
            .get("kind")
            .and_then(|v| v.as_str())
            .is_some_and(|k| k == "stream");

        if is_stream {
            std::thread::sleep(Duration::from_secs(8));
        }

        Ok(vec![ChainEventFactory::data_event(
            writer_id,
            "flowip_063e.join.out",
            event.payload().clone(),
        )])
    }

    fn on_source_eof(
        &self,
        _state: &mut Self::State,
        _source_id: StageId,
        _writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![])
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

fn stage_id_by_name(registry: &LivenessSnapshots, name: &str) -> StageId {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flowip_063e_join_keeps_active_edge_healthy_while_other_edge_idles() {
    let system_journal_slot: Arc<Mutex<Option<Arc<dyn Journal<SystemEvent>>>>> =
        Arc::new(Mutex::new(None));
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
        name: "flowip_063e_join_fan_in",
        journals: memory_journals(),
        middleware: [],

        stages: {
            ref_src = source!(OneRefEventSource::new());
            stream_src = async_source!(DelayedStreamSource::new());
            joiner = join!(catalog ref_src => SlowJoin);
            snk = sink!(NoopSink);
        },

        topology: {
            stream_src |> joiner;
            joiner |> snk;
        }
    };

    let run_handle = tokio::spawn(async move {
        FlowApplication::run_with_web_endpoints_and_hooks(flow_definition, Vec::new(), vec![hook])
            .await
    });

    tokio::time::timeout(Duration::from_secs(30), run_handle)
        .await
        .expect("flow did not complete within timeout")
        .expect("flow task join")
        .expect("flow should complete successfully");

    // Give liveness tasks a moment to flush their final transitions.
    tokio::time::sleep(Duration::from_millis(50)).await;

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

    let joiner_id = stage_id_by_name(&registry, "joiner");

    let envelopes = system_journal
        .read_causally_ordered()
        .await
        .expect("read system journal");

    let mut idle_upstreams: HashSet<StageId> = HashSet::new();
    for envelope in envelopes {
        match &envelope.event.event {
            SystemEventType::EdgeLiveness {
                upstream,
                reader,
                state,
                ..
            } => {
                if *reader == joiner_id && *state == obzenflow_core::event::EdgeLivenessState::Idle
                {
                    idle_upstreams.insert(*upstream);
                }
            }
            SystemEventType::ContractStatus { pass, .. } => {
                assert!(
                    *pass,
                    "unexpected ContractStatus(pass=false) while exercising join liveness"
                );
            }
            _ => {}
        }
    }

    assert!(
        !idle_upstreams.is_empty(),
        "expected at least one Idle liveness transition on a non-processing join upstream"
    );
    assert_eq!(
        idle_upstreams.len(),
        1,
        "expected exactly one join upstream edge to become Idle while the other is active"
    );
}
