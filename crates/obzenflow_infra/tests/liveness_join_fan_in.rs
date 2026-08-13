// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::{SystemEvent, SystemEventType};
use obzenflow_core::journal::Journal;
use obzenflow_core::StageId;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{async_source, flow, join, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::prelude::FlowHandle;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, JoinReferenceView, SinkDescription, SinkTerminalOutcome, SinkWriteContext,
    SinkWriteReport, TypedAsyncFiniteSourceHandler, TypedJoinHandler,
};
use obzenflow_runtime::stages::LivenessSnapshots;
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};

/// File-local payloads for the join-fan-in test. The two legs (reference
/// and stream) carry semantically different events; declaring them as
/// distinct types is the FLOWIP-114c correct way to model a join's two
/// concrete inputs.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct CatalogRecord {
    kind: String,
    value: u64,
}

impl TypedPayload for CatalogRecord {
    const EVENT_TYPE: &'static str = "catalog.record";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LiveEvent {
    kind: String,
    value: u64,
}

impl TypedPayload for LiveEvent {
    const EVENT_TYPE: &'static str = "stream.live_event";
}

/// The join's output type (this test uses a `NoopSink`, but the type slot
/// still needs declaring).
#[derive(Clone, Debug, Serialize, Deserialize)]
struct EnrichedRecord {
    kind: String,
    value: u64,
}

impl TypedPayload for EnrichedRecord {
    const EVENT_TYPE: &'static str = "join.enriched_record";
}
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Debug)]
struct OneRefEventSource {
    emitted: bool,
}

impl OneRefEventSource {
    fn new() -> Self {
        Self { emitted: false }
    }
}

impl obzenflow_runtime::stages::common::handlers::TypedFiniteSourceHandler for OneRefEventSource {
    type Output = CatalogRecord;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(vec![CatalogRecord {
            kind: "ref".to_string(),
            value: 1,
        }]))
    }
}

#[derive(Clone, Debug)]
struct DelayedStreamSource {
    emitted: bool,
}

impl DelayedStreamSource {
    fn new() -> Self {
        Self { emitted: false }
    }
}

#[async_trait]
impl TypedAsyncFiniteSourceHandler for DelayedStreamSource {
    type Output = LiveEvent;

    async fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }

        self.emitted = true;
        tokio::time::sleep(Duration::from_secs(1)).await;

        Ok(Some(vec![LiveEvent {
            kind: "stream".to_string(),
            value: 2,
        }]))
    }
}

#[derive(Clone, Debug)]
struct SlowJoin;

impl TypedJoinHandler for SlowJoin {
    type State = ();
    type ReferenceKey = u64;
    type Reference = CatalogRecord;
    type Stream = LiveEvent;
    type Output = EnrichedRecord;

    fn initial_state(&self) -> Self::State {}

    fn admit_reference(&self, reference: &Self::Reference) -> Result<u64, HandlerError> {
        Ok(reference.value)
    }

    fn process_stream(
        &self,
        _state: &mut Self::State,
        _references: &mut JoinReferenceView<'_, u64, CatalogRecord>,
        stream: LiveEvent,
    ) -> Result<Vec<EnrichedRecord>, HandlerError> {
        std::thread::sleep(Duration::from_secs(8));
        Ok(vec![EnrichedRecord {
            kind: stream.kind,
            value: stream.value,
        }])
    }
}

#[derive(Clone, Debug)]
struct NoopSink;

#[async_trait]
impl InlineSink for NoopSink {
    type Input = EnrichedRecord;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        _input: EnrichedRecord,
        _context: SinkWriteContext,
    ) -> Result<SinkWriteReport, HandlerError> {
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("Noop".to_string()),
            None,
        )))
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
async fn liveness_join_keeps_active_edge_healthy_while_other_edge_idles() {
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

    let flow_definition = FlowDefinition::materialize(move |_runtime_config| {
        let reference_source = OneRefEventSource::new();
        let stream_source = DelayedStreamSource::new();
        let slow_join = SlowJoin;
        let noop_sink = NoopSink;

        Ok(flow! {
            name: "liveness_join_fan_in",
            journals: memory_journals(),

            stages: {
                ref_src = source!(CatalogRecord => reference_source);
                stream_src = async_source!(LiveEvent => stream_source);
                joiner = join!(catalog ref_src: CatalogRecord, LiveEvent -> EnrichedRecord => slow_join);
                snk = sink!(EnrichedRecord => noop_sink);
            },

            topology: {
                stream_src |> joiner;
                joiner |> snk;
            }
        })
    });

    let run_handle = tokio::spawn(async move {
        FlowApplication::builder()
            .with_cli_args(["obzenflow"])
            .with_flow_handle_hook(hook)
            .run_async(flow_definition)
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
