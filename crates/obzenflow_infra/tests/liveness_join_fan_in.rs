// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::{ChainEvent, ChainPayload, EdgeLivenessState};
use obzenflow_core::journal::Journal;
use obzenflow_core::{StageId, TypedPayload};
use obzenflow_dsl::{async_source, flow, join, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::id_conversions::StageIdExt;
use obzenflow_runtime::prelude::FlowHandle;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, JoinReferenceView, SinkDescription, SinkWriteFailure,
    TypedAsyncFiniteSourceHandler, TypedJoinHandler,
};
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
use std::collections::{HashMap, HashSet};
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
struct SlowJoin {
    release: Arc<Mutex<std::sync::mpsc::Receiver<()>>>,
}

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
        // Keep this synchronous invocation in flight until the test observes
        // the idle reference edge. Dropping the sender also releases this wait
        // if the driver fails, so runtime shutdown cannot strand the worker.
        let _ = self.release.lock().unwrap().recv();
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
        SinkDescription::method(DeliveryMethod::Custom("Noop".to_string()))
    }

    async fn write(&mut self, _input: EnrichedRecord) -> Result<(), SinkWriteFailure> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn liveness_join_keeps_active_edge_healthy_while_other_edge_idles() {
    type StageJournals = Vec<Arc<dyn Journal<ChainEvent>>>;
    let stage_journals_slot: Arc<Mutex<Option<StageJournals>>> = Arc::new(Mutex::new(None));
    let stage_ids = Arc::new(Mutex::new(HashMap::<String, StageId>::new()));
    let stage_journals_slot_hook = stage_journals_slot.clone();
    let mut liveness = liveness_observations::LivenessTrace::default();
    let liveness_source = liveness.source.clone();
    let stage_ids_hook = stage_ids.clone();
    let (release_join, join_release) = std::sync::mpsc::channel();
    let join_release = Arc::new(Mutex::new(join_release));

    let hook = Box::new(move |handle: &Arc<FlowHandle>| {
        *liveness_source.lock().unwrap() = Some(handle.observations());
        let stage_journals = handle
            .stage_journals()
            .into_iter()
            .map(|(_, journal)| journal)
            .collect();
        *stage_journals_slot_hook
            .lock()
            .expect("stage_journals_slot lock") = Some(stage_journals);
        *stage_ids_hook.lock().unwrap() = handle
            .topology()
            .expect("flow topology")
            .stages()
            .map(|stage| (stage.name.clone(), StageId::from_topology_id(stage.id)))
            .collect();
        tokio::spawn(async {})
    });

    let flow_definition = FlowDefinition::materialize(move |_runtime_config| {
        let reference_source = OneRefEventSource::new();
        let stream_source = DelayedStreamSource::new();
        let slow_join = SlowJoin {
            release: join_release,
        };
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
                (ref_src, stream_src) |> joiner;
                joiner |> snk;
            }
        })
    });

    let mut run_handle = tokio::spawn(async move {
        FlowApplication::builder()
            .with_cli_args(["obzenflow"])
            .with_flow_handle_hook(hook)
            .run_async(flow_definition)
            .await
    });

    let mut released = false;
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            tokio::select! {
                result = &mut run_handle => break result,
                _ = tokio::time::sleep(Duration::from_millis(10)) => {
                    liveness.capture();
                    if !released {
                        let observed_idle = stage_ids.lock().unwrap().get("joiner").is_some_and(|joiner| {
                            liveness.states.iter().any(|(_, reader, state)| {
                                reader == joiner && *state == EdgeLivenessState::Idle
                            })
                        });
                        if observed_idle {
                            release_join.send(()).expect("release observed join invocation");
                            released = true;
                        }
                    }
                },
            }
        }
    })
    .await
    .expect("flow did not complete within timeout")
    .expect("flow task join")
    .expect("flow should complete successfully");

    liveness.capture();

    let stage_journals = stage_journals_slot
        .lock()
        .expect("stage_journals_slot lock")
        .clone()
        .expect("stage journals captured by hook");

    let stage_ids = stage_ids.lock().unwrap().clone();
    let joiner_id = stage_ids["joiner"];

    let mut envelopes = Vec::new();
    for journal in stage_journals {
        envelopes.extend(
            journal
                .read_all_unordered()
                .await
                .expect("read stage journal"),
        );
    }

    let idle_upstreams: HashSet<StageId> = liveness
        .states
        .iter()
        .filter(|(_, reader, state)| *reader == joiner_id && *state == EdgeLivenessState::Idle)
        .map(|(upstream, _, _)| *upstream)
        .collect();
    let mut contracts = 0;
    for envelope in envelopes {
        if let ChainPayload::Execution(ExecutionPayload::ContractStatus { pass, .. }) =
            &envelope.payload
        {
            contracts += 1;
            assert!(
                *pass,
                "unexpected ContractStatus(pass=false) while exercising join liveness"
            );
        }
    }
    assert!(
        contracts > 0,
        "the owning stage journals contain contract reports"
    );

    assert!(
        !idle_upstreams.is_empty(),
        "expected at least one Idle liveness transition on a non-processing join upstream"
    );
    assert_eq!(
        idle_upstreams,
        HashSet::from([stage_ids["ref_src"]]),
        "only the reference edge may become Idle while the stream handler is active"
    );
    assert!(
        liveness.states.contains(&(
            stage_ids["stream_src"],
            joiner_id,
            EdgeLivenessState::Healthy
        )),
        "the in-flight stream edge remains healthy"
    );
}

#[path = "support/liveness_observations.rs"]
mod liveness_observations;
