// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::{
    ChainEvent, ChainEventFactory, EdgeLivenessState, SystemEvent, SystemEventType,
};
use obzenflow_core::journal::Journal;
use obzenflow_core::{StageId, WriterId};
use obzenflow_dsl::{async_transform, flow, sink, source};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::prelude::FlowHandle;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    AsyncTransformHandler, FiniteSourceHandler, SinkHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

#[derive(Clone, Debug)]
struct OneEventSource {
    emitted: bool,
    writer_id: WriterId,
}

impl OneEventSource {
    fn new() -> Self {
        Self {
            emitted: false,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for OneEventSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            "flowip_063e.input",
            json!({ "value": 1 }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct StallingTransform {
    writer_id: WriterId,
}

impl StallingTransform {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl AsyncTransformHandler for StallingTransform {
    async fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        tokio::time::sleep(Duration::from_secs(130)).await;
        Ok(vec![ChainEventFactory::data_event(
            self.writer_id,
            "flowip_063e.output",
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

#[tokio::test(flavor = "current_thread")]
async fn flowip_063e_emits_stalled_transition_without_aborting_pipeline() {
    tokio::time::pause();

    let system_journal_slot: Arc<Mutex<Option<Arc<dyn Journal<SystemEvent>>>>> =
        Arc::new(Mutex::new(None));
    let system_journal_slot_hook = system_journal_slot.clone();

    let hook = Box::new(move |handle: &Arc<FlowHandle>| {
        let system_journal = handle.system_journal().expect("system journal available");
        *system_journal_slot_hook
            .lock()
            .expect("system_journal_slot lock") = Some(system_journal);
        tokio::spawn(async {})
    });

    let flow_definition = flow! {
        name: "flowip_063e_stalled_transition",
        journals: memory_journals(),
        middleware: [],

        stages: {
            numbers = source!(OneEventSource::new());
            slow = async_transform!(StallingTransform::new());
            sink = sink!(NoopSink);
        },

        topology: {
            numbers |> slow;
            slow |> sink;
        }
    };

    let mut run_task = tokio_test::task::spawn(async move {
        FlowApplication::run_with_web_endpoints_and_hooks(flow_definition, Vec::new(), vec![hook])
            .await
    });

    let mut result = None;
    for _ in 0..300 {
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

    let envelopes = system_journal
        .read_causally_ordered()
        .await
        .expect("read system journal");

    let mut saw_stalled = false;
    let mut saw_recovered = false;
    for envelope in envelopes {
        match &envelope.event.event {
            SystemEventType::EdgeLiveness { state, .. } => match state {
                EdgeLivenessState::Stalled => saw_stalled = true,
                EdgeLivenessState::Recovered => saw_recovered = true,
                _ => {}
            },
            SystemEventType::ContractStatus { pass, .. } => {
                assert!(
                    *pass,
                    "unexpected ContractStatus(pass=false) while exercising stalled transition"
                );
            }
            _ => {}
        }
    }

    assert!(
        saw_stalled,
        "expected EdgeLiveness Stalled during 130s handler call"
    );
    assert!(
        saw_recovered,
        "expected EdgeLiveness Recovered after handler returned and progress resumed"
    );
}
