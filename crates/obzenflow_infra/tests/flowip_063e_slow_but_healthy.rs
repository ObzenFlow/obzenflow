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
use obzenflow_runtime::stages::common::handlers::{AsyncTransformHandler, FiniteSourceHandler, SinkHandler};
use obzenflow_runtime::stages::SourceError;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

#[derive(Clone, Debug)]
struct TwoEventSource {
    next_value: u64,
    remaining: usize,
    writer_id: WriterId,
}

impl TwoEventSource {
    fn new() -> Self {
        Self {
            next_value: 1,
            remaining: 2,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for TwoEventSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.remaining == 0 {
            return Ok(None);
        }

        self.remaining = self.remaining.saturating_sub(1);
        let value = self.next_value;
        self.next_value = self.next_value.saturating_add(1);

        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            "flowip_063e.input",
            json!({ "value": value }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct SlowAiTransform {
    writer_id: WriterId,
    calls: Arc<AtomicUsize>,
}

impl SlowAiTransform {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl AsyncTransformHandler for SlowAiTransform {
    async fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let call_index = self.calls.fetch_add(1, Ordering::SeqCst);
        if call_index == 0 {
            tokio::time::sleep(Duration::from_secs(50)).await;
        } else if call_index == 1 {
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

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
    async fn consume(
        &mut self,
        _event: ChainEvent,
    ) -> std::result::Result<DeliveryPayload, HandlerError> {
        Ok(DeliveryPayload::success(
            "noop",
            DeliveryMethod::Custom("Noop".to_string()),
            None,
        ))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn flowip_063e_slow_but_healthy_completes_and_emits_liveness_transitions() {
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
        name: "flowip_063e_slow_but_healthy",
        journals: memory_journals(),
        middleware: [],

        stages: {
            numbers = source!(TwoEventSource::new());
            slow_ai = async_transform!(SlowAiTransform::new());
            sink = sink!(NoopSink);
        },

        topology: {
            numbers |> slow_ai;
            slow_ai |> sink;
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

    if result.is_none() {
        if let Poll::Ready(res) = run_task.poll() {
            result = Some(res);
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

    let mut saw_suspect = false;
    let mut saw_recovered = false;
    for envelope in envelopes {
        match &envelope.event.event {
            SystemEventType::EdgeLiveness { state, .. } => match state {
                EdgeLivenessState::Suspect => saw_suspect = true,
                EdgeLivenessState::Recovered => saw_recovered = true,
                _ => {}
            },
            SystemEventType::ContractStatus { pass, .. } => {
                assert!(
                    *pass,
                    "unexpected ContractStatus(pass=false) while exercising slow-but-healthy handler"
                );
            }
            _ => {}
        }
    }

    assert!(
        saw_suspect,
        "expected at least one EdgeLiveness Suspect transition during slow handler call"
    );
    assert!(
        saw_recovered,
        "expected EdgeLiveness Recovered after the handler returned and progress resumed"
    );
}
