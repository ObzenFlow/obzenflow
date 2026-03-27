// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::{ChainEvent, ChainEventFactory, SystemEvent, SystemEventType};
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
use obzenflow_runtime::stages::common::stage_handle::{STOP_REASON_TIMEOUT, STOP_REASON_USER_STOP};
use obzenflow_runtime::stages::SourceError;
use serde_json::json;
use std::future;
use std::sync::{Arc, Mutex};
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
struct HungTransform;

#[async_trait]
impl AsyncTransformHandler for HungTransform {
    async fn process(&self, _event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        future::pending::<()>().await;
        unreachable!("pending() never resolves")
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flowip_063e_hung_handler_can_be_cancelled_without_contract_failure() {
    let flow_handle_slot: Arc<Mutex<Option<Arc<FlowHandle>>>> = Arc::new(Mutex::new(None));
    let system_journal_slot: Arc<Mutex<Option<Arc<dyn Journal<SystemEvent>>>>> =
        Arc::new(Mutex::new(None));
    let flow_handle_slot_hook = flow_handle_slot.clone();
    let system_journal_slot_hook = system_journal_slot.clone();

    let hook = Box::new(move |handle: &Arc<FlowHandle>| {
        *flow_handle_slot_hook.lock().expect("flow_handle_slot lock") = Some(handle.clone());
        let system_journal = handle.system_journal().expect("system journal available");
        *system_journal_slot_hook
            .lock()
            .expect("system_journal_slot lock") = Some(system_journal);
        tokio::spawn(async {})
    });

    let flow_definition = flow! {
        name: "flowip_063e_hung_handler_cancel",
        journals: memory_journals(),
        middleware: [],

        stages: {
            numbers = source!(OneEventSource::new());
            hung = async_transform!(HungTransform);
            snk = sink!(NoopSink);
        },

        topology: {
            numbers |> hung;
            hung |> snk;
        }
    };

    let run_task = tokio::spawn(async move {
        FlowApplication::run_with_web_endpoints_and_hooks(flow_definition, Vec::new(), vec![hook])
            .await
    });

    // Wait for FlowHandle to be available and running before requesting stop.
    let flow_handle = {
        let mut captured_running = None;
        for _ in 0..200 {
            if let Some(handle) = flow_handle_slot
                .lock()
                .expect("flow_handle_slot lock")
                .clone()
            {
                if handle.is_running() {
                    captured_running = Some(handle);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        captured_running.expect("flow handle captured and running")
    };

    // Give the pipeline a moment to enter the hung handler call.
    tokio::time::sleep(Duration::from_millis(50)).await;

    flow_handle
        .stop_graceful(Duration::from_millis(100))
        .await
        .expect("stop_graceful request");

    // The supervisor uses real-time deadlines for graceful stop escalation, so use real time here.
    let _ = tokio::time::timeout(Duration::from_secs(5), run_task)
        .await
        .expect("flow should terminate after graceful stop timeout escalation")
        .expect("flow task join");

    // FlowApplication can tear down background tasks quickly on stop; give the pipeline
    // a moment to finish appending terminal lifecycle events to the system journal.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let system_journal = system_journal_slot
        .lock()
        .expect("system_journal_slot lock")
        .clone()
        .expect("system journal captured by hook");

    let envelopes = system_journal
        .read_causally_ordered()
        .await
        .expect("read system journal");

    let mut pipeline_events: Vec<String> = Vec::new();
    let mut saw_stop_requested = false;
    let mut saw_cancelled = false;
    let mut saw_stop_failed = false;
    for envelope in envelopes {
        match &envelope.event.event {
            SystemEventType::PipelineLifecycle(event) => {
                pipeline_events.push(format!("{event:?}"));
                match event {
                    obzenflow_core::event::PipelineLifecycleEvent::StopRequested { .. } => {
                        saw_stop_requested = true
                    }
                    obzenflow_core::event::PipelineLifecycleEvent::Cancelled { .. } => {
                        saw_cancelled = true
                    }
                    obzenflow_core::event::PipelineLifecycleEvent::Failed { reason, .. }
                        if reason == STOP_REASON_USER_STOP || reason == STOP_REASON_TIMEOUT =>
                    {
                        saw_stop_failed = true
                    }
                    _ => {}
                }
            }
            SystemEventType::StageLifecycle {
                event: obzenflow_core::event::StageLifecycleEvent::Cancelled { .. },
                ..
            } => saw_cancelled = true,
            SystemEventType::ContractStatus { pass, .. } => {
                assert!(
                    *pass,
                    "unexpected ContractStatus(pass=false) while exercising hung handler cancellation"
                );
            }
            _ => {}
        }
    }

    if !(saw_stop_requested || saw_cancelled || saw_stop_failed) {
        panic!(
            "expected a stop/cancel lifecycle event after stop escalation; saw pipeline events: {pipeline_events:?}"
        );
    }
}
