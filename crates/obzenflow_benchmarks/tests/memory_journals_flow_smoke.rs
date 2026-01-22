use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventContent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::WriterId;
use obzenflow_dsl_infra::{flow, sink, source};
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime_services::pipeline::PipelineState;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::{FiniteSourceHandler, SinkHandler};
use obzenflow_runtime_services::stages::SourceError;
use obzenflow_runtime_services::supervised_base::SupervisorHandle;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug)]
struct TestSource {
    total_events: u64,
    emitted: Arc<AtomicU64>,
    writer_id: WriterId,
}

impl TestSource {
    fn new(total_events: u64) -> Self {
        Self {
            total_events,
            emitted: Arc::new(AtomicU64::new(0)),
            writer_id: WriterId::from(obzenflow_core::StageId::new()),
        }
    }
}

impl FiniteSourceHandler for TestSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        let current = self.emitted.fetch_add(1, Ordering::Relaxed);
        if current < self.total_events {
            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id,
                "TestEvent",
                json!({ "n": current }),
            )]))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone, Debug)]
struct CountingSink {
    received: Arc<AtomicU64>,
}

impl CountingSink {
    fn new() -> Self {
        Self {
            received: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[async_trait]
impl SinkHandler for CountingSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        if matches!(event.content, ChainEventContent::Data { .. }) {
            self.received.fetch_add(1, Ordering::Relaxed);
        }
        Ok(DeliveryPayload::success("noop", DeliveryMethod::Noop, None))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_journals_flow_runs_to_completion() {
    let total_events = 10;
    let source = TestSource::new(total_events);
    let sink = CountingSink::new();
    let sink_clone = sink.clone();

    let handle = tokio::time::timeout(Duration::from_secs(10), async {
        flow! {
            journals: memory_journals(),
            middleware: [],

            stages: {
                src = source!("source" => source);
                snk = sink!("sink" => sink);
            },

            topology: {
                src |> snk;
            }
        }
        .await
    })
    .await
    .expect("flow creation did not complete within timeout")
    .expect("failed to create flow");

    let mut state_rx = handle.state_receiver();

    handle.start().await.expect("failed to start flow");

    let wait_for_sink = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if sink_clone.received.load(Ordering::Relaxed) >= total_events {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    if wait_for_sink.is_err() {
        eprintln!(
            "sink did not receive all events in time (received={}, expected={})",
            sink_clone.received.load(Ordering::Relaxed),
            total_events
        );
        eprintln!(
            "pipeline state at sink timeout: {:?}",
            handle.current_state()
        );
    }

    let observed_terminal_state = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match &*state_rx.borrow() {
                PipelineState::Drained
                | PipelineState::Failed { .. }
                | PipelineState::AbortRequested { .. } => break,
                _ => {}
            }
            if state_rx.changed().await.is_err() {
                break;
            }
        }
    })
    .await;

    if observed_terminal_state.is_err() && handle.is_running() {
        handle.stop_cancel().await.expect("stop_cancel failed");
    }

    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .expect("flow did not complete within timeout")
        .expect("flow run failed");

    assert_eq!(
        sink_clone.received.load(Ordering::Relaxed),
        total_events,
        "sink did not receive expected event count"
    );
}
