// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115b: end-to-end proof that a circuit breaker on a journal sink guards
//! delivery through the new sink-delivery boundary.
//!
//! A sink whose delivery always fails drives the breaker open. Once open, the
//! breaker rejects further deliveries at the boundary, so the sink handler is
//! not invoked for them: the handler call count stays strictly below the number
//! of events. This proves the breaker reaches the sink through the carrier and
//! the `SinkDeliveryBoundary` short-circuits delivery, rather than every event
//! reaching the handler as it would without a working sink boundary.

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::middleware::CircuitBreaker;
use obzenflow_core::event::payloads::delivery_payload::DeliveryPayload;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    StageId, TypedPayload, WriterId,
};
use obzenflow_dsl::{flow, sink, source};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{FiniteSourceHandler, SinkHandler};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn breaker(failures: usize) -> CircuitBreaker {
    CircuitBreaker::builder()
        .consecutive_failures(failures.try_into().expect("test threshold fits u32"))
        .build()
        .expect("test breaker configuration")
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SinkBreakerEvent {
    sequence: u64,
}

impl TypedPayload for SinkBreakerEvent {
    const EVENT_TYPE: &'static str = "circuit_breaker_sink.event";
}

/// Finite source emitting `count` events with no inter-event delay, so the whole
/// run completes well within the breaker cooldown (no half-open probe noise).
#[derive(Clone, Debug)]
struct BurstSource {
    count: u64,
    index: u64,
    writer_id: WriterId,
}

impl BurstSource {
    fn new(count: u64) -> Self {
        Self {
            count,
            index: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for BurstSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.index >= self.count {
            return Ok(None);
        }
        let event = ChainEventFactory::data_event(
            self.writer_id,
            <SinkBreakerEvent as TypedPayload>::EVENT_TYPE,
            json!({ "sequence": self.index }),
        );
        self.index += 1;
        Ok(Some(vec![event]))
    }
}

/// Sink whose delivery always fails, counting how many times the handler is
/// actually invoked. Deliveries the breaker rejects never reach this counter.
#[derive(Clone, Debug)]
struct AlwaysFailingSink {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl SinkHandler for AlwaysFailingSink {
    async fn consume(&mut self, _event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(HandlerError::Remote("sink delivery failed".to_string()))
    }
}

#[tokio::test]
async fn circuit_breaker_on_sink_opens_and_rejects_delivery() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .try_init();

    const TOTAL_EVENTS: u64 = 12;
    const THRESHOLD: usize = 3;

    let source = BurstSource::new(TOTAL_EVENTS);
    let calls = Arc::new(AtomicUsize::new(0));
    let sink_handler = AlwaysFailingSink {
        calls: calls.clone(),
    };

    let flow_handle = flow! {
        name: "circuit_breaker_sink_delivery_test",
        journals: disk_journals(std::path::PathBuf::from("target/cb_sink_delivery")),
        middleware: [],

        stages: {
            cb_source = source!(SinkBreakerEvent => source);
            cb_sink = sink!(SinkBreakerEvent => sink_handler, middleware: [
                breaker(THRESHOLD)
            ]);
        },

        topology: {
            cb_source |> cb_sink;
        }
    }
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow::anyhow!("Flow creation failed: {e:?}"))?;

    // The strict source-delivery contract may abort once the breaker rejects
    // downstream traffic; that is an expected terminal state for this flow.
    let run_result = flow_handle.run_with_metrics().await;
    if let Err(e) = run_result {
        let error = format!("{e:?}");
        assert!(
            error.contains("SeqDivergence")
                || error.contains("Pipeline abort")
                || error.contains("abort"),
            "unexpected sink breaker flow failure: {error}"
        );
    }

    let invoked = calls.load(Ordering::SeqCst);

    // The breaker needs at least `THRESHOLD` real delivery failures to open.
    assert!(
        invoked >= THRESHOLD,
        "expected at least {THRESHOLD} sink invocations to open the breaker, got {invoked}"
    );
    // Once open, the breaker rejects further deliveries at the boundary without
    // invoking the sink handler, so it is invoked strictly fewer than the total.
    assert!(
        (invoked as u64) < TOTAL_EVENTS,
        "expected the breaker to reject some deliveries (invoked {invoked} < {TOTAL_EVENTS}); \
         the sink-delivery boundary did not short-circuit"
    );

    Ok(())
}
