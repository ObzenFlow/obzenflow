// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// tests/basic_streaming.rs
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::StageOutputs;
use obzenflow_dsl::{flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
    TypedFiniteSourceHandler, TypedTransformHandler,
};
// FLOWIP-056-666: Monitoring middleware temporarily disabled pending redesign
use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::TypedPayload;
use serde::{Deserialize, Serialize};

/// File-local payloads for the basic-streaming tests. The two source
/// shapes correspond to `TestEventSource` (`index`) and `NumberSource`
/// (`value`); the types fingerprint the stage contract per FLOWIP-114c.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct StreamItem {
    index: u64,
}

impl TypedPayload for StreamItem {
    const EVENT_TYPE: &'static str = "basic_streaming.stream_item";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NumberItem {
    value: u64,
}

impl TypedPayload for NumberItem {
    const EVENT_TYPE: &'static str = "basic_streaming.number_item";
}
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Simple test sink that counts events
#[derive(Clone, Debug)]
struct EventCounterSink {
    count: Arc<AtomicU64>,
}

impl EventCounterSink {
    fn new(count: Arc<AtomicU64>) -> Self {
        Self { count }
    }
}

#[async_trait]
impl InlineSink for EventCounterSink {
    type Input = StreamItem;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        _event: StreamItem,
        _context: SinkWriteContext,
    ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("Count".to_string()),
            None,
        )))
    }
}

/// Source that generates a fixed number of events
#[derive(Clone, Debug)]
struct TestEventSource {
    count: usize,
    emitted: usize,
}

impl TestEventSource {
    fn new(count: usize) -> Self {
        Self { count, emitted: 0 }
    }
}

impl TypedFiniteSourceHandler for TestEventSource {
    type Output = StreamItem;

    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<Self::Output>>,
        obzenflow_runtime::stages::common::handlers::source::traits::SourceError,
    > {
        if self.emitted < self.count {
            let index = self.emitted;
            self.emitted += 1;
            Ok(Some(vec![StreamItem {
                index: index as u64,
            }]))
        } else {
            Ok(None)
        }
    }
}

#[tokio::test]
async fn test_basic_flow() -> Result<()> {
    let counter = Arc::new(AtomicU64::new(0));
    let counter_for_flow = counter.clone();

    // Create a simple flow
    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source_handler = TestEventSource::new(10);
        let sink_handler = EventCounterSink::new(counter_for_flow);

        Ok(flow! {
            name: "basic_flow_test",
            journals: disk_journals(std::path::PathBuf::from("target/basic_streaming_basic")),

            stages: {
                source = source!(StreamItem => source_handler);
                sink = sink!(StreamItem => sink_handler);
            },

            topology: {
                source |> sink;
            }
        })
    })
    .build(FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;

    // Run the flow
    handle.run().await?;

    // Check that events were processed
    let final_count = counter.load(Ordering::Relaxed);
    assert_eq!(
        final_count, 10,
        "Expected exactly 10 events to be processed, but got {final_count}"
    );

    // Journals left under target/ for inspection.
    Ok(())
}

/// Stage that doubles each event
#[derive(Clone, Debug)]
struct Doubler;

impl Doubler {
    fn new() -> Self {
        Self
    }
}

impl TypedTransformHandler for Doubler {
    type Input = StreamItem;
    type Output = StageOutputs<StreamItem>;

    fn process(
        &self,
        event: StreamItem,
    ) -> std::result::Result<StageOutputs<StreamItem>, HandlerError> {
        Ok(StageOutputs::many([event.clone(), event]))
    }
}

#[tokio::test]
async fn test_multi_stage_flow() -> Result<()> {
    let counter = Arc::new(AtomicU64::new(0));
    let counter_for_flow = counter.clone();

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source_handler = TestEventSource::new(5);
        let doubler_handler = Doubler::new();
        let sink_handler = EventCounterSink::new(counter_for_flow);

        Ok(flow! {
            name: "multi_stage_flow_test",
            journals: disk_journals(std::path::PathBuf::from("target/basic_streaming_multi")),

            stages: {
                source = source!(StreamItem => source_handler);
                doubler = transform!(StreamItem -> StreamItem => doubler_handler);
                sink = sink!(StreamItem => sink_handler);
            },

            topology: {
                source |> doubler;
                doubler |> sink;
            }
        })
    })
    .build(FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;

    // Run the flow
    handle.run().await?;

    // Would expect 10 events (5 * 2) after processing
    let final_count = counter.load(Ordering::Relaxed);
    assert_eq!(
        final_count, 10,
        "Expected exactly 10 events (5 * 2), but got {final_count}"
    );

    // Journals left under target/ for inspection.
    Ok(())
}

/// A source that generates numbered events
#[derive(Clone, Debug)]
struct NumberSource {
    count: usize,
    emitted: usize,
}

impl NumberSource {
    fn new(count: usize) -> Self {
        Self { count, emitted: 0 }
    }
}

impl TypedFiniteSourceHandler for NumberSource {
    type Output = NumberItem;

    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<Self::Output>>,
        obzenflow_runtime::stages::common::handlers::source::traits::SourceError,
    > {
        if self.emitted < self.count {
            let value = self.emitted + 1;
            self.emitted += 1;
            Ok(Some(vec![NumberItem {
                value: value as u64,
            }]))
        } else {
            Ok(None)
        }
    }
}

/// A transform that doubles numbers
#[derive(Clone, Debug)]
struct NumberDoubler;

impl NumberDoubler {
    fn new() -> Self {
        Self
    }
}

impl TypedTransformHandler for NumberDoubler {
    type Input = NumberItem;
    type Output = NumberItem;

    fn process(&self, event: NumberItem) -> std::result::Result<NumberItem, HandlerError> {
        Ok(NumberItem {
            value: event.value * 2,
        })
    }
}

/// A sink that sums all numbers it receives
#[derive(Clone, Debug)]
struct SumSink {
    sum: Arc<AtomicU64>,
}

impl SumSink {
    fn new(sum: Arc<AtomicU64>) -> Self {
        Self { sum }
    }
}

#[async_trait]
impl InlineSink for SumSink {
    type Input = NumberItem;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        event: NumberItem,
        _context: SinkWriteContext,
    ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
        self.sum.fetch_add(event.value, Ordering::Relaxed);
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("Sum".to_string()),
            None,
        )))
    }
}

#[tokio::test]
async fn test_pipeline_topology() -> Result<()> {
    let sum = Arc::new(AtomicU64::new(0));
    let sum_for_flow = sum.clone();

    // Pipeline: Source(1,2,3) -> Doubler(2,4,6) -> Sum
    // If topology filtering works, Sum should be 12 (2+4+6)
    // If it doesn't work (broadcast), Sum would be 21 (1+2+3+2+4+6)
    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source_handler = NumberSource::new(3);
        let doubler_handler = NumberDoubler::new();
        let sink_handler = SumSink::new(sum_for_flow);

        Ok(flow! {
            name: "pipeline_topology_test",
            journals: disk_journals(std::path::PathBuf::from(
                "target/basic_streaming_topology",
            )),

            stages: {
                source = source!(NumberItem => source_handler);
                doubler = transform!(NumberItem -> NumberItem => doubler_handler);
                sink = sink!(NumberItem => sink_handler);
            },

            topology: {
                source |> doubler;
                doubler |> sink;
            }
        })
    })
    .build(FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;

    // Run the flow
    handle.run().await?;

    // Check the sum
    let final_sum = sum.load(Ordering::Relaxed);
    println!("Final sum: {final_sum}");

    // With proper topology: 2 + 4 + 6 = 12
    // Without topology (broadcast): 1 + 2 + 3 + 2 + 4 + 6 = 18
    assert_eq!(
        final_sum, 12,
        "Expected sum of doubled values (2+4+6=12), but got {final_sum}"
    );

    // Journals left under target/ for inspection.
    Ok(())
}
