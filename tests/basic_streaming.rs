// tests/basic_streaming.rs
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::{StageId, WriterId};
use obzenflow_dsl_infra::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
// FLOWIP-056-666: Monitoring middleware temporarily disabled pending redesign
use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tempfile::tempdir;

/// Simple test sink that counts events
#[derive(Clone, Debug)]
struct EventCounterSink {
    count: Arc<AtomicU64>,
}

impl EventCounterSink {
    fn new() -> (Self, Arc<AtomicU64>) {
        let count = Arc::new(AtomicU64::new(0));
        (
            Self {
                count: count.clone(),
            },
            count,
        )
    }
}

#[async_trait]
impl SinkHandler for EventCounterSink {
    async fn consume(
        &mut self,
        _event: ChainEvent,
    ) -> obzenflow_core::Result<DeliveryPayload> {
        if _event.is_data() {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
        Ok(DeliveryPayload::success(
            "counter_sink",
            DeliveryMethod::Custom("Count".to_string()),
            None,
        ))
    }
}

/// Source that generates a fixed number of events
#[derive(Clone, Debug)]
struct TestEventSource {
    count: usize,
    emitted: usize,
    writer_id: WriterId,
}

impl TestEventSource {
    fn new(count: usize) -> Self {
        Self {
            count,
            emitted: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for TestEventSource {
    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError,
    > {
        if self.emitted < self.count {
            let index = self.emitted;
            self.emitted += 1;
            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id.clone(),
                "TestEvent",
                json!({ "index": index }),
            )]))
        } else {
            Ok(None)
        }
    }
}

#[tokio::test]
async fn test_basic_flow() -> Result<()> {
    let (counter_sink, counter) = EventCounterSink::new();

    // Create a simple flow
    let handle = flow! {
        name: "basic_flow_test",
        journals: disk_journals(std::path::PathBuf::from("target/basic_streaming_basic")),
        middleware: [],

        stages: {
            src = source!("source" => TestEventSource::new(10));
            snk = sink!("sink" => counter_sink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;

    // Run the flow
    handle.run().await?;

    // Check that events were processed
    let final_count = counter.load(Ordering::Relaxed);
    assert_eq!(
        final_count, 10,
        "Expected exactly 10 events to be processed, but got {}",
        final_count
    );

    // Cleanup handled by tempdir
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

#[async_trait]
impl TransformHandler for Doubler {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Return doubled events
        vec![event.clone(), event]
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn test_multi_stage_flow() -> Result<()> {
    let (counter_sink, counter) = EventCounterSink::new();

    let handle = flow! {
        name: "multi_stage_flow_test",
        journals: disk_journals(std::path::PathBuf::from("target/basic_streaming_multi")),
        middleware: [],

        stages: {
            src = source!("source" => TestEventSource::new(5));
            dbl = transform!("doubler" => Doubler::new());
            snk = sink!("sink" => counter_sink);
        },

        topology: {
            src |> dbl;
            dbl |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;

    // Run the flow
    handle.run().await?;

    // Would expect 10 events (5 * 2) after processing
    let final_count = counter.load(Ordering::Relaxed);
    assert_eq!(
        final_count, 10,
        "Expected exactly 10 events (5 * 2), but got {}",
        final_count
    );

    // Cleanup handled by tempdir
    Ok(())
}

/// A source that generates numbered events
#[derive(Clone, Debug)]
struct NumberSource {
    count: usize,
    emitted: usize,
    writer_id: WriterId,
}

impl NumberSource {
    fn new(count: usize) -> Self {
        Self {
            count,
            emitted: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for NumberSource {
    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError,
    > {
        if self.emitted < self.count {
            let value = self.emitted + 1;
            self.emitted += 1;
            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id.clone(),
                "Number",
                json!({ "value": value }),
            )]))
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

#[async_trait]
impl TransformHandler for NumberDoubler {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if let Some(value) = event
            .payload()
            .get("value")
            .and_then(|v| v.as_u64())
        {
            vec![ChainEventFactory::data_event(
                event.writer_id.clone(),
                "DoubledNumber",
                json!({ "value": value * 2 }),
            )]
        } else {
            vec![]
        }
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

/// A sink that sums all numbers it receives
#[derive(Clone, Debug)]
struct SumSink {
    sum: Arc<AtomicU64>,
}

impl SumSink {
    fn new() -> (Self, Arc<AtomicU64>) {
        let sum = Arc::new(AtomicU64::new(0));
        (Self { sum: sum.clone() }, sum)
    }
}

#[async_trait]
impl SinkHandler for SumSink {
    async fn consume(
        &mut self,
        event: ChainEvent,
    ) -> obzenflow_core::Result<DeliveryPayload> {
        if let Some(value) = event
            .payload()
            .get("value")
            .and_then(|v| v.as_u64())
        {
            self.sum.fetch_add(value, Ordering::Relaxed);
        }
        Ok(DeliveryPayload::success(
            "sum_sink",
            DeliveryMethod::Custom("Sum".to_string()),
            None,
        ))
    }
}

#[tokio::test]
async fn test_pipeline_topology() -> Result<()> {
    let (sum_sink, sum) = SumSink::new();

    // Pipeline: Source(1,2,3) -> Doubler(2,4,6) -> Sum
    // If topology filtering works, Sum should be 12 (2+4+6)
    // If it doesn't work (broadcast), Sum would be 21 (1+2+3+2+4+6)
    let handle = flow! {
        name: "pipeline_topology_test",
        journals: disk_journals(std::path::PathBuf::from(
            "target/basic_streaming_topology",
        )),
        middleware: [],

        stages: {
            src = source!("source" => NumberSource::new(3));
            dbl = transform!("doubler" => NumberDoubler::new());
            snk = sink!("sink" => sum_sink);
        },

        topology: {
            src |> dbl;
            dbl |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;

    // Run the flow
    handle.run().await?;

    // Check the sum
    let final_sum = sum.load(Ordering::Relaxed);
    println!("Final sum: {}", final_sum);

    // With proper topology: 2 + 4 + 6 = 12
    // Without topology (broadcast): 1 + 2 + 3 + 2 + 4 + 6 = 18
    assert_eq!(
        final_sum, 12,
        "Expected sum of doubled values (2+4+6=12), but got {}",
        final_sum
    );

    // Cleanup handled by tempdir
    Ok(())
}
