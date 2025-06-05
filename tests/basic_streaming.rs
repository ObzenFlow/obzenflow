// tests/basic_streaming.rs
use flowstate_rs::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Simple test stage that counts events
struct Counter {
    count: Arc<AtomicU64>,
}

impl Counter {
    fn new() -> (Self, Arc<AtomicU64>) {
        let count = Arc::new(AtomicU64::new(0));
        (Self { count: count.clone() }, count)
    }
}

impl PipelineStep for Counter {
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        self.count.fetch_add(1, Ordering::Relaxed);
        vec![event]
    }
}

/// Generator that produces a fixed number of events
struct TestGenerator {
    count: u64,
}

#[async_trait]
impl PipelineStep for TestGenerator {
    async fn process_stream(
        &mut self,
        _input: Receiver<ChainEvent>,
        output: Sender<ChainEvent>,
        metrics: StepMetrics,
    ) -> Result<()> {
        for i in 0..self.count {
            let event = ChainEvent::new(
                "TestEvent",
                json!({ "index": i }),
            );

            if output.send(event).await.is_err() {
                return Err("Output channel closed".into());
            }

            metrics.increment_processed(1);
            metrics.increment_emitted(1);
        }

        Ok(())
    }
}

#[tokio::test]
async fn test_basic_pipeline() {
    let (counter, count) = Counter::new();

    let pipeline = PipelineBuilder::new()
        .buffer_size(100)
        .stage("generator", TestGenerator { count: 10 })
        .stage("counter", counter)
        .build()
        .await
        .expect("Failed to build pipeline");

    let report = pipeline.wait().await.expect("Pipeline failed");

    // Verify all events were processed
    assert_eq!(count.load(Ordering::Relaxed), 10);

    // Note: The report.stages will be empty in our simple implementation
    // This is expected for now
}

#[tokio::test]
async fn test_simple_transform() {
    struct Doubler;

    impl PipelineStep for Doubler {
        fn handle(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
            if let Some(index) = event.payload["index"].as_u64() {
                event.payload["doubled"] = json!(index * 2);
            }
            vec![event]
        }
    }

    let (counter, count) = Counter::new();

    let pipeline = PipelineBuilder::new()
        .stage("generator", TestGenerator { count: 5 })
        .stage("doubler", Doubler)
        .stage("counter", counter)
        .build()
        .await
        .expect("Failed to build pipeline");

    pipeline.wait().await.expect("Pipeline failed");

    assert_eq!(count.load(Ordering::Relaxed), 5);
}
