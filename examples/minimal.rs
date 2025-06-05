// examples/minimal.rs
use flowstate_rs::prelude::*;
use std::time::Duration;

/// A simple transformer that adds a field to events
struct Enricher {
    field_name: String,
    field_value: Value,
}

impl PipelineStep for Enricher {
    fn handle(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        event.payload[&self.field_name] = self.field_value.clone();
        vec![event]
    }
}

/// A sink that prints events to console
struct ConsoleSink;

impl PipelineStep for ConsoleSink {
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        println!("[{}] {}: {}",
            event.timestamp.format("%H:%M:%S%.3f"),
            event.event_type,
            event.payload
        );
        vec![] // Sink doesn't forward events
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("flowstate_rs=debug")
        .init();

    println!("Starting FlowStateRS streaming pipeline...\n");

    // Build a simple pipeline
    let pipeline = PipelineBuilder::new()
        .buffer_size(100)
        .metrics_interval(Duration::from_secs(2))

        // Generate test events
        .stage("generator", TestEventGenerator {
            rate: 2.0,  // 2 events per second
            duration: Duration::from_secs(10),
        })

        // Add enrichment
        .stage("enricher", Enricher {
            field_name: "processed_by".to_string(),
            field_value: json!("FlowStateRS"),
        })

        // Rate limit output
        .stage("rate_limit", RateLimiter::new(1.0))

        // Print to console
        .stage("console", ConsoleSink)

        .build()
        .await?;

    println!("Pipeline running for 10 seconds...\n");

    // Wait for completion
    let report = pipeline.wait().await?;

    // Print final report
    println!("\n=== Pipeline Report ===");
    println!("Duration: {:?}", report.duration);
    println!("Total throughput: {:.2} events/sec\n", report.total_throughput());

    for stage in &report.stages {
        println!("Stage: {}", stage.name);
        println!("  Processed: {}", stage.processed);
        println!("  Emitted: {}", stage.emitted);
        println!("  Errors: {}", stage.errors);
        println!("  Latencies: p50={}μs, p95={}μs, p99={}μs",
            stage.p50_latency_us, stage.p95_latency_us, stage.p99_latency_us);
        println!();
    }

    Ok(())
}

/// Test event generator that runs for a fixed duration
struct TestEventGenerator {
    rate: f64,
    duration: Duration,
}

#[async_trait]
impl PipelineStep for TestEventGenerator {
    async fn process_stream(
        &mut self,
        _input: Receiver<ChainEvent>,
        output: Sender<ChainEvent>,
        metrics: StepMetrics,
    ) -> Result<()> {
        let start = Instant::now();
        let delay = Duration::from_secs_f64(1.0 / self.rate);
        let mut count = 0;

        while start.elapsed() < self.duration {
            let event = ChainEvent::new(
                "TestEvent",
                json!({
                    "index": count,
                    "message": format!("Event #{}", count),
                }),
            );

            if output.send(event).await.is_err() {
                break;
            }

            count += 1;
            metrics.increment_processed(1);
            metrics.increment_emitted(1);

            tokio::time::sleep(delay).await;
        }

        Ok(())
    }
}
