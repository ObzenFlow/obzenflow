// tests/volume_tests.rs
use flowstate_rs::prelude::*;
use std::time::{Duration, Instant};
use tokio::time::timeout;

/// Test fixture that generates events at a controlled rate
struct EventGenerator {
    rate: u64, // events per second
    total: u64,
    event_type: String,
}

#[async_trait]
impl PipelineStep for EventGenerator {
    async fn process_stream(
        &mut self,
        _input: Receiver<ChainEvent>,
        output: Sender<ChainEvent>,
        metrics: StepMetrics,
    ) -> Result<()> {
        let delay = Duration::from_secs_f64(1.0 / self.rate as f64);
        let mut sent = 0;

        while sent < self.total {
            let event = ChainEvent::new(
                &self.event_type,
                serde_json::json!({
                    "index": sent,
                    "timestamp": Instant::now(),
                }),
            );

            if output.send(event).await.is_err() {
                break;
            }

            sent += 1;
            tokio::time::sleep(delay).await;
        }

        Ok(())
    }
}

/// CPU-intensive stage for testing backpressure
struct CpuIntensiveStage {
    work_duration: Duration,
}

#[async_trait]
impl PipelineStep for CpuIntensiveStage {
    fn handle(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        // Simulate CPU work
        let start = Instant::now();
        while start.elapsed() < self.work_duration {
            // Busy work
            std::hint::black_box(event.ulid.as_bytes());
        }

        event.event_type = "Processed".to_string();
        vec![event]
    }
}

/// Memory-intensive stage
struct MemoryIntensiveStage {
    buffer_size: usize,
}

#[async_trait]
impl PipelineStep for MemoryIntensiveStage {
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Allocate some memory
        let _buffer: Vec<u8> = vec![0; self.buffer_size];
        vec![event]
    }
}

/// Sink that just counts events
struct CountingSink {
    counter: Arc<AtomicU64>,
}

#[async_trait]
impl PipelineStep for CountingSink {
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        self.counter.fetch_add(1, Ordering::Relaxed);
        vec![] // Sink doesn't emit
    }
}

#[tokio::test]
async fn test_basic_throughput() {
    let pipeline = Pipeline::builder()
        .buffer_size(1000)
        .source("generator", EventGenerator {
            rate: 10_000,
            total: 100_000,
            event_type: "TestEvent".to_string(),
        })
        .stage("passthrough", PassthroughStage)
        .stage("sink", CountingSink {
            counter: Arc::new(AtomicU64::new(0)),
        })
        .build()
        .await
        .unwrap();

    let report = timeout(Duration::from_secs(30), pipeline.wait())
        .await
        .expect("Pipeline timeout")
        .expect("Pipeline failed");

    println!("Pipeline Report: {:#?}", report);

    assert!(report.total_throughput() > 5_000.0,
        "Expected >5k events/sec, got {}", report.total_throughput());
    assert_eq!(report.total_processed(), 100_000);
}

#[tokio::test]
async fn test_backpressure() {
    // Fast producer, slow consumer
    let pipeline = Pipeline::builder()
        .buffer_size(100) // Small buffer to test backpressure
        .source("generator", EventGenerator {
            rate: 10_000,
            total: 10_000,
            event_type: "TestEvent".to_string(),
        })
        .stage("cpu_intensive", CpuIntensiveStage {
            work_duration: Duration::from_micros(100),
        })
        .stage("sink", CountingSink {
            counter: Arc::new(AtomicU64::new(0)),
        })
        .build()
        .await
        .unwrap();

    let report = timeout(Duration::from_secs(60), pipeline.wait())
        .await
        .expect("Pipeline timeout")
        .expect("Pipeline failed");

    // Should process all events despite backpressure
    assert_eq!(report.total_processed(), 10_000);

    // Check that backpressure worked (processing rate limited by slow stage)
    let cpu_stage = &report.stages[1];
    assert!(cpu_stage.throughput(report.duration) < 20_000.0);
}

#[tokio::test]
async fn test_memory_pressure() {
    let pipeline = Pipeline::builder()
        .buffer_size(50)
        .source("generator", EventGenerator {
            rate: 1_000,
            total: 5_000,
            event_type: "TestEvent".to_string(),
        })
        .stage("memory", MemoryIntensiveStage {
            buffer_size: 1024 * 1024, // 1MB per event
        })
        .stage("sink", CountingSink {
            counter: Arc::new(AtomicU64::new(0)),
        })
        .build()
        .await
        .unwrap();

    let report = pipeline.wait().await.expect("Pipeline failed");
    assert_eq!(report.total_processed(), 5_000);
}

#[tokio::test]
async fn test_sustained_load() {
    // Run for 60 seconds at steady rate
    let duration = Duration::from_secs(60);
    let rate = 5_000;

    let pipeline = Pipeline::builder()
        .buffer_size(1000)
        .metrics_interval(Duration::from_secs(5))
        .source("generator", EventGenerator {
            rate,
            total: rate * duration.as_secs(),
            event_type: "TestEvent".to_string(),
        })
        .stage("transform", |event: ChainEvent| {
            let mut e = event;
            e.payload["transformed"] = json!(true);
            vec![e]
        })
        .stage("sink", CountingSink {
            counter: Arc::new(AtomicU64::new(0)),
        })
        .build()
        .await
        .unwrap();

    let start = Instant::now();
    let report = pipeline.wait().await.expect("Pipeline failed");
    let actual_duration = start.elapsed();

    println!("Sustained load test results:");
    println!("Target duration: {:?}", duration);
    println!("Actual duration: {:?}", actual_duration);
    println!("Target events: {}", rate * duration.as_secs());
    println!("Processed events: {}", report.total_processed());
    println!("Throughput: {:.2} events/sec", report.total_throughput());

    // Verify we maintained the rate
    assert!(
        (report.total_throughput() - rate as f64).abs() < 100.0,
        "Throughput deviation too high"
    );

    // Check latency percentiles
    for stage in &report.stages {
        println!("\nStage '{}' latencies:", stage.name);
        println!("  p50: {}μs", stage.p50_latency_us);
        println!("  p95: {}μs", stage.p95_latency_us);
        println!("  p99: {}μs", stage.p99_latency_us);

        // Ensure reasonable latencies
        assert!(stage.p99_latency_us < 10_000,
            "Stage {} p99 latency too high", stage.name);
    }
}

/// Benchmark different buffer sizes
#[tokio::test]
async fn benchmark_buffer_sizes() {
    let sizes = vec![10, 100, 1000, 10000];
    let mut results = Vec::new();

    for buffer_size in sizes {
        let pipeline = Pipeline::builder()
            .buffer_size(buffer_size)
            .source("generator", EventGenerator {
                rate: 50_000,
                total: 100_000,
                event_type: "TestEvent".to_string(),
            })
            .stage("transform", PassthroughStage)
            .stage("sink", CountingSink {
                counter: Arc::new(AtomicU64::new(0)),
            })
            .build()
            .await
            .unwrap();

        let start = Instant::now();
        let report = pipeline.wait().await.expect("Pipeline failed");
        let duration = start.elapsed();

        results.push((buffer_size, report.total_throughput(), duration));
    }

    println!("\nBuffer Size Benchmark Results:");
    println!("Buffer Size | Throughput | Duration");
    println!("------------|------------|----------");
    for (size, throughput, duration) in results {
        println!("{:11} | {:10.2} | {:?}", size, throughput, duration);
    }
}
