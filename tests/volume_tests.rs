// tests/volume_tests.rs
use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Test fixture that generates events at a controlled rate
struct EventGenerator {
    rate: u64, // events per second
    total: u64,
    event_type: String,
    generated: AtomicU64,
    metrics: <RED as Taxonomy>::Metrics,
}

impl EventGenerator {
    fn new(rate: u64, total: u64, event_type: String) -> Self {
        Self {
            rate,
            total,
            event_type,
            generated: AtomicU64::new(0),
            metrics: RED::create_metrics("EventGenerator"),
        }
    }
}

impl Step for EventGenerator {
    type Taxonomy = RED;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &RED
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Source
    }
    
    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let current = self.generated.fetch_add(1, Ordering::Relaxed);
        if current < self.total {
            let event = ChainEvent::new(
                &self.event_type,
                json!({
                    "index": current,
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                }),
            );
            self.metrics.record_success(Duration::from_micros(10));
            vec![event]
        } else {
            vec![]
        }
    }
}

/// CPU-intensive stage for testing backpressure
struct CpuIntensiveStage {
    work_duration: Duration,
    metrics: <USE as Taxonomy>::Metrics,
}

impl CpuIntensiveStage {
    fn new(work_duration: Duration) -> Self {
        Self {
            work_duration,
            metrics: USE::create_metrics("CpuIntensiveStage"),
        }
    }
}

impl Step for CpuIntensiveStage {
    type Taxonomy = USE;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &USE
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Stage
    }
    
    fn handle(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        // Simulate CPU work
        let start = Instant::now();
        while start.elapsed() < self.work_duration {
            // Busy work
            std::hint::black_box(event.ulid.to_bytes());
        }

        event.event_type = "Processed".to_string();
        // USE taxonomy doesn't have record_utilization_time, just track as working
        vec![event]
    }
}

/// Memory-intensive stage
struct MemoryIntensiveStage {
    buffer_size: usize,
    metrics: <USE as Taxonomy>::Metrics,
}

impl MemoryIntensiveStage {
    fn new(buffer_size: usize) -> Self {
        Self {
            buffer_size,
            metrics: USE::create_metrics("MemoryIntensiveStage"),
        }
    }
}

impl Step for MemoryIntensiveStage {
    type Taxonomy = USE;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &USE
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Stage
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Allocate some memory
        let _buffer: Vec<u8> = vec![0; self.buffer_size];
        // Record saturation as current usage vs max capacity
        self.metrics.record_saturation(self.buffer_size / 1024, 100 * 1024); // Current KB vs 100MB in KB
        vec![event]
    }
}

/// Sink that just counts events
struct CountingSink {
    counter: Arc<AtomicU64>,
    metrics: <RED as Taxonomy>::Metrics,
}

impl CountingSink {
    fn new() -> (Self, Arc<AtomicU64>) {
        let counter = Arc::new(AtomicU64::new(0));
        (
            Self {
                counter: counter.clone(),
                metrics: RED::create_metrics("CountingSink"),
            },
            counter,
        )
    }
}

impl Step for CountingSink {
    type Taxonomy = RED;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &RED
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Sink
    }
    
    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        self.counter.fetch_add(1, Ordering::Relaxed);
        self.metrics.record_success(Duration::from_micros(10));
        vec![] // Sinks don't emit events
    }
}

/// Passthrough stage for testing
struct PassthroughStage {
    metrics: <RED as Taxonomy>::Metrics,
}

impl PassthroughStage {
    fn new() -> Self {
        Self {
            metrics: RED::create_metrics("PassthroughStage"),
        }
    }
}

impl Step for PassthroughStage {
    type Taxonomy = RED;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &RED
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Stage
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        self.metrics.record_success(Duration::from_micros(1));
        vec![event]
    }
}

#[tokio::test]
async fn test_basic_throughput() -> Result<()> {
    let (counter_sink, counter) = CountingSink::new();
    
    let store = EventStore::for_testing().await;
    
    let start = Instant::now();
    let handle = flow! {
        store: store,
        flow_taxonomy: GoldenSignals,
        ("source" => EventGenerator::new(1000, 1000, "TestEvent".to_string()), RED)
        |> ("passthrough" => PassthroughStage::new(), USE)
        |> ("sink" => counter_sink, SAAFE)
    }?;
    
    // Let it run for a bit
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // Shutdown gracefully
    handle.shutdown().await?;
    
    let elapsed = start.elapsed();
    let count = counter.load(Ordering::Relaxed);
    let throughput = count as f64 / elapsed.as_secs_f64();

    println!("Basic throughput test:");
    println!("  Processed: {} events", count);
    println!("  Duration: {:?}", elapsed);
    println!("  Throughput: {:.2} events/sec", throughput);

    assert!(count > 10, "Expected >10 events processed, got {}", count);
    assert!(throughput > 2.0,
        "Expected >2 events/sec, got {:.2}", throughput);
    
    Ok(())
}

#[tokio::test]
async fn test_backpressure() -> Result<()> {
    let (counter_sink, counter) = CountingSink::new();
    
    let store = EventStore::for_testing().await;

    // Fast producer, slow consumer
    let start = Instant::now();
    let handle = flow! {
        store: store,
        flow_taxonomy: GoldenSignals,
        ("source" => EventGenerator::new(1000, 100, "TestEvent".to_string()), RED)
        |> ("cpu_intensive" => CpuIntensiveStage::new(Duration::from_micros(100)), USE)
        |> ("sink" => counter_sink, SAAFE)
    }?;
    
    // Let it run
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // Shutdown gracefully
    handle.shutdown().await?;

    let elapsed = start.elapsed();
    let count = counter.load(Ordering::Relaxed);

    println!("Backpressure test:");
    println!("  Processed: {} events", count);
    println!("  Duration: {:?}", elapsed);
    println!("  Throughput: {:.2} events/sec", count as f64 / elapsed.as_secs_f64());

    // Should process some events despite backpressure
    assert!(count > 0, "Expected some events processed");
    Ok(())
}

#[tokio::test]
async fn test_memory_pressure() -> Result<()> {
    let (counter_sink, counter) = CountingSink::new();
    
    let store = EventStore::for_testing().await;

    let handle = flow! {
        store: store,
        flow_taxonomy: GoldenSignals,
        ("source" => EventGenerator::new(100, 50, "TestEvent".to_string()), RED)
        |> ("memory_intensive" => MemoryIntensiveStage::new(1024 * 1024), USE) // 1MB per event
        |> ("sink" => counter_sink, SAAFE)
    }?;
    
    // Let it run
    tokio::time::sleep(Duration::from_secs(3)).await;
    
    // Shutdown gracefully
    handle.shutdown().await?;
    
    let count = counter.load(Ordering::Relaxed);
    assert!(count > 0, "Expected some events processed");
    Ok(())
}
