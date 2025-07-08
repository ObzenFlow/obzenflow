// tests/volume_tests.rs
use obzenflow_dsl_infra::{flow, source, transform, sink};
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, TransformHandler, SinkHandler
};
use obzenflow_infra::journal::DiskJournal;
use obzenflow_core::event::event_id::EventId;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::journal::writer_id::WriterId;
// FLOWIP-056-666: Monitoring middleware temporarily disabled pending redesign
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use anyhow::Result;
use async_trait::async_trait;

/// Test fixture that generates events at a controlled rate
struct EventGenerator {
    rate: u64, // events per second  
    total: usize,
    event_type: String,
    generated: usize,
    writer_id: WriterId,
}

impl EventGenerator {
    fn new(rate: u64, total: usize, event_type: String) -> Self {
        Self {
            rate,
            total,
            event_type,
            generated: 0,
            writer_id: WriterId::new(),
        }
    }
}

impl FiniteSourceHandler for EventGenerator {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.generated < self.total {
            let index = self.generated;
            self.generated += 1;
            Some(ChainEvent::new(
                EventId::new(),
                self.writer_id.clone(),
                &self.event_type,
                json!({
                    "index": index,
                    "timestamp": std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                }),
            ))
        } else {
            None
        }
    }
    
    fn is_complete(&self) -> bool {
        self.generated >= self.total
    }
}

/// CPU-intensive stage for testing backpressure
struct CpuIntensiveStage {
    work_duration: Duration,
}

impl CpuIntensiveStage {
    fn new(work_duration: Duration) -> Self {
        Self {
            work_duration,
        }
    }
}

impl TransformHandler for CpuIntensiveStage {
    fn process(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        // Simulate CPU work
        let start = Instant::now();
        while start.elapsed() < self.work_duration {
            // Busy work
            std::hint::black_box(&event);
        }

        event.event_type = "Processed".to_string();
        vec![event]
    }
}

/// Memory-intensive stage
struct MemoryIntensiveStage {
    buffer_size: usize,
}

impl MemoryIntensiveStage {
    fn new(buffer_size: usize) -> Self {
        Self {
            buffer_size,
        }
    }
}

impl TransformHandler for MemoryIntensiveStage {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Allocate some memory
        let _buffer: Vec<u8> = vec![0; self.buffer_size];
        vec![event]
    }
}

/// Sink that just counts events
#[derive(Clone)]
struct CountingSink {
    counter: Arc<AtomicU64>,
}

impl CountingSink {
    fn new() -> (Self, Arc<AtomicU64>) {
        let counter = Arc::new(AtomicU64::new(0));
        (
            Self {
                counter: counter.clone(),
            },
            counter,
        )
    }
}

#[async_trait]
impl SinkHandler for CountingSink {
    fn consume(&mut self, _event: ChainEvent) -> obzenflow_core::Result<()> {
        self.counter.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// Passthrough stage for testing
struct PassthroughStage;

impl PassthroughStage {
    fn new() -> Self {
        Self
    }
}

impl TransformHandler for PassthroughStage {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        vec![event]
    }
}

#[tokio::test]
async fn test_basic_throughput() -> Result<()> {
    let temp_dir = tempdir()?;
    let (counter_sink, counter) = CountingSink::new();
    
    let journal_path = temp_dir.path().to_path_buf();
    let journal = Arc::new(DiskJournal::new(journal_path, "test_throughput").await?);
    
    let start = Instant::now();
    let handle = flow! {
        name: "high_throughput_test",
        journal: journal,
        middleware: [],
        
        stages: {
            src = source!("source" => EventGenerator::new(1000, 1000, "TestEvent".to_string()));
            pass = transform!("passthrough" => PassthroughStage::new());
            snk = sink!("sink" => counter_sink);
        },
        
        topology: {
            src |> pass;
            pass |> snk;
        }
    }.await.map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;
    
    // Run the pipeline
    handle.run().await?;
    
    let elapsed = start.elapsed();
    let count = counter.load(Ordering::Relaxed);
    let throughput = count as f64 / elapsed.as_secs_f64();

    println!("Basic throughput test:");
    println!("  Processed: {} events", count);
    println!("  Duration: {:?}", elapsed);
    println!("  Throughput: {:.2} events/sec", throughput);

    assert!(count >= 1000, "Expected 1000 events processed, got {}", count);
    
    Ok(())
}

#[tokio::test]
async fn test_backpressure() -> Result<()> {
    let temp_dir = tempdir()?;
    let (counter_sink, counter) = CountingSink::new();
    
    let journal_path = temp_dir.path().to_path_buf();
    let journal = Arc::new(DiskJournal::new(journal_path, "test_backpressure").await?);

    // Fast producer, slow consumer
    let start = Instant::now();
    let handle = flow! {
        name: "backpressure_test",
        journal: journal,
        middleware: [],
        
        stages: {
            src = source!("source" => EventGenerator::new(1000, 100, "TestEvent".to_string()));
            cpu = transform!("cpu_intensive" => CpuIntensiveStage::new(Duration::from_micros(100)));
            snk = sink!("sink" => counter_sink);
        },
        
        topology: {
            src |> cpu;
            cpu |> snk;
        }
    }.await.map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;
    
    // Run the pipeline
    handle.run().await?;

    let elapsed = start.elapsed();
    let count = counter.load(Ordering::Relaxed);

    println!("Backpressure test:");
    println!("  Processed: {} events", count);
    println!("  Duration: {:?}", elapsed);
    println!("  Throughput: {:.2} events/sec", count as f64 / elapsed.as_secs_f64());

    // Should process all events
    assert_eq!(count, 100, "Expected 100 events processed, got {}", count);
    Ok(())
}

#[tokio::test]
async fn test_memory_pressure() -> Result<()> {
    let temp_dir = tempdir()?;
    let (counter_sink, counter) = CountingSink::new();
    
    let journal_path = temp_dir.path().to_path_buf();
    let journal = Arc::new(DiskJournal::new(journal_path, "test_memory").await?);

    let handle = flow! {
        name: "disk_journal_memory_usage_test",
        journal: journal,
        middleware: [],
        
        stages: {
            src = source!("source" => EventGenerator::new(100, 50, "TestEvent".to_string()));
            mem = transform!("memory_intensive" => MemoryIntensiveStage::new(1024 * 1024)); // 1MB per event
            snk = sink!("sink" => counter_sink);
        },
        
        topology: {
            src |> mem;
            mem |> snk;
        }
    }.await.map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;
    
    // Run the pipeline
    handle.run().await?;
    
    let count = counter.load(Ordering::Relaxed);
    assert_eq!(count, 50, "Expected 50 events processed, got {}", count);
    Ok(())
}
