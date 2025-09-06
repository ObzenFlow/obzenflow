//! Prometheus 100k Demo with FlowApplication Framework
//! 
//! This demo processes 100,000 events with a rate limiter and uses the new
//! FlowApplication framework for automatic server management.
//! 
//! Run with: cargo run -p obzenflow --example prometheus_100k_demo --features obzenflow_infra/warp-server -- --server
//! 
//! The --server flag will start the web server with:
//! - /metrics endpoint for Prometheus metrics
//! - /api/topology endpoint for flow structure
//! - /health and /ready endpoints for monitoring

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    WriterId,
    id::StageId,
};
use obzenflow_dsl_infra::{flow, sink, source, transform};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryPayload, DeliveryMethod};
use serde_json::json;
use std::time::{Duration, Instant};

/// Source that generates 100k events
#[derive(Clone, Debug)]
struct HighVolumeSource {
    count: usize,
    writer_id: WriterId,
    total_events: usize,
}

impl HighVolumeSource {
    fn new(total_events: usize) -> Self {
        Self {
            count: 0,
            writer_id: WriterId::from(StageId::new()),
            total_events,
        }
    }
}

impl FiniteSourceHandler for HighVolumeSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.count >= self.total_events {
            return None;
        }

        self.count += 1;
        
        // Log progress every 10k events
        if self.count % 10_000 == 0 {
            println!("📊 Generated {} events...", self.count);
        }

        // Every 10th event will be marked for special processing
        let is_priority = self.count % 10 == 0;
        
        // Every 100th event will simulate an error
        let should_fail = self.count % 100 == 0;

        Some(ChainEventFactory::data_event(
            self.writer_id.clone(),
            "data.request",
            json!({
                "id": self.count,
                "priority": is_priority,
                "should_fail": should_fail,
                "batch": self.count / 1000,  // Group into batches of 1000
            }),
        ))
    }

    fn is_complete(&self) -> bool {
        self.count >= self.total_events
    }
}

/// Transform with rate limiting - processes events with a 100ms delay
#[derive(Clone, Debug)]
struct RateLimitedTransform {
    last_process_time: Option<Instant>,
    delay_ms: u64,
}

impl RateLimitedTransform {
    fn new(delay_ms: u64) -> Self {
        Self {
            last_process_time: None,
            delay_ms,
        }
    }
}

#[async_trait]
impl TransformHandler for RateLimitedTransform {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Apply rate limiting with 100ms delay
        if self.delay_ms > 0 {
            std::thread::sleep(Duration::from_millis(self.delay_ms));
        }
        
        let mut payload = event.payload();
        
        // Simulate some processing work
        let processing_start = Instant::now();
        let mut sum = 0u64;
        for i in 0..1000 {
            sum = sum.wrapping_add(i);
        }
        let processing_time = processing_start.elapsed();
        
        payload["rate_limited"] = json!(true);
        payload["processing_time_us"] = json!(processing_time.as_micros());
        payload["checksum"] = json!(sum);
        
        vec![ChainEventFactory::derived_data_event(
            event.writer_id.clone(),
            &event,
            "rate_limited.event",
            payload,
        )]
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

/// Transform that can fail on certain events
#[derive(Clone, Debug)]
struct ErrorProneTransform;

#[async_trait]
impl TransformHandler for ErrorProneTransform {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        let payload = event.payload();
        
        // Simulate variable processing time
        let base_delay = if payload["priority"].as_bool().unwrap_or(false) {
            2  // Priority events are processed faster
        } else {
            5  // Normal events take longer
        };
        std::thread::sleep(Duration::from_millis(base_delay));
        
        // Check if this event should fail
        if payload["should_fail"].as_bool().unwrap_or(false) {
            // Return error event
            let mut error_payload = payload.clone();
            error_payload["error"] = json!("Simulated processing error");
            error_payload["error_code"] = json!(500);
            
            vec![ChainEventFactory::derived_data_event(
                event.writer_id.clone(),
                &event,
                "error.event",
                error_payload,
            )]
        } else {
            // Successful processing
            let mut result_payload = payload.clone();
            result_payload["processed"] = json!(true);
            result_payload["processing_stage"] = json!("error_prone_transform");
            
            vec![ChainEventFactory::derived_data_event(
                event.writer_id.clone(),
                &event,
                "processed.event",
                result_payload,
            )]
        }
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

/// Sink that tracks statistics
#[derive(Clone, Debug)]
struct StatisticsSink {
    success_count: usize,
    error_count: usize,
    total_count: usize,
    start_time: Option<Instant>,
}

impl StatisticsSink {
    fn new() -> Self {
        Self {
            success_count: 0,
            error_count: 0,
            total_count: 0,
            start_time: None,
        }
    }
}

#[async_trait]
impl SinkHandler for StatisticsSink {
    async fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<DeliveryPayload> {
        // Initialize start time on first event
        if self.start_time.is_none() {
            self.start_time = Some(Instant::now());
        }
        
        self.total_count += 1;
        
        // Count successes and errors
        if event.event_type() == "error.event" {
            self.error_count += 1;
        } else {
            self.success_count += 1;
        }
        
        // Log progress every 10k events
        if self.total_count % 10_000 == 0 {
            let elapsed = self.start_time.unwrap().elapsed();
            let rate = self.total_count as f64 / elapsed.as_secs_f64();
            println!(
                "📈 Processed {} events | Success: {} | Errors: {} | Rate: {:.0} events/sec",
                self.total_count, self.success_count, self.error_count, rate
            );
        }
        
        Ok(DeliveryPayload::success(
            "statistics_sink",
            DeliveryMethod::Custom("InMemory".to_string()),
            Some(1),
        ))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Set environment to use prometheus exporter
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "prometheus");

    println!("🚀 Prometheus 100k Demo with FlowApplication Framework");
    println!("======================================================");
    println!("📊 Processing 100,000 events with rate limiting");
    println!("");
    println!("Usage:");
    println!("  Run with server:    cargo run -p obzenflow --example prometheus_100k_demo --features obzenflow_infra/warp-server -- --server");
    println!("  Custom port:        cargo run -p obzenflow --example prometheus_100k_demo --features obzenflow_infra/warp-server -- --server --server-port 8080");
    println!("  Without server:     cargo run -p obzenflow --example prometheus_100k_demo");
    println!("");

    // Use FlowApplication to handle everything
    FlowApplication::run(async {
        flow! {
            name: "prometheus_100k_demo",
            journals: disk_journals(std::path::PathBuf::from("target/prometheus_100k_demo_journal")),
            middleware: [],
            
            stages: {
                // Source generating 100k events
                src = source!("high_volume_source" => HighVolumeSource::new(100_000));
                
                // Rate limited transform with 100ms delay
                rate_limiter = transform!("rate_limiter" => RateLimitedTransform::new(100));
                
                // Error-prone transform for testing error metrics
                processor = transform!("error_processor" => ErrorProneTransform);
                
                // Statistics sink
                snk = sink!("statistics_sink" => StatisticsSink::new());
            },
            
            topology: {
                src |> rate_limiter;
                rate_limiter |> processor;
                processor |> snk;
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {}", e))
    })
    .await
    .map_err(|e| anyhow::anyhow!("Application failed: {}", e))?;

    Ok(())
}