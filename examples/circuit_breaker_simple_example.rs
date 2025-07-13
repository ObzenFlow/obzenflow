//! Simple Circuit Breaker Example
//! 
//! This example demonstrates the circuit breaker middleware pattern in FlowState:
//! 1. Automatic tracking of consecutive failures
//! 2. Circuit opens after threshold is reached
//! 3. Fast-failing requests when circuit is open
//! 4. Automatic recovery attempts after cooldown period
//! 
//! The circuit breaker middleware handles all state management transparently,
//! allowing handlers to focus on their core logic.

use obzenflow_dsl_infra::{flow, source, transform, sink};
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, TransformHandler, SinkHandler
};
use obzenflow_infra::journal::memory_journals;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::event_id::EventId;
use obzenflow_core::journal::writer_id::WriterId;
use obzenflow_adapters::middleware::{circuit_breaker, CircuitBreakerBuilder};
// Monitoring taxonomies are no longer needed with FLOWIP-056-666
// Metrics are automatically collected by MetricsAggregator from the event journal
use serde_json::json;
use std::sync::atomic::{AtomicU32, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use anyhow::Result;

/// Simulates a client that generates a finite number of requests
struct RequestGenerator {
    total_requests: usize,
    current_index: usize,
    operations: Vec<String>,
    writer_id: WriterId,
}

impl RequestGenerator {
    fn new(total_requests: usize) -> Self {
        Self {
            total_requests,
            current_index: 0,
            operations: vec!["query".to_string(), "update".to_string(), "heavy_compute".to_string()],
            writer_id: WriterId::new(),
        }
    }
}

impl FiniteSourceHandler for RequestGenerator {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.current_index < self.total_requests {
            let id = self.current_index as u32;
            self.current_index += 1;
            let operation = self.operations[fastrand::usize(..self.operations.len())].clone();
            
            Some(ChainEvent::new(
                EventId::new(),
                self.writer_id.clone(),
                "ServiceRequest",
                json!({
                    "id": id,
                    "operation": operation,
                })
            ))
        } else {
            None
        }
    }
    
    fn is_complete(&self) -> bool {
        self.current_index >= self.total_requests
    }
}

/// Simulates an unreliable external service (without circuit breaker logic)
struct UnreliableService {
    failure_rate: f32,
    is_degraded: Arc<AtomicBool>,
    requests_processed: AtomicU32,
    writer_id: WriterId,
}

impl UnreliableService {
    fn new(failure_rate: f32) -> Self {
        Self {
            failure_rate,
            is_degraded: Arc::new(AtomicBool::new(false)),
            requests_processed: AtomicU32::new(0),
            writer_id: WriterId::new(),
        }
    }
    
    /// Simulates service degradation
    fn start_degradation(&self) {
        self.is_degraded.store(true, Ordering::Relaxed);
        let is_degraded = self.is_degraded.clone();
        
        // Service recovers after 10 seconds
        tokio::spawn(async move {
            warn!("Service entering degraded state!");
            tokio::time::sleep(Duration::from_secs(10)).await;
            is_degraded.store(false, Ordering::Relaxed);
            info!("Service recovered from degradation");
        });
    }
}

impl TransformHandler for UnreliableService {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        let count = self.requests_processed.fetch_add(1, Ordering::Relaxed);
        
        // Trigger degradation after 50 requests
        if count == 50 {
            self.start_degradation();
        }
        
        // Extract request data
        let request_id = event.payload.get("id")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;
        let operation = event.payload.get("operation")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        
        // Higher failure rate when degraded
        let failure_rate = if self.is_degraded.load(Ordering::Relaxed) {
            0.8  // 80% failure when degraded
        } else {
            self.failure_rate
        };
        
        // Simulate processing delay
        let delay = if operation == "heavy_compute" {
            100 + fastrand::u64(..200)
        } else {
            20 + fastrand::u64(..80)
        };
        
        // Simulate failures
        if fastrand::f32() < failure_rate {
            warn!("Service failed processing request {}", request_id);
            // Return empty vec to indicate failure
            // The circuit breaker middleware will track these failures
            return vec![];
        }
        
        // Success - return response
        vec![ChainEvent::new(
            EventId::new(),
            self.writer_id.clone(),
            "ServiceResponse",
            json!({
                "request_id": request_id,
                "result": format!("Processed {}", operation),
                "processing_time_ms": delay,
            })
        )]
    }
}

/// Logs successful responses and tracks metrics
struct ResponseTracker {
    total_success: AtomicU32,
    total_failed: AtomicU32,
}

impl ResponseTracker {
    fn new() -> Self {
        Self {
            total_success: AtomicU32::new(0),
            total_failed: AtomicU32::new(0),
        }
    }
}

impl SinkHandler for ResponseTracker {
    fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<()> {
        if event.event_type == "ServiceResponse" {
            let count = self.total_success.fetch_add(1, Ordering::Relaxed) + 1;
            
            if count % 20 == 0 {
                let request_id = event.payload.get("request_id")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let processing_time = event.payload.get("processing_time_ms")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                
                info!(
                    "Successfully processed {} requests. Latest: id={}, time={}ms",
                    count, request_id, processing_time
                );
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info,obzenflow=debug")
        .init();
    
    info!("=== FlowState Circuit Breaker Middleware Example ===");
    info!("This example demonstrates circuit breaker middleware that automatically");
    info!("manages circuit state without handler involvement.");
    
    // Example 1: Basic Circuit Breaker
    info!("\n1. Basic Circuit Breaker (opens after 5 consecutive failures)");
    basic_circuit_breaker().await?;
    
    // Example 2: Circuit Breaker with Custom Threshold
    info!("\n2. Circuit Breaker with Lower Threshold (opens after 3 failures)");
    custom_threshold_circuit_breaker().await?;
    
    // Example 3: Circuit Breaker with Custom Cooldown
    info!("\n3. Circuit Breaker with Custom Cooldown (30 second recovery)");
    custom_cooldown_circuit_breaker().await?;
    
    Ok(())
}

async fn basic_circuit_breaker() -> Result<()> {
    info!("Running basic circuit breaker demo with 200 requests...");
    info!("Circuit breaker will open after 5 consecutive failures");
    
    let handle = flow! {
        journals: memory_journals(),
        middleware: [],
        
        stages: {
            src = source!("request_gen" => RequestGenerator::new(200));
            // Apply circuit breaker middleware with default settings
            service = transform!("service" => UnreliableService::new(0.3), [
                circuit_breaker(5)  // Opens after 5 consecutive failures
            ]);
            sink = sink!("tracker" => ResponseTracker::new());
        },
        
        topology: {
            src |> service;
            service |> sink;
        }
    }.await.map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;
    
    // Run the pipeline
    handle.run().await
        .map_err(|e| anyhow::anyhow!("Failed to run pipeline: {:?}", e))?;
    
    info!("Basic circuit breaker demo complete");
    info!("The circuit breaker protected the system from cascading failures");
    
    Ok(())
}

async fn custom_threshold_circuit_breaker() -> Result<()> {
    info!("Running custom threshold demo with 300 requests...");
    info!("Circuit breaker will open after only 3 consecutive failures");
    
    let handle = flow! {
        journals: memory_journals(),
        middleware: [],
        
        stages: {
            src = source!("request_gen" => RequestGenerator::new(300));
            // Use lower threshold for more sensitive circuit breaking
            service = transform!("service" => UnreliableService::new(0.15), [
                circuit_breaker(3)  // Opens after only 3 consecutive failures
            ]);
            sink = sink!("tracker" => ResponseTracker::new());
        },
        
        topology: {
            src |> service;
            service |> sink;
        }
    }.await.map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;
    
    handle.run().await
        .map_err(|e| anyhow::anyhow!("Failed to run pipeline: {:?}", e))?;
    
    info!("Custom threshold demo complete");
    info!("The circuit opened faster with a lower threshold, providing quicker protection");
    
    Ok(())
}

async fn custom_cooldown_circuit_breaker() -> Result<()> {
    info!("Running custom cooldown demo with 150 requests...");
    info!("Circuit breaker will use 30 second cooldown period");
    
    let handle = flow! {
        journals: memory_journals(),
        middleware: [],
        
        stages: {
            src = source!("request_gen" => RequestGenerator::new(150));
            // Use CircuitBreakerBuilder for custom configuration
            service = transform!("service" => UnreliableService::new(0.2), [
                CircuitBreakerBuilder::new(5)
                    .cooldown(Duration::from_secs(30))
                    .build()
            ]);
            sink = sink!("tracker" => ResponseTracker::new());
        },
        
        topology: {
            src |> service;
            service |> sink;
        }
    }.await.map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;
    
    handle.run().await
        .map_err(|e| anyhow::anyhow!("Failed to run pipeline: {:?}", e))?;
    
    info!("Custom cooldown demo complete");
    info!("The circuit breaker used a longer cooldown period for recovery attempts");
    
    Ok(())
}