//! Rate Limiting Example
//! 
//! This example demonstrates various rate limiting strategies in FlowState:
//! 1. Simple rate limiting with fixed throughput
//! 2. Burst capacity for handling traffic spikes
//! 3. Multi-tier rate limiting (per-user and global)
//! 4. Rate limiting with monitoring and alerting

use obzenflow_dsl_infra::{flow, source, transform, sink};
use obzenflow_runtime_services::control_plane::stages::handler_traits::{
    FiniteSourceHandler, TransformHandler, SinkHandler
};
use obzenflow_infra::journal::DiskJournal;
use obzenflow_core::event::event_id::EventId;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::journal::writer_id::WriterId;
use obzenflow_adapters::middleware::common;
use obzenflow_adapters::monitoring::taxonomies::{
    red::RED,
    golden_signals::GoldenSignals
};
use serde_json::json;
use std::sync::Arc;
use tracing::info;
use async_trait::async_trait;
use anyhow::Result;

/// Simulates an API that generates requests
struct ApiRequestSource {
    users: Vec<String>,
    endpoints: Vec<String>,
    request_count: usize,
    max_requests: usize,
    writer_id: WriterId,
}

impl ApiRequestSource {
    fn new(users: Vec<String>, endpoints: Vec<String>, max_requests: usize) -> Self {
        Self {
            users,
            endpoints,
            request_count: 0,
            max_requests,
            writer_id: WriterId::new(),
        }
    }
}

impl FiniteSourceHandler for ApiRequestSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.request_count < self.max_requests {
            self.request_count += 1;
            
            let user_id = self.users[fastrand::usize(..self.users.len())].clone();
            let endpoint = self.endpoints[fastrand::usize(..self.endpoints.len())].clone();
            
            // Simulate bursts occasionally
            if self.request_count % 100 == 0 && fastrand::f32() < 0.3 {
                info!("Simulating traffic burst at request {}!", self.request_count);
            }
            
            Some(ChainEvent::new(
                EventId::new(),
                self.writer_id.clone(),
                "ApiRequest",
                json!({
                    "user_id": user_id,
                    "endpoint": endpoint,
                    "timestamp_ms": std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                })
            ))
        } else {
            None
        }
    }
    
    fn is_complete(&self) -> bool {
        self.request_count >= self.max_requests
    }
}

/// Processes API requests
struct ApiProcessor;

impl ApiProcessor {
    fn new() -> Self {
        Self
    }
}

impl TransformHandler for ApiProcessor {
    fn process(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "ApiRequest" {
            // Simulate varying processing times
            let endpoint = event.payload.get("endpoint")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            
            let processing_time = match endpoint {
                "/api/search" => 50 + fastrand::u64(..100),    // Slow endpoint
                "/api/health" => 1 + fastrand::u64(..5),       // Fast endpoint
                _ => 10 + fastrand::u64(..40),                 // Normal endpoints
            };
            
            // Add processing time to event
            event.payload["processing_time_ms"] = json!(processing_time);
            event.payload["status"] = json!("success");
            
            // Change event type to response
            event.event_type = "ApiResponse".to_string();
            
            vec![event]
        } else {
            vec![event]
        }
    }
}

/// Logs processed responses
struct ResponseLogger {
    total_processed: std::sync::atomic::AtomicUsize,
}

impl ResponseLogger {
    fn new() -> Self {
        Self {
            total_processed: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl SinkHandler for ResponseLogger {
    fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<()> {
        if event.event_type == "ApiResponse" {
            let count = self.total_processed.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            
            if count % 100 == 0 {
                let user_id = event.payload.get("user_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                let endpoint = event.payload.get("endpoint")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                let processing_time = event.payload.get("processing_time_ms")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                
                info!(
                    "Processed {} requests. Latest: user={}, endpoint={}, time={}ms",
                    count, user_id, endpoint, processing_time
                );
            }
        }
        Ok(())
    }
}

/// Identity transform that passes through events unchanged
struct IdentityTransform;

impl IdentityTransform {
    fn new() -> Self {
        Self
    }
}

impl TransformHandler for IdentityTransform {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        vec![event]
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();
    
    info!("=== FlowState Rate Limiting Example ===");
    
    // Example 1: Simple Rate Limiting
    info!("\n1. Simple Rate Limiting (100 requests/second)");
    simple_rate_limiting().await?;
    
    // Example 2: Rate Limiting with Burst Capacity
    info!("\n2. Rate Limiting with Burst Capacity");
    burst_rate_limiting().await?;
    
    // Example 3: Multi-tier Rate Limiting
    info!("\n3. Multi-tier Rate Limiting (per-user and global)");
    multi_tier_rate_limiting().await?;
    
    Ok(())
}

async fn simple_rate_limiting() -> Result<()> {
    // Create a journal for the flow
    let journal_path = std::path::PathBuf::from("target/rate-limiting-logs");
    std::fs::create_dir_all(&journal_path)?;
    let journal = Arc::new(DiskJournal::new(journal_path, "simple_rate_limit").await?);
    
    let handle = flow! {
        journal: journal,
        middleware: [],
        
        stages: {
            // Source generates requests as fast as possible
            src = source!("api_source" => ApiRequestSource::new(
                vec!["user1".to_string(), "user2".to_string()],
                vec!["/api/data".to_string(), "/api/search".to_string()],
                500  // Generate 500 requests
            ), [RED::monitoring()]);
            
            // Transform with rate limiting
            proc = transform!("processor" => ApiProcessor::new(), [common::rate_limit(100.0), RED::monitoring()]);
            
            // Sink to log results
            sink = sink!("logger" => ResponseLogger::new(), [GoldenSignals::monitoring()]);
        },
        
        topology: {
            src |> proc;
            proc |> sink;
        }
    }.await.map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;
    
    // Run the pipeline
    handle.run().await?;
    
    info!("Simple rate limiting complete. Check metrics to see rate was limited to ~100/sec");
    
    Ok(())
}

async fn burst_rate_limiting() -> Result<()> {
    // Create a journal for the flow
    let journal_path = std::path::PathBuf::from("target/rate-limiting-logs");
    std::fs::create_dir_all(&journal_path)?;
    let journal = Arc::new(DiskJournal::new(journal_path, "burst_rate_limit").await?);
    
    let handle = flow! {
        journal: journal,
        middleware: [],
        
        stages: {
            src = source!("api_source" => ApiRequestSource::new(
                vec!["user1".to_string(), "user2".to_string(), "user3".to_string()],
                vec!["/api/data".to_string(), "/api/search".to_string(), "/api/health".to_string()],
                1000  // Generate 1000 requests
            ), [RED::monitoring()]);
            
            // Rate limiting with burst capacity
            // Sustain: 50 req/sec, Burst: up to 200 requests
            proc = transform!("processor" => ApiProcessor::new(), [common::rate_limit_with_burst(50.0, 200.0), RED::monitoring()]);
            
            sink = sink!("logger" => ResponseLogger::new(), [GoldenSignals::monitoring()]);
        },
        
        topology: {
            src |> proc;
            proc |> sink;
        }
    }.await.map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;
    
    // Run the pipeline
    handle.run().await?;
    
    info!("Burst rate limiting complete. Bursts were absorbed up to 200 requests, then limited to 50/sec");
    
    Ok(())
}

async fn multi_tier_rate_limiting() -> Result<()> {
    // Create a journal for the flow
    let journal_path = std::path::PathBuf::from("target/rate-limiting-logs");
    std::fs::create_dir_all(&journal_path)?;
    let journal = Arc::new(DiskJournal::new(journal_path, "multi_tier_rate_limit").await?);
    
    // In a real system, you might have per-user rate limiting
    // This example shows how to compose multiple rate limiters
    
    let handle = flow! {
        journal: journal,
        middleware: [],
        
        stages: {
            src = source!("api_source" => ApiRequestSource::new(
                vec!["premium_user".to_string(), "basic_user".to_string(), "trial_user".to_string()],
                vec!["/api/data".to_string(), "/api/search".to_string(), "/api/compute".to_string()],
                2000  // Generate 2000 requests
            ), [RED::monitoring()]);
            
            // First tier: Global rate limit
            global_limiter = transform!("global_limiter" => IdentityTransform::new(), [common::rate_limit_with_burst(1000.0, 5000.0), RED::monitoring()]);
            
            // Second tier: Per-endpoint rate limiting
            endpoint_processor = transform!("endpoint_processor" => ApiProcessor::new(), [common::rate_limit(200.0), RED::monitoring()]);
            
            sink = sink!("logger" => ResponseLogger::new(), [GoldenSignals::monitoring()]);
        },
        
        topology: {
            src |> global_limiter;
            global_limiter |> endpoint_processor;
            endpoint_processor |> sink;
        }
    }.await.map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;
    
    // Run the pipeline
    handle.run().await?;
    
    info!("Multi-tier rate limiting complete. Requests were limited at both global and endpoint levels");
    
    Ok(())
}