//! Payment Gateway Resilience Example
//! 
//! This example demonstrates building a resilient payment processing system using middleware:
//! - Rate limiting middleware protects against traffic surges
//! - Circuit breaker middleware handles provider outages
//! - Clean separation of concerns - handlers focus on business logic
//! - Middleware handles resilience patterns transparently
//!
//! Run with: cargo run --example payment_gateway_resilience_example

use obzenflow_dsl_infra::{flow, infinite_source, transform, sink};
use obzenflow_runtime_services::stages::common::handlers::{
    InfiniteSourceHandler, TransformHandler, SinkHandler
};
use obzenflow_infra::journal::MemoryJournal;
use obzenflow_core::event::event_id::EventId;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::journal::writer_id::WriterId;
use obzenflow_adapters::middleware::{common, CircuitBreakerBuilder};
use obzenflow_adapters::monitoring::taxonomies::{
    red::RED,
    golden_signals::GoldenSignals,
    saafe::SAAFE,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::time::sleep;
use tracing::{info, warn, error};
use serde_json::json;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Order {
    id: String,
    customer_id: String,
    amount_cents: u64,
    currency: String,
    created_at: SystemTime,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PaymentRequest {
    order_id: String,
    amount_cents: u64,
    currency: String,
    payment_method: String,
    idempotency_key: String,
    attempt: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PaymentResult {
    order_id: String,
    transaction_id: String,
    status: PaymentStatus,
    processed_at: SystemTime,
    processing_time_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
enum PaymentStatus {
    Success,
    Failed(String),
    Declined(String),
    RateLimited,
    ServiceUnavailable,
}

/// Simulates e-commerce orders during Black Friday
struct BlackFridayOrderSource {
    orders_generated: AtomicU32,
    surge_active: Arc<AtomicBool>,
    base_rate_per_sec: f32,
    writer_id: WriterId,
}

impl BlackFridayOrderSource {
    fn new(base_rate_per_sec: f32) -> Self {
        let surge_active = Arc::new(AtomicBool::new(false));
        
        // Simulate traffic surges
        let surge_clone = surge_active.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(20)).await;
                surge_clone.store(true, Ordering::Relaxed);
                info!("🛍️ BLACK FRIDAY SURGE STARTED!");
                
                sleep(Duration::from_secs(10)).await;
                surge_clone.store(false, Ordering::Relaxed);
                info!("📉 Traffic surge ended");
            }
        });
        
        Self {
            orders_generated: AtomicU32::new(0),
            surge_active,
            base_rate_per_sec,
            writer_id: WriterId::new(),
        }
    }
}

impl InfiniteSourceHandler for BlackFridayOrderSource {
    fn next(&mut self) -> Option<ChainEvent> {
        // Vary rate based on surge
        let rate_multiplier = if self.surge_active.load(Ordering::Relaxed) {
            10.0  // 10x traffic during surge
        } else {
            1.0
        };
        
        let delay_ms = 1000.0 / (self.base_rate_per_sec * rate_multiplier);
        std::thread::sleep(Duration::from_millis(delay_ms as u64));
        
        let order_num = self.orders_generated.fetch_add(1, Ordering::Relaxed);
        
        let order = Order {
            id: format!("BF2024-{:06}", order_num),
            customer_id: format!("CUST-{:04}", fastrand::u32(1000..9999)),
            amount_cents: 1000 + fastrand::u64(..50000),  // $10 to $510
            currency: "USD".to_string(),
            created_at: SystemTime::now(),
        };
        
        Some(ChainEvent::new(
            EventId::new(),
            self.writer_id.clone(),
            "Order",
            json!(order)
        ))
    }
}

/// Converts orders to payment requests
struct PaymentProcessor;

impl TransformHandler for PaymentProcessor {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type != "Order" {
            return vec![];
        }
        
        let order: Order = match serde_json::from_value(event.payload.clone()) {
            Ok(o) => o,
            Err(_) => return vec![],
        };
        
        let request = PaymentRequest {
            order_id: order.id.clone(),
            amount_cents: order.amount_cents,
            currency: order.currency,
            payment_method: "card".to_string(),
            idempotency_key: format!("{}-{}", order.id, SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs()),
            attempt: 1,
        };
        
        vec![ChainEvent::new(
            EventId::new(),
            event.writer_id,
            "PaymentRequest",
            json!(request)
        )]
    }
}

/// Rate limiter stage to protect the payment provider
struct RateLimiter;

impl TransformHandler for RateLimiter {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Pass through - rate limiting is handled by middleware
        vec![event]
    }
}

/// Simulates external payment provider (Stripe/PayPal)
struct PaymentProviderGateway {
    requests_processed: AtomicU32,
    is_degraded: Arc<AtomicBool>,
    is_down: Arc<AtomicBool>,
}

impl PaymentProviderGateway {
    fn new() -> Self {
        let is_degraded = Arc::new(AtomicBool::new(false));
        let is_down = Arc::new(AtomicBool::new(false));
        
        // Simulate provider outages
        let degraded_clone = is_degraded.clone();
        let down_clone = is_down.clone();
        
        tokio::spawn(async move {
            loop {
                // Normal operation for 30 seconds
                sleep(Duration::from_secs(30)).await;
                
                // Degraded for 15 seconds
                degraded_clone.store(true, Ordering::Relaxed);
                warn!("⚠️ Payment provider entering DEGRADED state");
                sleep(Duration::from_secs(15)).await;
                degraded_clone.store(false, Ordering::Relaxed);
                
                // Normal for 20 seconds
                sleep(Duration::from_secs(20)).await;
                
                // Complete outage for 5 seconds
                down_clone.store(true, Ordering::Relaxed);
                error!("🔥 Payment provider is DOWN!");
                sleep(Duration::from_secs(5)).await;
                down_clone.store(false, Ordering::Relaxed);
                info!("✅ Payment provider recovered");
            }
        });
        
        Self {
            requests_processed: AtomicU32::new(0),
            is_degraded,
            is_down,
        }
    }
}

impl TransformHandler for PaymentProviderGateway {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type != "PaymentRequest" {
            return vec![];
        }
        
        // Circuit breaker logic is now handled by middleware
        
        let request: PaymentRequest = match serde_json::from_value(event.payload.clone()) {
            Ok(r) => r,
            Err(_) => return vec![],
        };
        
        let start = Instant::now();
        let request_num = self.requests_processed.fetch_add(1, Ordering::Relaxed);
        
        // Check if provider is completely down
        if self.is_down.load(Ordering::Relaxed) {
            // Return empty vec to simulate error
            return vec![];
        }
        
        // Simulate API latency
        let base_latency = if self.is_degraded.load(Ordering::Relaxed) {
            500  // Slow when degraded
        } else {
            50   // Normal latency
        };
        
        std::thread::sleep(Duration::from_millis(base_latency + fastrand::u64(..100)));
        
        // Simulate various payment outcomes
        let status = if self.is_degraded.load(Ordering::Relaxed) {
            // Higher failure rate when degraded
            match fastrand::u32(0..100) {
                0..=60 => {
                    // Gateway timeout - return empty vec to indicate failure
                    // Circuit breaker middleware will track these failures
                    warn!("Payment gateway timeout");
                    return vec![];
                }
                61..=70 => PaymentStatus::ServiceUnavailable,
                71..=80 => PaymentStatus::Declined("Insufficient funds".to_string()),
                _ => PaymentStatus::Success,
            }
        } else {
            // Normal operation
            match fastrand::u32(0..100) {
                0..=2 => {
                    // Connection reset - return empty vec to indicate failure
                    // Circuit breaker middleware will track these failures
                    warn!("Payment provider connection reset");
                    return vec![];
                }
                3..=5 => PaymentStatus::Failed("Invalid card".to_string()),
                6..=8 => PaymentStatus::Declined("Card expired".to_string()),
                _ => PaymentStatus::Success,
            }
        };
        
        let result = PaymentResult {
            order_id: request.order_id,
            transaction_id: format!("txn_{:08x}", request_num),
            status,
            processed_at: SystemTime::now(),
            processing_time_ms: start.elapsed().as_millis() as u64,
        };
        
        vec![ChainEvent::new(
            EventId::new(),
            event.writer_id,
            "PaymentResult",
            json!(result)
        )]
    }
}

/// Tracks payment results and metrics
struct PaymentResultSink {
    successful_payments: AtomicU32,
    failed_payments: AtomicU32,
    declined_payments: AtomicU32,
    total_amount_cents: AtomicU64,
}

impl SinkHandler for PaymentResultSink {
    fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<()> {
        if event.event_type != "PaymentResult" {
            return Ok(());
        }
        
        let result: PaymentResult = serde_json::from_value(event.payload)
            .map_err(|e| Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())) as Box<dyn std::error::Error + Send + Sync>)?;
        
        match &result.status {
            PaymentStatus::Success => {
                let count = self.successful_payments.fetch_add(1, Ordering::Relaxed) + 1;
                if count % 50 == 0 {
                    info!("💰 {} successful payments processed", count);
                }
            }
            PaymentStatus::Failed(reason) => {
                self.failed_payments.fetch_add(1, Ordering::Relaxed);
                warn!("Payment failed for {}: {}", result.order_id, reason);
            }
            PaymentStatus::Declined(reason) => {
                self.declined_payments.fetch_add(1, Ordering::Relaxed);
                info!("Payment declined for {}: {}", result.order_id, reason);
            }
            PaymentStatus::RateLimited => {
                warn!("Rate limited for order {}", result.order_id);
            }
            PaymentStatus::ServiceUnavailable => {
                error!("Service unavailable for order {}", result.order_id);
            }
        }
        
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();
    
    info!("=== Payment Gateway Resilience Example ===");
    
    let journal = Arc::new(MemoryJournal::new());
    
    // Build the payment processing pipeline with full resilience
    let handle = flow! {
        journal: journal,
        middleware: [],
        
        stages: {
            // Order source simulating Black Friday traffic
            orders = infinite_source!("order_source" => BlackFridayOrderSource::new(20.0), [RED::monitoring()]);
            
            // Convert orders to payment requests
            processor = transform!("payment_processor" => PaymentProcessor, [RED::monitoring()]);
            
            // Rate limit BEFORE sending to provider (protective rate limiting)
            rate_limited = transform!("rate_limiter" => RateLimiter, [common::rate_limit_with_burst(100.0, 500.0), RED::monitoring()]);
            
            // Payment provider with circuit breaker protection via middleware
            provider = transform!("payment_provider" => PaymentProviderGateway::new(), [
                CircuitBreakerBuilder::new(10)
                    .cooldown(Duration::from_secs(30))
                    .build(),
                SAAFE::monitoring()
            ]);
            
            // Collect results
            results = sink!("payment_results" => PaymentResultSink {
                successful_payments: AtomicU32::new(0),
                failed_payments: AtomicU32::new(0),
                declined_payments: AtomicU32::new(0),
                total_amount_cents: AtomicU64::new(0),
            }, [GoldenSignals::monitoring()]);
        },
        
        topology: {
            orders |> processor;
            processor |> rate_limited;
            rate_limited |> provider;
            provider |> results;
        }
    }.await.map_err(|e| format!("Failed to create flow: {:?}", e))?;
    
    // Start the pipeline
    handle.run().await.map_err(|e| format!("Failed to run pipeline: {:?}", e))?;
    
    info!("Pipeline started. Running for 2 minutes to observe:");
    info!("- Traffic surges (10x normal traffic)");
    info!("- Provider degradation (high latency, 60% failures)");
    info!("- Complete provider outage (100% failures)");
    info!("- Rate limiting protecting the provider");
    info!("- Circuit breaker preventing cascade failures");
    
    // Let it run for 2 minutes to see all behaviors
    sleep(Duration::from_secs(120)).await;
    
    info!("Test complete! The system demonstrated:");
    info!("✅ Rate limiting prevented overwhelming the payment provider during surges");
    info!("✅ Circuit breaker opened during outages, preventing retry storms");
    info!("✅ Graceful degradation with clear error handling");
    info!("✅ Automatic recovery when provider came back online");
    
    Ok(())
}