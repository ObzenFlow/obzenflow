//! Custom Control Events Demo
//! 
//! This example shows how to create custom middleware that emits control events
//! for business-specific metrics. It demonstrates:
//! - Creating custom middleware
//! - Emitting CONTROL_METRICS_STATE events with business data
//! - Emitting CONTROL_METRICS_RESOURCE events based on processing
//! - How these events flow to MetricsAggregator
//!
//! Run with: cargo run --example custom_control_events_demo

use obzenflow_dsl_infra::{flow, source, transform, sink};
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, TransformHandler, SinkHandler
};
use obzenflow_adapters::middleware::{
    Middleware, MiddlewareContext, MiddlewareAction, 
    MiddlewareTransform, TransformMiddlewareBuilder
};
use obzenflow_adapters::monitoring::aggregator::{MetricsAggregator, MetricsEndpoint};
use obzenflow_infra::journal::memory::MemoryJournal;
use obzenflow_runtime_services::messaging::reactive_journal::ReactiveJournal;
use obzenflow_core::{ChainEvent, EventId, WriterId};
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::time::{sleep, Duration};
use anyhow::Result;

/// Business event representing a financial transaction
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Transaction {
    id: String,
    amount: f64,
    currency: String,
    transaction_type: String,
    merchant_category: String,
}

/// Custom middleware that tracks business-specific metrics
struct TransactionMetricsMiddleware {
    start_times: Arc<Mutex<std::collections::HashMap<String, Instant>>>,
}

impl TransactionMetricsMiddleware {
    fn new() -> Self {
        Self {
            start_times: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }
}

impl Middleware for TransactionMetricsMiddleware {
    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        // Track processing start time
        if let Some(id) = event.payload["id"].as_str() {
            self.start_times.lock().unwrap().insert(id.to_string(), Instant::now());
        }
        
        // Emit state metrics for transaction volume
        if event.event_type == "transaction.received" {
            if let Ok(txn) = serde_json::from_value::<Transaction>(event.payload.clone()) {
                // Emit business metrics as control events
                ctx.write_control_event(ChainEvent::control(
                    ChainEvent::CONTROL_METRICS_STATE,
                    json!({
                        "transaction_amount": txn.amount,
                        "transaction_currency": txn.currency,
                        "merchant_category": txn.merchant_category,
                        "transaction_type": txn.transaction_type,
                    })
                ));
            }
        }
        
        MiddlewareAction::Continue
    }
    
    fn post_handle(&self, event: &ChainEvent, results: &[ChainEvent], ctx: &mut MiddlewareContext) {
        // Calculate processing duration
        if let Some(id) = event.payload["id"].as_str() {
            if let Some(start_time) = self.start_times.lock().unwrap().remove(id) {
                let duration = start_time.elapsed();
                
                // Emit resource metrics based on processing time
                ctx.write_control_event(ChainEvent::control(
                    ChainEvent::CONTROL_METRICS_RESOURCE,
                    json!({
                        "processing_duration_ms": duration.as_millis() as u64,
                        "processed_count": if results.is_empty() { 0 } else { 1 },
                        "event_type": event.event_type,
                    })
                ));
                
                // Emit anomaly if processing was unusually slow
                if duration.as_millis() > 100 {
                    ctx.write_control_event(ChainEvent::control(
                        ChainEvent::CONTROL_METRICS_ANOMALY,
                        json!({
                            "anomaly_type": "slow_processing",
                            "duration_ms": duration.as_millis() as u64,
                            "threshold_ms": 100,
                            "transaction_id": id,
                        })
                    ));
                }
            }
        }
        
        // Track amendments (filtering or expansion)
        if results.is_empty() && event.event_type == "transaction.received" {
            ctx.write_control_event(ChainEvent::control(
                ChainEvent::CONTROL_METRICS_AMENDMENT,
                json!({
                    "amendment_type": "filtered",
                    "reason": "validation_failed",
                    "original_event_type": event.event_type,
                })
            ));
        } else if results.len() > 1 {
            ctx.write_control_event(ChainEvent::control(
                ChainEvent::CONTROL_METRICS_AMENDMENT,
                json!({
                    "amendment_type": "expanded",
                    "original_count": 1,
                    "result_count": results.len(),
                    "expansion_ratio": results.len() as f64,
                })
            ));
        }
    }
}

/// Risk assessment middleware that emits risk-related control events
struct RiskAssessmentMiddleware;

impl Middleware for RiskAssessmentMiddleware {
    fn post_handle(&self, _event: &ChainEvent, results: &[ChainEvent], ctx: &mut MiddlewareContext) {
        for result in results {
            if result.event_type == "transaction.validated" {
                if let Ok(txn) = serde_json::from_value::<Transaction>(result.payload.clone()) {
                    // Assess risk based on amount
                    let risk_score = if txn.amount > 10000.0 { 0.9 } 
                                   else if txn.amount > 5000.0 { 0.6 }
                                   else if txn.amount > 1000.0 { 0.3 }
                                   else { 0.1 };
                    
                    // Emit risk assessment as state metric
                    ctx.write_control_event(ChainEvent::control(
                        ChainEvent::CONTROL_METRICS_STATE,
                        json!({
                            "risk_score": risk_score,
                            "risk_category": if risk_score > 0.7 { "high" } 
                                           else if risk_score > 0.4 { "medium" } 
                                           else { "low" },
                            "transaction_amount": txn.amount,
                        })
                    ));
                    
                    // Flag high-risk transactions as anomalies
                    if risk_score > 0.7 {
                        ctx.write_control_event(ChainEvent::control(
                            ChainEvent::CONTROL_METRICS_ANOMALY,
                            json!({
                                "anomaly_type": "high_risk_transaction",
                                "risk_score": risk_score,
                                "amount": txn.amount,
                                "transaction_id": txn.id,
                            })
                        ));
                    }
                }
            }
        }
    }
}

/// Source that generates various transaction patterns
struct TransactionSource {
    transactions: Vec<Transaction>,
    index: usize,
    writer_id: WriterId,
}

impl TransactionSource {
    fn new() -> Self {
        let transactions = vec![
            // Normal transactions
            Transaction {
                id: "txn_001".into(),
                amount: 49.99,
                currency: "USD".into(),
                transaction_type: "purchase".into(),
                merchant_category: "grocery".into(),
            },
            Transaction {
                id: "txn_002".into(),
                amount: 125.00,
                currency: "USD".into(),
                transaction_type: "purchase".into(),
                merchant_category: "electronics".into(),
            },
            // Medium risk
            Transaction {
                id: "txn_003".into(),
                amount: 2500.00,
                currency: "USD".into(),
                transaction_type: "transfer".into(),
                merchant_category: "p2p".into(),
            },
            // High risk
            Transaction {
                id: "txn_004".into(),
                amount: 15000.00,
                currency: "USD".into(),
                transaction_type: "withdrawal".into(),
                merchant_category: "atm".into(),
            },
            // Small transactions
            Transaction {
                id: "txn_005".into(),
                amount: 9.99,
                currency: "USD".into(),
                transaction_type: "purchase".into(),
                merchant_category: "food".into(),
            },
            Transaction {
                id: "txn_006".into(),
                amount: 3.50,
                currency: "USD".into(),
                transaction_type: "purchase".into(),
                merchant_category: "transport".into(),
            },
            // Another high risk
            Transaction {
                id: "txn_007".into(),
                amount: 25000.00,
                currency: "EUR".into(),
                transaction_type: "wire".into(),
                merchant_category: "international".into(),
            },
        ];
        
        Self {
            transactions,
            index: 0,
            writer_id: WriterId::new(),
        }
    }
}

impl FiniteSourceHandler for TransactionSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.index >= self.transactions.len() {
            return None;
        }
        
        let txn = &self.transactions[self.index];
        self.index += 1;
        
        println!("💳 Emitting transaction {} (${:.2} {})", 
                 txn.id, txn.amount, txn.currency);
        
        Some(ChainEvent::new(
            EventId::new(),
            self.writer_id.clone(),
            "transaction.received",
            json!(txn),
        ))
    }
    
    fn is_complete(&self) -> bool {
        self.index >= self.transactions.len()
    }
}

/// Validator that simulates processing delays
struct TransactionValidator;

impl TransactionHandler for TransactionValidator {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if let Ok(txn) = serde_json::from_value::<Transaction>(event.payload.clone()) {
            // Simulate varying processing times
            let delay = if txn.amount > 10000.0 { 150 } 
                       else if txn.amount > 1000.0 { 50 }
                       else { 10 };
            
            std::thread::sleep(Duration::from_millis(delay));
            
            println!("  ✓ Validated {} in {}ms", txn.id, delay);
            
            let mut validated = event.clone();
            validated.event_type = "transaction.validated".to_string();
            vec![validated]
        } else {
            vec![]
        }
    }
}

/// Sink that collects results
struct TransactionSink {
    count: Arc<Mutex<usize>>,
}

impl TransactionSink {
    fn new() -> Self {
        Self {
            count: Arc::new(Mutex::new(0)),
        }
    }
}

impl SinkHandler for TransactionSink {
    fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<()> {
        *self.count.lock().unwrap() += 1;
        
        if let Ok(txn) = serde_json::from_value::<Transaction>(event.payload) {
            println!("  💰 Processed transaction {} (${:.2})", txn.id, txn.amount);
        }
        
        Ok(())
    }
}

/// Helper to wrap a handler with custom middleware
trait CustomMiddlewareExt: TransformHandler + Sized {
    fn with_custom_middleware(self) -> MiddlewareTransform<Self> {
        MiddlewareTransform::new(self)
            .with_middleware(Box::new(TransactionMetricsMiddleware::new()))
            .with_middleware(Box::new(RiskAssessmentMiddleware))
    }
}

impl<T: TransformHandler> CustomMiddlewareExt for T {}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("obzenflow=debug,custom_control_events_demo=debug")
        .init();

    println!("🎯 Custom Control Events Demo");
    println!("=============================\n");
    
    // Create journal and reactive journal
    let memory_journal = Arc::new(MemoryJournal::new());
    let reactive_journal = Arc::new(ReactiveJournal::new(memory_journal.clone()));
    
    // Create and start MetricsAggregator
    println!("📊 Starting MetricsAggregator...");
    let aggregator = Arc::new(Mutex::new(MetricsAggregator::new()));
    MetricsAggregator::start(aggregator.clone(), reactive_journal.clone()).await?;
    
    // Create metrics endpoint
    let endpoint = MetricsEndpoint::new(aggregator.clone());
    
    sleep(Duration::from_millis(100)).await;
    
    // Create sink
    let sink = TransactionSink::new();
    let processed_count = sink.count.clone();
    
    println!("🚀 Building flow with custom middleware...\n");
    
    // Note: Since the DSL doesn't support custom middleware directly,
    // we need to create the validator with middleware manually
    let validator_with_middleware = TransactionValidator.with_custom_middleware();
    
    // Build flow
    let flow_handle = flow! {
        journal: reactive_journal.clone(),
        middleware: [],
        
        stages: {
            source = source!("transaction_source" => TransactionSource::new());
            
            // Use the pre-wrapped validator
            validator = transform!("transaction_validator" => validator_with_middleware);
            
            sink = sink!("transaction_sink" => sink);
        },
        
        topology: {
            source |> validator;
            validator |> sink;
        }
    }.await?;
    
    println!("▶️  Running flow...\n");
    
    // Run the flow
    flow_handle.run().await?;
    
    // Give time for processing
    sleep(Duration::from_secs(2)).await;
    
    // Display results
    println!("\n📈 Results:");
    println!("   Transactions processed: {}", processed_count.lock().unwrap());
    
    // Get metrics
    println!("\n📊 Metrics from Custom Control Events:");
    let metrics_text = endpoint.render_metrics()?;
    
    // Check for our custom metrics
    let has_transaction_metrics = metrics_text.contains("transaction_amount") || 
                                 metrics_text.contains("risk_score");
    let has_anomaly_metrics = metrics_text.contains("obzenflow_anomalies_total");
    
    if has_transaction_metrics {
        println!("   ✅ Custom transaction metrics captured");
    }
    if has_anomaly_metrics {
        println!("   ✅ Anomaly detection metrics captured");
    }
    
    // Get detailed metrics from aggregator
    {
        let agg = aggregator.lock().unwrap();
        
        let total_events = agg.get_events_total("custom_control_events_demo", "transaction_validator");
        let anomalies = agg.get_anomalies_total("custom_control_events_demo", "transaction_validator");
        
        println!("\n🔍 Aggregated Metrics:");
        println!("   Total events processed: {:.0}", total_events);
        println!("   Anomalies detected: {:.0}", anomalies);
        
        // Note: Custom business metrics (transaction amounts, risk scores) would need
        // additional MetricsAggregator enhancements to be exposed as specific metrics
    }
    
    println!("\n✅ Demo complete!");
    println!("\n💡 Key Takeaways:");
    println!("   1. Custom middleware can emit any type of control event");
    println!("   2. Business metrics can be tracked via CONTROL_METRICS_STATE");
    println!("   3. Performance metrics via CONTROL_METRICS_RESOURCE");
    println!("   4. Risk/anomaly detection via CONTROL_METRICS_ANOMALY");
    println!("   5. All metrics flow through the journal to MetricsAggregator");
    
    Ok(())
}