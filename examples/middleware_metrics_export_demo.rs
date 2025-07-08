//! Demonstrates middleware metric export from control events
//!
//! This example shows how CONTROL_MIDDLEWARE_STATE and CONTROL_MIDDLEWARE_SUMMARY
//! events are converted to exportable Prometheus metrics.

use std::sync::{Arc, Mutex};
use obzenflow_adapters::monitoring::aggregator::{MetricsAggregator, MetricsEndpoint};
use obzenflow_core::{
    ChainEvent, EventId, WriterId, EventEnvelope,
    event::flow_context::{FlowContext, StageType},
};
use serde_json::json;

#[tokio::main]
async fn main() {
    // Create the metrics aggregator
    let aggregator = Arc::new(Mutex::new(MetricsAggregator::new()));
    
    // Simulate middleware events
    {
        let mut agg = aggregator.lock().unwrap();
        
        // Circuit breaker state transition to open
        let mut cb_state_event = ChainEvent::control(
            ChainEvent::CONTROL_MIDDLEWARE_STATE,
            json!({
                "middleware": "circuit_breaker",
                "state_transition": {
                    "from": "closed",
                    "to": "open",
                    "reason": "threshold_exceeded"
                }
            })
        );
        cb_state_event.flow_context = FlowContext {
            flow_name: "payment_processing".to_string(),
            flow_id: "flow_123".to_string(),
            stage_name: "validate_payment".to_string(),
            stage_type: StageType::Transform,
        };
        let cb_state_envelope = EventEnvelope::new(WriterId::new(), cb_state_event);
        agg.process_event(&cb_state_envelope);
        
        // Circuit breaker summary with rejection stats
        let mut cb_summary_event = ChainEvent::control(
            ChainEvent::CONTROL_MIDDLEWARE_SUMMARY,
            json!({
                "middleware": "circuit_breaker",
                "window_duration_s": 60,
                "stats": {
                    "requests_processed": 850,
                    "requests_rejected": 150,
                    "state": "open",
                    "consecutive_failures": 25
                }
            })
        );
        cb_summary_event.flow_context = FlowContext {
            flow_name: "payment_processing".to_string(),
            flow_id: "flow_123".to_string(),
            stage_name: "validate_payment".to_string(),
            stage_type: StageType::Transform,
        };
        let cb_summary_envelope = EventEnvelope::new(WriterId::new(), cb_summary_event);
        agg.process_event(&cb_summary_envelope);
        
        // Rate limiter summary
        let mut rl_summary_event = ChainEvent::control(
            ChainEvent::CONTROL_MIDDLEWARE_SUMMARY,
            json!({
                "middleware": "rate_limiter",
                "window_duration_s": 60,
                "stats": {
                    "window_stats": {
                        "requests_allowed": 900,
                        "requests_delayed": 100,
                        "utilization": 0.95
                    }
                }
            })
        );
        rl_summary_event.flow_context = FlowContext {
            flow_name: "payment_processing".to_string(),
            flow_id: "flow_123".to_string(),
            stage_name: "submit_payment".to_string(),
            stage_type: StageType::Transform,
        };
        let rl_summary_envelope = EventEnvelope::new(WriterId::new(), rl_summary_event);
        agg.process_event(&rl_summary_envelope);
    }
    
    // Create the metrics endpoint
    let endpoint = MetricsEndpoint::new(aggregator);
    
    // Render metrics in Prometheus format
    match endpoint.render_metrics() {
        Ok(output) => {
            println!("=== Middleware Metrics Export ===\n");
            
            // Print only the middleware-specific metrics
            for line in output.lines() {
                if line.contains("circuit_breaker") || line.contains("rate_limiter") {
                    println!("{}", line);
                }
            }
            
            println!("\n=== Key Observations ===");
            println!("1. Circuit breaker state: 1.0 (open)");
            println!("2. Circuit breaker rejection rate: 15% (150/1000)");
            println!("3. Circuit breaker consecutive failures: 25");
            println!("4. Rate limiter delay rate: 10% (100/1000)");
            println!("5. Rate limiter utilization: 95%");
        }
        Err(e) => {
            eprintln!("Error rendering metrics: {}", e);
        }
    }
}