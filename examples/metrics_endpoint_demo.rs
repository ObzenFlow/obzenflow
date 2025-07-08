//! Example demonstrating the MetricsEndpoint with Prometheus format
//!
//! This example shows how to:
//! 1. Create a MetricsAggregator
//! 2. Process some events to generate metrics
//! 3. Expose metrics via MetricsEndpoint in Prometheus format

use std::sync::{Arc, Mutex};
use obzenflow_adapters::monitoring::aggregator::{MetricsAggregator, MetricsEndpoint};
use obzenflow_core::{
    ChainEvent, EventId, WriterId, EventEnvelope,
    event::flow_context::{FlowContext, StageType},
    event::processing_info::ProcessingInfo,
    event::processing_outcome::ProcessingOutcome,
};
use serde_json::json;

#[tokio::main]
async fn main() {
    // Create the metrics aggregator
    let aggregator = Arc::new(Mutex::new(MetricsAggregator::new()));
    
    // Simulate processing some events
    {
        let mut agg = aggregator.lock().unwrap();
        
        // Process a successful event
        let event1 = create_event("order_processing", "validate_order", 45, ProcessingOutcome::Success);
        agg.process_event(&event1);
        
        // Process another successful event
        let event2 = create_event("order_processing", "calculate_total", 120, ProcessingOutcome::Success);
        agg.process_event(&event2);
        
        // Process an error event
        let event3 = create_event("order_processing", "validate_order", 25, 
            ProcessingOutcome::Error("Invalid credit card".to_string()));
        agg.process_event(&event3);
        
        // Process a control event for state metrics
        let mut state_event = ChainEvent::control(
            ChainEvent::CONTROL_METRICS_STATE,
            json!({
                "queue_depth": 15,
                "in_flight": 3
            })
        );
        state_event.flow_context = FlowContext {
            flow_name: "order_processing".to_string(),
            flow_id: "demo_flow".to_string(),
            stage_name: "validate_order".to_string(),
            stage_type: StageType::Transform,
        };
        let state_envelope = EventEnvelope::new(WriterId::new(), state_event);
        agg.process_event(&state_envelope);
        
        // Process a control event for resource metrics
        let mut resource_event = ChainEvent::control(
            ChainEvent::CONTROL_METRICS_RESOURCE,
            json!({
                "cpu_usage_ratio": 0.65,
                "memory_bytes": 104857600  // 100MB
            })
        );
        resource_event.flow_context = FlowContext {
            flow_name: "order_processing".to_string(),
            flow_id: "demo_flow".to_string(),
            stage_name: "calculate_total".to_string(),
            stage_type: StageType::Transform,
        };
        let resource_envelope = EventEnvelope::new(WriterId::new(), resource_event);
        agg.process_event(&resource_envelope);
    }
    
    // Create the metrics endpoint
    let endpoint = MetricsEndpoint::new(aggregator);
    
    // Render metrics in Prometheus format
    match endpoint.render_metrics() {
        Ok(output) => {
            println!("=== Prometheus Metrics Output ===\n");
            println!("{}", output);
            println!("\n=== End of Metrics ===");
        }
        Err(e) => {
            eprintln!("Error rendering metrics: {}", e);
        }
    }
    
    // In a real application, you would expose this via an HTTP endpoint
    // For example, using warp or axum:
    // 
    // let metrics_route = warp::path("metrics")
    //     .and(warp::get())
    //     .and(with_endpoint(endpoint))
    //     .and_then(handle_metrics);
}

fn create_event(flow: &str, stage: &str, processing_time_ms: u64, outcome: ProcessingOutcome) -> EventEnvelope {
    let mut event = ChainEvent::new(
        EventId::new(),
        WriterId::new(),
        "data.order",
        json!({"order_id": "12345"}),
    );
    
    event.flow_context = FlowContext {
        flow_name: flow.to_string(),
        flow_id: "demo_flow".to_string(),
        stage_name: stage.to_string(),
        stage_type: StageType::Transform,
    };
    
    event.processing_info = ProcessingInfo {
        processing_time_ms,
        outcome,
        ..Default::default()
    };
    
    EventEnvelope::new(WriterId::new(), event)
}