//! Tests for flow-level correlation and metrics

use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::event_id::EventId;
use obzenflow_core::event::flow_context::{FlowContext, StageType};
use obzenflow_core::journal::writer_id::WriterId;
use obzenflow_adapters::middleware::flow_boundary::FlowMetrics;
use obzenflow_adapters::middleware::flow_metrics_registry::FlowMetricsRegistry;
use std::sync::Arc;
use std::time::Duration;
use serde_json::json;

#[tokio::test]
async fn test_event_correlation_flow() {
    // Create flow metrics
    let flow_metrics = Arc::new(FlowMetrics::new("test_flow"));
    
    // Simulate source event with new correlation
    let source_event = create_event_with_correlation(
        "http_source",
        StageType::Source,
        json!({"request": "GET /api/data"}),
    );
    
    // Track at flow entry
    flow_metrics.on_event_start(&source_event);
    
    // Give async task time to complete
    tokio::time::sleep(Duration::from_millis(10)).await;
    
    // Verify entry was tracked
    assert_eq!(flow_metrics.active_correlation_count().await, 1);
    
    // Simulate transform propagating correlation
    let transform_event = create_derived_event(
        &source_event,
        "json_parser", 
        StageType::Transform,
        json!({"parsed": true}),
    );
    
    // Simulate some processing time
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    // Simulate sink with same correlation
    let sink_event = create_derived_event(
        &transform_event,
        "database_sink",
        StageType::Sink,
        json!({"stored": true}),
    );
    
    // Track at flow exit
    flow_metrics.on_event_complete(&sink_event, &[], Duration::default());
    
    // Give async cleanup time to complete
    tokio::time::sleep(Duration::from_millis(10)).await;
    
    // Verify correlation was cleaned up
    assert_eq!(flow_metrics.active_correlation_count().await, 0);
    
    // Export metrics and verify
    let snapshots = flow_metrics.export_metrics();
    
    // Should have entry count
    let entry_metric = snapshots.iter()
        .find(|s| s.name.contains("entries"))
        .expect("Should have entry metric");
    
    if let obzenflow_adapters::monitoring::metrics::core::MetricValue::Counter(count) = &entry_metric.value {
        assert_eq!(*count, 1);
    } else {
        panic!("Expected counter metric");
    }
    
    // Should have exit count
    let exit_metric = snapshots.iter()
        .find(|s| s.name.contains("exits"))
        .expect("Should have exit metric");
    
    if let obzenflow_adapters::monitoring::metrics::core::MetricValue::Counter(count) = &exit_metric.value {
        assert_eq!(*count, 1);
    } else {
        panic!("Expected counter metric");
    }
    
    // Should have latency recorded
    let latency_metric = snapshots.iter()
        .find(|s| s.name.contains("latency"))
        .expect("Should have latency metric");
    
    if let obzenflow_adapters::monitoring::metrics::core::MetricValue::Histogram { count, .. } = &latency_metric.value {
        assert_eq!(*count, 1);
    } else {
        panic!("Expected histogram metric");
    }
}

#[tokio::test]
async fn test_flow_metrics_registry() {
    let registry = FlowMetricsRegistry::new();
    
    // Create metrics for multiple flows
    let flow1_metrics = Arc::new(FlowMetrics::new("order_processing"));
    let flow2_metrics = Arc::new(FlowMetrics::new("user_registration"));
    
    // Register flows
    registry.register_flow("flow1", flow1_metrics.clone()).await;
    registry.register_flow("flow2", flow2_metrics.clone()).await;
    
    assert_eq!(registry.flow_count().await, 2);
    
    // Simulate events in flow1
    let event1 = create_event_with_correlation("api_source", StageType::Source, json!({}));
    flow1_metrics.on_event_start(&event1);
    
    // Export all metrics
    let all_metrics = registry.export_all_metrics().await;
    
    // Should have metrics from both flows
    let flow1_count = all_metrics.iter()
        .filter(|m| m.labels.get("flow_name") == Some(&"order_processing".to_string()))
        .count();
    let flow2_count = all_metrics.iter()
        .filter(|m| m.labels.get("flow_name") == Some(&"user_registration".to_string()))
        .count();
    
    assert!(flow1_count > 0);
    assert!(flow2_count > 0);
    
    // All metrics should have flow level label
    for metric in &all_metrics {
        assert_eq!(metric.labels.get("level"), Some(&"flow".to_string()));
    }
}

#[tokio::test]
async fn test_correlation_timeout_handling() {
    let flow_metrics = FlowMetrics::new("timeout_test");
    
    // Create event that enters but never exits
    let stuck_event = create_event_with_correlation(
        "source",
        StageType::Source,
        json!({"will_timeout": true}),
    );
    
    flow_metrics.on_event_start(&stuck_event);
    
    // Give async task time to complete
    tokio::time::sleep(Duration::from_millis(10)).await;
    
    assert_eq!(flow_metrics.active_correlation_count().await, 1);
    
    // Cleanup should detect timeout
    let timed_out = flow_metrics.cleanup_timeouts().await;
    // Won't timeout immediately (5 min default)
    assert_eq!(timed_out, 0);
    
    // But correlation is still tracked
    assert_eq!(flow_metrics.active_correlation_count().await, 1);
}

// Helper functions

fn create_event_with_correlation(
    stage_name: &str,
    stage_type: StageType,
    payload: serde_json::Value,
) -> ChainEvent {
    let mut event = ChainEvent::new(
        EventId::new(),
        WriterId::new(),
        "test_event",
        payload,
    );
    
    // Set flow context
    event.flow_context = FlowContext {
        flow_name: "test_flow".to_string(),
        flow_id: "flow123".to_string(),
        stage_name: stage_name.to_string(),
        stage_type,
    };
    
    // Add correlation for source events
    if stage_type == StageType::Source {
        event = event.with_new_correlation(stage_name);
    }
    
    event
}

fn create_derived_event(
    parent: &ChainEvent,
    stage_name: &str,
    stage_type: StageType,
    payload: serde_json::Value,
) -> ChainEvent {
    let mut event = ChainEvent::new(
        EventId::new(),
        WriterId::new(),
        "derived_event",
        payload,
    );
    
    // Set flow context
    event.flow_context = FlowContext {
        flow_name: parent.flow_context.flow_name.clone(),
        flow_id: parent.flow_context.flow_id.clone(),
        stage_name: stage_name.to_string(),
        stage_type,
    };
    
    // Propagate correlation
    event = event.with_correlation_from(parent);
    
    // Set causality
    event.causality = event.causality.add_parent(parent.id);
    
    event
}