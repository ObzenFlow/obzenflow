//! Simple test to verify event enrichment works

use flowstate_rs::chain_event::ChainEvent;
use flowstate_rs::event_sourcing::EventSourcedStage;
use flowstate_rs::event_store::EventStore;
use flowstate_rs::event_types::BoundaryType;
use flowstate_rs::topology::{PipelineBuilder, PipelineLifecycle};
use flowstate_rs::monitoring::{RED, Taxonomy};
use flowstate_rs::step::{Step, StepType};
use serde_json::json;
use std::sync::Arc;

// Simple step that returns one event on _tick
struct SimpleSource;

impl Step for SimpleSource {
    type Taxonomy = RED;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &RED
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        static METRICS: std::sync::OnceLock<<RED as Taxonomy>::Metrics> = std::sync::OnceLock::new();
        METRICS.get_or_init(|| RED::create_metrics("simple_source"))
    }
    
    fn step_type(&self) -> StepType {
        StepType::Source
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "_tick" {
            vec![ChainEvent::new("TestEvent", json!({"data": "test"}))]
        } else {
            vec![]
        }
    }
}

#[tokio::test]
async fn test_event_enrichment_basic() {
    // Create event store
    let store = EventStore::for_testing().await;
    
    // Build simple topology with source -> sink
    let mut builder = PipelineBuilder::new();
    let source_id = builder.add_stage(Some("test_source".to_string()));
    let sink_id = builder.add_stage(Some("test_sink".to_string()));
    
    let topology = Arc::new(builder.build().unwrap());
    let pipeline_lifecycle = Arc::new(PipelineLifecycle::new(topology.clone()));
    
    // Create source stage
    let mut source_stage = EventSourcedStage::builder()
        .with_step(SimpleSource)
        .with_topology(source_id, "test_source".to_string(), topology.clone())
        .with_store(store.clone())
        .with_pipeline_lifecycle(pipeline_lifecycle.clone())
        .build()
        .await
        .unwrap();
    
    // Run once to generate events
    let count = source_stage.run_once().await.unwrap();
    assert_eq!(count, 1, "Source should emit 1 event");
    
    // Read the event directly from the store
    let reader = store.reader();
    let events = reader.read_stage_events(source_id).await.unwrap();
    assert_eq!(events.len(), 1, "Should have written 1 event");
    
    let envelope = &events[0];
    let event = &envelope.event;
    
    // Verify event was enriched
    assert_eq!(event.event_type, "TestEvent");
    
    // Flow context should be populated from topology
    assert!(event.flow_context.flow_id.starts_with("test_source_flow"));
    assert_eq!(event.flow_context.flow_name, "test_source_flow");
    assert_eq!(event.flow_context.stage_name, "test_source");
    
    // Processing info should be populated
    assert_eq!(event.processing_info.processed_by, "test_source");
    assert!(event.processing_info.correlation_id.is_some(), "Source should generate correlation ID");
    assert_eq!(
        event.processing_info.is_boundary_event, 
        Some(BoundaryType::FlowEntry),
        "Source should be marked as flow entry"
    );
    
    // Causality should be empty for source events
    assert!(event.causality.parent_ids.is_empty(), "Source event should have no parents");
}