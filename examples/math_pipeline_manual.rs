use flowstate_rs::prelude::*;
use flowstate_rs::monitoring::{RED, USE, SAAFE, Taxonomy};
use flowstate_rs::step::{Step, StepType, ChainEvent};
use flowstate_rs::event_sourcing::EventSourcedStage;
use flowstate_rs::topology::{PipelineBuilder, PipelineLifecycle};
use flowstate_rs::stages::Monitor;
use serde_json::json;
use std::sync::Arc;
use std::path::PathBuf;

// Source: Emits 1
struct NumberSource {
    metrics: <RED as Taxonomy>::Metrics,
    count: std::sync::atomic::AtomicU32,
}

impl NumberSource {
    fn new() -> Self {
        Self {
            metrics: RED::create_metrics("number_source"),
            count: std::sync::atomic::AtomicU32::new(0),
        }
    }
}

impl Step for NumberSource {
    type Taxonomy = RED;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &RED
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Source
    }
    
    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let count = self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if count < 5 {  // Only emit 5 numbers
            println!("Source emitting: 1");
            vec![ChainEvent::new("number", json!({"value": 1}))]
        } else {
            vec![]
        }
    }
}

// Stage 1: Doubles the value
struct Doubler {
    metrics: <USE as Taxonomy>::Metrics,
}

impl Doubler {
    fn new() -> Self {
        Self {
            metrics: USE::create_metrics("doubler"),
        }
    }
}

impl Step for Doubler {
    type Taxonomy = USE;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &USE
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Stage
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "number" {
            if let Some(value) = event.payload.get("value").and_then(|v| v.as_i64()) {
                let doubled = value * 2;
                println!("Doubler: {} -> {}", value, doubled);
                return vec![ChainEvent::new("number", json!({"value": doubled}))];
            }
        }
        vec![]
    }
}

// Stage 2: Adds 1
struct Incrementer {
    metrics: <USE as Taxonomy>::Metrics,
}

impl Incrementer {
    fn new() -> Self {
        Self {
            metrics: USE::create_metrics("incrementer"),
        }
    }
}

impl Step for Incrementer {
    type Taxonomy = USE;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &USE
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Stage
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "number" {
            if let Some(value) = event.payload.get("value").and_then(|v| v.as_i64()) {
                let incremented = value + 1;
                println!("Incrementer: {} -> {}", value, incremented);
                return vec![ChainEvent::new("number", json!({"value": incremented}))];
            }
        }
        vec![]
    }
}

// Stage 3: Doubles again
struct FinalDoubler {
    metrics: <SAAFE as Taxonomy>::Metrics,
}

impl FinalDoubler {
    fn new() -> Self {
        Self {
            metrics: SAAFE::create_metrics("final_doubler"),
        }
    }
}

impl Step for FinalDoubler {
    type Taxonomy = SAAFE;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &SAAFE
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Stage
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "number" {
            if let Some(value) = event.payload.get("value").and_then(|v| v.as_i64()) {
                let doubled = value * 2;
                println!("FinalDoubler: {} -> {}", value, doubled);
                return vec![ChainEvent::new("number", json!({"value": doubled}))];
            }
        }
        vec![]
    }
}

// Sink: Prints final result
struct ResultSink {
    metrics: <RED as Taxonomy>::Metrics,
}

impl ResultSink {
    fn new() -> Self {
        Self {
            metrics: RED::create_metrics("result_sink"),
        }
    }
}

impl Step for ResultSink {
    type Taxonomy = RED;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &RED
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Sink
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "number" {
            if let Some(value) = event.payload.get("value").and_then(|v| v.as_i64()) {
                println!("SINK RESULT: {}", value);
                println!("(Expected: 1 -> 2 -> 3 -> 6)");
            }
        }
        vec![]
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("flowstate_rs=debug,math_pipeline_manual=debug")
        .init();

    println!("Building math pipeline manually...\n");

    // Create event store
    let store = EventStore::new(EventStoreConfig {
        path: PathBuf::from("./math_pipeline_store"),
        max_segment_size: 1024 * 1024,
    }).await?;

    // Phase 1: Build topology
    println!("Phase 1: Building topology...");
    let mut builder = PipelineBuilder::new();
    
    let source_id = builder.add_stage(Some("source".to_string()));
    let doubler_id = builder.add_stage(Some("doubler".to_string()));
    let incrementer_id = builder.add_stage(Some("incrementer".to_string()));
    let final_doubler_id = builder.add_stage(Some("final_doubler".to_string()));
    let sink_id = builder.add_stage(Some("sink".to_string()));
    
    let topology = Arc::new(builder.build()?);
    println!("Topology built with {} stages", topology.num_stages());

    // Create lifecycle coordinator
    let lifecycle = Arc::new(PipelineLifecycle::new(topology.clone()));

    // Phase 2: Create stages
    println!("\nPhase 2: Creating stages...");
    let mut handles = Vec::new();

    // Source
    {
        let monitored = Monitor::step(NumberSource::new(), RED);
        let mut source = EventSourcedStage::builder()
            .with_step(monitored)
            .with_topology(source_id, "source".to_string(), topology.clone())
            .with_store(store.clone())
            .with_pipeline_lifecycle(lifecycle.clone())
            .build()
            .await?;
        
        println!("Spawning source stage");
        handles.push(tokio::spawn(async move {
            source.run().await
        }));
    }

    // Doubler
    {
        let monitored = Monitor::step(Doubler::new(), USE);
        let mut doubler = EventSourcedStage::builder()
            .with_step(monitored)
            .with_topology(doubler_id, "doubler".to_string(), topology.clone())
            .with_store(store.clone())
            .with_pipeline_lifecycle(lifecycle.clone())
            .build()
            .await?;
        
        println!("Spawning doubler stage");
        handles.push(tokio::spawn(async move {
            doubler.run().await
        }));
    }

    // Incrementer
    {
        let monitored = Monitor::step(Incrementer::new(), USE);
        let mut incrementer = EventSourcedStage::builder()
            .with_step(monitored)
            .with_topology(incrementer_id, "incrementer".to_string(), topology.clone())
            .with_store(store.clone())
            .with_pipeline_lifecycle(lifecycle.clone())
            .build()
            .await?;
        
        println!("Spawning incrementer stage");
        handles.push(tokio::spawn(async move {
            incrementer.run().await
        }));
    }

    // Final Doubler
    {
        let monitored = Monitor::step(FinalDoubler::new(), SAAFE);
        let mut final_doubler = EventSourcedStage::builder()
            .with_step(monitored)
            .with_topology(final_doubler_id, "final_doubler".to_string(), topology.clone())
            .with_store(store.clone())
            .with_pipeline_lifecycle(lifecycle.clone())
            .build()
            .await?;
        
        println!("Spawning final_doubler stage");
        handles.push(tokio::spawn(async move {
            final_doubler.run().await
        }));
    }

    // Sink
    {
        let monitored = Monitor::step(ResultSink::new(), RED);
        let mut sink = EventSourcedStage::builder()
            .with_step(monitored)
            .with_topology(sink_id, "sink".to_string(), topology.clone())
            .with_store(store.clone())
            .with_pipeline_lifecycle(lifecycle.clone())
            .build()
            .await?;
        
        println!("Spawning sink stage");
        handles.push(tokio::spawn(async move {
            sink.run().await
        }));
    }

    // Create flow handle
    let flow_handle = flowstate_rs::event_sourcing::FlowHandle::new(handles, lifecycle);

    println!("\nPipeline running...");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    println!("\nShutting down...");
    flow_handle.shutdown().await?;

    println!("Done!");

    // Cleanup
    let _ = std::fs::remove_dir_all("./math_pipeline_store");

    Ok(())
}