// src/dsl.rs
//! DSL for building event-sourced flows
//! 
//! With FLOWIP-006, flows use EventStore as the ONLY communication method.
//! Each flow has its own EventStore and can be monitored as a resource.
//!
//! With FLOWIP-019, the DSL internally uses PipelineBuilder for strong stage
//! identification, but maintains the same user-friendly API.

/// Helper macro to create a monitored step
#[macro_export]
macro_rules! stage {
    // Stage with explicit taxonomy
    ($stage:expr, $taxonomy:expr) => {{
        $crate::stages::Monitor::step($stage, $taxonomy)
    }};
}

/// Main flow! macro for building event-sourced pipelines
/// 
/// # Syntax
/// ```ignore
/// use flowstate_rs::prelude::*;
/// use flowstate_rs::flow;
/// 
/// let store = EventStore::default();
/// let handle = flow! {
///     store: store,
///     flow_taxonomy: GoldenSignals,  // The flow itself is monitored!
///     (DataGenerator::new(), RED)     // Each stage has its taxonomy
///     |> (Transform::new(), USE)
///     |> (DataSink::new(), RED)
/// }?;
/// ```
/// 
/// # With Named Stages
/// ```ignore
/// flow! {
///     store: EventStore::default(),
///     flow_taxonomy: GoldenSignals,
///     ("data_generator" => DataGenerator::new(), RED)
///     |> ("transformer" => Transform::new(), USE)
///     |> ("api_sink" => DataSink::new(), RED)
/// }
/// ```
/// 
/// # Returns
/// Returns a `FlowHandle` that can be used to gracefully shut down the flow:
/// ```ignore
/// let handle = flow! { ... }?;
/// // ... do work ...
/// handle.shutdown().await?;  // Graceful shutdown
/// // or
/// handle.abort("reason");    // Immediate abort
/// ```

/// Internal helper macro for common flow logic using PipelineBuilder
#[macro_export]
macro_rules! flow_internal {
    {
        store: $store:expr,
        flow_taxonomy: $flow_tax:ty,
        stages: [
            ($src_name:expr, $src:expr, $src_tax:expr)
            $(, ($st_name:expr, $st:expr, $st_tax:expr))*
        ]
    } => {{
        async {
            use $crate::prelude::*;
            use $crate::event_sourcing::{EventSourcedStage, FlowHandle};
            use $crate::monitoring::{Taxonomy, TaxonomyMetrics};
            use $crate::topology::{PipelineBuilder, StageId, PipelineTopology, PipelineLifecycle};
            use tokio::task::JoinHandle;
            use std::sync::Arc;
            use std::collections::HashMap;
            type FlowResult = $crate::step::Result<()>;
            
            // The event store is already an Arc
            let store = $store;
            
            // FLOWIP-005 Integration Point: Flow-level monitoring taxonomy
            let _flow_taxonomy = stringify!($flow_tax);  // Placeholder for FLOWIP-005
            
            // Phase 1: Build pipeline topology using PipelineBuilder
            let mut builder = PipelineBuilder::new();
            let mut stage_ids = Vec::new();
            
            let src_id = builder.add_stage(Some($src_name.clone()));
            stage_ids.push(src_id);
            
            $(
                let stage_id = builder.add_stage(Some($st_name.clone()));
                stage_ids.push(stage_id);
            )*
            
            // Build and validate topology
            let topology = Arc::new(builder.build()
                .map_err(|e| format!("Failed to build pipeline topology: {}", e))?);
            
            tracing::debug!("Built topology with {} stages", topology.num_stages());
            
            // Create pipeline lifecycle coordinator
            let pipeline_lifecycle = Arc::new(PipelineLifecycle::new(topology.clone()));
            
            // Phase 2: Create event-sourced stages with monitoring
            let mut handles: Vec<JoinHandle<FlowResult>> = vec![];
            
            // Create source stage
            {
                let store_clone = Arc::clone(&store);
                let topology_clone = Arc::clone(&topology);
                let lifecycle_clone = Arc::clone(&pipeline_lifecycle);
                
                // Create monitored step
                let monitored = $crate::stages::Monitor::step($src, $src_tax);
                
                // Build event-sourced stage using builder pattern
                let mut event_stage = EventSourcedStage::builder()
                    .with_step(monitored)
                    .with_topology(src_id, $src_name.clone(), topology_clone)
                    .with_store(store_clone)
                    .with_pipeline_lifecycle(lifecycle_clone)
                    .build()
                    .await?;
                
                // Spawn stage task
                tracing::debug!("Spawning source stage with ID {:?}", src_id);
                let handle = tokio::spawn(async move {
                    event_stage.run().await
                });
                
                handles.push(handle);
            }
            
            // Create remaining stages using collected stage IDs
            let mut stage_iter = stage_ids.into_iter().skip(1); // Skip source ID
            $(
                {
                    let store_clone = Arc::clone(&store);
                    let topology_clone = Arc::clone(&topology);
                    let lifecycle_clone = Arc::clone(&pipeline_lifecycle);
                    
                    // Get the stage ID that was assigned during topology building
                    let stage_id = stage_iter.next()
                        .ok_or("Stage ID iterator exhausted")?;
                    
                    // Create monitored step
                    let monitored = $crate::stages::Monitor::step($st, $st_tax);
                    
                    // Build event-sourced stage using builder pattern
                    let mut event_stage = EventSourcedStage::builder()
                        .with_step(monitored)
                        .with_topology(stage_id, $st_name.clone(), topology_clone)
                        .with_store(store_clone)
                        .with_pipeline_lifecycle(lifecycle_clone)
                        .build()
                        .await?;
                    
                    // Spawn stage task
                    let handle = tokio::spawn(async move {
                        event_stage.run().await
                    });
                    
                    handles.push(handle);
                }
            )*
            
            // Return a FlowHandle for graceful shutdown
            Ok::<FlowHandle, Box<dyn std::error::Error + Send + Sync>>(
                FlowHandle::new(handles, pipeline_lifecycle)
            )
        }.await
    }};
}

#[macro_export]
macro_rules! flow {
    // EventStore-based flow with flow-level monitoring and named stages
    {
        store: $store:expr,
        flow_taxonomy: $flow_tax:ty,
        ( $src_name:literal => $src:expr, $src_tax:expr )
        $( |> ( $st_name:literal => $st:expr, $st_tax:expr ) )*
    } => {{
        $crate::flow_internal! {
            store: $store,
            flow_taxonomy: $flow_tax,
            stages: [
                ($src_name.to_string(), $src, $src_tax)
                $(, ($st_name.to_string(), $st, $st_tax))*
            ]
        }
    }};
    
    // EventStore-based flow with flow-level monitoring (auto-generated names)
    {
        store: $store:expr,
        flow_taxonomy: $flow_tax:ty,
        ( $src:expr, $src_tax:expr )
        $( |> ( $st:expr, $st_tax:expr ) )*
    } => {{
        async {
            use $crate::prelude::*;
            use $crate::event_sourcing::{EventSourcedStage, FlowHandle};
            use $crate::monitoring::{Taxonomy, TaxonomyMetrics};
            use $crate::topology::{PipelineBuilder, StageId, PipelineTopology, PipelineLifecycle};
            use tokio::task::JoinHandle;
            use std::sync::Arc;
            type FlowResult = $crate::step::Result<()>;
            
            let store = $store;
            let _flow_taxonomy = stringify!($flow_tax);
            
            // Phase 1: Build pipeline topology with auto-generated names
            let mut builder = PipelineBuilder::new();
            let mut stage_ids = Vec::new();
            
            // Add source stage to topology (None = auto-generate name)
            let src_id = builder.add_stage(None);
            stage_ids.push(src_id);
            
            $(
                let stage_id = builder.add_stage(None);
                stage_ids.push(stage_id);
                let _ = &$st; // Reference the stage to satisfy macro hygiene
            )*
            
            // Build and validate topology
            let topology = Arc::new(builder.build()
                .map_err(|e| format!("Failed to build pipeline topology: {}", e))?);
            
            // Create pipeline lifecycle coordinator
            let pipeline_lifecycle = Arc::new(PipelineLifecycle::new(topology.clone()));
            
            // Phase 2: Create event-sourced stages
            let mut handles: Vec<JoinHandle<FlowResult>> = vec![];
            
            // Create source stage
            {
                let store_clone = Arc::clone(&store);
                let topology_clone = Arc::clone(&topology);
                let lifecycle_clone = Arc::clone(&pipeline_lifecycle);
                
                let monitored = $crate::stages::Monitor::step($src, $src_tax);
                
                // Get auto-generated name from topology
                let src_name = topology_clone.stage_name(src_id)
                    .ok_or("Source stage not found in topology")?
                    .to_string();
                
                let mut event_stage = EventSourcedStage::builder()
                    .with_step(monitored)
                    .with_topology(src_id, src_name, topology_clone)
                    .with_store(store_clone)
                    .with_pipeline_lifecycle(lifecycle_clone)
                    .build()
                    .await?;
                
                let handle = tokio::spawn(async move {
                    event_stage.run().await
                });
                
                handles.push(handle);
            }
            
            // Create remaining stages using collected stage IDs
            let mut stage_id_iter = stage_ids.into_iter().skip(1); // Skip source ID
            $(
                {
                    let store_clone = Arc::clone(&store);
                    let topology_clone = Arc::clone(&topology);
                    let lifecycle_clone = Arc::clone(&pipeline_lifecycle);
                    
                    // Get the stage ID that was assigned during topology building
                    let stage_id = stage_id_iter.next()
                        .ok_or("Stage ID iterator exhausted")?;
                    
                    // Get stage name from topology (for debugging only)
                    let stage_name = topology.stage_name(stage_id)
                        .ok_or("Stage not found in topology")?
                        .to_string();
                    
                    let monitored = $crate::stages::Monitor::step($st, $st_tax);
                    
                    let mut event_stage = EventSourcedStage::builder()
                        .with_step(monitored)
                        .with_topology(stage_id, stage_name, topology_clone)
                        .with_store(store_clone)
                        .with_pipeline_lifecycle(lifecycle_clone)
                        .build()
                        .await?;
                    
                    let handle = tokio::spawn(async move {
                        event_stage.run().await
                    });
                    
                    handles.push(handle);
                }
            )*
            
            Ok::<FlowHandle, Box<dyn std::error::Error + Send + Sync>>(
                FlowHandle::new(handles, pipeline_lifecycle)
            )
        }.await
    }};
}

/// Macro to count the number of stages in a flow (used internally)
#[macro_export]
macro_rules! count_stages {
    () => { 0 };
    ( $head:tt $($tail:tt)* ) => { 1 + count_stages!($($tail)*) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::step::{Step, StepType, ChainEvent};
    use crate::monitoring::{RED, Taxonomy};
    use serde_json::json;
    
    // Simple test stages using RED taxonomy
    struct TestSource {
        metrics: <RED as Taxonomy>::Metrics,
    }
    
    struct TestTransform {
        metrics: <RED as Taxonomy>::Metrics,
    }
    
    struct TestSink {
        metrics: <RED as Taxonomy>::Metrics,
    }
    
    impl Step for TestSource {
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
            vec![ChainEvent::new("test_event", json!({"value": 42}))]
        }
    }
    
    impl Step for TestTransform {
        type Taxonomy = RED;
        
        fn taxonomy(&self) -> &Self::Taxonomy {
            &RED
        }
        
        fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
            &self.metrics
        }
        
        fn step_type(&self) -> StepType {
            StepType::Stage
        }
        
        fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
            vec![event] // Passthrough
        }
    }
    
    impl Step for TestSink {
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
        
        fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
            vec![] // Sink consumes events
        }
    }
    
    #[test]
    fn test_flow_macro_compiles() {
        // Test that we can create stages
        let _source = TestSource { metrics: RED::create_metrics("test_source") };
        let _transform = TestTransform { metrics: RED::create_metrics("test_transform") };
        let _sink = TestSink { metrics: RED::create_metrics("test_sink") };
        
        // The flow! macro is tested in integration tests
        // since it requires async runtime
    }
}