// src/dsl.rs
//! DSL for building event-sourced flows
//! 
//! With FLOWIP-006, flows use EventStore as the ONLY communication method.
//! Each flow has its own EventStore and can be monitored as a resource.
//!
//! With FLOWIP-019, the DSL internally uses PipelineBuilder for strong stage
//! identification, but maintains the same user-friendly API.

/// Helper macro to create a stage with middleware
#[macro_export]
macro_rules! stage {
    // Stage with monitoring middleware
    ($stage:expr, $taxonomy:ty) => {{
        {
            use $crate::middleware::{StepExt, MonitoringMiddleware};
            use $crate::monitoring::Taxonomy;
            let stage_name = stringify!($stage)
                .split("::").last()
                .unwrap_or("stage")
                .split("(").next()
                .unwrap_or("stage");
            $stage.middleware()
                .with(MonitoringMiddleware::<$taxonomy>::new(stage_name))
                .build()
        }
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

/// Helper to apply middleware array to a step
/// Stage name is passed in for monitoring middleware
#[macro_export]
macro_rules! apply_middleware {
    ($handler:expr, $stage_name:expr, []) => {
        $handler
    };
    ($handler:expr, $stage_name:expr, [$mw:expr]) => {{
        use $crate::middleware::EventHandlerExt;
        let middleware = $mw;
        $handler.middleware().with(middleware).build()
    }};
    ($handler:expr, $stage_name:expr, [$mw:expr, $($rest:expr),+]) => {{
        use $crate::middleware::{EventHandlerExt, MonitoringMiddleware};
        let middleware = $mw;
        // Check if this is a MonitoringMiddleware and inject stage name
        // For now, we just apply the middleware as-is
        // TODO: Implement stage name injection once we have set_stage_name method
        let handler_with_mw = $handler.middleware().with(middleware).build();
        $crate::apply_middleware!(handler_with_mw, $stage_name, [$($rest),+])
    }};
}

/// Internal helper macro for common flow logic using PipelineBuilder
#[macro_export]
macro_rules! flow_internal {
    // Pattern with flow middleware array (FLOWIP-055)
    {
        store: $store:expr,
        flow_middleware: [$($flow_mw:expr),*],
        stages: [
            ($src_name:expr, $src:expr, [$($src_mw:expr),*])
            $(, ($st_name:expr, $st:expr, [$($st_mw:expr),*]))*
        ]
    } => {{
        async {
            use $crate::prelude::*;
            use $crate::event_sourcing::{EventSourcedStage, FlowHandle};
            use $crate::monitoring::{Taxonomy, TaxonomyMetrics};
            use $crate::topology::{PipelineBuilder, StageId, PipelineTopology, LayeredPipelineLifecycle, StageLayerAdapter, ObserverLayerAdapter};
            use tokio::task::JoinHandle;
            use std::sync::Arc;
            use std::collections::HashMap;
            type FlowResult = $crate::step::Result<()>;
            
            // The event store is already an Arc
            let store = $store;
            let flow_name = store.flow_name();
            let flow_id = store.flow_id();
            
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
            
            // Create layered pipeline lifecycle coordinator
            let mut pipeline_lifecycle = LayeredPipelineLifecycle::new(topology.clone());
            
            // Phase 2: Create event-sourced stages with monitoring
            let mut handles: Vec<JoinHandle<FlowResult>> = vec![];
            
            // Phase 2a: Prepare flow-level middleware (if any) for Layer B
            let flow_middleware_vec = vec![$($flow_mw),*];
            if !flow_middleware_vec.is_empty() {
                use $crate::middleware::{FlowObserver, apply_middleware_vec};
                use $crate::lifecycle::GenericEventProcessor;
                use $crate::event_store::SubscriptionFilter;
                use $crate::topology::{ComponentType, ObserverLayerAdapter};
                
                // Three-layer architecture as per FLOWIP-026:
                
                // 1. Create the FlowObserver (business logic)
                let flow_observer = FlowObserver::new(flow_name.clone());
                
                // 2. Apply middleware if any
                let handler = apply_middleware_vec(flow_observer, flow_middleware_vec);
                
                // 3. Create processor (with Arc internally, so it's cloneable)
                let processor = GenericEventProcessor::new(
                    format!("flow_monitor_{}", flow_name),
                    handler,
                    Arc::clone(&store),
                    SubscriptionFilter::all(), // Subscribe to all flow events
                    ComponentType::FlowObserver,
                );
                
                // 4. Create observer adapter and register with Layer B
                let observer_adapter = ObserverLayerAdapter::new(
                    format!("flow_monitor_{}", flow_name),
                    processor,
                );
                pipeline_lifecycle.register_component(observer_adapter).await?;
            }
            
            // Create source stage
            {
                let store_clone = Arc::clone(&store);
                let topology_clone = Arc::clone(&topology);
                let stage_id = stage_ids[0];
                
                // Register stage adapter with Layer A
                let stage_adapter = StageLayerAdapter::new(stage_id, $src_name.clone());
                let adapter_drained_flag = stage_adapter.is_drained.clone();
                pipeline_lifecycle.register_component(stage_adapter).await?;
                
                // Apply user's middleware array to the EventHandler
                let handler_with_middleware = $crate::apply_middleware!($src, &$src_name, [$($src_mw),*]);
                
                // Get stage lifecycle handle from layered lifecycle
                let stage_lifecycle = pipeline_lifecycle.stage_lifecycle(stage_id).await?;
                
                // Build event-sourced stage using builder pattern
                let mut event_stage = EventSourcedStage::builder()
                    .with_handler(handler_with_middleware)
                    .with_topology(stage_id, $src_name.clone(), topology_clone)
                    .with_store(store_clone)
                    .with_stage_lifecycle_handle(stage_lifecycle)
                    .with_adapter_drained_flag(adapter_drained_flag)
                    .is_source(true)  // This is a source stage
                    .build()
                    .await?;
                
                // Spawn stage task
                tracing::debug!("Spawning source stage with ID {:?}", stage_id);
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
                    
                    // Get the stage ID that was assigned during topology building
                    let stage_id = stage_iter.next()
                        .ok_or("Stage ID iterator exhausted")?;
                    
                    // Register stage adapter with Layer A
                    let stage_adapter = StageLayerAdapter::new(stage_id, $st_name.clone());
                    let adapter_drained_flag = stage_adapter.is_drained.clone();
                    pipeline_lifecycle.register_component(stage_adapter).await?;
                    
                    // Apply user's middleware array to the EventHandler
                    let handler_with_middleware = $crate::apply_middleware!($st, &$st_name, [$($st_mw),*]);
                    
                    // Get stage lifecycle handle from layered lifecycle
                    let stage_lifecycle = pipeline_lifecycle.stage_lifecycle(stage_id).await?;
                    
                    // Build event-sourced stage using builder pattern
                    let mut event_stage = EventSourcedStage::builder()
                        .with_handler(handler_with_middleware)
                        .with_topology(stage_id, $st_name.clone(), topology_clone)
                        .with_store(store_clone)
                        .with_stage_lifecycle_handle(stage_lifecycle)
                        .with_adapter_drained_flag(adapter_drained_flag)
                        .build()
                        .await?;
                    
                    // Spawn stage task
                    let handle = tokio::spawn(async move {
                        event_stage.run().await
                    });
                    
                    handles.push(handle);
                }
            )*
            
            // Phase 3: Initialize all layers in order (stages first, then observers)
            let lifecycle_arc = Arc::new(pipeline_lifecycle);
            lifecycle_arc.initialize().await
                .map_err(|e| format!("Failed to initialize layers: {}", e))?;
            
            // Return a FlowHandle for graceful shutdown
            Ok::<FlowHandle, Box<dyn std::error::Error + Send + Sync>>(
                FlowHandle::new(handles, lifecycle_arc, store)
            )
        }.await
    }};
    
}

#[macro_export]
macro_rules! flow {
    // Flow with name and middleware array (FLOWIP-055)
    {
        name: $flow_name:expr,
        middleware: [$($flow_mw:expr),*],
        ( $src_name:literal => $src:expr, [$($src_mw:expr),*] )
        $( |> ( $st_name:literal => $st:expr, [$($st_mw:expr),*] ) )*
    } => {{
        async {
            let store = $crate::event_store::EventStore::for_flow($flow_name).await?;
            $crate::flow_internal! {
                store: store,
                flow_middleware: [$($flow_mw),*],
                stages: [
                    ($src_name.to_string(), $src, [$($src_mw),*])
                    $(, ($st_name.to_string(), $st, [$($st_mw),*]))*
                ]
            }
        }.await
    }};
    
    // Flow with name (no explicit store) - uses EventStore::for_flow()
    {
        name: $flow_name:expr,
        ( $src_name:literal => $src:expr, [$($src_mw:expr),*] )
        $( |> ( $st_name:literal => $st:expr, [$($st_mw:expr),*] ) )*
    } => {{
        async {
            let store = $crate::event_store::EventStore::for_flow($flow_name).await?;
            $crate::flow_internal! {
                store: store,
                flow_middleware: [],
                stages: [
                    ($src_name.to_string(), $src, [$($src_mw),*])
                    $(, ($st_name.to_string(), $st, [$($st_mw),*]))*
                ]
            }
        }.await
    }};
    
    
    // EventStore-based flow with flow-level monitoring and named stages
    {
        store: $store:expr,
        middleware: [$($flow_mw:expr),*],
        ( $src_name:literal => $src:expr, [$($src_mw:expr),*] )
        $( |> ( $st_name:literal => $st:expr, [$($st_mw:expr),*] ) )*
    } => {{
        $crate::flow_internal! {
            store: $store,
            flow_middleware: [$($flow_mw),*],
            stages: [
                ($src_name.to_string(), $src, [$($src_mw),*])
                $(, ($st_name.to_string(), $st, [$($st_mw),*]))*
            ]
        }
    }};
    
    // Flow with name (auto-generated stage names)
    {
        name: $flow_name:expr,
        ( $src:expr, [$($src_mw:expr),*] )
        $( |> ( $st:expr, [$($st_mw:expr),*] ) )*
    } => {{
        async {
            let store = $crate::event_store::EventStore::for_flow($flow_name).await?;
            let mut _stage_counter = 0;
            
            $crate::flow_internal! {
                store: store,
                flow_middleware: [],
                stages: [
                    ({_stage_counter += 1; format!("stage_{}", _stage_counter)}, $src, [$($src_mw),*])
                    $(, ({_stage_counter += 1; format!("stage_{}", _stage_counter)}, $st, [$($st_mw),*]))*
                ]
            }
        }.await
    }};
    
    // EventStore-based flow with flow-level monitoring (auto-generated names)
    {
        store: $store:expr,
        middleware: [$($flow_mw:expr),*],
        ( $src:expr, [$($src_mw:expr),*] )
        $( |> ( $st:expr, [$($st_mw:expr),*] ) )*
    } => {{
        // Count stages to generate names
        let mut _stage_counter = 0;
        
        // Delegate to flow_internal! with generated names
        $crate::flow_internal! {
            store: $store,
            flow_middleware: [$($flow_mw),*],
            stages: [
                ({_stage_counter += 1; format!("stage_{}", _stage_counter)}, $src, [$($src_mw),*])
                $(, ({_stage_counter += 1; format!("stage_{}", _stage_counter)}, $st, [$($st_mw),*]))*
            ]
        }
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
    use crate::step::{Step, StepType};
    use crate::chain_event::ChainEvent;
    use crate::monitoring::{RED, Taxonomy};
    use serde_json::json;
    
    // Simple test stages - no taxonomy requirements
    struct TestSource;
    
    struct TestTransform;
    
    struct TestSink;
    
    impl Step for TestSource {
        fn step_type(&self) -> StepType {
            StepType::Source
        }
        
        fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
            vec![ChainEvent::new("test_event", json!({"value": 42}))]
        }
    }
    
    impl Step for TestTransform {
        fn step_type(&self) -> StepType {
            StepType::Stage
        }
        
        fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
            vec![event] // Passthrough
        }
    }
    
    impl Step for TestSink {
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
        let _source = TestSource;
        let _transform = TestTransform;
        let _sink = TestSink;
        
        // The flow! macro is tested in integration tests
        // since it requires async runtime
    }
}