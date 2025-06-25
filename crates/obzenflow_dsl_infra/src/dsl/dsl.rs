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
            use $crate::prelude::{EventHandlerExt, MonitoringMiddleware, Taxonomy};
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
/// let journal = DiskJournal::new(...).await?;
/// let handle = flow! {
///     journal: journal,
///     middleware: [monitoring_middleware],  // Flow-level middleware
///     ("data_generator" => DataGenerator::new(), [])
///     |> ("transformer" => Transform::new(), [])
///     |> ("api_sink" => DataSink::new(), [])
/// }?;
/// ```
///
/// # With Stage Middleware
/// ```ignore
/// flow! {
///     journal: my_journal,
///     middleware: [],
///     ("data_generator" => DataGenerator::new(), [retry_middleware])
///     |> ("transformer" => Transform::new(), [logging_middleware])
///     |> ("api_sink" => DataSink::new(), [monitoring_middleware])
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
        use $crate::prelude::EventHandlerExt;
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
        journal: $journal:expr,
        flow_middleware: [$($flow_mw:expr),*],
        stages: [
            ($src_name:expr, $src:expr, [$($src_mw:expr),*])
            $(, ($st_name:expr, $st:expr, [$($st_mw:expr),*]))*
        ]
    } => {{
        async {
            use $crate::prelude::*;
            use tokio::task::JoinHandle;
            use std::sync::Arc;
            use std::collections::HashMap;

            // The journal is already an Arc<dyn Journal>
            let journal = $journal;

            // Phase 1: Build pipeline topology using TopologyBuilder
            let mut builder = TopologyBuilder::new();
            let mut stage_ids = Vec::new();

            let src_id = builder.add_stage(Some($src_name.clone()));
            stage_ids.push(src_id);

            $(
                let stage_id = builder.add_stage(Some($st_name.clone()));
                stage_ids.push(stage_id);
            )*

            // Build and validate topology
            let topology = Arc::new(builder.build()
                .map_err(|e| format!("Failed to build pipeline topology: {:?}", e))?);

            tracing::debug!("Built topology with {} stages", topology.num_stages());

            // Create Pipeline with journal and topology
            let mut pipeline = Pipeline::new(journal.clone(), topology.clone());

            // Phase 2a: Prepare flow-level middleware (if any) as observer
            let flow_middleware_vec = vec![$($flow_mw),*];
            if !flow_middleware_vec.is_empty() {
                use $crate::prelude::{FlowObserver, apply_middleware_vec};
                use obzenflow_runtime_services::control_plane::pipeline_supervisor::pipeline::pipeline::Pipeline;

                // Build observer processor closure
                let observer_name = "flow_monitor".to_string();

                // 1. Create the FlowObserver (business logic)
                let flow_observer = FlowObserver::new("flow".to_string());

                // 2. Apply middleware if any
                let handler = apply_middleware_vec(flow_observer, flow_middleware_vec);

                // Just store the configuration
                pipeline.add_observer(observer_name, Box::new(handler));
            }

            // Create source stage builder
            {
                let stage_id = stage_ids[0];
                let stage_name = $src_name.clone();

                // Apply user's middleware array to the EventHandler
                let handler_with_middleware = $crate::apply_middleware!($src, &$src_name, [$($src_mw),*]);

                // Just store the configuration
                pipeline.add_stage(stage_id, stage_name, Box::new(handler_with_middleware), true);
            }

            // Create remaining transform/sink stages
            let mut stage_iter = stage_ids.into_iter().skip(1); // Skip source ID
            $(
                {
                    let stage_id = stage_iter.next()
                        .ok_or("Stage ID iterator exhausted")?;
                    let stage_name = $st_name.clone();

                    // Apply user's middleware array to the EventHandler
                    let handler_with_middleware = $crate::apply_middleware!($st, &$st_name, [$($st_mw),*]);

                    // Just store the configuration
                    pipeline.add_stage(stage_id, stage_name, Box::new(handler_with_middleware), false);
                }
            )*

            // Create supervisor and materialize
            use $crate::prelude::{PipelineSupervisor, FlowHandle};
            use tokio::sync::RwLock;
            
            tracing::info!("flow! macro: creating pipeline supervisor");
            let mut supervisor = PipelineSupervisor::new(pipeline)
                .map_err(|e| format!("Failed to create pipeline supervisor: {:?}", e))?;
            
            tracing::info!("flow! macro: materializing pipeline");
            supervisor.materialize().await?;
            
            tracing::info!("flow! macro: creating flow handle");
            let handle = FlowHandle::new(Arc::new(RwLock::new(supervisor)));
            
            Ok::<FlowHandle, Box<dyn std::error::Error + Send + Sync>>(handle)
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
            // User must provide a journal when using named flows
            compile_error!("Named flows require a journal parameter. Use: flow! { journal: my_journal, ... }");
        }.await
    }};

    // Flow with name (no explicit journal) - deprecated
    {
        name: $flow_name:expr,
        ( $src_name:literal => $src:expr, [$($src_mw:expr),*] )
        $( |> ( $st_name:literal => $st:expr, [$($st_mw:expr),*] ) )*
    } => {{
        async {
            compile_error!("Named flows require a journal parameter. Use: flow! { journal: my_journal, ... }");
        }.await
    }};


    // Journal-based flow with flow-level monitoring and named stages
    {
        journal: $journal:expr,
        middleware: [$($flow_mw:expr),*],
        ( $src_name:literal => $src:expr, [$($src_mw:expr),*] )
        $( |> ( $st_name:literal => $st:expr, [$($st_mw:expr),*] ) )*
    } => {{
        $crate::flow_internal! {
            journal: $journal,
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
            compile_error!("Named flows require a journal parameter. Use: flow! { journal: my_journal, ... }");
        }.await
    }};

    // Journal-based flow with flow-level monitoring (auto-generated names)
    {
        journal: $journal:expr,
        middleware: [$($flow_mw:expr),*],
        ( $src:expr, [$($src_mw:expr),*] )
        $( |> ( $st:expr, [$($st_mw:expr),*] ) )*
    } => {{
        // Count stages to generate names
        let mut _stage_counter = 0;

        // Delegate to flow_internal! with generated names
        $crate::flow_internal! {
            journal: $journal,
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
