//! Main flow! macro - the primary API for building ObzenFlow pipelines
//!
//! This is now a clean implementation using the let bindings approach that
//! separates stage declaration from topology definition.

/// Parse topology edges supporting both |> and <| operators
#[macro_export]
macro_rules! parse_topology {
    // Base case - no more edges
    ($connections:expr,) => {};
    
    // Forward edge: from |> to;
    ($connections:expr, $from:ident |> $to:ident; $($rest:tt)*) => {
        $connections.push((
            stringify!($from).to_string(),
            stringify!($to).to_string()
        ));
        $crate::parse_topology!($connections, $($rest)*);
    };
    
    // Backward edge: from <| to; (reversed)
    ($connections:expr, $from:ident <| $to:ident; $($rest:tt)*) => {
        $connections.push((
            stringify!($to).to_string(),    // Note: reversed!
            stringify!($from).to_string()
        ));
        $crate::parse_topology!($connections, $($rest)*);
    };
}

/// The main flow! macro using the clean typed approach
#[macro_export]
macro_rules! flow {
    // Pattern with explicit flow name
    {
        name: $flow_name:literal,
        journals: $journals:expr,
        middleware: [$($flow_mw:expr),*],
        
        stages: {
            $($stage_name:ident = $stage_macro:ident!($name:literal => $handler:expr $(, [$($mw:expr),*])?);)*
        },
        
        topology: {
            $(
                $edge:tt
            )*
        }
    } => {{
        async move {
            use $crate::prelude::*;
            use $crate::dsl::stage_descriptor::*;
            use std::sync::Arc;
            use std::collections::HashMap;
            
            let journals = $journals;
            
            // Create stages
            let mut stages: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
            
            $(
                let descriptor = $stage_macro!($name => $handler $(, [$($mw),*])?);
                stages.insert(stringify!($stage_name).to_string(), descriptor);
            )*
            
            // Create connections
            let mut connections: Vec<(String, String)> = Vec::new();
            
            // Parse topology edges
            $crate::parse_topology!(connections, $($edge)*);
            
            
            // Create closure for flow middleware
            let create_flow_middleware = || vec![
                $(Box::new($flow_mw) as Box<dyn obzenflow_adapters::middleware::MiddlewareFactory>),*
            ];
            
            // Build the flow
            $crate::build_typed_flow!($flow_name, $journals, stages, connections, create_flow_middleware)
        }
    }};
    
    // Pattern without explicit flow name (uses "default")
    {
        journals: $journals:expr,
        middleware: [$($flow_mw:expr),*],
        
        stages: {
            $($stage_name:ident = $stage_macro:ident!($name:literal => $handler:expr $(, [$($mw:expr),*])?);)*
        },
        
        topology: {
            $(
                $edge:tt
            )*
        }
    } => {{
        async move {
            use $crate::prelude::*;
            use $crate::dsl::stage_descriptor::*;
            use std::sync::Arc;
            use std::collections::HashMap;
            
            let journals = $journals;
            
            // Create stages
            let mut stages: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
            
            $(
                let descriptor = $stage_macro!($name => $handler $(, [$($mw),*])?);
                stages.insert(stringify!($stage_name).to_string(), descriptor);
            )*
            
            // Create connections
            let mut connections: Vec<(String, String)> = Vec::new();
            
            // Parse topology edges
            $crate::parse_topology!(connections, $($edge)*);
            
            
            // Create closure for flow middleware
            let create_flow_middleware = || vec![
                $(Box::new($flow_mw) as Box<dyn obzenflow_adapters::middleware::MiddlewareFactory>),*
            ];
            
            // Build the flow with default name
            $crate::build_typed_flow!("default", $journals, stages, connections, create_flow_middleware)
        }
    }};
}

/// Build the actual flow from collected stages and connections
#[macro_export]
macro_rules! build_typed_flow {
    ($flow_name:expr, $journals:expr, $stages:expr, $connections:expr, $create_flow_middleware:expr) => {{
        use $crate::prelude::*;
        use std::sync::Arc;
        use std::collections::HashMap;
        
        // Helper functions for clean ID conversions
        fn to_core_id(topology_id: obzenflow_topology::StageId) -> StageId {
            // Convert topology's idkit Id to standard ulid crate's Ulid
            StageId::from_ulid(topology_id.ulid())
        }
        
        fn to_topology_id(core_id: StageId) -> obzenflow_topology::StageId {
            // Convert standard ulid crate's Ulid to topology's idkit Id
            obzenflow_topology::StageId::from_ulid(core_id.as_ulid())
        }
        
        let journal_factory_provider = $journals;
        let stages = $stages;
        let connections = $connections;
        let create_flow_middleware = $create_flow_middleware;
        
        // Build topology
        let mut builder = TopologyBuilder::new();
        let mut name_to_id = HashMap::new();
        let mut descriptors = HashMap::new();
        
        // Add stages - use real ULID generation from core
        for (name, descriptor) in stages {
            // Generate a real ULID using the core crate's StageId::new()
            let core_id = StageId::new();
            let topology_id = to_topology_id(core_id);
            
            // Use add_stage_with_id to provide the real ULID
            builder.add_stage_with_id(topology_id, Some(descriptor.name().to_string()));
            
            name_to_id.insert(name.clone(), core_id);
            descriptors.insert(name, descriptor);
            // Break auto-connection
            builder.reset_current();
        }
        
        
        // Add connections
        for (from, to) in connections {
            if let (Some(&from_id), Some(&to_id)) = 
                (name_to_id.get(&from), name_to_id.get(&to)) {
                builder.add_edge(to_topology_id(from_id), to_topology_id(to_id));
            }
        }
        
        let topology = Arc::new(builder.build()
            .map_err(|e| format!("Failed to build topology: {:?}", e))?);
        
        // Create services
        use obzenflow_runtime_services::pipeline::config::StageConfig;
        use obzenflow_runtime_services::metrics::DefaultMetricsConfig;
        use obzenflow_core::{SystemId, FlowId};
        use obzenflow_adapters::monitoring::exporters::MetricsExporterBuilder;
        
        // Create stage-local journals using the builder pattern (FLOWIP-008)
        let flow_id = FlowId::new();
        let pipeline_id = SystemId::new();
        
        // Get the journal factory for this specific flow
        let mut journal_factory = journal_factory_provider(flow_id.clone())
            .map_err(|e| format!("Failed to create journal factory: {:?}", e))?;
        
        // Create all journals upfront with proper ownership
        use obzenflow_core::journal::journal_name::JournalName;
        use obzenflow_core::journal::journal_owner::JournalOwner;
        
        let control_journal = journal_factory.create_system_journal(
            JournalName::System,
            JournalOwner::system(pipeline_id.clone())
        ).map_err(|e| format!("Failed to create system journal: {:?}", e))?;
        
        let mut stage_journals = HashMap::new();
        let mut error_journals = HashMap::new();
        for (name, &stage_id) in name_to_id.iter() {
            // Get the descriptor to access stage type and name
            let descriptor = descriptors.get(name)
                .ok_or_else(|| format!("Missing descriptor for stage {}", name))?;
            
            let journal = journal_factory.create_chain_journal(
                JournalName::Stage {
                    id: stage_id,
                    stage_type: descriptor.stage_type(),
                    name: descriptor.name().to_string(),
                },
                JournalOwner::stage(stage_id)
            ).map_err(|e| format!("Failed to create journal for stage {:?}: {:?}", stage_id, e))?;
            stage_journals.insert(stage_id, journal);
            
            // Create error journal for this stage (FLOWIP-082e)
            let error_journal = journal_factory.create_chain_journal(
                JournalName::Stage {
                    id: stage_id,
                    stage_type: descriptor.stage_type(),
                    name: format!("{}_error", descriptor.name()),
                },
                JournalOwner::stage(stage_id)
            ).map_err(|e| format!("Failed to create error journal for stage {:?}: {:?}", stage_id, e))?;
            error_journals.insert(stage_id, error_journal);
        }
        
        // Use StageResourcesBuilder to handle all the complex wiring
        use obzenflow_runtime_services::stages::resources_builder::StageResourcesBuilder;
        
        let resources_builder = StageResourcesBuilder::new(
            flow_id.clone(),
            pipeline_id.clone(),
            topology.clone(),
            control_journal,
            stage_journals,
            error_journals,
        );
        
        let stage_resources_set = resources_builder.build()
            .map_err(|e| format!("Failed to build stage resources: {:?}", e))?;
        
        // Create metrics exporter using the builder pattern
        let metrics_exporter = if DefaultMetricsConfig::default().is_enabled() {
            Some(MetricsExporterBuilder::from_env().build())
        } else {
            None
        };
        
        // Create stage supervisors using resources from StageResourcesBuilder
        let mut stages = Vec::new();
        let mut stage_resources = stage_resources_set.stage_resources;
        
        // We need to create wrapped descriptors that will merge middleware
        // This is done by wrapping the existing descriptors
        for (name, id) in name_to_id {
            if let Some(descriptor) = descriptors.remove(&name) {
                let config = StageConfig {
                    stage_id: id,
                    name: descriptor.name().to_string(),
                    flow_name: $flow_name.to_string(),
                };
                
                // Get the pre-built resources for this stage
                let resources = stage_resources.remove(&id)
                    .ok_or_else(|| format!("No resources found for stage {:?}", id))?;
                
                // Check if this stage is in a cycle and needs CycleGuard
                let mut flow_middleware = create_flow_middleware();
                let is_in_cycle = topology.is_in_cycle(to_topology_id(id));
                tracing::info!(
                    "Checking stage '{}' (id={:?}) for cycles: {}",
                    name,
                    id,
                    is_in_cycle
                );
                
                if is_in_cycle {
                    tracing::info!(
                        "Stage '{}' detected in cycle, auto-attaching CycleGuard middleware",
                        name
                    );
                    // Add CycleGuard middleware with a reasonable default (10 iterations)
                    use obzenflow_adapters::middleware::control::cycle_guard;
                    flow_middleware.push(cycle_guard(10));
                }
                
                // Create handle with flow middleware (including CycleGuard if needed)
                let handle = descriptor.create_handle_with_flow_middleware(
                    config, 
                    resources,
                    flow_middleware
                ).await
                    .map_err(|e| format!("Failed to create stage '{}': {}", name, e))?;
                stages.push(handle);
            }
        }
        
        // Create flow handle using builder pattern
        use $crate::prelude::{PipelineBuilder, FlowHandle};
        use obzenflow_runtime_services::supervised_base::SupervisorBuilder;
        
        let builder = PipelineBuilder::new(
                topology.clone(),
                stage_resources_set.system_journal.clone(),
            )
            .with_flow_name($flow_name)
            .with_stages(stages)
            .with_stage_journals(stage_resources_set.stage_journals.clone())
            .with_error_journals(stage_resources_set.error_journals.clone());
        
        let builder = if let Some(exporter) = metrics_exporter {
            builder.with_metrics(exporter)
        } else {
            builder
        };
        
        let handle = builder.build().await
            .map_err(|e| format!("Failed to build pipeline: {:?}", e))?;
        
        Ok::<FlowHandle, Box<dyn std::error::Error + Send + Sync>>(handle)
    }};
}
