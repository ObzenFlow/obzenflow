//! Main flow! macro - the primary API for building ObzenFlow pipelines
//!
//! This is now a clean implementation using the let bindings approach that
//! separates stage declaration from topology definition.

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
            $($from:ident |> $to:ident;)*
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
            $(
                connections.push((
                    stringify!($from).to_string(), 
                    stringify!($to).to_string()
                ));
            )*
            
            // Build the flow
            $crate::build_typed_flow!($flow_name, $journals, stages, connections, [$($flow_mw),*])
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
            $($from:ident |> $to:ident;)*
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
            $(
                connections.push((
                    stringify!($from).to_string(), 
                    stringify!($to).to_string()
                ));
            )*
            
            // Build the flow with default name
            $crate::build_typed_flow!("default", $journals, stages, connections, [$($flow_mw),*])
        }
    }};
}

/// Build the actual flow from collected stages and connections
#[macro_export]
macro_rules! build_typed_flow {
    ($flow_name:expr, $journals:expr, $stages:expr, $connections:expr, [$($flow_mw:expr),*]) => {{
        use $crate::prelude::*;
        use std::sync::Arc;
        use std::collections::HashMap;
        
        let journal_factory_provider = $journals;
        let stages = $stages;
        let connections = $connections;
        
        // Build topology
        let mut builder = TopologyBuilder::new();
        let mut name_to_id = HashMap::new();
        let mut descriptors = HashMap::new();
        
        // Add stages
        for (name, descriptor) in stages {
            let id = builder.add_stage(Some(descriptor.name().to_string()));
            name_to_id.insert(name.clone(), id);
            descriptors.insert(name, descriptor);
            // Break auto-connection
            builder.reset_current();
        }
        
        // Add connections
        for (from, to) in connections {
            if let (Some(&from_id), Some(&to_id)) = 
                (name_to_id.get(&from), name_to_id.get(&to)) {
                builder.add_edge(from_id, to_id);
            }
        }
        
        let topology = Arc::new(builder.build()
            .map_err(|e| format!("Failed to build topology: {:?}", e))?);
        
        // Create services
        use obzenflow_runtime_services::pipeline::config::StageConfig;
        use obzenflow_runtime_services::metrics::DefaultMetricsConfig;
        use obzenflow_core::{PipelineId, FlowId};
        use obzenflow_adapters::monitoring::exporters::MetricsExporterBuilder;
        
        // Create stage-local journals using the builder pattern (FLOWIP-008)
        let flow_id = FlowId::new();
        let pipeline_id = PipelineId::new();
        
        // Get the journal factory for this specific flow
        let mut journal_factory = journal_factory_provider(flow_id.clone())
            .map_err(|e| format!("Failed to create journal factory: {:?}", e))?;
        
        // Create all journals upfront with proper ownership
        use obzenflow_core::journal::journal_name::JournalName;
        use obzenflow_core::journal::journal_owner::JournalOwner;
        use obzenflow_infra::journal::JournalFactory;
        
        let control_journal = journal_factory.create_journal(
            JournalName::Control,
            JournalOwner::pipeline(pipeline_id.clone())
        ).map_err(|e| format!("Failed to create control journal: {:?}", e))?;
        
        let mut stage_journals = HashMap::new();
        for &stage_id in name_to_id.values() {
            let journal = journal_factory.create_journal(
                JournalName::Stage(stage_id),
                JournalOwner::stage(stage_id)
            ).map_err(|e| format!("Failed to create journal for stage {:?}: {:?}", stage_id, e))?;
            stage_journals.insert(stage_id, journal);
        }
        
        // Use StageResourcesBuilder to handle all the complex wiring
        use obzenflow_runtime_services::stages::resources_builder::StageResourcesBuilder;
        
        let resources_builder = StageResourcesBuilder::new(
            flow_id.clone(),
            pipeline_id.clone(),
            topology.clone(),
            control_journal,
            stage_journals,
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
                
                let handle = descriptor.create_handle(config, resources).await
                    .map_err(|e| format!("Failed to create stage '{}': {}", name, e))?;
                stages.push(handle);
            }
        }
        
        // Create flow handle using builder pattern
        use $crate::prelude::{PipelineBuilder, FlowHandle};
        use obzenflow_runtime_services::supervised_base::SupervisorBuilder;
        
        let builder = PipelineBuilder::new(topology.clone(), Arc::new(stage_resources_set.pipeline_journal))
            .with_stages(stages)
            .with_metrics_journal(Arc::new(stage_resources_set.metrics_journal));
        
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
