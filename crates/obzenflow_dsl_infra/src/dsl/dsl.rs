//! Main flow! macro - the primary API for building ObzenFlow pipelines
//!
//! This is now a clean implementation using the let bindings approach that
//! separates stage declaration from topology definition.

/// The main flow! macro using the clean typed approach
#[macro_export]
macro_rules! flow {
    {
        name: $flow_name:literal,
        journal: $journal:expr,
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
            
            let journal = $journal;
            
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
            $crate::build_typed_flow!($flow_name, journal, stages, connections, [$($flow_mw),*])
        }
    }};
}

/// Build the actual flow from collected stages and connections
#[macro_export]
macro_rules! build_typed_flow {
    ($flow_name:expr, $journal:expr, $stages:expr, $connections:expr, [$($flow_mw:expr),*]) => {{
        use $crate::prelude::*;
        use std::sync::Arc;
        use std::collections::HashMap;
        
        let journal = $journal;
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
        use obzenflow_runtime_services::event_flow::reactive_journal::ReactiveJournal;
        use obzenflow_runtime_services::message_bus::FsmMessageBus;
        use obzenflow_runtime_services::pipeline::config::StageConfig;
        use obzenflow_runtime_services::stages::common::resources::StageResources;
        use obzenflow_runtime_services::metrics::{DefaultMetricsConfig, MetricsAggregatorSupervisor};
        use obzenflow_core::metrics::MetricsExporter;
        use std::sync::Mutex;
        
        // Wire metrics if enabled
        let (reactive_journal, metrics_exporter) = {
            let metrics_config = DefaultMetricsConfig::default();
            let journal = Arc::new(ReactiveJournal::new(journal.clone()));
            let mut metrics_exporter = None;
            
            if metrics_config.is_enabled() {
                // Use clean PrometheusExporter - no blocking observer!
                use obzenflow_adapters::monitoring::exporters::PrometheusExporter;
                
                let exporter = Arc::new(PrometheusExporter::new());
                tracing::info!("Created metrics exporter at {:p}", Arc::as_ptr(&exporter));
                let exporter_dyn: Arc<dyn MetricsExporter> = exporter.clone();
                metrics_exporter = Some(exporter_dyn.clone());
                
                // Metrics aggregator is now started by the pipeline supervisor
                // No need to create it here
                
                // TODO: Wire InfraMetricsObserver when ReactiveJournal supports it
                // The InfraMetricsObserver exists in runtime_services but needs
                // a hook in ReactiveJournal to measure write latency without
                // going through the journal (observer paradox).
                // 
                // if metrics_config.collect_infra_metrics {
                //     use obzenflow_runtime_services::metrics::InfraMetricsObserver;
                //     let observer = InfraMetricsObserver::new();
                //     // Need: journal.with_infra_observer(observer, exporter);
                // }
            }
            
            (journal, metrics_exporter)
        };
        let message_bus = Arc::new(FsmMessageBus::new());
        
        // Create stage supervisors
        let mut stages = Vec::new();
        for (name, id) in name_to_id {
            if let Some(descriptor) = descriptors.remove(&name) {
                let upstream_stages = topology.upstream_stages(id);
                
                let config = StageConfig {
                    stage_id: id,
                    name: descriptor.name().to_string(),
                    flow_name: $flow_name.to_string(),
                };
                
                let resources = StageResources {
                    journal: reactive_journal.clone(),
                    message_bus: message_bus.clone(),
                    upstream_stages,
                };
                
                let supervisor = descriptor.create_supervisor(config, resources);
                stages.push(supervisor);
            }
        }
        
        // Create flow handle
        use $crate::prelude::{PipelineSupervisor, FlowHandle};
        use tokio::sync::RwLock;
        
        let mut supervisor = PipelineSupervisor::new(
            topology.clone(), 
            reactive_journal.clone(), 
            stages,
            metrics_exporter
        )
        .map_err(|e| format!("Failed to create supervisor: {:?}", e))?;
        
        supervisor.materialize().await?;
        
        let handle = FlowHandle::new(Arc::new(RwLock::new(supervisor)));
        
        Ok::<FlowHandle, Box<dyn std::error::Error + Send + Sync>>(handle)
    }};
}
