//! Builder for creating MetricsAggregator with proper FSM lifecycle
//!
//! This builder ensures the metrics aggregator is created and started correctly
//! according to the FSM architecture patterns, returning only a handle for control.

use super::{
    supervisor::MetricsAggregatorSupervisor,
    fsm::{MetricsAggregatorContext, MetricsAggregatorEvent, MetricsAggregatorState},
    config::DefaultMetricsConfig,
};
use crate::{
    supervised_base::{
        SupervisorBuilder, BuilderError, ChannelBuilder, HandleBuilder, StandardHandle,
        SelfSupervisedExt, SupervisorTaskBuilder,
    },
};
use obzenflow_core::{
    metrics::{MetricsExporter, StageMetadata},
    journal::journal::Journal,
    event::{ChainEvent, SystemEvent},
    StageId,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;

/// Builder for creating a metrics aggregator with proper FSM lifecycle
pub struct MetricsAggregatorBuilder {
    /// All stage journals to read metrics from
    stage_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
    
    /// System journal for reporting
    system_journal: Arc<dyn Journal<SystemEvent>>,
    
    /// Metrics exporter
    exporter: Arc<dyn MetricsExporter>,
    
    /// Stage metadata for display and categorization
    stage_metadata: HashMap<StageId, StageMetadata>,
    
    config: DefaultMetricsConfig,
    export_interval_secs: u64,
}

impl MetricsAggregatorBuilder {
    /// Create a new metrics aggregator builder
    pub fn new(
        stage_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
        system_journal: Arc<dyn Journal<SystemEvent>>,
        exporter: Arc<dyn MetricsExporter>,
    ) -> Self {
        Self {
            stage_journals,
            system_journal,
            exporter,
            stage_metadata: HashMap::new(),
            config: DefaultMetricsConfig::default(),
            export_interval_secs: 10, // Default to 10 seconds
        }
    }
    
    /// Set a custom configuration
    pub fn with_config(mut self, config: DefaultMetricsConfig) -> Self {
        self.config = config;
        self
    }
    
    /// Set the export interval in seconds
    pub fn with_export_interval(mut self, seconds: u64) -> Self {
        self.export_interval_secs = seconds;
        self
    }
    
    /// Set stage metadata for display and categorization
    pub fn with_stage_metadata(mut self, metadata: HashMap<StageId, StageMetadata>) -> Self {
        self.stage_metadata = metadata;
        self
    }
    
}

#[async_trait::async_trait]
impl SupervisorBuilder for MetricsAggregatorBuilder {
    type Handle = StandardHandle<MetricsAggregatorEvent, MetricsAggregatorState>;
    type Error = BuilderError;
    
    async fn build(self) -> Result<Self::Handle, Self::Error> {
        // Create system ID for metrics aggregator
        let system_id = obzenflow_core::id::SystemId::new();
        
        // Create metrics context with all mutable state
        let metrics_context = Arc::new(
            MetricsAggregatorContext::new(
                self.stage_journals.clone(),
                self.system_journal.clone(),
                Some(self.exporter),
                self.export_interval_secs,
                system_id,
                self.stage_metadata,
            )
            .await
            .map_err(|e| BuilderError::Other(e))?
        );
        
        // Create channels for supervisor communication
        // Even though metrics runs autonomously, we still create channels
        // for consistency and potential future use
        let (event_sender, _, state_watcher) = 
            ChannelBuilder::<MetricsAggregatorEvent, MetricsAggregatorState>::new()
                .with_event_buffer(10) // Small buffer, rarely used
                .build(MetricsAggregatorState::Initializing);
        
        // Create supervisor (private struct)
        let supervisor = MetricsAggregatorSupervisor {
            name: "metrics_aggregator".to_string(),
            context: metrics_context.clone(),
            system_journal: self.system_journal.clone(),
        };
        
        // Clone what we need for the task
        let context_for_task = metrics_context;
        let state_watcher_for_task = state_watcher.clone();
        
        // Spawn the supervisor task
        let supervisor_task = SupervisorTaskBuilder::<MetricsAggregatorSupervisor>::new("metrics_aggregator")
            .spawn(move || async move {
                // Run the supervisor directly - metrics doesn't need external events
                let supervisor_with_state_updates = supervisor;
                
                // We'll update state manually in dispatch_state
                // This is simpler than the wrapper pattern for autonomous supervisors
                let result = SelfSupervisedExt::run(
                    supervisor_with_state_updates,
                    MetricsAggregatorState::Initializing,
                    context_for_task.as_ref().clone(),
                )
                .await;
                
                // Update final state
                let _ = state_watcher_for_task.update(MetricsAggregatorState::Drained {
                    last_event_id: None,
                });
                
                result
            });
        
        // Build and return the standard handle
        HandleBuilder::new()
            .with_event_sender(event_sender)
            .with_state_watcher(state_watcher)
            .with_supervisor_task(supervisor_task)
            .build_standard()
            .map_err(|e| BuilderError::Other(e.to_string()))
    }
}