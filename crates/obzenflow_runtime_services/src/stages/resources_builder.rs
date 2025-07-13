//! Builder for creating stage resources with all their dependencies
//!
//! This module handles the complex wiring of stage-local journals, upstream journals,
//! control journals, message bus, and other resources that stages need.

use std::collections::HashMap;
use std::sync::Arc;
use obzenflow_core::{StageId, PipelineId, FlowId, JournalOwner, WriterId};
use obzenflow_core::journal::journal::Journal;
use crate::messaging::reactive_journal::{ReactiveJournal, ReactiveJournalBuilder};
use crate::message_bus::FsmMessageBus;
use obzenflow_topology_services::topology::Topology;

/// Resources provided to stage creation
#[derive(Clone)]
pub struct StageResources {
    pub journal: Arc<ReactiveJournal>,
    pub message_bus: Arc<FsmMessageBus>,
    pub upstream_stages: Vec<StageId>,
}

/// Builder for creating all stage resources with proper wiring
pub struct StageResourcesBuilder {
    flow_id: FlowId,
    pipeline_id: PipelineId,
    topology: Arc<Topology>,
    control_journal: Arc<dyn Journal>,
    stage_journals: HashMap<StageId, Arc<dyn Journal>>,
}

impl StageResourcesBuilder {
    /// Create a new builder
    pub fn new(
        flow_id: FlowId,
        pipeline_id: PipelineId,
        topology: Arc<Topology>,
        control_journal: Arc<dyn Journal>,
        stage_journals: HashMap<StageId, Arc<dyn Journal>>,
    ) -> Self {
        Self {
            flow_id,
            pipeline_id,
            topology,
            control_journal,
            stage_journals,
        }
    }
    
    /// Build all resources for all stages
    pub fn build(self) -> Result<StageResourcesSet, String> {
        // Create the topology map for ReactiveJournalBuilder
        let mut topology_map = HashMap::new();
        for stage_info in self.topology.stages() {
            let stage_id = stage_info.id;
            let upstream_ids = self.topology.upstream_stages(stage_id);
            topology_map.insert(stage_id, upstream_ids);
        }
        
        // Build stage-local journals using ReactiveJournalBuilder with pre-created journals
        let journal_builder = ReactiveJournalBuilder::new(
            topology_map,
            self.control_journal.clone(),
            self.stage_journals.clone(),
            self.pipeline_id.clone(),
        );
        
        let journal_set = journal_builder.build()
            .map_err(|e| format!("Failed to build stage-local journals: {:?}", e))?;
        
        // Create shared message bus
        let message_bus = Arc::new(FsmMessageBus::new());
        
        // Build stage resources for each stage
        let mut stage_resources = HashMap::new();
        let mut stage_journals = journal_set.stage_journals;
        
        // Keep track of raw stage journals for metrics aggregator
        let mut raw_stage_journals: Vec<(StageId, Arc<dyn Journal>)> = Vec::new();
        
        for stage_info in self.topology.stages() {
            let stage_id = stage_info.id;
            let stage_journal = stage_journals.remove(&stage_id)
                .ok_or_else(|| format!("No journal found for stage {:?}", stage_id))?;
            
            let upstream_stages = self.topology.upstream_stages(stage_id);
            
            // Keep a reference to the raw journal for metrics
            raw_stage_journals.push((stage_id, stage_journal.my_journal.clone()));
            
            let resources = StageResources {
                journal: Arc::new(stage_journal),
                message_bus: message_bus.clone(),
                upstream_stages,
            };
            
            stage_resources.insert(stage_id, resources);
        }
        
        // Create metrics aggregator's ReactiveJournal that reads from ALL stage journals
        let metrics_journal = ReactiveJournal::new(
            journal_set.pipeline_journal.control_journal.clone(), // Writes to control journal
            WriterId::new(),
            journal_set.pipeline_journal.control_journal.clone(),
            raw_stage_journals, // Reads from ALL stage journals
        );
        
        Ok(StageResourcesSet {
            flow_id: self.flow_id,
            pipeline_id: self.pipeline_id,
            pipeline_journal: journal_set.pipeline_journal,
            metrics_journal,
            stage_resources,
            message_bus,
        })
    }
}

/// Complete set of resources for all stages in a flow
pub struct StageResourcesSet {
    /// Flow execution ID
    pub flow_id: FlowId,
    
    /// Pipeline ID
    pub pipeline_id: PipelineId,
    
    /// Pipeline's reactive journal (writes to control journal)
    pub pipeline_journal: ReactiveJournal,
    
    /// Metrics aggregator's reactive journal (reads from ALL stage journals)
    pub metrics_journal: ReactiveJournal,
    
    /// Resources for each stage
    pub stage_resources: HashMap<StageId, StageResources>,
    
    /// Shared message bus
    pub message_bus: Arc<FsmMessageBus>,
}

impl StageResourcesSet {
    /// Get resources for a specific stage
    pub fn get_stage_resources(&self, stage_id: StageId) -> Option<&StageResources> {
        self.stage_resources.get(&stage_id)
    }
    
    /// Take resources for a specific stage (removes from set)
    pub fn take_stage_resources(&mut self, stage_id: StageId) -> Option<StageResources> {
        self.stage_resources.remove(&stage_id)
    }
    
    /// Get the shared message bus
    pub fn message_bus(&self) -> &Arc<FsmMessageBus> {
        &self.message_bus
    }
}