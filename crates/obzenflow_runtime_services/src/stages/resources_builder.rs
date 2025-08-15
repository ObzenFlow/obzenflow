//! Builder for creating stage resources with all their dependencies
//!
//! This module handles the complex wiring of stage-local journals, upstream journals,
//! control journals, message bus, and other resources that stages need.

use std::collections::HashMap;
use std::sync::Arc;
use obzenflow_core::{StageId, FlowId, ChainEvent, SystemId};
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::journal::Journal;
use crate::message_bus::FsmMessageBus;
use obzenflow_topology::Topology;

/// Resources provided to stage creation
#[derive(Clone)]
pub struct StageResources {
    /// Flow execution ID (from pipeline)
    pub flow_id: FlowId,
    
    /// Stage's own journal for writing data events
    pub data_journal: Arc<dyn Journal<ChainEvent>>,
    
    /// Stage's own journal for writing error events (FLOWIP-082e)
    pub error_journal: Arc<dyn Journal<ChainEvent>>,
    
    /// Shared system journal for lifecycle events
    pub system_journal: Arc<dyn Journal<SystemEvent>>,
    
    /// Upstream journals for reading events
    pub upstream_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
    
    /// Message bus for FSM communication
    pub message_bus: Arc<FsmMessageBus>,
    
    /// List of upstream stage IDs
    pub upstream_stages: Vec<StageId>,
    
    /// All error journals from all stages (only populated for ErrorSink)
    pub error_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
}

/// Builder for creating all stage resources with proper wiring
pub struct StageResourcesBuilder {
    flow_id: FlowId,
    pipeline_system_id: SystemId,
    topology: Arc<Topology>,
    system_journal: Arc<dyn Journal<SystemEvent>>,
    stage_journals: HashMap<StageId, Arc<dyn Journal<ChainEvent>>>,
    error_journals: HashMap<StageId, Arc<dyn Journal<ChainEvent>>>,
}

impl StageResourcesBuilder {
    /// Create a new builder
    pub fn new(
        flow_id: FlowId,
        pipeline_system_id: SystemId,
        topology: Arc<Topology>,
        system_journal: Arc<dyn Journal<SystemEvent>>,
        stage_journals: HashMap<StageId, Arc<dyn Journal<ChainEvent>>>,
        error_journals: HashMap<StageId, Arc<dyn Journal<ChainEvent>>>,
    ) -> Self {
        Self {
            flow_id,
            pipeline_system_id,
            topology,
            system_journal,
            stage_journals,
            error_journals,
        }
    }
    
    /// Build all resources for all stages
    pub fn build(self) -> Result<StageResourcesSet, String> {
        // Create shared message bus
        let message_bus = Arc::new(FsmMessageBus::new());
        
        // Build stage resources for each stage
        let mut stage_resources = HashMap::new();
        
        // Keep track of all stage journals for metrics aggregator
        let mut all_stage_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)> = Vec::new();
        
        // Keep track of all error journals for ErrorSink
        let mut all_error_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)> = Vec::new();
        
        for stage_info in self.topology.stages() {
            let stage_ulid = stage_info.id;
            let stage_id = StageId::from_ulid(stage_ulid);
            
            // Get the stage's own journal
            let data_journal = self.stage_journals.get(&stage_id)
                .ok_or_else(|| format!("No journal found for stage {:?}", stage_id))?
                .clone();
            
            // Get the stage's error journal
            let error_journal = self.error_journals.get(&stage_id)
                .ok_or_else(|| format!("No error journal found for stage {:?}", stage_id))?
                .clone();
            
            // Keep a reference for metrics aggregator
            all_stage_journals.push((stage_id, data_journal.clone()));
            
            // Keep a reference for ErrorSink
            all_error_journals.push((stage_id, error_journal.clone()));
            
            // Get upstream journals
            let upstream_ulids = self.topology.upstream_stages(stage_ulid);
            let upstream_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)> = upstream_ulids
                .iter()
                .filter_map(|upstream_ulid| {
                    let upstream_id = StageId::from_ulid(*upstream_ulid);
                    self.stage_journals.get(&upstream_id).map(|journal| {
                        (upstream_id, journal.clone())
                    })
                })
                .collect();
            
            // Check if this is the ErrorSink stage (FLOWIP-082g)
            let error_journals_for_stage = if stage_info.name == "__error_sink" {
                all_error_journals.clone()
            } else {
                Vec::new()
            };
            
            let resources = StageResources {
                flow_id: self.flow_id.clone(),
                data_journal,
                error_journal,
                system_journal: self.system_journal.clone(),
                upstream_journals,
                message_bus: message_bus.clone(),
                upstream_stages: upstream_ulids.into_iter().map(StageId::from_ulid).collect(),
                error_journals: error_journals_for_stage,
            };
            
            stage_resources.insert(stage_id, resources);
        }
        
        Ok(StageResourcesSet {
            flow_id: self.flow_id,
            pipeline_system_id: self.pipeline_system_id,
            system_journal: self.system_journal,
            stage_journals: all_stage_journals,
            error_journals: all_error_journals,
            stage_resources,
            message_bus,
        })
    }
}

/// Complete set of resources for all stages in a flow
pub struct StageResourcesSet {
    /// Flow execution ID
    pub flow_id: FlowId,
    
    /// Pipeline system ID
    pub pipeline_system_id: SystemId,
    
    /// System journal for orchestration events
    pub system_journal: Arc<dyn Journal<SystemEvent>>,
    
    /// All stage journals (for metrics aggregator to read)
    pub stage_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
    
    /// All error journals (for ErrorSink to read) (FLOWIP-082e)
    pub error_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
    
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