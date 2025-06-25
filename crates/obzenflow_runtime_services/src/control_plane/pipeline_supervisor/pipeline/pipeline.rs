//! Pipeline - An unstarted flow configuration
//!
//! This is a simple data structure holding the topology and component configurations.
//! The PipelineSupervisor is responsible for materializing and running it.

use crate::control_plane::pipeline_supervisor::pipeline::{
    stage_config::StageConfig,
    observer_config::ObserverConfig,
};
use crate::control_plane::stage_supervisor::event_handler::EventHandler;
use obzenflow_core::Journal;
use obzenflow_topology_services::topology::Topology;
use obzenflow_topology_services::stages::StageId;
use std::sync::Arc;

/// An unstarted pipeline holding topology and component configs
pub struct Pipeline {
    pub journal: Arc<dyn Journal>,
    pub topology: Arc<Topology>,
    pub stages: Vec<StageConfig>,
    pub observers: Vec<ObserverConfig>,
}

impl Pipeline {
    /// Create a new unstarted pipeline
    pub fn new(journal: Arc<dyn Journal>, topology: Arc<Topology>) -> Self {
        Self {
            journal,
            topology,
            stages: Vec::new(),
            observers: Vec::new(),
        }
    }

    /// Add a stage configuration
    pub fn add_stage(&mut self, stage_id: StageId, name: String, handler: Box<dyn EventHandler>, is_source: bool) {
        self.stages.push(StageConfig {
            stage_id,
            name,
            handler: Arc::from(handler),
            is_source,
        });
    }

    /// Add an observer configuration
    pub fn add_observer(&mut self, name: String, handler: Box<dyn EventHandler>) {
        self.observers.push(ObserverConfig {
            name,
            handler: Arc::from(handler),
        });
    }
}