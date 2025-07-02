//! Pipeline - An unstarted flow configuration
//!
//! This is a simple data structure holding the topology and component configurations.
//! The PipelineSupervisor is responsible for materializing and running it.

use super::config::ObserverConfig;
use crate::stages::common::stage_handle::BoxedStageHandle;
use obzenflow_core::Journal;
use obzenflow_topology_services::topology::Topology;
use std::sync::Arc;

/// An unstarted pipeline holding topology and component configs
pub struct Pipeline {
    pub journal: Arc<dyn Journal>,
    pub topology: Arc<Topology>,
    pub stages: Vec<BoxedStageHandle>,
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

    /// Add a stage handle
    pub fn add_stage(&mut self, stage: BoxedStageHandle) {
        self.stages.push(stage);
    }

    /// Add an observer configuration
    /// TODO: Update when observer handler traits are designed
    pub fn add_observer(&mut self, name: String) {
        self.observers.push(ObserverConfig {
            name,
        });
    }
}