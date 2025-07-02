//! Factory for creating the appropriate stage supervisor based on handler type

use crate::pipeline::config::StageConfig;
use crate::stages::common::resources::StageResources;
use crate::stages::source::FiniteSourceSupervisor;
use crate::stages::transform::TransformSupervisor;

// Note: The old create_stage_supervisor function that used dyn EventHandler
// has been removed. Use StageSupervisorBuilder instead for type-safe creation.

/// Builder pattern for creating stage supervisors with proper types
pub struct StageSupervisorBuilder {
    config: StageConfig,
    resources: StageResources,
}

impl StageSupervisorBuilder {
    pub fn new(config: StageConfig, resources: StageResources) -> Self {
        Self { config, resources }
    }
    
    /// Create a finite source supervisor
    pub fn finite_source<H>(self, handler: H) -> FiniteSourceSupervisor<H>
    where
        H: crate::stages::common::handlers::FiniteSourceHandler + 'static
    {
        FiniteSourceSupervisor::new(handler, self.config, self.resources)
    }
    
    /// Create a transform supervisor
    pub fn transform<H>(self, handler: H) -> TransformSupervisor<H>
    where
        H: crate::stages::common::handlers::TransformHandler + 'static
    {
        TransformSupervisor::new(handler, self.config, self.resources)
    }
    
    // TODO: Add infinite_source and sink methods
}