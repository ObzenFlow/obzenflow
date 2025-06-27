//! Factory for creating the appropriate stage supervisor based on handler type

use crate::control_plane::stages::supervisors::{
    StageConfig,
    FiniteSourceSupervisor, TransformSupervisor,
};

// Note: The old create_stage_supervisor function that used dyn EventHandler
// has been removed. Use StageSupervisorBuilder instead for type-safe creation.

/// Builder pattern for creating stage supervisors with proper types
pub struct StageSupervisorBuilder {
    config: StageConfig,
}

impl StageSupervisorBuilder {
    pub fn new(config: StageConfig) -> Self {
        Self { config }
    }
    
    /// Create a finite source supervisor
    pub fn finite_source<H>(self, handler: H) -> FiniteSourceSupervisor<H>
    where
        H: crate::control_plane::stages::handler_traits::FiniteSourceHandler + 'static
    {
        FiniteSourceSupervisor::new(handler, self.config)
    }
    
    /// Create a transform supervisor
    pub fn transform<H>(self, handler: H) -> TransformSupervisor<H>
    where
        H: crate::control_plane::stages::handler_traits::TransformHandler + 'static
    {
        TransformSupervisor::new(handler, self.config)
    }
    
    // TODO: Add infinite_source and sink methods
}