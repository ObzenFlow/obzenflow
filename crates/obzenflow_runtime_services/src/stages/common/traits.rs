//! Common traits for all stage supervisors

use obzenflow_topology_services::stages::StageId;

/// Common interface for all stage supervisors
#[async_trait::async_trait]
pub trait StageSupervisor: Send + Sync {
    /// Get the stage ID
    fn stage_id(&self) -> StageId;
    
    /// Get the stage name
    fn stage_name(&self) -> &str;
    
    /// Initialize the stage
    async fn initialize(&mut self) -> Result<(), String>;
    
    /// Start the stage (only sources need to handle this)
    async fn start(&mut self) -> Result<(), String>;
    
    /// Begin draining the stage
    async fn begin_drain(&mut self) -> Result<(), String>;
    
    /// Check if the stage is drained
    async fn is_drained(&self) -> bool;
    
    /// Force shutdown
    async fn force_shutdown(&mut self) -> Result<(), String>;
}

/// Type-erased stage supervisor for pipeline management
pub enum AnyStageSupervisor {
    FiniteSource(Box<dyn StageSupervisor>),
    InfiniteSource(Box<dyn StageSupervisor>),
    Transform(Box<dyn StageSupervisor>),
    Sink(Box<dyn StageSupervisor>),
}