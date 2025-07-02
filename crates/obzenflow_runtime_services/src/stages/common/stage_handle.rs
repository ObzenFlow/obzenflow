//! Stage handle trait for pipeline coordination
//!
//! This trait defines the interface that all stage supervisors must implement
//! so the Pipeline FSM can coordinate them properly.

use obzenflow_topology_services::stages::StageId;

/// Generic stage events that Pipeline uses for coordination
#[derive(Debug, Clone)]
pub enum StageEvent {
    Initialize,
    Start,
    Shutdown,
    Error(String),
}

/// A handle to a stage that the Pipeline FSM can use for coordination
///
/// This trait exposes exactly what the Pipeline needs:
/// - Identity (stage_id, name)
/// - Lifecycle control (initialize, start, drain)
/// - State queries (is_ready, is_drained)
#[async_trait::async_trait]
pub trait StageHandle: Send + Sync {
    /// Get the stage ID
    fn stage_id(&self) -> StageId;
    
    /// Get the stage name
    fn stage_name(&self) -> &str;
    
    /// Get the stage type (for pipeline decisions)
    fn stage_type(&self) -> StageType;
    
    /// Initialize the stage (allocate resources, create subscriptions)
    async fn initialize(&mut self) -> Result<(), String>;
    
    /// Start the stage (only sources need this, others can no-op)
    async fn start(&mut self) -> Result<(), String>;
    
    /// Send an event to the stage FSM
    async fn send_event(&mut self, event: StageEvent) -> Result<(), String>;
    
    /// Begin draining the stage
    async fn begin_drain(&mut self) -> Result<(), String>;
    
    /// Check if the stage is ready
    fn is_ready(&self) -> bool;
    
    /// Check if the stage is drained
    fn is_drained(&self) -> bool;
    
    /// Force shutdown
    async fn force_shutdown(&mut self) -> Result<(), String>;
}

/// Stage type for pipeline coordination decisions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageType {
    FiniteSource,
    InfiniteSource,
    Transform,
    Sink,
    Stateful,
}

impl StageType {
    /// Check if this is a source that needs explicit start
    pub fn is_source(&self) -> bool {
        matches!(self, StageType::FiniteSource | StageType::InfiniteSource)
    }
    
    /// Check if this stage generates events
    pub fn generates_events(&self) -> bool {
        self.is_source()
    }
    
    /// Check if this stage consumes events
    pub fn consumes_events(&self) -> bool {
        !self.is_source()
    }
}

/// Type-erased stage handle for pipeline storage
pub type BoxedStageHandle = Box<dyn StageHandle>;