//! Stage handle trait for pipeline coordination
//!
//! This trait defines the interface that all stage supervisors must implement
//! so the Pipeline FSM can coordinate them properly.

use obzenflow_core::StageId;
use std::fmt;

/// Error type for stage operations
#[derive(Debug, Clone)]
pub enum StageError {
    /// Failed to initialize stage
    InitializationFailed(String),
    /// Failed to send event to stage
    EventSendFailed(String),
    /// Stage is in invalid state for operation
    InvalidState(String),
    /// Operation timed out
    Timeout,
    /// Generic error
    Other(String),
}

impl fmt::Display for StageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StageError::InitializationFailed(msg) => write!(f, "Stage initialization failed: {}", msg),
            StageError::EventSendFailed(msg) => write!(f, "Failed to send event to stage: {}", msg),
            StageError::InvalidState(msg) => write!(f, "Invalid stage state: {}", msg),
            StageError::Timeout => write!(f, "Stage operation timed out"),
            StageError::Other(msg) => write!(f, "Stage error: {}", msg),
        }
    }
}

impl std::error::Error for StageError {}

impl From<String> for StageError {
    fn from(s: String) -> Self {
        StageError::Other(s)
    }
}

impl From<&str> for StageError {
    fn from(s: &str) -> Self {
        StageError::Other(s.to_string())
    }
}

/// Generic stage events that Pipeline uses for coordination
#[derive(Debug, Clone)]
pub enum StageEvent {
    Initialize,
    Start,
    BeginDrain,
    ForceShutdown,
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
    async fn initialize(&self) -> Result<(), StageError>;
    
    /// Start the stage (only sources need this, others can no-op)
    async fn start(&self) -> Result<(), StageError>;
    
    /// Send an event to the stage FSM
    async fn send_event(&self, event: StageEvent) -> Result<(), StageError>;
    
    /// Begin draining the stage
    async fn begin_drain(&self) -> Result<(), StageError>;
    
    /// Check if the stage is ready
    fn is_ready(&self) -> bool;
    
    /// Check if the stage is drained
    fn is_drained(&self) -> bool;
    
    /// Force shutdown
    async fn force_shutdown(&self) -> Result<(), StageError>;
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