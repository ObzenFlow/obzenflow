//! Runtime services for ObzenFlow
//!
//! This layer contains business logic related to flow execution, runtime coordination,
//! and execution management.

// Core modules
pub mod errors;
pub mod message_bus;

// Major subsystems
pub mod pipeline;
pub mod stages;
pub mod event_flow;
pub mod factory;


// Re-export commonly used types
pub mod prelude {
    // Errors
    pub use crate::errors::{FlowError, MessageBusError, PipelineSupervisorError, RuntimeResult};
    
    // Pipeline
    pub use crate::pipeline::{
        PipelineSupervisor, Pipeline,
        PipelineState, PipelineEvent, PipelineAction,
        PipelineStageConfig, ObserverConfig,
    };
    
    // Message bus
    pub use crate::message_bus::{FsmMessageBus, StageCommand};
    
    // Handlers
    pub use crate::stages::{
        FiniteSourceHandler, InfiniteSourceHandler, TransformHandler, SinkHandler,
        ObserverHandler, StatefulHandler, ResourceManaged,
    };
    
    // Supervisors
    pub use crate::stages::source::{FiniteSourceSupervisor, InfiniteSourceSupervisor};
    pub use crate::stages::transform::TransformSupervisor;
    pub use crate::stages::sink::SinkSupervisor;
    
    // Event flow
    pub use crate::event_flow::{
        JournalSubscription, SubscriptionFilter,
        EventEnricher, BoundaryType, StageSemantics,
    };
}