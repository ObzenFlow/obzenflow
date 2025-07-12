//! Runtime services for ObzenFlow
//!
//! This layer contains business logic related to flow execution, runtime coordination,
//! and execution management.

// Core modules
pub mod errors;
pub mod message_bus;
pub mod supervised_base;

// Major subsystems
pub mod pipeline;
pub mod stages;
pub mod messaging;
pub mod metrics;


// Re-export commonly used types
pub mod prelude {
    // Errors
    pub use crate::errors::{FlowError, MessageBusError, PipelineSupervisorError, RuntimeResult};
    
    // Pipeline
    pub use crate::pipeline::{
        PipelineBuilder, FlowHandle,
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
    
    // Note: Supervisors are internal implementation details and not exported.
    // Use builders and handles for creating and controlling stages.
    
    // Event flow
    pub use crate::messaging::{
        JournalSubscription, SubscriptionFilter,
    };
    
    // Metrics
    pub use crate::metrics::DefaultMetricsConfig;
}