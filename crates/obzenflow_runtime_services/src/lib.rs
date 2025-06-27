//! Runtime services for ObzenFlow
//!
//! This layer contains business logic related to flow execution, runtime coordination,
//! and execution management.

// Top-level supervision and coordination
pub mod supervisor;
pub mod message_bus;
pub mod errors;

// Control plane - state management
pub mod control_plane {
    pub mod pipeline;
    pub mod stages;
}

// Data plane - event flow and routing
pub mod data_plane {
    pub mod journal_subscription;
    pub mod stage_router;
    pub mod stage_semantics;
    pub mod event_enrichment;
}

// Re-export commonly used types
pub mod prelude {
    // Errors
    pub use crate::errors::{FlowError, MessageBusError, PipelineSupervisorError, RuntimeResult};
    
    // Supervision
    pub use crate::supervisor::{PipelineSupervisor, FlowHandle};
    pub use crate::control_plane::pipeline::{PipelineState, PipelineEvent};
    pub use crate::message_bus::{FsmMessageBus, StageCommand};
    
    // Control plane - handler traits (from stages)
    pub use crate::control_plane::stages::handler_traits::{
        ResourceManaged, HealthStatus, ResourceInfo,
    };

    // Control plane - stage
    pub use crate::control_plane::stages::{
        StageSupervisor, StageConfig, 
        StageEvent, StageHandle, BoxedStageHandle,
        FiniteSourceSupervisor, TransformSupervisor,
    };

    // Data plane
    pub use crate::data_plane::{
        journal_subscription::{ReactiveJournal, JournalSubscription, SubscriptionFilter},
        stage_semantics::StageSemantics,
        stage_router::StageRouter,
    };
}
