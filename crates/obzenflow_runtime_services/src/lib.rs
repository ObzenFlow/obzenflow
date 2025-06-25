//! Runtime services for ObzenFlow
//!
//! This layer contains business logic related to flow execution, runtime coordination,
//! and execution management.

// Top-level supervision and coordination
pub mod supervisor;
pub mod message_bus;
pub mod errors;

// Control plane - lifecycle and state management
pub mod control_plane {
    pub mod pipeline_supervisor {
        pub mod pipeline_fsm;
        pub mod drainable;
        pub mod in_flight_tracker;

        pub mod pipeline {
            pub mod pipeline;
            pub mod pipeline_ext;
            pub mod stage_config;
            pub mod observer_config;
        }
    }

    pub mod stage_supervisor {
        pub mod stage_fsm;
        pub mod stage_supervisor;
        pub mod event_handler;
        pub mod drainable;
        pub mod completable;
        pub mod resource_managed;
    }
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
    pub use crate::control_plane::pipeline_supervisor::pipeline_fsm::{PipelineState, PipelineEvent};
    pub use crate::message_bus::{FsmMessageBus, StageCommand, StageNotification};
    
    // Control plane
    pub use crate::control_plane::pipeline_supervisor::{
        drainable::Drainable,
        in_flight_tracker::InFlightTracker,
    };

    pub use crate::control_plane::stage_supervisor::{
        stage_supervisor::StageSupervisor,
        stage_fsm::{StageHandle, StageState, StageEvent},
        event_handler::EventHandler,
        completable::Completable,
        resource_managed::ResourceManaged,
    };

    // Data plane
    pub use crate::data_plane::{
        journal_subscription::{ReactiveJournal, JournalSubscription, SubscriptionFilter},
        stage_semantics::StageSemantics,
        stage_router::StageRouter,
    };
}
