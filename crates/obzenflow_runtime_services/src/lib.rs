//! Runtime services for ObzenFlow
//!
//! This layer contains business logic related to flow execution, runtime coordination,
//! and execution management.

// Core modules
pub mod errors;
pub mod id_conversions;
pub mod message_bus;
pub mod supervised_base;
pub mod contracts;

// Major subsystems
pub mod messaging;
pub mod metrics;
pub mod pipeline;
pub mod stages;

// Re-export commonly used types
pub mod prelude {
    // Errors
    pub use crate::errors::{FlowError, MessageBusError, PipelineSupervisorError, RuntimeResult};

    // Pipeline
    pub use crate::pipeline::{
        FlowHandle, ObserverConfig, PipelineAction, PipelineBuilder, PipelineEvent,
        PipelineStageConfig, PipelineState,
    };

    // Message bus
    pub use crate::message_bus::{FsmMessageBus, StageCommand};

    // Handlers
    pub use crate::stages::{
        FiniteSourceHandler, InfiniteSourceHandler, ObserverHandler, ResourceManaged, SinkHandler,
        SourceError, StatefulHandler, TransformHandler,
    };

    // Note: Supervisors are internal implementation details and not exported.
    // Use builders and handles for creating and controlling stages.

    // Event flow
    pub use crate::messaging::UpstreamSubscription;

    // Metrics
    pub use crate::metrics::DefaultMetricsConfig;
}
