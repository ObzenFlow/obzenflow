//! Common components shared across all stage types

pub mod handlers;
pub mod control_strategies;
pub mod stage_handler;
pub mod stage_handle;
pub mod traits;
pub mod resources;

// Re-export commonly used types
pub use stage_handler::StageHandler;
pub use stage_handle::{StageHandle, StageType, BoxedStageHandle, StageEvent};
pub use traits::{StageSupervisor, AnyStageSupervisor};
pub use resources::StageResources;

// Re-export handler traits for convenience
pub use handlers::{
    FiniteSourceHandler, InfiniteSourceHandler, FiniteSourceContext, InfiniteSourceContext,
    FiniteSourceWrapper, InfiniteSourceWrapper,
    TransformHandler, TransformContext, TransformWrapper,
    SinkHandler, SinkContext, SinkWrapper,
    ObserverHandler, StatefulHandler, ResourceManaged,
};

// Re-export control strategies
pub use control_strategies::{
    ControlEventStrategy, ControlEventAction, ProcessingContext,
    JonestownStrategy, RetryStrategy, BackoffStrategy,
    WindowingStrategy, CompositeStrategy,
};