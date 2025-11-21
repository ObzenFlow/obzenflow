//! Common components shared across all stage types

pub mod control_strategies;
pub mod handlers;
pub mod source_handle;
pub mod stage_handle;

// Re-export handler traits for convenience
pub use handlers::{
    FiniteSourceHandler, InfiniteSourceHandler, ObserverHandler, ResourceManaged, SinkHandler,
    StatefulHandler, TransformHandler,
};

// Re-export control strategies
pub use control_strategies::{
    BackoffStrategy, CompositeStrategy, ControlEventAction, ControlEventStrategy,
    JonestownStrategy, ProcessingContext, RetryStrategy, WindowingStrategy,
};
