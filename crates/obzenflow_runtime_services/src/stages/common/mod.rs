//! Common components shared across all stage types

pub mod handlers;
pub mod control_strategies;
pub mod stage_handle;

// Re-export handler traits for convenience
pub use handlers::{
    FiniteSourceHandler, InfiniteSourceHandler,
    TransformHandler,
    SinkHandler,
    ObserverHandler, StatefulHandler, ResourceManaged,
};

// Re-export control strategies
pub use control_strategies::{
    ControlEventStrategy, ControlEventAction, ProcessingContext,
    JonestownStrategy, RetryStrategy, BackoffStrategy,
    WindowingStrategy, CompositeStrategy,
};