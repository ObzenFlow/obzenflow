//! Handler-related components organized by type

pub mod join;
pub mod observer;
pub mod resource_managed;
pub mod sink;
pub mod source;
pub mod stateful;
pub mod transform;

// Re-export all handler traits for convenience
pub use join::JoinHandler;
pub use observer::ObserverHandler;
pub use resource_managed::ResourceManaged;
pub use sink::SinkHandler;
pub use source::{
    AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler, FiniteSourceHandler,
    InfiniteSourceHandler,
};
pub use stateful::{StatefulHandler, StatefulHandlerExt, StatefulHandlerWithEmission};
pub use transform::{AsyncTransformHandler, TransformHandler};
