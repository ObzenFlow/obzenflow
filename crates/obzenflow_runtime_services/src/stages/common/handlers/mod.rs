//! Handler-related components organized by type

pub mod source;
pub mod transform;
pub mod sink;
pub mod observer;
pub mod stateful;
pub mod resource_managed;
pub mod error_sink_handler;

// Re-export all handler traits for convenience
pub use source::{FiniteSourceHandler, InfiniteSourceHandler};
pub use transform::{TransformHandler};
pub use sink::{SinkHandler};
pub use observer::ObserverHandler;
pub use stateful::StatefulHandler;
pub use resource_managed::ResourceManaged;
pub use error_sink_handler::{ErrorSinkHandler, DefaultErrorSinkHandler};