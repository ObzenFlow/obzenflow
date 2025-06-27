//! FLOWIP-084: Specialized handler traits for different stage types
//!
//! These traits replace the old monolithic EventHandler with type-specific
//! interfaces that express exactly what each stage type does.
//!
//! Each handler trait:
//! - Has a single clear responsibility
//! - Uses appropriate method names (next, process, consume, observe)
//! - Maps to a specific FSM type

pub mod finite_source_handler;
pub mod infinite_source_handler;
pub mod transform_handler;
pub mod sink_handler;
pub mod stateful_handler;
pub mod observer_handler;
pub mod resource_managed;

pub use finite_source_handler::FiniteSourceHandler;
pub use infinite_source_handler::InfiniteSourceHandler;
pub use transform_handler::TransformHandler;
pub use sink_handler::SinkHandler;
pub use stateful_handler::StatefulHandler;
pub use observer_handler::ObserverHandler;
pub use resource_managed::{ResourceManaged, HealthStatus, ResourceInfo};