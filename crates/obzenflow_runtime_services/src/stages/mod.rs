//! Stage implementations organized by type

pub mod common;
pub mod join;
pub mod resources_builder;
pub mod sink;
pub mod source;
pub mod stateful;
pub mod transform;

// Re-export commonly used types from common
pub use common::{
    ControlEventAction, ControlEventStrategy, FiniteSourceHandler, InfiniteSourceHandler,
    ObserverHandler, ProcessingContext, ResourceManaged, SinkHandler, StatefulHandler,
    TransformHandler,
};

// Re-export JoinHandler from common::handlers
pub use common::handlers::JoinHandler;

// Re-export resources builder
pub use resources_builder::{StageResources, StageResourcesBuilder, StageResourcesSet};
