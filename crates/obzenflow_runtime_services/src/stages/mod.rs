//! Stage implementations organized by type

pub mod common;
pub mod source;
pub mod transform;
pub mod sink;
pub mod resources_builder;

// Re-export commonly used types from common
pub use common::{
    FiniteSourceHandler, InfiniteSourceHandler, TransformHandler, SinkHandler,
    ObserverHandler, StatefulHandler, ResourceManaged,
    ControlEventStrategy, ControlEventAction, ProcessingContext,
};

// Re-export resources builder
pub use resources_builder::{StageResources, StageResourcesBuilder, StageResourcesSet};