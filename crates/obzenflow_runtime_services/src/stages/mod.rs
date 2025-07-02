//! Stage implementations organized by type

pub mod common;
pub mod source;
pub mod transform;
pub mod sink;

// Re-export commonly used types from common
pub use common::{
    StageHandler,
    FiniteSourceHandler, InfiniteSourceHandler, TransformHandler, SinkHandler,
    ObserverHandler, StatefulHandler, ResourceManaged,
    ControlEventStrategy, ControlEventAction, ProcessingContext,
};