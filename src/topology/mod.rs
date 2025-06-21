//! Pipeline topology with strong stage identification and directed edges

mod stage_id;
mod stage_info;
mod edge;
mod topology;
mod builder;
mod validation;
mod lifecycle;
mod layered_lifecycle;
mod layer_adapters;
mod drainable;

pub use stage_id::StageId;
pub use stage_info::StageInfo;
pub use edge::DirectedEdge;
pub use topology::{PipelineTopology, TopologyMetrics};
pub use builder::PipelineBuilder;
pub use validation::TopologyError;
pub use lifecycle::{PipelineLifecycle, StageLifecycle, ShutdownSignal, DrainReport};
pub use layered_lifecycle::{LayeredPipelineLifecycle, LayerComponent, LayerType, LayerState, StageLifecycleHandle};
pub use layer_adapters::{StageLayerAdapter, ObserverLayerAdapter};
pub use drainable::{Drainable, ComponentType, DrainState};