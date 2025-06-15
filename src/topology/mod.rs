//! Pipeline topology with strong stage identification and directed edges

mod stage_id;
mod stage_info;
mod edge;
mod topology;
mod builder;
mod validation;
mod lifecycle;

pub use stage_id::StageId;
pub use stage_info::StageInfo;
pub use edge::DirectedEdge;
pub use topology::{PipelineTopology, TopologyMetrics};
pub use builder::PipelineBuilder;
pub use validation::TopologyError;
pub use lifecycle::{PipelineLifecycle, StageLifecycle, ShutdownSignal, DrainReport};