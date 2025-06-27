use obzenflow_topology_services::stages::StageId;
use crate::control_plane::stages::handler_traits::{
    FiniteSourceHandler, InfiniteSourceHandler, TransformHandler, SinkHandler
};

/// Stage handler type that can be converted to BoxedStageHandle
pub enum StageHandlerType {
    FiniteSource(Box<dyn FiniteSourceHandler>),
    InfiniteSource(Box<dyn InfiniteSourceHandler>),
    Transform(Box<dyn TransformHandler>),
    Sink(Box<dyn SinkHandler>),
    // TODO: FLOWIP-080 will fix Stateful with proper type erasure for associated types
}

/// Stage configuration data - everything needed to build a stage supervisor
pub struct StageConfig {
    pub stage_id: StageId,
    pub name: String,
    pub handler: StageHandlerType,
}