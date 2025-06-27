//! Stage supervisors for different handler types

pub mod config;
pub mod traits;
pub mod stage_handle;
pub mod finite_source_supervisor;
pub mod infinite_source_supervisor;
pub mod transform_supervisor;
pub mod sink_supervisor;
// TODO: Add these when implemented:
// pub mod stateful_supervisor;

pub use config::StageConfig;
pub use traits::{StageSupervisor, AnyStageSupervisor};
pub use stage_handle::{StageHandle, StageType, BoxedStageHandle, StageEvent};
pub use finite_source_supervisor::FiniteSourceSupervisor;
pub use infinite_source_supervisor::InfiniteSourceSupervisor;
pub use transform_supervisor::TransformSupervisor;
pub use sink_supervisor::SinkSupervisor;