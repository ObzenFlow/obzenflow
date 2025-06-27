//! Pipeline management and coordination

pub mod pipeline;
pub mod pipeline_fsm;
pub mod stage_config;
pub mod observer_config;

// Re-export commonly used types
pub use pipeline::Pipeline;
pub use pipeline_fsm::{PipelineState, PipelineEvent, PipelineAction};
pub use stage_config::StageConfig as PipelineStageConfig;
pub use observer_config::ObserverConfig;