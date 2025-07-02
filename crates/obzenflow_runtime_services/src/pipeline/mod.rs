//! Pipeline-level coordination and management

pub mod supervisor;
pub mod fsm;
pub mod pipeline;
pub mod config;

// Re-export commonly used types
pub use supervisor::PipelineSupervisor;
pub use fsm::{PipelineState, PipelineEvent, PipelineAction};
pub use pipeline::Pipeline;
pub use config::{StageConfig as PipelineStageConfig, ObserverConfig, StageHandlerType};