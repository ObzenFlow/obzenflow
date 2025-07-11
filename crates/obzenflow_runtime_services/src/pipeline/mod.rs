//! Pipeline-level coordination and management

pub mod supervisor;
pub mod fsm;
pub mod config;
pub mod handle;
pub mod builder;

// Re-export commonly used types
// Note: PipelineSupervisor is intentionally NOT exported - use PipelineBuilder
pub use handle::FlowHandle;
pub use builder::PipelineBuilder;
pub use fsm::{PipelineState, PipelineEvent, PipelineAction};
pub use config::{StageConfig as PipelineStageConfig, ObserverConfig, StageHandlerType};