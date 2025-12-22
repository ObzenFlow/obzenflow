//! Pipeline-level coordination and management

pub mod builder;
pub mod config;
pub mod fsm;
pub mod handle;
pub mod join_metadata;
pub mod supervisor;

// Re-export commonly used types
// Note: PipelineSupervisor is intentionally NOT exported - use PipelineBuilder
pub use builder::PipelineBuilder;
pub use config::{ObserverConfig, StageConfig as PipelineStageConfig, StageHandlerType};
pub use fsm::{PipelineAction, PipelineEvent, PipelineState};
pub use handle::{FlowHandle, MiddlewareStackConfig};
pub use join_metadata::JoinMetadata;
