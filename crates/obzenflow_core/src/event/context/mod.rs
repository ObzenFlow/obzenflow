//! Event context and metadata

pub mod causality_context;
pub mod flow_context;
pub mod intent_context;
pub mod observability_context;
pub mod processing_context;
pub mod replay_context;
pub mod runtime_context;
pub mod stage_type;

pub use flow_context::FlowContext;
pub use intent_context::IntentContext;
pub use processing_context::ProcessingContext;
pub use replay_context::ReplayContext;
pub use runtime_context::RuntimeContext;
pub use stage_type::{SimpleStageType, StageType};
