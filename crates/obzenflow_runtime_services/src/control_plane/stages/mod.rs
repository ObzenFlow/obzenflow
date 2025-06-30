//! FLOWIP-084: Specialized stage FSMs with clear organization
//!
//! This module provides type-specific FSMs for different stage behaviors,
//! eliminating conditional logic and runtime type checks.

pub mod states;
pub mod events;
pub mod actions;
pub mod handler_traits;
pub mod handler_contexts;
pub mod handler_impls;
pub mod fsm_builders;
pub mod control_strategy;
pub mod supervisors;
pub mod stage_supervisor_factory;

// Re-export commonly used types
pub use states::{SourceState, TransformState, SinkState};
pub use events::{SourceEvent, TransformEvent, SinkEvent};
pub use actions::{SourceAction, TransformAction, SinkAction};

// Re-export handler implementations and wrappers
pub use handler_impls::{
    StageHandler, 
    FiniteSourceWrapper, 
    InfiniteSourceWrapper,
    TransformWrapper,
    SinkWrapper,
};

// Re-export supervisor types
pub use supervisors::{
    StageConfig,
    StageSupervisor,
    AnyStageSupervisor,
    StageHandle,
    StageType,
    BoxedStageHandle,
    StageEvent,
    FiniteSourceSupervisor,
    TransformSupervisor,
};