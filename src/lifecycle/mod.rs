//! Unified lifecycle management for event processing components
//! 
//! This module provides composable traits for different aspects of component lifecycle:
//! - EventHandler: Core processing patterns (transform, observe, aggregate)
//! - Drainable: Graceful shutdown coordination
//! - Stateful: State management and checkpointing
//! - ResourceManaged: External resource lifecycle
//! - FlowControlled: Backpressure and rate limiting

pub mod event_handler;
pub mod drainable;
pub mod stateful;
pub mod resource_managed;
pub mod flow_controlled;
pub mod generic_processor;

pub use event_handler::{EventHandler, ProcessingMode};
pub use drainable::{Drainable, ComponentType};
pub use stateful::{Stateful, CheckpointData, StateInfo};
pub use resource_managed::{ResourceManaged, HealthStatus, ResourceInfo};
pub use flow_controlled::{FlowControlled, CircuitState};
pub use generic_processor::{GenericEventProcessor, ProcessorState};