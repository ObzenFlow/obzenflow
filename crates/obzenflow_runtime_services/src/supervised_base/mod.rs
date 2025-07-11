//! Base abstractions for the runtime services
//! 
//! This module provides supervised state machine patterns that enforce FSM-driven control flow:
//! - `SelfSupervisedStateMachine` - for state machines that contain their own logic
//! - `HandlerSupervisedStateMachine` - for state machines that delegate to handlers

pub mod base;
pub mod self_supervised;
pub mod handler_supervised;
pub mod builder;
pub mod handle;

// Re-export main types
pub use base::EventLoopDirective;
pub use self_supervised::{SelfSupervised, SelfSupervisedExt};
pub use handler_supervised::{HandlerSupervised, HandlerSupervisedExt};

// Builder and handle infrastructure
pub use builder::{
    SupervisorBuilder, BuilderError, SupervisorHandle, HandleError,
    ChannelBuilder, EventSender, EventReceiver, StateWatcher,
};
pub use handle::{HandleBuilder, StandardHandle, SupervisorTaskBuilder};

// DON'T export Supervisor - force users to choose SelfSupervised or HandlerSupervised