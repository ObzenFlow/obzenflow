//! Base abstractions for the runtime services
//!
//! This module provides supervised state machine patterns that enforce FSM-driven control flow:
//! - `SelfSupervisedStateMachine` - for state machines that contain their own logic
//! - `HandlerSupervisedStateMachine` - for state machines that delegate to handlers

pub mod base;
pub mod builder;
pub mod handle;
pub mod handler_supervised;
pub(crate) mod idle_backoff;
pub mod self_supervised;

// Re-export main types
pub use base::EventLoopDirective;
pub use handler_supervised::{HandlerSupervised, HandlerSupervisedExt};
pub use self_supervised::{SelfSupervised, SelfSupervisedExt};

// Builder and handle infrastructure
pub use builder::{
    BuilderError, ChannelBuilder, EventReceiver, EventSender, HandleError, StateWatcher,
    SupervisorBuilder, SupervisorHandle,
};
pub use handle::{HandleBuilder, StandardHandle, SupervisorTaskBuilder};

// DON'T export Supervisor - force users to choose SelfSupervised or HandlerSupervised
