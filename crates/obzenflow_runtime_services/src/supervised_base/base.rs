//! Common base functionality for supervised state machines
//!
//! This module provides shared types and traits used by both self-supervised
//! and handler-supervised state machine implementations.

use obzenflow_fsm::{EventVariant, FsmAction, FsmContext, StateVariant};

/// Directives that control a state's event loop
#[derive(Debug, Clone)]
pub enum EventLoopDirective<E> {
    /// Continue running this state's event loop
    Continue,

    /// This state is done - transition via this event
    Transition(E),

    /// This state machine should terminate
    Terminate,
}

/// Private module to seal the Supervisor trait
pub mod private {
    pub trait Sealed {}
}

/// Base trait that all supervisors must implement
/// This enforces that every supervisor provides FSM building capabilities
/// 
/// This trait is sealed - it can ONLY be implemented by types that also implement
/// either SelfSupervised or HandlerSupervised. This prevents anyone from creating
/// a "supervisor" that bypasses FSM patterns.
pub trait Supervisor: private::Sealed {
    type State: StateVariant;
    type Event: EventVariant;
    type Context: FsmContext;
    type Action: FsmAction<Context = Self::Context>;

    /// Configure the FSM builder with state transitions
    /// Each supervisor must implement this to define its state transitions
    /// The actual .build() will be called by the framework
    fn configure_fsm(
        &self,
        builder: obzenflow_fsm::FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action>,
    ) -> obzenflow_fsm::FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action>;

    /// Get the name of this supervised component
    fn name(&self) -> &str;
}

