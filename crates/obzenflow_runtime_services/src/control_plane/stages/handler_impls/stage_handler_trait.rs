//! StageHandler trait that ties handlers to their FSM types
//!
//! This trait ensures compile-time safety: each handler type
//! automatically gets the correct FSM states, events, and actions.

use async_trait::async_trait;
use obzenflow_fsm::StateMachine;

/// Base trait that connects stage handlers to their FSM types
/// 
/// By implementing this trait for each handler type (FiniteSourceHandler,
/// TransformHandler, etc.), we ensure that:
/// 
/// 1. Sources get SourceState/SourceEvent/SourceAction
/// 2. Transforms get TransformState/TransformEvent/TransformAction
/// 3. The compiler prevents mixing incompatible types
#[async_trait]
pub trait StageHandler: Send + Sync {
    /// The state type for this handler's FSM
    type State: Clone + Send + Sync + 'static;
    
    /// The event type for this handler's FSM
    type Event: Clone + Send + Sync + 'static;
    
    /// The action type for this handler's FSM
    type Action: Clone + Send + Sync + 'static;
    
    /// Context type for the FSM (contains handler and runtime resources)
    type Context: Send + Sync + 'static;
    
    /// Build the FSM for this handler type
    /// 
    /// This is called once during stage creation to construct
    /// the appropriate FSM for the handler type.
    fn build_fsm() -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action>;
    
    /// Get the initial state for this handler
    fn initial_state(&self) -> Self::State;
}