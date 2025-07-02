//! StageHandler implementations for source handlers
//!
//! This automatically gives all FiniteSourceHandler and InfiniteSourceHandler
//! implementations the correct FSM types.

use crate::stages::common::stage_handler::StageHandler;
use super::traits::{FiniteSourceHandler, InfiniteSourceHandler};
use super::context::{FiniteSourceContext, InfiniteSourceContext};
use crate::stages::source::{
    states::SourceState,
    events::SourceEvent,
    actions::SourceAction,
    fsm::{build_finite_source_fsm, build_infinite_source_fsm},
};
use obzenflow_fsm::StateMachine;

/// Wrapper for finite source handlers to implement StageHandler
pub struct FiniteSourceWrapper<H: FiniteSourceHandler>(pub H);

impl<H> StageHandler for FiniteSourceWrapper<H>
where 
    H: FiniteSourceHandler + 'static
{
    type State = SourceState;
    type Event = SourceEvent;
    type Action = SourceAction;
    type Context = FiniteSourceContext<H>;
    
    fn build_fsm() -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        build_finite_source_fsm::<H>()
    }
    
    fn initial_state(&self) -> Self::State {
        SourceState::Created
    }
}

/// Wrapper for infinite source handlers to implement StageHandler
pub struct InfiniteSourceWrapper<H: InfiniteSourceHandler>(pub H);

impl<H> StageHandler for InfiniteSourceWrapper<H>
where 
    H: InfiniteSourceHandler + 'static
{
    type State = SourceState;
    type Event = SourceEvent;
    type Action = SourceAction;
    type Context = InfiniteSourceContext<H>;
    
    fn build_fsm() -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        build_infinite_source_fsm::<H>()
    }
    
    fn initial_state(&self) -> Self::State {
        SourceState::Created
    }
}