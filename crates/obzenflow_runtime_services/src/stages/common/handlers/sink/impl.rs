//! StageHandler implementation for sink handlers

use crate::stages::common::stage_handler::StageHandler;
use super::traits::SinkHandler;
use super::context::SinkContext;
use crate::stages::sink::{
    states::SinkState,
    events::SinkEvent,
    actions::SinkAction,
    fsm::build_sink_fsm,
};
use obzenflow_fsm::StateMachine;

/// Wrapper for sink handlers to implement StageHandler
/// 
/// This gives every SinkHandler implementation:
/// - SinkState as its state type (with Flushing!)
/// - SinkEvent as its event type (with BeginFlush/FlushComplete!)
/// - SinkAction as its action type
pub struct SinkWrapper<H: SinkHandler>(pub H);

impl<H> StageHandler for SinkWrapper<H>
where 
    H: SinkHandler + 'static
{
    type State = SinkState;
    type Event = SinkEvent;
    type Action = SinkAction;
    type Context = SinkContext<H>;
    
    fn build_fsm() -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        build_sink_fsm::<H>()
    }
    
    fn initial_state(&self) -> Self::State {
        SinkState::Created
    }
}