//! StageHandler implementation for transform handlers

use crate::stages::common::stage_handler::StageHandler;
use super::traits::TransformHandler;
use super::context::TransformContext;
use crate::stages::transform::{
    states::TransformState,
    events::TransformEvent,
    actions::TransformAction,
    fsm::build_transform_fsm,
};
use obzenflow_fsm::StateMachine;

/// Wrapper for transform handlers to implement StageHandler
/// 
/// This gives every TransformHandler implementation:
/// - TransformState as its state type (no WaitingForGun!)
/// - TransformEvent as its event type (no Start!)
/// - TransformAction as its action type
pub struct TransformWrapper<H: TransformHandler>(pub H);

impl<H> StageHandler for TransformWrapper<H>
where 
    H: TransformHandler + 'static
{
    type State = TransformState;
    type Event = TransformEvent;
    type Action = TransformAction;
    type Context = TransformContext<H>;
    
    fn build_fsm() -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        build_transform_fsm::<H>()
    }
    
    fn initial_state(&self) -> Self::State {
        TransformState::Created
    }
}