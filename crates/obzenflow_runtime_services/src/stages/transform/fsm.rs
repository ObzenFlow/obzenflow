//! FSM builder for transform stages
//!
//! Constructs the state machine for transforms that start processing
//! immediately without waiting for a start signal.

use std::sync::Arc;
use obzenflow_fsm::{FsmBuilder, StateMachine, Transition, StateVariant};
use crate::stages::common::handlers::{TransformHandler, TransformContext};
use super::{
    states::TransformState,
    events::TransformEvent,
    actions::TransformAction,
};

/// Build FSM for transform handlers
pub fn build_transform_fsm<H: TransformHandler + 'static>() 
    -> StateMachine<TransformState, TransformEvent, TransformContext<H>, TransformAction> 
{
    FsmBuilder::new(TransformState::Created)
        // Created -> Initialized
        .when("Created")
            .on("Initialize", |_state, _event: &TransformEvent, _ctx| async move {
                Ok(Transition {
                    next_state: TransformState::Initialized,
                    actions: vec![
                        TransformAction::AllocateResources,
                    ],
                })
            })
            .done()
        
        // Initialized -> Running (NO WAITING!)
        .when("Initialized")
            .on("Ready", |_state, _event, ctx: Arc<TransformContext<H>>| async move {
                tracing::info!("[{}] Transform starting immediately", ctx.stage_name);
                
                Ok(Transition {
                    next_state: TransformState::Running,
                    actions: vec![
                        TransformAction::StartProcessing,
                    ],
                })
            })
            .done()
        
        // Running -> Drained (directly - no more waiting for phantom events!)
        .when("Running")
            .on("ReceivedEOF", |_state, _event, ctx: Arc<TransformContext<H>>| async move {
                // Transforms immediately forward EOF and complete
                tracing::info!("[{}] Transform received EOF, forwarding and completing", 
                    ctx.stage_name);
                
                // The processing loop will exit after forwarding EOF and write completion event
                Ok(Transition {
                    next_state: TransformState::Drained,
                    actions: vec![
                        TransformAction::ForwardEOF,
                        TransformAction::Cleanup,
                    ],
                })
            })
            .on("BeginDrain", |_state, _event, ctx| async move {
                tracing::info!("[{}] Transform beginning drain", ctx.stage_name);
                
                // Force drain goes directly to Drained
                Ok(Transition {
                    next_state: TransformState::Drained,
                    actions: vec![
                        TransformAction::Cleanup,
                    ],
                })
            })
            .done()
        
        // Error handling
        .from_any()
            .on("Error", |state, event, ctx: Arc<TransformContext<H>>| {
                let state_name = state.variant_name().to_string();
                let event_clone = event.clone();
                async move {
                    if let TransformEvent::Error(ref msg) = event_clone {
                        tracing::error!("[{}] Transform error in state {}: {}", 
                            ctx.stage_name, state_name, msg);
                        
                        Ok(Transition {
                            next_state: TransformState::Failed(msg.clone()),
                            actions: vec![
                                TransformAction::ForwardEOF,
                                TransformAction::Cleanup,
                            ],
                        })
                    } else {
                        Err("Invalid event".to_string())
                    }
                }
            })
            .done()
        
        .build()
}