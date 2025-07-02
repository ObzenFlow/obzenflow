//! FSM builder for source stages
//!
//! Constructs the state machine for sources with their unique
//! WaitingForGun state and Start event handling.

use std::sync::Arc;
use obzenflow_fsm::{FsmBuilder, StateMachine, Transition, StateVariant};
use crate::stages::common::handlers::{FiniteSourceHandler, InfiniteSourceHandler, FiniteSourceContext, InfiniteSourceContext};
use super::{
    states::SourceState,
    events::SourceEvent,
    actions::SourceAction,
};

/// Build FSM for finite source handlers
pub fn build_finite_source_fsm<H: FiniteSourceHandler + 'static>() 
    -> StateMachine<SourceState, SourceEvent, FiniteSourceContext<H>, SourceAction> 
{
    FsmBuilder::new(SourceState::Created)
        // Created -> Initialized
        .when("Created")
            .on("Initialize", |_state, _event: &SourceEvent, _ctx| async move {
                Ok(Transition {
                    next_state: SourceState::Initialized,
                    actions: vec![
                        SourceAction::AllocateResources,
                    ],
                })
            })
            .done()
        
        // Initialized -> WaitingForGun
        .when("Initialized")
            .on("Ready", |_state, _event, _ctx| async move {
                Ok(Transition {
                    next_state: SourceState::WaitingForGun,
                    actions: vec![],  // Sources wait quietly
                })
            })
            .done()
        
        // WaitingForGun -> Running (THE KEY DIFFERENCE!)
        .when("WaitingForGun")
            .on("Start", |_state, _event, ctx: Arc<FiniteSourceContext<H>>| async move {
                // Enable emission
                *ctx.can_emit.write().await = true;
                
                Ok(Transition {
                    next_state: SourceState::Running,
                    actions: vec![
                        SourceAction::StartEmitting,
                    ],
                })
            })
            .done()
        
        // Running -> Drained (directly after sending EOF)
        .when("Running")
            .on("Completed", |_state, _event, ctx: Arc<FiniteSourceContext<H>>| async move {
                tracing::info!("[{}] Source completed naturally", ctx.stage_name);
                
                // Source will write EOF and completion event, then terminate
                Ok(Transition {
                    next_state: SourceState::Drained,
                    actions: vec![
                        SourceAction::SendEOF,
                        SourceAction::Cleanup,
                    ],
                })
            })
            .on("BeginDrain", |_state, _event, ctx| async move {
                tracing::info!("[{}] Source beginning drain", ctx.stage_name);
                
                Ok(Transition {
                    next_state: SourceState::Drained,
                    actions: vec![
                        SourceAction::SendEOF,
                        SourceAction::Cleanup,
                    ],
                })
            })
            .done()
        
        // Error handling from any state
        .from_any()
            .on("Error", |state, event, ctx: Arc<FiniteSourceContext<H>>| {
                let state_name = state.variant_name().to_string();
                let event_clone = event.clone();
                async move {
                    if let SourceEvent::Error(ref msg) = event_clone {
                        tracing::error!("[{}] Source error in state {}: {}", 
                            ctx.stage_name, state_name, msg);
                        
                        Ok(Transition {
                            next_state: SourceState::Failed(msg.clone()),
                            actions: vec![
                                SourceAction::SendEOF,
                                SourceAction::Cleanup,
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

/// Build FSM for infinite source handlers
pub fn build_infinite_source_fsm<H: InfiniteSourceHandler + 'static>() 
    -> StateMachine<SourceState, SourceEvent, InfiniteSourceContext<H>, SourceAction> 
{
    // Very similar to finite source, but no Completed event
    FsmBuilder::new(SourceState::Created)
        // Created -> Initialized
        .when("Created")
            .on("Initialize", |_state, _event: &SourceEvent, _ctx| async move {
                Ok(Transition {
                    next_state: SourceState::Initialized,
                    actions: vec![
                        SourceAction::AllocateResources,
                    ],
                })
            })
            .done()
        
        // Initialized -> WaitingForGun
        .when("Initialized")
            .on("Ready", |_state, _event, _ctx| async move {
                Ok(Transition {
                    next_state: SourceState::WaitingForGun,
                    actions: vec![],
                })
            })
            .done()
        
        // WaitingForGun -> Running
        .when("WaitingForGun")
            .on("Start", |_state, _event, ctx: Arc<InfiniteSourceContext<H>>| async move {
                *ctx.can_emit.write().await = true;
                
                Ok(Transition {
                    next_state: SourceState::Running,
                    actions: vec![
                        SourceAction::StartEmitting,
                    ],
                })
            })
            .done()
        
        // Running -> Drained (only via shutdown)
        .when("Running")
            .on("BeginDrain", |_state, _event, ctx: Arc<InfiniteSourceContext<H>>| async move {
                tracing::info!("[{}] Infinite source beginning drain", ctx.stage_name);
                *ctx.shutdown_requested.write().await = true;
                
                // Processing loop will exit, write EOF and completion event
                Ok(Transition {
                    next_state: SourceState::Drained,
                    actions: vec![
                        SourceAction::SendEOF,
                        SourceAction::Cleanup,
                    ],
                })
            })
            .done()
        
        // Error handling
        .from_any()
            .on("Error", |state, event, ctx: Arc<InfiniteSourceContext<H>>| {
                let state_name = state.variant_name().to_string();
                let event_clone = event.clone();
                async move {
                    if let SourceEvent::Error(ref msg) = event_clone {
                        tracing::error!("[{}] Source error in state {}: {}", 
                            ctx.stage_name, state_name, msg);
                        
                        Ok(Transition {
                            next_state: SourceState::Failed(msg.clone()),
                            actions: vec![
                                SourceAction::SendEOF,
                                SourceAction::Cleanup,
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