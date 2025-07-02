//! FSM builder for sink stages
//!
//! Constructs the state machine for sinks with their unique
//! flushing state to ensure data durability.

use std::sync::Arc;
use obzenflow_fsm::{FsmBuilder, StateMachine, Transition, StateVariant};
use crate::stages::common::handlers::{SinkHandler, SinkContext};
use super::{
    states::SinkState,
    events::SinkEvent,
    actions::SinkAction,
};

/// Build FSM for sink handlers
pub fn build_sink_fsm<H: SinkHandler + 'static>() 
    -> StateMachine<SinkState, SinkEvent, SinkContext<H>, SinkAction> 
{
    FsmBuilder::new(SinkState::Created)
        // Created -> Initialized
        .when("Created")
            .on("Initialize", |_state, _event: &SinkEvent, _ctx| async move {
                Ok(Transition {
                    next_state: SinkState::Initialized,
                    actions: vec![
                        SinkAction::AllocateResources,
                    ],
                })
            })
            .done()
        
        // Initialized -> Running
        .when("Initialized")
            .on("Ready", |_state, _event, ctx: Arc<SinkContext<H>>| async move {
                tracing::info!("[{}] Sink starting consumption", ctx.stage_name);
                
                Ok(Transition {
                    next_state: SinkState::Running,
                    actions: vec![
                        SinkAction::StartConsuming,
                    ],
                })
            })
            .done()
        
        // Running -> Drained
        // Note: The processing loop already calls drain() when it receives EOF,
        // so we don't need a separate Flushing state or FlushBuffers action
        .when("Running")
            .on("ReceivedEOF", |_state, _event, ctx: Arc<SinkContext<H>>| async move {
                tracing::info!("[{}] Sink received EOF notification", ctx.stage_name);
                
                // Processing loop has already called drain() and will write completion
                Ok(Transition {
                    next_state: SinkState::Drained,
                    actions: vec![
                        SinkAction::Cleanup,
                    ],
                })
            })
            .on("BeginDrain", |_state, _event, ctx| async move {
                tracing::info!("[{}] Sink beginning forced drain", ctx.stage_name);
                
                // Force drain - need to flush explicitly
                Ok(Transition {
                    next_state: SinkState::Drained,
                    actions: vec![
                        SinkAction::FlushBuffers,
                        SinkAction::Cleanup,
                    ],
                })
            })
            .done()
        
        // Error handling
        .from_any()
            .on("Error", |state, event, ctx: Arc<SinkContext<H>>| {
                let state_name = state.variant_name().to_string();
                let event_clone = event.clone();
                async move {
                    if let SinkEvent::Error(ref msg) = event_clone {
                        tracing::error!("[{}] Sink error in state {}: {}", 
                            ctx.stage_name, state_name, msg);
                        
                        // Try to flush even on error
                        let mut actions = vec![SinkAction::FlushBuffers];
                        
                        actions.extend(vec![
                            SinkAction::Cleanup,
                        ]);
                        
                        Ok(Transition {
                            next_state: SinkState::Failed(msg.clone()),
                            actions,
                        })
                    } else {
                        Err("Invalid event".to_string())
                    }
                }
            })
            .done()
        
        .build()
}