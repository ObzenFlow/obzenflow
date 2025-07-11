//! Self-supervised state machine implementation
//!
//! This module provides supervision for state machines that contain their own logic,
//! such as the metrics aggregator and pipeline supervisor.

use super::base::{EventLoopDirective, Supervisor};
use obzenflow_fsm::{FsmAction, StateVariant};
use std::sync::Arc;

/// Trait that self-supervised components MUST implement
/// This ensures they provide all required functionality
/// 
/// By implementing SelfSupervised, you get:
/// - Enforced implementation of build_state_machine()
/// - Enforced implementation of dispatch_state()
/// - Free run() method via SelfSupervisedExt
/// - No way to bypass the supervised pattern
#[async_trait::async_trait]
pub trait SelfSupervised: Supervisor {
    /// Run the state dispatch logic for the current state
    /// Returns a directive indicating what to do next
    async fn dispatch_state(
        &mut self,
        state: &Self::State,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Extension trait to add run functionality to any SelfSupervised type
pub trait SelfSupervisedExt: SelfSupervised {
    /// Run the supervision loop
    async fn run(
        mut self,
        initial_state: Self::State,
        context: Self::Context,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        Self: Sized,
        Self::State: Send + Sync + 'static,
        Self::Event: Send + Sync + 'static,
        Self::Context: 'static,
        Self::Action: 'static,
    {
        let context = Arc::new(context);
        
        // Create the builder and let the supervisor configure it
        let builder = obzenflow_fsm::FsmBuilder::new(initial_state);
        let configured_builder = self.configure_fsm(builder);
        
        // Build the state machine
        let mut machine = configured_builder.build();

        loop {
            // Get current state
            let current_state = machine.state().clone();

            // Get directive from the supervisor's dispatch logic
            let directive = self.dispatch_state(&current_state).await?;

            match directive {
                EventLoopDirective::Continue => continue,

                EventLoopDirective::Transition(event) => {
                    let actions = machine
                        .handle(event, context.clone())
                        .await
                        .map_err(|e| format!("FSM error: {}", e))?;

                    for action in actions {
                        action
                            .execute(&context)
                            .await
                            .map_err(|e| format!("Action error: {}", e))?;
                    }
                }

                EventLoopDirective::Terminate => {
                    self.write_completion_event(machine.state().variant_name())
                        .await?;
                    break;
                }
            }
        }

        Ok(())
    }
}

// Blanket implementation - any type that implements SelfSupervised gets run() for free
impl<T: SelfSupervised> SelfSupervisedExt for T {}

// Seal implementation - any type that implements SelfSupervised can be a Supervisor
impl<T: SelfSupervised> super::base::private::Sealed for T {}

