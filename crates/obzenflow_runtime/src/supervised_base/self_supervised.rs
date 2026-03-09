// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Self-supervised state machine implementation
//!
//! This module provides supervision for state machines that contain their own logic,
//! such as the metrics aggregator and pipeline supervisor.

use super::base::{EventLoopDirective, Supervisor};
use obzenflow_core::event::WriterId;
use obzenflow_fsm::{FsmAction, StateVariant};

/// Trait that self-supervised components MUST implement
/// This ensures they provide all required functionality
///
/// By implementing SelfSupervised, you get:
/// - Enforced implementation of build_state_machine()
/// - Enforced implementation of dispatch_state()
/// - Free run() method via SelfSupervisedExt
/// - No way to bypass the supervised pattern
#[async_trait::async_trait]
pub trait SelfSupervised: Supervisor + Sync {
    /// Run the state dispatch logic for the current state
    /// Returns a directive indicating what to do next
    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        context: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>>;

    /// Get the writer ID for this component
    fn writer_id(&self) -> WriterId;

    /// Optional termination hook for components that need a final marker after the
    /// FSM reaches a terminal state.
    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    /// Map an action error into a supervisor-specific failure event.
    ///
    /// This is used by the supervision loop to ensure that any action
    /// failure drives the FSM through an explicit failure path instead
    /// of terminating the task with an opaque error.
    fn event_for_action_error(&self, msg: String) -> Self::Event;
}

/// Extension trait to add run functionality to any SelfSupervised type
#[async_trait::async_trait]
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
        let supervisor_name = self.name().to_string();
        let supervisor_writer = self.writer_id();
        tracing::info!(
            supervisor = %supervisor_name,
            writer_id = ?supervisor_writer,
            "SelfSupervised::run() starting with initial state: {:?}",
            initial_state
        );
        let mut context = context;

        // Build the state machine via the Supervisor API
        tracing::debug!(
            supervisor = %supervisor_name,
            "Building FSM via Supervisor::build_state_machine"
        );
        let mut machine = self.build_state_machine(initial_state);

        let mut iteration = 0;
        loop {
            iteration += 1;

            // Get current state
            let current_state = machine.state().clone();
            // tracing::debug!("Loop iteration {}: Current state: {:?}", iteration, current_state);

            // Get directive from the supervisor's dispatch logic, with full access to context
            tracing::trace!("Calling dispatch_state for state: {:?}", current_state);
            let directive = match self.dispatch_state(&current_state, &mut context).await {
                Ok(d) => d,
                Err(e) => {
                    tracing::error!(
                        supervisor = %supervisor_name,
                        writer_id = ?supervisor_writer,
                        state = %current_state.variant_name(),
                        error = %e,
                        "dispatch_state returned error; driving FSM through failure path"
                    );

                    let failure_event = self.event_for_action_error(format!(
                        "dispatch_state error in {}: {e}",
                        current_state.variant_name()
                    ));
                    let failure_actions = machine
                        .handle(failure_event, &mut context)
                        .await
                        .map_err(|fe| format!("FSM error after dispatch_state failure: {fe}"))?;

                    tracing::debug!(
                        supervisor = %supervisor_name,
                        writer_id = ?supervisor_writer,
                        iteration,
                        failure_action_count = failure_actions.len(),
                        "Loop iteration {}: Executing {} failure-handling actions",
                        iteration,
                        failure_actions.len()
                    );
                    for (i, failure_action) in failure_actions.into_iter().enumerate() {
                        tracing::debug!(
                            supervisor = %supervisor_name,
                            writer_id = ?supervisor_writer,
                            iteration,
                            failure_action_index = i,
                            action = ?failure_action,
                            "Executing failure-handling action"
                        );
                        failure_action.execute(&mut context).await.map_err(|e2| {
                            format!("Action error during dispatch_state failure handling: {e2}")
                        })?;
                    }

                    continue;
                }
            };
            tracing::debug!(
                supervisor = %supervisor_name,
                writer_id = ?supervisor_writer,
                "Loop iteration {}: dispatch_state returned: {:?}",
                iteration,
                directive
            );

            match directive {
                EventLoopDirective::Continue => {
                    tracing::trace!(
                        supervisor = %supervisor_name,
                        writer_id = ?supervisor_writer,
                        "Loop iteration {}: Got Continue directive, continuing loop",
                        iteration
                    );
                    // Yield to prevent busy loop when waiting for external events
                    tokio::task::yield_now().await;
                    continue;
                }

                EventLoopDirective::Transition(event) => {
                    tracing::debug!(
                        supervisor = %supervisor_name,
                        writer_id = ?supervisor_writer,
                        "Loop iteration {}: Transitioning with event: {:?}",
                        iteration,
                        event
                    );
                    let actions = machine
                        .handle(event, &mut context)
                        .await
                        .map_err(|e| format!("FSM error: {e}"))?;

                    tracing::debug!(
                        supervisor = %supervisor_name,
                        writer_id = ?supervisor_writer,
                        "Loop iteration {}: Got {} actions to execute",
                        iteration,
                        actions.len()
                    );
                    for (i, action) in actions.iter().enumerate() {
                        tracing::debug!(
                            supervisor = %supervisor_name,
                            writer_id = ?supervisor_writer,
                            "Loop iteration {}: Executing action {}/{}: {:?}",
                            iteration,
                            i + 1,
                            actions.len(),
                            action
                        );
                        if let Err(e) = action.execute(&mut context).await {
                            tracing::error!(
                                supervisor = %supervisor_name,
                                writer_id = ?supervisor_writer,
                                iteration,
                                action_index = i,
                                error = %e,
                                "SelfSupervised action failed; emitting failure event"
                            );

                            // Drive the FSM with a supervisor-specific failure event so
                            // that it can transition into a Failed state and emit the
                            // appropriate lifecycle / coordination events.
                            let failure_event = self.event_for_action_error(format!("{e}"));
                            let failure_actions = machine
                                .handle(failure_event, &mut context)
                                .await
                                .map_err(|fe| format!("FSM error after action failure: {fe}"))?;

                            tracing::debug!(
                                supervisor = %supervisor_name,
                                writer_id = ?supervisor_writer,
                                iteration,
                                failure_action_count = failure_actions.len(),
                                "Loop iteration {}: Executing {} failure-handling actions",
                                iteration,
                                failure_actions.len()
                            );
                            for (j, failure_action) in failure_actions.iter().enumerate() {
                                tracing::debug!(
                                    supervisor = %supervisor_name,
                                    writer_id = ?supervisor_writer,
                                    iteration,
                                    failure_action_index = j,
                                    action = ?failure_action,
                                    "Executing failure-handling action"
                                );
                                failure_action.execute(&mut context).await.map_err(|e2| {
                                    format!("Action error during failure handling: {e2}")
                                })?;
                            }

                            // After executing failure-handling actions, break out of the
                            // current action sequence. The next loop iteration will see
                            // the new FSM state (typically Failed/Drained) and perform
                            // the appropriate terminal behaviour.
                            break;
                        }
                        tracing::debug!(
                            supervisor = %supervisor_name,
                            writer_id = ?supervisor_writer,
                            "Loop iteration {}: Action {}/{} completed",
                            iteration,
                            i + 1,
                            actions.len()
                        );
                    }
                    tracing::debug!(
                        supervisor = %supervisor_name,
                        writer_id = ?supervisor_writer,
                        "Loop iteration {}: Transition complete, new state: {:?}",
                        iteration,
                        machine.state()
                    );
                }

                EventLoopDirective::Terminate => {
                    tracing::info!(
                        supervisor = %supervisor_name,
                        writer_id = ?supervisor_writer,
                        "Loop iteration {}: Got Terminate directive",
                        iteration
                    );
                    self.write_completion_event().await?;
                    break;
                }
            }
        }

        tracing::info!(
            supervisor = %supervisor_name,
            writer_id = ?supervisor_writer,
            "SelfSupervised::run() completed"
        );
        Ok(())
    }
}

// Blanket implementation - any type that implements SelfSupervised gets run() for free
impl<T: SelfSupervised> SelfSupervisedExt for T {}
