//! Handler-supervised state machine implementation
//!
//! This module provides supervision for state machines that delegate to handlers,
//! such as source, transform, and sink supervisors.

use super::base::{EventLoopDirective, Supervisor};
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::event::WriterId;
use obzenflow_core::{ChainEvent, StageId};
use obzenflow_fsm::FsmAction;
use obzenflow_fsm::StateVariant;
use tokio::task::JoinHandle;

/// Trait for handler-supervised components
/// This ensures they provide handler access while still going through FSM
#[async_trait::async_trait]
pub trait HandlerSupervised: Supervisor {
    type Handler: Send + Sync;

    /// Dispatch state logic with access to handler
    /// Similar to SelfSupervised but with handler access and mutable FSM context
    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        context: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>>;

    /// Get the writer ID for this component
    fn writer_id(&self) -> WriterId;

    /// Get the stage ID for this component
    fn stage_id(&self) -> StageId;

    /// Write a completion event when the stage terminates
    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Map an action error into a stage-specific failure event.
    ///
    /// This is used by the supervision loop to ensure that any action
    /// failure drives the FSM through an explicit failure path instead
    /// of terminating the task with an opaque error.
    fn event_for_action_error(&self, msg: String) -> Self::Event;

    /// Helper method to run a processing function only if the event doesn't have Error status
    /// If the event has Error status, it's passed through unchanged
    fn run_if_not_error<F>(&self, event: ChainEvent, next: F) -> Vec<ChainEvent>
    where
        F: FnOnce(ChainEvent) -> Vec<ChainEvent>,
    {
        if matches!(event.processing_info.status, ProcessingStatus::Error { .. }) {
            vec![event] // pass straight through
        } else {
            next(event)
        }
    }
}

/// Extension trait to add run functionality to any HandlerSupervised type
pub trait HandlerSupervisedExt: HandlerSupervised {
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
        let mut context = context;

        // Build the state machine via the Supervisor API
        let mut machine = self.build_state_machine(initial_state);
        let mut loop_iteration: u64 = 0;

        loop {
            loop_iteration += 1;

            // Get current state
            let current_state = machine.state().clone();

            tracing::debug!(
                target: "obzenflow_runtime_services::supervised_base::handler_supervised",
                iteration = loop_iteration,
                state = %current_state.variant_name(),
                "HandlerSupervised::run loop iteration start"
            );

            // Get directive from the supervisor's dispatch logic (with mutable context)
            let directive = self.dispatch_state(&current_state, &mut context).await?;

            tracing::debug!(
                target: "obzenflow_runtime_services::supervised_base::handler_supervised",
                iteration = loop_iteration,
                state = %current_state.variant_name(),
                directive = ?directive,
                "HandlerSupervised::run dispatch_state returned directive"
            );

            match directive {
                EventLoopDirective::Continue => {
                    // Yield to prevent busy loop when waiting for external events
                    tokio::task::yield_now().await;
                    continue;
                }

                EventLoopDirective::Transition(event) => {
                    tracing::info!(
                        target: "flowip-080o",
                        iteration = loop_iteration,
                        event = ?event,
                        "HandlerSupervised: handling FSM transition event"
                    );
                    let actions = machine
                        .handle(event, &mut context)
                        .await
                        .map_err(|e| format!("FSM error: {e}"))?;

                    tracing::info!(
                        target: "flowip-080o",
                        iteration = loop_iteration,
                        action_count = actions.len(),
                        "HandlerSupervised: FSM returned actions, executing sequentially"
                    );
                    for (i, action) in actions.into_iter().enumerate() {
                        tracing::info!(
                            target: "flowip-080o",
                            iteration = loop_iteration,
                            action_index = i,
                            action = ?action,
                            "HandlerSupervised: executing action"
                        );
                        if let Err(e) = action.execute(&mut context).await {
                            tracing::error!(
                                target: "flowip-080o",
                                iteration = loop_iteration,
                                action_index = i,
                                error = %e,
                                "HandlerSupervised action failed; emitting failure event"
                            );

                            // Drive the FSM with a stage-specific failure event so that
                            // it can transition into a Failed state and emit the
                            // appropriate lifecycle events.
                            let failure_event = self.event_for_action_error(format!("{e}"));
                            let failure_actions = machine
                                .handle(failure_event, &mut context)
                                .await
                                .map_err(|fe| {
                                    format!("FSM error after action failure: {fe}")
                                })?;

                            tracing::info!(
                                target: "flowip-080o",
                                iteration = loop_iteration,
                                action_count = failure_actions.len(),
                                "HandlerSupervised: executing failure-handling actions"
                            );
                            for (j, failure_action) in failure_actions.into_iter().enumerate() {
                                tracing::info!(
                                    target: "flowip-080o",
                                    iteration = loop_iteration,
                                    action_index = j,
                                    action = ?failure_action,
                                    "HandlerSupervised: executing failure-handling action"
                                );
                                failure_action
                                    .execute(&mut context)
                                    .await
                                    .map_err(|e2| {
                                        format!(
                                            "Action error during failure handling: {e2}"
                                        )
                                    })?;
                            }

                            // After executing failure-handling actions, break out of the
                            // current action sequence. The next loop iteration will see
                            // the new FSM state (typically Failed/Drained) and perform
                            // the appropriate terminal behaviour.
                            break;
                        }
                        tracing::info!(
                            target: "flowip-080o",
                            iteration = loop_iteration,
                            action_index = i,
                            "HandlerSupervised: action completed"
                        );
                    }
                    tracing::info!(
                        target: "flowip-080o",
                        iteration = loop_iteration,
                        "HandlerSupervised: all actions completed"
                    );
                }

                EventLoopDirective::Terminate => {
                    self.write_completion_event().await?;
                    break;
                }
            }
        }

        Ok(())
    }

    /// Helper to spawn a task and return the handle
    /// Useful for handler-based supervisors that need to spawn processing tasks
    async fn spawn_task<F>(future: F) -> JoinHandle<()>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(future)
    }

    /// Helper to cancel a task handle
    async fn cancel_task(handle: JoinHandle<()>) {
        handle.abort();
    }
}

// Blanket implementation - any type that implements HandlerSupervised gets run() for free
impl<T: HandlerSupervised> HandlerSupervisedExt for T {}
