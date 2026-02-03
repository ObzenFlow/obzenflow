// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Builder for stateful stages

use std::sync::Arc;

use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::control_strategies::{ControlEventStrategy, JonestownStrategy};
use crate::stages::common::handlers::StatefulHandler;
use crate::stages::resources_builder::StageResources;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{
    BuilderError, ChannelBuilder, EventLoopDirective, EventReceiver, HandleBuilder,
    HandlerSupervised, HandlerSupervisedExt, StateWatcher, SupervisorBuilder,
    SupervisorTaskBuilder,
};

use super::config::StatefulConfig;
use super::fsm::{StatefulAction, StatefulContext, StatefulEvent, StatefulState};
use super::handle::StatefulHandle;
use super::supervisor::StatefulSupervisor;

/// Builder for creating stateful stages
pub struct StatefulBuilder<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> {
    handler: H,
    config: StatefulConfig,
    resources: StageResources,
    instrumentation: Option<Arc<StageInstrumentation>>,
}

impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StatefulBuilder<H> {
    /// Create a new stateful builder with StageResources
    pub fn new(handler: H, config: StatefulConfig, resources: StageResources) -> Self {
        Self {
            handler,
            config,
            resources,
            instrumentation: None,
        }
    }

    /// Set the instrumentation for this stateful stage
    pub fn with_instrumentation(mut self, instrumentation: Arc<StageInstrumentation>) -> Self {
        self.instrumentation = Some(instrumentation);
        self
    }

    /// Set a custom control strategy (defaults to JonestownStrategy)
    pub fn with_control_strategy(mut self, strategy: Arc<dyn ControlEventStrategy>) -> Self {
        self.config.control_strategy = Some(strategy);
        self
    }
}

#[async_trait::async_trait]
impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> SupervisorBuilder
    for StatefulBuilder<H>
{
    type Handle = StatefulHandle<H>;
    type Error = BuilderError;

    async fn build(self) -> Result<Self::Handle, Self::Error> {
        // Create channels for supervisor communication
        let (event_sender, event_receiver, state_watcher) =
            ChannelBuilder::new().build(StatefulState::<H>::Created);

        // Use provided strategy or default to JonestownStrategy
        let control_strategy = self
            .config
            .control_strategy
            .unwrap_or_else(|| Arc::new(JonestownStrategy));

        // Create instrumentation if not provided
        let instrumentation = self
            .instrumentation
            .unwrap_or_else(|| Arc::new(StageInstrumentation::new()));

        // Create context with subscription factory from resources
        let handler = self.handler;
        let initial_state = handler.initial_state();
        let context = StatefulContext {
            handler: Arc::new(handler),
            stage_id: self.config.stage_id,
            stage_name: self.config.stage_name.clone(),
            flow_name: self.config.flow_name.clone(),
            flow_id: self.resources.flow_id,
            current_state: initial_state,
            data_journal: self.resources.data_journal.clone(),
            error_journal: self.resources.error_journal.clone(),
            system_journal: self.resources.system_journal.clone(),
            bus: self.resources.message_bus.clone(),
            writer_id: None,
            subscription: None,
            contract_state: Vec::new(),
            control_strategy,
            buffered_eof: None,
            last_consumed_envelope: None,
            instrumentation,
            upstream_subscription_factory: self.resources.upstream_subscription_factory,
            events_since_last_heartbeat: 0,
            last_data_event_time: None,
            emit_interval: self.config.emit_interval,
            backpressure_writer: self.resources.backpressure_writer.clone(),
            backpressure_readers: self.resources.backpressure_readers.clone(),
            pending_outputs: std::collections::VecDeque::new(),
            pending_transition: None,
            pending_ack_upstream: None,
            backpressure_pulse:
                crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse::new(),
            backpressure_backoff:
                crate::supervised_base::idle_backoff::IdleBackoff::exponential_with_cap(
                    std::time::Duration::from_millis(1),
                    std::time::Duration::from_millis(50),
                ),
        };

        // Create supervisor (private - not exposed)
        let supervisor = StatefulSupervisor {
            name: format!("stateful_{}", self.config.stage_name),
            system_journal: self.resources.system_journal.clone(),
            stage_id: self.config.stage_id,
            _marker: std::marker::PhantomData,
        };

        // Clone what we need for the task
        let state_watcher_for_task = state_watcher.clone();

        // Spawn the supervisor task
        let supervisor_name = format!("stateful_{}", self.config.stage_name);
        let task = SupervisorTaskBuilder::<StatefulSupervisor<H>>::new(&supervisor_name).spawn(
            move || async move {
                // Create a wrapper that handles external events
                let supervisor_with_events = HandlerSupervisedWithExternalEvents {
                    supervisor,
                    external_events: event_receiver,
                    state_watcher: state_watcher_for_task,
                    last_state: None,
                };

                // Run with the wrapper
                HandlerSupervisedExt::run(
                    supervisor_with_events,
                    StatefulState::<H>::Created,
                    context,
                )
                .await
            },
        );

        // Build and return handle
        HandleBuilder::new()
            .with_event_sender(event_sender)
            .with_state_watcher(state_watcher)
            .with_supervisor_task(task)
            .build_standard()
            .map_err(|e| BuilderError::Other(e.to_string()))
    }
}

/// Internal wrapper that bridges external events with the handler-supervised supervisor
struct HandlerSupervisedWithExternalEvents<
    H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
    supervisor: StatefulSupervisor<H>,
    external_events: EventReceiver<StatefulEvent<H>>,
    state_watcher: StateWatcher<StatefulState<H>>,
    last_state: Option<StatefulState<H>>,
}

// Delegate trait implementations to the inner supervisor
impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor
    for HandlerSupervisedWithExternalEvents<H>
{
    type State = StatefulState<H>;
    type Event = StatefulEvent<H>;
    type Context = StatefulContext<H>;
    type Action = StatefulAction<H>;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> obzenflow_fsm::StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        // Delegate to the inner supervisor so we reuse its FSM definition
        // (currently expressed via the typed `fsm!` DSL) instead of
        // constructing a separate builder here.
        self.supervisor.build_state_machine(initial_state)
    }

    fn name(&self) -> &str {
        self.supervisor.name()
    }
}

// Implement Sealed for the wrapper
impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    crate::supervised_base::base::private::Sealed for HandlerSupervisedWithExternalEvents<H>
{
}

#[async_trait::async_trait]
impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised
    for HandlerSupervisedWithExternalEvents<H>
{
    type Handler = H;

    fn writer_id(&self) -> obzenflow_core::WriterId {
        self.supervisor.writer_id()
    }

    fn stage_id(&self) -> obzenflow_core::StageId {
        self.supervisor.stage_id()
    }

    fn event_for_action_error(&self, msg: String) -> StatefulEvent<H> {
        self.supervisor.event_for_action_error(msg)
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.supervisor.write_completion_event().await
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        context: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        // Update state for external observers only when it changes (FLOWIP-086i).
        if self.last_state.as_ref() != Some(state) {
            let new_state = state.clone();
            let _ = self.state_watcher.update(new_state.clone());
            self.last_state = Some(new_state);
        }

        // Created is a pure "wait for Initialize" state; block on control events to avoid spin.
        if matches!(state, StatefulState::Created) {
            match self.external_events.recv().await {
                Some(event) => return Ok(EventLoopDirective::Transition(event)),
                None => {
                    if !matches!(state, StatefulState::Failed(_)) {
                        return Ok(EventLoopDirective::Transition(StatefulEvent::Error(
                            "External control channel closed".to_string(),
                        )));
                    }
                }
            }
        }

        // Check for external events first
        match self.external_events.try_recv() {
            Ok(event) => {
                // Got an external event, transition to handle it
                return Ok(EventLoopDirective::Transition(event));
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                // No external events, proceed with normal dispatch
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                // Channel closed, initiate shutdown only if not already failed
                if !matches!(state, StatefulState::Failed(_)) {
                    return Ok(EventLoopDirective::Transition(StatefulEvent::Error(
                        "External control channel closed".to_string(),
                    )));
                }
            }
        }

        // Delegate to the actual supervisor
        self.supervisor.dispatch_state(state, context).await
    }
}
