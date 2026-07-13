// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Builder for stateful stages

use std::sync::Arc;

use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::control_strategies::{JonestownSignalStrategy, SignalGate};
use crate::stages::common::handlers::UnifiedStatefulHandler;
use crate::stages::common::heartbeat::{spawn_heartbeat, HeartbeatConfig, HeartbeatState};
use crate::stages::resources_builder::StageResources;
use crate::supervised_base::{
    BuilderError, ChannelBuilder, HandleBuilder, HandlerSupervisedExt,
    HandlerSupervisedWithExternalEvents, SupervisorBuilder, SupervisorTaskBuilder,
};

use super::config::StatefulConfig;
use super::fsm::{StatefulContext, StatefulState};
use super::handle::StatefulHandle;
use super::supervisor::StatefulSupervisor;

/// Builder for creating stateful stages
pub struct StatefulBuilder<
    H: UnifiedStatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
    handler: H,
    config: StatefulConfig,
    resources: StageResources,
    instrumentation: Option<Arc<StageInstrumentation>>,
    heartbeat_config: HeartbeatConfig,
}

impl<H: UnifiedStatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    StatefulBuilder<H>
{
    /// Create a new stateful builder with StageResources
    pub fn new(handler: H, config: StatefulConfig, resources: StageResources) -> Self {
        Self {
            handler,
            config,
            resources,
            instrumentation: None,
            heartbeat_config: HeartbeatConfig::default(),
        }
    }

    /// Set the instrumentation for this stateful stage
    pub fn with_instrumentation(mut self, instrumentation: Arc<StageInstrumentation>) -> Self {
        self.instrumentation = Some(instrumentation);
        self
    }

    /// Set a custom control strategy (defaults to JonestownSignalStrategy)
    pub fn with_control_strategy(mut self, strategy: Arc<dyn SignalGate>) -> Self {
        self.config.control_strategy = Some(strategy);
        self
    }

    pub fn with_heartbeat(mut self, heartbeat_config: HeartbeatConfig) -> Self {
        self.heartbeat_config = heartbeat_config;
        self
    }
}

#[async_trait::async_trait]
impl<H: UnifiedStatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> SupervisorBuilder
    for StatefulBuilder<H>
{
    type Handle = StatefulHandle<H>;
    type Error = BuilderError;

    async fn build(self) -> Result<Self::Handle, Self::Error> {
        // Create channels for supervisor communication
        let (event_sender, event_receiver, state_watcher) =
            ChannelBuilder::new().build(StatefulState::<H>::Created);

        // Use provided strategy or default to JonestownSignalStrategy
        let control_strategy = self
            .config
            .control_strategy
            .unwrap_or_else(|| Arc::new(JonestownSignalStrategy));

        // Create instrumentation if not provided
        let instrumentation = self
            .instrumentation
            .unwrap_or_else(|| Arc::new(StageInstrumentation::new()));

        let heartbeat_config = self.heartbeat_config.clone();
        let heartbeat = if self
            .resources
            .runtime_execution
            .heartbeat_policy_for(self.config.stage_id)
            == crate::execution::HeartbeatExecutionPolicy::Suppressed
            || !heartbeat_config.enabled
        {
            None
        } else {
            let heartbeat_state = HeartbeatState::new(self.resources.upstream_stages.clone());
            Some(spawn_heartbeat(
                self.config.stage_id,
                self.config.stage_name.clone(),
                self.resources.system_journal.clone(),
                self.resources.liveness_snapshots.clone(),
                heartbeat_state,
                heartbeat_config,
                self.resources.runtime_execution.clone(),
            ))
        };

        // Create context with subscription factory from resources.
        // FLOWIP-010 §7: handler meets resources here; install the
        // build-resolved lineage policy before the handler is shared.
        let mut handler = self.handler;
        handler.install_lineage_policy(self.resources.lineage_policy);
        let emit_interval = self
            .config
            .emit_interval
            .or_else(|| handler.emit_interval_hint());
        let initial_state = handler.initial_state();
        let (boundary_stop_controller, boundary_stop) =
            crate::stages::common::boundary_stop_channel();
        let context = StatefulContext {
            handler: Arc::new(handler),
            stage_id: self.config.stage_id,
            stage_name: self.config.stage_name.clone(),
            observers: self.config.observers.clone(),
            flow_name: self.config.flow_name.clone(),
            flow_id: self.resources.flow_id,
            current_state: initial_state,
            data_journal: self.resources.data_journal.clone(),
            effect_history: None,
            runtime_execution: self.resources.runtime_execution.clone(),
            boundary_stop_controller,
            boundary_stop,
            effect_ports: self.resources.effect_ports.clone(),
            effect_declarations: self.resources.effect_declarations.clone(),
            last_input_position: None,
            error_journal: self.resources.error_journal.clone(),
            system_journal: self.resources.system_journal.clone(),
            bus: self.resources.message_bus.clone(),
            writer_id: None,
            lineage_policy: self.resources.lineage_policy,
            subscription: None,
            contract_state: Vec::new(),
            last_contract_check: None,
            control_strategy,
            processing_context:
                crate::stages::common::control_strategies::ProcessingContext::default(),
            buffered_eof: None,
            terminal_eof_kind: None,
            last_consumed_envelope: None,
            instrumentation,
            upstream_subscription_factory: self.resources.upstream_subscription_factory,
            events_since_last_heartbeat: 0,
            heartbeat_interval: self.resources.heartbeat_interval,
            last_data_event_time: None,
            emit_interval,
            backpressure_writer: self.resources.backpressure_writer.clone(),
            output_contract: self.resources.output_contract.clone(),
            backpressure_readers: self.resources.backpressure_readers.clone(),
            pending_outputs: std::collections::VecDeque::new(),
            pending_transition: None,
            pending_ack_upstream: None,
            backpressure_pulse:
                crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse::new(),
            backpressure_stall: None,
            heartbeat,
            catch_up_flip: None,
        };

        // Create supervisor (private - not exposed)
        let supervisor = StatefulSupervisor {
            name: format!("stateful_{}", self.config.stage_name),
            stage_id: self.config.stage_id,
            subscription: None,
            _marker: std::marker::PhantomData,
        };

        // Clone what we need for the task
        let state_watcher_for_task = state_watcher.clone();

        // Spawn the supervisor task
        let supervisor_name = format!("stateful_{}", self.config.stage_name);
        let task = SupervisorTaskBuilder::<StatefulSupervisor<H>>::new(&supervisor_name).spawn(
            move || async move {
                let supervisor_with_events = HandlerSupervisedWithExternalEvents::new(
                    supervisor,
                    event_receiver,
                    state_watcher_for_task,
                );

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
