// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Builder for journal sink stages

use std::sync::Arc;

use super::config::JournalSinkConfig;
use super::fsm::{JournalSinkContext, JournalSinkState};
use super::handle::JournalSinkHandle;
use super::supervisor::JournalSinkSupervisor;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::control_strategies::{ControlEventStrategy, JonestownStrategy};
use crate::stages::common::heartbeat::{spawn_heartbeat, HeartbeatConfig, HeartbeatState};
use crate::stages::common::handlers::SinkHandler;
use crate::stages::resources_builder::StageResources;
use crate::supervised_base::{
    BuilderError, ChannelBuilder, HandleBuilder, HandlerSupervisedExt,
    HandlerSupervisedWithExternalEvents, SupervisorBuilder, SupervisorTaskBuilder,
};

/// Builder for creating journal sink stages
pub struct JournalSinkBuilder<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> {
    handler: H,
    config: JournalSinkConfig,
    resources: StageResources,
    instrumentation: Option<Arc<StageInstrumentation>>,
    heartbeat_config: HeartbeatConfig,
}

impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> JournalSinkBuilder<H> {
    /// Create a new journal sink builder
    pub fn new(handler: H, config: JournalSinkConfig, resources: StageResources) -> Self {
        Self {
            handler,
            config,
            resources,
            instrumentation: None,
            heartbeat_config: HeartbeatConfig::default(),
        }
    }

    /// Set the instrumentation for this sink
    pub fn with_instrumentation(mut self, instrumentation: Arc<StageInstrumentation>) -> Self {
        self.instrumentation = Some(instrumentation);
        self
    }

    pub fn with_heartbeat(mut self, heartbeat_config: HeartbeatConfig) -> Self {
        self.heartbeat_config = heartbeat_config;
        self
    }
}

#[async_trait::async_trait]
impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> SupervisorBuilder
    for JournalSinkBuilder<H>
{
    type Handle = JournalSinkHandle<H>;
    type Error = BuilderError;

    async fn build(self) -> Result<Self::Handle, Self::Error> {
        // Create channels for supervisor communication
        let (event_sender, event_receiver, state_watcher) =
            ChannelBuilder::new().build(JournalSinkState::<H>::Created);

        // Create instrumentation if not provided
        let instrumentation = self
            .instrumentation
            .unwrap_or_else(|| Arc::new(StageInstrumentation::new()));

        // Create context
        let control_strategy: Arc<dyn ControlEventStrategy> = self
            .config
            .control_strategy
            .unwrap_or_else(|| Arc::new(JonestownStrategy));

        let heartbeat_config = self.heartbeat_config.clone();
        let heartbeat = if self.resources.replay_archive.is_some() || !heartbeat_config.enabled {
            None
        } else {
            let heartbeat_state = HeartbeatState::new(self.resources.upstream_stages.clone());
            Some(spawn_heartbeat(
                self.config.stage_id,
                self.config.stage_name.clone(),
                self.resources.system_journal.clone(),
                self.resources.liveness_registry.clone(),
                heartbeat_state,
                heartbeat_config,
                /* is_replay */ false,
            ))
        };

        let context = JournalSinkContext {
            handler: self.handler,
            stage_id: self.config.stage_id,
            stage_name: self.config.stage_name.clone(),
            flow_name: self.config.flow_name.clone(),
            flow_id: self.resources.flow_id,
            data_journal: self.resources.data_journal.clone(),
            error_journal: self.resources.error_journal.clone(),
            system_journal: self.resources.system_journal.clone(),
            bus: self.resources.message_bus.clone(),
            writer_id: None,
            subscription: None,
            contract_state: Vec::new(),
            last_contract_check: None,
            instrumentation,
            upstream_subscription_factory: self.resources.upstream_subscription_factory,
            control_strategy,
            backpressure_writer: self.resources.backpressure_writer.clone(),
            backpressure_readers: self.resources.backpressure_readers.clone(),
            heartbeat,
        };

        // Create supervisor (private - not exposed)
        let supervisor = JournalSinkSupervisor {
            name: format!("sink_{}", self.config.stage_name),
            stage_id: self.config.stage_id,
            subscription: None,
            _marker: std::marker::PhantomData,
        };

        // Clone what we need for the task
        let state_watcher_for_task = state_watcher.clone();

        // Spawn the supervisor task
        let supervisor_name = format!("sink_{}", self.config.stage_name);
        let task = SupervisorTaskBuilder::<JournalSinkSupervisor<H>>::new(&supervisor_name).spawn(
            move || async move {
                let supervisor_with_events = HandlerSupervisedWithExternalEvents::new(
                    supervisor,
                    event_receiver,
                    state_watcher_for_task,
                );

                // Run with the wrapper
                HandlerSupervisedExt::run(
                    supervisor_with_events,
                    JournalSinkState::<H>::Created,
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
