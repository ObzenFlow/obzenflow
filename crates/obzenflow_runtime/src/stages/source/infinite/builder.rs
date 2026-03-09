// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Builder for infinite source stages

use std::sync::Arc;
use std::time::Duration;

use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::handlers::InfiniteSourceHandler;
use crate::stages::resources_builder::StageResources;
use crate::stages::source::replay_lifecycle::ReplayCompletionGuard;
use crate::stages::source::strategies::{JonestownSourceStrategy, SourceControlStrategy};
use crate::supervised_base::{
    BuilderError, ChannelBuilder, HandleBuilder, HandlerSupervisedExt,
    HandlerSupervisedWithExternalEvents, SupervisorBuilder, SupervisorTaskBuilder,
};
use obzenflow_core::WriterId;

use super::config::InfiniteSourceConfig;
use super::fsm::{InfiniteSourceContext, InfiniteSourceContextInit, InfiniteSourceState};
use super::handle::InfiniteSourceHandle;
use super::supervisor::InfiniteSourceSupervisor;

/// Builder for creating infinite source stages
pub struct InfiniteSourceBuilder<
    H: InfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
    handler: H,
    config: InfiniteSourceConfig,
    resources: StageResources,
    instrumentation: Option<Arc<StageInstrumentation>>,
}

impl<H: InfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    InfiniteSourceBuilder<H>
{
    /// Create a new infinite source builder
    pub fn new(handler: H, config: InfiniteSourceConfig, resources: StageResources) -> Self {
        Self {
            handler,
            config,
            resources,
            instrumentation: None,
        }
    }

    /// Set the instrumentation for this source
    pub fn with_instrumentation(mut self, instrumentation: Arc<StageInstrumentation>) -> Self {
        self.instrumentation = Some(instrumentation);
        self
    }

    /// Set a custom source control strategy (defaults to JonestownSourceStrategy)
    pub fn with_control_strategy(mut self, strategy: Arc<dyn SourceControlStrategy>) -> Self {
        self.config.control_strategy = Some(strategy);
        self
    }
}

#[async_trait::async_trait]
impl<H: InfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> SupervisorBuilder
    for InfiniteSourceBuilder<H>
{
    type Handle = InfiniteSourceHandle<H>;
    type Error = BuilderError;

    async fn build(self) -> Result<Self::Handle, Self::Error> {
        // Create channels for supervisor communication
        let (event_sender, event_receiver, state_watcher) =
            ChannelBuilder::new().build(InfiniteSourceState::<H>::Created);

        // Use provided strategy or default to JonestownSourceStrategy
        let control_strategy = self
            .config
            .control_strategy
            .unwrap_or_else(|| Arc::new(JonestownSourceStrategy));

        // Create instrumentation if not provided
        let instrumentation = self
            .instrumentation
            .unwrap_or_else(|| Arc::new(StageInstrumentation::new()));

        // Create context
        let context = InfiniteSourceContext::<H>::new(InfiniteSourceContextInit {
            stage_id: self.config.stage_id,
            stage_name: self.config.stage_name.clone(),
            flow_name: self.config.flow_name.clone(),
            flow_id: self.resources.flow_id,
            data_journal: self.resources.data_journal.clone(),
            error_journal: self.resources.error_journal.clone(),
            system_journal: self.resources.system_journal.clone(),
            replay_archive: self.resources.replay_archive.clone(),
            bus: self.resources.message_bus.clone(),
            instrumentation,
            control_strategy,
            backpressure_writer: self.resources.backpressure_writer.clone(),
        });

        // Ensure the handler (and any wrappers) receive the stage writer id before running (FLOWIP-081d).
        let mut handler = self.handler;
        handler.bind_writer_id(WriterId::from(self.config.stage_id));

        // Create supervisor (private - not exposed)
        let supervisor = InfiniteSourceSupervisor {
            name: format!("infinite_source_{}", self.config.stage_name),
            handler,
            system_journal: self.resources.system_journal.clone(),
            stage_id: self.config.stage_id,
            idle_backoff: crate::supervised_base::idle_backoff::IdleBackoff::exponential_with_cap(
                Duration::from_millis(1),
                Duration::from_millis(10),
            ),
            replay_driver: None,
            replay_started_at: None,
            replay_completion: ReplayCompletionGuard::default(),
        };

        // Clone what we need for the task
        let state_watcher_for_task = state_watcher.clone();

        // Spawn the supervisor task
        let supervisor_name = format!("infinite_source_{}", self.config.stage_name);
        let task = SupervisorTaskBuilder::<InfiniteSourceSupervisor<H>>::new(&supervisor_name)
            .spawn(move || async move {
                let supervisor_with_events = HandlerSupervisedWithExternalEvents::new(
                    supervisor,
                    event_receiver,
                    state_watcher_for_task,
                );

                // Run with the wrapper
                HandlerSupervisedExt::run(
                    supervisor_with_events,
                    InfiniteSourceState::<H>::Created,
                    context,
                )
                .await
            });

        // Build and return handle
        HandleBuilder::new()
            .with_event_sender(event_sender)
            .with_state_watcher(state_watcher)
            .with_supervisor_task(task)
            .build_standard()
            .map_err(|e| BuilderError::Other(e.to_string()))
    }
}
