// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Builder for async infinite source stages

use std::sync::Arc;

use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::handlers::AsyncInfiniteSourceHandler;
use crate::stages::resources_builder::StageResources;
use crate::stages::source::replay_lifecycle::ReplayCompletionGuard;
use crate::stages::source::strategies::{JonestownSourceStrategy, SourceControlStrategy};
use crate::supervised_base::{
    BuilderError, ChannelBuilder, HandleBuilder, HandlerSupervisedExt, SupervisorBuilder,
    SupervisorTaskBuilder,
};
use obzenflow_core::WriterId;

use super::async_supervisor::AsyncInfiniteSourceSupervisor;
use super::config::InfiniteSourceConfig;
use super::fsm::{InfiniteSourceContext, InfiniteSourceContextInit, InfiniteSourceState};
use super::handle::InfiniteSourceHandle;

/// Builder for creating async infinite source stages.
pub struct AsyncInfiniteSourceBuilder<
    H: AsyncInfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
    handler: H,
    config: InfiniteSourceConfig,
    resources: StageResources,
    instrumentation: Option<Arc<StageInstrumentation>>,
}

impl<H: AsyncInfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    AsyncInfiniteSourceBuilder<H>
{
    pub fn new(handler: H, config: InfiniteSourceConfig, resources: StageResources) -> Self {
        Self {
            handler,
            config,
            resources,
            instrumentation: None,
        }
    }

    pub fn with_instrumentation(mut self, instrumentation: Arc<StageInstrumentation>) -> Self {
        self.instrumentation = Some(instrumentation);
        self
    }

    pub fn with_control_strategy(mut self, strategy: Arc<dyn SourceControlStrategy>) -> Self {
        self.config.control_strategy = Some(strategy);
        self
    }
}

#[async_trait::async_trait]
impl<H: AsyncInfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    SupervisorBuilder for AsyncInfiniteSourceBuilder<H>
{
    type Handle = InfiniteSourceHandle<H>;
    type Error = BuilderError;

    async fn build(self) -> Result<Self::Handle, Self::Error> {
        let (event_sender, event_receiver, state_watcher) =
            ChannelBuilder::new().build(InfiniteSourceState::<H>::Created);

        let control_strategy = self
            .config
            .control_strategy
            .unwrap_or_else(|| Arc::new(JonestownSourceStrategy));

        let instrumentation = self
            .instrumentation
            .unwrap_or_else(|| Arc::new(StageInstrumentation::new()));

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

        let supervisor = AsyncInfiniteSourceSupervisor {
            name: format!("async_infinite_source_{}", self.config.stage_name),
            handler,
            system_journal: self.resources.system_journal.clone(),
            stage_id: self.config.stage_id,
            external_events: event_receiver,
            state_watcher: state_watcher.clone(),
            last_state: None,
            replay_driver: None,
            replay_started_at: None,
            replay_completion: ReplayCompletionGuard::default(),
        };

        let supervisor_name = format!("async_infinite_source_{}", self.config.stage_name);
        let stage_name_for_trace = self.config.stage_name.clone();
        let task = SupervisorTaskBuilder::<AsyncInfiniteSourceSupervisor<H>>::new(&supervisor_name)
            .spawn(move || async move {
                tracing::debug!(
                    "Spawned task for async_infinite_source_{}",
                    stage_name_for_trace
                );

                HandlerSupervisedExt::run(supervisor, InfiniteSourceState::<H>::Created, context)
                    .await
            });

        HandleBuilder::new()
            .with_event_sender(event_sender)
            .with_state_watcher(state_watcher)
            .with_supervisor_task(task)
            .build_standard()
            .map_err(|e| BuilderError::Other(e.to_string()))
    }
}
