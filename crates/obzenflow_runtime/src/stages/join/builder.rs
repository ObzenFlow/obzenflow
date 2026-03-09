// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Builder for join stages

use std::sync::Arc;

use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::handlers::JoinHandler;
use crate::stages::resources_builder::StageResources;
use crate::supervised_base::{
    BuilderError, ChannelBuilder, HandleBuilder, HandlerSupervisedExt,
    HandlerSupervisedWithExternalEvents, SupervisorBuilder, SupervisorTaskBuilder,
};
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, StageId};

use super::config::JoinConfig;
use super::fsm::{JoinContext, JoinState};
use super::handle::JoinHandle;
use super::supervisor::JoinSupervisor;

/// Error type for join builder
#[derive(Debug, thiserror::Error)]
pub enum JoinBuilderError {
    #[error("Missing upstream journal for {0} source")]
    MissingUpstream(&'static str),
}

/// Builder for creating join stages
pub struct JoinBuilder<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static> {
    handler: H,
    config: JoinConfig,
    resources: StageResources,
    reference_journal: Arc<dyn Journal<ChainEvent>>,
    stream_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
    control_strategy: Arc<dyn crate::stages::common::control_strategies::ControlEventStrategy>,
    instrumentation: Option<Arc<StageInstrumentation>>,
}

impl<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static> JoinBuilder<H> {
    /// Create a new join builder with StageResources
    pub fn new(
        handler: H,
        config: JoinConfig,
        resources: StageResources,
        reference_journal: Arc<dyn Journal<ChainEvent>>,
        stream_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
        control_strategy: Arc<dyn crate::stages::common::control_strategies::ControlEventStrategy>,
    ) -> Result<Self, JoinBuilderError> {
        tracing::info!(
            "JoinBuilder: Creating join with reference_stage_id={:?}, stream_stages={:?}",
            config.reference_source_id,
            stream_journals.iter().map(|(id, _)| id).collect::<Vec<_>>()
        );

        Ok(Self {
            handler,
            config,
            resources,
            reference_journal,
            stream_journals,
            control_strategy,
            instrumentation: None,
        })
    }

    /// Set the instrumentation for this join
    pub fn with_instrumentation(mut self, instrumentation: Arc<StageInstrumentation>) -> Self {
        self.instrumentation = Some(instrumentation);
        self
    }
}

#[async_trait::async_trait]
impl<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static> SupervisorBuilder
    for JoinBuilder<H>
{
    type Handle = JoinHandle<H>;
    type Error = BuilderError;

    async fn build(self) -> Result<Self::Handle, Self::Error> {
        // Create channels for supervisor communication
        let (event_sender, event_receiver, state_watcher) =
            ChannelBuilder::new().build(JoinState::<H>::Created);

        // Create instrumentation if not provided
        let instrumentation = self
            .instrumentation
            .unwrap_or_else(|| Arc::new(StageInstrumentation::new()));

        // Bind factories for reference and stream subscriptions (after DSL split)
        let reference_subscription_factory = self.resources.subscription_factory.bind(&[(
            self.config.reference_source_id,
            self.reference_journal.clone(),
        )]);

        let stream_subscription_factory = self
            .resources
            .subscription_factory
            .bind(&self.stream_journals);

        // Create context with subscription factory from resources
        let handler = self.handler;
        let handler_state = handler.initial_state();
        let context = JoinContext {
            handler: Arc::new(handler),
            handler_state,
            stage_id: self.config.stage_id,
            stage_name: self.config.stage_name.clone(),
            flow_name: self.config.flow_name.clone(),
            flow_id: self.resources.flow_id,
            reference_stage_id: self.config.reference_source_id,
            data_journal: self.resources.data_journal.clone(),
            error_journal: self.resources.error_journal.clone(),
            system_journal: self.resources.system_journal.clone(),
            bus: self.resources.message_bus.clone(),
            writer_id: None,
            reference_subscription: None,
            stream_subscription: None,
            reference_contract_state: Vec::new(),
            reference_last_contract_check: None,
            stream_contract_state: Vec::new(),
            stream_last_contract_check: None,
            buffered_eof: None,
            instrumentation: instrumentation.clone(),
            control_strategy: self.control_strategy.clone(),
            reference_subscription_factory,
            stream_subscription_factory,
            reference_mode: self.config.reference_mode,
            reference_batch_cap: self.config.reference_batch_cap,
            reference_since_last_stream: 0,
            events_since_last_heartbeat: 0,
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

        // Create supervisor
        let supervisor = JoinSupervisor {
            name: format!("join_{}", self.config.stage_name),
            stage_id: self.config.stage_id,
            reference_subscription: None,
            stream_subscription: None,
            _marker: std::marker::PhantomData,
        };

        // Clone what we need for the task
        let state_watcher_for_task = state_watcher.clone();

        // Spawn the supervisor task
        let supervisor_name = format!("join_{}", self.config.stage_name);
        let task = SupervisorTaskBuilder::<JoinSupervisor<H>>::new(&supervisor_name).spawn(
            move || async move {
                let supervisor_with_events = HandlerSupervisedWithExternalEvents::new(
                    supervisor,
                    event_receiver,
                    state_watcher_for_task,
                );

                // Run with the wrapper
                HandlerSupervisedExt::run(supervisor_with_events, JoinState::<H>::Created, context)
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
