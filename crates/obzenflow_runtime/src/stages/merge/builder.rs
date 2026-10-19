// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Builder for merge stages

use std::sync::Arc;

use crate::messaging::upstream_subscription::ReaderSelectionPolicy;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::control_strategies::{ProcessingContext, SignalGate};
use crate::stages::common::heartbeat::{spawn_heartbeat, HeartbeatConfig, HeartbeatState};
use crate::stages::resources_builder::StageResources;
use crate::supervised_base::{
    BuilderError, ChannelBuilder, HandleBuilder, HandlerSupervisedWithExternalEvents,
    SupervisorBuilder, SupervisorTaskBuilder,
};
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, StageId, WriterId};

use super::config::MergeConfig;
use super::fsm::{MergeContext, MergeState};
use super::handle::MergeHandle;
use super::supervisor::MergeSupervisor;

/// Builder for merge stages.
pub struct MergeBuilder {
    config: MergeConfig,
    resources: StageResources,
    upstream_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
    control_strategy: Arc<dyn SignalGate>,
    instrumentation: Option<Arc<StageInstrumentation>>,
    heartbeat_config: HeartbeatConfig,
}

impl MergeBuilder {
    pub fn new(
        config: MergeConfig,
        resources: StageResources,
        upstream_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
        control_strategy: Arc<dyn SignalGate>,
    ) -> Self {
        Self {
            config,
            resources,
            upstream_journals,
            control_strategy,
            instrumentation: None,
            heartbeat_config: HeartbeatConfig::default(),
        }
    }

    pub fn with_instrumentation(mut self, instrumentation: Arc<StageInstrumentation>) -> Self {
        self.instrumentation = Some(instrumentation);
        self
    }

    pub fn with_heartbeat(mut self, heartbeat_config: HeartbeatConfig) -> Self {
        self.heartbeat_config = heartbeat_config;
        self
    }

    fn journals_of(&self, producers: &[StageId]) -> Vec<(StageId, Arc<dyn Journal<ChainEvent>>)> {
        self.upstream_journals
            .iter()
            .filter(|(id, _)| producers.contains(id))
            .cloned()
            .collect()
    }
}

#[async_trait::async_trait]
impl SupervisorBuilder for MergeBuilder {
    type Handle = MergeHandle;
    type Error = BuilderError;

    async fn build(self) -> Result<Self::Handle, Self::Error> {
        let (event_sender, event_receiver, state_watcher) =
            ChannelBuilder::new().build(MergeState::Created);

        let instrumentation = self
            .instrumentation
            .clone()
            .unwrap_or_else(|| Arc::new(StageInstrumentation::new()));
        instrumentation.bind_observations(
            self.resources.flow_id,
            WriterId::from(self.config.stage_id),
            &self.resources.runtime_execution,
        );

        let mut first_phase_subscription_factory = self
            .resources
            .upstream_subscription_factory
            .rebind(&self.journals_of(&self.config.first_phase));
        first_phase_subscription_factory.reader_selection = ReaderSelectionPolicy::CanonicalMerge;
        first_phase_subscription_factory.recorded = None;
        let mut held_subscription_factory = self
            .resources
            .upstream_subscription_factory
            .rebind(&self.journals_of(&self.config.held));
        if self.config.held.len() < 2 {
            held_subscription_factory.recorded = None;
        }

        let publications = crate::supervised_base::publication::PublicationScope::new();
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
            Some(publications.enter_sync(|| {
                spawn_heartbeat(
                    self.config.stage_id,
                    self.config.stage_name.clone(),
                    instrumentation.clone(),
                    self.resources.liveness_snapshots.clone(),
                    heartbeat_state,
                    heartbeat_config,
                    self.resources.runtime_execution.clone(),
                )
            }))
        };

        let context = MergeContext {
            stage_id: self.config.stage_id,
            stage_name: self.config.stage_name.clone(),
            flow_name: self.config.flow_name.clone(),
            flow_id: self.resources.flow_id,
            strategy: self.config.strategy,
            writer_id: None,
            runtime_execution: self.resources.runtime_execution.clone(),
            lineage_policy: self.resources.lineage_policy,
            data_journal: self.resources.data_journal.clone(),
            instrumentation: instrumentation.clone(),
            control_strategy: self.control_strategy.clone(),
            processing_context: ProcessingContext::default(),
            first_phase_subscription_factory,
            held_subscription_factory,
            first_phase: Default::default(),
            held: Default::default(),
            held_producers: self.config.held.clone(),
            backpressure_writer: self.resources.backpressure_writer.clone(),
            backpressure_readers: self.resources.backpressure_readers.clone(),
            output_contract: self.resources.output_contract.clone(),
            pending: None,
            backpressure_pulse:
                crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse::new(),
            backpressure_stall: Default::default(),
            heartbeat,
            catch_up: crate::stages::common::supervision::catch_up::CatchUpContext::producing(
                crate::stages::common::supervision::flow_context_factory::make_flow_context(
                    &self.config.flow_name,
                    &self.resources.flow_id.to_string(),
                    &self.config.stage_name,
                    self.config.stage_id,
                    obzenflow_core::event::context::StageType::Merge,
                ),
                self.resources.data_journal.clone(),
                instrumentation.clone(),
                self.resources.runtime_execution.clone(),
            ),
        };

        let supervisor = MergeSupervisor::new(self.config.stage_name.clone(), self.config.stage_id);
        let supervisor_name = format!("merge_{}", self.config.stage_name);
        let supervisor_with_events = HandlerSupervisedWithExternalEvents::new(
            supervisor,
            event_receiver,
            state_watcher.clone(),
            crate::supervised_base::with_external_events::stage_commands(
                self.resources.data_journal.clone(),
                self.resources.flow_context.clone(),
            ),
        );
        let task = SupervisorTaskBuilder::new(&supervisor_name)
            .with_publications(publications)
            .spawn_handler_supervised(supervisor_with_events, MergeState::Created, context);

        HandleBuilder::new()
            .with_event_sender(event_sender)
            .with_state_watcher(state_watcher)
            .with_supervisor_task(task)
            .build_standard()
            .map_err(|e| BuilderError::Other(e.to_string()))
    }
}
