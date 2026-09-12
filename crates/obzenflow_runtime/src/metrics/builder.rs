// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Builder for creating MetricsAggregator with proper FSM lifecycle
//!
//! This builder ensures the metrics aggregator is created and started correctly
//! according to the FSM architecture patterns, returning only a handle for control.

use super::{
    fsm::{MetricsAggregatorContext, MetricsAggregatorEvent, MetricsAggregatorState},
    inputs::MetricsInputs,
    supervisor::MetricsAggregatorSupervisor,
};
use crate::supervised_base::{
    BuilderError, ChannelBuilder, HandleBuilder, StandardHandle, SupervisorBuilder,
    SupervisorTaskBuilder,
};
use obzenflow_core::{
    event::SystemEvent,
    journal::Journal,
    metrics::{CompositeBoundary, MetricsSnapshotExporter, StageMetadata},
    StageId,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Builder for creating a metrics aggregator with proper FSM lifecycle
pub struct MetricsAggregatorBuilder {
    /// Metrics inputs containing stage and system journals
    inputs: MetricsInputs,

    /// System journal for reporting
    system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Publishes the latest snapshots to the reporting read model.
    metrics_exporter: Arc<dyn MetricsSnapshotExporter>,

    /// Stage metadata for display and categorization
    stage_metadata: HashMap<StageId, StageMetadata>,

    /// Composite boundaries for composite RED projection (FLOWIP-128a B4).
    composite_boundaries: Vec<CompositeBoundary>,

    export_interval_secs: u64,
    pipeline_writer: Option<obzenflow_core::event::WriterId>,
}

impl MetricsAggregatorBuilder {
    /// Create a new metrics aggregator builder with MetricsInputs
    pub fn new(
        inputs: MetricsInputs,
        system_journal: Arc<dyn Journal<SystemEvent>>,
        metrics_exporter: Arc<dyn MetricsSnapshotExporter>,
    ) -> Self {
        Self {
            inputs,
            system_journal,
            metrics_exporter,
            stage_metadata: HashMap::new(),
            composite_boundaries: Vec::new(),
            pipeline_writer: None,
            export_interval_secs: 10, // Default to 10 seconds
        }
    }

    pub(crate) fn with_pipeline_writer(mut self, writer: obzenflow_core::event::WriterId) -> Self {
        self.pipeline_writer = Some(writer);
        self
    }

    /// Set the export interval in seconds
    pub fn with_export_interval(mut self, seconds: u64) -> Self {
        self.export_interval_secs = seconds;
        self
    }

    /// Set stage metadata for display and categorization
    pub fn with_stage_metadata(mut self, metadata: HashMap<StageId, StageMetadata>) -> Self {
        self.stage_metadata = metadata;
        self
    }

    /// Set composite boundaries for composite RED projection (FLOWIP-128a B4).
    #[doc(hidden)]
    pub fn with_composite_boundaries(mut self, boundaries: Vec<CompositeBoundary>) -> Self {
        self.composite_boundaries = boundaries;
        self
    }
}

#[async_trait::async_trait]
impl SupervisorBuilder for MetricsAggregatorBuilder {
    type Handle = StandardHandle<MetricsAggregatorEvent, MetricsAggregatorState>;
    type Error = BuilderError;

    async fn build(self) -> Result<Self::Handle, Self::Error> {
        self.prepare().await?.start()
    }
}

/// Prepared journal inputs, with no live child and no published readiness.
pub(crate) struct PreparedMetricsAggregator {
    context: MetricsAggregatorContext,
    io: super::fsm::MetricsAggregatorIo,
    system_journal: Arc<dyn Journal<SystemEvent>>,
    system_id: obzenflow_core::id::SystemId,
}

impl MetricsAggregatorBuilder {
    pub(crate) async fn prepare(self) -> Result<PreparedMetricsAggregator, BuilderError> {
        // Create system ID for metrics aggregator
        let system_id = obzenflow_core::id::SystemId::new();

        // Create metrics context with all mutable state
        let (mut metrics_context, metrics_io) = MetricsAggregatorContext::new(
            self.inputs.clone(),
            self.system_journal.clone(),
            self.metrics_exporter,
            self.export_interval_secs,
            system_id,
            self.stage_metadata,
            self.composite_boundaries,
        )
        .await
        .map_err(BuilderError::Other)?;

        metrics_context.pipeline_writer = self.pipeline_writer;

        Ok(PreparedMetricsAggregator {
            context: metrics_context,
            io: metrics_io,
            system_journal: self.system_journal,
            system_id,
        })
    }
}

impl PreparedMetricsAggregator {
    pub(crate) fn writer_id(&self) -> obzenflow_core::event::WriterId {
        self.system_id.into()
    }

    /// The caller authorises the child's lifetime. All fallible journal input
    /// construction happened in prepare; spawning does no storage I/O.
    pub(crate) fn start(self) -> Result<super::MetricsHandle, BuilderError> {
        let Self {
            context: metrics_context,
            io: metrics_io,
            system_journal,
            system_id,
        } = self;

        // Create channels for supervisor communication
        // Even though metrics runs autonomously, we still create channels
        // for consistency and potential future use
        let (event_sender, _event_receiver, state_watcher) =
            ChannelBuilder::<MetricsAggregatorEvent, MetricsAggregatorState>::new()
                .with_event_buffer(10) // Small buffer, rarely used
                .build(MetricsAggregatorState::Initializing);

        // Create supervisor (private struct)
        let supervisor = MetricsAggregatorSupervisor {
            name: "metrics_aggregator".to_string(),
            system_journal,
            system_id,
            data_subscription: Some(metrics_io.data_subscription),
            error_subscription: metrics_io.error_subscription,
            system_subscription: Some(metrics_io.system_subscription),
            system_retry_at: None,
            next_input: 0,
            state_watcher: state_watcher.clone(),
            last_state: Some(MetricsAggregatorState::Initializing),
        };

        // Spawn the supervisor task
        let supervisor_task =
            SupervisorTaskBuilder::<MetricsAggregatorSupervisor>::new("metrics_aggregator")
                .spawn_self_supervised(
                    supervisor,
                    MetricsAggregatorState::Initializing,
                    metrics_context,
                );

        // Build and return the standard handle
        HandleBuilder::new()
            .with_event_sender(event_sender)
            .with_state_watcher(state_watcher)
            .with_supervisor_task(supervisor_task)
            .build_standard()
            .map_err(|e| BuilderError::Other(e.to_string()))
    }
}
