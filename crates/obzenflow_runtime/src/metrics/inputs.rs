// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Input structure for MetricsAggregator to separate data and error journals
//!
//! This module defines the MetricsInputs struct which cleanly separates
//! stage data journals from stage error journals, allowing MetricsAggregator
//! to subscribe to both types of journals for comprehensive metrics collection.

use crate::backpressure::BackpressureRegistry;
use obzenflow_core::{event::ChainEvent, journal::Journal, StageId};
use std::sync::Arc;

/// Input structure for MetricsAggregator that separates data and error journals
///
/// Per FLOWIP-082g (descoped), MetricsAggregator subscribes to both data and
/// error journals to provide immediate error visibility without requiring a
/// centralized ErrorSink.
#[derive(Clone)]
pub struct MetricsInputs {
    pub(crate) execution: Option<(obzenflow_core::FlowId, crate::execution::RuntimeExecution)>,
    pub observations: Arc<super::observations::ObservationHub>,
    /// Stage data journals - normal outputs from pipeline stages
    pub stage_data_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,

    /// Stage error journals - error outputs from pipeline stages (FLOWIP-082g)
    pub error_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,

    /// Flow-scoped backpressure registry for observability (FLOWIP-086k).
    pub backpressure_registry: Option<Arc<BackpressureRegistry>>,
}

impl MetricsInputs {
    pub(crate) fn with_execution(
        mut self,
        flow: obzenflow_core::FlowId,
        execution: Option<crate::execution::RuntimeExecution>,
    ) -> Self {
        self.execution = execution.map(|execution| (flow, execution));
        self
    }

    pub fn with_observations(
        mut self,
        observations: Arc<super::observations::ObservationHub>,
    ) -> Self {
        self.observations = observations;
        self
    }

    /// Create a new MetricsInputs with both data and error journals
    pub fn new(
        stage_data_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
        error_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
    ) -> Self {
        Self {
            execution: None,
            observations: Arc::new(super::observations::ObservationHub::default()),
            stage_data_journals,
            error_journals,
            backpressure_registry: None,
        }
    }

    pub fn with_backpressure_registry(mut self, registry: Arc<BackpressureRegistry>) -> Self {
        self.backpressure_registry = Some(registry);
        self
    }

    pub fn with_backpressure_registry_opt(
        mut self,
        registry: Option<Arc<BackpressureRegistry>>,
    ) -> Self {
        self.backpressure_registry = registry;
        self
    }

    /// Get all data journals
    pub fn data_journals(&self) -> &[(StageId, Arc<dyn Journal<ChainEvent>>)] {
        &self.stage_data_journals
    }

    /// Get all error journals
    pub fn error_journals(&self) -> &[(StageId, Arc<dyn Journal<ChainEvent>>)] {
        &self.error_journals
    }

    /// Get all journals for subscription (data + error)
    pub fn all_journals(&self) -> impl Iterator<Item = (StageId, &Arc<dyn Journal<ChainEvent>>)> {
        self.stage_data_journals
            .iter()
            .map(|(id, journal)| (*id, journal))
            .chain(
                self.error_journals
                    .iter()
                    .map(|(id, journal)| (*id, journal)),
            )
    }
}
