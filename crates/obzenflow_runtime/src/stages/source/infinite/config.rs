// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Configuration for infinite source stages

use crate::stages::common::control_strategies::{AdmissionGate, AttemptObserver};
use crate::stages::source::strategies::CompletionGate;
use obzenflow_core::StageId;
use std::sync::Arc;

/// Configuration for an infinite source stage
#[derive(Clone)]
pub struct InfiniteSourceConfig {
    /// Stage ID
    pub stage_id: StageId,

    /// Human-readable stage name
    pub stage_name: String,

    /// Flow name this source belongs to
    pub flow_name: String,

    /// Optional source control strategy for shutdown / FlowControl decisions.
    /// When not provided, the builder will default to JonestownSourceStrategy.
    pub control_strategy: Option<Arc<dyn CompletionGate>>,

    /// Runtime-owned source admission gates (FLOWIP-115a).
    pub admission_gates: Vec<Arc<dyn AdmissionGate>>,

    /// Runtime-owned source attempt observers (FLOWIP-115a).
    pub attempt_observers: Vec<Arc<dyn AttemptObserver>>,
}

impl std::fmt::Debug for InfiniteSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InfiniteSourceConfig")
            .field("stage_id", &self.stage_id)
            .field("stage_name", &self.stage_name)
            .field("flow_name", &self.flow_name)
            .field("control_strategy", &self.control_strategy)
            .field("admission_gates", &self.admission_gates.len())
            .field("attempt_observers", &self.attempt_observers.len())
            .finish()
    }
}
