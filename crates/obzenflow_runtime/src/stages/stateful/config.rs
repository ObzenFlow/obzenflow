// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Configuration for stateful stages

use crate::stages::common::control_strategies::SignalGate;
use crate::stages::observer::StageObserverBindings;
use obzenflow_core::StageId;
use std::sync::Arc;
use std::time::Duration;

/// Configuration for a stateful stage
#[derive(Clone)]
pub struct StatefulConfig {
    /// Stage ID
    pub stage_id: StageId,

    /// Human-readable stage name
    pub stage_name: String,

    /// Flow name this stateful stage belongs to
    pub flow_name: String,

    /// IDs of upstream stages this stateful stage reads from
    pub upstream_stages: Vec<StageId>,

    /// Closed observer inputs. The runtime builder validates and materialises
    /// these against the concrete stateful target.
    pub(crate) observer_bindings: StageObserverBindings,

    /// Optional supervisor-driven emit interval for timer-driven emission while idle.
    pub emit_interval: Option<Duration>,

    /// Control event handling strategy (defaults to JonestownSignalStrategy if not specified)
    pub control_strategy: Option<Arc<dyn SignalGate>>,
}

impl StatefulConfig {
    pub fn new(
        stage_id: StageId,
        stage_name: impl Into<String>,
        flow_name: impl Into<String>,
        upstream_stages: Vec<StageId>,
    ) -> Self {
        Self {
            stage_id,
            stage_name: stage_name.into(),
            flow_name: flow_name.into(),
            upstream_stages,
            observer_bindings: StageObserverBindings::default(),
            emit_interval: None,
            control_strategy: None,
        }
    }

    pub fn with_observer_bindings(mut self, bindings: StageObserverBindings) -> Self {
        self.observer_bindings = bindings;
        self
    }
}
