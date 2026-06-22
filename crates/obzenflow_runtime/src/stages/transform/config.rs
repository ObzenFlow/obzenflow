// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Configuration for transform stages

use crate::pipeline::config::CycleGuardConfig;
use crate::stages::common::control_strategies::SignalGate;
use crate::StageObserverBundle;
use obzenflow_core::StageId;
use std::sync::Arc;

/// Configuration for a transform stage
#[derive(Clone)]
pub struct TransformConfig {
    /// Stage ID
    pub stage_id: StageId,

    /// Human-readable stage name
    pub stage_name: String,

    /// Flow name this transform belongs to
    pub flow_name: String,

    /// IDs of upstream stages this transform reads from
    pub upstream_stages: Vec<StageId>,

    /// Runtime observer bundle attached to this stage.
    pub observers: StageObserverBundle,

    /// Control event handling strategy (defaults to JonestownSignalStrategy if not specified)
    pub control_strategy: Option<Arc<dyn SignalGate>>,

    /// Supervisor-level cycle protection configuration (FLOWIP-051l).
    ///
    /// When present, the transform supervisor allocates a cycle guard that
    /// enforces iteration limits for data events and deduplicates flow signals.
    pub cycle_guard: Option<CycleGuardConfig>,
}
