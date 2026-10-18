// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Merge stage configuration.

use crate::stages::common::control_strategies::SignalGate;
use crate::stages::merges::MergeStrategyKind;
use obzenflow_core::StageId;
use std::sync::Arc;

/// Configuration for a merge stage.
#[derive(Clone)]
pub struct MergeConfig {
    pub stage_id: StageId,
    pub stage_name: String,
    pub flow_name: String,
    /// The framework strategy the supervisor interprets.
    pub strategy: MergeStrategyKind,
    /// Producers read in the first phase, through the canonical merge: a
    /// `then_live` merge's batch producers, or a `concat` merge's first one.
    pub first_phase: Vec<StageId>,
    /// Producers held until every first-phase producer has authored natural
    /// EOF: a `then_live` merge's live producers, or a `concat` merge's other one.
    pub held: Vec<StageId>,
    pub control_strategy: Option<Arc<dyn SignalGate>>,
}

impl MergeConfig {
    pub fn new(
        stage_id: StageId,
        stage_name: impl Into<String>,
        flow_name: impl Into<String>,
        strategy: MergeStrategyKind,
        first_phase: Vec<StageId>,
        held: Vec<StageId>,
    ) -> Self {
        Self {
            stage_id,
            stage_name: stage_name.into(),
            flow_name: flow_name.into(),
            strategy,
            first_phase,
            held,
            control_strategy: None,
        }
    }
}
