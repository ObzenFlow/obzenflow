// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Merge stage configuration (FLOWIP-084q B7, B8).

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
    /// Producers whose roots are all finite, read through the canonical merge
    /// in the batch phase.
    pub batch_producers: Vec<StageId>,
    /// Producers with a live root, held until every batch producer has authored
    /// natural EOF.
    pub live_producers: Vec<StageId>,
    pub control_strategy: Option<Arc<dyn SignalGate>>,
}

impl MergeConfig {
    pub fn new(
        stage_id: StageId,
        stage_name: impl Into<String>,
        flow_name: impl Into<String>,
        strategy: MergeStrategyKind,
        batch_producers: Vec<StageId>,
        live_producers: Vec<StageId>,
    ) -> Self {
        Self {
            stage_id,
            stage_name: stage_name.into(),
            flow_name: flow_name.into(),
            strategy,
            batch_producers,
            live_producers,
            control_strategy: None,
        }
    }
}
