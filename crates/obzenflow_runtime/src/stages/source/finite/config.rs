// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Configuration for finite source stages

use crate::stages::source::boundary::SourceBoundary;
use crate::stages::source::strategies::CompletionGate;
use obzenflow_core::StageId;
use std::sync::Arc;

/// Configuration for a finite source stage
#[derive(Clone)]
pub struct FiniteSourceConfig {
    /// Stage ID
    pub stage_id: StageId,

    /// Human-readable stage name
    pub stage_name: String,

    /// Flow name this source belongs to
    pub flow_name: String,

    /// Optional source control strategy for shutdown / FlowControl decisions.
    /// When not provided, the builder will default to JonestownSourceStrategy.
    pub control_strategy: Option<Arc<dyn CompletionGate>>,

    /// Runtime-neutral source boundary seam (FLOWIP-115a).
    pub source_boundary: Option<Arc<dyn SourceBoundary>>,
}

impl std::fmt::Debug for FiniteSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FiniteSourceConfig")
            .field("stage_id", &self.stage_id)
            .field("stage_name", &self.stage_name)
            .field("flow_name", &self.flow_name)
            .field("control_strategy", &self.control_strategy)
            .field("has_source_boundary", &self.source_boundary.is_some())
            .finish()
    }
}
