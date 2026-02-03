// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Configuration for transform stages

use crate::stages::common::control_strategies::ControlEventStrategy;
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

    /// Control event handling strategy (defaults to JonestownStrategy if not specified)
    pub control_strategy: Option<Arc<dyn ControlEventStrategy>>,
}
