// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Join stage configuration

use crate::stages::common::control_strategies::ControlEventStrategy;
use obzenflow_core::StageId;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Default Live join fairness cap (FLOWIP-084g).
///
/// When the join is in Live mode, after N reference events without a stream event the supervisor
/// prioritizes polling the stream to prevent starvation.
pub const DEFAULT_REFERENCE_BATCH_CAP: usize = 8;

/// Controls whether the join requires reference EOF before processing the stream.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum JoinReferenceMode {
    /// Current behavior: hydrate the reference side until EOF, then consume stream events.
    #[default]
    FiniteEof,

    /// Live mode: consume reference updates and stream events concurrently (no reference EOF gate).
    Live,
}

/// Configuration for a join stage
#[derive(Clone, Serialize, Deserialize)]
pub struct JoinConfig {
    /// Stage ID for this join
    pub stage_id: StageId,

    /// Unique name for this join stage
    pub stage_name: String,

    /// Flow name
    pub flow_name: String,

    /// Stage ID for the reference upstream
    pub reference_source_id: StageId,

    /// Stage ID for the stream upstream
    pub stream_source_id: StageId,

    /// Reference join mode (default: FiniteEof).
    pub reference_mode: JoinReferenceMode,

    /// Live-mode fairness cap: after N reference events without a stream event, prioritize a
    /// stream poll. `None` disables the cap (pure priority based on selection order).
    ///
    /// Default: `Some(DEFAULT_REFERENCE_BATCH_CAP)`.
    pub reference_batch_cap: Option<usize>,

    /// Control strategy for handling control events
    #[serde(skip)]
    pub control_strategy: Option<Arc<dyn ControlEventStrategy>>,

    /// List of upstream stage IDs
    pub upstream_stages: Vec<StageId>,
}

impl JoinConfig {
    pub fn new(
        stage_id: StageId,
        stage_name: impl Into<String>,
        flow_name: impl Into<String>,
        reference_source_id: StageId,
        stream_source_id: StageId,
    ) -> Self {
        Self {
            stage_id,
            stage_name: stage_name.into(),
            flow_name: flow_name.into(),
            reference_source_id,
            stream_source_id,
            reference_mode: JoinReferenceMode::FiniteEof,
            reference_batch_cap: Some(DEFAULT_REFERENCE_BATCH_CAP),
            control_strategy: None,
            upstream_stages: vec![reference_source_id, stream_source_id],
        }
    }
}
