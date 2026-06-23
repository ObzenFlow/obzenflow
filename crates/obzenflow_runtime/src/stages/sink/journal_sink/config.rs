// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Configuration for journal sink stages

use super::boundary::SinkDeliveryBoundary;
use crate::stages::common::control_strategies::SignalGate;
use crate::stages::observer::StageObserverBundle;
use obzenflow_core::StageId;
use std::sync::Arc;

/// Configuration for a journal sink stage
#[derive(Clone)]
pub struct JournalSinkConfig {
    /// Stage ID
    pub stage_id: StageId,

    /// Human-readable stage name
    pub stage_name: String,

    /// Flow name this sink belongs to
    pub flow_name: String,

    /// IDs of upstream stages this sink consumes from
    pub upstream_stages: Vec<StageId>,

    /// Buffer size for event batching (optional optimization)
    pub buffer_size: Option<usize>,

    /// Flush interval in milliseconds (optional optimization)
    pub flush_interval_ms: Option<u64>,

    /// Control strategy for handling FlowControl events (defaults applied in builder)
    pub control_strategy: Option<Arc<dyn SignalGate>>,

    /// Runtime-neutral sink-delivery boundary seam (FLOWIP-115b). Wraps the
    /// data-event `consume_report` attempt; `None` means no sink policies.
    pub sink_delivery_boundary: Option<Arc<dyn SinkDeliveryBoundary>>,

    /// Observe-only middleware hooks for sink delivery.
    pub observers: StageObserverBundle,
}
