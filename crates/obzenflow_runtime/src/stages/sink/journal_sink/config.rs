// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Configuration for journal sink stages

use super::boundary::SinkDeliveryBoundary;
use crate::stages::common::control_strategies::SignalGate;
use crate::stages::observer::StageObserverBindings;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
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

    /// Closed observer inputs validated by the concrete sink builder.
    pub(crate) observer_bindings: StageObserverBindings,

    /// Connector-described destination identity. The stage name remains the
    /// fallback for connectors that do not supply one.
    pub receipt_destination: Option<String>,

    /// Connector-described normal receipt method, used by writer outcomes
    /// that carry only per-attempt deltas and by runtime-authored failures.
    pub default_delivery_method: Option<DeliveryMethod>,
}

impl JournalSinkConfig {
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
            buffer_size: None,
            flush_interval_ms: None,
            control_strategy: None,
            sink_delivery_boundary: None,
            observer_bindings: StageObserverBindings::default(),
            receipt_destination: None,
            default_delivery_method: None,
        }
    }

    pub fn with_observer_bindings(mut self, bindings: StageObserverBindings) -> Self {
        self.observer_bindings = bindings;
        self
    }
}
