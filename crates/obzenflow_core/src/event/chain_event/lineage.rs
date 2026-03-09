// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::ChainEvent;
use crate::event::payloads::correlation_payload::CorrelationPayload;
use crate::event::types::CorrelationId;

impl ChainEvent {
    /// Create correlation for a source event (flow entry)
    pub fn with_new_correlation(mut self, stage_name: impl Into<String>) -> Self {
        let correlation_id = CorrelationId::new();
        self.correlation_id = Some(correlation_id);
        self.correlation_payload = Some(CorrelationPayload::new(stage_name, self.id));
        self
    }

    /// Propagate correlation and cycle state from parent event to derived event
    pub fn with_correlation_from(mut self, parent: &ChainEvent) -> Self {
        self.correlation_id = parent.correlation_id;
        self.correlation_payload = parent.correlation_payload.clone();
        self.cycle_depth = parent.cycle_depth;
        self.cycle_scc_id = parent.cycle_scc_id;
        self
    }

    /// Check if this event has correlation info
    pub fn has_correlation(&self) -> bool {
        self.correlation_id.is_some()
    }

    /// Calculate latency if this event has correlation payload
    pub fn correlation_latency(&self) -> Option<std::time::Duration> {
        self.correlation_payload
            .as_ref()
            .map(|p| p.calculate_latency())
    }
}
