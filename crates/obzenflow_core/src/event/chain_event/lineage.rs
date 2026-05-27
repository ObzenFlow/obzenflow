// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{ChainEvent, CorrelationContext};
use crate::event::payloads::correlation_payload::CorrelationPayload;
use crate::event::types::CorrelationId;

impl ChainEvent {
    /// Create correlation for a source event (flow entry)
    pub fn with_new_correlation(mut self, stage_name: impl Into<String>) -> Self {
        let correlation_id = CorrelationId::new();
        self.set_single_correlation(
            correlation_id,
            Some(CorrelationPayload::new(stage_name, self.id)),
        );
        self
    }

    /// Propagate correlation and cycle state from parent event to derived event
    pub fn with_correlation_from(mut self, parent: &ChainEvent) -> Self {
        self.correlation = parent.correlation.clone();
        self.cycle_depth = parent.cycle_depth;
        self.cycle_scc_id = parent.cycle_scc_id;
        self
    }

    /// Check if this event has correlation info
    pub fn has_correlation(&self) -> bool {
        self.correlation.is_some()
    }

    /// Calculate latency if this event has correlation payload
    pub fn correlation_latency(&self) -> Option<std::time::Duration> {
        self.correlation_payload().map(|p| p.calculate_latency())
    }

    pub fn correlation_id(&self) -> Option<CorrelationId> {
        self.correlation.as_ref().and_then(|c| c.single_id())
    }

    pub fn correlation_ids(&self) -> Option<&[CorrelationId]> {
        self.correlation.as_ref().map(|c| c.ids.as_slice())
    }

    pub fn correlation_payload(&self) -> Option<&CorrelationPayload> {
        self.correlation.as_ref().and_then(|c| c.payload.as_ref())
    }

    pub fn correlation_ids_truncated(&self) -> bool {
        self.correlation
            .as_ref()
            .map(|c| c.truncated)
            .unwrap_or(false)
    }

    pub fn set_single_correlation(
        &mut self,
        id: CorrelationId,
        payload: Option<CorrelationPayload>,
    ) {
        self.correlation = Some(CorrelationContext::single(id, payload));
    }

    pub fn set_correlation_sample(&mut self, ids: Vec<CorrelationId>, truncated: bool) {
        self.correlation = Some(CorrelationContext::sample(ids, truncated));
    }

    pub fn clear_correlation(&mut self) {
        self.correlation = None;
    }
}
