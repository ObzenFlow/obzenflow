// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::context::ReplayContext;
use obzenflow_core::event::payloads::correlation_payload::CorrelationPayload;
use obzenflow_core::event::types::{CorrelationId, EventId};
use obzenflow_core::ChainEvent;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, VecDeque};

fn max_lineage_depth() -> usize {
    // Match `ChainEventFactory::derived_event` defaults.
    const DEFAULT_MAX_LINEAGE_DEPTH: usize = 100;
    std::env::var("OBZENFLOW_MAX_LINEAGE_DEPTH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MAX_LINEAGE_DEPTH)
        .max(1)
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct TraceState {
    parent_ids: VecDeque<EventId>,

    correlation_seen: bool,
    correlation_mixed: bool,
    correlation_ids: BTreeSet<CorrelationId>,
    correlation_id: Option<CorrelationId>,
    correlation_payload: Option<CorrelationPayload>,
    replay_context: Option<ReplayContext>,
}

impl TraceState {
    pub(crate) fn record_event(&mut self, event: &ChainEvent) {
        if event.is_lifecycle() || event.is_control() {
            return;
        }

        let max_depth = max_lineage_depth();

        self.parent_ids.push_back(event.id);
        while self.parent_ids.len() > max_depth {
            self.parent_ids.pop_front();
        }

        if let Some(correlation_id) = event.correlation_id {
            if self.correlation_ids.len() < max_depth
                || self.correlation_ids.contains(&correlation_id)
            {
                self.correlation_ids.insert(correlation_id);
            }
        }

        if !self.correlation_seen {
            self.correlation_seen = true;
            self.correlation_id = event.correlation_id;
            self.correlation_payload = event.correlation_payload.clone();
            self.replay_context = event.replay_context.clone();
            return;
        }

        if self.correlation_mixed {
            return;
        }

        if self.correlation_id != event.correlation_id {
            self.correlation_mixed = true;
            self.correlation_id = None;
            self.correlation_payload = None;
            self.replay_context = None;
            return;
        }

        if self.correlation_payload != event.correlation_payload {
            self.correlation_payload = None;
        }
        if self.replay_context != event.replay_context {
            self.replay_context = None;
        }
    }

    pub(crate) fn reset(&mut self) {
        *self = Self::default();
    }

    pub(crate) fn parent_ids(&self) -> Vec<EventId> {
        self.parent_ids.iter().copied().collect()
    }

    pub(crate) fn correlation_id(&self) -> Option<CorrelationId> {
        self.correlation_id
    }

    pub(crate) fn correlation_payload(&self) -> Option<CorrelationPayload> {
        self.correlation_payload.clone()
    }

    pub(crate) fn replay_context(&self) -> Option<ReplayContext> {
        self.replay_context.clone()
    }

    pub(crate) fn mixed_correlation_ids(&self) -> Option<Vec<CorrelationId>> {
        if self.correlation_mixed && !self.correlation_ids.is_empty() {
            Some(self.correlation_ids.iter().copied().collect())
        } else {
            None
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.parent_ids.is_empty()
    }
}
