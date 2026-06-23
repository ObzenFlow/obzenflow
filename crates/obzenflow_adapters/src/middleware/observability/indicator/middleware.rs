// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The indicator measurement engine (FLOWIP-115f).
//!
//! Brackets one operation execution with a wall-clock measurement and builds a
//! single typed [`IndicatorSample`] from it. The engine is observe-only: it
//! never mutates outputs, steers control flow, or aggregates across executions.

use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::payloads::observability_payload::{
    IndicatorKind, IndicatorSample, IndicatorTag, MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::time::MetricsDuration;
use obzenflow_core::{EventId, StageId, WriterId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Resolved authoring configuration for one indicator attachment.
#[derive(Debug, Clone)]
pub struct IndicatorConfig {
    /// The indicator kind. Only `Latency` ships in FLOWIP-115f.
    pub kind: IndicatorKind,
    /// The named operation being measured, e.g. `"payment.authorization"`.
    pub operation: Option<String>,
    /// The indicator name within the operation, e.g. `"authorization.latency"`.
    pub indicator: Option<String>,
    /// Static authoring-time tags carried on every sample.
    pub tags: Vec<(String, String)>,
}

impl Default for IndicatorConfig {
    fn default() -> Self {
        Self {
            kind: IndicatorKind::Latency,
            operation: None,
            indicator: None,
            tags: Vec::new(),
        }
    }
}

/// Observe-only middleware that records one per-execution service-level
/// indicator sample.
///
/// For the latency kind it brackets the handler with a wall-clock measurement
/// (`before_handle` remembers the start, `after_handle` reads the elapsed time)
/// and emits exactly one [`IndicatorSample`] per input. It is `LiveOnly`, so
/// strict replay suppresses the live measurement; the sample recorded during the
/// live run is not re-emitted on replay (it remains in the original journal).
#[derive(Debug, Clone)]
pub struct IndicatorMiddleware {
    config: IndicatorConfig,
    starts: Arc<Mutex<HashMap<EventId, Instant>>>,
}

impl IndicatorMiddleware {
    /// Build an indicator engine from resolved authoring configuration.
    pub fn with_config(config: IndicatorConfig) -> Self {
        Self {
            config,
            starts: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(super) fn remember_start(&self, event: &ChainEvent) {
        self.starts
            .lock()
            .expect("IndicatorMiddleware starts lock poisoned")
            .insert(event.id, Instant::now());
    }

    pub(super) fn duration_for_input(&self, event: &ChainEvent) -> MetricsDuration {
        let elapsed = self
            .starts
            .lock()
            .expect("IndicatorMiddleware starts lock poisoned")
            .remove(&event.id)
            .map(|start| start.elapsed())
            .unwrap_or_default();
        MetricsDuration::from_nanos(elapsed.as_nanos().min(u64::MAX as u128) as u64)
    }

    /// Build the per-execution sample for a measured value. Records the raw
    /// measurement only: the objective and good/bad classification are read-side
    /// (FLOWIP-115l), never baked into the sample.
    pub(super) fn sample(&self, value: MetricsDuration) -> IndicatorSample {
        IndicatorSample {
            kind: self.config.kind,
            operation: self.config.operation.clone().unwrap_or_default(),
            indicator: self.config.indicator.clone().unwrap_or_default(),
            value_ms: value.as_millis(),
            tags: self
                .config
                .tags
                .iter()
                .map(|(key, value)| IndicatorTag {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
        }
    }

    /// Build the journalled wide-event diagnostic carrying one sample.
    pub(super) fn diagnostic(&self, stage_id: StageId, value: MetricsDuration) -> ChainEvent {
        ChainEventFactory::observability_event(
            WriterId::from(stage_id),
            ObservabilityPayload::Middleware(MiddlewareLifecycle::Indicator(self.sample(value))),
        )
    }
}
