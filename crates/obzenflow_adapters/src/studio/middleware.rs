// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Factual revisions and selected measurements are independent Studio updates.

use super::messages::*;
use obzenflow_core::event::journal_record::ChainJournalRecord;
use obzenflow_core::event::observability::{ObservabilityContext, ObservationRecord};
use obzenflow_core::event::payloads::execution_payload::{
    CircuitBreakerFact, CircuitState, ExecutionPayload, RateLimiterFact, RateLimiterMode,
};
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::ChainPayload;
use obzenflow_core::metrics::ControlUnit;
use obzenflow_core::{web::SseFrame, StageId, StageKey};
use std::collections::{BTreeSet, HashMap};

#[derive(Clone, Default)]
pub(super) struct MiddlewareView {
    flow_id: Option<String>,
    flow_name: Option<String>,
    stage_names: HashMap<StageId, String>,
    writers: HashMap<ControlUnit, StageId>,
    circuit_breakers: HashMap<ControlUnit, CircuitBreakerSnapshot>,
    rate_limiters: HashMap<ControlUnit, RateLimiterSnapshot>,
    last_vector_clock: Option<VectorClock>,
}

pub(super) fn policy_unit(payload: &ExecutionPayload) -> Option<&StageKey> {
    match payload {
        ExecutionPayload::CircuitBreaker(fact) => Some(fact.unit()),
        ExecutionPayload::RateLimiter(fact) => Some(fact.unit()),
        _ => None,
    }
}

pub(super) fn fact_unit(stage_id: StageId, stage_name: &str, unit: &str) -> ControlUnit {
    if unit == stage_name {
        ControlUnit::Stage(stage_id)
    } else {
        ControlUnit::Composite(unit.into())
    }
}

fn unit_label(unit: &ControlUnit, stage_names: &HashMap<StageId, String>) -> Option<String> {
    match unit {
        ControlUnit::Stage(stage_id) => stage_names.get(stage_id).cloned(),
        ControlUnit::Composite(composite) => Some(composite.as_str().to_owned()),
    }
}

fn circuit_label(state: CircuitState) -> &'static str {
    match state {
        CircuitState::Closed => "closed",
        CircuitState::Open => "open",
        CircuitState::HalfOpen => "half_open",
    }
}
fn limiter_label(mode: RateLimiterMode) -> &'static str {
    match mode {
        RateLimiterMode::Normal => "normal",
        RateLimiterMode::Limiting => "limiting",
    }
}

impl MiddlewareView {
    pub(super) fn observe(&mut self, record: &ChainJournalRecord) {
        self.last_vector_clock = Some(record.envelope.provenance.journal.vector_clock.clone());
        let ChainPayload::Execution(middleware) = &record.payload else {
            return;
        };
        if !matches!(
            middleware,
            ExecutionPayload::CircuitBreaker(_) | ExecutionPayload::RateLimiter(_)
        ) {
            return;
        }
        let context = &record.envelope.provenance.event.flow_context;
        let stage_id = context.stage_id;
        self.stage_names
            .insert(stage_id, context.stage_name.clone());
        self.flow_id.get_or_insert_with(|| context.flow_id.clone());
        self.flow_name
            .get_or_insert_with(|| context.flow_name.clone());
        let Some(unit) = policy_unit(middleware) else {
            return;
        };
        let unit = fact_unit(stage_id, &context.stage_name, unit.as_str());
        let observed = (
            record.envelope.provenance.event.processing.event_time,
            record.local_sequence(),
        );
        match self.message(&unit, middleware) {
            Some(MiddlewareUpdate::CircuitBreaker(CircuitBreakerUpdate::StateChange {
                state_to,
                ..
            })) => {
                let state_to = state_to.to_owned();
                let entry = self.circuit_breakers.entry(unit.clone()).or_default();
                if entry
                    .state_updated_at_ms
                    .zip(entry.revision)
                    .is_none_or(|previous| observed > previous)
                {
                    entry.state = Some(state_to);
                    entry.state_updated_at_ms = Some(observed.0);
                    entry.revision = Some(observed.1);
                    self.writers.insert(unit, stage_id);
                }
            }
            Some(MiddlewareUpdate::RateLimiter(RateLimiterUpdate::ModeChange {
                mode_to, ..
            })) => {
                let mode = mode_to.to_owned();
                let entry = self.rate_limiters.entry(unit.clone()).or_default();
                if entry
                    .state_updated_at_ms
                    .zip(entry.revision)
                    .is_none_or(|previous| observed > previous)
                {
                    entry.mode = Some(mode);
                    entry.state_updated_at_ms = Some(observed.0);
                    entry.revision = Some(observed.1);
                    self.writers.insert(unit, stage_id);
                }
            }
            _ => {}
        }
    }

    pub(super) fn message<'a>(
        &'a self,
        unit: &ControlUnit,
        middleware: &'a ExecutionPayload,
    ) -> Option<MiddlewareUpdate<'a>> {
        Some(match middleware {
            ExecutionPayload::CircuitBreaker(event) => {
                let state_from = self
                    .circuit_breakers
                    .get(unit)
                    .and_then(|snapshot| snapshot.state.as_deref());
                MiddlewareUpdate::CircuitBreaker(match event {
                    CircuitBreakerFact::Opened {
                        cooldown_ms,
                        error_rate,
                        failure_count,
                        trigger,
                        observed_calls,
                        slow_call_rate,
                        slow_call_count,
                        last_error,
                        ..
                    } => CircuitBreakerUpdate::StateChange {
                        state_from,
                        state_to: "open",
                        context: CircuitTransition::Opened {
                            cooldown_ms: *cooldown_ms,
                            error_rate: *error_rate,
                            failure_count: *failure_count,
                            trigger: *trigger,
                            observed_calls: *observed_calls,
                            slow_call_rate: *slow_call_rate,
                            slow_call_count: *slow_call_count,
                            last_error: last_error.as_deref(),
                        },
                    },
                    CircuitBreakerFact::Closed {
                        success_count,
                        recovery_duration_ms,
                        ..
                    } => CircuitBreakerUpdate::StateChange {
                        state_from,
                        state_to: "closed",
                        context: CircuitTransition::Closed {
                            success_count: *success_count,
                            recovery_duration_ms: *recovery_duration_ms,
                        },
                    },
                    CircuitBreakerFact::HalfOpen {
                        test_request_count, ..
                    } => CircuitBreakerUpdate::StateChange {
                        state_from,
                        state_to: "half_open",
                        context: CircuitTransition::HalfOpen {
                            test_request_count: *test_request_count,
                        },
                    },
                    CircuitBreakerFact::StateChanged {
                        from_state,
                        to_state,
                        timestamp,
                        ..
                    } => CircuitBreakerUpdate::StateChange {
                        state_from: Some(circuit_label(*from_state)),
                        state_to: circuit_label(*to_state),
                        context: CircuitTransition::StateChanged {
                            timestamp: *timestamp,
                        },
                    },
                    _ => return None,
                })
            }
            ExecutionPayload::RateLimiter(RateLimiterFact::ModeChange {
                mode_from,
                mode_to,
                limit_rate,
                ..
            }) => MiddlewareUpdate::RateLimiter(RateLimiterUpdate::ModeChange {
                mode_from: limiter_label(*mode_from),
                mode_to: limiter_label(*mode_to),
                limit_rate: *limit_rate,
            }),
            _ => return None,
        })
    }

    /// The caller has already selected these families by capture scope, owner,
    /// kind and subject. No measurement reads or changes a factual revision.
    pub(super) fn measurements(&mut self, packet: &ObservabilityContext) -> Vec<SseFrame> {
        let Some(stage_id) = packet.capture.observer.as_stage().copied() else {
            return Vec::new();
        };
        let timestamp_ms = packet.capture.observed_at_ms;
        let stage_unit = ControlUnit::Stage(stage_id);
        let mut updates = Vec::new();
        if let Some(runtime) = &packet.runtime {
            if let Some(cb) = &runtime.circuit_breaker {
                let entry = self.circuit_breakers.entry(stage_unit.clone()).or_default();
                let totals = entry.totals.get_or_insert_with(Default::default);
                totals.successes_total = Some(cb.successes_total);
                totals.failures_total = Some(cb.failures_total);
                totals.opened_total = Some(cb.opened_total);
                totals.time_in_closed_s = Some(cb.time_closed_seconds);
                totals.time_in_open_s = Some(cb.time_open_seconds);
                totals.time_in_half_open_s = Some(cb.time_half_open_seconds);
                entry.totals_observed_at_ms = Some(timestamp_ms);
                updates.push(MiddlewareUpdate::CircuitBreaker(
                    CircuitBreakerUpdate::Measurements {
                        measurements: cb.clone(),
                    },
                ));
            }
            if let Some(rl) = &runtime.rate_limiter {
                let entry = self.rate_limiters.entry(stage_unit.clone()).or_default();
                if rl.bucket_capacity > 0.0 {
                    entry
                        .window
                        .get_or_insert_with(Default::default)
                        .utilization_pct =
                        Some((1.0 - rl.bucket_tokens / rl.bucket_capacity).clamp(0.0, 1.0) * 100.0);
                    entry.window_observed_at_ms = Some(timestamp_ms);
                }
                updates.push(MiddlewareUpdate::RateLimiter(
                    RateLimiterUpdate::Measurements {
                        measurements: rl.clone(),
                    },
                ));
            }
        }
        for record in &packet.records {
            match record {
                ObservationRecord::CircuitBreakerSummary {
                    effect_type: None,
                    window_duration_s,
                    requests_processed,
                    requests_rejected,
                    consecutive_failures,
                    rejection_rate,
                    successes_total,
                    failures_total,
                    opened_total,
                    time_in_closed_seconds,
                    time_in_open_seconds,
                    time_in_half_open_seconds,
                    ..
                } => {
                    let totals = CircuitTotals {
                        requests_processed: Some(*requests_processed),
                        requests_rejected: Some(*requests_rejected),
                        consecutive_failures: Some(*consecutive_failures),
                        rejection_rate: Some(*rejection_rate),
                        successes_total: Some(*successes_total),
                        failures_total: Some(*failures_total),
                        opened_total: Some(*opened_total),
                        time_in_closed_s: Some(*time_in_closed_seconds),
                        time_in_open_s: Some(*time_in_open_seconds),
                        time_in_half_open_s: Some(*time_in_half_open_seconds),
                    };
                    let entry = self.circuit_breakers.entry(stage_unit.clone()).or_default();
                    entry.totals = Some(totals.clone());
                    entry.totals_observed_at_ms = Some(timestamp_ms);
                    updates.push(MiddlewareUpdate::CircuitBreaker(
                        CircuitBreakerUpdate::Summary {
                            summary: CircuitSummary {
                                window_duration_s: *window_duration_s,
                                totals,
                            },
                        },
                    ));
                }
                ObservationRecord::RateLimiterUtilisation {
                    effect_type: None,
                    utilization_percent,
                    events_in_window,
                    window_size_ms,
                    ..
                } => {
                    let window = RateLimiterWindow {
                        utilization_pct: Some(*utilization_percent),
                        events_in_window: Some(*events_in_window),
                        window_size_ms: Some(*window_size_ms),
                    };
                    let entry = self.rate_limiters.entry(stage_unit.clone()).or_default();
                    entry.window = Some(window.clone());
                    entry.window_observed_at_ms = Some(timestamp_ms);
                    updates.push(MiddlewareUpdate::RateLimiter(
                        RateLimiterUpdate::WindowUtilization { window },
                    ));
                }
                ObservationRecord::RateLimiterActivity {
                    effect_type: None,
                    window_ms,
                    delayed_events,
                    delay_ms_total,
                    delay_ms_max,
                    limit_rate,
                    ..
                } => updates.push(MiddlewareUpdate::RateLimiter(
                    RateLimiterUpdate::ActivityPulse {
                        window_ms: *window_ms,
                        delayed_events: *delayed_events,
                        delay_ms_total: *delay_ms_total,
                        delay_ms_max: *delay_ms_max,
                        limit_rate: *limit_rate,
                    },
                )),
                ObservationRecord::BackpressureActivity {
                    window_ms,
                    delayed_events,
                    delay_ms_total,
                    delay_ms_max,
                    min_credit,
                    limiting_downstream_stage_id,
                } => updates.push(MiddlewareUpdate::Backpressure(
                    BackpressureUpdate::ActivityPulse {
                        window_ms: *window_ms,
                        delayed_events: *delayed_events,
                        delay_ms_total: *delay_ms_total,
                        delay_ms_max: *delay_ms_max,
                        context: BackpressureContext {
                            min_credit: *min_credit,
                            limiting_downstream_stage_id: limiting_downstream_stage_id
                                .map(|stage| stage.to_string()),
                        },
                    },
                )),
                _ => {}
            }
        }
        updates
            .into_iter()
            .map(|update| {
                StudioMessage::MiddlewareMeasurements {
                    stage_id,
                    update,
                    timestamp_ms,
                    capture: packet.capture,
                }
                .frame(None)
            })
            .collect()
    }

    pub(super) fn snapshot_frame(&self, timestamp_ms: u64) -> Option<SseFrame> {
        if self.circuit_breakers.is_empty() && self.rate_limiters.is_empty() {
            return None;
        }
        let units: BTreeSet<_> = self
            .circuit_breakers
            .keys()
            .chain(self.rate_limiters.keys())
            .cloned()
            .collect();
        let middleware = units
            .into_iter()
            .filter_map(|unit| {
                let stage_id = match &unit {
                    ControlUnit::Stage(stage_id) => *stage_id,
                    ControlUnit::Composite(_) => *self.writers.get(&unit)?,
                };
                Some(StageMiddlewareSnapshot {
                    stage_id,
                    stage_name: self.stage_names.get(&stage_id).map(String::as_str),
                    unit: unit_label(&unit, &self.stage_names),
                    circuit_breaker: self.circuit_breakers.get(&unit),
                    rate_limiter: self.rate_limiters.get(&unit),
                })
            })
            .collect();
        Some(
            StudioMessage::MiddlewareSnapshot(MiddlewareSnapshot {
                timestamp_ms,
                flow_id: self.flow_id.as_deref(),
                flow_name: self.flow_name.as_deref(),
                vector_clock: self.last_vector_clock.as_ref(),
                middleware,
            })
            .frame(None),
        )
    }
}
