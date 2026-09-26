// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Factual revisions and selected measurements are independent Studio updates.

use super::messages::*;
use obzenflow_core::event::observability::{ObservabilityContext, ObservationRecord};
use obzenflow_core::event::payloads::execution_payload::{
    CircuitBreakerFact, CircuitState, MiddlewareFact, RateLimiterFact, RateLimiterMode,
};
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::SupervisorRecord;
use obzenflow_core::event::SystemPayload;
use obzenflow_core::{web::SseFrame, StageId};
use std::collections::{BTreeSet, HashMap};

#[derive(Clone, Default)]
pub(super) struct MiddlewareView {
    flow_id: Option<String>,
    flow_name: Option<String>,
    stage_names: HashMap<StageId, String>,
    circuit_breakers: HashMap<StageId, CircuitBreakerSnapshot>,
    rate_limiters: HashMap<StageId, RateLimiterSnapshot>,
    last_vector_clock: Option<VectorClock>,
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
    pub(super) fn observe(&mut self, record: &SupervisorRecord) {
        self.last_vector_clock = Some(record.journal().vector_clock.clone());
        let SystemPayload::MiddlewareLifecycle {
            stage_id,
            stage_name,
            flow_id,
            flow_name,
            origin,
            middleware,
        } = &record.payload
        else {
            return;
        };
        if let Some(name) = stage_name {
            self.stage_names.insert(*stage_id, name.clone());
        }
        if self.flow_id.is_none() {
            self.flow_id.clone_from(flow_id);
        }
        if self.flow_name.is_none() {
            self.flow_name.clone_from(flow_name);
        }
        let revision = origin.seq.0;
        match self.message(*stage_id, middleware) {
            Some(MiddlewareUpdate::CircuitBreaker(CircuitBreakerUpdate::StateChange {
                state_to,
                ..
            })) => {
                let entry = self.circuit_breakers.entry(*stage_id).or_default();
                if entry.revision.is_none_or(|previous| revision > previous) {
                    entry.state = Some(state_to.into());
                    entry.revision = Some(revision);
                    entry.state_updated_at_ms = Some(record.timestamp());
                }
            }
            Some(MiddlewareUpdate::RateLimiter(RateLimiterUpdate::ModeChange {
                mode_to, ..
            })) => {
                let mode = mode_to.to_owned();
                let entry = self.rate_limiters.entry(*stage_id).or_default();
                if entry.revision.is_none_or(|previous| revision > previous) {
                    entry.mode = Some(mode);
                    entry.revision = Some(revision);
                    entry.state_updated_at_ms = Some(record.timestamp());
                }
            }
            _ => {}
        }
    }

    pub(super) fn message<'a>(
        &'a self,
        stage_id: StageId,
        middleware: &'a MiddlewareFact,
    ) -> Option<MiddlewareUpdate<'a>> {
        Some(match middleware {
            MiddlewareFact::CircuitBreaker(event) => {
                let state_from = self
                    .circuit_breakers
                    .get(&stage_id)
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
                    } => CircuitBreakerUpdate::StateChange {
                        state_from,
                        state_to: "closed",
                        context: CircuitTransition::Closed {
                            success_count: *success_count,
                            recovery_duration_ms: *recovery_duration_ms,
                        },
                    },
                    CircuitBreakerFact::HalfOpen { test_request_count } => {
                        CircuitBreakerUpdate::StateChange {
                            state_from,
                            state_to: "half_open",
                            context: CircuitTransition::HalfOpen {
                                test_request_count: *test_request_count,
                            },
                        }
                    }
                    CircuitBreakerFact::StateChanged {
                        from_state,
                        to_state,
                        timestamp,
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
            MiddlewareFact::RateLimiter(RateLimiterFact::ModeChange {
                mode_from,
                mode_to,
                limit_rate,
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
        let mut updates = Vec::new();
        if let Some(runtime) = &packet.runtime {
            if let Some(cb) = &runtime.circuit_breaker {
                let entry = self.circuit_breakers.entry(stage_id).or_default();
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
                let entry = self.rate_limiters.entry(stage_id).or_default();
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
                    let entry = self.circuit_breakers.entry(stage_id).or_default();
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
                } => {
                    let window = RateLimiterWindow {
                        utilization_pct: Some(*utilization_percent),
                        events_in_window: Some(*events_in_window),
                        window_size_ms: Some(*window_size_ms),
                    };
                    let entry = self.rate_limiters.entry(stage_id).or_default();
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
        let stage_ids: BTreeSet<_> = self
            .circuit_breakers
            .keys()
            .chain(self.rate_limiters.keys())
            .copied()
            .collect();
        let middleware = stage_ids
            .into_iter()
            .map(|stage_id| StageMiddlewareSnapshot {
                stage_id,
                stage_name: self.stage_names.get(&stage_id).map(String::as_str),
                circuit_breaker: self.circuit_breakers.get(&stage_id),
                rate_limiter: self.rate_limiters.get(&stage_id),
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
