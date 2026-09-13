// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Builds Studio's circuit breaker and rate limiter messages, and keeps their
//! latest state so a browser can display it without waiting for another change.

use super::messages::{
    CircuitBreakerSnapshot, CircuitBreakerUpdate, CircuitSummary, CircuitTotals, CircuitTransition,
    MiddlewareSnapshot, MiddlewareUpdate, RateLimiterSnapshot, RateLimiterUpdate,
    RateLimiterWindow, StageMiddlewareSnapshot, StudioMessage,
};
use obzenflow_core::event::{
    event_envelope::SystemEventEnvelope,
    payloads::observability_payload::{CircuitBreakerEvent, MiddlewareLifecycle, RateLimiterEvent},
    vector_clock::VectorClock,
    SystemEventType,
};
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

impl MiddlewareView {
    pub(super) fn observe(&mut self, envelope: &SystemEventEnvelope) {
        self.last_vector_clock = Some(envelope.vector_clock.clone());
        let SystemEventType::MiddlewareLifecycle {
            stage_id,
            stage_name,
            flow_id,
            flow_name,
            origin,
            middleware,
        } = &envelope.event.event
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
                let entry = self.circuit_breakers.entry(*stage_id).or_insert_with(|| {
                    CircuitBreakerSnapshot {
                        state: state_to.into(),
                        revision,
                        totals: None,
                    }
                });
                entry.state = state_to.into();
                entry.revision = revision;
            }
            Some(MiddlewareUpdate::CircuitBreaker(CircuitBreakerUpdate::Summary {
                current_state,
                summary,
            })) => {
                self.circuit_breakers.insert(
                    *stage_id,
                    CircuitBreakerSnapshot {
                        state: current_state,
                        revision,
                        totals: Some(summary.totals),
                    },
                );
            }
            Some(MiddlewareUpdate::RateLimiter(RateLimiterUpdate::ModeChange {
                mode_to, ..
            })) => {
                let mode = mode_to.to_owned();
                let entry =
                    self.rate_limiters
                        .entry(*stage_id)
                        .or_insert_with(|| RateLimiterSnapshot {
                            mode: mode.clone(),
                            revision,
                            window: None,
                        });
                entry.mode = mode;
                entry.revision = revision;
            }
            Some(MiddlewareUpdate::RateLimiter(RateLimiterUpdate::WindowUtilization {
                mode,
                window,
            })) => {
                let mode = mode.to_owned();
                self.rate_limiters.insert(
                    *stage_id,
                    RateLimiterSnapshot {
                        mode,
                        revision,
                        window: Some(window),
                    },
                );
            }
            Some(MiddlewareUpdate::RateLimiter(RateLimiterUpdate::ActivityPulse { .. })) | None => {
            }
        }
    }

    pub(super) fn message<'a>(
        &'a self,
        stage_id: StageId,
        middleware: &'a MiddlewareLifecycle,
    ) -> Option<MiddlewareUpdate<'a>> {
        Some(match middleware {
            MiddlewareLifecycle::CircuitBreaker(event) => {
                let state_from = self
                    .circuit_breakers
                    .get(&stage_id)
                    .map(|snapshot| snapshot.state.as_str());
                MiddlewareUpdate::CircuitBreaker(match event {
                    CircuitBreakerEvent::Opened {
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
                            error_rate: *error_rate,
                            failure_count: *failure_count,
                            trigger: *trigger,
                            observed_calls: *observed_calls,
                            slow_call_rate: *slow_call_rate,
                            slow_call_count: *slow_call_count,
                            last_error: last_error.as_deref(),
                        },
                    },
                    CircuitBreakerEvent::Closed {
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
                    CircuitBreakerEvent::HalfOpen { test_request_count } => {
                        CircuitBreakerUpdate::StateChange {
                            state_from,
                            state_to: "half_open",
                            context: CircuitTransition::HalfOpen {
                                test_request_count: *test_request_count,
                            },
                        }
                    }
                    CircuitBreakerEvent::Summary {
                        window_duration_s,
                        requests_processed,
                        requests_rejected,
                        state,
                        consecutive_failures,
                        rejection_rate,
                        successes_total,
                        failures_total,
                        opened_total,
                        time_in_closed_seconds,
                        time_in_open_seconds,
                        time_in_half_open_seconds,
                    } => CircuitBreakerUpdate::Summary {
                        current_state: normalize_circuit_state(state),
                        summary: CircuitSummary {
                            window_duration_s: *window_duration_s,
                            totals: CircuitTotals {
                                requests_processed: *requests_processed,
                                requests_rejected: *requests_rejected,
                                consecutive_failures: *consecutive_failures,
                                rejection_rate: *rejection_rate,
                                successes_total: *successes_total,
                                failures_total: *failures_total,
                                opened_total: *opened_total,
                                time_in_closed_s: *time_in_closed_seconds,
                                time_in_open_s: *time_in_open_seconds,
                                time_in_half_open_s: *time_in_half_open_seconds,
                            },
                        },
                    },
                    _ => return None,
                })
            }
            MiddlewareLifecycle::RateLimiter(event) => MiddlewareUpdate::RateLimiter(match event {
                RateLimiterEvent::ActivityPulse {
                    window_ms,
                    delayed_events,
                    delay_ms_total,
                    delay_ms_max,
                    limit_rate,
                } => RateLimiterUpdate::ActivityPulse {
                    window_ms: *window_ms,
                    delayed_events: *delayed_events,
                    delay_ms_total: *delay_ms_total,
                    delay_ms_max: *delay_ms_max,
                    limit_rate: *limit_rate,
                },
                RateLimiterEvent::ModeChange {
                    mode_from,
                    mode_to,
                    limit_rate,
                } => RateLimiterUpdate::ModeChange {
                    mode_from,
                    mode_to,
                    limit_rate: *limit_rate,
                },
                RateLimiterEvent::WindowUtilization {
                    utilization_percent,
                    events_in_window,
                    window_size_ms,
                } => RateLimiterUpdate::WindowUtilization {
                    mode: self
                        .rate_limiters
                        .get(&stage_id)
                        .map(|snapshot| snapshot.mode.as_str())
                        .unwrap_or("normal"),
                    window: RateLimiterWindow {
                        utilization_pct: *utilization_percent,
                        events_in_window: *events_in_window,
                        window_size_ms: *window_size_ms,
                    },
                },
                _ => return None,
            }),
        })
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

fn normalize_circuit_state(state: &str) -> String {
    let lower = state.to_ascii_lowercase();
    match lower.as_str() {
        "halfopen" | "half_open" | "half-open" => "half_open".to_owned(),
        _ => lower,
    }
}
