// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Journal-derived middleware snapshots and payload translation.

use obzenflow_core::event::event_envelope::SystemEventEnvelope;
use obzenflow_core::web::SseFrame;
use std::collections::HashMap;

#[derive(Clone, Default)]
pub(super) struct MiddlewareSseState {
    flow_id: Option<String>,
    flow_name: Option<String>,
    stage_names: HashMap<obzenflow_core::StageId, String>,
    circuit_breakers: HashMap<obzenflow_core::StageId, CircuitBreakerSnapshot>,
    rate_limiters: HashMap<obzenflow_core::StageId, RateLimiterSnapshot>,
    pub(super) last_vector_clock: Option<obzenflow_core::event::vector_clock::VectorClock>,
}

#[derive(Clone)]
struct CircuitBreakerSnapshot {
    state: String,
    revision: u64,
    successes_total: Option<u64>,
    failures_total: Option<u64>,
    opened_total: Option<u64>,
    requests_processed: Option<u64>,
    requests_rejected: Option<u64>,
    rejection_rate: Option<f64>,
    consecutive_failures: Option<usize>,
    time_in_closed_s: Option<f64>,
    time_in_open_s: Option<f64>,
    time_in_half_open_s: Option<f64>,
}

#[derive(Clone)]
struct RateLimiterSnapshot {
    mode: String,
    revision: u64,
    utilization_pct: Option<f64>,
    events_in_window: Option<u64>,
    window_size_ms: Option<u64>,
}

impl MiddlewareSseState {
    pub(super) fn observe(&mut self, envelope: &SystemEventEnvelope) {
        self.last_vector_clock = Some(envelope.vector_clock.clone());

        let obzenflow_core::event::SystemEventType::MiddlewareLifecycle {
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

        self.observe_middleware_metadata(
            *stage_id,
            stage_name.as_deref(),
            flow_id.as_deref(),
            flow_name.as_deref(),
            None,
        );
        self.apply_middleware_event(*stage_id, origin.seq.0, middleware);
    }

    pub(super) fn observe_middleware_metadata(
        &mut self,
        stage_id: obzenflow_core::StageId,
        stage_name: Option<&str>,
        flow_id: Option<&str>,
        flow_name: Option<&str>,
        vector_clock: Option<&obzenflow_core::event::vector_clock::VectorClock>,
    ) {
        if let Some(name) = stage_name {
            self.stage_names.insert(stage_id, name.to_string());
        }
        if self.flow_id.is_none() {
            if let Some(id) = flow_id {
                self.flow_id = Some(id.to_string());
            }
        }
        if self.flow_name.is_none() {
            if let Some(name) = flow_name {
                self.flow_name = Some(name.to_string());
            }
        }
        if let Some(vc) = vector_clock {
            self.last_vector_clock = Some(vc.clone());
        }
    }

    pub(super) fn project_middleware_event(
        &mut self,
        stage_id: obzenflow_core::StageId,
        revision: u64,
        middleware: &obzenflow_core::event::payloads::observability_payload::MiddlewareLifecycle,
    ) -> Option<serde_json::Value> {
        use obzenflow_core::event::payloads::observability_payload::{
            CircuitBreakerEvent, MiddlewareLifecycle, RateLimiterEvent,
        };
        use serde_json::json;

        match middleware {
            MiddlewareLifecycle::CircuitBreaker(cb) => {
                let previous_state = self
                    .circuit_breakers
                    .get(&stage_id)
                    .map(|snapshot| snapshot.state.clone());

                match cb {
                    CircuitBreakerEvent::Opened {
                        error_rate,
                        failure_count,
                        trigger,
                        observed_calls,
                        slow_call_rate,
                        slow_call_count,
                        last_error,
                    } => {
                        let state_to = "open".to_string();

                        self.apply_middleware_event(stage_id, revision, middleware);

                        let mut payload = json!({
                            "middleware": "circuit_breaker",
                            "event_type": "state_change",
                            "state_to": state_to,
                            "context": {
                                "error_rate": error_rate,
                                "failure_count": failure_count,
                                "trigger": trigger,
                                "observed_calls": observed_calls,
                            }
                        });
                        if let Some(from) = previous_state {
                            payload["state_from"] = json!(from);
                        }
                        if let Some(err) = last_error {
                            payload["context"]["last_error"] = json!(err);
                        }
                        if let Some(slow_call_rate) = slow_call_rate {
                            payload["context"]["slow_call_rate"] = json!(slow_call_rate);
                        }
                        if let Some(slow_call_count) = slow_call_count {
                            payload["context"]["slow_call_count"] = json!(slow_call_count);
                        }
                        Some(payload)
                    }
                    CircuitBreakerEvent::Closed {
                        success_count,
                        recovery_duration_ms,
                    } => {
                        let state_to = "closed".to_string();

                        self.apply_middleware_event(stage_id, revision, middleware);

                        let mut payload = json!({
                            "middleware": "circuit_breaker",
                            "event_type": "state_change",
                            "state_to": state_to,
                            "context": {
                                "success_count": success_count,
                                "recovery_duration_ms": recovery_duration_ms,
                            }
                        });
                        if let Some(from) = previous_state {
                            payload["state_from"] = json!(from);
                        }
                        Some(payload)
                    }
                    CircuitBreakerEvent::HalfOpen { test_request_count } => {
                        let state_to = "half_open".to_string();

                        self.apply_middleware_event(stage_id, revision, middleware);

                        let mut payload = json!({
                            "middleware": "circuit_breaker",
                            "event_type": "state_change",
                            "state_to": state_to,
                            "context": {
                                "test_request_count": test_request_count,
                            }
                        });
                        if let Some(from) = previous_state {
                            payload["state_from"] = json!(from);
                        }
                        Some(payload)
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
                    } => {
                        let current_state = normalize_circuit_state(state);

                        self.apply_middleware_event(stage_id, revision, middleware);

                        Some(json!({
                            "middleware": "circuit_breaker",
                            "event_type": "summary",
                            "current_state": current_state,
                            "summary": {
                                "window_duration_s": window_duration_s,
                                "requests_processed": requests_processed,
                                "requests_rejected": requests_rejected,
                                "consecutive_failures": consecutive_failures,
                                "rejection_rate": rejection_rate,
                                "successes_total": successes_total,
                                "failures_total": failures_total,
                                "opened_total": opened_total,
                                "time_in_closed_s": time_in_closed_seconds,
                                "time_in_open_s": time_in_open_seconds,
                                "time_in_half_open_s": time_in_half_open_seconds,
                            }
                        }))
                    }
                    // High-volume (not mirrored) or unsupported variants.
                    _ => None,
                }
            }
            MiddlewareLifecycle::RateLimiter(rl) => match rl {
                RateLimiterEvent::ActivityPulse {
                    window_ms,
                    delayed_events,
                    delay_ms_total,
                    delay_ms_max,
                    limit_rate,
                } => Some(json!({
                    "middleware": "rate_limiter",
                    "event_type": "activity_pulse",
                    "window_ms": window_ms,
                    "delayed_events": delayed_events,
                    "delay_ms_total": delay_ms_total,
                    "delay_ms_max": delay_ms_max,
                    "limit_rate": limit_rate,
                })),
                RateLimiterEvent::ModeChange {
                    mode_from,
                    mode_to,
                    limit_rate,
                } => {
                    self.apply_middleware_event(stage_id, revision, middleware);

                    Some(json!({
                        "middleware": "rate_limiter",
                        "event_type": "mode_change",
                        "mode_from": mode_from,
                        "mode_to": mode_to,
                        "limit_rate": limit_rate,
                    }))
                }
                RateLimiterEvent::WindowUtilization {
                    utilization_percent,
                    events_in_window,
                    window_size_ms,
                } => {
                    self.apply_middleware_event(stage_id, revision, middleware);

                    let mode = self
                        .rate_limiters
                        .get(&stage_id)
                        .map(|snapshot| snapshot.mode.clone())
                        .unwrap_or_else(|| "normal".to_string());

                    Some(json!({
                        "middleware": "rate_limiter",
                        "event_type": "window_utilization",
                        "utilization_pct": utilization_percent,
                        "events_in_window": events_in_window,
                        "window_size_ms": window_size_ms,
                        "mode": mode,
                    }))
                }
                _ => None,
            },
        }
    }

    fn apply_middleware_event(
        &mut self,
        stage_id: obzenflow_core::StageId,
        revision: u64,
        middleware: &obzenflow_core::event::payloads::observability_payload::MiddlewareLifecycle,
    ) {
        use obzenflow_core::event::payloads::observability_payload::{
            CircuitBreakerEvent, MiddlewareLifecycle, RateLimiterEvent,
        };

        match middleware {
            MiddlewareLifecycle::CircuitBreaker(cb) => {
                match cb {
                    CircuitBreakerEvent::Opened { .. } => {
                        let entry = self.circuit_breakers.entry(stage_id).or_insert(
                            CircuitBreakerSnapshot {
                                state: "open".to_string(),
                                revision,
                                successes_total: None,
                                failures_total: None,
                                opened_total: None,
                                requests_processed: None,
                                requests_rejected: None,
                                rejection_rate: None,
                                consecutive_failures: None,
                                time_in_closed_s: None,
                                time_in_open_s: None,
                                time_in_half_open_s: None,
                            },
                        );
                        entry.state = "open".to_string();
                        entry.revision = revision;
                    }
                    CircuitBreakerEvent::Closed { .. } => {
                        let entry = self.circuit_breakers.entry(stage_id).or_insert(
                            CircuitBreakerSnapshot {
                                state: "closed".to_string(),
                                revision,
                                successes_total: None,
                                failures_total: None,
                                opened_total: None,
                                requests_processed: None,
                                requests_rejected: None,
                                rejection_rate: None,
                                consecutive_failures: None,
                                time_in_closed_s: None,
                                time_in_open_s: None,
                                time_in_half_open_s: None,
                            },
                        );
                        entry.state = "closed".to_string();
                        entry.revision = revision;
                    }
                    CircuitBreakerEvent::HalfOpen { .. } => {
                        let entry = self.circuit_breakers.entry(stage_id).or_insert(
                            CircuitBreakerSnapshot {
                                state: "half_open".to_string(),
                                revision,
                                successes_total: None,
                                failures_total: None,
                                opened_total: None,
                                requests_processed: None,
                                requests_rejected: None,
                                rejection_rate: None,
                                consecutive_failures: None,
                                time_in_closed_s: None,
                                time_in_open_s: None,
                                time_in_half_open_s: None,
                            },
                        );
                        entry.state = "half_open".to_string();
                        entry.revision = revision;
                    }
                    CircuitBreakerEvent::Summary {
                        state,
                        requests_processed,
                        requests_rejected,
                        rejection_rate,
                        consecutive_failures,
                        successes_total,
                        failures_total,
                        opened_total,
                        time_in_closed_seconds,
                        time_in_open_seconds,
                        time_in_half_open_seconds,
                        ..
                    } => {
                        self.circuit_breakers.insert(
                            stage_id,
                            CircuitBreakerSnapshot {
                                state: normalize_circuit_state(state),
                                revision,
                                successes_total: Some(*successes_total),
                                failures_total: Some(*failures_total),
                                opened_total: Some(*opened_total),
                                requests_processed: Some(*requests_processed),
                                requests_rejected: Some(*requests_rejected),
                                rejection_rate: Some(*rejection_rate),
                                consecutive_failures: Some(*consecutive_failures),
                                time_in_closed_s: Some(*time_in_closed_seconds),
                                time_in_open_s: Some(*time_in_open_seconds),
                                time_in_half_open_s: Some(*time_in_half_open_seconds),
                            },
                        );
                    }
                    _ => {}
                }
            }
            MiddlewareLifecycle::RateLimiter(rl) => match rl {
                RateLimiterEvent::ModeChange { mode_to, .. } => {
                    let entry = self
                        .rate_limiters
                        .entry(stage_id)
                        .or_insert(RateLimiterSnapshot {
                            mode: mode_to.clone(),
                            revision,
                            utilization_pct: None,
                            events_in_window: None,
                            window_size_ms: None,
                        });
                    entry.mode = mode_to.clone();
                    entry.revision = revision;
                }
                RateLimiterEvent::WindowUtilization {
                    utilization_percent,
                    events_in_window,
                    window_size_ms,
                } => {
                    let mode = self
                        .rate_limiters
                        .get(&stage_id)
                        .map(|snapshot| snapshot.mode.clone())
                        .unwrap_or_else(|| "normal".to_string());

                    self.rate_limiters.insert(
                        stage_id,
                        RateLimiterSnapshot {
                            mode,
                            revision,
                            utilization_pct: Some(*utilization_percent),
                            events_in_window: Some(*events_in_window),
                            window_size_ms: Some(*window_size_ms),
                        },
                    );
                }
                _ => {}
            },
        }
    }

    pub(super) fn build_snapshot_sse_event(&self, timestamp_ms: u64) -> Option<SseFrame> {
        use serde_json::json;

        if self.circuit_breakers.is_empty() && self.rate_limiters.is_empty() {
            return None;
        }

        let mut middleware = Vec::new();

        let mut stage_ids: std::collections::BTreeSet<obzenflow_core::StageId> =
            std::collections::BTreeSet::new();
        stage_ids.extend(self.circuit_breakers.keys().copied());
        stage_ids.extend(self.rate_limiters.keys().copied());

        for stage_id in stage_ids {
            let mut stage_obj = json!({
                "stage_id": stage_id.to_string(),
            });

            if let Some(stage_name) = self.stage_names.get(&stage_id) {
                stage_obj["stage_name"] = json!(stage_name);
            }

            if let Some(cb) = self.circuit_breakers.get(&stage_id) {
                let mut cb_obj = json!({
                    "state": cb.state,
                    "revision": cb.revision,
                });
                if let Some(v) = cb.successes_total {
                    cb_obj["successes_total"] = json!(v);
                }
                if let Some(v) = cb.failures_total {
                    cb_obj["failures_total"] = json!(v);
                }
                if let Some(v) = cb.opened_total {
                    cb_obj["opened_total"] = json!(v);
                }
                if let Some(v) = cb.requests_processed {
                    cb_obj["requests_processed"] = json!(v);
                }
                if let Some(v) = cb.requests_rejected {
                    cb_obj["requests_rejected"] = json!(v);
                }
                if let Some(v) = cb.rejection_rate {
                    cb_obj["rejection_rate"] = json!(v);
                }
                if let Some(v) = cb.consecutive_failures {
                    cb_obj["consecutive_failures"] = json!(v);
                }
                if let Some(v) = cb.time_in_closed_s {
                    cb_obj["time_in_closed_s"] = json!(v);
                }
                if let Some(v) = cb.time_in_open_s {
                    cb_obj["time_in_open_s"] = json!(v);
                }
                if let Some(v) = cb.time_in_half_open_s {
                    cb_obj["time_in_half_open_s"] = json!(v);
                }
                stage_obj["circuit_breaker"] = cb_obj;
            }

            if let Some(rl) = self.rate_limiters.get(&stage_id) {
                let mut rl_obj = json!({
                    "mode": rl.mode,
                    "revision": rl.revision,
                });
                if let Some(v) = rl.utilization_pct {
                    rl_obj["utilization_pct"] = json!(v);
                }
                if let Some(v) = rl.events_in_window {
                    rl_obj["events_in_window"] = json!(v);
                }
                if let Some(v) = rl.window_size_ms {
                    rl_obj["window_size_ms"] = json!(v);
                }
                stage_obj["rate_limiter"] = rl_obj;
            }

            middleware.push(stage_obj);
        }

        let mut data = json!({
            "timestamp_ms": timestamp_ms,
            "middleware": middleware,
        });

        if let Some(flow_id) = &self.flow_id {
            data["flow_id"] = json!(flow_id);
        }
        if let Some(flow_name) = &self.flow_name {
            data["flow_name"] = json!(flow_name);
        }
        if let Some(vc) = &self.last_vector_clock {
            if let Ok(v) = serde_json::to_value(vc) {
                data["vector_clock"] = v;
            }
        }

        Some(SseFrame::event(
            "middleware_state_snapshot",
            data.to_string(),
        ))
    }
}

fn normalize_circuit_state(state: &str) -> String {
    let lower = state.to_ascii_lowercase();
    match lower.as_str() {
        "closed" => "closed".to_string(),
        "open" => "open".to_string(),
        "halfopen" | "half_open" | "half-open" => "half_open".to_string(),
        other => other.to_string(),
    }
}
