// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The same optional-evidence limits apply to attachments and live handoff.

use super::{ObservabilityContext, ObservationRecord};
use std::io::Write;

const MAX_PACKET_BYTES: usize = 64 * 1024;
const MAX_PACKET_FAMILIES: usize = 128;

fn finite(values: &[f64]) -> bool {
    values.iter().all(|value| value.is_finite())
}

impl ObservabilityContext {
    /// Invalid or oversized measurements cannot make their factual carrier fail.
    /// A malformed timing bundle is omitted without discarding other families.
    pub fn validated(mut self) -> Option<Self> {
        let mut families = self.records.len()
            + usize::from(self.runtime_snapshot.is_some())
            + usize::from(self.processing_time.is_some())
            + usize::from(self.metrics.is_some())
            + usize::from(self.sli.is_some());
        if let Some(runtime) = &mut self.runtime {
            runtime.timing = runtime.timing.take().filter(|timing| timing.is_valid());
            families += [
                runtime.in_flight.is_some(),
                runtime.join_reference_since_last_stream.is_some(),
                runtime.time_in_state_ms.is_some(),
                runtime.event_loops_total.is_some(),
                runtime.event_loops_with_work_total.is_some(),
                runtime.timing.is_some(),
                runtime.circuit_breaker.is_some(),
                runtime.rate_limiter.is_some(),
            ]
            .into_iter()
            .filter(|present| *present)
            .count()
                + runtime.effect_circuit_breakers.len()
                + runtime.effect_rate_limiters.len();
            if runtime.circuit_breaker.as_ref().is_some_and(|cb| {
                !finite(&[
                    cb.time_closed_seconds,
                    cb.time_open_seconds,
                    cb.time_half_open_seconds,
                ])
            }) || runtime.rate_limiter.as_ref().is_some_and(|rl| {
                !finite(&[
                    rl.tokens_consumed_total,
                    rl.delay_seconds_total,
                    rl.bucket_tokens,
                    rl.bucket_capacity,
                ])
            }) || runtime.effect_circuit_breakers.iter().any(|cb| {
                !finite(&[
                    cb.cb_time_closed_seconds,
                    cb.cb_time_open_seconds,
                    cb.cb_time_half_open_seconds,
                    cb.cb_state,
                ])
            }) || runtime.effect_rate_limiters.iter().any(|rl| {
                !finite(&[
                    rl.rl_tokens_consumed_total,
                    rl.rl_delay_seconds_total,
                    rl.rl_bucket_tokens,
                    rl.rl_bucket_capacity,
                ])
            }) {
                return None;
            }
        }
        if families == 0 || families > MAX_PACKET_FAMILIES {
            return None;
        }
        if self.metrics.as_ref().is_some_and(|metrics| {
            !finite(&[
                metrics.processing_rate,
                metrics.error_rate,
                metrics.latency_p50_ms,
                metrics.latency_p99_ms,
            ])
        }) || self.sli.as_ref().is_some_and(|sli| {
            !finite(&[
                sli.availability,
                sli.error_budget_remaining,
                sli.latency_budget_used,
            ])
        }) {
            return None;
        }
        for record in &self.records {
            let valid = match record {
                ObservationRecord::CircuitBreakerSummary {
                    rejection_rate,
                    time_in_closed_seconds,
                    time_in_open_seconds,
                    time_in_half_open_seconds,
                    ..
                } => finite(&[
                    *rejection_rate,
                    *time_in_closed_seconds,
                    *time_in_open_seconds,
                    *time_in_half_open_seconds,
                ]),
                ObservationRecord::RateLimiterActivity { limit_rate, .. } => limit_rate.is_finite(),
                ObservationRecord::RateLimiterUtilisation {
                    utilization_percent,
                    ..
                } => utilization_percent.is_finite(),
                ObservationRecord::ResourceUsage { cpu_percent, .. } => cpu_percent.is_finite(),
                ObservationRecord::HttpPull(http) => finite(&[
                    http.wait_seconds_rate_limit,
                    http.wait_seconds_poll_interval,
                    http.wait_seconds_backoff,
                ]),
                ObservationRecord::Llm { .. }
                | ObservationRecord::BackpressureActivity { .. }
                | ObservationRecord::AiChunkingWork { .. }
                | ObservationRecord::StageHeartbeat { .. }
                | ObservationRecord::EdgeLiveness { .. }
                | ObservationRecord::HttpSurface { .. } => true,
            };
            if !valid {
                return None;
            }
        }
        serde_json::to_writer(SizeLimit(MAX_PACKET_BYTES), &self).ok()?;
        Some(self)
    }
}

struct SizeLimit(usize);
impl Write for SizeLimit {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.0 {
            return Err(std::io::Error::other("observation packet limit"));
        }
        self.0 -= bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
