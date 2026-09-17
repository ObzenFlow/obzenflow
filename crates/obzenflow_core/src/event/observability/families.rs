// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared family selection for live retention and journal attachment indexing.
use super::*;
use crate::StageId;
const MAX_PACKET_FAMILIES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ObservationKind {
    RuntimeSnapshot,
    InFlight,
    JoinReference,
    StateAge,
    EventLoops,
    WorkLoops,
    Timing,
    CircuitBreaker,
    RateLimiter,
    EffectCircuitBreaker(String),
    EffectRateLimiter(String),
    ProcessingTime,
    Metrics,
    Sli,
    Llm,
    BreakerSummary(Option<String>),
    LimiterActivity(Option<String>),
    LimiterUtilisation(Option<String>),
    Backpressure,
    Resource,
    HttpPull,
    AiChunking,
    HttpSurface,
    StageHeartbeat,
    EdgeLiveness(StageId, StageId),
}

pub fn observation_families(
    packet: ObservabilityContext,
) -> Option<Vec<(ObservationKind, ObservabilityContext)>> {
    let mut families = Vec::new();
    let stamp = packet.capture;
    if let Some(snapshot) = packet.runtime_snapshot {
        let mut part = ObservabilityContext::new(snapshot.capture);
        part.runtime_snapshot = Some(snapshot);
        families.push((ObservationKind::RuntimeSnapshot, part));
    }
    if let Some(runtime) = packet.runtime {
        macro_rules! field {
            ($name:ident, $kind:ident) => {
                if let Some(value) = runtime.$name {
                    let mut part = ObservabilityContext::new(stamp);
                    part.runtime = Some(RuntimeObservability {
                        $name: Some(value),
                        ..Default::default()
                    });
                    families.push((ObservationKind::$kind, part));
                }
            };
        }
        field!(in_flight, InFlight);
        field!(join_reference_since_last_stream, JoinReference);
        field!(time_in_state_ms, StateAge);
        field!(event_loops_total, EventLoops);
        field!(event_loops_with_work_total, WorkLoops);
        if let Some(timing) = runtime.timing {
            if timing.is_valid() {
                let mut part = ObservabilityContext::new(stamp);
                part.runtime = Some(RuntimeObservability {
                    timing: Some(timing),
                    ..Default::default()
                });
                families.push((ObservationKind::Timing, part));
            }
        }
        field!(circuit_breaker, CircuitBreaker);
        field!(rate_limiter, RateLimiter);
        for value in runtime.effect_circuit_breakers {
            let key = ObservationKind::EffectCircuitBreaker(value.effect_type.clone());
            let mut part = ObservabilityContext::new(stamp);
            part.runtime = Some(RuntimeObservability {
                effect_circuit_breakers: vec![value],
                ..Default::default()
            });
            families.push((key, part));
        }
        for value in runtime.effect_rate_limiters {
            let key = ObservationKind::EffectRateLimiter(value.effect_type.clone());
            let mut part = ObservabilityContext::new(stamp);
            part.runtime = Some(RuntimeObservability {
                effect_rate_limiters: vec![value],
                ..Default::default()
            });
            families.push((key, part));
        }
    }
    macro_rules! field {
        ($name:ident, $kind:ident) => {
            if let Some(value) = packet.$name {
                let mut part = ObservabilityContext::new(stamp);
                part.$name = Some(value);
                families.push((ObservationKind::$kind, part));
            }
        };
    }
    field!(processing_time, ProcessingTime);
    field!(metrics, Metrics);
    field!(sli, Sli);
    for record in packet.records {
        let kind = match &record {
            ObservationRecord::Llm { .. } => ObservationKind::Llm,
            ObservationRecord::CircuitBreakerSummary { effect_type, .. } => {
                ObservationKind::BreakerSummary(effect_type.clone())
            }
            ObservationRecord::RateLimiterActivity { effect_type, .. } => {
                ObservationKind::LimiterActivity(effect_type.clone())
            }
            ObservationRecord::RateLimiterUtilisation { effect_type, .. } => {
                ObservationKind::LimiterUtilisation(effect_type.clone())
            }
            ObservationRecord::BackpressureActivity { .. } => ObservationKind::Backpressure,
            ObservationRecord::ResourceUsage { .. } => ObservationKind::Resource,
            ObservationRecord::HttpPull(_) => ObservationKind::HttpPull,
            ObservationRecord::AiChunkingWork { .. } => ObservationKind::AiChunking,
            ObservationRecord::HttpSurface { .. } => ObservationKind::HttpSurface,
            ObservationRecord::StageHeartbeat { .. } => ObservationKind::StageHeartbeat,
            ObservationRecord::EdgeLiveness {
                upstream, reader, ..
            } => ObservationKind::EdgeLiveness(*upstream, *reader),
        };
        let mut part = ObservabilityContext::new(stamp);
        part.records.push(record);
        families.push((kind, part));
    }
    (families.len() <= MAX_PACKET_FAMILIES).then_some(families)
}
