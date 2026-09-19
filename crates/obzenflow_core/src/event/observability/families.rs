// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared family selection for live retention and journal attachment indexing.
use super::*;
use crate::StageId;
use std::borrow::Cow;

const MAX_PACKET_FAMILIES: usize = 128;

/// An internal retention key, named by the selected Core field or record variant.
/// These names are not journal or HTTP/SSE wire tags. They only identify samples.
/// Live keys borrow static names; deserialized checkpoint keys own their names.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationFamily {
    pub name: Cow<'static, str>,
    pub subject: ObservationSubject,
}

impl ObservationFamily {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name: Cow::Borrowed(name),
            subject: ObservationSubject::None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ObservationSubject {
    None,
    Effect(String),
    Edge { upstream: StageId, reader: StageId },
}

fn record_family(record: &ObservationRecord) -> ObservationFamily {
    let subject = match record {
        ObservationRecord::CircuitBreakerSummary { effect_type, .. }
        | ObservationRecord::RateLimiterActivity { effect_type, .. }
        | ObservationRecord::RateLimiterUtilisation { effect_type, .. } => effect_type
            .clone()
            .map(ObservationSubject::Effect)
            .unwrap_or(ObservationSubject::None),
        ObservationRecord::EdgeLiveness {
            upstream, reader, ..
        } => ObservationSubject::Edge {
            upstream: *upstream,
            reader: *reader,
        },
        _ => ObservationSubject::None,
    };
    ObservationFamily {
        name: Cow::Borrowed(record.into()),
        subject,
    }
}

pub fn observation_families(
    packet: ObservabilityContext,
) -> Option<Vec<(ObservationFamily, ObservabilityContext)>> {
    let mut families = Vec::new();
    visit_families(&packet, |key, _, materialise| {
        families.push((key, materialise()));
        families.len() <= MAX_PACKET_FAMILIES
    });
    (families.len() <= MAX_PACKET_FAMILIES).then_some(families)
}

/// Inspect actual family stamps without copying measurement values. Nested
/// runtime snapshots keep their own owner and sequence, independently of the packet.
pub fn any_observation_family(
    packet: &ObservabilityContext,
    mut predicate: impl FnMut(ObservationFamily, CaptureStamp) -> bool,
) -> bool {
    let mut found = false;
    visit_families(packet, |key, stamp, _| {
        found = predicate(key, stamp);
        !found
    });
    found
}

fn visit_families(
    packet: &ObservabilityContext,
    mut visit: impl FnMut(ObservationFamily, CaptureStamp, &dyn Fn() -> ObservabilityContext) -> bool,
) {
    // Exhaustive destructuring makes a new packet or runtime field require an
    // explicit selection decision here. There is no separate kind catalogue.
    let ObservabilityContext {
        capture: stamp,
        runtime_snapshot,
        runtime,
        processing_time,
        metrics,
        sli,
        records,
    } = packet;
    let stamp = *stamp;
    macro_rules! emit {
        ($key:expr, $stamp:expr, $part:expr) => {
            if !visit($key, $stamp, &$part) {
                return;
            }
        };
    }
    if let Some(snapshot) = runtime_snapshot {
        emit!(
            ObservationFamily::new(stringify!(runtime_snapshot)),
            snapshot.capture,
            || {
                let mut part = ObservabilityContext::new(snapshot.capture);
                part.runtime_snapshot = Some(snapshot.clone());
                part
            }
        );
    }
    if let Some(runtime) = runtime {
        let RuntimeObservability {
            in_flight,
            join_reference_since_last_stream,
            time_in_state_ms,
            event_loops_total,
            event_loops_with_work_total,
            timing,
            circuit_breaker,
            rate_limiter,
            effect_circuit_breakers,
            effect_rate_limiters,
        } = runtime;
        macro_rules! field {
            ($name:ident) => {
                if let Some(measurement) = $name {
                    emit!(
                        ObservationFamily::new(concat!("runtime.", stringify!($name))),
                        stamp,
                        || {
                            let mut part = ObservabilityContext::new(stamp);
                            part.runtime = Some(RuntimeObservability {
                                $name: Some(measurement.clone()),
                                ..Default::default()
                            });
                            part
                        }
                    );
                }
            };
        }
        field!(in_flight);
        field!(join_reference_since_last_stream);
        field!(time_in_state_ms);
        field!(event_loops_total);
        field!(event_loops_with_work_total);
        let timing = timing.as_ref().filter(|timing| timing.is_valid());
        field!(timing);
        field!(circuit_breaker);
        field!(rate_limiter);
        macro_rules! effects {
            ($name:ident) => {
                for effect_measurements in $name {
                    let key = ObservationFamily {
                        subject: ObservationSubject::Effect(
                            effect_measurements.effect_type.clone(),
                        ),
                        ..ObservationFamily::new(concat!("runtime.", stringify!($name)))
                    };
                    emit!(key, stamp, || {
                        let mut part = ObservabilityContext::new(stamp);
                        part.runtime = Some(RuntimeObservability {
                            $name: vec![effect_measurements.clone()],
                            ..Default::default()
                        });
                        part
                    });
                }
            };
        }
        effects!(effect_circuit_breakers);
        effects!(effect_rate_limiters);
    }
    macro_rules! field {
        ($name:ident) => {
            if let Some(measurement) = $name {
                emit!(ObservationFamily::new(stringify!($name)), stamp, || {
                    let mut part = ObservabilityContext::new(stamp);
                    part.$name = Some(measurement.clone());
                    part
                });
            }
        };
    }
    field!(processing_time);
    field!(metrics);
    field!(sli);
    for record in records {
        emit!(record_family(record), stamp, || {
            let mut part = ObservabilityContext::new(stamp);
            part.records.push(record.clone());
            part
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn family_keys_separate_effects_edges_and_variants_but_not_measurement_values() {
        let mut keys = HashSet::new();
        for effect_type in [None, Some("payments".into()), Some("email".into())] {
            for delayed_events in [0, 10] {
                let key = record_family(&ObservationRecord::RateLimiterActivity {
                    effect_type: effect_type.clone(),
                    window_ms: 100,
                    delayed_events,
                    delay_ms_total: 0,
                    delay_ms_max: 0,
                    limit_rate: 100.0,
                });
                assert!(matches!(key.name, Cow::Borrowed(_)));
                let saved = serde_json::to_string(&key).unwrap();
                let restored: ObservationFamily = serde_json::from_str(&saved).unwrap();
                assert_eq!(key, restored);
                assert_eq!(keys.insert(key), delayed_events == 0);
            }
            assert!(
                keys.insert(record_family(&ObservationRecord::RateLimiterUtilisation {
                    effect_type,
                    utilization_percent: 0.0,
                    events_in_window: 0,
                    window_size_ms: 100,
                }))
            );
        }
        let upstream = StageId::new();
        let reader = StageId::new();
        for (upstream, reader) in [
            (upstream, reader),
            (upstream, StageId::new()),
            (StageId::new(), reader),
        ] {
            assert!(keys.insert(record_family(&ObservationRecord::EdgeLiveness {
                upstream,
                reader,
                state: EdgeLivenessState::Healthy,
                idle_ms: DurationMs(0),
                last_reader_seq: None,
                last_event_id: None,
            })));
        }
        assert_eq!(keys.len(), 9);
    }
}
