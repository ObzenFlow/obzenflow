// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Deterministic current-schema records for codec correctness and timing tests.
//! The stream models a source, a transform that rejects the first input, and a
//! receipt sink. Each stage contributes 16 successful records: 48 event IDs and
//! 17 distinct origins, with downstream records reusing complete source origins.

use chrono::{DateTime, Duration, Utc};
use obzenflow_core::event::chain_event::CorrelationContext;
use obzenflow_core::event::context::{
    EventTypeCountContext, ExecutionAccounting, ExecutionProgress, FlowContext, MeasurementWindow,
    RateLimiterMeasurements, RuntimeObservability, RuntimeProvenance, RuntimeSnapshot, StageType,
    TimingMeasurements, UpstreamEventTypeCountContext,
};
use obzenflow_core::event::journal_record::JournalRecord;
use obzenflow_core::event::observation::{
    CaptureReason, CaptureScope, CaptureSeq, CaptureStamp, ObservabilityContext,
};
use obzenflow_core::event::payloads::correlation_payload::CorrelationPayload;
use obzenflow_core::event::payloads::delivery_payload::{
    DeliveryMethod, DeliveryPayload, DeliveryResult,
};
use obzenflow_core::event::provenance::JournalProvenance;
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::{ChainEventFactory, ChainPayload, CorrelationId};
use obzenflow_core::time::MetricsDuration;
use obzenflow_core::{
    AdmissionSeq, EventId, FlowId, JournalId, JournalWriterId, ReaderGeneration, StageId, WriterId,
};
use serde_json::json;
use ulid::Ulid;

pub(super) const RECORDS_PER_STAGE: usize = 16;
const START_MS: u64 = 1_789_512_042_500;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Stage {
    Source,
    Transform,
    Receipt,
}

impl Stage {
    fn id(self) -> StageId {
        StageId::from(ulid(10 + self as u64))
    }

    fn journal_writer(self) -> JournalWriterId {
        JournalWriterId::from(JournalId::from(ulid(20 + self as u64)))
    }

    fn event_id(self, input: u64) -> EventId {
        EventId::from(ulid(256 + self as u64 * 256 + input))
    }

    fn descriptor(self) -> (&'static str, StageType, &'static str) {
        match self {
            Self::Source => (
                "high_volume_source",
                StageType::FiniteSource,
                "data.request.v1",
            ),
            Self::Transform => (
                "error_processor",
                StageType::Transform,
                "processed.event.v1",
            ),
            Self::Receipt => ("completion_sink", StageType::Sink, "sink.delivery"),
        }
    }

    fn clock(self, input: u64) -> VectorClock {
        let mut clock = VectorClock::new();
        for stage in [Self::Source, Self::Transform, Self::Receipt] {
            if stage as u64 > self as u64 {
                break;
            }
            let sequence = if stage == Self::Source {
                input + 2
            } else {
                input
            };
            clock
                .clocks
                .insert(WriterId::from(stage.id()).to_string(), sequence);
        }
        clock
    }
}

fn ulid(value: u64) -> Ulid {
    Ulid::from((u128::from(START_MS) << 80) | u128::from(value))
}

/// Records are grouped by stage, matching the former corpus's cache workload.
pub(super) fn records() -> Vec<JournalRecord<ChainPayload>> {
    [Stage::Source, Stage::Transform, Stage::Receipt]
        .into_iter()
        .flat_map(|stage| (0..RECORDS_PER_STAGE as u64).map(move |index| record(stage, index)))
        .collect()
}

pub(super) fn record(stage: Stage, index: u64) -> JournalRecord<ChainPayload> {
    // Input zero fails at the transform, so its successful outputs start at one.
    let input = index + u64::from(stage != Stage::Source);
    let emitted = index + 1;
    let processed = emitted + u64::from(stage == Stage::Transform);
    let upstream = match stage {
        Stage::Source => None,
        Stage::Transform => Some(Stage::Source),
        Stage::Receipt => Some(Stage::Transform),
    };
    let (stage_name, stage_type, event_type) = stage.descriptor();
    let writer = WriterId::from(stage.id());
    let flow_id = FlowId::from(ulid(1));
    let event_time = START_MS + input * 2 + stage as u64 * 20;
    let timestamp = DateTime::<Utc>::from_timestamp_millis(event_time as i64).unwrap()
        + Duration::nanoseconds(123_456 + index as i64);
    let payload = match stage {
        Stage::Source => ChainPayload::Fact(json!({
            "batch": 0, "id": input, "should_fail": input == 0,
        })),
        Stage::Transform => ChainPayload::Fact(json!({
            "batch": 0, "id": input, "processed": true,
            "processing_stage": "error_prone_transform", "should_fail": false,
        })),
        Stage::Receipt => ChainPayload::Delivery(DeliveryPayload {
            result: DeliveryResult::Success {
                confirmation: None,
                response_headers: None,
            },
            destination: stage_name.into(),
            delivery_method: DeliveryMethod::Custom("InMemory".into()),
            bytes_processed: Some(1),
            items_delivered: None,
            processed_at: timestamp - Duration::nanoseconds(123),
            middleware_context: None,
        }),
    };
    let mut event = ChainEventFactory::create_event(writer, payload);
    event.id = stage.event_id(input);
    event.event_type = event_type.into();
    event.processing.event_time = event_time;
    event.flow_context = FlowContext {
        flow_name: "prometheus_demo".into(),
        flow_id: flow_id.to_string(),
        stage_name: stage_name.into(),
        stage_id: stage.id(),
        stage_type,
    };
    event.causality.parent_ids = upstream
        .map(|stage| stage.event_id(input))
        .into_iter()
        .collect();
    event.correlation = Some(CorrelationContext::single(
        CorrelationId::from(ulid(1024 + input)),
        Some(CorrelationPayload {
            entry_time_ns: (START_MS + input * 2) * 1_000_000 + 67,
            entry_event_id: Stage::Source.event_id(input),
            metadata: None,
        }),
    ));
    event.admission_seq = Some(AdmissionSeq(stage as u64 * 20 + emitted));
    let mut accounting = ExecutionAccounting {
        events_processed_total: processed,
        events_emitted_total: emitted,
        ..ExecutionAccounting::default()
    };
    if stage == Stage::Transform {
        accounting.errors_total = 1;
        accounting.errors_by_kind.insert(ErrorKind::Unknown, 1);
    }
    if stage != Stage::Receipt {
        accounting
            .data_outputs_by_event_type
            .push(EventTypeCountContext {
                event_type: event_type.into(),
                total: emitted,
            });
    }
    if let Some(upstream) = upstream {
        accounting
            .data_inputs_by_upstream_event_type
            .push(UpstreamEventTypeCountContext {
                upstream: upstream.id(),
                event_type: upstream.descriptor().2.into(),
                total: processed,
            });
    }
    event.runtime = Some(RuntimeProvenance { accounting });

    let capture = CaptureStamp {
        capture_scope: CaptureScope {
            flow_id,
            resume_generation: ReaderGeneration(0),
        },
        observer: writer,
        capture_seq: CaptureSeq(emitted * 2),
        capture_reason: CaptureReason::Record,
        observed_at_ms: event_time,
    };
    let mut observations = ObservabilityContext::new(capture);
    if stage != Stage::Receipt {
        let processing_ns = 14_000 + index * 137 + stage as u64 * 10_000;
        observations.processing_time = Some(MetricsDuration::from_nanos(processing_ns));
        observations.runtime = Some(RuntimeObservability {
            in_flight: Some(0),
            join_reference_since_last_stream: Some(0),
            time_in_state_ms: Some(input * 2),
            event_loops_total: Some(processed + stage as u64 * 2),
            event_loops_with_work_total: Some(processed + stage as u64),
            timing: Some(TimingMeasurements {
                processing_time_count: processed,
                processing_time_sum_nanos: processed * (14_000 + stage as u64 * 10_000)
                    + index * (index + 1) / 2 * 137,
                recent_p50_ms: Some(0),
                recent_p90_ms: Some(0),
                recent_p95_ms: Some(0),
                recent_p99_ms: Some(0),
                recent_p999_ms: Some(0),
                window: MeasurementWindow {
                    started_at_ms: START_MS,
                    ended_at_ms: event_time,
                },
            }),
            rate_limiter: (stage == Stage::Source).then_some(RateLimiterMeasurements {
                events_total: emitted,
                delayed_total: 0,
                tokens_consumed_total: emitted as f64,
                delay_seconds_total: 0.0,
                bucket_tokens: 999.162083 - index as f64 * 0.875,
                bucket_capacity: 1000.0,
            }),
            ..RuntimeObservability::default()
        });
    }
    // Facts have a separately stamped snapshot; receipt-only packets share it.
    observations.runtime_snapshot = Some(RuntimeSnapshot {
        capture: CaptureStamp {
            capture_seq: CaptureSeq(capture.capture_seq.0 + u64::from(stage != Stage::Receipt)),
            ..capture
        },
        progress: ExecutionProgress {
            reader_seq: upstream.map_or(0, |stage| {
                if stage == Stage::Source {
                    input + 2
                } else {
                    input + 1
                }
            }),
            writer_seq: emitted,
            last_consumed_event_id: upstream.map(|stage| stage.event_id(input)),
            last_consumed_writer: upstream.map(Stage::journal_writer),
            last_consumed_vector_clock: upstream.map(|stage| stage.clock(input)),
            last_emitted_event_id: Some(event.id),
            last_emitted_writer: Some(writer),
            ..ExecutionProgress::default()
        },
        fsm_state: "Running".into(),
    });
    event.envelope.observability = Some(observations);
    JournalRecord::commit_event(
        event,
        JournalProvenance {
            journal_writer_id: stage.journal_writer(),
            vector_clock: stage.clock(input),
            timestamp,
            journal_group_id: None,
            journal_group_member: None,
        },
    )
    .unwrap()
}
