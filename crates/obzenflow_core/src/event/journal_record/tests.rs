// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::ai::{AiProvider, LlmHashes, LlmObservability};
use crate::event::observability::{
    CaptureReason, CaptureScope, CaptureSeq, CaptureStamp, ExecutionProgress, ObservabilityContext,
    RuntimeObservability, RuntimeSnapshot,
};
use crate::event::payloads::execution_payload::ExecutionPayload;
use crate::event::payloads::system_payload::StageLifecycleEvent;
use crate::event::provenance::causality_context::CausalityContext;
use crate::event::provenance::{AuthoredProvenance, ProcessingProvenance};
use crate::event::provenance::{ChainEventProvenance, SystemEventProvenance};
use crate::event::provenance::{
    CompositeActivationContext, FlowContext, JournalGroupMember, RuntimeProvenance,
};
use crate::event::status::processing_status::ProcessingStatus;
use crate::event::vector_clock::VectorClock;
use crate::event::EventKind;
use crate::id::{CompositeId, FlowId, SystemId};
use crate::{ChainEvent, EventId, JournalWriterId, ReaderGeneration, StageId, WriterId};
use chrono::{TimeZone, Utc};
use serde_json::json;

fn chain_record(payload: ChainPayload, event_type: &str) -> JournalRecord<ChainPayload> {
    let stage_id = StageId::new();
    let journal_writer_id = JournalWriterId::new();
    let id = EventId::new();
    let mut runtime = RuntimeProvenance::default();
    runtime.accounting.events_processed_total = 7;
    let provenance = ChainEventProvenance {
        id,
        writer_id: WriterId::from(stage_id),
        event_kind: payload.kind(),
        event_type: event_type.to_string(),
        causality: CausalityContext::new(),
        flow_context: FlowContext::default(),
        processing: ProcessingProvenance {
            event_time: 10,
            status: ProcessingStatus::Success,
        },
        correlation: None,
        replay_context: None,
        ingress_context: None,
        cycle_depth: None,
        cycle_scc_id: None,
        effect_provenance: None,
        admission_seq: None,
        runtime: Some(runtime),
        composite_activations: vec![CompositeActivationContext::new(
            CompositeId::new("digest"),
            id,
            "in",
            10,
        )],
    };
    let mut observation = ObservabilityContext::new(CaptureStamp {
        capture_scope: CaptureScope {
            flow_id: FlowId::new(),
            resume_generation: ReaderGeneration(0),
        },
        observer: WriterId::from(stage_id),
        capture_seq: CaptureSeq(3),
        capture_reason: CaptureReason::Record,
        observed_at_ms: 11,
    });
    observation.runtime = Some(RuntimeObservability {
        in_flight: Some(0),
        ..Default::default()
    });
    observation.runtime_snapshot = Some(RuntimeSnapshot {
        capture: observation.capture,
        progress: ExecutionProgress {
            reader_seq: 9,
            ..Default::default()
        },
        fsm_state: "Running".into(),
    });
    JournalRecord::commit(
        AuthoredEnvelope {
            provenance: AuthoredProvenance { event: provenance },
            observability: Some(observation),
        },
        payload,
        JournalProvenance {
            run_id: crate::FlowId::new(),
            causal: Default::default(),
            journal_writer_id,
            vector_clock: VectorClock {
                clocks: [(crate::event::CausalCoordinate::new(journal_writer_id), 1)].into(),
            },
            timestamp: Utc.timestamp_millis_opt(12).unwrap(),
            journal_group_id: Some("fixture-group".into()),
            journal_group_member: Some(JournalGroupMember { index: 0, size: 1 }),
        },
    )
    .unwrap()
}

#[test]
fn application_json_is_selected_by_descriptor_including_null_and_framework_keys() {
    for value in [
        json!(42),
        json!([1, null]),
        Value::Null,
        json!({"execution_type":"source_poll_error","system_event_type":"stage_lifecycle"}),
    ] {
        let record = chain_record(ChainPayload::Fact(value.clone()), "application.value.v1");
        let json = serde_json::to_value(record).unwrap();
        assert_eq!(json.as_object().unwrap().len(), 2);
        assert_eq!(json["payload"], value);
        let decoded: JournalRecord<ChainPayload> = serde_json::from_value(json).unwrap();
        assert!(matches!(decoded.payload, ChainPayload::Fact(ref payload) if payload == &value));
    }
}

#[test]
fn required_record_roots_do_not_confuse_absence_with_business_null() {
    let record = chain_record(ChainPayload::Fact(Value::Null), "business.null.v1");
    let authored = record.authored();
    for root in ["envelope", "payload"] {
        let mut committed = serde_json::to_value(&record).unwrap();
        committed.as_object_mut().unwrap().remove(root);
        assert!(serde_json::from_value::<JournalRecord<ChainPayload>>(committed).is_err());
        let mut authored = serde_json::to_value(&authored).unwrap();
        authored.as_object_mut().unwrap().remove(root);
        assert!(serde_json::from_value::<ChainEvent>(authored).is_err());
    }
}

#[test]
fn malformed_records_report_the_boundary_and_field_path() {
    let original = serde_json::to_value(chain_record(
        ChainPayload::Fact(Value::Null),
        "business.null.v1",
    ))
    .unwrap();
    for (path, value, diagnostic) in [
        (
            "/envelope/provenance/event/event_kind",
            json!("data"),
            "invalid provenance at envelope.provenance.event.event_kind:",
        ),
        (
            "/envelope/provenance/journal/vector_clock",
            json!(42),
            "invalid provenance at envelope.provenance.journal.vector_clock:",
        ),
        (
            "/envelope/observability/runtime",
            json!({"in_flight": 0, "custom": 1}),
            "unknown observability field at envelope.observability.runtime.custom",
        ),
        (
            "/envelope/observability",
            json!({"custom": {"anything": true}}),
            "unknown observability field at envelope.observability.custom",
        ),
    ] {
        let mut invalid = original.clone();
        *invalid.pointer_mut(path).unwrap() = value;
        let error = serde_json::from_value::<JournalRecord<ChainPayload>>(invalid).unwrap_err();
        assert!(error.to_string().contains(diagnostic), "{error}");
    }
    for root in ["envelope", "payload"] {
        let mut invalid = original.clone();
        invalid.as_object_mut().unwrap().remove(root);
        let error = serde_json::from_value::<JournalRecord<ChainPayload>>(invalid).unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("invalid record shape: expected envelope and payload"),
            "{error}"
        );
    }
    let extra_root = original.to_string().replacen('{', "{\"event\": {},", 1);
    let duplicate_root = original.to_string().replacen('{', "{\"payload\": null,", 1);
    for invalid in [extra_root, duplicate_root] {
        let error = serde_json::from_str::<JournalRecord<ChainPayload>>(&invalid).unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("invalid record shape: expected envelope and payload"),
            "{error}"
        );
    }
}

#[test]
fn removing_observations_preserves_complete_provenance_and_atomic_membership() {
    let original = serde_json::to_value(chain_record(
        ChainPayload::Fact(json!({"ok":true})),
        "application.result.v1",
    ))
    .unwrap();
    assert_eq!(
        original["envelope"]["provenance"]["event"]["runtime"],
        json!({"accounting": original["envelope"]["provenance"]["event"]["runtime"]["accounting"]})
    );
    let snapshot = &original["envelope"]["observability"]["runtime_snapshot"];
    assert_eq!(snapshot["progress"]["reader_seq"], 9);
    assert_eq!(snapshot["fsm_state"], "Running");
    let mut omitted = original.clone();
    omitted["envelope"]["observability"] = Value::Null;
    let decoded: JournalRecord<ChainPayload> = serde_json::from_value(omitted).unwrap();
    let round_trip = serde_json::to_value(decoded).unwrap();
    assert_eq!(
        round_trip["envelope"]["provenance"],
        original["envelope"]["provenance"]
    );
    assert_eq!(round_trip["payload"], original["payload"]);
    assert!(round_trip["envelope"]["observability"].is_null());
}

#[test]
fn execution_descriptor_mismatch_and_unknown_family_are_rejected() {
    let record = chain_record(
        ChainPayload::Execution(ExecutionPayload::AccumulatorProgress {
            inputs_since_last_report: 100,
        }),
        "stateful.accumulation_progress",
    );
    let json = serde_json::to_value(record).unwrap();
    assert_eq!(
        json["payload"],
        json!({"execution_type":"accumulator_progress","inputs_since_last_report":100})
    );
    let mut mismatch = json.clone();
    mismatch["envelope"]["provenance"]["event"]["event_type"] = json!("join.reference_progress");
    assert!(
        serde_json::from_value::<JournalRecord<ChainPayload>>(mismatch)
            .unwrap_err()
            .to_string()
            .contains("event descriptor does not match payload")
    );
    let mut unknown = json;
    unknown["envelope"]["provenance"]["event"]["event_kind"] = json!("data");
    assert!(serde_json::from_value::<JournalRecord<ChainPayload>>(unknown).is_err());
}

#[test]
fn authored_records_and_old_roots_cannot_decode_as_committed_records() {
    let mut record = serde_json::to_value(chain_record(
        ChainPayload::Fact(Value::Null),
        "application.null.v1",
    ))
    .unwrap();
    record["envelope"]["provenance"]
        .as_object_mut()
        .unwrap()
        .remove("journal");
    assert!(serde_json::from_value::<JournalRecord<ChainPayload>>(record).is_err());
    assert!(serde_json::from_value::<JournalRecord<ChainPayload>>(json!({"event":{"content":{"content_type":"data","payload":null}},"vector_clock":{"clocks":{}}})).is_err());
}

#[test]
fn system_records_keep_typed_discriminants_and_separate_creation_and_append_time() {
    let journal_writer_id = JournalWriterId::new();
    let writer_id = WriterId::from(SystemId::new());
    let record = JournalRecord::commit(
        AuthoredEnvelope {
            provenance: AuthoredProvenance {
                event: SystemEventProvenance {
                    id: EventId::new(),
                    writer_id,
                    event_kind: EventKind::System,
                    event_type: "system.stage.running".into(),
                    timestamp: 10,
                },
            },
            observability: None,
        },
        SystemPayload::StageLifecycle {
            stage_id: StageId::new(),
            event: StageLifecycleEvent::Running,
        },
        JournalProvenance {
            run_id: crate::FlowId::new(),
            causal: Default::default(),
            journal_writer_id,
            vector_clock: VectorClock {
                clocks: [(crate::event::CausalCoordinate::new(journal_writer_id), 1)].into(),
            },
            timestamp: Utc.timestamp_millis_opt(12).unwrap(),
            journal_group_id: None,
            journal_group_member: None,
        },
    )
    .unwrap();
    let json = serde_json::to_value(record).unwrap();
    assert_eq!(json["payload"]["system_event_type"], "stage_lifecycle");
    assert_eq!(json["payload"]["lifecycle_event"], "running");
    assert_eq!(json["envelope"]["provenance"]["event"]["timestamp"], 10);
    assert!(json["envelope"]["provenance"]["event"]
        .get("flow_context")
        .is_none());
    let decoded: JournalRecord<SystemPayload> = serde_json::from_value(json).unwrap();
    assert_eq!(
        decoded
            .envelope
            .provenance
            .journal
            .timestamp
            .timestamp_millis(),
        12
    );
}

#[test]
fn unknown_observation_fields_do_not_become_unrestricted_json() {
    let mut record = serde_json::to_value(chain_record(
        ChainPayload::Fact(Value::Null),
        "application.null.v1",
    ))
    .unwrap();
    record["envelope"]["observability"]["custom"] = json!({"anything": true});
    assert!(serde_json::from_value::<JournalRecord<ChainPayload>>(record).is_err());
}

#[test]
fn invalid_or_oversized_observations_are_omitted_without_changing_the_committed_fact() {
    use crate::event::observability::ObservationRecord;
    for record in [
        ObservationRecord::ResourceUsage {
            cpu_percent: f64::NAN,
            memory_bytes: 1,
            thread_count: None,
        },
        ObservationRecord::Llm {
            metadata: LlmObservability::new(
                AiProvider::new("test"),
                "x".repeat(70_000),
                LlmHashes::new("prompt".into(), "params".into()),
            ),
        },
    ] {
        let original = chain_record(ChainPayload::Fact(json!({"value": 7})), "example.fact");
        let expected_provenance = serde_json::to_value(&original.envelope.provenance).unwrap();
        let journal = original.envelope.provenance.journal.clone();
        let mut authored = original.into_authored();
        authored
            .envelope
            .observability
            .as_mut()
            .unwrap()
            .records
            .push(record);
        let committed = JournalRecord::<ChainPayload>::commit_event(authored, journal).unwrap();
        assert!(committed.envelope.observability.is_none());
        assert_eq!(
            serde_json::to_value(&committed.envelope.provenance).unwrap(),
            expected_provenance
        );
        assert_eq!(
            serde_json::to_value(&committed).unwrap()["payload"],
            json!({"value": 7})
        );
    }
}

#[test]
fn malformed_timing_omits_only_its_family_and_measured_zero_remains_present() {
    use crate::event::observability::{MeasurementWindow, TimingMeasurements};
    let original = chain_record(ChainPayload::Fact(Value::Null), "example.null");
    let journal = original.envelope.provenance.journal.clone();
    let mut authored = original.into_authored();
    authored
        .envelope
        .observability
        .as_mut()
        .unwrap()
        .runtime
        .as_mut()
        .unwrap()
        .timing = Some(TimingMeasurements {
        processing_time_count: 0,
        processing_time_sum_nanos: 0,
        recent_p50_ms: Some(0),
        recent_p90_ms: None,
        recent_p95_ms: None,
        recent_p99_ms: None,
        recent_p999_ms: None,
        window: MeasurementWindow {
            started_at_ms: 1,
            ended_at_ms: 2,
        },
    });
    let committed = JournalRecord::<ChainPayload>::commit_event(authored, journal).unwrap();
    let runtime = committed.envelope.observability.unwrap().runtime.unwrap();
    assert_eq!(runtime.in_flight, Some(0));
    assert!(runtime.timing.is_none());
}
