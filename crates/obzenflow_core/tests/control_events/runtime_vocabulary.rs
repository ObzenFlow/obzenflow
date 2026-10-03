// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Literal schema-14 expectations, independent of vocabulary constants. These
//! cases predate the naming consolidation and exercise authored/decoded events.

use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::provenance::{FlowContext, RecordProvenance};
use obzenflow_core::event::{ChainEventFactory, SystemEvent, SystemPayload};
use obzenflow_core::{ChainEvent, StageId, SystemId};
use serde_json::{json, Value};

fn assert_chain(event: ChainEvent, name: &str, kind: &str) {
    assert_eq!(event.event_type(), name);
    let encoded = serde_json::to_value(event).unwrap();
    let descriptor = &encoded["envelope"]["provenance"]["event"];
    assert_eq!(descriptor["event_kind"], kind);
    assert_eq!(descriptor["payload_schema_version"], 1);
    let decoded: ChainEvent = serde_json::from_value(encoded.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), encoded);
    for incorrect in [format!("{name}.renamed"), name.replace('.', "_")] {
        let mut invalid = encoded.clone();
        invalid["envelope"]["provenance"]["event"]["event_type"] = json!(incorrect);
        assert!(
            serde_json::from_value::<ChainEvent>(invalid).is_err(),
            "{name}"
        );
    }
}

#[test]
fn stream_and_subscription_wire_names_are_unchanged() {
    let stage = StageId::new();
    let scope =
        json!({"upstream": stage, "reader": StageId::new(), "selection": {"selection": "all"}});
    for (payload, name) in [
        (
            json!({"flow_control_type":"eof", "kind":"natural", "timestamp":1, "writer_seq_by_event_type_complete":false}),
            "runtime.stream.end_declared",
        ),
        (
            json!({"flow_control_type":"watermark", "timestamp":1}),
            "runtime.stream.watermark_declared",
        ),
        (
            json!({"flow_control_type":"catch_up_complete", "generation":1, "stage_key":"orders"}),
            "runtime.stream.catch_up_completed",
        ),
        (
            json!({"flow_control_type":"checkpoint", "id":"checkpoint-1"}),
            "runtime.stream.checkpoint_declared",
        ),
        (
            json!({"flow_control_type":"drain"}),
            "runtime.stream.drain_requested",
        ),
        (
            json!({"flow_control_type":"pipeline_abort", "reason":{"violation_type":"other", "details":"stopped"}}),
            "runtime.pipeline.abort_requested",
        ),
        (
            json!({"flow_control_type":"source_contract", "source_id":stage, "journal_path":"orders.log", "journal_index":0}),
            "runtime.source.production_declared",
        ),
        (
            json!({"flow_control_type":"production_final", "produced_count":0, "produced_by_event_type":[], "end_kind":"natural"}),
            "runtime.source.production_finalized",
        ),
        (
            json!({"flow_control_type":"consumption_progress", "scope":scope, "consumed_count":0, "reader_seq":0, "eof_seen":false, "reader_path":"orders.log", "reader_index":0}),
            "runtime.subscription.progress_reported",
        ),
        (
            json!({"flow_control_type":"consumption_gap", "from_seq":1, "to_seq":3, "upstream":stage}),
            "runtime.subscription.gap_detected",
        ),
        (
            json!({"flow_control_type":"consumption_final", "scope":scope, "pass":true, "consumed_count":0, "reader_seq":0, "eof_seen":true}),
            "runtime.subscription.consumption_finalized",
        ),
        (
            json!({"flow_control_type":"reader_stalled", "upstream":stage, "stalled_since":10}),
            "runtime.subscription.stall_detected",
        ),
        (
            json!({"flow_control_type":"at_least_once_violation", "upstream":stage, "reason":{"violation_type":"gap_detected", "details":{"from":1, "to":3}}, "reader_seq":1}),
            "runtime.subscription.at_least_once_violated",
        ),
    ] {
        let payload: FlowControlPayload = serde_json::from_value(payload).unwrap();
        assert_chain(
            ChainEventFactory::flow_signal_event(stage.into(), payload),
            name,
            "flow_signal",
        );
    }
}

#[test]
fn stage_and_middleware_wire_names_are_unchanged() {
    let stage = StageId::new();
    let cursor =
        json!({"recorded_flow_id":"flow", "stage_key":"orders", "input_seq":1, "effect_ordinal":0});
    for (payload, name) in [
        (
            json!({"execution_type":"stage_lifecycle", "stage_state":"running", "stage_id":stage}),
            "supervisor.stage.orders%2Ev2.milestone.ready",
        ),
        (
            json!({"execution_type":"stage_lifecycle", "stage_state":"draining", "stage_id":stage}),
            "supervisor.stage.orders%2Ev2.milestone.drain_started",
        ),
        (
            json!({"execution_type":"stage_lifecycle", "stage_state":"drained", "stage_id":stage}),
            "supervisor.stage.orders%2Ev2.milestone.drain_completed",
        ),
        (
            json!({"execution_type":"stage_lifecycle", "stage_state":"completed", "stage_id":stage}),
            "supervisor.stage.orders%2Ev2.outcome.completed",
        ),
        (
            json!({"execution_type":"stage_lifecycle", "stage_state":"cancelled", "stage_id":stage, "reason":"requested"}),
            "supervisor.stage.orders%2Ev2.outcome.cancelled",
        ),
        (
            json!({"execution_type":"stage_lifecycle", "stage_state":"failed", "stage_id":stage, "error":"failed"}),
            "supervisor.stage.orders%2Ev2.outcome.failed",
        ),
        (
            json!({"execution_type":"resilience_occurrence", "action":"opened", "cooldown_ms":10, "error_rate":1.0, "failure_count":1, "trigger":"consecutive_failures", "observed_calls":1}),
            "runtime.circuit_breaker.opened",
        ),
        (
            json!({"execution_type":"resilience_occurrence", "action":"closed", "success_count":1, "recovery_duration_ms":1}),
            "runtime.circuit_breaker.closed",
        ),
        (
            json!({"execution_type":"resilience_occurrence", "action":"half_open", "test_request_count":1}),
            "runtime.circuit_breaker.half_open_entered",
        ),
        (
            json!({"execution_type":"resilience_occurrence", "action":"rejected"}),
            "runtime.circuit_breaker.admission_rejected",
        ),
        (
            json!({"execution_type":"resilience_occurrence", "action":"attempt_settled", "cursor":cursor, "attempt":1, "health_classification":"success", "slow":false, "dependency_elapsed_ms":1, "admission_wait_ms":0}),
            "runtime.circuit_breaker.attempt_assessed",
        ),
        (
            json!({"execution_type":"resilience_occurrence", "action":"retry_scheduled", "cursor":cursor, "next_attempt":2, "delay_ms":1}),
            "runtime.retry.scheduled",
        ),
        (
            json!({"execution_type":"resilience_occurrence", "action":"retry_succeeded", "cursor":cursor, "total_attempts":2, "terminal_classification":"success"}),
            "runtime.retry.succeeded",
        ),
        (
            json!({"execution_type":"resilience_occurrence", "action":"retry_exhausted", "cursor":cursor, "total_attempts":2, "reason":"attempt_limit"}),
            "runtime.retry.exhausted",
        ),
        (
            json!({"execution_type":"resilience_occurrence", "action":"retry_stopped_non_retryable", "cursor":cursor, "total_attempts":1}),
            "runtime.retry.stopped_non_retryable",
        ),
        (
            json!({"execution_type":"resilience_occurrence", "action":"recovery_completed", "cursor":cursor, "total_attempts":1, "backoff_elapsed_ms":0, "recovery_elapsed_ms":1}),
            "runtime.resilience.evaluation_finished",
        ),
        (
            json!({"execution_type":"resilience_occurrence", "action":"state_changed", "from_state":"closed", "to_state":"open", "timestamp":1}),
            "runtime.circuit_breaker.opened",
        ),
        (
            json!({"execution_type":"resilience_occurrence", "action":"state_changed", "from_state":"open", "to_state":"half_open", "timestamp":1}),
            "runtime.circuit_breaker.half_open_entered",
        ),
        (
            json!({"execution_type":"resilience_occurrence", "action":"state_changed", "from_state":"half_open", "to_state":"closed", "timestamp":1}),
            "runtime.circuit_breaker.closed",
        ),
        (
            json!({"execution_type":"rate_limiter", "action":"delayed", "delay_ms":1, "current_rate":1.0, "limit_rate":1.0}),
            "runtime.rate_limiter.wait_started",
        ),
        (
            json!({"execution_type":"rate_limiter", "action":"mode_change", "mode_from":"normal", "mode_to":"limiting", "limit_rate":1.0}),
            "runtime.rate_limiter.mode_changed",
        ),
        (
            json!({"execution_type":"rate_limiter", "action":"config_changed", "old_rate":1.0, "new_rate":2.0}),
            "runtime.rate_limiter.configuration_changed",
        ),
        (
            json!({"execution_type":"backpressure", "action":"stalled", "upstream":stage, "downstream":stage, "window":1, "stall_timeout_ms":1, "elapsed_ms":1, "in_flight":1}),
            "runtime.backpressure.stall_detected",
        ),
    ] {
        let payload: ExecutionPayload =
            serde_json::from_value(payload).unwrap_or_else(|err| panic!("{name}: {err}"));
        let event = ChainEventFactory::execution_event(stage.into(), payload).with_flow_context(
            FlowContext {
                stage_name: "orders.v2".into(),
                ..Default::default()
            },
        );
        assert_chain(event, name, "execution");
    }
}

#[test]
fn system_wire_names_keep_the_pipeline_author_for_metrics_requests() {
    let metrics = json!({"events_in_total":0, "events_out_total":0, "errors_total":0});
    for (payload, name) in [
        (
            json!({"system_event_type":"pipeline_lifecycle", "pipeline_event":"starting"}),
            "supervisor.runtime.pipeline_supervisor.command.start.admitted",
        ),
        (
            json!({"system_event_type":"pipeline_lifecycle", "pipeline_event":"ready_for_run"}),
            "supervisor.runtime.pipeline_supervisor.milestone.ready_for_run",
        ),
        (
            json!({"system_event_type":"pipeline_lifecycle", "pipeline_event":"running"}),
            "supervisor.runtime.pipeline_supervisor.milestone.sources_started",
        ),
        (
            json!({"system_event_type":"pipeline_lifecycle", "pipeline_event":"stop_admitted", "admission":{"mode":"graceful", "timeout_ms":10}}),
            "supervisor.runtime.pipeline_supervisor.command.graceful_stop.admitted",
        ),
        (
            json!({"system_event_type":"pipeline_lifecycle", "pipeline_event":"stop_admitted", "admission":{"mode":"cancel", "cause":"requested"}}),
            "supervisor.runtime.pipeline_supervisor.command.cancel.admitted",
        ),
        (
            json!({"system_event_type":"pipeline_lifecycle", "pipeline_event":"not_started"}),
            "supervisor.runtime.pipeline_supervisor.outcome.not_started",
        ),
        (
            json!({"system_event_type":"pipeline_lifecycle", "pipeline_event":"draining"}),
            "supervisor.runtime.pipeline_supervisor.milestone.drain_started",
        ),
        (
            json!({"system_event_type":"pipeline_lifecycle", "pipeline_event":"all_stages_completed"}),
            "supervisor.runtime.pipeline_supervisor.milestone.all_stages_completed",
        ),
        (
            json!({"system_event_type":"pipeline_lifecycle", "pipeline_event":"drained"}),
            "supervisor.runtime.pipeline_supervisor.milestone.final_marker_published",
        ),
        (
            json!({"system_event_type":"pipeline_lifecycle", "pipeline_event":"completed", "duration_ms":1, "metrics":metrics}),
            "supervisor.runtime.pipeline_supervisor.outcome.completed",
        ),
        (
            json!({"system_event_type":"pipeline_lifecycle", "pipeline_event":"failed", "duration_ms":1, "reason":"failed"}),
            "supervisor.runtime.pipeline_supervisor.outcome.failed",
        ),
        (
            json!({"system_event_type":"pipeline_lifecycle", "pipeline_event":"cancelled", "duration_ms":1, "reason":"requested"}),
            "supervisor.runtime.pipeline_supervisor.outcome.cancelled",
        ),
        (
            json!({"system_event_type":"metrics_coordination", "metrics_event":"ready"}),
            "supervisor.runtime.metrics_aggregator.milestone.ready",
        ),
        (
            json!({"system_event_type":"metrics_coordination", "metrics_event":"drain_requested"}),
            "supervisor.runtime.pipeline_supervisor.command.finalize_metrics.requested",
        ),
        (
            json!({"system_event_type":"metrics_coordination", "metrics_event":"drained"}),
            "supervisor.runtime.metrics_aggregator.finalization.completed",
        ),
        (
            json!({"system_event_type":"metrics_coordination", "metrics_event":"shutdown"}),
            "supervisor.runtime.metrics_aggregator.milestone.refresh_readers_stopped",
        ),
        (
            json!({"system_event_type":"metrics_coordination", "metrics_event":"exported", "watermark":{"entries":[]}}),
            "supervisor.runtime.metrics_aggregator.snapshot.published",
        ),
    ] {
        let payload: SystemPayload =
            serde_json::from_value(payload).unwrap_or_else(|err| panic!("{name}: {err}"));
        let event = SystemEvent::new(SystemId::new().into(), payload);
        assert_eq!(event.event_type(), name);
        let mut encoded = serde_json::to_value(event).unwrap();
        let descriptor = &encoded["envelope"]["provenance"]["event"];
        assert_eq!(descriptor["event_kind"], "system");
        assert_eq!(descriptor["payload_schema_version"], 1);
        let decoded: SystemEvent = serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), encoded);
        encoded["envelope"]["provenance"]["event"]["event_type"] =
            Value::String(format!("{name}.renamed"));
        assert!(serde_json::from_value::<SystemEvent>(encoded).is_err());
    }
}

#[test]
fn contract_wire_names_preserve_all_findings_and_both_policy_verdicts() {
    let upstream = StageId::new();
    let reader = StageId::new();
    let evidence = json!({
        "contract_name":"CustomContract", "upstream_stage":upstream,
        "downstream_stage":reader, "evaluated_at":"2026-10-02T00:00:00Z",
        "details":{"evidence_type":"custom", "details":{}}
    });
    let mut violation = evidence.clone();
    let object = violation.as_object_mut().unwrap();
    let timestamp = object.remove("evaluated_at").unwrap();
    object.insert("detected_at".into(), timestamp);
    object.insert("cause".into(), json!({"Other":"failed"}));
    for (status, body, name) in [
        (
            "passed",
            evidence.clone(),
            "runtime.contract.verification_passed",
        ),
        ("failed", violation, "runtime.contract.verification_failed"),
        (
            "pending",
            json!({"reason":"evaluation_unavailable", "evidence":evidence}),
            "runtime.contract.verification_pending",
        ),
        (
            "skipped",
            json!({"reason":"no_production_expectation", "evidence":evidence}),
            "runtime.contract.verification_skipped",
        ),
    ] {
        let payload = serde_json::from_value(json!({
            "execution_type":"contract_result", "upstream":upstream, "reader":reader,
            "contract_name":"CustomContract", "status":status, "phase":"final",
            "result":{"outcome":status, "evidence":body}
        }))
        .unwrap();
        assert_chain(
            ChainEventFactory::execution_event(reader.into(), payload),
            name,
            "execution",
        );
    }
    for (pass, name) in [
        (true, "runtime.contract.policy_accepted"),
        (false, "runtime.contract.policy_rejected"),
    ] {
        let payload = serde_json::from_value(json!({
            "execution_type":"contract_status", "upstream":upstream, "reader":reader, "pass":pass
        }))
        .unwrap();
        assert_chain(
            ChainEventFactory::execution_event(reader.into(), payload),
            name,
            "execution",
        );
    }
}

#[test]
fn registration_wire_names_preserve_runtime_and_escaped_stage_names() {
    use obzenflow_core::event::payloads::supervisor_descriptor::{
        SupervisionMode, SupervisorDescriptor, SupervisorKind,
    };
    for (name, kind, expected) in [
        (
            "pipeline_supervisor",
            SupervisorKind::Pipeline,
            "supervisor.runtime.pipeline_supervisor.registered",
        ),
        (
            "metrics_aggregator",
            SupervisorKind::MetricsAggregator,
            "supervisor.runtime.metrics_aggregator.registered",
        ),
    ] {
        let descriptor = SupervisorDescriptor {
            name: name.into(),
            kind,
            supervision: SupervisionMode::SelfSupervised,
        };
        let event = SystemEvent::new(
            SystemId::new().into(),
            SystemPayload::SupervisorRegistered { descriptor },
        );
        assert_eq!(event.event_type(), expected);
        serde_json::from_value::<SystemEvent>(serde_json::to_value(event).unwrap()).unwrap();
    }
    for (name, expected) in [
        ("orders.v2", "supervisor.stage.orders%2Ev2.registered"),
        ("orders%2Ev2", "supervisor.stage.orders%252Ev2.registered"),
        (
            "café/entrée",
            "supervisor.stage.caf%C3%A9%2Fentr%C3%A9e.registered",
        ),
    ] {
        let descriptor = SupervisorDescriptor {
            name: name.into(),
            kind: SupervisorKind::Transform,
            supervision: SupervisionMode::HandlerSupervised,
        };
        assert_chain(
            ChainEventFactory::execution_event(
                StageId::new().into(),
                ExecutionPayload::SupervisorRegistered { descriptor },
            ),
            expected,
            "execution",
        );
    }
}
