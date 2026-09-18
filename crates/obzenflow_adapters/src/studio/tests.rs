// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use obzenflow_core::event::journal_record::JournalRecord;
use obzenflow_core::event::payloads::execution_payload::{
    CircuitBreakerFact, CircuitBreakerOpenTrigger, CircuitState, MiddlewareFact, RateLimiterFact,
    RateLimiterMode,
};
use obzenflow_core::event::payloads::system_payload::MiddlewareEventOrigin;
use obzenflow_core::event::provenance::ExecutionAccounting;
use obzenflow_core::event::types::{SeqNo, WriterId};
use obzenflow_core::event::{StageLifecycleEvent, SystemEvent};
use obzenflow_core::FlowId;
use obzenflow_core::{
    id::{JournalId, SystemId},
    JournalWriterId, StageId,
};
use serde_json::{json, Value};

fn fact(event: SystemPayload) -> SystemJournalRecord {
    JournalRecord::new(
        JournalWriterId::from(JournalId::new()),
        SystemEvent::new(WriterId::from(SystemId::new()), event),
    )
}

#[test]
fn middleware_rebuild_and_live_projection_use_supplied_time_and_preserve_revisions() {
    let stage = StageId::new();
    let envelope = fact(SystemPayload::MiddlewareLifecycle {
        stage_id: stage,
        stage_name: Some("worker".into()),
        flow_id: Some("flow".into()),
        flow_name: Some("demo".into()),
        origin: MiddlewareEventOrigin {
            event_id: EventId::new(),
            writer_key: "worker".into(),
            seq: SeqNo(7),
        },
        middleware: MiddlewareFact::RateLimiter(RateLimiterFact::ModeChange {
            mode_from: RateLimiterMode::Normal,
            mode_to: RateLimiterMode::Limiting,
            limit_rate: 10.0,
        }),
    });
    let mut rebuilt = StudioProjection::new(vec![], ContractBoundaryAliases::default()).unwrap();
    let mut live = rebuilt.clone();
    rebuilt.rebuild(&envelope);
    let frames = live.project(&envelope, 123);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].event.as_deref(), Some("middleware_lifecycle"));
    assert_eq!(
        frames[0].id.as_deref(),
        Some(envelope.envelope.provenance.event.id.to_string().as_str())
    );
    let snapshot = rebuilt.middleware_snapshot(456).unwrap();
    assert_eq!(snapshot, live.middleware_snapshot(456).unwrap());
    assert!(snapshot.id.is_none());
    assert_eq!(
        serde_json::from_str::<Value>(&snapshot.data).unwrap(),
        json!({
            "timestamp_ms": 456, "flow_id": "flow", "flow_name": "demo", "vector_clock": envelope.envelope.provenance.journal.vector_clock,
            "middleware": [{"stage_id": stage.to_string(), "stage_name": "worker", "rate_limiter": {"mode": "limiting", "revision": 7, "state_updated_at_ms": envelope.envelope.provenance.event.timestamp}}]
        })
    );
    let running = fact(SystemPayload::PipelineLifecycle(
        PipelineLifecycleEvent::Running {
            stage_count: Some(1),
        },
    ));
    let frames = live.project(&running, 789);
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].event.as_deref(), Some("flow_lifecycle"));
    assert_eq!(
        frames[1].event.as_deref(),
        Some("middleware_state_snapshot")
    );
    assert!(frames[1].id.is_none());
    assert_eq!(
        serde_json::from_str::<Value>(&frames[1].data).unwrap()["timestamp_ms"],
        789
    );
}

#[test]
fn stage_bootstrap_retains_terminal_accounting_after_an_empty_duplicate() {
    let stage = StageId::new();
    let metrics = serde_json::to_value(ExecutionAccounting {
        events_processed_total: 100,
        events_emitted_total: 99,
        errors_total: 1,
        ..Default::default()
    })
    .unwrap();
    let enriched = fact(SystemPayload::StageLifecycle {
        stage_id: stage,
        event: StageLifecycleEvent::Completed {
            accounting: Some(serde_json::from_value(metrics.clone()).unwrap()),
        },
    });
    let duplicate = fact(SystemPayload::StageLifecycle {
        stage_id: stage,
        event: StageLifecycleEvent::Completed { accounting: None },
    });
    let mut projection = StudioProjection::new(vec![], ContractBoundaryAliases::default()).unwrap();
    projection.rebuild(&enriched);
    projection.rebuild(&duplicate);
    let snapshots = projection.snapshots();
    assert_eq!(snapshots.len(), 1);
    assert!(snapshots[0].id.is_none());
    let payload: Value = serde_json::from_str(&snapshots[0].data).unwrap();
    assert_eq!(
        payload["timestamp_ms"],
        enriched.envelope.provenance.event.timestamp
    );
    assert_eq!(payload["accounting"], metrics);
}

fn payload(frame: &SseFrame) -> Value {
    serde_json::from_str(&frame.data).unwrap()
}

#[test]
fn every_stage_message_has_the_same_payload_live_and_in_a_snapshot() {
    let stage = StageId::new();
    let cases = [
        (
            StageLifecycleEvent::Running,
            json!({"event_type": "stage_running"}),
        ),
        (
            StageLifecycleEvent::Draining { accounting: None },
            json!({"event_type": "stage_draining"}),
        ),
        (
            StageLifecycleEvent::Drained,
            json!({"event_type": "stage_drained"}),
        ),
        (
            StageLifecycleEvent::Completed { accounting: None },
            json!({"event_type": "stage_completed"}),
        ),
        (
            StageLifecycleEvent::Cancelled {
                reason: "requested".into(),
                accounting: None,
            },
            json!({"event_type": "stage_cancelled", "reason": "requested"}),
        ),
        (
            StageLifecycleEvent::Failed {
                error: "failed".into(),
                recoverable: Some(false),
                accounting: None,
                causal_event_id: Some(EventId::new()),
            },
            json!({"event_type": "stage_failed", "error": "failed", "recoverable": false}),
        ),
    ];
    for (event, mut expected) in cases {
        let envelope = fact(SystemPayload::StageLifecycle {
            stage_id: stage,
            event,
        });
        let mut projection =
            StudioProjection::new(vec![], ContractBoundaryAliases::default()).unwrap();
        let live = projection.project(&envelope, 123).remove(0);
        expected["system_event_type"] = json!("stage_lifecycle");
        expected["stage_id"] = json!(stage.to_string());
        expected["timestamp_ms"] = json!(envelope.envelope.provenance.event.timestamp);
        expected["vector_clock"] = json!(envelope.envelope.provenance.journal.vector_clock);
        assert_eq!(payload(&live), expected);
        assert_eq!(live.event.as_deref(), Some("stage_lifecycle"));
        assert_eq!(
            live.id,
            Some(envelope.envelope.provenance.event.id.to_string())
        );

        let mut rebuilt =
            StudioProjection::new(vec![], ContractBoundaryAliases::default()).unwrap();
        rebuilt.rebuild(&envelope);
        let snapshot = rebuilt.snapshots().remove(0);
        assert_eq!(payload(&snapshot), expected);
        assert_eq!(snapshot.event, live.event);
        assert!(snapshot.id.is_none());
    }
}

#[test]
fn flow_replay_and_metrics_messages_preserve_the_studio_wire_vocabulary() {
    // Each pair is a journal event followed by the JSON Studio should receive.
    let flow_cases = [
        (
            json!({"pipeline_event": "starting"}),
            json!({"event_type": "flow_starting"}),
        ),
        (
            json!({"pipeline_event": "ready_for_run"}),
            json!({"event_type": "flow_ready_for_run"}),
        ),
        (
            json!({"pipeline_event": "running", "stage_count": 2}),
            json!({"event_type": "flow_running", "stage_count": 2}),
        ),
        (
            json!({"pipeline_event": "stop_admitted", "admission": {"mode": "graceful", "timeout_ms": 500}}),
            json!({"event_type": "flow_stop_admitted", "admission": {"mode": "graceful", "timeout_ms": 500}}),
        ),
        (
            json!({"pipeline_event": "not_started"}),
            json!({"event_type": "flow_not_started"}),
        ),
        (
            json!({"pipeline_event": "draining", "metrics": {"events_in_total": 3, "events_out_total": 2, "errors_total": 1}}),
            json!({"event_type": "flow_draining", "metrics": {"events_in_total": 3, "events_out_total": 2, "errors_total": 1}}),
        ),
        (
            json!({"pipeline_event": "all_stages_completed"}),
            json!({"event_type": "flow_stages_completed"}),
        ),
        (
            json!({"pipeline_event": "drained"}),
            json!({"event_type": "flow_drained"}),
        ),
        (
            json!({"pipeline_event": "completed", "duration_ms": 10, "metrics": {"events_in_total": 3, "events_out_total": 2, "errors_total": 1}}),
            json!({"event_type": "flow_completed", "duration_ms": 10, "metrics": {"events_in_total": 3, "events_out_total": 2, "errors_total": 1}}),
        ),
        (
            json!({"pipeline_event": "failed", "reason": "missing input", "duration_ms": 10, "failure_cause": {"violation_type": "other", "details": "missing"}}),
            json!({"event_type": "flow_failed", "reason": "missing input", "duration_ms": 10, "failure_cause": {"violation_type": "other", "details": "missing"}}),
        ),
        (
            json!({"pipeline_event": "cancelled", "reason": "requested", "duration_ms": 10}),
            json!({"event_type": "flow_cancelled", "reason": "requested", "duration_ms": 10}),
        ),
    ];
    for (journal, expected) in flow_cases {
        assert_fact_payload(
            fact(SystemPayload::PipelineLifecycle(
                serde_json::from_value(journal).unwrap(),
            )),
            "flow_lifecycle",
            Some("pipeline_lifecycle"),
            expected,
        );
    }

    let replay_cases = [
        (
            json!({"replay_event": "started", "archive_path": "/archive", "archive_flow_id": "archived", "archive_status": "completed", "archive_status_derivation": {"terminal_events_found": 1, "chosen": "completed"}, "allow_incomplete": false, "source_stages": ["input"]}),
            json!({"event_type": "replay_started", "archive_path": "/archive", "archive_flow_id": "archived", "archive_status": "completed", "archive_status_derivation": {"terminal_events_found": 1, "chosen": "completed"}, "allow_incomplete": false, "source_stages": ["input"]}),
        ),
        (
            json!({"replay_event": "completed", "replayed_count": 3, "skipped_count": 0, "duration_ms": 10}),
            json!({"event_type": "replay_completed", "replayed_count": 3, "skipped_count": 0, "duration_ms": 10}),
        ),
        (
            json!({"replay_event": "resumed_live", "archive_flow_id": "archived", "replayed_count": 3, "generation": 1}),
            json!({"event_type": "resumed_live", "archive_flow_id": "archived", "replayed_count": 3, "generation": 1}),
        ),
    ];
    for (journal, expected) in replay_cases {
        let envelope = fact(SystemPayload::ReplayLifecycle(
            serde_json::from_value(journal).unwrap(),
        ));
        assert_fact_payload(
            envelope.clone(),
            "replay_lifecycle",
            Some("replay_lifecycle"),
            expected.clone(),
        );
        let stage = StageId::new();
        let mut stage_envelope = envelope;
        stage_envelope.envelope.provenance.event.writer_id = WriterId::from(stage);
        let mut expected = expected;
        expected["stage_id"] = json!(stage.to_string());
        assert_fact_payload(
            stage_envelope,
            "replay_lifecycle",
            Some("replay_lifecycle"),
            expected,
        );
    }

    use obzenflow_core::event::MetricsCoordinationEvent;
    for (event, name) in [
        (MetricsCoordinationEvent::Ready, "metrics_ready"),
        (
            MetricsCoordinationEvent::DrainRequested,
            "metrics_drain_requested",
        ),
        (MetricsCoordinationEvent::Drained, "metrics_drained"),
        (MetricsCoordinationEvent::Shutdown, "metrics_shutdown"),
    ] {
        assert_fact_payload(
            fact(SystemPayload::MetricsCoordination(event)),
            "metrics_coordination",
            Some("metrics_coordination"),
            json!({"event_type": name}),
        );
    }
    let watermark = obzenflow_core::event::vector_clock::VectorClock::new();
    let envelope = fact(SystemPayload::MetricsCoordination(
        MetricsCoordinationEvent::Exported {
            watermark: watermark.clone(),
        },
    ));
    let expected = json!({"export_id": envelope.envelope.provenance.event.id.to_string(), "watermark": watermark});
    assert_fact_payload(envelope, "metrics_watermark", None, expected);
}

fn assert_fact_payload(
    envelope: SystemJournalRecord,
    name: &str,
    family: Option<&str>,
    mut expected: Value,
) {
    if let Some(family) = family {
        expected["system_event_type"] = json!(family);
    }
    expected["timestamp_ms"] = json!(envelope.envelope.provenance.event.timestamp);
    expected["vector_clock"] = json!(envelope.envelope.provenance.journal.vector_clock);
    let mut projection = StudioProjection::new(vec![], ContractBoundaryAliases::default()).unwrap();
    let frames = projection.project(&envelope, 123);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].event.as_deref(), Some(name));
    assert_eq!(
        frames[0].id,
        Some(envelope.envelope.provenance.event.id.to_string())
    );
    assert_eq!(payload(&frames[0]), expected);
}

#[test]
fn discarded_commands_remain_visible_as_journal_backed_studio_facts() {
    use obzenflow_core::event::CommandDiscardDisposition;
    for (command, disposition, error, label) in [
        (
            "Ready",
            CommandDiscardDisposition::ObsoleteControl,
            None,
            "obsolete_control",
        ),
        (
            "Error",
            CommandDiscardDisposition::UnexpectedError,
            Some("late failure"),
            "unexpected_error",
        ),
    ] {
        let stage_id = StageId::new();
        let envelope = JournalRecord::new(
            JournalWriterId::from(JournalId::new()),
            SystemEvent::new(
                WriterId::from(stage_id),
                SystemPayload::SupervisorCommandDiscarded {
                    supervisor: "transform_orders".into(),
                    terminal_state: "Drained".into(),
                    command: command.into(),
                    disposition,
                    error: error.map(str::to_owned),
                },
            ),
        );
        let mut expected = json!({
            "stage_id": stage_id.to_string(),
            "supervisor": "transform_orders",
            "terminal_state": "Drained",
            "command": command,
            "disposition": label,
        });
        if let Some(error) = error {
            expected["error"] = json!(error);
        }
        assert_fact_payload(
            envelope,
            "supervisor_command_discarded",
            Some("supervisor_command_discarded"),
            expected,
        );
    }
}

#[test]
fn middleware_transitions_and_snapshots_survive_every_replay_to_live_boundary() {
    use obzenflow_core::event::observability::*;
    let stage = StageId::new();
    let capture_scope = CaptureScope {
        flow_id: FlowId::new(),
        resume_generation: Default::default(),
    };
    enum Input {
        Fact(Box<SystemJournalRecord>),
        Measurement(Box<ObservabilityContext>),
    }
    let measured = |seq, record| {
        let mut packet = ObservabilityContext::new(CaptureStamp {
            capture_scope,
            observer: stage.into(),
            capture_seq: CaptureSeq(seq),
            capture_reason: CaptureReason::Periodic,
            observed_at_ms: seq,
        });
        packet.records.push(record);
        Input::Measurement(Box::new(packet))
    };
    let factual = |revision, middleware| {
        Input::Fact(Box::new(fact(SystemPayload::MiddlewareLifecycle {
            stage_id: stage,
            stage_name: Some("worker".into()),
            flow_id: Some("flow".into()),
            flow_name: Some("demo".into()),
            origin: MiddlewareEventOrigin {
                event_id: EventId::new(),
                writer_key: "worker".into(),
                seq: SeqNo(revision),
            },
            middleware,
        })))
    };
    let summary = |successes| ObservationRecord::CircuitBreakerSummary {
        effect_type: None,
        window_duration_s: 5,
        requests_processed: 20,
        requests_rejected: 2,
        observed_state: CircuitState::Closed,
        consecutive_failures: 0,
        rejection_rate: 0.1,
        successes_total: successes,
        failures_total: 2,
        opened_total: 0,
        time_in_closed_seconds: 5.0,
        time_in_open_seconds: 0.0,
        time_in_half_open_seconds: 0.0,
    };
    let tape = vec![
        measured(1, summary(18)),
        factual(
            7,
            MiddlewareFact::CircuitBreaker(CircuitBreakerFact::Opened {
                error_rate: 0.5,
                failure_count: 2,
                trigger: CircuitBreakerOpenTrigger::FailureRate,
                observed_calls: 4,
                slow_call_rate: Some(0.25),
                slow_call_count: Some(1),
                last_error: Some("timeout".into()),
            }),
        ),
        measured(2, summary(19)),
        factual(
            8,
            MiddlewareFact::CircuitBreaker(CircuitBreakerFact::HalfOpen {
                test_request_count: 1,
            }),
        ),
        factual(
            9,
            MiddlewareFact::CircuitBreaker(CircuitBreakerFact::Closed {
                success_count: 1,
                recovery_duration_ms: 250,
            }),
        ),
        measured(
            1000,
            ObservationRecord::RateLimiterUtilisation {
                effect_type: None,
                utilization_percent: 25.0,
                events_in_window: 5,
                window_size_ms: 1000,
            },
        ),
        factual(
            7,
            MiddlewareFact::RateLimiter(RateLimiterFact::ModeChange {
                mode_from: RateLimiterMode::Normal,
                mode_to: RateLimiterMode::Limiting,
                limit_rate: 10.0,
            }),
        ),
        measured(
            1001,
            ObservationRecord::RateLimiterActivity {
                effect_type: None,
                window_ms: 1000,
                delayed_events: 3,
                delay_ms_total: 60,
                delay_ms_max: 30,
                limit_rate: 10.0,
            },
        ),
        factual(
            8,
            MiddlewareFact::RateLimiter(RateLimiterFact::ConfigChanged {
                old_rate: 10.0,
                new_rate: 20.0,
            }),
        ),
    ];
    let empty = StudioProjection::new(vec![], ContractBoundaryAliases::default()).unwrap();
    let apply = |view: &mut StudioProjection, input: &Input| match input {
        Input::Fact(record) => view.project(record, 123),
        Input::Measurement(packet) => view.project_measurements((**packet).clone()),
    };
    let mut live = empty.clone();
    let mut frames = Vec::new();
    let mut snapshots = Vec::new();
    for input in &tape {
        let projected = apply(&mut live, input);
        if matches!(input, Input::Measurement(_)) {
            for frame in &projected {
                assert!(frame.id.is_none());
                let body = payload(frame);
                assert!(body.get("capture").is_some());
                assert!(body.get("revision").is_none() && body.get("origin").is_none());
            }
        }
        frames.push(projected);
        snapshots.push(live.middleware_snapshot(456));
    }
    assert!(
        payload(snapshots[0].as_ref().unwrap())["middleware"][0]["circuit_breaker"]
            .get("state")
            .is_none()
    );
    assert!(payload(&frames[1][0]).get("state_from").is_none());
    assert_eq!(payload(&frames[1][0])["state_to"], "open");
    assert_eq!(
        payload(snapshots[2].as_ref().unwrap())["middleware"][0]["circuit_breaker"]["state"],
        "open"
    );
    assert_eq!(payload(&frames[3][0])["state_from"], "open");
    assert_eq!(payload(&frames[4][0])["state_from"], "half_open");
    let final_snapshot = payload(snapshots.last().unwrap().as_ref().unwrap());
    let cb = &final_snapshot["middleware"][0]["circuit_breaker"];
    assert_eq!(cb["state"], "closed");
    assert_eq!(cb["revision"], 9);
    assert_eq!(cb["successes_total"], 19);
    let rl = &final_snapshot["middleware"][0]["rate_limiter"];
    assert_eq!(rl["mode"], "limiting");
    assert_eq!(rl["revision"], 7);
    assert_eq!(rl["utilization_pct"], 25.0);
    for boundary in 0..=tape.len() {
        let mut resumed = empty.clone();
        for (index, input) in tape.iter().enumerate() {
            if index < boundary {
                match input {
                    Input::Fact(record) => resumed.rebuild(record),
                    Input::Measurement(packet) => {
                        resumed.project_measurements((**packet).clone());
                    }
                }
            } else {
                assert_eq!(
                    apply(&mut resumed, input),
                    frames[index],
                    "boundary {boundary}, update {index}"
                );
            }
            assert_eq!(
                resumed.middleware_snapshot(456),
                snapshots[index],
                "boundary {boundary}, update {index}"
            );
        }
    }
    assert!(apply(&mut live, &measured(1, summary(999))).is_empty());
    assert_eq!(
        payload(live.middleware_snapshot(456).as_ref().unwrap()),
        final_snapshot
    );
    let Input::Fact(mut carrier) = factual(
        10,
        MiddlewareFact::CircuitBreaker(CircuitBreakerFact::HalfOpen {
            test_request_count: 1,
        }),
    ) else {
        unreachable!()
    };
    let Input::Measurement(stale) = measured(1, summary(999)) else {
        unreachable!()
    };
    carrier.envelope.observability = Some(*stale);
    let frames = live.project(&carrier, 789);
    assert_eq!(
        frames.len(),
        1,
        "stale attachment does not suppress its carrier fact"
    );
    assert_eq!(payload(&frames[0])["revision"], 10);
    assert_eq!(frames[0].id, Some(carrier.id().to_string()));
    let snapshot = payload(live.middleware_snapshot(789).as_ref().unwrap());
    let cb = &snapshot["middleware"][0]["circuit_breaker"];
    assert_eq!(cb["state"], "half_open");
    assert_eq!(cb["revision"], 10);
    assert_eq!(cb["successes_total"], 19);
}
