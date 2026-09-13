// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use obzenflow_core::event::{
    event_envelope::EventEnvelope,
    payloads::observability_payload::{MiddlewareLifecycle, RateLimiterEvent},
    system_event::MiddlewareEventOrigin,
    types::{SeqNo, WriterId},
    StageLifecycleEvent, SystemEvent,
};
use obzenflow_core::{
    id::{JournalId, SystemId},
    JournalWriterId, StageId,
};
use serde_json::{json, Value};

fn fact(event: SystemEventType) -> SystemEventEnvelope {
    EventEnvelope::new(
        JournalWriterId::from(JournalId::new()),
        SystemEvent::new(WriterId::from(SystemId::new()), event),
    )
}

#[test]
fn middleware_rebuild_and_live_projection_use_supplied_time_and_preserve_revisions() {
    let stage = StageId::new();
    let envelope = fact(SystemEventType::MiddlewareLifecycle {
        stage_id: stage,
        stage_name: Some("worker".into()),
        flow_id: Some("flow".into()),
        flow_name: Some("demo".into()),
        origin: MiddlewareEventOrigin {
            event_id: EventId::new(),
            writer_key: "worker".into(),
            seq: SeqNo(7),
        },
        middleware: MiddlewareLifecycle::RateLimiter(RateLimiterEvent::ModeChange {
            mode_from: "normal".into(),
            mode_to: "throttled".into(),
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
        Some(envelope.event.id.to_string().as_str())
    );
    let snapshot = rebuilt.middleware_snapshot(456).unwrap();
    assert_eq!(snapshot, live.middleware_snapshot(456).unwrap());
    assert!(snapshot.id.is_none());
    assert_eq!(
        serde_json::from_str::<Value>(&snapshot.data).unwrap(),
        json!({
            "timestamp_ms": 456, "flow_id": "flow", "flow_name": "demo", "vector_clock": envelope.vector_clock,
            "middleware": [{"stage_id": stage.to_string(), "stage_name": "worker", "rate_limiter": {"mode": "throttled", "revision": 7}}]
        })
    );
    let running = fact(SystemEventType::PipelineLifecycle(
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
fn stage_bootstrap_retains_enriched_terminal_metrics_after_an_empty_duplicate() {
    let stage = StageId::new();
    let metrics = json!({
        "events_processed_total": 100, "events_accumulated_total": 0, "events_emitted_total": 99,
        "errors_total": 1, "errors_by_kind": {}, "in_flight": 0,
        "recent_p50_ms": 0, "recent_p90_ms": 0, "recent_p95_ms": 0, "recent_p99_ms": 0, "recent_p999_ms": 0,
        "processing_time_sum_nanos": 123, "event_loops_total": 100, "event_loops_with_work_total": 100
    });
    let enriched = fact(SystemEventType::StageLifecycle {
        stage_id: stage,
        event: StageLifecycleEvent::Completed {
            metrics: Some(serde_json::from_value(metrics.clone()).unwrap()),
        },
    });
    let duplicate = fact(SystemEventType::StageLifecycle {
        stage_id: stage,
        event: StageLifecycleEvent::Completed { metrics: None },
    });
    let mut projection = StudioProjection::new(vec![], ContractBoundaryAliases::default()).unwrap();
    projection.rebuild(&enriched);
    projection.rebuild(&duplicate);
    let snapshots = projection.snapshots();
    assert_eq!(snapshots.len(), 1);
    assert!(snapshots[0].id.is_none());
    let payload: Value = serde_json::from_str(&snapshots[0].data).unwrap();
    assert_eq!(payload["timestamp_ms"], enriched.event.timestamp);
    assert_eq!(payload["metrics"], metrics);
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
            StageLifecycleEvent::Draining { metrics: None },
            json!({"event_type": "stage_draining"}),
        ),
        (
            StageLifecycleEvent::Drained,
            json!({"event_type": "stage_drained"}),
        ),
        (
            StageLifecycleEvent::Completed { metrics: None },
            json!({"event_type": "stage_completed"}),
        ),
        (
            StageLifecycleEvent::Cancelled {
                reason: "requested".into(),
                metrics: None,
            },
            json!({"event_type": "stage_cancelled", "reason": "requested"}),
        ),
        (
            StageLifecycleEvent::Failed {
                error: "failed".into(),
                recoverable: Some(false),
                metrics: None,
                causal_event_id: Some(EventId::new()),
            },
            json!({"event_type": "stage_failed", "error": "failed", "recoverable": false}),
        ),
    ];
    for (event, mut expected) in cases {
        let envelope = fact(SystemEventType::StageLifecycle {
            stage_id: stage,
            event,
        });
        let mut projection =
            StudioProjection::new(vec![], ContractBoundaryAliases::default()).unwrap();
        let live = projection.project(&envelope, 123).remove(0);
        expected["system_event_type"] = json!("stage_lifecycle");
        expected["stage_id"] = json!(stage.to_string());
        expected["timestamp_ms"] = json!(envelope.event.timestamp);
        expected["vector_clock"] = json!(envelope.vector_clock);
        assert_eq!(payload(&live), expected);
        assert_eq!(live.event.as_deref(), Some("stage_lifecycle"));
        assert_eq!(live.id, Some(envelope.event.id.to_string()));

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
    // Inputs use journal field names; expected messages use Studio field names.
    // These are deliberately distinct schemas, including omitted optional fields.
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
            fact(SystemEventType::PipelineLifecycle(
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
        let envelope = fact(SystemEventType::ReplayLifecycle(
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
        stage_envelope.event.writer_id = WriterId::from(stage);
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
            fact(SystemEventType::MetricsCoordination(event)),
            "metrics_coordination",
            Some("metrics_coordination"),
            json!({"event_type": name}),
        );
    }
    let watermark = obzenflow_core::event::vector_clock::VectorClock::new();
    let envelope = fact(SystemEventType::MetricsCoordination(
        MetricsCoordinationEvent::Exported {
            watermark: watermark.clone(),
        },
    ));
    let expected = json!({"export_id": envelope.event.id.to_string(), "watermark": watermark});
    assert_fact_payload(envelope, "metrics_watermark", None, expected);
}

fn assert_fact_payload(
    envelope: SystemEventEnvelope,
    name: &str,
    family: Option<&str>,
    mut expected: Value,
) {
    if let Some(family) = family {
        expected["system_event_type"] = json!(family);
    }
    expected["timestamp_ms"] = json!(envelope.event.timestamp);
    expected["vector_clock"] = json!(envelope.vector_clock);
    let mut projection = StudioProjection::new(vec![], ContractBoundaryAliases::default()).unwrap();
    let frames = projection.project(&envelope, 123);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].event.as_deref(), Some(name));
    assert_eq!(frames[0].id, Some(envelope.event.id.to_string()));
    assert_eq!(payload(&frames[0]), expected);
}

#[test]
fn middleware_transitions_and_snapshots_survive_every_replay_to_live_boundary() {
    use obzenflow_core::event::payloads::observability_payload::{
        CircuitBreakerEvent, CircuitBreakerOpenTrigger,
    };

    let stage = StageId::new();
    let updates = [
        MiddlewareLifecycle::CircuitBreaker(CircuitBreakerEvent::Summary {
            window_duration_s: 5,
            requests_processed: 20,
            requests_rejected: 2,
            state: "CLOSED".into(),
            consecutive_failures: 0,
            rejection_rate: 0.1,
            successes_total: 18,
            failures_total: 2,
            opened_total: 0,
            time_in_closed_seconds: 5.0,
            time_in_open_seconds: 0.0,
            time_in_half_open_seconds: 0.0,
        }),
        MiddlewareLifecycle::CircuitBreaker(CircuitBreakerEvent::Opened {
            error_rate: 0.5,
            failure_count: 2,
            trigger: CircuitBreakerOpenTrigger::FailureRate,
            observed_calls: 4,
            slow_call_rate: Some(0.25),
            slow_call_count: Some(1),
            last_error: Some("timeout".into()),
        }),
        MiddlewareLifecycle::CircuitBreaker(CircuitBreakerEvent::HalfOpen {
            test_request_count: 1,
        }),
        MiddlewareLifecycle::CircuitBreaker(CircuitBreakerEvent::Closed {
            success_count: 1,
            recovery_duration_ms: 250,
        }),
        MiddlewareLifecycle::RateLimiter(RateLimiterEvent::WindowUtilization {
            utilization_percent: 25.0,
            events_in_window: 5,
            window_size_ms: 1000,
        }),
        MiddlewareLifecycle::RateLimiter(RateLimiterEvent::ModeChange {
            mode_from: "normal".into(),
            mode_to: "throttled".into(),
            limit_rate: 10.0,
        }),
        MiddlewareLifecycle::RateLimiter(RateLimiterEvent::ActivityPulse {
            window_ms: 1000,
            delayed_events: 3,
            delay_ms_total: 60,
            delay_ms_max: 30,
            limit_rate: 10.0,
        }),
        MiddlewareLifecycle::RateLimiter(RateLimiterEvent::ConfigChanged {
            old_rate: 10.0,
            new_rate: 20.0,
        }),
    ];
    let tape: Vec<_> = updates
        .into_iter()
        .enumerate()
        .map(|(index, middleware)| {
            fact(SystemEventType::MiddlewareLifecycle {
                stage_id: stage,
                stage_name: Some("worker".into()),
                flow_id: Some("flow".into()),
                flow_name: Some("demo".into()),
                origin: MiddlewareEventOrigin {
                    event_id: EventId::new(),
                    writer_key: "worker".into(),
                    seq: SeqNo(index as u64 + 1),
                },
                middleware,
            })
        })
        .collect();
    let empty = StudioProjection::new(vec![], ContractBoundaryAliases::default()).unwrap();
    let mut live = empty.clone();
    let mut frames = Vec::new();
    let mut snapshots = Vec::new();
    for envelope in &tape {
        frames.push(live.project(envelope, 123));
        snapshots.push(live.middleware_snapshot(456));
    }
    let opened = payload(&frames[1][0]);
    assert_eq!(opened["state_from"], "closed");
    assert_eq!(opened["state_to"], "open");
    assert_eq!(
        opened["context"],
        json!({"error_rate": 0.5, "failure_count": 2, "trigger": "failure_rate", "observed_calls": 4, "slow_call_rate": 0.25, "slow_call_count": 1, "last_error": "timeout"})
    );
    assert_eq!(payload(&frames[2][0])["state_from"], "open");
    assert_eq!(payload(&frames[3][0])["state_from"], "half_open");
    assert_eq!(payload(&frames[4][0])["mode"], "normal");
    assert_eq!(
        frames[7],
        vec![SseFrame::comment("unsupported_middleware_event_skipped")]
    );
    let snapshot = payload(snapshots.last().unwrap().as_ref().unwrap());
    assert_eq!(
        snapshot["middleware"],
        json!([{
            "stage_id": stage.to_string(), "stage_name": "worker",
            "circuit_breaker": {"state": "closed", "revision": 4, "requests_processed": 20, "requests_rejected": 2, "consecutive_failures": 0, "rejection_rate": 0.1, "successes_total": 18, "failures_total": 2, "opened_total": 0, "time_in_closed_s": 5.0, "time_in_open_s": 0.0, "time_in_half_open_s": 0.0},
            "rate_limiter": {"mode": "throttled", "revision": 6, "utilization_pct": 25.0, "events_in_window": 5, "window_size_ms": 1000}
        }])
    );

    for boundary in 0..=tape.len() {
        let mut resumed = empty.clone();
        for (index, envelope) in tape.iter().enumerate() {
            if index < boundary {
                resumed.rebuild(envelope);
            } else {
                assert_eq!(
                    resumed.project(envelope, 123),
                    frames[index],
                    "resume at {boundary}, fact {index}"
                );
            }
            assert_eq!(
                resumed.middleware_snapshot(456),
                snapshots[index],
                "resume at {boundary}, fact {index}"
            );
        }
    }
}
