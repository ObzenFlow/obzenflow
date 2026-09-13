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
    let mut rebuilt =
        FlowEventsProjection::new(vec![], ContractBoundaryAliases::default()).unwrap();
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
    let mut projection =
        FlowEventsProjection::new(vec![], ContractBoundaryAliases::default()).unwrap();
    projection.rebuild(&enriched);
    projection.rebuild(&duplicate);
    let snapshots = projection.snapshots();
    assert_eq!(snapshots.len(), 1);
    assert!(snapshots[0].id.is_none());
    let payload: Value = serde_json::from_str(&snapshots[0].data).unwrap();
    assert_eq!(payload["timestamp_ms"], enriched.event.timestamp);
    assert_eq!(payload["metrics"], metrics);
}
