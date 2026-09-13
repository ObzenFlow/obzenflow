// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Latest lifecycle snapshots by stage identity.

use obzenflow_core::event::{event_envelope::SystemEventEnvelope, SystemEvent};
use obzenflow_core::web::SseFrame;

#[derive(Clone, Default)]
pub(super) struct StageLifecycleSseState {
    /// Latest lifecycle envelope per stage (best-effort).
    ///
    /// Used to bootstrap new SSE clients so the UI can render stage state even
    /// if it connected after the original stage_running events were emitted.
    latest_by_stage: std::collections::BTreeMap<obzenflow_core::StageId, SystemEventEnvelope>,
}

impl StageLifecycleSseState {
    pub(super) fn observe(&mut self, envelope: &SystemEventEnvelope) {
        use obzenflow_core::event::system_event::StageLifecycleEvent;
        use obzenflow_core::event::SystemEventType;

        let SystemEventType::StageLifecycle { stage_id, event } = &envelope.event.event else {
            return;
        };

        // Prefer terminal lifecycle events with metrics when duplicates exist
        // (some stages write both a supervisor completion marker and a later
        // metrics-enriched completion event).
        let should_replace = match (self.latest_by_stage.get(stage_id), event) {
            (None, _) => true,
            (Some(prev), StageLifecycleEvent::Completed { metrics: None }) => !matches!(
                prev.event.event,
                SystemEventType::StageLifecycle {
                    event: StageLifecycleEvent::Completed { metrics: Some(_) },
                    ..
                }
            ),
            (Some(prev), StageLifecycleEvent::Cancelled { metrics: None, .. }) => !matches!(
                prev.event.event,
                SystemEventType::StageLifecycle {
                    event: StageLifecycleEvent::Cancelled {
                        metrics: Some(_),
                        ..
                    },
                    ..
                }
            ),
            (Some(prev), StageLifecycleEvent::Failed { metrics: None, .. }) => !matches!(
                prev.event.event,
                SystemEventType::StageLifecycle {
                    event: StageLifecycleEvent::Failed {
                        metrics: Some(_),
                        ..
                    },
                    ..
                }
            ),
            _ => true,
        };

        if should_replace {
            self.latest_by_stage.insert(*stage_id, envelope.clone());
        }
    }

    pub(super) fn build_snapshot_sse_events(&self) -> Vec<SseFrame> {
        let mut out = Vec::new();
        for envelope in self.latest_by_stage.values() {
            if let Some(ev) = map_stage_lifecycle_to_sse_snapshot(envelope) {
                out.push(ev);
            }
        }
        out
    }
}

fn map_stage_lifecycle_to_sse_snapshot(envelope: &SystemEventEnvelope) -> Option<SseFrame> {
    use obzenflow_core::event::system_event::StageLifecycleEvent;
    use obzenflow_core::event::SystemEventType;
    use serde_json::json;

    let event: &SystemEvent = &envelope.event;
    let vector_clock_value = serde_json::to_value(&envelope.vector_clock).ok();

    let SystemEventType::StageLifecycle {
        stage_id,
        event: lifecycle,
    } = &event.event
    else {
        return None;
    };

    let (event_type, metrics_value, error, recoverable, reason) = match lifecycle {
        StageLifecycleEvent::Running => ("stage_running", None, None, None, None),
        StageLifecycleEvent::Draining { metrics } => (
            "stage_draining",
            metrics.as_ref().and_then(|m| serde_json::to_value(m).ok()),
            None,
            None,
            None,
        ),
        StageLifecycleEvent::Drained => ("stage_drained", None, None, None, None),
        StageLifecycleEvent::Completed { metrics } => (
            "stage_completed",
            metrics.as_ref().and_then(|m| serde_json::to_value(m).ok()),
            None,
            None,
            None,
        ),
        StageLifecycleEvent::Cancelled { reason, metrics } => (
            "stage_cancelled",
            metrics.as_ref().and_then(|m| serde_json::to_value(m).ok()),
            None,
            None,
            Some(reason.clone()),
        ),
        StageLifecycleEvent::Failed {
            error,
            recoverable,
            metrics,
            ..
        } => (
            "stage_failed",
            metrics.as_ref().and_then(|m| serde_json::to_value(m).ok()),
            Some(error.clone()),
            *recoverable,
            None,
        ),
    };

    let mut data = json!({
        "system_event_type": "stage_lifecycle",
        "event_type": event_type,
        "stage_id": stage_id.to_string(),
        "timestamp_ms": event.timestamp,
    });

    if let Some(vc) = &vector_clock_value {
        data["vector_clock"] = vc.clone();
    }
    if let Some(m) = metrics_value {
        data["metrics"] = m;
    }
    if let Some(err) = error {
        data["error"] = serde_json::Value::String(err);
    }
    if let Some(r) = reason {
        data["reason"] = serde_json::Value::String(r);
    }
    if let Some(rec) = recoverable {
        data["recoverable"] = serde_json::Value::Bool(rec);
    }

    Some(SseFrame::event("stage_lifecycle", data.to_string()))
}
