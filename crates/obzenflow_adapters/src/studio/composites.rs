// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Provides the status Studio displays for a composite group.
//!
//! Core combines member-stage events into a group status; this module prepares
//! the `composite_status` messages. A group can still be running after one member
//! completes, so Studio needs its status as well as each stage's status.

use super::messages::{CompositeStatusPayloadV1, CompositeStatusWireV1, StudioMessage};
#[cfg(test)]
use obzenflow_core::composite::CompositeDefinition;
use obzenflow_core::composite::{CompositeLifecycleProjection, CompositeStatus};
use obzenflow_core::event::journal_record::SystemJournalRecord;
#[cfg(test)]
use obzenflow_core::event::SystemEvent;
use obzenflow_core::web::SseFrame;
use obzenflow_core::EventId;

#[derive(Clone, Debug)]
pub(super) struct CompositeStatusSnapshot {
    composite_id: obzenflow_core::id::CompositeId,
    status: CompositeStatus,
    revision: u64,
    as_of_event_id: Option<EventId>,
    timestamp_ms: u64,
}

#[derive(Clone)]
pub(super) struct CompositeLifecycleView {
    projection: CompositeLifecycleProjection,
    latest_by_composite:
        std::collections::BTreeMap<obzenflow_core::id::CompositeId, CompositeStatusSnapshot>,
    // Snapshots identify the last entry read, even if no group status changed.
    as_of_event_id: Option<EventId>,
    as_of_timestamp_ms: u64,
}

impl CompositeLifecycleView {
    pub(super) fn new(projection: CompositeLifecycleProjection) -> Self {
        let latest_by_composite = projection
            .statuses()
            .into_iter()
            .map(|(composite_id, status)| {
                (
                    composite_id.clone(),
                    CompositeStatusSnapshot {
                        composite_id,
                        status,
                        revision: 0,
                        as_of_event_id: None,
                        timestamp_ms: 0,
                    },
                )
            })
            .collect();

        Self {
            projection,
            latest_by_composite,
            as_of_event_id: None,
            as_of_timestamp_ms: 0,
        }
    }

    pub(super) fn observe(
        &mut self,
        envelope: &SystemJournalRecord,
    ) -> Option<CompositeStatusSnapshot> {
        use obzenflow_core::event::SystemPayload;

        self.as_of_event_id = Some(envelope.envelope.provenance.event.id);
        self.as_of_timestamp_ms = envelope.envelope.provenance.event.timestamp;

        let SystemPayload::StageLifecycle { stage_id, event } = &envelope.payload else {
            return None;
        };
        let composite_id = self.projection.composite_for_stage(*stage_id)?.clone();
        let before = self
            .projection
            .status(&composite_id)
            .expect("indexed composite has projection state");
        let apply_error = self.projection.apply(*stage_id, event).err();
        let after = self
            .projection
            .status(&composite_id)
            .expect("indexed composite has projection state");

        if before == after {
            return None;
        }

        if let Some(error) = apply_error {
            tracing::error!(
                composite = %composite_id,
                stage = %stage_id,
                error = %error,
                "Composite lifecycle projection detected an invalid member history"
            );
        }

        let snapshot = self
            .latest_by_composite
            .get_mut(&composite_id)
            .expect("projection snapshot exists for every composite");
        snapshot.status = after;
        snapshot.revision = snapshot.revision.saturating_add(1);
        snapshot.as_of_event_id = Some(envelope.envelope.provenance.event.id);
        snapshot.timestamp_ms = envelope.envelope.provenance.event.timestamp;
        Some(snapshot.clone())
    }

    fn snapshots(&self) -> Vec<CompositeStatusSnapshot> {
        self.latest_by_composite
            .values()
            .cloned()
            .map(|mut snapshot| {
                snapshot.as_of_event_id = self.as_of_event_id;
                snapshot.timestamp_ms = self.as_of_timestamp_ms;
                snapshot
            })
            .collect()
    }

    pub(super) fn snapshot_frames(&self) -> Vec<SseFrame> {
        self.snapshots()
            .iter()
            .filter_map(composite_status_frame)
            .collect()
    }
}

const COMPOSITE_STATUS_SCHEMA_V1: u32 = 1;

#[derive(Debug, thiserror::Error)]
#[error("composite status has no schema-v1 wire representation")]
pub(super) struct UnsupportedCompositeStatusV1;

impl TryFrom<&CompositeStatusSnapshot> for CompositeStatusPayloadV1 {
    type Error = UnsupportedCompositeStatusV1;

    fn try_from(snapshot: &CompositeStatusSnapshot) -> Result<Self, Self::Error> {
        let (status, reason, at, error) = match &snapshot.status {
            CompositeStatus::Waiting => (CompositeStatusWireV1::Waiting, None, None, None),
            CompositeStatus::Running => (CompositeStatusWireV1::Running, None, None, None),
            CompositeStatus::Completed => (CompositeStatusWireV1::Completed, None, None, None),
            CompositeStatus::Cancelled { reason } => (
                CompositeStatusWireV1::Cancelled,
                Some(reason.clone()),
                None,
                None,
            ),
            CompositeStatus::Failed { at, error } => (
                CompositeStatusWireV1::Failed,
                None,
                Some(at.to_string()),
                Some(error.clone()),
            ),
            CompositeStatus::Invalid { error } => (
                CompositeStatusWireV1::Invalid,
                None,
                None,
                Some(error.clone()),
            ),
            _ => return Err(UnsupportedCompositeStatusV1),
        };

        Ok(Self {
            schema_version: COMPOSITE_STATUS_SCHEMA_V1,
            message_type: "composite_status",
            composite_id: snapshot.composite_id.to_string(),
            status,
            revision: snapshot.revision,
            as_of_event_id: snapshot.as_of_event_id.map(|id| id.to_string()),
            timestamp_ms: snapshot.timestamp_ms,
            reason,
            at,
            error,
        })
    }
}

pub(super) fn composite_status_frame(snapshot: &CompositeStatusSnapshot) -> Option<SseFrame> {
    let payload = match CompositeStatusPayloadV1::try_from(snapshot) {
        Ok(payload) => payload,
        Err(error) => {
            tracing::error!(
                composite = %snapshot.composite_id,
                error = %error,
                "Composite status cannot be exposed on the schema-v1 SSE contract"
            );
            return None;
        }
    };
    // Reconnect IDs must refer to journal entries; this summary has no entry of its own.
    Some(StudioMessage::CompositeStatus(payload).frame(None))
}

#[cfg(test)]
fn composite_status_payload(
    snapshot: &CompositeStatusSnapshot,
) -> Result<serde_json::Value, UnsupportedCompositeStatusV1> {
    CompositeStatusPayloadV1::try_from(snapshot).map(|payload| {
        serde_json::to_value(payload)
            .expect("schema-v1 composite status DTO contains only serializable fields")
    })
}

#[cfg(test)]
mod composite_status_projection_tests {
    use super::*;
    use obzenflow_core::event::{JournalRecord, StageLifecycleEvent, SystemPayload, WriterId};
    use obzenflow_core::id::{CompositeId, RoleId, StageId, SystemId};

    async fn envelope(stage: StageId, event: StageLifecycleEvent) -> SystemJournalRecord {
        let system_id = SystemId::new();
        JournalRecord::new(
            obzenflow_core::event::JournalWriterId::from(obzenflow_core::id::JournalId::new()),
            SystemEvent::new(
                WriterId::from(system_id),
                SystemPayload::StageLifecycle {
                    stage_id: stage,
                    event,
                },
            ),
        )
    }

    fn state(map: StageId, finish: StageId) -> CompositeLifecycleView {
        CompositeLifecycleView::new(
            CompositeLifecycleProjection::new(vec![CompositeDefinition::new(
                CompositeId::new("ai_map_reduce:digest"),
                vec![(map, RoleId::new("map")), (finish, RoleId::new("finalize"))],
            )])
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn live_updates_are_state_changes_with_source_identity() {
        let map = StageId::new();
        let finish = StageId::new();
        let mut state = state(map, finish);

        let running_envelope = envelope(map, StageLifecycleEvent::Running).await;
        let running = state
            .observe(&running_envelope)
            .expect("first running changes the view");
        assert_eq!(running.status, CompositeStatus::Running);
        assert_eq!(running.revision, 1);
        assert_eq!(
            running.as_of_event_id,
            Some(running_envelope.envelope.provenance.event.id)
        );

        let sibling_running = envelope(finish, StageLifecycleEvent::Running).await;
        assert!(state.observe(&sibling_running).is_none());
        let catch_up_snapshot = state.snapshots().pop().unwrap();
        assert_eq!(catch_up_snapshot.revision, 1);
        assert_eq!(
            catch_up_snapshot.as_of_event_id,
            Some(sibling_running.envelope.provenance.event.id)
        );

        let map_completed =
            envelope(map, StageLifecycleEvent::Completed { accounting: None }).await;
        assert!(state.observe(&map_completed).is_none());

        let finish_completed = envelope(finish, StageLifecycleEvent::Drained).await;
        let completed = state
            .observe(&finish_completed)
            .expect("all terminal changes the view");
        assert_eq!(completed.status, CompositeStatus::Completed);
        assert_eq!(completed.revision, 2);

        let payload = composite_status_payload(&completed).unwrap();
        assert_eq!(payload["schema_version"], COMPOSITE_STATUS_SCHEMA_V1);
        assert_eq!(payload["message_type"], "composite_status");
        assert_eq!(payload["status"], "completed");
        assert_eq!(payload["revision"], 2);
        assert_eq!(
            payload["as_of_event_id"],
            finish_completed.envelope.provenance.event.id.to_string()
        );
        assert!(payload.get("system_event_type").is_none());
    }

    #[tokio::test]
    async fn contradictory_tape_is_an_explicit_invalid_view() {
        let map = StageId::new();
        let finish = StageId::new();
        let mut state = state(map, finish);

        let completed = envelope(map, StageLifecycleEvent::Completed { accounting: None }).await;
        assert!(state.observe(&completed).is_none());

        let cancelled = envelope(
            map,
            StageLifecycleEvent::Cancelled {
                reason: "late stop".to_string(),
                accounting: None,
            },
        )
        .await;
        let invalid = state
            .observe(&cancelled)
            .expect("integrity failure changes the view");
        assert!(matches!(invalid.status, CompositeStatus::Invalid { .. }));
        assert_eq!(
            composite_status_payload(&invalid).unwrap()["status"],
            "invalid"
        );
    }

    #[test]
    fn fresh_snapshot_includes_waiting_composites_at_revision_zero() {
        let map = StageId::new();
        let finish = StageId::new();
        let state = state(map, finish);
        let snapshot = state
            .latest_by_composite
            .get(&CompositeId::new("ai_map_reduce:digest"))
            .unwrap();

        assert_eq!(snapshot.status, CompositeStatus::Waiting);
        assert_eq!(snapshot.revision, 0);
        assert!(snapshot.as_of_event_id.is_none());
    }

    #[test]
    fn schema_v1_golden_covers_the_complete_status_vocabulary() {
        let cases = [
            (CompositeStatus::Waiting, "waiting", None, None, None),
            (CompositeStatus::Running, "running", None, None, None),
            (CompositeStatus::Completed, "completed", None, None, None),
            (
                CompositeStatus::Cancelled {
                    reason: "operator stop".to_string(),
                },
                "cancelled",
                Some("operator stop"),
                None,
                None,
            ),
            (
                CompositeStatus::Failed {
                    at: RoleId::new("map"),
                    error: "provider unavailable".to_string(),
                },
                "failed",
                None,
                Some("map"),
                Some("provider unavailable"),
            ),
            (
                CompositeStatus::Invalid {
                    error: "conflicting member terminals".to_string(),
                },
                "invalid",
                None,
                None,
                Some("conflicting member terminals"),
            ),
        ];

        for (status, expected_status, expected_reason, expected_at, expected_error) in cases {
            let snapshot = CompositeStatusSnapshot {
                composite_id: CompositeId::new("ai_map_reduce:digest"),
                status,
                revision: 2,
                as_of_event_id: None,
                timestamp_ms: 178,
            };
            let payload = composite_status_payload(&snapshot).unwrap();
            assert_eq!(payload["schema_version"], 1);
            assert_eq!(payload["message_type"], "composite_status");
            assert_eq!(payload["composite_id"], "ai_map_reduce:digest");
            assert_eq!(payload["status"], expected_status);
            assert_eq!(payload["revision"], 2);
            assert!(payload["as_of_event_id"].is_null());
            assert_eq!(payload["timestamp_ms"], 178);
            assert_eq!(
                payload.get("reason").and_then(|v| v.as_str()),
                expected_reason
            );
            assert_eq!(payload.get("at").and_then(|v| v.as_str()), expected_at);
            assert_eq!(
                payload.get("error").and_then(|v| v.as_str()),
                expected_error
            );
            assert!(payload.get("system_event_type").is_none());
        }
    }

    #[test]
    fn composite_status_frames_remain_cursorless() {
        let snapshot = CompositeStatusSnapshot {
            composite_id: CompositeId::new("test:pair"),
            status: CompositeStatus::Running,
            revision: 1,
            as_of_event_id: Some(EventId::new()),
            timestamp_ms: 178,
        };
        let frame = composite_status_frame(&snapshot).unwrap();
        assert_eq!(frame.event.as_deref(), Some("composite_status"));
        assert_eq!(frame.id, None);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&frame.data).unwrap()["as_of_event_id"],
            snapshot.as_of_event_id.unwrap().to_string()
        );
    }
}
