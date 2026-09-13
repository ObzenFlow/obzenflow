// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Latest lifecycle snapshots by stage identity.

use obzenflow_core::event::event_envelope::SystemEventEnvelope;
use obzenflow_core::web::SseFrame;

#[derive(Clone, Default)]
pub(super) struct StageLifecycleView {
    /// Latest lifecycle envelope per stage (best-effort).
    ///
    /// Used to bootstrap new SSE clients so the UI can render stage state even
    /// if it connected after the original stage_running events were emitted.
    latest_by_stage: std::collections::BTreeMap<obzenflow_core::StageId, SystemEventEnvelope>,
}

impl StageLifecycleView {
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
        self.latest_by_stage
            .values()
            .filter_map(super::facts::stage_message)
            .map(|message| message.frame(None))
            .collect()
    }
}
