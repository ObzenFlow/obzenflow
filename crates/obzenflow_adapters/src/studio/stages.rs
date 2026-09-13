// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Keeps each stage's latest status and any final metrics for Studio's initial
//! display, including stages that finished before the browser connected.

use obzenflow_core::event::event_envelope::SystemEventEnvelope;
use obzenflow_core::web::SseFrame;

#[derive(Clone, Default)]
pub(super) struct StageLifecycleView {
    latest_by_stage: std::collections::BTreeMap<obzenflow_core::StageId, SystemEventEnvelope>,
}

impl StageLifecycleView {
    pub(super) fn observe(&mut self, envelope: &SystemEventEnvelope) {
        use obzenflow_core::event::system_event::StageLifecycleEvent;
        use obzenflow_core::event::SystemEventType;

        let SystemEventType::StageLifecycle { stage_id, event } = &envelope.event.event else {
            return;
        };

        // A stage can report the same outcome again without metrics. Preserve the
        // earlier totals so a newly connected browser still sees them.
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

    pub(super) fn snapshot_frames(&self) -> Vec<SseFrame> {
        self.latest_by_stage
            .values()
            .filter_map(super::facts::stage_message)
            .map(|message| message.frame(None))
            .collect()
    }
}
