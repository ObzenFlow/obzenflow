// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::types::{Count, DurationMs};
use obzenflow_core::event::{ReplayLifecycleEvent, SystemEvent, SystemEventType, WriterId};
use obzenflow_core::journal::Journal;
use obzenflow_core::StageId;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Default, Clone)]
pub(crate) struct ReplayCompletionGuard {
    emitted: bool,
}

impl ReplayCompletionGuard {
    pub(crate) async fn maybe_emit_completed(
        &mut self,
        stage_id: StageId,
        stage_name: &str,
        system_journal: &Arc<dyn Journal<SystemEvent>>,
        replay_started_at: Option<Instant>,
        replayed_count: u64,
        skipped_count: u64,
    ) {
        if self.emitted {
            return;
        }
        self.emitted = true;

        let duration_ms = replay_started_at
            .map(|started| {
                let ms = started.elapsed().as_millis();
                u64::try_from(ms).unwrap_or(u64::MAX)
            })
            .unwrap_or(0);

        let completed_event = SystemEvent::new(
            WriterId::from(stage_id),
            SystemEventType::ReplayLifecycle(ReplayLifecycleEvent::Completed {
                replayed_count: Count(replayed_count),
                skipped_count: Count(skipped_count),
                duration_ms: DurationMs(duration_ms),
            }),
        );

        if let Err(e) = system_journal.append(completed_event, None).await {
            tracing::error!(
                stage_name = %stage_name,
                journal_error = %e,
                "Failed to append ReplayLifecycle::Completed system event"
            );
        }
    }
}
