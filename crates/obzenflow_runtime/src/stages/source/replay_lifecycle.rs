// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::event::types::{Count, DurationMs};
use obzenflow_core::event::{ReplayLifecycleEvent, SystemEvent, SystemEventType, WriterId};
use obzenflow_core::journal::Journal;
use obzenflow_core::StageId;
use std::sync::Arc;
use std::time::Instant;

/// The completion facts one exhaustion (or resume handoff) records.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReplayCompletionFacts {
    pub replayed_count: u64,
    pub skipped_count: u64,
    /// Terminal EOF kind synthesized at exhaustion (FLOWIP-095k); `None` on
    /// the resume handoff, which synthesizes no terminal EOF.
    pub synthesized_eof_kind: Option<EofKind>,
}

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
        facts: ReplayCompletionFacts,
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
                replayed_count: Count(facts.replayed_count),
                skipped_count: Count(facts.skipped_count),
                duration_ms: DurationMs(duration_ms),
                synthesized_eof_kind: facts.synthesized_eof_kind,
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
