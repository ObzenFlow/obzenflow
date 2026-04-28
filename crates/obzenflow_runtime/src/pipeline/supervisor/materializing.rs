// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{BoxError, PipelineContext, PipelineEvent, PipelineSupervisor};
use crate::supervised_base::EventLoopDirective;

pub(super) async fn dispatch_materializing(
    _supervisor: &mut PipelineSupervisor,
    context: &mut PipelineContext,
) -> Result<EventLoopDirective<PipelineEvent>, BoxError> {
    // Check if all stages have been initialised.
    let supervisors = &context.stage_supervisors;
    let source_supers = &context.source_supervisors;
    let expected_count = context.topology.stages().count();
    let initialized_count = supervisors.len() + source_supers.len();

    if initialized_count == expected_count && expected_count > 0 {
        // All stages created, transition to Materialized.
        tracing::info!("✅ All stages initialized, transitioning to Materialized");
        Ok(EventLoopDirective::Transition(
            PipelineEvent::MaterializationComplete,
        ))
    } else {
        // Still waiting for stages to be created. Log details once before failing hard.
        tracing::error!(
            initialized_count,
            expected_count,
            "⚠️ MISMATCH DETECTED: supervisors vs topology stages"
        );
        tracing::debug!(
            supervisors = ?supervisors
                .keys()
                .map(|id| format!("{id:?}"))
                .collect::<Vec<_>>(),
            sources = ?source_supers
                .keys()
                .map(|id| format!("{id:?}"))
                .collect::<Vec<_>>(),
            topology = ?context
                .topology
                .stages()
                .map(|s| format!("{} ({:?})", s.name, s.id))
                .collect::<Vec<_>>(),
            "Materialization mismatch details"
        );

        Ok(EventLoopDirective::Transition(PipelineEvent::Error {
            message: format!(
                "Stage count mismatch: {initialized_count} supervisors vs {expected_count} topology stages"
            ),
        }))
    }
}
