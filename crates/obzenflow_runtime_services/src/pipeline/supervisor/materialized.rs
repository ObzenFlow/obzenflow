// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{startup_mode_manual, BoxError, PipelineContext, PipelineEvent, PipelineSupervisor};
use crate::id_conversions::StageIdExt;
use crate::messaging::SubscriptionPoller;
use crate::supervised_base::EventLoopDirective;
use obzenflow_core::StageId;
use std::time::Duration;

pub(super) async fn dispatch_materialized(
    supervisor: &mut PipelineSupervisor,
    context: &mut PipelineContext,
) -> Result<EventLoopDirective<PipelineEvent>, BoxError> {
    // First check if any stages are already running (they might have published before we subscribed).
    let supervisors = &context.stage_supervisors;
    for (stage_id, stage) in supervisors.iter() {
        if stage.is_ready()
            && !context
                .topology
                .upstream_stages(stage_id.to_topology_id())
                .is_empty()
        {
            // This is a non-source stage that's already running.
            if context.running_stages.insert(*stage_id) {
                tracing::info!("Stage '{}' was already running", stage.stage_name());
            }
        }
    }

    // Poll for stage running events to know when all non-source stages are ready.
    let subscription = context
        .completion_subscription
        .as_mut()
        .ok_or("No subscription available - should have been initialized during materialization")?;

    // Check for stage running events.
    use crate::messaging::PollResult;
    match subscription.poll_next().await {
        PollResult::Event(envelope) => {
            // Process stage running event.
            let event = &envelope.event;
            // Track last system event ID for tail reconciliation.
            context.last_system_event_id_seen = Some(event.id);
            if let obzenflow_core::event::SystemEventType::StageLifecycle {
                stage_id,
                event: obzenflow_core::event::StageLifecycleEvent::Running,
            } = &event.event
            {
                // Track this stage as running.
                context.running_stages.insert(*stage_id);

                // Get stage name from topology.
                let stage_info = context
                    .topology
                    .stages()
                    .find(|s| s.id == stage_id.to_topology_id());
                let stage_name = stage_info
                    .map(|s| s.name.clone())
                    .unwrap_or_else(|| "unknown".to_string());

                // Log based on stage type.
                if context
                    .topology
                    .upstream_stages(stage_id.to_topology_id())
                    .is_empty()
                {
                    tracing::debug!(
                        "Source stage '{}' is running (waiting for pipeline signal)",
                        stage_name
                    );
                } else {
                    tracing::info!("Stage '{}' is now running", stage_name);
                }
            }
        }
        PollResult::NoEvents => {
            // No new events, but that's OK: stages might already be running.
            // Add a brief sleep to avoid busy loop.
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        PollResult::Error(e) => {
            tracing::error!("Error polling system journal in Awaiting: {}", e);
            return Ok(EventLoopDirective::Transition(PipelineEvent::Error {
                message: format!("System journal error: {e}"),
            }));
        }
    }

    // Check if all non-source stages are running.
    let running_stages = &context.running_stages;
    let topology = &context.topology;

    // Get all non-source stage IDs.
    let non_source_stages: std::collections::HashSet<_> = topology
        .stages()
        .filter(|stage_info| !topology.upstream_stages(stage_info.id).is_empty())
        .map(|stage_info| StageId::from_topology_id(stage_info.id))
        .collect();

    // Check if all non-source stages are in the running set.
    let all_ready = !non_source_stages.is_empty()
        && non_source_stages
            .iter()
            .all(|stage_id| running_stages.contains(stage_id));

    if all_ready {
        if startup_mode_manual() {
            // In manual startup mode, we deliberately DO NOT auto-run
            // the pipeline; instead we wait for an explicit Run event
            // from FlowHandle (e.g. /api/flow/control Play).
            if supervisor.should_log_manual_wait() {
                tracing::info!(
                    "All {} non-source stages are running (startup_mode=manual); waiting for external Run",
                    non_source_stages.len()
                );
            }
            // Avoid a tight poll/log loop while waiting for an external Run.
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(EventLoopDirective::Continue)
        } else {
            tracing::info!(
                "All {} non-source stages are running, starting pipeline",
                non_source_stages.len()
            );
            Ok(EventLoopDirective::Transition(PipelineEvent::Run))
        }
    } else {
        // Still waiting for stages to report running.
        let waiting_for = non_source_stages.difference(running_stages).count();
        if waiting_for > 0 {
            tracing::debug!(
                "Waiting for {} more stage(s) to report running ({}/{} ready)",
                waiting_for,
                running_stages.len(),
                non_source_stages.len()
            );
        }
        Ok(EventLoopDirective::Continue)
    }
}
